#![allow(dead_code, unused_imports)]
use selene_manager::{
    Key,
    control,
    record::{self, Descriptor, Header, RankSizes},
    store::Config,
};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

pub mod channel;
pub mod store_client;
pub mod net;
pub mod pool;
#[cfg(feature = "rdma")]
pub mod rdma;

pub use store_client::*;

static SERIAL: AtomicU64 = AtomicU64::new(0);
static HOST_BUSY: AtomicBool = AtomicBool::new(false);

// Serialises manager-backed tests: each manager owns region files, a disk
// worker and a socket, and running twenty of them at once exhausts container
// file descriptors.
thread_local! {
    static HOST_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct HostSlot {
    root: bool,
}

impl HostSlot {
    fn acquire() -> Self {
        let root = HOST_DEPTH.with(|depth| {
            let value = depth.get();
            depth.set(value + 1);
            value == 0
        });
        if root {
            while HOST_BUSY.swap(true, Ordering::Acquire) {
                thread::sleep(Duration::from_millis(5));
            }
        }
        Self { root }
    }
}

impl Drop for HostSlot {
    fn drop(&mut self) {
        HOST_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
        if self.root {
            HOST_BUSY.store(false, Ordering::Release);
        }
    }
}

pub fn key(tokens: u64) -> Key {
    Key {
        consumed_tokens: tokens,
        digest: (tokens as u128 * 7919).to_le_bytes(),
    }
}

pub fn descriptor() -> Descriptor {
    Descriptor {
        identity: [7; 32],
        fingerprint: [3; 16],
        page_tokens: 64,
        ranks: vec![RankSizes {
            planes: vec![4000],
            row_bytes: 1000,
        }],
        pool_name: "tenant".into(),
    }
}

pub fn base(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "selene-{}-{}-{label}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Staging for a test manager: small, since many run at once in a small `/dev/shm`.
pub const TEST_STAGING: usize = 16 * 1024 * 1024;

/// Removes an instance's shared-memory directory when dropped. The manager keeps its region files
/// across restarts, so a test that runs the binary must clean up after it.
pub struct ShmCleanup(String);

impl ShmCleanup {
    pub fn new(name: &str) -> Self {
        Self(name.to_owned())
    }
}

impl Drop for ShmCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(std::path::Path::new("/dev/shm/selene").join(&self.0));
    }
}

/// Removes the shared-memory directories of a test child that was killed, which cannot clean up
/// after itself. They are named by the child's pid, which is gone.
pub fn remove_children_shm(pid: u32) {
    let prefix = format!("selene-{pid}-");
    if let Ok(entries) = std::fs::read_dir("/dev/shm/selene") {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
}

pub fn instance_name(label: &str) -> String {
    format!(
        "selene-{}-{label}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    )
}

/// A probed loopback port for a manager's metrics endpoint. The probe can lose the port to
/// another process before the manager binds it; `Host::start` retries with a fresh one.
fn free_metrics_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    drop(listener);
    address
}

pub fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(5));
    }
}

pub struct Host {
    pub base: PathBuf,
    pub config: Config,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    _slot: HostSlot,
}

impl Host {
    pub fn new(memory: usize, nvme: u64) -> Self {
        let base = base("host");
        let name = instance_name("host");
        let mut config = Config::new(base.join("records"), base.join("run/m.sock"), memory, nvme);
        config.name = name;
        config.staging = TEST_STAGING;
        config.metrics = Some(free_metrics_address());
        Self {
            base,
            config,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
            _slot: HostSlot::acquire(),
        }
    }

    pub fn dram_only(memory: usize) -> Self {
        let base = base("host");
        let name = instance_name("host");
        let mut config = Config::dram_only(base.join("run/m.sock"), memory);
        config.name = name;
        config.staging = TEST_STAGING;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        config.metrics = Some(listener.local_addr().unwrap().to_string());
        drop(listener);
        Self {
            base,
            config,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
            _slot: HostSlot::acquire(),
        }
    }

    pub fn open_config(mut config: Config) -> Self {
        if config.metrics.is_none() {
            config.metrics = Some(free_metrics_address());
        }
        let base = config
            .root
            .as_ref()
            .and_then(|root| root.parent().map(Path::to_path_buf))
            .or_else(|| {
                config
                    .socket
                    .parent()
                    .and_then(|path| path.parent().map(Path::to_path_buf))
            })
            .unwrap_or_else(|| std::env::temp_dir().join("selene-host"));
        Self {
            base,
            config,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
            _slot: HostSlot::acquire(),
        }
    }

    pub fn wait(&self, mut condition: impl FnMut(&Host) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !condition(self) {
            assert!(Instant::now() < deadline, "condition timed out");
            thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn start(&mut self) {
        for attempt in 0..4 {
            if attempt > 0 {
                // A failed attempt may have lost its metrics port after the probe.
                self.config.metrics = Some(free_metrics_address());
            }
            match self.try_start() {
                Ok(()) => {
                    // A manager that failed after creating its socket is not serving.
                    thread::sleep(Duration::from_millis(20));
                    if self
                        .thread
                        .as_ref()
                        .is_some_and(|thread| !thread.is_finished())
                    {
                        return;
                    }
                    if let Some(thread) = self.thread.take() {
                        let _ = thread.join();
                    }
                }
                Err(error) => {
                    assert_eq!(
                        error.kind(),
                        std::io::ErrorKind::AddrInUse,
                        "manager did not start: {error}"
                    );
                }
            }
        }
        panic!("manager did not start");
    }

    pub fn try_start(&mut self) -> std::io::Result<()> {
        self.stop.store(false, Ordering::Release);
        let config = self.config.clone();
        let stop = self.stop.clone();
        self.thread = Some(thread::spawn(move || {
            if let Err(error) = control::run(config, stop) {
                eprintln!("manager exited: {error}");
            }
        }));
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.config.socket.exists() {
            if self
                .thread
                .as_ref()
                .is_some_and(thread::JoinHandle::is_finished)
            {
                let handle = self.thread.take().unwrap();
                let _ = handle.join();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    "manager failed to start",
                ));
            }
            if Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "manager did not start",
                ));
            }
            thread::sleep(Duration::from_millis(5));
        }
        Ok(())
    }

    /// Asks the manager to stop without waiting for it.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        if let Some(root) = &self.config.root {
            wait(|| selene_manager::disk::RootLock::acquire(root).is_ok());
        }
    }

    pub async fn client(&self) -> Client {
        Client::connect(&self.config, descriptor())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "a client could not connect to the manager at {}: {error}",
                    self.config.socket.display()
                )
            })
    }

    pub async fn client_with(&self, descriptor: Descriptor) -> std::result::Result<Client, String> {
        Client::connect(&self.config, descriptor).await
    }

    pub fn metrics(&self) -> String {
        let mut stream = TcpStream::connect(self.config.metrics.as_ref().unwrap()).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut text = String::new();
        stream.read_to_string(&mut text).unwrap();
        text
    }

    pub fn gauge(&self, name: &str) -> u64 {
        self.metrics()
            .lines()
            .find_map(|line| line.strip_prefix(&format!("selene_{name} ")))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    pub fn durable_records(&self) -> usize {
        self.metrics()
            .lines()
            .filter(|line| line.starts_with("selene_records{") && line.contains("tier=\"disk\""))
            .filter_map(|line| line.rsplit(' ').next().unwrap().parse::<usize>().ok())
            .sum()
    }

    pub fn durable(&self, count: usize) {
        wait(|| {
            self.metrics()
                .lines()
                .filter(|line| {
                    line.starts_with("selene_records{") && line.contains("tier=\"disk\"")
                })
                .filter_map(|line| line.rsplit(' ').next().unwrap().parse::<usize>().ok())
                .sum::<usize>()
                == count
        });
    }

    pub fn shm_directory(&self) -> PathBuf {
        PathBuf::from("/dev/shm/selene").join(&self.config.name)
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.stop();
        // The fixture owns only directories it was given for itself: removing a shared
        // parent (/dev/shm, a scratch root) would delete other managers' live files.
        let own = self
            .base
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("selene-"));
        if own {
            let _ = fs::remove_dir_all(&self.base);
        } else {
            eprintln!(
                "fixture: not removing {}: it is not private to this test",
                self.base.display()
            );
        }
        let _ = fs::remove_dir_all(self.shm_directory());
    }
}

pub fn encode_header(header: &Header) -> [u8; 4096] {
    header.encode().unwrap()
}

pub fn content_for(
    descriptor: &Descriptor,
    key: Key,
    parent: Option<Key>,
    first: u32,
) -> (Header, Vec<u8>, Vec<u64>) {
    let pages = key.consumed_tokens / descriptor.page_tokens - u64::from(first);
    let mut header = selene_manager::record::new_header(descriptor, key, parent, [4; 16], pages as u32).unwrap();
    let mut bytes = vec![0; header.length_with_row() as usize];
    let page_bytes = header.page_record_bytes() as usize;
    let mut hashes = Vec::new();
    for page in 0..pages as usize {
        let offset = header.page_segment as usize + page * page_bytes;
        let data = &mut bytes[offset..offset + page_bytes];
        for (index, byte) in data.iter_mut().enumerate() {
            *byte = ((page + first as usize) * 31 + index * 7) as u8;
        }
        hashes.push(record::checksum(data));
    }
    for (index, byte) in bytes[header.row_segment as usize..].iter_mut().enumerate() {
        *byte = (key.consumed_tokens as usize + index * 13) as u8;
    }
    header.row_checksum = record::checksum(&bytes[header.row_segment as usize..]);
    bytes[..4096].copy_from_slice(&encode_header(&header));
    for (index, hash) in hashes.iter().enumerate() {
        bytes[4096 + index * 8..4096 + index * 8 + 8].copy_from_slice(&hash.to_le_bytes());
    }
    (header, bytes, hashes)
}

pub fn recovered_keys(root: &Path) -> Vec<Key> {
    selene_manager::disk::recover(root)
        .unwrap()
        .iter()
        .flat_map(|namespace| namespace.records.iter().map(|(header, _, _)| header.key))
        .collect()
}
