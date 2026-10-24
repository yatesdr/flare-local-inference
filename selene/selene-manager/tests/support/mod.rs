#![allow(dead_code)]
use selene_manager::{
    Key,
    codec::Encoder,
    control,
    proto::{
        self, CatalogEntry, CatalogEvent, GetMessage, HelloRequest, PutMessage, PutReply,
        SubscribeRequest, get_message, get_reply, local_cache_client::LocalCacheClient,
        put_message, put_reply,
    },
    record::{self, Descriptor, Header, RankSizes},
    store::Config,
};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    Request, Status, Streaming,
    transport::{Channel, Endpoint, Uri},
};

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
        tenant: String::new(),
    }
}

pub fn base(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "cm-{}-{}-{label}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ))
}

pub fn instance_name(label: &str) -> String {
    format!(
        "cm-{}-{label}-{}",
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

pub fn key_msg(key: Key) -> proto::Key {
    proto::Key {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.to_vec(),
    }
}

pub fn key_from_msg(key: &proto::Key) -> Key {
    Key {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.as_slice().try_into().unwrap(),
    }
}

pub fn descriptor_msg(descriptor: &Descriptor) -> proto::Descriptor {
    proto::Descriptor {
        identity: descriptor.identity.to_vec(),
        fingerprint: descriptor.fingerprint.to_vec(),
        page_tokens: descriptor.page_tokens,
        ranks: descriptor
            .ranks
            .iter()
            .map(|rank| proto::RankPlanes {
                plane_bytes: rank.planes.clone(),
                row_bytes: rank.row_bytes,
            })
            .collect(),
        page_record_bytes: descriptor.page_bytes(),
        row_record_bytes: descriptor.row_bytes(),
        tenant: descriptor.tenant.clone(),
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
            .unwrap_or_else(|| std::env::temp_dir().join("cm-host"));
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
            .find_map(|line| line.strip_prefix(&format!("cm_{name} ")))
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    }

    pub fn durable_records(&self) -> usize {
        self.metrics()
            .lines()
            .filter(|line| line.starts_with("cm_records{") && line.contains("tier=\"disk\""))
            .filter_map(|line| line.rsplit(' ').next().unwrap().parse::<usize>().ok())
            .sum()
    }

    pub fn durable(&self, count: usize) {
        wait(|| {
            self.metrics()
                .lines()
                .filter(|line| line.starts_with("cm_records{") && line.contains("tier=\"disk\""))
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
        let _ = fs::remove_dir_all(&self.base);
        let _ = fs::remove_dir_all(self.shm_directory());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reg {
    Arena,
    Staging,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelAt {
    None,
    Accept,
    Grant,
    Filled,
    Commit,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PutOptions {
    pub cancel: Option<CancelAt>,
    pub cancel_after_fills: Option<u32>,
    pub abort_after_commit: bool,
    pub early_commit: bool,
    pub skip_filled: bool,
    pub corrupt_page: Option<usize>,
    pub corrupt_row: bool,
}

#[derive(Debug)]
pub enum PutOutcome {
    Stored(Vec<u8>),
    Refused(u8),
    Cancelled,
    Abandoned,
    Status(Status),
}

impl PartialEq for PutOutcome {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stored(left), Self::Stored(right)) => left == right,
            (Self::Refused(left), Self::Refused(right)) => left == right,
            (Self::Cancelled, Self::Cancelled) => true,
            (Self::Abandoned, Self::Abandoned) => true,
            (Self::Status(left), Self::Status(right)) => {
                left.code() == right.code() && left.message() == right.message()
            }
            _ => false,
        }
    }
}

impl PartialEq for GetOutcome {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Done(left), Self::Done(right)) => left == right,
            (Self::Refused(left), Self::Refused(right)) => left == right,
            (Self::Cancelled, Self::Cancelled) => true,
            (Self::Status(left), Self::Status(right)) => {
                left.code() == right.code() && left.message() == right.message()
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct GetOptions {
    pub cancel: Option<CancelAt>,
    pub corrupt_copy: bool,
}

#[derive(Debug)]
pub enum GetOutcome {
    Done(Vec<Vec<u8>>),
    Refused(u8),
    Cancelled,
    Status(Status),
}

pub struct Client {
    pub inner: LocalCacheClient<Channel>,
    pub generation: u64,
    pub descriptor: Descriptor,
    pub arena: File,
    pub staging: File,
    pub arena_bytes: u64,
    pub staging_bytes: u64,
    pub arena_path: PathBuf,
    pub staging_path: PathBuf,
    pub next: u64,
}

impl Client {
    pub async fn connect(
        config: &Config,
        descriptor: Descriptor,
    ) -> std::result::Result<Self, String> {
        let path = config.socket.clone();
        let channel = Endpoint::try_from("http://[::]:0")
            .map_err(|error| error.to_string())?
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|error| error.to_string())?;
        let mut inner = LocalCacheClient::new(channel);
        let hello = inner
            .hello(Request::new(HelloRequest {
                protocol: 1,
                replica: "l0".into(),
                descriptor: Some(descriptor_msg(&descriptor)),
            }))
            .await
            .map_err(|status| status.to_string())?
            .into_inner();
        let arena_path = PathBuf::from(&hello.arena_path);
        let staging_path = PathBuf::from(&hello.staging_path);
        let arena = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&arena_path)
            .map_err(|error| error.to_string())?;
        let staging = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&staging_path)
            .map_err(|error| error.to_string())?;
        if arena.metadata().map_err(|e| e.to_string())?.len() != hello.arena_bytes
            || staging.metadata().map_err(|e| e.to_string())?.len() != hello.staging_bytes
        {
            return Err("shared region size mismatch".into());
        }
        Ok(Self {
            inner,
            generation: hello.generation,
            descriptor,
            arena,
            staging,
            arena_bytes: hello.arena_bytes,
            staging_bytes: hello.staging_bytes,
            arena_path,
            staging_path,
            next: 1,
        })
    }

    fn file(&self, region: Reg) -> &File {
        match region {
            Reg::Arena => &self.arena,
            Reg::Staging => &self.staging,
        }
    }

    pub fn write(&self, region: Reg, offset: u64, bytes: &[u8]) {
        self.file(region).write_all_at(bytes, offset).unwrap();
    }

    pub fn read(&self, region: Reg, offset: u64, length: usize) -> Vec<u8> {
        let mut bytes = vec![0; length];
        self.file(region).read_exact_at(&mut bytes, offset).unwrap();
        bytes
    }

    pub fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "x-selene-generation",
            self.generation.to_string().parse().unwrap(),
        );
        request
    }

    pub fn begin(&self, key: Key, parent: Option<Key>) -> put_message::M {
        put_message::M::Begin(proto::PutBegin {
            namespace_id: self.descriptor.namespace_id().to_vec(),
            key: Some(key_msg(key)),
            chain_pages: key.consumed_tokens / self.descriptor.page_tokens,
            row: true,
            published_parent: parent.map(key_msg),
            has_published_parent: parent.is_some(),
            bucket: vec![4; 16],
            published_tail_pages: key.consumed_tokens / self.descriptor.page_tokens
                - parent.map_or(0, |parent| {
                    parent.consumed_tokens / self.descriptor.page_tokens
                }),
            descriptor: Some(descriptor_msg(&self.descriptor)),
        })
    }

    pub fn content(
        &self,
        key: Key,
        parent: Option<Key>,
        first: u32,
    ) -> (Header, Vec<u8>, Vec<u64>) {
        content_for(&self.descriptor, key, parent, first)
    }

    pub async fn raw_begin(
        &mut self,
        key: Key,
        parent: Option<Key>,
    ) -> (mpsc::Sender<PutMessage>, Streaming<PutReply>) {
        let (sender, receiver) = mpsc::channel(8);
        let mut replies = self
            .inner
            .put(self.request(ReceiverStream::new(receiver)))
            .await
            .unwrap()
            .into_inner();
        sender
            .send(PutMessage {
                m: Some(self.begin(key, parent)),
            })
            .await
            .unwrap();
        let _ = replies.message().await.unwrap().unwrap();
        (sender, replies)
    }

    pub async fn store(&mut self, key: Key, parent: Option<Key>) -> Vec<u8> {
        match self.put(key, parent, PutOptions::default()).await {
            PutOutcome::Stored(bytes) => bytes,
            other => panic!("store failed for {key:?} parent {parent:?}: {other:?}"),
        }
    }

    /// Stores content prepared ahead of time, skipping this call's content generation.
    pub async fn store_prepared(
        &mut self,
        key: Key,
        parent: Option<Key>,
        prepared: (Header, Vec<u8>),
    ) -> Vec<u8> {
        match self
            .put_with(key, parent, PutOptions::default(), Some(prepared))
            .await
        {
            PutOutcome::Stored(bytes) => bytes,
            other => panic!("store_prepared failed for {key:?} parent {parent:?}: {other:?}"),
        }
    }

    pub async fn put(&mut self, key: Key, parent: Option<Key>, options: PutOptions) -> PutOutcome {
        self.put_with(key, parent, options, None).await
    }

    pub async fn put_with(
        &mut self,
        key: Key,
        parent: Option<Key>,
        options: PutOptions,
        mut prepared: Option<(Header, Vec<u8>)>,
    ) -> PutOutcome {
        let (sender, receiver) = mpsc::channel(8);
        let mut replies = match self
            .inner
            .put(self.request(ReceiverStream::new(receiver)))
            .await
        {
            Ok(replies) => replies.into_inner(),
            Err(status) => return PutOutcome::Status(status),
        };
        if sender
            .send(PutMessage {
                m: Some(self.begin(key, parent)),
            })
            .await
            .is_err()
        {
            return PutOutcome::Status(Status::internal("client send"));
        }
        let mut header: Option<Header> = None;
        let mut bytes: Option<Vec<u8>> = None;
        let mut first = 0u32;
        let mut covered = 0u64;
        let mut commit_sent = false;
        let mut grants = 0u32;
        loop {
            let reply = match replies.message().await {
                Ok(Some(reply)) => reply,
                Ok(None) => return PutOutcome::Status(Status::aborted("reply stream ended")),
                Err(status) => return PutOutcome::Status(status),
            };
            match reply.r {
                Some(put_reply::R::Accept(accept)) => {
                    if options.cancel == Some(CancelAt::Accept) {
                        let _ = sender
                            .send(PutMessage {
                                m: Some(put_message::M::Cancel(proto::Cancel {})),
                            })
                            .await;
                        let _ = tokio::time::timeout(Duration::from_millis(500), replies.message())
                            .await;
                        return PutOutcome::Cancelled;
                    }
                    first = u32::try_from(accept.first_page).unwrap();
                    let parent = accept.parent.as_ref().map(key_from_msg);
                    let (value, data, _) = match prepared.take() {
                        Some((value, data)) if first == 0 => (value, data, Vec::new()),
                        Some(_) | None => self.content(key, parent, first),
                    };
                    header = Some(value);
                    bytes = Some(data);
                }
                Some(put_reply::R::Grant(grant)) => {
                    grants += 1;
                    if options.cancel == Some(CancelAt::Grant) {
                        let _ = sender
                            .send(PutMessage {
                                m: Some(put_message::M::Cancel(proto::Cancel {})),
                            })
                            .await;
                        let _ = tokio::time::timeout(Duration::from_millis(500), replies.message())
                            .await;
                        return PutOutcome::Cancelled;
                    }
                    let header = header.as_ref().unwrap().clone();
                    let source = bytes.as_mut().unwrap();
                    let memory = grant.memory.as_ref().unwrap();
                    let region = if memory.region == proto::Region::Arena as i32 {
                        Reg::Arena
                    } else {
                        Reg::Staging
                    };
                    let offset = grant.record_offset;
                    let length = usize::try_from(grant.length).unwrap();
                    let start = usize::try_from(offset).unwrap();
                    let page_bytes = header.page_record_bytes() as usize;
                    let mut page_hashes = Vec::new();
                    for position in 0..grant.page_count as usize {
                        let relative = (grant.page_first - u64::from(first)) as usize + position;
                        let page = header.page_segment as usize + relative * page_bytes;
                        if options.corrupt_page == Some(relative) {
                            source[page] ^= 1;
                        }
                        page_hashes.push(record::checksum(&source[page..page + page_bytes]));
                    }
                    let row_hash = if grant.row {
                        let row = header.row_segment as usize;
                        if options.corrupt_row {
                            source[row] ^= 1;
                        }
                        Some(record::checksum(&source[row..]))
                    } else {
                        None
                    };
                    self.write(region, memory.offset, &source[start..start + length]);
                    if options.skip_filled && grants == 1 {
                        // deliberately never acknowledge this window
                    } else if sender
                        .send(PutMessage {
                            m: Some(put_message::M::Filled(proto::WindowFilled {
                                transfer: grant.transfer,
                                index: grant.index,
                                page_xxh3: page_hashes,
                                row_xxh3: row_hash.unwrap_or(0),
                                has_row: grant.row,
                            })),
                        })
                        .await
                        .is_err()
                    {
                        return match replies.message().await {
                            Ok(Some(reply)) => match reply.r {
                                Some(put_reply::R::Refused(refused)) => {
                                    PutOutcome::Refused(u8::try_from(refused.reason).unwrap_or(0))
                                }
                                Some(put_reply::R::Stored(_)) => {
                                    PutOutcome::Stored(bytes.unwrap_or_default())
                                }
                                _ => PutOutcome::Status(Status::internal("client send")),
                            },
                            _ => PutOutcome::Status(Status::internal("client send")),
                        };
                    }
                    covered += grant.length;
                    if options
                        .cancel_after_fills
                        .is_some_and(|fills| grants >= fills)
                    {
                        let _ = sender
                            .send(PutMessage {
                                m: Some(put_message::M::Cancel(proto::Cancel {})),
                            })
                            .await;
                        let _ = tokio::time::timeout(Duration::from_millis(500), replies.message())
                            .await;
                        return PutOutcome::Cancelled;
                    }
                    let header = header.clone();
                    let payload = header.length_with_row() - header.page_segment;
                    if !commit_sent
                        && (covered >= payload || options.early_commit || options.skip_filled)
                    {
                        if options.cancel == Some(CancelAt::Filled) {
                            let _ = sender
                                .send(PutMessage {
                                    m: Some(put_message::M::Cancel(proto::Cancel {})),
                                })
                                .await;
                            let _ =
                                tokio::time::timeout(Duration::from_millis(500), replies.message())
                                    .await;
                            return PutOutcome::Cancelled;
                        }
                        if options.early_commit {
                            let fragment = vec![0u8; 4096];
                            let _ = sender
                                .send(PutMessage {
                                    m: Some(put_message::M::Commit(proto::PutCommit {
                                        transfer: grant.transfer,
                                        metadata_offset: 0,
                                        metadata: fragment,
                                    })),
                                })
                                .await;
                        } else {
                            for (index, chunk) in source[..header.page_segment as usize]
                                .chunks(4096)
                                .enumerate()
                            {
                                let _ = sender
                                    .send(PutMessage {
                                        m: Some(put_message::M::Commit(proto::PutCommit {
                                            transfer: grant.transfer,
                                            metadata_offset: (index * 4096) as u64,
                                            metadata: chunk.to_vec(),
                                        })),
                                    })
                                    .await;
                            }
                        }
                        commit_sent = true;
                        if options.abort_after_commit {
                            // Let the request body flush so the manager owns the
                            // committed Put before this stream disappears.
                            tokio::time::sleep(Duration::from_millis(300)).await;
                            return PutOutcome::Abandoned;
                        }
                        if options.cancel == Some(CancelAt::Commit) {
                            let _ = sender
                                .send(PutMessage {
                                    m: Some(put_message::M::Cancel(proto::Cancel {})),
                                })
                                .await;
                        }
                    }
                }
                Some(put_reply::R::Stored(_)) => {
                    return PutOutcome::Stored(bytes.unwrap_or_default());
                }
                Some(put_reply::R::Refused(refused)) => {
                    return PutOutcome::Refused(u8::try_from(refused.reason).unwrap_or(0));
                }
                None => return PutOutcome::Status(Status::internal("empty Put reply")),
            }
        }
    }

    pub async fn fetch(&mut self, keys: &[Key], skip: u32, pages: u32) -> Vec<Vec<u8>> {
        match self.get(keys, skip, pages, GetOptions::default()).await {
            GetOutcome::Done(records) => records,
            other => panic!("fetch failed: {other:?}"),
        }
    }

    /// Fetch into caller-owned buffers so a benchmark can reuse a pre-touched
    /// destination across samples. Returns false when the Get does not finish.
    pub async fn fetch_into(
        &mut self,
        keys: &[Key],
        skip: u32,
        pages: u32,
        destination: &mut Vec<Vec<u8>>,
    ) -> bool {
        let outcome = get_once(
            self.inner.clone(),
            self.descriptor.clone(),
            self.generation,
            self.arena.try_clone().unwrap(),
            self.staging.try_clone().unwrap(),
            keys.to_vec(),
            skip,
            pages,
            GetOptions::default(),
            Some(destination),
        )
        .await;
        match outcome {
            GetOutcome::Done(records) => {
                *destination = records;
                true
            }
            _ => false,
        }
    }

    pub async fn get(
        &mut self,
        keys: &[Key],
        skip: u32,
        pages: u32,
        options: GetOptions,
    ) -> GetOutcome {
        get_once(
            self.inner.clone(),
            self.descriptor.clone(),
            self.generation,
            self.arena.try_clone().unwrap(),
            self.staging.try_clone().unwrap(),
            keys.to_vec(),
            skip,
            pages,
            options,
            None,
        )
        .await
    }

    pub async fn fetch_two(
        &mut self,
        left: (Vec<Key>, u32, u32),
        right: (Vec<Key>, u32, u32),
    ) -> (GetOutcome, GetOutcome) {
        let descriptor = self.descriptor.clone();
        let generation = self.generation;
        let arena = self.arena.try_clone().unwrap();
        let staging = self.staging.try_clone().unwrap();
        tokio::join!(
            get_once(
                self.inner.clone(),
                descriptor.clone(),
                generation,
                arena.try_clone().unwrap(),
                staging.try_clone().unwrap(),
                left.0,
                left.1,
                left.2,
                GetOptions::default(),
                None,
            ),
            get_once(
                self.inner.clone(),
                descriptor,
                generation,
                arena,
                staging,
                right.0,
                right.1,
                right.2,
                GetOptions::default(),
                None,
            ),
        )
    }

    pub async fn subscribe(&mut self) -> Subscription {
        let stream = self
            .inner
            .subscribe(self.request(SubscribeRequest {
                namespace_id: self.descriptor.namespace_id().to_vec(),
                generation: self.generation,
                descriptor: Some(descriptor_msg(&self.descriptor)),
            }))
            .await
            .unwrap()
            .into_inner();
        let mut subscription = Subscription {
            stream,
            entries: HashMap::new(),
            seq: 0,
            snapshots: Vec::new(),
            snapshot: Vec::new(),
        };
        subscription.read_snapshot().await;
        subscription
    }

    pub async fn subscribe_descriptor(
        &mut self,
        descriptor: &Descriptor,
    ) -> std::result::Result<tonic::Response<Streaming<CatalogEvent>>, Status> {
        let mut request = Request::new(SubscribeRequest {
            namespace_id: descriptor.namespace_id().to_vec(),
            generation: self.generation,
            descriptor: Some(descriptor_msg(descriptor)),
        });
        request.metadata_mut().insert(
            "x-selene-generation",
            self.generation.to_string().parse().unwrap(),
        );
        self.inner.subscribe(request).await
    }

    pub async fn subscribe_with_generation(
        &mut self,
        generation: u64,
    ) -> std::result::Result<Subscription, Status> {
        let mut request = Request::new(SubscribeRequest {
            namespace_id: self.descriptor.namespace_id().to_vec(),
            generation,
            descriptor: Some(descriptor_msg(&self.descriptor)),
        });
        request.metadata_mut().insert(
            "x-selene-generation",
            generation.to_string().parse().unwrap(),
        );
        let stream = self.inner.subscribe(request).await?.into_inner();
        Ok(Subscription {
            stream,
            entries: HashMap::new(),
            seq: 0,
            snapshots: Vec::new(),
            snapshot: Vec::new(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub parent: Option<Key>,
    pub pages: u32,
    pub row: bool,
    pub tier: i32,
    pub durable: bool,
    pub bytes: u64,
}

pub fn entry_from_proto(entry: &CatalogEntry) -> Entry {
    Entry {
        key: key_from_msg(entry.key.as_ref().unwrap()),
        parent: entry.parent.as_ref().map(key_from_msg),
        pages: u32::try_from(entry.tail_pages).unwrap(),
        row: entry.row,
        tier: entry.tier,
        durable: entry.durable,
        bytes: entry.bytes,
    }
}

pub struct Subscription {
    pub stream: Streaming<CatalogEvent>,
    pub entries: HashMap<Key, Entry>,
    pub seq: u64,
    pub snapshots: Vec<Vec<Key>>,
    snapshot: Vec<Key>,
}

impl Subscription {
    async fn read_snapshot(&mut self) {
        loop {
            let event = self.stream.message().await.unwrap().unwrap();
            self.seq = event.sequence;
            match event.event.unwrap() {
                proto::catalog_event::Event::SnapshotBegin(_) => {
                    self.entries.clear();
                    self.snapshot.clear();
                }
                proto::catalog_event::Event::Entry(entry) => {
                    let entry = entry_from_proto(&entry);
                    assert!(
                        entry
                            .parent
                            .is_none_or(|parent| self.entries.contains_key(&parent)),
                        "snapshot child before parent"
                    );
                    self.snapshot.push(entry.key);
                    self.entries.insert(entry.key, entry);
                }
                proto::catalog_event::Event::SnapshotEnd(_) => {
                    self.snapshots.push(self.snapshot.clone());
                    return;
                }
                _ => {}
            }
        }
    }

    pub async fn next(&mut self) -> Option<proto::catalog_event::Event> {
        let event = self.stream.message().await.ok()??;
        assert_eq!(event.sequence, self.seq + 1, "catalog sequence gap");
        self.seq = event.sequence;
        let event = event.event?;
        match &event {
            proto::catalog_event::Event::Added(entry)
            | proto::catalog_event::Event::Changed(entry) => {
                let entry = entry_from_proto(entry);
                self.entries.insert(entry.key, entry);
            }
            proto::catalog_event::Event::Removed(key) => {
                let key = key_from_msg(key);
                self.entries.remove(&key);
            }
            _ => {}
        }
        Some(event)
    }

    pub async fn until_added(&mut self, key: Key) {
        while !self.entries.contains_key(&key) {
            self.next().await.expect("catalog stream ended");
        }
    }

    pub async fn until_room(&mut self) -> u64 {
        loop {
            if let proto::catalog_event::Event::Room(room) =
                self.next().await.expect("catalog stream ended")
            {
                return room.bytes;
            }
        }
    }
}

pub mod throughput;

pub fn encode_header(header: &Header) -> [u8; 4096] {
    let mut encoder = Encoder::default();
    encoder.bytes(b"FLARECKP");
    encoder.u32(1);
    encoder.u32(header.flags);
    encoder.bytes(&header.identity);
    encoder.bytes(&header.fingerprint);
    encoder.key(header.key);
    encoder.u8(header.parent.is_some() as u8);
    encoder.key(header.parent.unwrap_or_default());
    encoder.bytes(&header.bucket);
    encoder.u64(header.page_tokens);
    encoder.u32(header.tail_pages);
    encoder.u64(header.page_segment);
    encoder.u64(header.row_segment);
    encoder.u64(header.row_checksum);
    encoder.u64(header.created);
    record::encode_ranks(&mut encoder, &header.ranks);
    let mut bytes = [0; 4096];
    bytes[..encoder.0.len()].copy_from_slice(&encoder.0);
    bytes
}

pub fn content_for(
    descriptor: &Descriptor,
    key: Key,
    parent: Option<Key>,
    first: u32,
) -> (Header, Vec<u8>, Vec<u64>) {
    let pages = key.consumed_tokens / descriptor.page_tokens - u64::from(first);
    let mut header = Header::new(descriptor, key, parent, [4; 16], pages as u32).unwrap();
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

#[allow(clippy::too_many_arguments)]
async fn get_once(
    mut inner: LocalCacheClient<Channel>,
    descriptor: Descriptor,
    generation: u64,
    arena: File,
    staging: File,
    keys: Vec<Key>,
    skip: u32,
    pages: u32,
    options: GetOptions,
    destination: Option<&mut Vec<Vec<u8>>>,
) -> GetOutcome {
    let (sender, receiver) = mpsc::channel(8);
    let mut request = Request::new(ReceiverStream::new(receiver));
    request.metadata_mut().insert(
        "x-selene-generation",
        generation.to_string().parse().unwrap(),
    );
    let mut replies = match inner.get(request).await {
        Ok(replies) => replies.into_inner(),
        Err(status) => return GetOutcome::Status(status),
    };
    if sender
        .send(GetMessage {
            m: Some(get_message::M::Begin(proto::GetBegin {
                namespace_id: descriptor.namespace_id().to_vec(),
                chain: keys.iter().map(|key| key_msg(*key)).collect(),
                skip_pages: u64::from(skip),
                expected_pages: u64::from(pages),
                row: true,
                destinations: Vec::new(),
                transfer: 0,
            })),
        })
        .await
        .is_err()
    {
        return GetOutcome::Status(Status::internal("client send"));
    }
    let read = |region: Reg, offset: u64, length: usize| {
        let file = match region {
            Reg::Arena => &arena,
            Reg::Staging => &staging,
        };
        let mut bytes = vec![0; length];
        file.read_exact_at(&mut bytes, offset).unwrap();
        bytes
    };
    // A caller-owned destination is reused across Gets (benchmarks), so its
    // pages stay resident; otherwise fresh zeroed records are allocated.
    let mut owned = Vec::new();
    let records: &mut Vec<Vec<u8>> = destination.unwrap_or(&mut owned);
    let timing = std::env::var_os("SELENE_GET_TIMING").is_some();
    // Diagnostics for the Get profile: reuse one read buffer across windows and/or touch
    // every page of the destination records before the window loop.
    let reuse = std::env::var_os("SELENE_GET_REUSE").is_some();
    let prefault = std::env::var_os("SELENE_GET_PREFAULT").is_some();
    let mut scratch: Vec<u8> = Vec::new();
    let begin_sent = Instant::now();
    let mut last_release = begin_sent;
    let mut waits: Vec<f64> = Vec::new();
    let mut reads: Vec<f64> = Vec::new();
    let mut copies: Vec<f64> = Vec::new();
    let mut releases: Vec<f64> = Vec::new();
    loop {
        let reply = match replies.message().await {
            Ok(Some(reply)) => reply,
            Ok(None) => return GetOutcome::Status(Status::aborted("reply stream ended")),
            Err(status) => return GetOutcome::Status(status),
        };
        match reply.r {
            Some(get_reply::R::Leased(leased)) => {
                // Grow a reused destination in place; every byte is
                // overwritten by the metadata and the window payload.
                records.truncate(leased.record_bytes.len());
                records.resize_with(leased.record_bytes.len(), Vec::new);
                for (record, bytes) in records.iter_mut().zip(&leased.record_bytes) {
                    record.resize(*bytes as usize, 0);
                }
                if prefault {
                    for record in records.iter_mut() {
                        record.fill(0);
                    }
                }
            }
            Some(get_reply::R::Meta(meta)) => {
                let record = meta.record as usize;
                let offset = meta.metadata_offset as usize;
                let end = offset + meta.metadata.len();
                if end > records[record].len() {
                    return GetOutcome::Status(Status::out_of_range("metadata"));
                }
                records[record][offset..end].copy_from_slice(&meta.metadata);
            }
            Some(get_reply::R::Ready(grant)) => {
                if options.cancel == Some(CancelAt::Grant) {
                    let _ = sender
                        .send(GetMessage {
                            m: Some(get_message::M::Cancel(proto::Cancel {})),
                        })
                        .await;
                    let _ =
                        tokio::time::timeout(Duration::from_millis(500), replies.message()).await;
                    return GetOutcome::Cancelled;
                }
                let grant_at = Instant::now();
                let wait = (grant_at - last_release).as_secs_f64();
                let memory = grant.memory.as_ref().unwrap();
                let region = if memory.region == proto::Region::Arena as i32 {
                    Reg::Arena
                } else {
                    Reg::Staging
                };
                let length = usize::try_from(memory.length).unwrap();
                let read_started = Instant::now();
                if reuse {
                    scratch.resize(length, 0);
                    let file = match region {
                        Reg::Arena => &arena,
                        Reg::Staging => &staging,
                    };
                    file.read_exact_at(&mut scratch, memory.offset).unwrap();
                } else {
                    scratch = read(region, memory.offset, length);
                }
                let read_span = read_started.elapsed().as_secs_f64();
                if options.corrupt_copy {
                    scratch[0] ^= 1;
                }
                let data = &scratch[..length];
                let record = grant.record as usize;
                let start = grant.record_offset as usize;
                let copy_started = Instant::now();
                records[record][start..start + length].copy_from_slice(data);
                let copy_span = copy_started.elapsed().as_secs_f64();
                let release_started = Instant::now();
                let _ = sender
                    .send(GetMessage {
                        m: Some(get_message::M::Released(proto::WindowReleased {
                            transfer: grant.transfer,
                            index: grant.index,
                        })),
                    })
                    .await;
                let release_span = release_started.elapsed().as_secs_f64();
                last_release = Instant::now();
                if timing {
                    waits.push(wait);
                    reads.push(read_span);
                    copies.push(copy_span);
                    releases.push(release_span);
                }
            }
            Some(get_reply::R::Done(_)) => {
                if timing {
                    let summary = |values: &mut Vec<f64>| -> (f64, f64) {
                        if values.is_empty() {
                            return (0.0, 0.0);
                        }
                        let mean = values.iter().sum::<f64>() / values.len() as f64;
                        let max = values.iter().copied().fold(0.0f64, f64::max);
                        (mean, max)
                    };
                    let (wait_mean, wait_max) = summary(&mut waits);
                    let (read_mean, read_max) = summary(&mut reads);
                    let (copy_mean, copy_max) = summary(&mut copies);
                    let (release_mean, release_max) = summary(&mut releases);
                    eprintln!(
                        "get-timing windows={} total_s={:.6} wait_mean_ms={:.3}                          wait_max_ms={:.3} read_mean_ms={:.3} read_max_ms={:.3}                          copy_mean_ms={:.3} copy_max_ms={:.3} release_mean_ms={:.3}                          release_max_ms={:.3}",
                        waits.len(),
                        begin_sent.elapsed().as_secs_f64(),
                        wait_mean * 1e3,
                        wait_max * 1e3,
                        read_mean * 1e3,
                        read_max * 1e3,
                        copy_mean * 1e3,
                        copy_max * 1e3,
                        release_mean * 1e3,
                        release_max * 1e3,
                    );
                }
                return GetOutcome::Done(std::mem::take(records));
            }
            Some(get_reply::R::Refused(refused)) => {
                return GetOutcome::Refused(u8::try_from(refused.reason).unwrap_or(0));
            }
            None => return GetOutcome::Status(Status::internal("empty Get reply")),
        }
    }
}
