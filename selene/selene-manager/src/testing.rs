//! In-process manager used by client and native integration tests.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
pub struct Manager {
    pub config: crate::store::Config,
    pub base: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Manager {
    pub fn new() -> Self {
        Self::sized(8 << 20, 8 << 20)
    }
    pub fn start() -> Self {
        Self::with_memory(16 << 20)
    }
    pub fn with_memory(memory: usize) -> Self {
        let mut manager = Self::sized(memory, 16 << 20);
        manager.run();
        manager
    }
    fn sized(memory: usize, staging: u64) -> Self {
        static SERIAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let name = format!(
            "flare-pool-startup-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        );
        let directory = std::env::temp_dir().join(&name);
        std::fs::create_dir_all(&directory).unwrap();
        let mut config = crate::store::Config::dram_only(directory.join("manager.sock"), memory);
        config.name = name;
        config.staging = staging;
        Self {
            config,
            base: directory,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
        }
    }
    pub fn run(&mut self) {
        self.stop.store(false, Ordering::Release);
        let (config, stop) = (self.config.clone(), self.stop.clone());
        self.thread = Some(thread::spawn(move || {
            crate::control::run(config, stop).unwrap()
        }));
        wait("manager socket", || self.config.socket.exists());
        if let Some(address) = self.config.listen {
            wait("manager network listener", || {
                std::net::TcpStream::connect(address).is_ok()
            });
        }
    }
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
    pub fn restart(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.config.socket);
        self.run();
    }
    pub fn address(&self) -> String {
        format!("unix:{}", self.config.socket.display())
    }
}
impl Drop for Manager {
    fn drop(&mut self) {
        self.stop();
        std::fs::remove_dir_all(&self.base).unwrap();
        let regions = std::path::Path::new("/dev/shm/selene").join(&self.config.name);
        if regions.exists() {
            std::fs::remove_dir_all(regions).unwrap();
        }
    }
}
pub fn wait(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(10));
    }
}

/// Each test process hands out each port only once, across all its fixtures.
pub fn free_address() -> std::net::SocketAddr {
    use std::sync::atomic::AtomicU32;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let index = NEXT.fetch_add(1, Ordering::Relaxed);
        assert!(index < 4_000, "the test process exhausted its port range");
        let port = 20_000 + (std::process::id() % 2_000) * 4 + index;
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], port as u16));
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
}
