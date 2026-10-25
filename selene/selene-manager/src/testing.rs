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
    directory: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Manager {
    pub fn new() -> Self {
        static SERIAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let name = format!(
            "flare-pool-startup-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        );
        let directory = std::env::temp_dir().join(&name);
        std::fs::create_dir_all(&directory).unwrap();
        let mut config = crate::store::Config::dram_only(directory.join("manager.sock"), 8 << 20);
        config.name = name;
        config.staging = 8 << 20;
        Self {
            config,
            directory,
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
        wait(|| self.config.socket.exists());
    }
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
    pub fn address(&self) -> String {
        format!("unix:{}", self.config.socket.display())
    }
}
impl Drop for Manager {
    fn drop(&mut self) {
        self.stop();
        std::fs::remove_dir_all(&self.directory).unwrap();
        let regions = std::path::Path::new("/dev/shm/selene").join(&self.config.name);
        if regions.exists() {
            std::fs::remove_dir_all(regions).unwrap();
        }
    }
}
pub fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(Instant::now() < deadline, "startup condition timed out");
        thread::sleep(Duration::from_millis(10));
    }
}
