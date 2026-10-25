//! A manager with a network listener and a data listener, started the way the binary
//! starts it, for tests of sessions that reach it from another host.

use super::pool::Pool;
use selene_manager::{config::Config as NetConfig, control, store::Config};
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tonic::transport::Endpoint;

pub struct Net {
    pub socket: PathBuf,
    pub address: SocketAddr,
    pub data: SocketAddr,
    pub base: PathBuf,
    pub name: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

/// A loopback address nobody listens on. Taken from below the ephemeral range and handed out
/// once, so concurrent tests never share one or meet a connection's own port.
pub fn free_address() -> SocketAddr {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let port = 20_000
            + (std::process::id() % 2_000) * 4
            + NEXT.fetch_add(1, Ordering::Relaxed) % 4_000;
        let address = SocketAddr::from(([127, 0, 0, 1], port as u16));
        if TcpListener::bind(address).is_ok() {
            return address;
        }
    }
}

impl Net {
    pub fn start(tweak: impl FnOnce(&mut NetConfig)) -> Self {
        Self::start_with(tweak, |_| {})
    }

    pub fn start_with(
        tweak: impl FnOnce(&mut NetConfig),
        store_tweak: impl FnOnce(&mut Config),
    ) -> Self {
        let base = std::env::temp_dir().join(format!(
            "selene-net-{}-{}",
            std::process::id(),
            super::instance_name("n")
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let socket = base.join("run/m.sock");
        let address = free_address();
        let data = free_address();
        let mut store = Config::dram_only(socket.clone(), 8 * 1024 * 1024);
        let net_name = super::instance_name("net");
        store.name = net_name.clone();
        store_tweak(&mut store);
        let mut net = NetConfig::defaults();
        net.name = store.name.clone();
        net.socket = socket.clone();
        net.socket_explicit = true;
        net.memory = 8 * 1024 * 1024;
        net.staging = 8 << 20;
        net.listen = Some(address);
        net.listen_data = Some(data);
        tweak(&mut net);
        let data = net.data_address().unwrap().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::spawn(move || {
            if let Err(error) = control::serve(net, store, flag) {
                eprintln!("manager exited: {error}");
            }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(address).is_err() || TcpStream::connect(data).is_err() {
            assert!(Instant::now() < deadline, "the listeners never came up");
            thread::sleep(Duration::from_millis(10));
        }
        Self {
            socket,
            address,
            data,
            base,
            name: net_name,
            stop,
            thread: Some(thread),
        }
    }

    pub async fn unix(&self) -> Pool {
        Pool::connect(&self.socket).await
    }

    pub async fn tcp(&self) -> Pool {
        let channel = Endpoint::from_shared(format!("http://{}", self.address))
            .unwrap()
            .connect()
            .await
            .expect("connect over TCP");
        Pool::over(channel, &self.socket).await
    }
}

impl Drop for Net {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_dir_all(&self.base);
        let _ = std::fs::remove_dir_all(std::path::Path::new("/dev/shm/selene").join(&self.name));
    }
}
