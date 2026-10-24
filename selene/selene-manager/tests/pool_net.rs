//! Pool protocol over the network listener: sessions without shared memory, TLS, RDMA
//! and the startup warnings. The data channel is not served yet, so claims and puts that
//! need it are refused with a reason; everything else works as on the Unix socket.

mod support;

use selene_manager::{
    config::Config as NetConfig,
    control,
    proto::v2::{self, client_message as c, server_message::M},
    store::Config,
};
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::pool::{self, Ident, Pool, Session, chain, saved};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

/// A manager with a network listener, started the way the binary starts it.
struct Net {
    socket: PathBuf,
    address: SocketAddr,
    base: PathBuf,
    name: String,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

impl Net {
    fn start(tweak: impl FnOnce(&mut NetConfig)) -> Self {
        let base = std::env::temp_dir().join(format!(
            "selene-net-{}-{}",
            std::process::id(),
            support::instance_name("n")
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let socket = base.join("run/m.sock");
        let address = free_address();
        let mut store = Config::dram_only(socket.clone(), 8 * 1024 * 1024);
        let net_name = support::instance_name("net");
        store.name = net_name.clone();
        store.pool.source_wait = Duration::ZERO;
        let mut net = NetConfig::defaults();
        net.name = store.name.clone();
        net.socket = socket.clone();
        net.socket_explicit = true;
        net.memory = 8 * 1024 * 1024;
        net.listen = Some(address);
        tweak(&mut net);
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = thread::spawn(move || {
            if let Err(error) = control::serve(net, store, flag) {
                eprintln!("manager exited: {error}");
            }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(address).is_err() {
            assert!(Instant::now() < deadline, "the listener never came up");
            thread::sleep(Duration::from_millis(10));
        }
        Self {
            socket,
            address,
            base,
            name: net_name,
            stop,
            thread: Some(thread),
        }
    }

    async fn unix(&self) -> Pool {
        Pool::connect(&self.socket).await
    }

    async fn tcp(&self) -> Pool {
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

async fn open(pool: &mut Pool, name: &str) -> Session {
    let mut session = pool.open(&Ident::new(name)).await;
    assert_eq!(session.opened.status, v2::OpenStatus::OpenOk as i32);
    session.snapshot(vec![]).await;
    session
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h6_a_tcp_session_has_no_shared_memory_and_serves_no_exports() {
    let net = Net::start(|_| {});
    let unix = net.unix().await;
    assert!(unix.hello.shared_memory.is_some());
    let mut tcp = net.tcp().await;
    assert!(
        tcp.hello.shared_memory.is_none(),
        "no shared memory over TCP"
    );
    assert_eq!(tcp.hello.encryption, v2::Encryption::Plaintext as i32);
    assert_eq!(
        tcp.hello.data_transports,
        vec![v2::DataTransport::Tcp as i32]
    );
    assert_eq!(tcp.hello.host_id, unix.hello.host_id);
    let session = tcp.open(&Ident::new("remote")).await;
    assert_eq!(
        session.opened.export_slots, 0,
        "an instance without shared memory is no source"
    );
    assert_eq!(session.opened.claim_slots, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l4_and_a22_remote_sessions_see_only_the_managers_records_as_level_five() {
    let net = Net::start(|_| {});
    let mut unix = net.unix().await;
    // A same-host source with shared memory, and a record in the manager's store.
    let mut source = open(&mut unix, "source").await;
    source
        .notices(
            chain(&[1024], v2::Tier::Dram, 1)
                .into_iter()
                .map(saved)
                .collect(),
        )
        .await;
    let mut writer = open(&mut unix, "writer").await;
    let record = support::key(2048);
    match writer
        .put_record(1, support::key(1024), None, pool::Damage::default())
        .await
    {
        pool::PutResult::Stored(_) => {}
        other => panic!("{other:?}"),
    }
    let _ = record;
    // Same host: the manager's DRAM is level 3.
    let mut near = open(&mut unix, "near").await;
    let local = near.manager_candidate(1, 1024).await;
    assert_eq!(local.level, v2::Level::HostDram as i32);
    // Another host: every manager record is level 5, and instance sources are not offered.
    let mut tcp = net.tcp().await;
    let mut far = open(&mut tcp, "far").await;
    let remote = far.manager_candidate(2, 1024).await;
    assert_eq!(remote.level, v2::Level::RemoteNvme as i32);
    assert!(remote.source.as_ref().unwrap().instance_id.is_empty());
    let reply = far.lookup(3, 1024, vec![]).await;
    assert!(
        reply.candidates.iter().all(|candidate| candidate
            .source
            .as_ref()
            .is_some_and(|source| source.instance_id.is_empty())),
        "an instance-sourced candidate needs shared memory on both sides"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_data_channel_is_refused_with_a_reason() {
    let net = Net::start(|_| {});
    let mut tcp = net.tcp().await;
    let mut far = open(&mut tcp, "far").await;
    // A claim in the channel mode, and one in the mode this session cannot use.
    let mut claim = v2::Claim {
        claim_id: 1,
        level: v2::Level::RemoteNvme as i32,
        source: Some(v2::SourceRef::default()),
        tier: v2::Tier::Dram as i32,
        key: Some(pool::pkey(1024)),
        generation: 1,
        begin_tokens: 0,
        want_row: true,
        mode: v2::CopyMode::DataChannel as i32,
        deadline_ms: 0,
        plan_id: 0,
        part: 1,
    };
    far.claim(claim.clone()).await;
    assert_eq!(
        far.claim_terminal(1).await.reason,
        v2::ClaimReason::InvalidClaim as i32
    );
    claim.claim_id = 2;
    claim.mode = v2::CopyMode::SharedWindows as i32;
    far.claim(claim).await;
    assert_eq!(
        far.claim_terminal(2).await.reason,
        v2::ClaimReason::InvalidClaim as i32
    );
    // The same for a put, with the reason in the detail.
    for (id, mode, needle) in [
        (1, v2::CopyMode::DataChannel, "data channel"),
        (2, v2::CopyMode::SharedWindows, "shared windows"),
    ] {
        far.send(c::M::PutBegin(v2::PutBegin {
            put_id: id,
            key: Some(pool::pkey(1024)),
            chain_pages: 16,
            row: true,
            has_published_parent: false,
            published_parent: None,
            bucket: vec![4; 16],
            published_tail_pages: 16,
            mode: mode as i32,
            generation: 1,
        }))
        .await;
        let refused = far
            .take(|m| match m {
                M::PutRefused(refused) if refused.put_id == id => Some(refused.clone()),
                _ => None,
            })
            .await;
        assert_eq!(refused.reason, v2::Reason::Invalid as i32);
        assert!(refused.detail.contains(needle), "{}", refused.detail);
    }
}

/// A self-signed certificate for `localhost`, as PEM files in `dir`.
fn certificate(dir: &std::path::Path) -> (PathBuf, PathBuf, String) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = dir.join("server.pem");
    let key = dir.join("server-key.pem");
    let pem = generated.cert.pem();
    std::fs::write(&cert, &pem).unwrap();
    std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
    (cert, key, pem)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h4_a_tls_manager_declares_tls_and_offers_no_rdma() {
    let dir = std::env::temp_dir().join(format!("selene-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key, pem) = certificate(&dir);
    let net = Net::start(|config| {
        config.tls_cert = Some(cert.clone());
        config.tls_key = Some(key.clone());
    });
    let channel = Endpoint::from_shared(format!("https://localhost:{}", net.address.port()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(pem.clone()))
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await
        .expect("a client with the manager's CA connects");
    let mut tls = Pool::over(channel, &net.socket).await;
    assert_eq!(tls.hello.encryption, v2::Encryption::Tls as i32);
    assert!(
        !tls.hello
            .data_transports
            .contains(&(v2::DataTransport::Rdma as i32)),
        "RDMA is never offered by a TLS manager"
    );
    let session = tls.open(&Ident::new("far")).await;
    assert_eq!(session.opened.status, v2::OpenStatus::OpenOk as i32);
    // A client that does not hold the manager's CA is refused before any call.
    let other = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let refused = Endpoint::from_shared(format!("https://localhost:{}", net.address.port()))
        .unwrap()
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(other.cert.pem()))
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await;
    assert!(
        refused.is_err(),
        "a manager whose certificate the client does not trust"
    );
    let _ = std::fs::remove_dir_all(dir);
}

fn selene(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_selene"))
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap()
}

#[test]
fn rdma_with_tls_is_refused_at_startup_with_the_reason() {
    let dir = std::env::temp_dir().join(format!("selene-rdma-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert, key, _) = certificate(&dir);
    let socket = dir.join("run/m.sock");
    let output = selene(&[
        "--socket",
        socket.to_str().unwrap(),
        "--memory",
        "8M",
        "--listen",
        "127.0.0.1:0",
        "--rdma",
        "--tls-cert",
        cert.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stderr);
    if cfg!(feature = "rdma") {
        assert!(text.contains("not encrypted"), "{text}");
    } else {
        assert!(text.contains("`rdma` feature"), "{text}");
    }
    assert!(!socket.exists(), "nothing was started");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn startup_names_each_option_that_widens_the_exposure() {
    let dir = std::env::temp_dir().join(format!("selene-warn-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let socket = dir.join("run/m.sock");
    let address = free_address();
    let child = Command::new(env!("CARGO_BIN_EXE_selene"))
        .args([
            "--name",
            &support::instance_name("warn"),
            "--socket",
            socket.to_str().unwrap(),
            "--memory",
            "8M",
            "--listen",
            &address.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "the manager never started");
        thread::sleep(Duration::from_millis(20));
    }
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let output = child.wait_with_output().unwrap();
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(
        text.contains("warning: network listening enabled on") && text.contains("plaintext"),
        "{text}"
    );
    assert!(!text.contains("RDMA"), "{text}");
    let _ = std::fs::remove_dir_all(dir);
}
