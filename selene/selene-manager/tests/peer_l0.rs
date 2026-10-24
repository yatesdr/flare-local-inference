//! Two-manager L0: near manager forwards to a storage manager over a real TCP peer channel.
//!
//! Covers the committed wiring end to end: local Put -> ForwardEvent -> forward_source lease ->
//! PeerCache.Put into the storage manager's windows, a remote fetch back into a client region,
//! and a forward that fails after the storage manager goes away.

mod support;

/// TLS handshakes and two managers per test are fd-heavy; run these one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, SanType,
};
use selene_manager::{
    arena::Arena,
    config::{self, PeerTransport, Upstream},
    control, proto, record,
    regions::LocalRegions,
    store::Config as StoreConfig,
};
use std::net::{IpAddr, Ipv4Addr};
use std::{
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use support::*;

struct Manager {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    socket: PathBuf,
    metrics: u16,
}

impl Manager {
    fn start(config: config::Config) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let socket = config.socket.clone();
        let metrics = config.metrics.unwrap().port();
        let store = control::store_config(&config).unwrap();
        let thread = thread::spawn(move || {
            let _ = control::run_peer(config, store, thread_stop);
        });
        wait_slow(|| socket.exists());
        Self {
            stop,
            thread: Some(thread),
            socket,
            metrics,
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }

    fn metric(&self, name: &str) -> f64 {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Ok(response) = TcpStream::connect(("127.0.0.1", self.metrics)) {
                use std::io::{Read, Write};
                let mut response = response;
                let _ = response.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n");
                let mut text = String::new();
                let _ = response.read_to_string(&mut text);
                for line in text.lines() {
                    if let Some(value) = line.strip_prefix(&format!("cm_{name} ")) {
                        return value.parse().unwrap_or(0.0);
                    }
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        0.0
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.stop();
        }
    }
}

fn port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn wait_connect(address: u16) {
    wait_slow(|| TcpStream::connect(("127.0.0.1", address)).is_ok());
}

/// Poll less often than `support::wait`: every attempt opens a client connection, and the
/// container's file-descriptor limit is low.
fn wait_slow(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(50));
    }
}

struct Tls {
    ca: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

fn tls_paths(base: &Path) -> Tls {
    Tls {
        ca: base.join("ca.pem"),
        server_cert: base.join("server.pem"),
        server_key: base.join("server-key.pem"),
        client_cert: base.join("client.pem"),
        client_key: base.join("client-key.pem"),
    }
}

fn make_tls(base: &Path) -> Tls {
    let paths = tls_paths(base);
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .key_usages
        .push(rcgen::KeyUsagePurpose::KeyCertSign);
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();
    std::fs::write(&paths.ca, ca.pem()).unwrap();
    let issuer = Issuer::new(ca_params.clone(), ca_key);
    let mut server_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = KeyPair::generate().unwrap();
    let server = server_params.signed_by(&server_key, &issuer).unwrap();
    std::fs::write(&paths.server_cert, server.pem()).unwrap();
    std::fs::write(&paths.server_key, server_key.serialize_pem()).unwrap();
    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_key = KeyPair::generate().unwrap();
    let client = client_params.signed_by(&client_key, &issuer).unwrap();
    std::fs::write(&paths.client_cert, client.pem()).unwrap();
    std::fs::write(&paths.client_key, client_key.serialize_pem()).unwrap();
    paths
}

fn storage(
    base: &Path,
    name: &str,
    peer: u16,
    data: u16,
    metrics: u16,
    tls: &Tls,
) -> config::Config {
    let mut config = config::Config::defaults();
    config.name = name.to_string();
    config.socket = base.join(format!("{name}.sock"));
    config.memory = 16 << 20;
    config.staging = 8 << 20;
    config.listen_peer = Some(format!("127.0.0.1:{peer}").parse().unwrap());
    config.listen_data = Some(format!("127.0.0.1:{data}").parse().unwrap());
    config.metrics = Some(format!("127.0.0.1:{metrics}").parse().unwrap());
    config.tls_cert = Some(tls.server_cert.clone());
    config.tls_key = Some(tls.server_key.clone());
    config.tls_ca = Some(tls.ca.clone());
    config
}

fn near(
    base: &Path,
    name: &str,
    upstream: u16,
    metrics: u16,
    transport: PeerTransport,
    retain_remote: bool,
    tls: &Tls,
) -> config::Config {
    let mut config = config::Config::defaults();
    config.name = name.to_string();
    config.socket = base.join(format!("{name}.sock"));
    config.memory = 16 << 20;
    config.staging = 8 << 20;
    config.metrics = Some(format!("127.0.0.1:{metrics}").parse().unwrap());
    config.upstreams = vec![Upstream {
        name: "storage".to_string(),
        address: format!("127.0.0.1:{upstream}"),
        weight: 1,
    }];
    config.peer_transport = transport;
    config.retain_remote = retain_remote;
    config.tls_cert = Some(tls.client_cert.clone());
    config.tls_key = Some(tls.client_key.clone());
    config.tls_ca = Some(tls.ca.clone());
    config
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn local(socket: &Path) -> StoreConfig {
    StoreConfig::dram_only(socket.to_path_buf(), 16 << 20)
}

/// Fetch one record straight from the storage manager into scratch shared regions.
fn remote_fetch(
    peer: u16,
    key: selene_manager::Key,
    transport: &str,
    tls: &Tls,
) -> (Vec<u8>, Option<bool>) {
    let descriptor = descriptor();
    let regions = Arc::new(LocalRegions::new(
        Arena::new(1 << 20).unwrap(),
        Arena::new(1 << 20).unwrap(),
    ));
    runtime().block_on(async move {
        let shared: Arc<dyn selene_manager::data::RegionAccess> = regions.clone();
        let mut client = selene_manager::peer_client::PeerClient::connect(
            &format!("127.0.0.1:{peer}"),
            "l0-fetch",
            shared,
            2,
            Some(selene_manager::peer_client::TlsSettings {
                ca: std::fs::read(&tls.ca).unwrap(),
                cert: std::fs::read(&tls.client_cert).unwrap(),
                key: std::fs::read(&tls.client_key).unwrap(),
                domain: "127.0.0.1".to_string(),
            }),
            transport,
        )
        .await
        .unwrap();
        let rdma = client.uses_rdma();
        let fetched = client
            .fetch(selene_manager::peer_client::FetchRequest {
                descriptor: selene_manager::peer_client::descriptor_to_proto(&descriptor, ""),
                namespace: descriptor.namespace_id(),
                chain: vec![key],
                skip_pages: 0,
                expected_pages: key.consumed_tokens / descriptor.page_tokens,
                row: true,
                destinations: vec![proto::GetDestination {
                    record: 0,
                    region: proto::Region::Arena as i32,
                    offset: 0,
                    bytes: 1 << 20,
                }],
            })
            .await
            .unwrap();
        let mut metadata = Vec::new();
        use selene_manager::data::RegionAccess as _;
        regions
            .with_read(
                selene_manager::data::Region::Arena,
                0,
                fetched.metadata[0].len(),
                &mut |bytes| {
                    metadata.extend_from_slice(bytes);
                    Ok(())
                },
            )
            .unwrap();
        (metadata, Some(rdma))
    })
}

fn two_managers(
    label: &str,
    transport: PeerTransport,
) -> (Manager, Manager, PathBuf, u16, u16, Tls) {
    let base = base(label);
    std::fs::create_dir_all(&base).unwrap();
    let peer = port();
    let data = port();
    let storage_metrics = port();
    let near_metrics = port();
    let name = instance_name(label);
    let tls = make_tls(&base);
    let b = Manager::start(storage(
        &base,
        &format!("{name}-storage"),
        peer,
        data,
        storage_metrics,
        &tls,
    ));
    wait_connect(peer);
    let a = Manager::start(near(
        &base,
        &format!("{name}-near"),
        peer,
        near_metrics,
        transport,
        true,
        &tls,
    ));
    (a, b, base, peer, data, tls)
}

#[test]
fn peer_l0_forwards_fetches_and_misses_after_loss() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let (mut near, mut storage, base, peer, _data, tls) =
        two_managers("peer", PeerTransport::Stream);
    let record_key = key(128);
    let expected = {
        let runtime = runtime();
        runtime.block_on(async {
            let mut client = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            client.store(record_key, None).await
        })
    };

    // The storage manager's catalog gains the forwarded record.
    wait_slow(|| {
        runtime().block_on(async {
            let mut client = Client::connect(&local(&storage.socket), descriptor())
                .await
                .unwrap();
            let subscription = client.subscribe().await;
            subscription.entries.contains_key(&record_key)
        })
    });
    // The near counts the forward when its owner applies the reply; wait for it.
    wait_slow(|| near.metric("forwarded_total") == 1.0);

    // Remote fetch: the bytes and metadata come back over the peer channel.
    let (metadata, _) = remote_fetch(peer, record_key, "stream", &tls);
    let header = record::Header::decode(&metadata).unwrap();
    assert_eq!(header.key, record_key);
    assert_eq!(header.tail_pages, (record_key.consumed_tokens / 64) as u32);
    assert_eq!(metadata.len() as u64, header.page_segment);
    assert_eq!(header.length_with_row(), expected.len() as u64);

    // Losing the storage manager makes later forwards miss.
    storage.stop();
    let runtime = runtime();
    runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(key(192), None).await;
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    while near.metric("forward_retries_total") == 0.0 {
        assert!(
            Instant::now() < deadline,
            "forward failure was not reported"
        );
        thread::sleep(Duration::from_millis(50));
    }
    near.stop();
    let _ = std::fs::remove_dir_all(base);
}

#[cfg(feature = "rdma")]
#[test]
fn peer_l0_rdma_moves_a_forwarded_record() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let (mut near, mut storage, base, peer, _data, tls) =
        two_managers("peer-rdma", PeerTransport::Auto);
    let record_key = key(128);
    runtime().block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(record_key, None).await;
    });
    wait_slow(|| {
        runtime().block_on(async {
            let mut client = Client::connect(&local(&storage.socket), descriptor())
                .await
                .unwrap();
            let subscription = client.subscribe().await;
            subscription.entries.contains_key(&record_key)
        })
    });
    let (metadata, rdma) = remote_fetch(peer, record_key, "rdma", &tls);
    let header = record::Header::decode(&metadata).unwrap();
    assert_eq!(header.key, record_key);
    assert_eq!(
        rdma,
        Some(true),
        "the storage manager did not accept an RDMA session"
    );
    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

const CHILD_BASE: &str = "PEER_L0_STORAGE_BASE";
const CHILD_NAME: &str = "PEER_L0_STORAGE_NAME";
const CHILD_PEER: &str = "PEER_L0_STORAGE_PEER";
const CHILD_DATA: &str = "PEER_L0_STORAGE_DATA";
const CHILD_METRICS: &str = "PEER_L0_STORAGE_METRICS";

/// Storage manager child process for the SIGKILL test. Does nothing unless the parent set the
/// environment.
#[test]
fn peer_l0_child_storage() {
    let Ok(base) = std::env::var(CHILD_BASE) else {
        return;
    };
    let number = |name: &str| -> u16 { std::env::var(name).unwrap().parse().unwrap() };
    let base = Path::new(&base);
    let config = storage(
        base,
        &std::env::var(CHILD_NAME).unwrap(),
        number(CHILD_PEER),
        number(CHILD_DATA),
        number(CHILD_METRICS),
        &tls_paths(base),
    );
    let store = control::store_config(&config).unwrap();
    let _ = control::run_peer(config, store, Arc::new(AtomicBool::new(false)));
}

fn spawn_storage(base: &Path, name: &str, peer: u16, data: u16, metrics: u16) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "peer_l0_child_storage", "--nocapture"])
        .env(CHILD_BASE, base)
        .env(CHILD_NAME, name)
        .env(CHILD_PEER, peer.to_string())
        .env(CHILD_DATA, data.to_string())
        .env(CHILD_METRICS, metrics.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn child_socket(base: &Path, name: &str) -> PathBuf {
    base.join(format!("{name}.sock"))
}

/// Kill the storage manager with SIGKILL, watch the near tier miss, restart the storage manager
/// and watch the queued forward and the remote fetch recover.
#[test]
fn peer_l0_sigkill_misses_then_recovers() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = base("peer-kill");
    std::fs::create_dir_all(&base).unwrap();
    let tls = make_tls(&base);
    let peer = port();
    let data = port();
    let storage_metrics = port();
    let near_metrics = port();
    let name = instance_name("peer-kill-storage");
    let socket = child_socket(&base, &name);
    let mut child = spawn_storage(&base, &name, peer, data, storage_metrics);
    wait_slow(|| socket.exists());
    wait_connect(peer);

    let mut near = Manager::start(near(
        &base,
        &format!("{name}-near"),
        peer,
        near_metrics,
        PeerTransport::Stream,
        true,
        &tls,
    ));
    let runtime = runtime();
    let first = key(128);
    runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(first, None).await;
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&first)
        })
    });

    child.kill().unwrap();
    child.wait().unwrap();

    // The near tier keeps serving locally; forwarding reports a miss and retries.
    let second = key(192);
    runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(second, None).await;
    });
    let deadline = Instant::now() + Duration::from_secs(15);
    while near.metric("forward_retries_total") == 0.0 {
        assert!(Instant::now() < deadline, "peer loss was not observed");
        thread::sleep(Duration::from_millis(20));
    }

    // Restarting the storage manager lets the retry and the remote fetch succeed again.
    let mut restarted = spawn_storage(&base, &name, peer, data, storage_metrics);
    wait_slow(|| socket.exists());
    wait_connect(peer);
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&second)
        })
    });
    let (metadata, _) = remote_fetch(peer, second, "stream", &tls);
    let header = record::Header::decode(&metadata).unwrap();
    assert_eq!(header.key, second);

    restarted.kill().unwrap();
    restarted.wait().unwrap();
    near.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// A read-only near manager loses its upstream subscription when the storage manager dies and
/// recovers it after the restart. The near stores nothing, so it never forwards: the new record
/// can reach its catalog only through a subscription that was retried until the peer returned.
#[test]
fn peer_l0_read_only_near_recovers_its_catalog() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = base("peer-recover");
    std::fs::create_dir_all(&base).unwrap();
    let tls = make_tls(&base);
    let peer = port();
    let data = port();
    let name = instance_name("peer-recover-storage");
    let mut storage_node = Manager::start(storage(&base, &name, peer, data, port(), &tls));
    wait_connect(peer);
    let mut near = Manager::start(near(
        &base,
        &format!("{name}-near"),
        peer,
        port(),
        PeerTransport::Stream,
        true,
        &tls,
    ));
    let runtime = runtime();
    let local_near = local(&near.socket);
    let local_storage = local(&storage_node.socket);

    // The near's own catalog session registers the namespace and starts the upstream
    // subscription; the near stores no records.
    runtime.block_on(async {
        let mut client = Client::connect(&local_near, descriptor()).await.unwrap();
        client.subscribe().await;
    });
    let first = key(128);
    runtime.block_on(async {
        let mut client = Client::connect(&local_storage, descriptor()).await.unwrap();
        client.store(first, None).await;
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local_near, descriptor()).await.unwrap();
            client.subscribe().await.entries.contains_key(&first)
        })
    });

    // Losing the storage manager ends the subscription; the near drops the dead connection and
    // the store forgets the remote-only record.
    storage_node.stop();
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local_near, descriptor()).await.unwrap();
            !client.subscribe().await.entries.contains_key(&first)
        })
    });

    // After the restart the subscription comes back on its own, with no forward to trigger it.
    let mut restarted = Manager::start(storage(&base, &name, peer, data, port(), &tls));
    wait_connect(peer);
    let second = key(192);
    runtime.block_on(async {
        let mut client = Client::connect(&local_storage, descriptor()).await.unwrap();
        client.store(second, None).await;
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local_near, descriptor()).await.unwrap();
            client.subscribe().await.entries.contains_key(&second)
        })
    });
    assert_eq!(near.metric("forwarded_total"), 0.0);

    restarted.stop();
    near.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// A record that exists only on disk forwards through a bounded staging window: the near's
/// arena is smaller than the record (so it streams to NVMe) and the record is larger than the
/// near's whole staging region, which staging it whole would need.
#[test]
fn peer_l0_forwards_a_disk_record_larger_than_staging() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = base("peer-disk");
    std::fs::create_dir_all(&base).unwrap();
    let tls = make_tls(&base);
    let peer = port();
    let data = port();
    let name = instance_name("peer-disk-storage");
    let mut storage_config = storage(&base, &name, peer, data, port(), &tls);
    storage_config.memory = 32 << 20;
    let mut storage = Manager::start(storage_config);
    wait_connect(peer);
    let mut near_config = near(
        &base,
        &format!("{name}-near"),
        peer,
        port(),
        PeerTransport::Stream,
        true,
        &tls,
    );
    near_config.memory = 8 << 20;
    near_config.staging = 8 << 20;
    near_config.root = Some(base.join("near-root"));
    near_config.nvme = 64 << 20;
    let mut near = Manager::start(near_config);
    let runtime = runtime();

    // 3072 pages of 4096 bytes: 12.6 MiB, past both the arena and the staging region.
    let large = key(196_608);
    runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(large, None).await;
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&storage.socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&large)
        })
    });
    // The storage lists the record as soon as it stores it; the near counts the forward when
    // its owner thread applies the reply, so wait for the counter rather than race it.
    wait_slow(|| near.metric("forwarded_total") == 1.0);

    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// A remote chain that cannot fit the near's arena is reported as a miss, not as a failed Get.
#[test]
fn peer_l0_remote_chain_larger_than_the_arena_is_a_miss() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = base("peer-miss");
    std::fs::create_dir_all(&base).unwrap();
    let tls = make_tls(&base);
    let peer = port();
    let data = port();
    let name = instance_name("peer-miss-storage");
    let mut storage_config = storage(&base, &name, peer, data, port(), &tls);
    storage_config.memory = 32 << 20;
    let mut storage = Manager::start(storage_config);
    wait_connect(peer);
    let mut near_config = near(
        &base,
        &format!("{name}-near"),
        peer,
        port(),
        PeerTransport::Stream,
        true,
        &tls,
    );
    near_config.memory = 8 << 20;
    let mut near = Manager::start(near_config);
    let runtime = runtime();
    let large = key(196_608);
    runtime.block_on(async {
        let mut client = Client::connect(&local(&storage.socket), descriptor())
            .await
            .unwrap();
        client.store(large, None).await;
    });
    // Wait until the near's merged catalog advertises the remote record, so the Get plans a
    // fetch rather than missing for want of a catalog entry.
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&large)
        })
    });
    let outcome = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client
            .get(
                &[large],
                0,
                (large.consumed_tokens / 64) as u32,
                GetOptions::default(),
            )
            .await
    });
    assert!(
        matches!(outcome, GetOutcome::Refused(reason) if reason == proto::Reason::Invalid as u8),
        "unexpected outcome: {outcome:?}"
    );

    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// A record that only exists on the storage manager is served to a local replica through a
/// remote fetch into local DRAM.
#[test]
fn peer_l0_serves_a_remote_record_locally() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let (mut near, mut storage, base, _peer, _data, _tls) =
        two_managers("peer-remote", PeerTransport::Stream);
    let runtime = runtime();
    let key = key(128);
    let expected = runtime.block_on(async {
        let mut client = Client::connect(&local(&storage.socket), descriptor())
            .await
            .unwrap();
        client.store(key, None).await
    });
    // Opening a local session on the near manager registers its namespace, which starts the
    // upstream subscription; the snapshot then advertises the remote record.
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&key)
        })
    });
    let fetched = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.fetch(&[key], 0, 2).await
    });
    assert_eq!(fetched[0], expected);
    // The second fetch is served from the local copy adopted by the first.
    let again = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.fetch(&[key], 0, 2).await
    });
    assert_eq!(again[0], expected);
    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// Two tenants share one upstream connection: both catalogs arrive and both chains fetch.
#[test]
fn peer_l0_carries_multiple_namespaces_on_one_upstream() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let (mut near, mut storage, base, _peer, _data, _tls) =
        two_managers("peer-ns", PeerTransport::Stream);
    let runtime = runtime();
    let mut other = descriptor();
    other.tenant = "tenant-b".to_string();
    // The same Key in two tenants: a Key-only identity would conflate their forwarding.
    let first = key(128);
    let second = first;
    let bytes_a = runtime.block_on(async {
        let mut client = Client::connect(&local(&storage.socket), descriptor())
            .await
            .unwrap();
        client.store(first, None).await
    });
    let bytes_b = runtime.block_on(async {
        let mut client = Client::connect(&local(&storage.socket), other.clone())
            .await
            .unwrap();
        client.store(second, None).await
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut a = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            let mut b = Client::connect(&local(&near.socket), other.clone())
                .await
                .unwrap();
            a.subscribe().await.entries.contains_key(&first)
                && b.subscribe().await.entries.contains_key(&second)
        })
    });
    let fetched_a = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.fetch(&[first], 0, 2).await
    });
    let fetched_b = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), other.clone())
            .await
            .unwrap();
        client.fetch(&[second], 0, 2).await
    });
    assert_eq!(fetched_a[0], bytes_a);
    assert_eq!(fetched_b[0], bytes_b);
    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// The near tier starts before its storage peer and keeps retrying until it appears.
#[test]
fn peer_l0_near_starts_before_storage() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = base("peer-order");
    std::fs::create_dir_all(&base).unwrap();
    let tls = make_tls(&base);
    let peer = port();
    let data = port();
    let storage_metrics = port();
    let near_metrics = port();
    let name = instance_name("peer-order");
    let mut near = Manager::start(near(
        &base,
        &format!("{name}-near"),
        peer,
        near_metrics,
        PeerTransport::Stream,
        true,
        &tls,
    ));
    thread::sleep(Duration::from_millis(500));
    let mut storage = Manager::start(storage(
        &base,
        &format!("{name}-storage"),
        peer,
        data,
        storage_metrics,
        &tls,
    ));
    wait_connect(peer);
    let runtime = runtime();
    let key = key(128);
    runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(key, None).await;
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&storage.socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&key)
        })
    });
    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// A fetched record is released after its Get when retention is off.
#[test]
fn peer_l0_releases_a_fetched_record_when_not_retained() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    let base = base("peer-noretain");
    std::fs::create_dir_all(&base).unwrap();
    let tls = make_tls(&base);
    let peer = port();
    let data = port();
    let storage_metrics = port();
    let near_metrics = port();
    let name = instance_name("peer-noretain");
    let mut storage = Manager::start(storage(
        &base,
        &format!("{name}-storage"),
        peer,
        data,
        storage_metrics,
        &tls,
    ));
    wait_connect(peer);
    let mut near = Manager::start(near(
        &base,
        &format!("{name}-near"),
        peer,
        near_metrics,
        PeerTransport::Stream,
        false,
        &tls,
    ));
    let runtime = runtime();
    let key = key(128);
    let expected = runtime.block_on(async {
        let mut client = Client::connect(&local(&storage.socket), descriptor())
            .await
            .unwrap();
        client.store(key, None).await
    });
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            client.subscribe().await.entries.contains_key(&key)
        })
    });
    let fetched = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.fetch(&[key], 0, 2).await
    });
    assert_eq!(fetched[0], expected);
    // The adopted copy is gone; the record is remote again.
    wait_slow(|| {
        runtime.block_on(async {
            let mut client = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            client
                .subscribe()
                .await
                .entries
                .get(&key)
                .is_some_and(|entry| entry.tier == proto::Tier::Remote as i32)
        })
    });
    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}

/// A local chain prefix is not fetched again: only the missing suffix moves.
#[test]
fn peer_l0_fetches_only_the_missing_chain_suffix() {
    let (mut near, mut storage, base, _peer, _data, _tls) =
        two_managers("peer-suffix", PeerTransport::Stream);
    let runtime = runtime();
    let root = key(128);
    let child = key(192);
    let child_bytes = runtime.block_on(async {
        let mut client = Client::connect(&local(&storage.socket), descriptor())
            .await
            .unwrap();
        client.store(root, None).await;
        client.store(child, Some(root)).await
    });
    runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        client.store(root, None).await;
    });
    // Wait until the near catalog lists the child upstream and the root locally; otherwise the
    // plan may legitimately fetch both records.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (root_tier, has_child) = runtime.block_on(async {
            let mut client = Client::connect(&local(&near.socket), descriptor())
                .await
                .unwrap();
            let subscription = client.subscribe().await;
            (
                subscription.entries.get(&root).map(|entry| entry.tier),
                subscription.entries.contains_key(&child),
            )
        });
        if root_tier == Some(proto::Tier::Dram as i32) && has_child {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "wait: root tier {root_tier:?}, child {has_child}"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let fetched = runtime.block_on(async {
        let mut client = Client::connect(&local(&near.socket), descriptor())
            .await
            .unwrap();
        match client
            .get(&[root, child], 2, 1, GetOptions::default())
            .await
        {
            GetOutcome::Done(records) => records,
            other => panic!(
                "suffix get failed: {other:?}; fetched={}",
                near.metric("remote_fetched_total")
            ),
        }
    });
    assert_eq!(fetched[1], child_bytes);
    assert_eq!(
        near.metric("remote_fetched_total"),
        1.0,
        "only the missing record should be fetched"
    );
    near.stop();
    storage.stop();
    let _ = std::fs::remove_dir_all(base);
}
