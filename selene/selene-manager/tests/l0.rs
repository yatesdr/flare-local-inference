mod support;
use selene_manager::{
    arena::{Arena, ExtentSlice},
    control, disk, proto,
    record::{self, Header},
    store::Config,
    tree::{Eviction, Tree},
};
use std::{
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;

const CHILD: &str = "SELENE_CHILD_BASE";
const CHILD_POINT: &str = "SELENE_CHILD_POINT";
const CHILD_DUMP: &str = "SELENE_CHILD_DUMP";
const CHILD_QUARANTINE: &str = "SELENE_CHILD_QUARANTINE";

#[test]
fn l0_01_tree_insert_chain_remove_parity() {
    for pt in [64, 128, 256] {
        let mut tree = Tree::new(pt);
        let keys = [key(700), key(1300), key(1990)];
        for (index, key) in keys.iter().enumerate() {
            let parent = index.checked_sub(1).map(|index| keys[index]);
            let pages =
                key.consumed_tokens / pt - parent.map_or(0, |parent| parent.consumed_tokens / pt);
            tree.insert(*key, parent, [4; 16], pages as usize, true, ())
                .unwrap();
            tree.access_at(key, index as u64 + 1);
        }
        let parent = tree.node(&keys[2]).unwrap().parent.unwrap();
        assert_eq!(parent, keys[1]);
        assert_eq!(tree.node(&parent).unwrap().parent, Some(keys[0]));
        let available =
            tree.node(&parent).unwrap().tail_pages + tree.node(&keys[2]).unwrap().tail_pages;
        let skip = (1000 / pt - 700 / pt) as usize;
        assert_eq!(available - skip, (1990 / pt - 1000 / pt) as usize);
        assert!(
            tree.insert(key(2000), Some(key(1999)), [0; 16], 0, true, ())
                .is_err()
        );
        assert!(
            tree.insert(key(2000), Some(keys[2]), [0; 16], 20, true, ())
                .is_err()
        );
        assert_eq!(tree.node(&keys[2]).unwrap().children(), 0);
        assert!(tree.remove_node(&keys[0]).is_err());
        tree.pin(&keys[2]).unwrap();
        assert!(tree.remove_node(&keys[2]).is_err());
        tree.unpin(&keys[2]).unwrap();
        assert_eq!(tree.remove_node(&keys[2]).unwrap().key, keys[2]);
        assert_eq!(tree.node(&keys[1]).unwrap().children(), 0);
    }
    let fixture = include_bytes!("fixtures/flare-header-v1.bin");
    let decoded = Header::decode(fixture).unwrap();
    let mut planned = Header::new(&descriptor(), key(130), Some(key(65)), [4; 16], 1).unwrap();
    planned.created = 1_700_000_000;
    planned.row_checksum = 0x0123456789abcdef;
    assert_eq!(decoded, planned);
    assert_eq!(&encode_header(&planned), fixture);
    for (offset, value) in [(129, 0u64), (173, 0), (181, u64::MAX)] {
        let mut invalid = *fixture;
        invalid[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        assert!(Header::decode(&invalid).is_err());
    }
    let arena = Arena::new(1024 * 1024).unwrap();
    let extent = Arc::new(arena.allocate(9000).unwrap());
    assert_eq!(extent.bytes().as_ptr() as usize % 4096, 0);
    let reader = extent.clone();
    drop(extent);
    assert_eq!(arena.used(), 16384);
    drop(reader);
    assert_eq!(arena.used(), 0);
    assert!(arena.allocate(1024 * 1024).is_some());
}

#[test]
fn l0_02_leaf_lru_compaction_and_cascade() {
    let mut tree = Tree::new(64);
    for (tokens, parent) in [(200, None), (400, Some(200)), (600, Some(400))] {
        tree.insert(
            key(tokens),
            parent.map(key),
            [0; 16],
            (tokens / 64 - parent.unwrap_or(0) / 64) as usize,
            true,
            (),
        )
        .unwrap();
        tree.access_at(&key(tokens), tokens);
    }
    tree.access_at(&key(200), 1000);
    tree.pin(&key(600)).unwrap();
    assert!(tree.evict(false, |_| true).is_none());
    assert!(matches!(tree.evict(true, |_| true), Some(Eviction::Compacted(k)) if k == key(400)));
    assert!(!tree.node(&key(400)).unwrap().row);
    assert!(tree.node(&key(600)).unwrap().row);
    tree.unpin(&key(600)).unwrap();
    let Some(Eviction::Leaves(removed)) = tree.evict(false, |_| true) else {
        panic!("leaves")
    };
    assert_eq!(
        removed.iter().map(|node| node.key).collect::<Vec<_>>(),
        [key(600), key(400)]
    );
    assert_eq!(tree.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_03_accept_shapes_and_present_inflight() {
    let mut host = Host::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    host.start();
    let mut a = host.client().await;
    let mut b = host.client().await;
    // A child shaped against a readable parent gets first_page = parent pages.
    let (sender, replies) = a.raw_begin(key(128), None).await;
    // A second connection sees INFLIGHT while the first Put is open.
    assert_eq!(
        b.put(key(128), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Inflight as u8)
    );
    sender
        .send(proto::PutMessage {
            m: Some(proto::put_message::M::Cancel(proto::Cancel {})),
        })
        .await
        .unwrap();
    drop(sender);
    drop(replies);
    host.wait(|host| host.gauge("open_ops") == 0);
    let bytes = b.store(key(128), None).await;
    assert!(!bytes.is_empty());
    assert_eq!(
        b.put(key(128), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Present as u8)
    );
    let mut c = host.client().await;
    let mut open = Vec::new();
    for tokens in [640, 768, 896, 1024] {
        open.push(c.raw_begin(key(tokens), None).await);
    }
    assert_eq!(
        c.put(key(1152), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Queue as u8)
    );
    drop(open);
    host.wait(|host| host.gauge("open_ops") == 0);
    b.store(key(256), Some(key(128))).await;
    b.store(key(512), None).await;
    let mut fresh = host.client().await;
    let mut leaves = Vec::new();
    for (index, tokens) in [3u64, 5].into_iter().enumerate() {
        let bytes = fresh.store(key(tokens), (index > 0).then(|| key(3))).await;
        assert!(!bytes.is_empty());
        leaves.push(tokens);
    }
    let data = fresh.fetch(&[key(3), key(5)], 0, 0).await;
    assert_eq!(data.len(), 2);
    assert!(!data[0].is_empty() && !data[1].is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_04_invalid_checksum_short_payload_and_offsets() {
    let mut host = Host::new(4 * 1024 * 1024, 4 * 1024 * 1024);
    host.start();
    for (options, tokens) in [
        (
            PutOptions {
                corrupt_page: Some(0),
                ..PutOptions::default()
            },
            128,
        ),
        (
            PutOptions {
                early_commit: true,
                ..PutOptions::default()
            },
            128 * 64,
        ),
        (
            PutOptions {
                corrupt_row: true,
                ..PutOptions::default()
            },
            256,
        ),
        (
            PutOptions {
                skip_filled: true,
                ..PutOptions::default()
            },
            192 * 64,
        ),
    ] {
        let mut client = host.client().await;
        let outcome = client.put(key(tokens), None, options).await;
        assert!(
            matches!(outcome, PutOutcome::Refused(reason) if reason == proto::Reason::Invalid as u8),
            "{tokens}: {outcome:?}"
        );
    }
    host.wait(|host| host.gauge("nvme_reserved_bytes") == 0);
    assert_eq!(host.gauge("staging_allocated_bytes"), 0);
    let mut check = host.client().await;
    let subscription = check.subscribe().await;
    assert!(subscription.entries.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_05_parent_first_chain_closed_durability() {
    let base = base("durable");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4 * 1024 * 1024,
        4 * 1024 * 1024,
    );
    config.name = instance_name("durable");
    config.faults.pause_at = Some("parent_write".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    wait(|| signal.exists());
    client.store(key(256), Some(key(128))).await;
    let namespace = client.descriptor.name();
    assert!(
        !host
            .config
            .root
            .as_ref()
            .unwrap()
            .join(&namespace)
            .join(record::file_name(&key(256)))
            .exists()
    );
    assert_eq!(host.gauge("nvme_bytes_total"), 0);
    fs::remove_file(&signal).unwrap();
    wait(|| signal.exists());
    assert!(
        host.config
            .root
            .as_ref()
            .unwrap()
            .join(&namespace)
            .join(record::file_name(&key(128)))
            .exists()
    );
    fs::remove_file(signal).unwrap();
    host.durable(2);
    let subscription = client.subscribe().await;
    assert!(
        subscription.entries.values().all(|entry| entry.durable),
        "both records must be chain-durable"
    );
}

#[test]
fn l0_06_multiprocess_kill_recovery() {
    if let Ok(base) = std::env::var(CHILD) {
        let base = PathBuf::from(base);
        let point = std::env::var(CHILD_POINT).unwrap_or_default();
        let memory = if point == "parent_write" {
            4 * 1024 * 1024
        } else {
            4096
        };
        let mut config = Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            memory,
            64 * 1024 * 1024,
        );
        config.name = instance_name("child");
        config.faults.pause_at = Some(point.clone());
        config.faults.signal = Some(base.join("gate"));
        if let Ok(dump) = std::env::var(CHILD_DUMP) {
            let store = selene_manager::store::Store::open(config).unwrap_or_else(|error| {
                panic!("the snapshot child could not open the store: {error}")
            });
            let snapshot = store
                .namespace_id()
                .and_then(|id| store.snapshot(id))
                .map(|(_, entries)| entries.len())
                .unwrap_or(0);
            fs::write(dump, snapshot.to_string()).unwrap();
            loop {
                std::thread::park();
            }
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut host = Host::open_config(config);
            host.start();
            let mut client = host.client().await;
            if point == "data" {
                let _ = client
                    .put(
                        key(128),
                        None,
                        PutOptions {
                            cancel: Some(CancelAt::Filled),
                            ..PutOptions::default()
                        },
                    )
                    .await;
            } else {
                client.store(key(128), None).await;
            }
            loop {
                std::thread::park();
            }
        });
    }
    for point in ["data", "before_rename", "after_rename", "parent_write"] {
        let base = base(point);
        let config = Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            4096,
            64 * 1024 * 1024,
        );
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_06_multiprocess_kill_recovery", "--nocapture"])
            .env(CHILD, &base)
            .env(CHILD_POINT, point)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        wait(|| base.join("gate").exists());
        child.kill().unwrap();
        let status = child.wait().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let lock = disk::RootLock::acquire(&config.root.as_ref().unwrap().clone()).unwrap();
        let recovered = recovered_keys(config.root.as_ref().unwrap());
        assert_eq!(
            recovered,
            if point == "after_rename" {
                vec![key(128)]
            } else {
                vec![]
            },
            "{point}"
        );
        assert!(
            fs::read_dir(config.root.as_ref().unwrap().join(descriptor().name()))
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
        );
        drop(lock);
        let _ = fs::remove_file(base.join("gate"));
        let dump = base.join("dump");
        let mut restarted = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "l0_06_multiprocess_kill_recovery", "--nocapture"])
            .env(CHILD, &base)
            .env(CHILD_DUMP, &dump)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        // `exists` can be true before the child has written the snapshot, so wait for a
        // value the file actually parses to.
        let mut snapshot = None;
        wait(|| {
            snapshot = fs::read_to_string(&dump)
                .ok()
                .and_then(|text| text.parse::<usize>().ok());
            snapshot.is_some()
        });
        assert_eq!(snapshot.unwrap(), recovered.len());
        restarted.kill().unwrap();
        restarted.wait().unwrap();
        fs::remove_dir_all(base).unwrap();
    }
}

/// A startup failure after the store opened must release the instance and root locks and
/// leave no socket, so the same manager can start again on the same paths immediately.
#[test]
fn l0_67_a_failed_start_releases_the_instance_and_root() {
    let base = base("failed-start");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        65536,
        64 * 1024 * 1024,
    );
    config.name = instance_name("failed-start");
    let squatter = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut failed = config.clone();
    failed.metrics = Some(squatter.local_addr().unwrap().to_string());
    let error = control::run(failed, Arc::new(AtomicBool::new(false)))
        .expect_err("a taken metrics port must fail the start");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::AddrInUse,
        "the metrics refusal is reported: {error}"
    );
    assert!(!config.socket.exists(), "a failed start leaves no socket");
    drop(squatter);
    // The instance, lock and root are free again: the same manager starts at once.
    config.metrics = None;
    let mut host = Host::open_config(config);
    host.try_start()
        .expect("the previous attempt released its locks");
    host.stop();
    let _ = fs::remove_dir_all(base);
}

/// Certificate errors are found before the manager opens its store or any listener, so a
/// failed start leaves no locks, socket or accepting data port behind.
#[test]
fn l0_68_a_tls_error_starts_nothing() {
    let base = base("tls-error");
    fs::create_dir_all(&base).unwrap();
    let socket = base.join("run/m.sock");
    let mut local = selene_manager::config::Config::defaults();
    local.name = instance_name("tls-error");
    local.socket = socket.clone();
    local.socket_explicit = true;
    local.root = Some(base.join("records"));
    local.memory = 65536;
    local.staging = 8 << 20;
    local.nvme = 64 << 20;
    local.tls_cert = Some(base.join("missing-cert.pem"));
    local.tls_key = Some(base.join("missing-key.pem"));
    local.tls_ca = Some(base.join("missing-ca.pem"));
    local.peer_transport = selene_manager::config::PeerTransport::Stream;
    let mut store = Config::new(base.join("records"), socket.clone(), 65536, 64 << 20);
    store.name = local.name.clone();
    let error = control::run_peer(local, store, Arc::new(AtomicBool::new(false)))
        .expect_err("a missing certificate must fail the start");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::NotFound,
        "the certificate error is reported: {error}"
    );
    assert!(!socket.exists(), "nothing was created");
    // A key that does not belong to the certificate is only caught by building the full
    // server configuration, which now happens before anything starts.
    let good = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let other = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert = base.join("cert.pem");
    let key = base.join("key.pem");
    let ca = base.join("ca.pem");
    fs::write(&cert, good.cert.pem()).unwrap();
    fs::write(&key, other.signing_key.serialize_pem()).unwrap();
    fs::write(&ca, good.cert.pem()).unwrap();
    let mut local = selene_manager::config::Config::defaults();
    local.name = instance_name("tls-mismatch");
    local.socket = socket.clone();
    local.socket_explicit = true;
    local.root = Some(base.join("records"));
    local.memory = 65536;
    local.staging = 8 << 20;
    local.nvme = 64 << 20;
    local.tls_cert = Some(cert);
    local.tls_key = Some(key);
    local.tls_ca = Some(ca);
    local.peer_transport = selene_manager::config::PeerTransport::Stream;
    let mut store = Config::new(base.join("records"), socket.clone(), 65536, 64 << 20);
    store.name = local.name.clone();
    let error = control::run_peer(local, store, Arc::new(AtomicBool::new(false)))
        .expect_err("a key that does not match the certificate must fail the start");
    assert!(
        error.to_string().contains("TLS"),
        "the certificate error is named: {error}"
    );
    assert!(!socket.exists(), "nothing was created");
    let _ = fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_07_streaming_and_nvme_oversize() {
    let mut host = Host::new(65536, 2 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let bytes = client.store(key(128 * 64), None).await;
    host.durable(1);
    assert_eq!(host.gauge("memory_allocated_bytes"), 0);
    let data = client.fetch(&[key(128 * 64)], 0, 128).await;
    assert_eq!(data[0], bytes);
    assert_eq!(
        client
            .put(key(1024 * 64), None, PutOptions::default())
            .await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
    host.stop();
    host.start();
    let mut client = host.client().await;
    let subscription = client.subscribe().await;
    assert_eq!(subscription.entries.len(), 1);
    assert_eq!(
        client.fetch(&[key(128 * 64)], 64, 64).await[0].len(),
        bytes.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_08_refill_and_split_units() {
    let mut refill = Host::new(65536, 1024 * 1024);
    refill.start();
    let mut client = refill.client().await;
    client.store(key(128), None).await;
    refill.durable(1);
    refill.stop();
    refill.start();
    let mut client = refill.client().await;
    let subscription = client.subscribe().await;
    assert_eq!(
        subscription.entries[&key(128)].tier,
        proto::Tier::Nvme as i32
    );
    client.fetch(&[key(128)], 0, 2).await;
    refill.wait(|host| host.gauge("memory_allocated_bytes") > 0);
    let subscription = client.subscribe().await;
    assert_eq!(
        subscription.entries[&key(128)].tier,
        proto::Tier::Dram as i32
    );

    let mut split = Host::new(4096, 32 * 1024 * 1024);
    split.start();
    let mut geometry = descriptor();
    geometry.ranks[0].planes = vec![4 * 1024 * 1024 + 4096];
    let mut client = split.client_with(geometry).await.unwrap();
    let expected = client.store(key(128), None).await;
    let actual = client.fetch(&[key(128)], 0, 2).await;
    assert_eq!(actual[0], expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_09_disk_failure_is_refused_io() {
    let base = base("full");
    fs::create_dir_all(&base).unwrap();
    let full = base.join("full");
    fs::write(&full, b"full").unwrap();
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("full");
    config.faults.disk_full = Some(full.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    // The record does not fit the tiny arena, so it streams through staging.
    assert_eq!(
        client.put(key(512 * 64), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Io as u8)
    );
    host.wait(|host| host.gauge("nvme_errors_total") > 0);
    // A single-window stream must also get its IO refusal: the failing chunk is
    // the last pending user, so removing the operation first would lose it.
    let mut single = host.client().await;
    assert_eq!(
        single.put(key(64 * 64), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Io as u8)
    );
    fs::remove_file(full).unwrap();
    let bytes = client.store(key(128), None).await;
    assert!(!bytes.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_10_cancel_drains_and_staging_gauge() {
    let base = base("cancel");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        4 * 1024 * 1024,
    );
    config.name = instance_name("cancel");
    config.faults.delay_ms = 300;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    // Streaming Put: two pages plus the row, one window of 12288 bytes.
    assert_eq!(
        client
            .put(
                key(128),
                None,
                PutOptions {
                    cancel: Some(CancelAt::Filled),
                    ..PutOptions::default()
                },
            )
            .await,
        PutOutcome::Cancelled
    );
    // The fixed hold for the two-page record (2 x a 16 KiB window) lives
    // until its delayed disk write completes, then drains.
    host.wait(|host| host.gauge("staging_allocated_bytes") > 0);
    assert_eq!(host.gauge("staging_allocated_bytes"), 32768);
    host.wait(|host| host.gauge("staging_allocated_bytes") == 0);
    host.wait(|host| host.gauge("nvme_reserved_bytes") == 0);
    // A Get is not blocked by a draining Put.
    let stored = client.store(key(128 * 64), None).await;
    let mut getter = host.client().await;
    let start = Instant::now();
    let data = getter.fetch(&[key(128 * 64)], 0, 128).await;
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(data[0], stored);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_12_round_robin_and_dead_client_cleanup() {
    let mut host = Host::new(8 * 1024 * 1024, 64 * 1024 * 1024);
    host.start();
    let mut a = host.client().await;
    let mut client = host.client().await;
    let mut previous = None;
    for index in 1..=160u64 {
        let value = key(index * 64);
        client.store(value, previous).await;
        previous = Some(value);
    }
    host.durable(160);
    let keys: Vec<_> = (1..=160).map(|index| key(index * 64)).collect();
    let mut dead = host.client().await;
    for _ in 0..4 {
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let _replies = dead
            .inner
            .get(dead.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
            .await
            .unwrap();
        sender
            .send(proto::GetMessage {
                m: Some(proto::get_message::M::Begin(proto::GetBegin {
                    namespace_id: dead.descriptor.namespace_id().to_vec(),
                    chain: keys.iter().map(|key| key_msg(*key)).collect(),
                    skip_pages: 0,
                    expected_pages: 160,
                    row: true,
                    destinations: Vec::new(),
                    transfer: 0,
                })),
            })
            .await
            .unwrap();
        drop(sender);
    }
    drop(dead);
    let start = Instant::now();
    a.fetch(&[key(64)], 0, 1).await;
    assert!(start.elapsed() < Duration::from_millis(500));
    host.wait(|host| host.gauge("leases") == 0 && host.gauge("cleanup_records") == 0);
    // Unknown transfers are dropped without disturbing a live connection.
    let mut unknown = host.client().await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = unknown
        .inner
        .get(unknown.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::GetMessage {
            m: Some(proto::get_message::M::Begin(proto::GetBegin {
                namespace_id: unknown.descriptor.namespace_id().to_vec(),
                chain: vec![key_msg(key(64))],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                destinations: Vec::new(),
                transfer: 0,
            })),
        })
        .await
        .unwrap();
    let _leased = loop {
        let reply = replies.message().await.unwrap().unwrap();
        if matches!(reply.r, Some(proto::get_reply::R::Leased(_))) {
            break reply;
        }
    };
    sender
        .send(proto::GetMessage {
            m: Some(proto::get_message::M::Released(proto::WindowReleased {
                transfer: 999_999,
                index: 0,
            })),
        })
        .await
        .unwrap();
    let refused = loop {
        let reply = replies.message().await.unwrap().unwrap();
        if matches!(reply.r, Some(proto::get_reply::R::Refused(_))) {
            break reply;
        }
    };
    assert!(matches!(refused.r, Some(proto::get_reply::R::Refused(_))));
    let start = Instant::now();
    a.fetch(&[key(64)], 0, 1).await;
    assert!(start.elapsed() < Duration::from_millis(500));
}

#[test]
fn l0_14_throughput() {
    support::throughput::smoke();
}

#[test]
fn l0_15_namespace_id_vectors() {
    let id = record::namespace_id(&[0x63; 32], &[0x1b; 16], "test");
    assert_eq!(
        record::hex(&id),
        "83bbb4befe798c5d717456d55695acc1d166d45c214e54e253f249cd025d4040"
    );
    let id = record::namespace_id(&[0x63; 32], &[0x1b; 16], "");
    assert_eq!(
        record::hex(&id),
        "6fbf3a101213ca13f48c6ab6ae91e7e1e35fd409de023b30696e32b18c97ed74"
    );
}

#[test]
fn l0_16_disk_queue_read_write_interleave() {
    let base = base("disk");
    fs::create_dir_all(&base).unwrap();
    let lock = Arc::new(disk::RootLock::acquire(&base.join("root")).unwrap());
    let trace = Arc::new(std::sync::Mutex::new(Vec::new()));
    let faults = disk::Faults {
        delay_ms: 10,
        trace: Some(trace.clone()),
        ..disk::Faults::default()
    };
    let worker = disk::Worker::start(faults, lock);
    let source = base.join("source");
    let mut bytes = vec![0u8; 4096];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = index as u8;
    }
    fs::write(&source, &bytes).unwrap();
    let file = Arc::new(disk::open_read(&source).unwrap());
    let arena = Arena::new(8 * 4096).unwrap();
    let reader = worker.reader();
    let write_arena = Arena::new(4 * 4096).unwrap();
    for index in 0..4 {
        let mut extent = write_arena.allocate(4096).unwrap();
        extent.bytes_mut().fill(index as u8);
        let temporary = Arc::new(disk::DiskExtent::new(base.join(format!("w-{index}.tmp"))));
        temporary.retire();
        worker
            .sender
            .send(disk::Task::Chunk {
                id: index,
                temporary,
                offset: 0,
                data: ExtentSlice::new(Arc::new(extent), 0, 4096),
            })
            .unwrap();
    }
    let mut readers = Vec::new();
    for _ in 0..8 {
        let reader = reader.clone();
        let file = file.clone();
        let extent = arena.allocate(4096).unwrap();
        readers.push(std::thread::spawn(move || {
            reader.read(&file, 0, extent).unwrap();
        }));
    }
    for _ in 0..4 {
        worker.receiver.recv().unwrap();
    }
    for reader in readers {
        reader.join().unwrap();
    }
    // The worker records its own service order, so the observation cannot be
    // reordered by reader or completion-receiver scheduling.
    let trace = trace.lock().unwrap();
    assert_eq!(trace.iter().filter(|kind| **kind == b'R').count(), 8);
    assert_eq!(trace.iter().filter(|kind| **kind == b'W').count(), 4);
    let mut run = 0;
    let mut saw_write = false;
    for &kind in trace.iter() {
        if kind == b'R' {
            run += 1;
            assert!(
                !saw_write || run <= 4,
                "more than four reads between writes"
            );
        } else {
            saw_write = true;
            run = 0;
        }
    }
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_17_write_budget_and_room() {
    let base = base("budget");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        64 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = instance_name("budget");
    config.write_budget = 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut a = host.client().await;
    let mut subscription = a.subscribe().await;
    a.store(key(128), None).await;
    a.store(key(2048 * 64), None).await;
    // Gets are never throttled by the write budget.
    a.fetch(&[key(128)], 0, 2).await;
    assert_eq!(
        a.put(key(256), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    let room = subscription.until_room().await;
    assert!(room > 0);
    let _ = a.store(key(256), None).await;
    // Buckets are independent per connection.
    let mut b = host.client().await;
    b.store(key(384), None).await;
    // 0 means unlimited.
    let mut unlimited = Host::new(64 * 1024 * 1024, 64 * 1024 * 1024);
    unlimited.start();
    let mut c = unlimited.client().await;
    c.store(key(2048 * 64), None).await;
    c.store(key(2048 * 64 + 64), Some(key(2048 * 64))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_18_descriptor_format_and_recovery_refusal() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let directory = host
        .config
        .root
        .as_ref()
        .unwrap()
        .join(client.descriptor.name());
    let bytes = fs::read(directory.join("descriptor")).unwrap();
    assert_eq!(&bytes[..8], b"FLCDSC2\0");
    let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    assert_eq!(bytes.len(), 16 + length);
    assert_eq!(
        disk::crc32(&bytes[..12 + length]),
        u32::from_le_bytes(bytes[12 + length..].try_into().unwrap())
    );
    host.stop();
    let root = host.config.root.clone().unwrap();
    let v3 = root.join("v3-root");
    fs::create_dir_all(&v3).unwrap();
    let error = disk::recover(&root).err().expect("recover must fail");
    assert!(error.to_string().contains(&v3.display().to_string()));
    assert!(error.to_string().contains("not migratable"));
    fs::remove_dir_all(&v3).unwrap();
    let other = record::namespace_id(&[1; 32], &[2; 16], "");
    let other = root.join(record::hex(&other));
    fs::create_dir_all(&other).unwrap();
    fs::write(other.join("descriptor"), b"FLCDSC1\0....").unwrap();
    let error = disk::recover(&root).err().expect("recover must fail");
    assert!(
        error
            .to_string()
            .contains(&other.join("descriptor").display().to_string())
    );
    assert!(error.to_string().contains("not migratable"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_19_two_managers_one_name() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let base = base("second");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        1024 * 1024,
        1024 * 1024,
    );
    config.name = host.config.name.clone();
    let mut second = Host::open_config(config);
    assert!(second.try_start().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_21_live_disconnected_client_quarantine() {
    if let Ok(socket) = std::env::var(CHILD_QUARANTINE) {
        let mut config = Config::dram_only(PathBuf::from(&socket), 32768);
        config.name = instance_name("quarantine-child");
        let mut client = Client::connect(&config, descriptor()).await.unwrap();
        let _ = client
            .get(
                &[key(128)],
                0,
                2,
                GetOptions {
                    cancel: Some(CancelAt::Grant),
                    ..GetOptions::default()
                },
            )
            .await;
        drop(client);
        fs::write(PathBuf::from(&socket).with_extension("ready"), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
    let mut host = Host::dram_only(32768);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    assert!(host.gauge("memory_allocated_bytes") > 0);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "l0_21_live_disconnected_client_quarantine",
            "--nocapture",
        ])
        .env(CHILD_QUARANTINE, &host.config.socket)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let ready = host.config.socket.with_extension("ready");
    wait(|| ready.exists());
    // The connection is gone but the process lives: the lease is quarantined.
    host.wait(|host| host.gauge("connections") <= 1);
    assert!(host.gauge("memory_allocated_bytes") > 0);
    let mut probe = host.client().await;
    assert_eq!(
        probe.put(key(256), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    child.kill().unwrap();
    child.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match probe.put(key(256), None, PutOptions::default()).await {
            PutOutcome::Stored(_) => break,
            PutOutcome::Refused(reason) if reason == proto::Reason::Budget as u8 => {
                assert!(Instant::now() < deadline, "quarantine was not reclaimed");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            other => panic!("unexpected Put outcome after exit: {other:?}"),
        }
    }
    fs::remove_file(ready).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_22_cancel_every_state_and_commit_wins() {
    let mut host = Host::new(4096, 4 * 1024 * 1024);
    host.start();
    for cancel in [
        CancelAt::Accept,
        CancelAt::Grant,
        CancelAt::Filled,
        CancelAt::Commit,
    ] {
        let mut client = host.client().await;
        let value = key(match cancel {
            CancelAt::Accept => 192,
            CancelAt::Grant => 256,
            CancelAt::Filled => 320,
            CancelAt::Commit => 384,
            CancelAt::None => 128,
        });
        let outcome = client
            .put(
                value,
                None,
                PutOptions {
                    cancel: Some(cancel),
                    ..PutOptions::default()
                },
            )
            .await;
        match cancel {
            CancelAt::Commit => assert!(matches!(outcome, PutOutcome::Stored(_))),
            _ => assert!(
                matches!(outcome, PutOutcome::Cancelled),
                "{cancel:?}: {outcome:?}"
            ),
        }
    }
    let mut client = host.client().await;
    let bytes = client.store(key(128), None).await;
    assert_eq!(
        client
            .get(
                &[key(128)],
                0,
                2,
                GetOptions {
                    cancel: Some(CancelAt::Grant),
                    ..GetOptions::default()
                },
            )
            .await,
        GetOutcome::Cancelled
    );
    let mut after = host.client().await;
    assert_eq!(after.fetch(&[key(128)], 0, 2).await[0], bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_23_commit_waits_for_durability() {
    let base = base("commit");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("commit");
    config.faults.pause_at = Some("before_rename".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let stored = Arc::new(AtomicBool::new(false));
    let flag = stored.clone();
    let task = tokio::spawn(async move {
        let outcome = client.put(key(128), None, PutOptions::default()).await;
        flag.store(true, Ordering::Release);
        outcome
    });
    wait(|| signal.exists());
    assert!(!stored.load(Ordering::Acquire), "Stored before durability");
    assert_eq!(host.durable_records(), 0);
    fs::remove_file(signal).unwrap();
    assert!(matches!(task.await.unwrap(), PutOutcome::Stored(_)));
    host.durable(1);
}

#[test]
fn l0_24_signal_handling() {
    let base = base("signal");
    fs::create_dir_all(base.join("run")).unwrap();
    let socket = base.join("run/manager.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_selene"))
        .args([
            "--name",
            &instance_name("signal"),
            "--socket",
            socket.to_str().unwrap(),
            "--memory",
            "8M",
            "--staging",
            "8M",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    wait(|| socket.exists());
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    let status = child.wait().unwrap();
    assert!(status.success(), "{status:?}");
    assert!(!socket.exists());
    let _ = fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_25_verify_before_window_reuse() {
    let mut host = Host::new(4096, 4 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let outcome = client
        .put(
            key(128),
            None,
            PutOptions {
                corrupt_page: Some(0),
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Refused(proto::Reason::Invalid as u8));
    host.wait(|host| host.gauge("staging_allocated_bytes") == 0);
    let mut fresh = host.client().await;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match fresh.put(key(128), None, PutOptions::default()).await {
            PutOutcome::Stored(_) => break,
            PutOutcome::Refused(reason) if reason == proto::Reason::Inflight as u8 => {
                assert!(Instant::now() < deadline, "inflight did not drain");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_27_no_progress_timeout_ends_the_call() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(key(256), None)),
        })
        .await
        .unwrap();
    loop {
        match replies.message().await.unwrap().unwrap().r {
            Some(proto::put_reply::R::Accept(_)) => break,
            Some(_) => continue,
            None => panic!("reply stream ended"),
        }
    }
    let start = Instant::now();
    let mut timed_out = false;
    loop {
        match replies.message().await {
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
                timed_out = true;
                break;
            }
            Ok(Some(reply)) => match reply.r {
                Some(proto::put_reply::R::Refused(refused)) => {
                    assert_eq!(refused.reason, proto::Reason::Invalid as i32);
                    timed_out = true;
                    break;
                }
                Some(_) => continue,
                None => break,
            },
            Ok(None) => break,
        }
    }
    assert!(timed_out, "the call did not end on the no-progress timer");
    assert!(start.elapsed() >= Duration::from_secs(2));
    assert!(start.elapsed() < Duration::from_secs(8));
    drop(sender);
    let mut fresh = host.client().await;
    assert!(!fresh.fetch(&[key(128)], 0, 2).await[0].is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_28_rootless_eviction_skips_internal_nodes() {
    let mut host = Host::dram_only(65536);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    client.store(key(192), Some(key(128))).await;
    // The arena is full and the root now has a child. Eviction must take the
    // leaf child, not loop on the row-bearing internal root.
    let stored = tokio::time::timeout(Duration::from_secs(5), client.store(key(256), None))
        .await
        .expect("rootless eviction must make progress");
    assert!(!stored.is_empty());
    let subscription = client.subscribe().await;
    assert!(subscription.entries.contains_key(&key(256)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_29_cancel_retires_blocked_chunks() {
    let base = base("blocked");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("blocked");
    config.faults.delay_ms = 300;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let outcome = client
        .put(
            key(1024 * 64),
            None,
            PutOptions {
                cancel_after_fills: Some(12),
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Cancelled);
    // The cancelled Put must release INFLIGHT and its reservation; blocked
    // chunks that were never submitted must not hold the drain open.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut probe = host.client().await;
    loop {
        let outcome = probe
            .put(
                key(1024 * 64),
                None,
                PutOptions {
                    cancel: Some(CancelAt::Accept),
                    ..PutOptions::default()
                },
            )
            .await;
        match outcome {
            PutOutcome::Refused(reason) if reason == proto::Reason::Inflight as u8 => {
                assert!(Instant::now() < deadline, "INFLIGHT did not drain");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            PutOutcome::Cancelled => break,
            other => panic!("unexpected probe outcome: {other:?}"),
        }
    }
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_30_committed_detached_put_finishes() {
    let base = base("detached");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("detached");
    config.faults.delay_ms = 100;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let outcome = client
        .put(
            key(512 * 64),
            None,
            PutOptions {
                abort_after_commit: true,
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Abandoned);
    drop(client);
    host.wait(|host| host.durable_records() == 1);
    let mut check = host.client().await;
    let subscription = check.subscribe().await;
    assert!(subscription.entries.contains_key(&key(512 * 64)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_31_timed_out_publication_finishes_instead_of_orphaning() {
    let base = base("timeout-publish");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("timeout-publish");
    config.faults.pause_at = Some("before_rename".into());
    let signal = base.join("gate");
    config.faults.signal = Some(signal.clone());
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let task = tokio::spawn(async move { client.put(key(128), None, PutOptions::default()).await });
    let deadline = Instant::now() + Duration::from_secs(10);
    while !signal.exists() {
        assert!(
            Instant::now() < deadline,
            "publication never reached before_rename"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Let the 3 s no-progress timer end the call while the rename is paused.
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(host.durable_records(), 0);
    fs::remove_file(&signal).unwrap();
    let outcome = task.await.unwrap();
    assert!(
        matches!(
            outcome,
            PutOutcome::Status(_) | PutOutcome::Cancelled | PutOutcome::Abandoned
        ),
        "{outcome:?}"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while host.durable_records() != 1 {
        assert!(Instant::now() < deadline, "publication did not complete");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut check = host.client().await;
    let subscription = check.subscribe().await;
    assert!(subscription.entries.contains_key(&key(128)));
    // Recovery must find exactly the record the catalog announced.
    drop(subscription);
    drop(check);
    host.stop();
    eprintln!("l0_31: stopped");
    host.start();
    let mut after = host.client().await;
    let subscription = after.subscribe().await;
    assert_eq!(subscription.entries.len(), 1);
    assert!(subscription.entries.contains_key(&key(128)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_32_streaming_reservation_includes_the_disk_record() {
    let mut host = Host::new(4096, 64 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let (sender, replies) = client.raw_begin(key(128), None).await;
    // key(128) is 2 pages + row: page_segment 8192, row_segment 16384,
    // row 4096, so envelope + 20480 = 24576 of disk record plus the fixed
    // 2 x 16 KiB staging hold is reserved before any window is granted.
    let deadline = Instant::now() + Duration::from_secs(5);
    while host.gauge("nvme_reserved_bytes") < 24576 + 32768 {
        assert!(
            Instant::now() < deadline,
            "streaming reservation was undercharged"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let before = host.gauge("nvme_reserved_bytes");
    assert!(before >= 24576 + 32768, "hold was not included: {before}");
    sender
        .send(proto::PutMessage {
            m: Some(proto::put_message::M::Cancel(proto::Cancel {})),
        })
        .await
        .unwrap();
    drop(sender);
    drop(replies);
    drop(client);
    // A quarantine may keep the fixed hold, but the disk record reservation
    // must be released.
    let deadline = Instant::now() + Duration::from_secs(10);
    while host.gauge("nvme_reserved_bytes") > before - 24576 {
        assert!(
            Instant::now() < deadline,
            "streaming reservation was not released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_33_write_budget_uses_the_streaming_charge() {
    let base = base("stream-budget");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("stream-budget");
    config.write_budget = 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let mut subscription = client.subscribe().await;
    // 8 MiB streamed record: admitted with a negative balance.
    client.store(key(2048 * 64), None).await;
    assert_eq!(
        client.put(key(256), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    let room = subscription.until_room().await;
    assert!(room > 0);
    client.store(key(256), None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_34_concurrent_nvme_gets_share_connection_credits() {
    let mut host = Host::new(4096, 64 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let one = client.store(key(64 * 64), None).await;
    let two = client.store(key(128 * 64), None).await;
    let (left, right) = client
        .fetch_two((vec![key(64 * 64)], 0, 64), (vec![key(128 * 64)], 0, 128))
        .await;
    match (left, right) {
        (GetOutcome::Done(records), GetOutcome::Done(other)) => {
            assert_eq!(records[0], one);
            assert_eq!(other[0], two);
        }
        other => panic!("concurrent Gets failed: {other:?}"),
    }
    host.wait(|host| {
        host.gauge("leases") == 0
            && host.gauge("cleanup_records") == 0
            && host.gauge("open_ops") == 0
    });
}

#[test]
fn l0_38_async_reads_overtake_queued_writes() {
    let base = base("read-priority");
    fs::create_dir_all(&base).unwrap();
    let lock = Arc::new(disk::RootLock::acquire(&base.join("root")).unwrap());
    // Long enough that enqueueing the read cannot race the first write completing.
    let worker = disk::Worker::start(
        disk::Faults {
            delay_ms: 2000,
            ..disk::Faults::default()
        },
        lock,
    );
    let source = base.join("source");
    fs::write(&source, vec![7u8; 4096]).unwrap();
    let file = Arc::new(disk::open_read(&source).unwrap());
    let arena = Arena::new(4096).unwrap();
    let write_arena = Arena::new(8 * 4096).unwrap();
    for index in 0..8u64 {
        let mut extent = write_arena.allocate(4096).unwrap();
        extent.bytes_mut().fill(index as u8);
        let temporary = Arc::new(disk::DiskExtent::new(base.join(format!("w-{index}.tmp"))));
        temporary.retire();
        worker
            .sender
            .send(disk::Task::Chunk {
                id: index,
                temporary,
                offset: 0,
                data: ExtentSlice::new(Arc::new(extent), 0, 4096),
            })
            .unwrap();
    }
    worker
        .read_sender
        .send(disk::Task::Read {
            id: 999,
            file,
            offset: 0,
            buffer: arena.allocate(4096).unwrap(),
        })
        .unwrap();
    // The write channel holds all eight writes, so the read is enqueued while at most the
    // first write is in flight; it must be the next completion.
    let mut writes_ahead = 0;
    loop {
        let completion = worker.receiver.recv().unwrap();
        if completion.id == 999 {
            assert_eq!(completion.data.unwrap().bytes(), &[7u8; 4096]);
            break;
        }
        writes_ahead += 1;
    }
    assert!(
        writes_ahead <= 1,
        "the read waited behind {writes_ahead} queued writes"
    );
    for _ in writes_ahead..8 {
        worker.receiver.recv().unwrap();
    }
    fs::remove_dir_all(base).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_40_detached_finalization_uses_the_put_namespace() {
    let base = base("detached-ns");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        64 * 1024 * 1024,
    );
    config.name = instance_name("detached-ns");
    config.faults.delay_ms = 50;
    let mut host = Host::open_config(config);
    host.start();
    let mut first = host.client().await;
    let mut other_descriptor = descriptor();
    other_descriptor.fingerprint = [9; 16];
    let mut other = host.client_with(other_descriptor.clone()).await.unwrap();
    let _other_subscription = other.subscribe().await;
    // Parent in the first namespace, then a child whose connection goes away
    // after commit. Finalization must use the Put's namespace, not an
    // arbitrary one.
    first.store(key(64), None).await;
    host.wait(|host| host.durable_records() == 1);
    let outcome = first
        .put(
            key(576 * 64),
            Some(key(64)),
            PutOptions {
                abort_after_commit: true,
                ..PutOptions::default()
            },
        )
        .await;
    assert_eq!(outcome, PutOutcome::Abandoned);
    drop(first);
    host.wait(|host| host.durable_records() == 2);
    drop(_other_subscription);
    drop(other);
    host.stop();
    host.start();
    let mut first = host.client().await;
    let subscription = first.subscribe().await;
    assert!(subscription.entries.contains_key(&key(64)));
    assert!(subscription.entries.contains_key(&key(576 * 64)));
    let mut other = host.client_with(other_descriptor.clone()).await.unwrap();
    let subscription = other.subscribe().await;
    assert!(subscription.entries.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_41_streaming_windows_respect_the_nvme_cap() {
    let base = base("staging-cap");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        24 * 1024,
    );
    config.name = instance_name("staging-cap");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    // The record's streamed admission charge (envelope, record and the
    // two-window staging hold) can never fit the cap, so it is refused
    // outright rather than waiting for a Room that cannot arrive.
    assert_eq!(
        client.put(key(128), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
    assert!(host.gauge("nvme_reserved_bytes") <= 24 * 1024);
    // A row-only record whose window does fit still streams.
    let _ = client.store(key(3), None).await;
    assert!(host.gauge("nvme_reserved_bytes") <= 24 * 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_42_cancel_before_dispatch_withdraws_the_put() {
    let mut host = Host::new(1024 * 1024, 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(key(128), None)),
        })
        .await
        .unwrap();
    sender
        .send(proto::PutMessage {
            m: Some(proto::put_message::M::Cancel(proto::Cancel {})),
        })
        .await
        .unwrap();
    while let Ok(Ok(Some(_))) =
        tokio::time::timeout(Duration::from_millis(200), replies.message()).await
    {}
    drop(sender);
    drop(replies);
    host.wait(|host| host.gauge("open_ops") == 0);
    let mut fresh = host.client().await;
    // The withdrawn Put must not leave INFLIGHT behind.
    let _ = fresh.store(key(128), None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_43_failed_get_drains_release_every_pin() {
    let mut host = Host::new(4096, 64 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    client.store(key(256), Some(key(128))).await;
    host.durable(2);
    let directory = host
        .config
        .root
        .as_ref()
        .unwrap()
        .join(client.descriptor.name());
    for value in [key(128), key(256)] {
        fs::write(directory.join(record::file_name(&value)), b"").unwrap();
    }
    // Two streamed records mean two reads are in flight; the first failure
    // starts the drain, and the second completion must decrement it.
    let outcome = client
        .get(&[key(128), key(256)], 0, 4, GetOptions::default())
        .await;
    assert!(
        matches!(outcome, GetOutcome::Refused(reason) if reason == proto::Reason::Io as u8),
        "{outcome:?}"
    );
    host.wait(|host| host.gauge("cleanup_records") == 0 && host.gauge("leases") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_45_reserve_before_pin_with_four_open_puts() {
    let base = base("reserve-pin");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        8 * 1024 * 1024,
        24 * 1024 * 1024,
    );
    config.name = instance_name("reserve-pin");
    config.staging = 8 * 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut descriptor = descriptor();
    descriptor.ranks[0].row_bytes = 4096 * 1023;
    let mut client = host.client_with(descriptor).await.unwrap();
    client.store(key(64), None).await;
    host.durable(1);
    // Four open child Puts, as Flare keeps. Before reserve-before-pin all
    // four pinned the parent and BUDGETed; at least the first must be admitted
    // now that the reservation may evict and reshape.
    let mut streams = Vec::new();
    for index in 0..4u64 {
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        let replies = client
            .inner
            .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
            .await
            .unwrap()
            .into_inner();
        sender
            .send(proto::PutMessage {
                m: Some(client.begin(key(128 + index * 64), Some(key(64)))),
            })
            .await
            .unwrap();
        streams.push((sender, replies));
    }
    let mut accepts = 0;
    let mut budget = 0;
    for (_, replies) in streams.iter_mut() {
        loop {
            match replies.message().await.unwrap().unwrap().r {
                Some(proto::put_reply::R::Accept(_)) => {
                    accepts += 1;
                    break;
                }
                Some(proto::put_reply::R::Refused(refused))
                    if refused.reason == proto::Reason::Budget as i32 =>
                {
                    budget += 1;
                    break;
                }
                Some(proto::put_reply::R::Grant(_)) => continue,
                Some(_) => break,
                None => break,
            }
        }
    }
    assert!(
        accepts >= 1,
        "no child Put was admitted (accepts={accepts}, budget={budget})"
    );
    drop(streams);
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_46_small_arena_admits_the_record_that_fits_it() {
    // 4 KiB pages/row: a one-page record is a 16 KiB arena extent plus the
    // 4 KiB envelope, so 20 KiB. The old max of the arena and streaming
    // paths wanted 28 KiB and refused forever.
    let base = base("small-arena");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        16 * 1024,
        24 * 1024,
    );
    config.name = instance_name("small-arena");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let stored = match client.put(key(64), None, PutOptions::default()).await {
        PutOutcome::Stored(bytes) => bytes,
        other => panic!("one-page record was not admitted: {other:?}"),
    };
    let fetched = client.fetch(&[key(64)], 0, 1).await;
    assert_eq!(fetched[0], stored);
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_47_dram_only_evicts_the_parent_to_store_the_root() {
    // A 32 KiB arena holds a two-page parent. Its next-page child fits the
    // arena only as a three-page root after the parent is evicted.
    let mut host = Host::dram_only(32 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let stored = match client
        .put(key(192), Some(key(128)), PutOptions::default())
        .await
    {
        PutOutcome::Stored(bytes) => bytes,
        other => panic!("next-page child was not admitted: {other:?}"),
    };
    let fetched = client.fetch(&[key(192)], 0, 3).await;
    assert_eq!(fetched[0], stored);
    assert_eq!(
        client.get(&[key(128)], 0, 2, GetOptions::default()).await,
        GetOutcome::Refused(proto::Reason::Invalid as u8)
    );
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_48_streaming_reserves_both_grantable_windows() {
    // 128 pages and a 4 KiB row: 528 KiB of disk record plus a fixed 1 MiB
    // hold for two of the 512 KiB trailing windows. A 1,100 KiB budget can
    // never take the complete requirement even though the older charge of
    // disk plus the largest window (1,040 KiB) would have fitted, so the
    // record is refused OVERSIZE rather than waiting for a Room.
    let tight = base("two-windows-small");
    let mut config = Config::new(
        tight.join("records"),
        tight.join("run/m.sock"),
        4096,
        1100 * 1024,
    );
    config.name = instance_name("two-windows-small");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(key(128 * 64), None)),
        })
        .await
        .unwrap();
    let first = replies.message().await.unwrap().unwrap();
    match first.r {
        Some(proto::put_reply::R::Refused(refused)) => {
            assert_eq!(refused.reason, proto::Reason::Oversize as i32);
        }
        other => panic!("Put was accepted before its reservation fitted: {other:?}"),
    }
    drop(sender);
    drop(replies);

    let fitted = base("two-windows-fit");
    let mut config = Config::new(
        fitted.join("records"),
        fitted.join("run/m.sock"),
        4096,
        2 * 1024 * 1024,
    );
    config.name = instance_name("two-windows-fit");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let stored = client.store(key(128 * 64), None).await;
    let fetched = client.fetch(&[key(128 * 64)], 0, 128).await;
    assert_eq!(fetched[0], stored);
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_49_oversized_window_is_refused_oversize() {
    // A 16 MiB page cannot fit the 8 MiB staging region and the record does
    // not fit the 16 MiB arena, so no shape can ever be admitted. The refusal
    // is OVERSIZE: a Budget would arm a Room that can never arrive.
    let base = base("oversized-window");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = instance_name("oversized-window");
    config.staging = 8 * 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut value = descriptor();
    value.ranks[0].planes = vec![16 * 1024 * 1024];
    let mut client = host.client_with(value).await.unwrap();
    assert_eq!(
        client.put(key(64), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_50_streaming_reserves_the_largest_trailing_window() {
    // 192 pages and a 4 KiB row: 784 KiB of disk record plus a fixed 1 MiB
    // hold for two 512 KiB trailing windows. A 1,400 KiB budget cannot take
    // that complete requirement even though the first two windows would fit.
    let tight = base("trailing-window-tight");
    let mut config = Config::new(
        tight.join("records"),
        tight.join("run/m.sock"),
        4096,
        1400 * 1024,
    );
    config.name = instance_name("trailing-window-tight");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(key(192 * 64), None)),
        })
        .await
        .unwrap();
    let first = replies.message().await.unwrap().unwrap();
    match first.r {
        Some(proto::put_reply::R::Refused(refused)) => {
            assert_eq!(refused.reason, proto::Reason::Oversize as i32);
        }
        other => panic!("Put was accepted before the hold fitted: {other:?}"),
    }
    drop(sender);
    drop(replies);

    let fitted = base("trailing-window-fit");
    let mut config = Config::new(
        fitted.join("records"),
        fitted.join("run/m.sock"),
        4096,
        2 * 1024 * 1024,
    );
    config.name = instance_name("trailing-window-fit");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let stored = client.store(key(192 * 64), None).await;
    let fetched = client.fetch(&[key(192 * 64)], 0, 192).await;
    assert_eq!(fetched[0], stored);
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_51_rootless_child_that_cannot_fit_beside_its_parent_waits() {
    // The 32 KiB arena holds a two-page parent, whose rounded extent fills
    // it, so the four-page-tail child cannot be admitted while the parent
    // stays. That shortage depends on what must stay resident, not on the
    // child's own shape, so it is a Budget rather than an OVERSIZE; no Room
    // arrives and nothing is stored.
    let mut host = Host::dram_only(32 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(128), None).await;
    let mut subscription = client.subscribe().await;
    assert_eq!(
        client
            .put(key(384), Some(key(128)), PutOptions::default())
            .await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), subscription.until_room())
            .await
            .is_err(),
        "Room was offered for an allocation that needs the protected parent"
    );
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_52_hold_slots_stay_busy_until_writes_drain() {
    // With a slow writer, a third window can only be granted after one of
    // the first two writes completes: the hold slots, not copied buffers,
    // carry the in-flight windows.
    let base = base("hold-drain");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        4096,
        4 * 1024 * 1024,
    );
    config.name = instance_name("hold-drain");
    config.faults.delay_ms = 300;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let record = key(192 * 64);
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(record, None)),
        })
        .await
        .unwrap();
    let mut header: Option<Header> = None;
    let mut source: Option<Vec<u8>> = None;
    let mut first = 0u32;
    let mut grants = Vec::new();
    let mut fills = Vec::new();
    let mut covered = 0u64;
    let mut commit_sent = false;
    let mut stored = false;
    while !stored {
        let reply = replies.message().await.unwrap().unwrap();
        match reply.r {
            Some(proto::put_reply::R::Accept(accept)) => {
                first = u32::try_from(accept.first_page).unwrap();
                let parent = accept.parent.as_ref().map(key_from_msg);
                let (value, data, _) = client.content(record, parent, first);
                header = Some(value);
                source = Some(data);
            }
            Some(proto::put_reply::R::Grant(grant)) => {
                grants.push(Instant::now());
                let value = header.as_ref().unwrap();
                let data = source.as_mut().unwrap();
                let memory = grant.memory.as_ref().unwrap();
                let region = if memory.region == proto::Region::Arena as i32 {
                    Reg::Arena
                } else {
                    Reg::Staging
                };
                let length = usize::try_from(grant.length).unwrap();
                let start = usize::try_from(grant.record_offset).unwrap();
                let page_bytes = value.page_record_bytes() as usize;
                let mut hashes = Vec::new();
                for position in 0..grant.page_count as usize {
                    let relative = (grant.page_first - u64::from(first)) as usize + position;
                    let page = value.page_segment as usize + relative * page_bytes;
                    hashes.push(record::checksum(&data[page..page + page_bytes]));
                }
                let row_hash = if grant.row {
                    Some(record::checksum(&data[value.row_segment as usize..]))
                } else {
                    None
                };
                client.write(region, memory.offset, &data[start..start + length]);
                sender
                    .send(proto::PutMessage {
                        m: Some(proto::put_message::M::Filled(proto::WindowFilled {
                            transfer: grant.transfer,
                            index: grant.index,
                            page_xxh3: hashes,
                            row_xxh3: row_hash.unwrap_or(0),
                            has_row: grant.row,
                        })),
                    })
                    .await
                    .unwrap();
                covered += grant.length;
                fills.push(Instant::now());
                let value = header.as_ref().unwrap();
                let payload = value.length_with_row() - value.page_segment;
                if !commit_sent && covered >= payload {
                    for (index, chunk) in source.as_ref().unwrap()[..value.page_segment as usize]
                        .chunks(4096)
                        .enumerate()
                    {
                        sender
                            .send(proto::PutMessage {
                                m: Some(proto::put_message::M::Commit(proto::PutCommit {
                                    transfer: grant.transfer,
                                    metadata_offset: (index * 4096) as u64,
                                    metadata: chunk.to_vec(),
                                })),
                            })
                            .await
                            .unwrap();
                    }
                    commit_sent = true;
                }
            }
            Some(proto::put_reply::R::Stored(_)) => stored = true,
            Some(proto::put_reply::R::Refused(refused)) => {
                panic!("streamed Put refused: {refused:?}")
            }
            None => break,
        }
    }
    let data = source.unwrap();
    assert_eq!(grants.len(), 3);
    assert_eq!(fills.len(), 3);
    assert!(
        grants[2].duration_since(fills[1]) >= Duration::from_millis(150),
        "the third window was granted before a slow write drained"
    );
    let fetched = client.fetch(&[record], 0, 192).await;
    assert_eq!(fetched[0], data);
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_53_room_reconsiders_the_arena_after_a_lease_releases() {
    // 16 MiB arena, 8 MiB staging. An 8 MiB page record needs a 16 MiB
    // arena extent but its streaming hold would be 16 MiB. While the
    // resident record is leased the Put is refused; after the lease
    // releases, Room must offer the arena retry and the Put must complete.
    let base = base("room-arena-retry");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        16 * 1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = instance_name("room-arena-retry");
    config.staging = 8 * 1024 * 1024;
    let mut host = Host::open_config(config);
    host.start();
    let mut geometry = descriptor();
    geometry.ranks[0].planes = vec![8 * 1024 * 1024];
    let mut resident = host.client_with(geometry.clone()).await.unwrap();
    let stored = resident.store(key(64), None).await;
    host.durable(1);
    // A second namespace (same geometry) for the Put that has to wait.
    let mut other = geometry;
    other.identity = [9; 32];
    other.fingerprint = [4; 16];
    let mut waiter = host.client_with(other).await.unwrap();
    let mut subscription = waiter.subscribe().await;
    // Lease the resident record and hold the window.
    let (lease_sender, lease_receiver) = tokio::sync::mpsc::channel(8);
    let mut lease_replies = resident
        .inner
        .get(resident.request(tokio_stream::wrappers::ReceiverStream::new(lease_receiver)))
        .await
        .unwrap()
        .into_inner();
    lease_sender
        .send(proto::GetMessage {
            m: Some(proto::get_message::M::Begin(proto::GetBegin {
                namespace_id: resident.descriptor.namespace_id().to_vec(),
                chain: vec![key_msg(key(64))],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                destinations: Vec::new(),
                transfer: 0,
            })),
        })
        .await
        .unwrap();
    // Hold every lease window the record's Get grants (the 8 MiB page and
    // the row arrive as separate windows).
    let mut held = Vec::new();
    while let Ok(Ok(Some(reply))) =
        tokio::time::timeout(Duration::from_millis(100), lease_replies.message()).await
    {
        match reply.r {
            Some(proto::get_reply::R::Ready(grant)) => {
                held.push((grant.transfer, grant.index));
            }
            Some(proto::get_reply::R::Refused(refused)) => {
                panic!("lease Get refused: {refused:?}")
            }
            Some(proto::get_reply::R::Done(_)) | None => break,
            Some(_) => continue,
        }
    }
    assert!(!held.is_empty(), "lease Get granted no window");
    assert_eq!(
        waiter.put(key(64), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), subscription.until_room())
            .await
            .is_err(),
        "Room was offered while the arena record was still leased"
    );
    for (transfer, index) in held {
        lease_sender
            .send(proto::GetMessage {
                m: Some(proto::get_message::M::Released(proto::WindowReleased {
                    transfer,
                    index,
                })),
            })
            .await
            .unwrap();
    }
    loop {
        match lease_replies.message().await.unwrap().unwrap().r {
            Some(proto::get_reply::R::Done(_)) | None => break,
            Some(_) => continue,
        }
    }
    drop(lease_sender);
    drop(lease_replies);
    tokio::time::timeout(Duration::from_secs(2), subscription.until_room())
        .await
        .expect("Room never reconsidered the arena after the lease released");
    let stored_again = waiter.store(key(64), None).await;
    let fetched = waiter.fetch(&[key(64)], 0, 1).await;
    assert_eq!(fetched[0], stored_again);
    assert!(!stored.is_empty());
    host.wait(|host| host.gauge("open_ops") == 0);
}

async fn diagnostic_lease_all(
    client: &mut Client,
    keys: Vec<selene_manager::Key>,
    pages: u32,
) -> (
    tokio::sync::mpsc::Sender<proto::GetMessage>,
    tonic::Streaming<proto::GetReply>,
    Vec<(u64, u32)>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .get(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::GetMessage {
            m: Some(proto::get_message::M::Begin(proto::GetBegin {
                namespace_id: client.descriptor.namespace_id().to_vec(),
                chain: keys.iter().map(|key| key_msg(*key)).collect(),
                skip_pages: 0,
                expected_pages: u64::from(pages),
                row: true,
                destinations: Vec::new(),
                transfer: 0,
            })),
        })
        .await
        .unwrap();
    let mut held = Vec::new();
    while let Ok(Ok(Some(reply))) =
        tokio::time::timeout(Duration::from_millis(100), replies.message()).await
    {
        match reply.r {
            Some(proto::get_reply::R::Ready(grant)) => {
                held.push((grant.transfer, grant.index));
            }
            Some(proto::get_reply::R::Refused(refused)) => {
                panic!("diagnostic lease Get refused: {refused:?}")
            }
            Some(proto::get_reply::R::Done(_)) | None => break,
            Some(_) => continue,
        }
    }
    (sender, replies, held)
}

async fn diagnostic_release_all(
    sender: tokio::sync::mpsc::Sender<proto::GetMessage>,
    mut replies: tonic::Streaming<proto::GetReply>,
    held: Vec<(u64, u32)>,
) {
    for (transfer, index) in held {
        sender
            .send(proto::GetMessage {
                m: Some(proto::get_message::M::Released(proto::WindowReleased {
                    transfer,
                    index,
                })),
            })
            .await
            .unwrap();
    }
    while let Ok(Ok(Some(reply))) =
        tokio::time::timeout(Duration::from_millis(100), replies.message()).await
    {
        if matches!(reply.r, Some(proto::get_reply::R::Done(_)) | None) {
            break;
        }
    }
    drop(sender);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_54_get_sources_survive_eviction_pressure() {
    // Diagnostic: a Get's delivered bytes must be the bytes the manager
    // verified when it read the record. Hold leases on every arena record,
    // force a streamed Put, release the leases so the read refills the
    // arena, and apply Put pressure while the slow read is in flight. Then
    // repeat with a record too large for the arena so the header and the
    // payload windows are read at different times.
    let base = base("get-pressure");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        32 * 1024,
        4 * 1024 * 1024,
    );
    config.name = instance_name("get-pressure");
    config.faults.delay_ms = 150;
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let first = client.store(key(64), None).await;
    let second = client.store(key(128), Some(key(64))).await;
    host.durable(2);
    let (one_sender, one_replies, one) = diagnostic_lease_all(&mut client, vec![key(64)], 1).await;
    let (two_sender, two_replies, two) = diagnostic_lease_all(&mut client, vec![key(128)], 1).await;
    assert!(!one.is_empty() && !two.is_empty());
    // Both arena records are pinned, so this three-page root streams to disk.
    let streamed = client.store(key(192), None).await;
    diagnostic_release_all(one_sender, one_replies, one).await;
    diagnostic_release_all(two_sender, two_replies, two).await;
    // The arena is now free for the read to verify and refill the record.
    let mut reader = host.client().await;
    let mut pressure = host.client().await;
    let streamed_key = [key(192)];
    let fetch = reader.fetch(&streamed_key, 0, 3);
    let put = async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        pressure.store(key(256), None).await
    };
    let (fetched, pressed) = tokio::join!(fetch, put);
    assert_eq!(fetched[0], streamed);
    assert!(!pressed.is_empty());
    assert!(!first.is_empty() && !second.is_empty());

    // 128 pages do not fit the arena: the header is read once and each
    // payload window later, while another Put adds eviction pressure.
    let large = client.store(key(128 * 64), None).await;
    let large_key = [key(128 * 64)];
    let fetch = reader.fetch(&large_key, 0, 128);
    let put = async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        pressure.store(key(384), None).await
    };
    let (fetched, pressed) = tokio::join!(fetch, put);
    assert_eq!(fetched[0], large);
    assert!(!pressed.is_empty());
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_55_cancelled_get_lease_keeps_its_bytes() {
    // Diagnostic: after a Get is cancelled its tree pins are released, but
    // the client still holds the granted window. Eviction pressure must not
    // free or reuse that arena extent before the client reads it.
    let mut host = Host::new(32 * 1024, 4 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let stored = client.store(key(64), None).await;
    host.durable(1);
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let mut replies = client
        .inner
        .get(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::GetMessage {
            m: Some(proto::get_message::M::Begin(proto::GetBegin {
                namespace_id: client.descriptor.namespace_id().to_vec(),
                chain: vec![key_msg(key(64))],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                destinations: Vec::new(),
                transfer: 0,
            })),
        })
        .await
        .unwrap();
    let (region, offset, length, record_offset) = loop {
        match replies.message().await.unwrap().unwrap().r {
            Some(proto::get_reply::R::Ready(grant)) => {
                let memory = grant.memory.unwrap();
                let region = if memory.region == proto::Region::Arena as i32 {
                    Reg::Arena
                } else {
                    Reg::Staging
                };
                break (
                    region,
                    memory.offset,
                    usize::try_from(memory.length).unwrap(),
                    usize::try_from(grant.record_offset).unwrap(),
                );
            }
            Some(proto::get_reply::R::Refused(refused)) => {
                panic!("diagnostic Get refused: {refused:?}")
            }
            Some(_) => continue,
            None => panic!("diagnostic Get ended before a window"),
        }
    };
    sender
        .send(proto::GetMessage {
            m: Some(proto::get_message::M::Cancel(proto::Cancel {})),
        })
        .await
        .unwrap();
    while let Ok(Ok(Some(reply))) =
        tokio::time::timeout(Duration::from_millis(200), replies.message()).await
    {
        if matches!(
            reply.r,
            Some(proto::get_reply::R::Done(_)) | Some(proto::get_reply::R::Refused(_)) | None
        ) {
            break;
        }
    }
    // Pressure: a 32 KiB root cannot fit beside the leased 16 KiB extent, so
    // eviction must either skip the lease or corrupt the client's window.
    let pressed = client.store(key(128), None).await;
    assert!(!pressed.is_empty());
    let window = client.read(region, offset, length);
    assert_eq!(window, stored[record_offset..record_offset + length]);
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_56_dram_only_arena_boundary_ignores_the_envelope() {
    // The 4 KiB disk envelope lives outside the arena. The record's buddy
    // extent includes its own header and checksums, so an extent of exactly
    // the 16 MiB arena is storable; only a larger extent is OVERSIZE.
    let mut host = Host::dram_only(16 * 1024 * 1024);
    host.start();
    let mut value = descriptor();
    value.ranks = vec![selene_manager::record::RankSizes {
        planes: vec![65_539, 4_100],
        row_bytes: 1_000_003,
    }];
    let mut client = host.client_with(value.clone()).await.unwrap();
    let fits = key(140 * 64 + 3);
    let header = Header::new(&value, fits, None, [4; 16], 140).unwrap();
    assert_eq!(header.length_with_row(), 11_907_072);
    assert_eq!(
        header.length_with_row().max(4096).next_power_of_two(),
        16 * 1024 * 1024
    );
    assert!(
        matches!(
            client.put(fits, None, PutOptions::default()).await,
            PutOutcome::Stored(_)
        ),
        "a 16 MiB extent must fit a 16 MiB DRAM-only arena"
    );
    let larger = key(220 * 64 + 3);
    let header = Header::new(&value, larger, None, [4; 16], 220).unwrap();
    assert!(header.length_with_row() > 16 * 1024 * 1024);
    assert_eq!(
        client.put(larger, None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
}

#[test]
fn l0_57_sigkill_never_blocks_a_restart_on_the_same_instance() {
    let base = base("sigkill-restart");
    fs::create_dir_all(base.join("run")).unwrap();
    let socket = base.join("run/manager.sock");
    let name = instance_name("sigkill-restart");
    let spawn = |stderr: &str| {
        Command::new(env!("CARGO_BIN_EXE_selene"))
            .args([
                "--name",
                &name,
                "--socket",
                socket.to_str().unwrap(),
                "--memory",
                "8M",
                "--staging",
                "8M",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::from(fs::File::create(base.join(stderr)).unwrap()))
            .spawn()
            .unwrap()
    };

    let mut first = spawn("first.err");
    wait(|| socket.exists());
    unsafe {
        libc::kill(first.id() as i32, libc::SIGKILL);
    }
    let status = first.wait().unwrap();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    // The killed manager leaves its socket, lock and region files behind. The
    // flock alone decides ownership: the directory's existence must not refuse
    // the restart, and the stale socket must be replaced.
    assert!(
        socket.exists(),
        "the killed manager leaves its socket behind"
    );
    let mut restarted = spawn("second.err");
    wait(|| std::os::unix::net::UnixStream::connect(&socket).is_ok());
    assert!(
        restarted.try_wait().unwrap().is_none(),
        "the restarted manager must stay up"
    );
    unsafe {
        libc::kill(restarted.id() as i32, libc::SIGTERM);
    }
    let status = restarted.wait().unwrap();
    assert!(status.success(), "{status:?}");
    assert!(!socket.exists());
    let log = fs::read_to_string(base.join("second.err")).unwrap();
    assert!(
        log.contains("reclaimed stale instance"),
        "missing reclaim log: {log}"
    );
    let _ = fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_58_dram_only_refuses_a_record_whose_rounded_extent_cannot_fit() {
    // The arena rounds a record's length up to a power of two. A 12 MiB arena
    // has no 16 MiB block, so a record just over 8 MiB can never be stored and
    // a DRAM-only manager has nowhere to stream it: the refusal is OVERSIZE,
    // not a BUDGET wait whose Room can never arrive.
    let value = descriptor();
    let mut host = Host::dram_only(12 * 1024 * 1024);
    host.start();
    let mut client = host.client().await;
    let large = key(2048 * 64);
    let header = Header::new(&value, large, None, [4; 16], 2048).unwrap();
    assert!(header.length_with_row() <= 12 * 1024 * 1024);
    assert!(header.length_with_row().max(4096).next_power_of_two() > 12 * 1024 * 1024);
    assert_eq!(
        client.put(large, None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
    // A record whose rounded extent fits is still admitted.
    let fits = key(1024 * 64);
    assert!(matches!(
        client.put(fits, None, PutOptions::default()).await,
        PutOutcome::Stored(_)
    ));
}

fn streaming_host(staging: usize) -> Host {
    let base = base("staging-oversize");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        1024 * 1024,
        64 * 1024 * 1024,
    );
    config.name = instance_name("staging-oversize");
    config.staging = staging;
    Host::open_config(config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_59_refuses_a_streamed_record_whose_staging_hold_cannot_fit() {
    // The row is one indivisible window, and a streamed Put reserves a hold of
    // two of its largest windows. An 8 MiB row in an 8 MiB staging region has
    // a 16 MiB hold, so the record can never be admitted: OVERSIZE, not
    // BUDGET. A 16 MiB region holds the same record.
    let value = selene_manager::record::Descriptor {
        identity: [7; 32],
        fingerprint: [3; 16],
        page_tokens: 64,
        ranks: vec![selene_manager::record::RankSizes {
            planes: vec![4000],
            row_bytes: 8 * 1024 * 1024,
        }],
        tenant: String::new(),
    };
    let mut host = streaming_host(8 * 1024 * 1024);
    host.start();
    let mut client = host.client_with(value.clone()).await.unwrap();
    assert_eq!(
        client.put(key(64), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
    drop(client);
    drop(host);

    let mut host = streaming_host(16 * 1024 * 1024);
    host.start();
    let mut client = host.client_with(value).await.unwrap();
    assert!(matches!(
        client.put(key(64), None, PutOptions::default()).await,
        PutOutcome::Stored(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_60_restore_is_not_refused_when_the_nvme_budget_is_full() {
    // One streamed record and two arena records fill the durable budget close
    // to --nvme. A restore window is transient staging traffic, not a durable
    // reservation, so the disk read must still be served.
    let base = base("restore-budget");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        1024 * 1024,
        3_200_000,
    );
    config.name = instance_name("restore-budget");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    let streamed = client.store(key(512 * 64), None).await;
    client.store(key(64 * 64), None).await;
    client.store(key(64 * 64 + 1), None).await;
    host.durable(3);
    assert!(
        host.gauge("nvme_reserved_bytes") > 3_000_000,
        "the durable budget must be nearly full: {}",
        host.gauge("nvme_reserved_bytes")
    );
    assert_eq!(client.fetch(&[key(512 * 64)], 0, 512).await[0], streamed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_61_rootless_temporary_pressure_waits_instead_of_oversize() {
    // A 48 KiB arena: a 16 KiB parent and a 32 KiB child fit together once
    // nothing else holds memory. An accepted but uncommitted Put holds an
    // unrelated 32 KiB extent, so the child cannot be allocated now and its
    // six-page full-chain fallback would need 64 KiB. That is temporary
    // pressure, not an impossible shape, so the child is refused BUDGET: it
    // may fit beside its parent once the other operation ends. (The extent
    // stays leased until its writer's process exits, so this test asserts the
    // classification rather than the eventual retry.)
    let mut host = Host::dram_only(48 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(64), None).await;

    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut replies = client
        .inner
        .clone()
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(key(3 * 64), None)),
        })
        .await
        .unwrap();
    let accept = replies.message().await.unwrap().unwrap();
    assert!(
        matches!(accept.r, Some(proto::put_reply::R::Accept(_))),
        "{accept:?}"
    );

    assert_eq!(
        client
            .put(key(6 * 64), Some(key(64)), PutOptions::default())
            .await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    drop(sender);
    drop(replies);
}

#[test]
fn l0_62_socket_directory_modes() {
    // The manager sets a mode only on a directory it created. An existing
    // directory keeps its mode: pointing the socket into /tmp or /run must not
    // change a host directory, and a manager started as root must not either.
    use std::os::unix::fs::PermissionsExt;
    let base = base("socket-mode");
    fs::create_dir_all(&base).unwrap();
    fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
    let command = |name: &str, socket: &std::path::Path| {
        Command::new(env!("CARGO_BIN_EXE_selene"))
            .args([
                "--name",
                name,
                "--socket",
                socket.to_str().unwrap(),
                "--memory",
                "8M",
                "--staging",
                "8M",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let socket = base.join("manager.sock");
    let mut child = command(&instance_name("socket-mode"), &socket);
    wait(|| socket.exists());
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    assert!(child.wait().unwrap().success());
    let mode = fs::metadata(&base).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o755, "an existing socket directory keeps its mode");
    // A directory the manager creates gets 0770.
    let directory = base.join("run");
    let socket = directory.join("manager.sock");
    let mut child = command(&instance_name("socket-mode"), &socket);
    wait(|| socket.exists());
    unsafe {
        libc::kill(child.id() as i32, libc::SIGTERM);
    }
    assert!(child.wait().unwrap().success());
    let mode = fs::metadata(&directory).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o770, "a created socket directory gets 0770");
    let _ = fs::remove_dir_all(base);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_63_a_paused_put_keeps_its_window_past_the_deadline() {
    // A Put that stalls past the no-progress deadline is failed, but its
    // granted window stays leased: the client may still be writing into it.
    // A later Put cannot take the extent, and bytes written after the deadline
    // are intact.
    use std::os::unix::fs::FileExt;
    let mut host = Host::dram_only(32 * 1024);
    host.start();
    let client = host.client().await;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut replies = client
        .inner
        .clone()
        .put(client.request(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    sender
        .send(proto::PutMessage {
            m: Some(client.begin(key(3 * 64), None)),
        })
        .await
        .unwrap();
    let (offset, length) = loop {
        match replies.message().await.unwrap().unwrap().r {
            Some(proto::put_reply::R::Grant(grant)) => {
                let memory = grant.memory.unwrap();
                break (memory.offset, memory.length);
            }
            Some(proto::put_reply::R::Refused(refused)) => panic!("refused: {refused:?}"),
            Some(_) => continue,
            None => panic!("the Put ended before a grant"),
        }
    };
    // Stall until the manager ends the operation for no progress: the stream
    // task fails it with DEADLINE_EXCEEDED and the store timer refuses it with
    // INVALID, and either may reach the client first.
    match tokio::time::timeout(Duration::from_secs(10), replies.message())
        .await
        .expect("the no-progress deadline did not fire")
    {
        Ok(None) => {}
        Ok(Some(reply)) => {
            let refused = matches!(
                &reply.r,
                Some(proto::put_reply::R::Refused(refused))
                    if refused.reason == proto::Reason::Invalid as i32
            );
            assert!(refused, "unexpected reply after the deadline: {reply:?}");
        }
        Err(status) => assert!(status.message().contains("no progress"), "{status:?}"),
    }
    // Resume writing into the window; the extent must not be reusable.
    let pattern: Vec<u8> = (0..length as usize)
        .map(|index| (index % 251) as u8)
        .collect();
    client.arena.write_all_at(&pattern, offset).unwrap();
    let mut other = host.client().await;
    assert_eq!(
        other.put(key(64), None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Budget as u8),
        "the stalled Put's extent must stay leased"
    );
    assert_eq!(client.read(Reg::Arena, offset, length as usize), pattern);
}

fn charge_host(nvme: u64) -> Host {
    let base = base("charge-oversize");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        2 * 1024 * 1024,
        nvme,
    );
    config.name = instance_name("charge-oversize");
    Host::open_config(config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_64_refuses_a_record_whose_admission_charge_cannot_fit() {
    // The record's logical length fits --nvme, but the arena keeps it in a
    // rounded 2 MiB extent, so its admission charge is the envelope plus that
    // extent and can never fit 1.5 MiB. The arena shape is the one admission
    // selects and there is no streaming fallback for a budget refusal: that is
    // OVERSIZE, not a permanent Budget.
    let record = key(300 * 64);
    let mut host = charge_host(1_500_000);
    host.start();
    let mut client = host.client().await;
    assert_eq!(
        client.put(record, None, PutOptions::default()).await,
        PutOutcome::Refused(proto::Reason::Oversize as u8)
    );
    drop(client);
    drop(host);

    // With room for the rounded extent the same record is admitted.
    let mut host = charge_host(2_300_000);
    host.start();
    let mut client = host.client().await;
    assert!(matches!(
        client.put(record, None, PutOptions::default()).await,
        PutOutcome::Stored(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_65_rootless_tail_shortage_waits_without_room() {
    // A 64 KiB arena holds a five-page root and a five-page tail. The next
    // four-page tail needs 32 KiB beside both retained ancestors (96 KiB in
    // total), which depends on what must stay resident rather than on the
    // record's own shape: it is a Budget, and no Room can arrive.
    let mut host = Host::dram_only(64 * 1024);
    host.start();
    let mut client = host.client().await;
    client.store(key(5 * 64), None).await;
    client.store(key(10 * 64), Some(key(5 * 64))).await;
    let mut subscription = client.subscribe().await;
    assert_eq!(
        client
            .put(key(14 * 64), Some(key(10 * 64)), PutOptions::default())
            .await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), subscription.until_room())
            .await
            .is_err(),
        "Room was offered for an allocation the retained chain can never free"
    );
    host.wait(|host| host.gauge("open_ops") == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn l0_66_protected_parent_shortage_is_a_budget() {
    // The parent's reservation and the child's together exceed --nvme, and
    // the parent is protected by this admission. That is a capacity shortage
    // caused by what must stay resident, not by the record's own charge: it
    // is a Budget with no Room, and nothing is stored.
    let base = base("protected-ancestry");
    let mut config = Config::new(
        base.join("records"),
        base.join("run/m.sock"),
        64 * 1024,
        64 * 1024,
    );
    config.name = instance_name("protected-ancestry");
    let mut host = Host::open_config(config);
    host.start();
    let mut client = host.client().await;
    client.store(key(5 * 64), None).await;
    let mut subscription = client.subscribe().await;
    assert_eq!(
        client
            .put(key(10 * 64), Some(key(5 * 64)), PutOptions::default())
            .await,
        PutOutcome::Refused(proto::Reason::Budget as u8)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), subscription.until_room())
            .await
            .is_err(),
        "Room was offered for a charge the protected parent keeps occupied"
    );
    host.wait(|host| host.gauge("open_ops") == 0);
}
