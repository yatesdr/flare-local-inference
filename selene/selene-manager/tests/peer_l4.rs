//! L4: two managers on an emulated network (CPU only).
//!
//! Two manager processes are started in separate network namespaces joined by a veth pair,
//! with `netem` delay and bandwidth limits in both directions, and driven through their Unix
//! sockets from this process.
//!
//! The test is ignored by default because it changes host networking state. Run it as root
//! inside a privileged container that has `ip` and `tc` installed:
//!
//! ```text
//! cargo test --release --test peer_l4 -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Every check prints PASS or FAIL with its name; the test fails at the end if any check failed.
//! Namespaces, veth pairs and manager processes are removed by their guards, including on panic.
//!
//! The test pins `--peer-transport stream` and `--stream-connections 1`. The build may have
//! RDMA enabled, and `auto` would then carry bulk bytes over RDMA, bypassing the shaped veth;
//! and several parallel stream connections would each get their own share of the link,
//! defeating the per-link rate that makes an in-flight fetch interruptible.

mod support;

/// These tests own host networking state; never run two at once.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, SanType,
};
use selene_manager::{Key, proto, store::Config as StoreConfig};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use support::*;

// ------------------------------------------------------------------------------- checks

struct Checks {
    failed: Vec<String>,
}

impl Checks {
    fn new() -> Self {
        Self { failed: Vec::new() }
    }

    fn pass(&mut self, name: &str, detail: impl std::fmt::Display) {
        println!("PASS {name}: {detail}");
    }

    fn fail(&mut self, name: &str, detail: impl std::fmt::Display) {
        println!("FAIL {name}: {detail}");
        self.failed.push(name.to_string());
    }

    fn check(&mut self, name: &str, outcome: Result<String, String>) {
        match outcome {
            Ok(detail) => self.pass(name, detail),
            Err(detail) => self.fail(name, detail),
        }
    }

    fn finish(&self) {
        if !self.failed.is_empty() {
            panic!("failed checks: {}", self.failed.join(", "));
        }
    }
}

// ------------------------------------------------------------------------------- processes

/// One manager process started inside a network namespace (or the current one).
struct Proc {
    child: Option<Child>,
    socket: PathBuf,
    log: PathBuf,
}

impl Proc {
    fn spawn(namespace: Option<&str>, args: &[String], socket: &Path, log: &Path) -> Self {
        let out = std::fs::File::create(log).unwrap();
        let err = out.try_clone().unwrap();
        let mut command = match namespace {
            Some(namespace) => {
                let mut command = Command::new("ip");
                command.args(["netns", "exec", namespace]);
                command
            }
            None => Command::new(env!("CARGO_BIN_EXE_selene")),
        };
        if namespace.is_some() {
            command.arg(env!("CARGO_BIN_EXE_selene"));
        }
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err);
        let child = command.spawn().expect("spawn manager");
        Self {
            child: Some(child),
            socket: socket.to_path_buf(),
            log: log.to_path_buf(),
        }
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn tail(&self, lines: usize) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill();
    }
}

fn wait_ready(manager: &Proc) {
    let runtime = runtime();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if manager.socket.exists() {
            let config = StoreConfig::dram_only(manager.socket.clone(), 4 << 20);
            let descriptor = descriptor();
            let ready = runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(2), Client::connect(&config, descriptor))
                    .await
                    .is_ok_and(|result| result.is_ok())
            });
            if ready {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "manager did not become ready; log:\n{}",
            manager.tail(20)
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Tail pages of a record fetched on its own, which is what a Get must expect.
fn record_pages(key: &Key, parent: Option<&Key>) -> u32 {
    let first = parent.map_or(0, |parent| parent.consumed_tokens);
    u32::try_from((key.consumed_tokens - first) / descriptor().page_tokens).unwrap()
}

// ------------------------------------------------------------------------------- topology

/// A pair of network namespaces joined by a rate- and delay-limited veth pair.
struct Topology {
    near: String,
    storage: String,
    near_addr: String,
    storage_addr: String,
}

impl Topology {
    fn create(label: &str) -> Self {
        let tag = format!("{}-{label}", std::process::id());
        let topology = Self {
            near: format!("selene-l4-a-{tag}"),
            storage: format!("selene-l4-b-{tag}"),
            near_addr: "192.0.2.1".into(),
            storage_addr: "192.0.2.2".into(),
        };
        // Interface names are limited to 15 bytes.
        let near_dev = format!("la{}", std::process::id() % 100_000);
        let storage_dev = format!("lb{}", std::process::id() % 100_000);
        let (near, storage) = (topology.near.clone(), topology.storage.clone());
        let (near_addr, storage_addr) = (topology.near_addr.clone(), topology.storage_addr.clone());
        sh(&["netns", "add", &near]);
        sh(&["netns", "add", &storage]);
        sh(&[
            "link",
            "add",
            &near_dev,
            "type",
            "veth",
            "peer",
            "name",
            &storage_dev,
        ]);
        sh(&["link", "set", &near_dev, "netns", &near]);
        sh(&["link", "set", &storage_dev, "netns", &storage]);
        for (namespace, dev, addr) in [
            (&near, &near_dev, &near_addr),
            (&storage, &storage_dev, &storage_addr),
        ] {
            sh(&["netns", "exec", namespace, "ip", "link", "set", "lo", "up"]);
            sh(&[
                "netns",
                "exec",
                namespace,
                "ip",
                "addr",
                "add",
                &format!("{addr}/24"),
                "dev",
                dev,
            ]);
            sh(&["netns", "exec", namespace, "ip", "link", "set", dev, "up"]);
            // A few milliseconds each way and a rate low enough that a multi-megabyte fetch
            // stays in flight long enough to interrupt it.
            sh(&[
                "netns", "exec", namespace, "tc", "qdisc", "add", "dev", dev, "root", "netem",
                "delay", "8ms", "rate", "20mbit",
            ]);
        }
        topology
    }
}

impl Drop for Topology {
    fn drop(&mut self) {
        for namespace in [&self.near, &self.storage] {
            let _ = Command::new("ip")
                .args(["netns", "del", namespace])
                .status();
        }
    }
}

fn sh(args: &[&str]) {
    let output = Command::new("ip")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("ip {args:?}: {error}"));
    assert!(
        output.status.success(),
        "ip {args:?} failed: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
}

// ------------------------------------------------------------------------------- TLS

struct Tls {
    ca: PathBuf,
    server_cert: PathBuf,
    server_key: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

fn make_tls(base: &Path, server_ips: &[IpAddr]) -> Tls {
    let paths = Tls {
        ca: base.join("ca.pem"),
        server_cert: base.join("server.pem"),
        server_key: base.join("server-key.pem"),
        client_cert: base.join("client.pem"),
        client_key: base.join("client-key.pem"),
    };
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
    for ip in server_ips {
        server_params
            .subject_alt_names
            .push(SanType::IpAddress(*ip));
    }
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

// ------------------------------------------------------------------------------- managers

const STORAGE_PEER: u16 = 7443;
const STORAGE_DATA: u16 = 7444;

fn storage_args(base: &Path, instance: &str, address: &str, tls: &Tls) -> Vec<String> {
    let name = instance;
    vec![
        "--name".into(),
        format!("{name}-storage"),
        "--root".into(),
        base.join("root-storage").display().to_string(),
        "--nvme".into(),
        "256M".into(),
        "--memory".into(),
        "64M".into(),
        "--staging".into(),
        "32M".into(),
        "--socket".into(),
        base.join("storage.sock").display().to_string(),
        "--listen-peer".into(),
        format!("{address}:{STORAGE_PEER}"),
        "--listen-data".into(),
        format!("{address}:{STORAGE_DATA}"),
        // The harness emulates the link with tc, so the bulk data must use TCP rather than
        // auto-selecting RDMA and bypassing the shaped veth.
        "--peer-transport".into(),
        "stream".into(),
        // One stream connection so the shaped veth link, not parallelism, sets the rate.
        "--stream-connections".into(),
        "1".into(),
        "--tls-cert".into(),
        tls.server_cert.display().to_string(),
        "--tls-key".into(),
        tls.server_key.display().to_string(),
        "--tls-ca".into(),
        tls.ca.display().to_string(),
    ]
}

fn near_args(base: &Path, instance: &str, address: &str, tls: &Tls) -> Vec<String> {
    let name = instance;
    vec![
        "--name".into(),
        format!("{name}-near"),
        "--memory".into(),
        "64M".into(),
        "--staging".into(),
        "32M".into(),
        "--socket".into(),
        base.join("near.sock").display().to_string(),
        "--upstream".into(),
        format!("storage={address}:{STORAGE_PEER}"),
        "--peer-transport".into(),
        "stream".into(),
        "--stream-connections".into(),
        "1".into(),
        "--tls-cert".into(),
        tls.client_cert.display().to_string(),
        "--tls-key".into(),
        tls.client_key.display().to_string(),
        "--tls-ca".into(),
        tls.ca.display().to_string(),
    ]
}

struct Pair {
    tls: Tls,
    /// The manager name prefix reused by a restart.
    instance: String,
    near: Proc,
    storage: Proc,
}

impl Pair {
    fn start(
        base: &Path,
        label: &str,
        storage_addr: &str,
        near_ns: &str,
        storage_ns: &str,
    ) -> Self {
        let tls = make_tls(
            base,
            &[storage_addr.parse().unwrap(), Ipv4Addr::LOCALHOST.into()],
        );
        let instance = instance_name(label);
        let storage = Proc::spawn(
            Some(storage_ns),
            &storage_args(base, &instance, storage_addr, &tls),
            &base.join("storage.sock"),
            &base.join("storage.log"),
        );
        wait_ready(&storage);
        let near = Proc::spawn(
            Some(near_ns),
            &near_args(base, &instance, storage_addr, &tls),
            &base.join("near.sock"),
            &base.join("near.log"),
        );
        wait_ready(&near);
        Self {
            tls,
            instance,
            near,
            storage,
        }
    }

    async fn client(&self, manager: &Proc, memory: usize) -> Client {
        let config = StoreConfig::dram_only(manager.socket.clone(), memory);
        Client::connect(&config, descriptor()).await.unwrap()
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        self.near.kill();
        self.storage.kill();
    }
}

// ------------------------------------------------------------------------------- admin

async fn admin(path: &Path) -> proto::admin_client::AdminClient<tonic::transport::Channel> {
    use tonic::transport::Endpoint;
    let path = path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .unwrap();
    proto::admin_client::AdminClient::new(channel)
}

fn namespace_id() -> Vec<u8> {
    descriptor().namespace_id().to_vec()
}

/// Prometheus series summed across label sets, by series name.
fn stats(socket: &Path) -> HashMap<String, f64> {
    runtime().block_on(async {
        let text = admin(socket)
            .await
            .stats(proto::StatsRequest {})
            .await
            .unwrap()
            .into_inner()
            .prometheus;
        let mut values = HashMap::new();
        for line in text.lines() {
            let Some((series, value)) = line.split_once(' ') else {
                continue;
            };
            let name = series.split('{').next().unwrap_or(series);
            if let Ok(value) = value.trim().parse::<f64>() {
                *values.entry(name.to_string()).or_insert(0.0) += value;
            }
        }
        values
    })
}

fn metric(socket: &Path, name: &str) -> f64 {
    *stats(socket).get(name).unwrap_or(&0.0)
}

fn purge(socket: &Path) -> u64 {
    runtime().block_on(async {
        admin(socket)
            .await
            .purge(proto::PurgeRequest {
                namespace_id: namespace_id(),
                all_namespaces: false,
            })
            .await
            .unwrap()
            .into_inner()
            .records
    })
}

fn set_quota(socket: &Path, dram: u64, nvme: u64) -> u64 {
    runtime().block_on(async {
        admin(socket)
            .await
            .set_quota(proto::QuotaRequest {
                namespace_id: namespace_id(),
                dram_bytes: dram,
                nvme_bytes: nvme,
            })
            .await
            .unwrap()
            .into_inner()
            .blocked_bytes
    })
}

/// The catalog of one manager after its snapshot.
fn catalog(socket: &Path) -> HashMap<Key, Entry> {
    runtime().block_on(async {
        let config = StoreConfig::dram_only(socket.to_path_buf(), 4 << 20);
        let mut client = Client::connect(&config, descriptor()).await.unwrap();
        client.subscribe().await.entries
    })
}

fn wait_for(
    mut condition: impl FnMut() -> bool,
    what: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    while !condition() {
        if Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}"));
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

/// Store, retrying while a namespace still drains the extents retired by a purge.
async fn store_after_purge(client: &mut Client, key: Key, parent: Option<Key>) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match client.put(key, parent, PutOptions::default()).await {
            PutOutcome::Stored(bytes) => return bytes,
            PutOutcome::Refused(reason)
                if reason == proto::Reason::Queue as u8 && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            other => panic!("store failed for {key:?} parent {parent:?}: {other:?}"),
        }
    }
}

// ------------------------------------------------------------------------------- tests

/// Forwarding, remote reads, peer loss during a fetch, purge propagation and quotas over a
/// delayed, rate-limited link.
#[test]
#[ignore = "needs root, CAP_NET_ADMIN, ip and tc"]
fn peer_l4_stream_over_an_emulated_network() {
    let _serial = SERIAL.lock().unwrap_or_else(|error| error.into_inner());
    assert!(
        identity_is_root(),
        "this harness changes network namespaces; run it as root"
    );
    let base = base("l4-stream");
    std::fs::create_dir_all(&base).unwrap();
    let topology = Topology::create("stream");
    let mut pair = Pair::start(
        &base,
        "l4-stream",
        &topology.storage_addr,
        &topology.near,
        &topology.storage,
    );
    let mut checks = Checks::new();

    // 1. A record stored on the near manager is forwarded to the storage manager.
    let root = key(128);
    let child = key(192);
    let stored_root = runtime().block_on(async {
        let mut near = pair.client(&pair.near, 16 << 20).await;
        near.store(root, None).await
    });
    let stored_child = runtime().block_on(async {
        let mut near = pair.client(&pair.near, 16 << 20).await;
        near.store(child, Some(root)).await
    });
    checks.check(
        "forward",
        wait_for(
            || {
                let entries = catalog(&pair.storage.socket);
                entries.contains_key(&root) && entries.contains_key(&child)
            },
            "the storage catalog to gain the forwarded chain",
            Duration::from_secs(30),
        )
        .map(|_| "near -> storage, two records".to_string()),
    );
    checks.check(
        "forward-bytes",
        runtime().block_on(async {
            let mut storage = pair.client(&pair.storage, 16 << 20).await;
            let root_outcome = storage
                .get(&[root], 0, record_pages(&root, None), GetOptions::default())
                .await;
            let child_outcome = storage
                .get(
                    &[child],
                    0,
                    record_pages(&child, Some(&root)),
                    GetOptions::default(),
                )
                .await;
            match (root_outcome, child_outcome) {
                (GetOutcome::Done(root_records), GetOutcome::Done(child_records))
                    if root_records == vec![stored_root] && child_records == vec![stored_child] =>
                {
                    Ok("stored bytes match the near side".into())
                }
                other => Err(format!("storage Get returned {other:?}")),
            }
        }),
    );
    println!("INFO forward certificates: {}", pair.tls.ca.exists());

    // 2. A record that only the storage manager holds is fetched through the link.
    let remote = key(320);
    let remote_bytes = runtime().block_on(async {
        let mut storage = pair.client(&pair.storage, 16 << 20).await;
        storage.store(remote, None).await
    });
    checks.check(
        "remote-catalog",
        wait_for(
            || catalog(&pair.near.socket).contains_key(&remote),
            "the near catalog to list the upstream record",
            Duration::from_secs(30),
        )
        .map(|_| "near catalog lists the storage record".to_string()),
    );
    let fetched = metric(&pair.near.socket, "cm_remote_fetched_total");
    checks.check(
        "remote-get",
        runtime().block_on(async {
            let mut near = pair.client(&pair.near, 16 << 20).await;
            match near
                .get(
                    &[remote],
                    0,
                    record_pages(&remote, None),
                    GetOptions::default(),
                )
                .await
            {
                GetOutcome::Done(records) if records == vec![remote_bytes] => {
                    Ok("fetched over the emulated link".into())
                }
                other => Err(format!("near Get returned {other:?}")),
            }
        }),
    );
    let fetched_after = metric(&pair.near.socket, "cm_remote_fetched_total");
    checks.check(
        "remote-charge",
        if fetched_after == fetched + 1.0 {
            Ok(format!("remote_fetched_total {fetched} -> {fetched_after}"))
        } else {
            Err(format!(
                "remote_fetched_total {fetched} -> {fetched_after}, expected one fetch"
            ))
        },
    );

    // 3. Killing the storage manager in the middle of a multi-megabyte fetch fails the Get
    //    promptly and leaves the near manager serving. The record stays under 16 MiB so the
    //    near manager's power-of-two DRAM extent for it is at most 32 MiB and fits the 64 MiB
    //    arena alongside the records it already holds.
    let big = selene_manager::Key {
        consumed_tokens: 64 * 4000,
        digest: [9; 16],
    };
    runtime().block_on(async {
        let mut storage_client = pair.client(&pair.storage, 16 << 20).await;
        storage_client.store(big, None).await;
    });
    checks.check(
        "loss-setup",
        wait_for(
            || catalog(&pair.near.socket).contains_key(&big),
            "the near catalog to list the large record",
            Duration::from_secs(30),
        )
        .map(|_| "large record listed before the kill".to_string()),
    );
    let (sender, receiver) = mpsc::channel();
    let socket = pair.near.socket.clone();
    let page_count = record_pages(&big, None);
    let started = Instant::now();
    let transfer = thread::spawn(move || {
        let config = StoreConfig::dram_only(socket, 96 << 20);
        let outcome = runtime().block_on(async {
            let mut client = Client::connect(&config, descriptor()).await.unwrap();
            client
                .get(&[big], 0, page_count, GetOptions::default())
                .await
        });
        let _ = sender.send(outcome);
    });
    thread::sleep(Duration::from_millis(1500));
    pair.storage.kill();
    let outcome = receiver.recv_timeout(Duration::from_secs(20));
    let elapsed = started.elapsed();
    let _ = transfer.join();
    checks.check(
        "peer-loss-mid-transfer",
        match outcome {
            Ok(GetOutcome::Done(_)) => Err(format!(
                "the Get completed after {elapsed:?} although the storage manager was killed"
            )),
            Ok(other) => Ok(format!(
                "Get ended after {elapsed:?} without a record: {other:?}"
            )),
            Err(_) => Err("the Get did not end within 20 s of the kill".into()),
        },
    );
    checks.check(
        "near-still-serves",
        runtime().block_on(async {
            let mut near = pair.client(&pair.near, 16 << 20).await;
            let fresh = key(384);
            let bytes = near.store(fresh, None).await;
            match near
                .get(
                    &[fresh],
                    0,
                    record_pages(&fresh, None),
                    GetOptions::default(),
                )
                .await
            {
                GetOutcome::Done(records) if records == vec![bytes] => {
                    Ok("the near manager stores and serves after the loss".into())
                }
                other => Err(format!("near Get returned {other:?}")),
            }
        }),
    );

    // 4. A restart brings the storage manager back; a purge there propagates as removals.
    let storage = Proc::spawn(
        Some(&topology.storage),
        &storage_args(&base, &pair.instance, &topology.storage_addr, &pair.tls),
        &base.join("storage.sock"),
        &base.join("storage.log"),
    );
    wait_ready(&storage);
    // The restart recovers the durable records and offers them again before the purge.
    checks.check(
        "restart-recovery",
        wait_for(
            || {
                let entries = catalog(&pair.near.socket);
                entries.contains_key(&remote) && entries.contains_key(&big)
            },
            "the near catalog to list the recovered storage records",
            Duration::from_secs(30),
        )
        .map(|_| "recovered records are listed again".to_string()),
    );
    let purged = purge(&storage.socket);
    checks.check(
        "purge-propagation",
        wait_for(
            || {
                let entries = catalog(&pair.near.socket);
                // `big` was never adopted locally, so the upstream removal drops it here.
                // `remote` was fetched into near DRAM by the remote Get above; an upstream
                // purge clears its remote source (tier REMOTE) but the local copy stays
                // cached, so it may remain listed as a local record.
                !entries.contains_key(&big)
                    && entries.get(&remote).is_none_or(|entry| entry.tier != 2)
            },
            "the near catalog to drop the upstream-only record and its remote source",
            Duration::from_secs(30),
        )
        .map(|_| format!("purged {purged} records on the storage manager")),
    );
    checks.check(
        "purge-miss",
        runtime().block_on(async {
            let mut near = pair.client(&pair.near, 16 << 20).await;
            match near
                .get(&[big], 0, record_pages(&big, None), GetOptions::default())
                .await
            {
                GetOutcome::Done(_) => Err("the purged record was still served".into()),
                other => Ok(format!("miss after purge: {other:?}")),
            }
        }),
    );

    // 5. Quotas: a cap that fits refuses the next publication and a reduction frees room.
    let _ = purge(&storage.socket);
    let baseline = metric(&storage.socket, "cm_nvme_reserved_bytes");
    let first = key(512);
    runtime().block_on(async {
        let mut storage_client = pair.client(&storage, 16 << 20).await;
        store_after_purge(&mut storage_client, first, None).await;
    });
    let charge = metric(&storage.socket, "cm_nvme_reserved_bytes") - baseline;
    checks.check(
        "quota-measure",
        if charge > 0.0 {
            Ok(format!("one record is charged {charge} bytes"))
        } else {
            Err("the storage manager charged nothing for a stored record".into())
        },
    );
    checks.check("quota-set", {
        let blocked = set_quota(&storage.socket, 0, charge as u64);
        if blocked == 0 {
            Ok(format!("quota {} with no blocked bytes", charge as u64))
        } else {
            Err(format!("{blocked} bytes were blocked by the new quota"))
        }
    });
    checks.check(
        "quota-refuses",
        runtime().block_on(async {
            let mut storage_client = pair.client(&storage, 16 << 20).await;
            match storage_client
                .put(key(576), None, PutOptions::default())
                .await
            {
                PutOutcome::Refused(reason) if reason == proto::Reason::Quota as u8 => {
                    Ok("the over-quota Put was refused with QUOTA".into())
                }
                other => Err(format!("over-quota Put returned {other:?}")),
            }
        }),
    );
    checks.check("quota-reduce", {
        let blocked = set_quota(&storage.socket, 0, (charge / 2.0) as u64);
        let records = metric(&storage.socket, "cm_records");
        if blocked == 0 && records == 0.0 {
            Ok("reduced to zero records".to_string())
        } else {
            Err(format!("blocked={blocked}, records={records}"))
        }
    });

    checks.finish();
    drop(storage);
    drop(pair);
}

fn identity_is_root() -> bool {
    Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .is_some_and(|uid| uid.trim() == "0")
}
