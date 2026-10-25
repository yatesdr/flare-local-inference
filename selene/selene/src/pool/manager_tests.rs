//! The client against the real manager, in process over a Unix socket: two
//! instances sharing records through shared memory, a put into the manager's
//! store, characterization, and a manager that restarts.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    History, HostRegistrar, InboundTicket, PageId, PrefixKey, Registration, Result, Scratch,
    SplitMapping, StateSlotId, StoreEvent, StoreOutcome, StoreStats,
    characterize::Ranking,
    geometry::fingerprint,
    plan::{self, Identity, Request},
    pool::{
        Checksums, FetchPart, HostChain, HostPin, HostRecord, HostWindow, IndexEntry, LookupState,
        PoolAddress, PoolClient, PoolConfig, PoolEvent, PoolFetch, PoolStatus, PooledStore,
        PutBytes, PutRequest, PutSource, STARTUP_WAIT, Scope, Tier,
    },
    record::checksum,
    testing::{GpuMemory, SimulatedGpu, TestHistory, geometry},
};

use selene_manager::{control, store::Config};

use crate::{
    CacheGeometry, CheckpointId, HostStore, Layout, PublishedCheckpoint,
    composite::CompositeStore,
    disk::DiskThread,
    disk_store::{DiskStore, Staged},
    memory::MemoryStore,
    record,
    slab::SlabPool,
};

pub(super) const PT: u64 = 16;
const PAGE: usize = 8192;
const ROW: usize = 4096;

static SERIAL: AtomicU32 = AtomicU32::new(0);

pub(super) struct OkRegistrar;
struct Registered;
impl Registration for Registered {
    fn device_address(&self) -> u64 {
        0
    }
}
impl HostRegistrar for OkRegistrar {
    fn register(&self, _: usize, _: usize, _: usize) -> Result<Box<dyn Registration>> {
        Ok(Box::new(Registered))
    }
    fn map_split(&self, _: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
        Err("not used".into())
    }
}

/// A manager that also listens for instances on other hosts, on loopback.
struct Network {
    config: selene_manager::config::Config,
    /// The certificate to trust, when the manager serves TLS.
    ca: Option<PathBuf>,
}

/// A loopback address nobody listens on. Taken from below the ephemeral range and handed out
/// once, so concurrent tests never share one or meet a connection's own port.
fn free_address() -> std::net::SocketAddr {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let port = 20_000
            + (std::process::id() % 2_000) * 4
            + NEXT.fetch_add(1, Ordering::Relaxed) % 4_000;
        let address = std::net::SocketAddr::from(([127, 0, 0, 1], port as u16));
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
}

pub(super) struct Manager {
    pub(super) socket: PathBuf,
    base: PathBuf,
    config: Config,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    network: Option<Network>,
}

impl Manager {
    pub(super) fn start() -> Self {
        Self::with_memory(16 << 20)
    }

    /// A manager whose store holds `memory` bytes.
    fn with_memory(memory: usize) -> Self {
        let id = SERIAL.fetch_add(1, Ordering::Relaxed);
        let base =
            std::env::temp_dir().join(format!("selene-client-it-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("run")).unwrap();
        let socket = base.join("run/m.sock");
        let mut config = Config::dram_only(socket.clone(), memory);
        config.name = format!("selene-client-it-{}-{id}", std::process::id());
        // Small and explicit: many managers run at once in a small /dev/shm.
        config.staging = 16 << 20;
        let mut manager = Self {
            socket,
            base,
            config,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
            network: None,
        };
        manager.run();
        manager
    }

    /// A manager reached over TCP, with TLS when `tls`.
    pub(super) fn start_network(tls: bool) -> Self {
        Self::start_network_with(tls, false)
    }

    /// As [`Manager::start_network`], also offering RDMA when `rdma`.
    fn start_network_with(tls: bool, rdma: bool) -> Self {
        let mut manager = Self::start();
        manager.stop();
        let _ = std::fs::remove_file(&manager.socket);
        let free = free_address;
        let mut net = selene_manager::config::Config::defaults();
        net.name = manager.config.name.clone();
        net.socket = manager.socket.clone();
        net.socket_explicit = true;
        net.memory = 16 << 20;
        net.staging = 8 << 20;
        net.listen = Some(free());
        net.listen_data = Some(free());
        net.metrics = Some(free());
        net.rdma = rdma;
        let mut ca = None;
        if tls {
            let generated =
                rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
            let (cert, key) = (
                manager.base.join("server.pem"),
                manager.base.join("server-key.pem"),
            );
            std::fs::write(&cert, generated.cert.pem()).unwrap();
            std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
            net.tls_cert = Some(cert.clone());
            net.tls_key = Some(key);
            ca = Some(cert);
        }
        manager.network = Some(Network { config: net, ca });
        manager.run();
        manager
    }

    /// What the data channel has moved so far, as `(windows, bytes)` by carrier, read from the
    /// manager's metrics.
    fn moved(&self, carrier: &str) -> (u64, u64) {
        use std::io::{Read, Write};
        let address = self.network.as_ref().unwrap().config.metrics.unwrap();
        let mut stream = std::net::TcpStream::connect(address).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut text = String::new();
        stream.read_to_string(&mut text).unwrap();
        let value = |name: &str| {
            text.lines()
                .find_map(|line| {
                    line.strip_prefix(&format!(
                        "selene_pool_channel_{name}_total{{carrier=\"{carrier}\"}} "
                    ))
                })
                .and_then(|value| value.trim().parse().ok())
                .unwrap_or(0)
        };
        (value("windows"), value("bytes"))
    }

    /// Where an instance on another host reaches the manager, and the CA it trusts.
    pub(super) fn remote(&self) -> Option<(PoolAddress, Option<PathBuf>)> {
        let network = self.network.as_ref()?;
        let port = network.config.listen.unwrap().port();
        let host = if network.ca.is_some() {
            "localhost"
        } else {
            "127.0.0.1"
        };
        let address = format!("{host}:{port}");
        Some((
            if network.ca.is_some() {
                PoolAddress::Tcp(address)
            } else {
                PoolAddress::TcpPlaintext(address)
            },
            network.ca.clone(),
        ))
    }

    fn run(&mut self) {
        self.stop.store(false, Ordering::Release);
        let (config, stop) = (self.config.clone(), self.stop.clone());
        let network = self.network.as_ref().map(|n| n.config.clone());
        self.thread = Some(thread::spawn(move || {
            let result = match network {
                Some(net) => control::serve(net, config, stop),
                None => control::run(config, stop),
            };
            if let Err(error) = result {
                eprintln!("manager exited: {error}");
            }
        }));
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.socket.exists() {
            assert!(Instant::now() < deadline, "the manager did not start");
            thread::sleep(Duration::from_millis(5));
        }
        if let Some((PoolAddress::Tcp(address) | PoolAddress::TcpPlaintext(address), _)) =
            self.remote()
        {
            let port = address.rsplit_once(':').unwrap().1.to_string();
            while std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_err() {
                assert!(Instant::now() < deadline, "the manager did not listen");
                thread::sleep(Duration::from_millis(5));
            }
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// Restart the manager on the same socket.
    pub(super) fn restart(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.socket);
        self.run();
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.base);
        // The manager keeps its region files across restarts; a test is done with them.
        let _ = std::fs::remove_dir_all(
            std::path::Path::new("/dev/shm/selene").join(&self.config.name),
        );
    }
}

fn geo() -> Vec<crate::CacheGeometry> {
    vec![geometry(PT, &[("k", 4096), ("v", 4096)], 4096)]
}

fn config(manager: &Manager, name: &str, gpu: u8) -> PoolConfig {
    config_on(manager, name, gpu, false)
}

/// As [`config`]; `local` reaches a network manager over its socket, as an instance on the
/// manager's own host does.
fn config_on(manager: &Manager, name: &str, gpu: u8, local: bool) -> PoolConfig {
    let geometry = geo();
    let (address, ca) = manager
        .remote()
        .filter(|_| !local)
        .unwrap_or_else(|| (PoolAddress::Unix(manager.socket.clone()), None));
    PoolConfig {
        address,
        tenant: "t".into(),
        ca,
        scope: Scope::new("t", [7; 32], fingerprint(&geometry), &geometry).unwrap(),
        instance_name: name.into(),
        gpu_uuids: vec![[gpu; 16]],
        export_slots: 2,
        claim_slots: 2,
        level_order: None,
        registration_context: 0,
        approve: None,
    }
}

struct Instance {
    client: PoolClient,
    gpu: GpuMemory,
    events: Vec<PoolEvent>,
}

fn instance(manager: &Manager, name: &str, gpu: u8) -> Instance {
    instance_on(manager, name, gpu, false)
}

fn instance_on(manager: &Manager, name: &str, gpu: u8, local: bool) -> Instance {
    let (copier, memory) = SimulatedGpu::shared(&geo()).unwrap();
    let client = PoolClient::start(
        config_on(manager, name, gpu, local),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    Instance {
        client,
        gpu: memory,
        events: Vec::new(),
    }
}

pub(super) fn wait(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(5));
    }
}

impl Instance {
    fn ready(&self) {
        wait("Ready", || self.client.status() == PoolStatus::Ready);
    }

    fn poll(&mut self) {
        let mut events = Vec::new();
        self.client.poll(&mut events).unwrap();
        self.events.extend(events);
    }

    /// Poll until `take` finds an event, removing it.
    fn event<T>(&mut self, what: &str, mut take: impl FnMut(&PoolEvent) -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            self.poll();
            if let Some(position) = self.events.iter().position(|e| take(e).is_some()) {
                let event = self.events.remove(position);
                return take(&event).unwrap();
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            thread::sleep(Duration::from_millis(2));
        }
    }
}

fn history() -> TestHistory {
    history_of(4000)
}

pub(super) fn history_of(tokens: u32) -> TestHistory {
    let mut history = TestHistory::new([7; 32]);
    history
        .append(&(0..tokens).collect::<Vec<u32>>(), &[])
        .unwrap();
    history
}

fn identity() -> Identity {
    Identity {
        tenant: "t".into(),
        model: [7; 32],
        layout: fingerprint(&geo()),
    }
}

/// A host pin over known bytes, serving the checksums it was given.
pub(super) struct TestPin {
    chain: HostChain,
    data: Vec<u8>,
    sums: Vec<u64>,
    row: u64,
}

impl HostPin for TestPin {
    fn chain(&self) -> &HostChain {
        &self.chain
    }
    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums> {
        let from = window.first_page as usize * PAGE;
        let n = window.page_count as usize * PAGE;
        dst[..n].copy_from_slice(&self.data[from..from + n]);
        if window.row {
            dst[n..].copy_from_slice(&self.data[self.data.len() - ROW..]);
        }
        Ok(Checksums {
            pages: self.sums[window.first_page as usize..][..window.page_count as usize].to_vec(),
            row: window.row.then_some(self.row),
        })
    }
}

fn pin_for(key: PrefixKey, generation: u64, pages: usize) -> Box<dyn HostPin> {
    let data: Vec<u8> = (0..pages * PAGE + ROW)
        .map(|i| (i * 13 + 5) as u8)
        .collect();
    let sums = (0..pages)
        .map(|p| checksum(&data[p * PAGE..(p + 1) * PAGE]))
        .collect();
    let row = checksum(&data[pages * PAGE..]);
    Box::new(TestPin {
        chain: HostChain {
            records: vec![HostRecord {
                key,
                generation,
                first_page: 0,
                pages: pages as u32,
                row: true,
            }],
            total_pages: pages as u64,
            row: true,
        },
        data,
        sums,
        row,
    })
}

fn expected_page(page: usize) -> Vec<u8> {
    (page * PAGE..(page + 1) * PAGE)
        .map(|i| (i * 13 + 5) as u8)
        .collect()
}

/// Plan a restore of `history` from what the lookup answered, as the store does.
fn fetch_for(
    instance: &mut Instance,
    history: &TestHistory,
    pages: usize,
    scratch: &[u32],
) -> PoolFetch {
    let ticket = instance.client.begin_lookup(history, Vec::new());
    wait("the lookup", || {
        !matches!(ticket.state(), LookupState::Pending)
    });
    let LookupState::Ready(answer) = ticket.state() else {
        panic!("the lookup has no answer");
    };
    assert!(
        !answer.candidates.is_empty(),
        "the manager offered a candidate"
    );
    let ranking = Ranking::default();
    let request = Request {
        history,
        identity: &identity(),
        page_tokens: PT,
        resident: 0,
        before: u64::MAX,
        ranking: &ranking,
        prefill_tokens_per_second: 8000.0,
    };
    let plan = plan::plan(&request, &answer.plan_candidates()).expect("a plan");
    assert_eq!(plan.pages(), pages);
    let part = plan.a.clone();
    let candidate = answer
        .candidates
        .iter()
        .find(|c| c.plan.source == part.source && c.plan.generation == part.generation)
        .unwrap()
        .clone();
    PoolFetch {
        id: 0,
        page_tokens: PT,
        parts: vec![FetchPart {
            candidate,
            part,
            pages: scratch.iter().map(|p| PageId(*p)).collect(),
            slot: Some(StateSlotId(9)),
        }],
    }
}

#[test]
fn an_instance_restores_a_neighbours_host_record_through_the_real_manager() {
    let manager = Manager::start();
    let mut source = instance(&manager, "source", 1);
    let mut requester = instance(&manager, "requester", 2);
    source.ready();
    requester.ready();
    let history = history();
    let pages = 4usize;
    let key = history.key_at(pages as u64 * PT).unwrap();
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Dram,
        generation: 11,
        bytes: (pages * PAGE + ROW) as u64,
    });
    // The manager learns of it; the requester's lookup then finds it.
    wait("the manager to offer the record", || {
        let ticket = requester.client.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let scratch = [100, 101, 102, 103];
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    // The source is asked to export; its store (here, the test) pins the chain.
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some((request.export, request.tier, request.generation)),
        _ => None,
    });
    assert_eq!((export.1, export.2), (Tier::Dram, 11));
    source.client.host_pinned(export.0, pin_for(key, 11, pages));
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * PAGE + ROW) as u64));
    let gpu = requester.gpu.pages.lock().unwrap();
    for page in 0..pages {
        assert_eq!(
            gpu[&(100 + page as u32)],
            expected_page(page),
            "page {page}"
        );
    }
    assert_eq!(
        requester.gpu.rows.lock().unwrap()[&9],
        (pages * PAGE..pages * PAGE + ROW)
            .map(|i| (i * 13 + 5) as u8)
            .collect::<Vec<u8>>()
    );
    drop(gpu);
}

/// Bytes for a put, with the checksums they carry.
struct Bytes {
    data: Vec<u8>,
    sums: Vec<u64>,
    row: u64,
}

impl PutBytes for Bytes {
    fn fill(
        &mut self,
        first_page: u64,
        page_count: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        let from = first_page as usize * PAGE;
        let n = page_count as usize * PAGE;
        dst[..n].copy_from_slice(&self.data[from..from + n]);
        if row {
            dst[n..].copy_from_slice(&self.data[self.data.len() - ROW..]);
        }
        Ok(Checksums {
            pages: self.sums[first_page as usize..][..page_count as usize].to_vec(),
            row: row.then_some(self.row),
        })
    }
}

fn put_request(history: &TestHistory, pages: usize) -> (PutRequest, PrefixKey) {
    let key = history.key_at(pages as u64 * PT).unwrap();
    let data: Vec<u8> = (0..pages * PAGE + ROW)
        .map(|i| (i * 13 + 5) as u8)
        .collect();
    let sums = (0..pages)
        .map(|p| checksum(&data[p * PAGE..(p + 1) * PAGE]))
        .collect();
    let row = checksum(&data[pages * PAGE..]);
    (
        PutRequest {
            key,
            parent: None,
            bucket: history.bucket_at(key.consumed_tokens).unwrap(),
            chain_pages: pages as u64,
            tail_pages: pages as u64,
            row: true,
            generation: 1,
            source: PutSource::Host(Box::new(Bytes { data, sums, row })),
        },
        key,
    )
}

fn restore(requester: &mut Instance, history: &TestHistory, pages: usize, scratch: &[u32]) {
    let fetch = fetch_for(requester, history, pages, scratch);
    requester.client.fetch(fetch).unwrap();
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * PAGE + ROW) as u64));
    let gpu = requester.gpu.pages.lock().unwrap();
    for page in 0..pages {
        assert_eq!(gpu[&scratch[page]], expected_page(page), "page {page}");
    }
}

#[test]
fn a_record_put_into_the_manager_store_is_restored_by_another_instance() {
    let manager = Manager::start();
    let mut writer = instance(&manager, "writer", 1);
    let mut requester = instance(&manager, "requester", 2);
    writer.ready();
    requester.ready();
    let history = history();
    let (request, _) = put_request(&history, 5);
    writer.client.put(request).unwrap();
    let outcome = writer.event("the put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert!(outcome.starts_with("Stored"), "{outcome}");
    restore(&mut requester, &history, 5, &[200, 201, 202, 203, 204]);
    // The same record put again is a successful deduplication.
    let (request, _) = put_request(&history, 5);
    writer.client.put(request).unwrap();
    let outcome = writer.event("the second put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert_eq!(outcome, "Present");
}

#[test]
fn a_gpu_checkpoint_is_exported_by_device_copies_and_its_pin_released() {
    let manager = Manager::start();
    let mut source = instance(&manager, "source", 1);
    let mut requester = instance(&manager, "requester", 2);
    source.ready();
    requester.ready();
    let history = history();
    let pages = 3usize;
    let key = history.key_at(pages as u64 * PT).unwrap();
    // The checkpoint's pages on the source's device.
    for page in 0..pages {
        source
            .gpu
            .pages
            .lock()
            .unwrap()
            .insert(20 + page as u32, expected_page(page));
    }
    let row: Vec<u8> = (pages * PAGE..pages * PAGE + ROW)
        .map(|i| (i * 13 + 5) as u8)
        .collect();
    source.gpu.rows.lock().unwrap().insert(5, row.clone());
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Gpu,
        generation: 5,
        bytes: (pages * PAGE + ROW) as u64,
    });
    wait("the manager to offer it", || {
        let ticket = requester.client.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let fetch = fetch_for(&mut requester, &history, pages, &[300, 301, 302]);
    assert_eq!(
        fetch.parts[0].candidate.plan.level,
        crate::characterize::Level::NeighbourGpu
    );
    requester.client.fetch(fetch).unwrap();
    // The worker's side: pin the chain and answer, then release at the end.
    let (export, reserved) = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => {
            assert_eq!(
                (request.tier, request.generation, request.key),
                (Tier::Gpu, 5, key)
            );
            Some((request.export, request.reserved_bytes))
        }
        _ => None,
    });
    assert!(reserved >= (pages * PAGE + ROW) as u64);
    source.client.export_pinned(
        export,
        crate::ExportChain {
            nodes: vec![crate::ExportNode {
                key,
                generation: 5,
                pages: (20..20 + pages as u32).map(PageId).collect(),
            }],
            skip_pages: 0,
            row: Some(StateSlotId(5)),
        },
    );
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * PAGE + ROW) as u64));
    for page in 0..pages {
        assert_eq!(
            requester.gpu.pages.lock().unwrap()[&(300 + page as u32)],
            expected_page(page)
        );
    }
    assert_eq!(requester.gpu.rows.lock().unwrap()[&9], row);
    source.event("the pin to be released", |e| match e {
        PoolEvent::ExportEnded { export: ended } if *ended == export => Some(()),
        _ => None,
    });
}

#[test]
fn announcements_survive_a_manager_restart_and_the_instances_reconnect() {
    let mut manager = Manager::start();
    let mut source = instance(&manager, "source", 1);
    let mut requester = instance(&manager, "requester", 2);
    source.ready();
    requester.ready();
    let history = history();
    let key = history.key_at(4 * PT).unwrap();
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: 4,
        row: true,
        tier: Tier::Dram,
        generation: 3,
        bytes: (4 * PAGE + ROW) as u64,
    });
    let offered = |requester: &Instance| {
        let ticket = requester.client.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    };
    wait("the record to be offered", || offered(&requester));
    manager.restart();
    // Both instances fall back to their local tiers and come back.
    wait("the instances to reconnect", || {
        source.client.status() == PoolStatus::Ready
            && requester.client.status() == PoolStatus::Ready
    });
    // The source announced from its own index again under the same id.
    wait("the record to be offered again", || offered(&requester));
    source.client.close().unwrap();
    requester.client.close().unwrap();
}

/// Counts what is registered, and refuses nothing.
#[derive(Default)]
struct CountingRegistrar {
    calls: std::sync::Mutex<Vec<(usize, usize)>>,
}
impl HostRegistrar for CountingRegistrar {
    fn register(&self, _: usize, address: usize, bytes: usize) -> Result<Box<dyn Registration>> {
        self.calls.lock().unwrap().push((address, bytes));
        Ok(Box::new(Registered))
    }
    fn map_split(&self, _: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
        Err("not used".into())
    }
}

fn counted(manager: &Manager, registrar: &Arc<CountingRegistrar>, wait: Duration) -> PoolClient {
    counted_approving(manager, registrar, wait, None)
}

fn counted_approving(
    manager: &Manager,
    registrar: &Arc<CountingRegistrar>,
    wait: Duration,
    approve: Option<super::Approve>,
) -> PoolClient {
    let (copier, _gpu) = SimulatedGpu::shared(&geo()).unwrap();
    let mut config = config(manager, "counted", 1);
    config.approve = approve;
    PoolClient::start_waiting(
        config,
        None,
        copier,
        registrar.clone(),
        super::net::Roots::System,
        wait,
    )
    .unwrap()
}

fn inode(manager: &Manager, file: &str) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(
        std::path::Path::new("/dev/shm/selene")
            .join(&manager.config.name)
            .join(file),
    )
    .unwrap()
    .ino()
}

#[test]
fn a_manager_restart_reuses_the_regions_and_the_client_registers_nothing_again() {
    let mut manager = Manager::start();
    let registrar = Arc::new(CountingRegistrar::default());
    let client = counted(&manager, &registrar, STARTUP_WAIT);
    let regions = client.registered_regions().to_vec();
    assert_eq!(
        regions.iter().map(|r| r.name).collect::<Vec<_>>(),
        ["arena", "staging"],
        "the startup registered the manager's two regions"
    );
    assert_eq!(registrar.calls.lock().unwrap().len(), 2);
    wait("Ready", || client.status() == PoolStatus::Ready);
    let (before, files) = (
        client.topology(),
        (inode(&manager, "arena"), inode(&manager, "staging")),
    );

    manager.restart();
    wait("a session with the restarted manager", || {
        client.topology() != before && client.status() == PoolStatus::Ready
    });
    assert_eq!(
        (inode(&manager, "arena"), inode(&manager, "staging")),
        files,
        "the manager reopened the same files"
    );
    assert_eq!(
        registrar.calls.lock().unwrap().len(),
        2,
        "the reconnect registered nothing"
    );
    assert_eq!(client.registered_regions(), regions);
}

/// An approval that records each plan with the registrations made before it, and says `verdict`.
fn recording(
    registrar: &Arc<CountingRegistrar>,
    verdict: std::result::Result<(), String>,
) -> (
    super::Approve,
    Arc<std::sync::Mutex<Vec<(Vec<super::PlannedRegion>, usize)>>>,
) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (log, registrar) = (seen.clone(), registrar.clone());
    let approve: super::Approve = Arc::new(move |plan| {
        let before = registrar.calls.lock().unwrap().len();
        log.lock().unwrap().push((plan.to_vec(), before));
        verdict.clone()
    });
    (approve, seen)
}

#[test]
fn the_plan_arrives_whole_before_any_registration_and_a_reconnect_asks_nothing() {
    let mut manager = Manager::start();
    let registrar = Arc::new(CountingRegistrar::default());
    let (approve, seen) = recording(&registrar, Ok(()));
    let client = counted_approving(&manager, &registrar, STARTUP_WAIT, Some(approve));
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let (plan, registered_before) = &seen[0];
        assert_eq!(*registered_before, 0, "approved before the first register");
        assert_eq!(
            plan.iter().map(|r| r.name).collect::<Vec<_>>(),
            ["arena", "staging"]
        );
        let sizes: Vec<_> = client
            .registered_regions()
            .iter()
            .map(|r| r.bytes as u64)
            .collect();
        assert_eq!(plan.iter().map(|r| r.bytes).collect::<Vec<_>>(), sizes);
    }
    wait("Ready", || client.status() == PoolStatus::Ready);
    let before = client.topology();
    manager.restart();
    wait("a session with the restarted manager", || {
        client.topology() != before && client.status() == PoolStatus::Ready
    });
    assert_eq!(seen.lock().unwrap().len(), 1, "the reconnect asked nothing");
    assert_eq!(registrar.calls.lock().unwrap().len(), 2);
}

#[test]
fn a_rejected_plan_registers_nothing_and_turns_the_pool_off() {
    let manager = Manager::start();
    let registrar = Arc::new(CountingRegistrar::default());
    let (approve, seen) = recording(&registrar, Err("over the host budget".into()));
    let mut client = counted_approving(&manager, &registrar, STARTUP_WAIT, Some(approve));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(registrar.calls.lock().unwrap().is_empty());
    assert!(client.registered_regions().is_empty());
    let PoolStatus::Disabled(reason) = client.status() else {
        panic!("{:?}", client.status());
    };
    assert!(
        reason.contains("over the host budget")
            && reason.contains("arena")
            && reason.contains("staging"),
        "{reason}"
    );
    thread::sleep(Duration::from_millis(500));
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(registrar.calls.lock().unwrap().is_empty());
    client.close().unwrap();
}

#[test]
fn a_changed_staging_size_turns_the_pool_off_for_a_running_instance() {
    let mut manager = Manager::start();
    let registrar = Arc::new(CountingRegistrar::default());
    let mut client = counted(&manager, &registrar, STARTUP_WAIT);
    wait("Ready", || client.status() == PoolStatus::Ready);

    manager.stop();
    manager.config.staging *= 2;
    let _ = std::fs::remove_file(&manager.socket);
    manager.run();
    wait("the instance to notice", || {
        matches!(client.status(), PoolStatus::Disabled(_))
    });
    let PoolStatus::Disabled(reason) = client.status() else {
        unreachable!()
    };
    assert!(
        reason.contains("recreated its shared memory") && reason.contains("restarted"),
        "{reason}"
    );
    thread::sleep(Duration::from_millis(500));
    assert!(matches!(client.status(), PoolStatus::Disabled(_)));
    assert_eq!(
        registrar.calls.lock().unwrap().len(),
        2,
        "nothing was registered late"
    );
    client.close().unwrap();
}

#[test]
fn a_manager_that_is_not_there_at_startup_is_never_attached_to_later() {
    let mut manager = Manager::start();
    manager.stop();
    let _ = std::fs::remove_file(&manager.socket);
    let registrar = Arc::new(CountingRegistrar::default());
    let started = Instant::now();
    let mut client = counted(&manager, &registrar, Duration::from_millis(400));
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(client.registered_regions().is_empty());
    let PoolStatus::Disabled(reason) = client.status() else {
        panic!("{:?}", client.status());
    };
    assert!(reason.contains("local tiers only"), "{reason}");

    // The manager comes up after the instance gave up on it.
    manager.run();
    thread::sleep(Duration::from_secs(1));
    assert!(matches!(client.status(), PoolStatus::Disabled(_)));
    assert!(registrar.calls.lock().unwrap().is_empty());
    client.close().unwrap();
}

struct NoStore;

impl crate::HostStore for NoStore {
    fn lookup(&mut self, _: &dyn History, _: u64, _: u64) -> Option<crate::HostHit> {
        None
    }
    fn store(&mut self, _: &crate::PublishedCheckpoint) -> Result<StoreOutcome> {
        Ok(StoreOutcome::Oversize)
    }
    fn fetch(&mut self, _: crate::HostHit, _: InboundTicket) -> Result<()> {
        Err("no tiers".into())
    }
    fn poll(&mut self, _: &mut Vec<StoreEvent>) -> Result<()> {
        Ok(())
    }
    fn stats(&self) -> StoreStats {
        StoreStats::default()
    }
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn a_store_with_no_tiers_of_its_own_times_the_manager_level_through_a_real_claim() {
    let manager = Manager::start();
    let (copier, _gpu) = SimulatedGpu::shared(&geo()).unwrap();
    let mut store = PooledStore::connect(
        NoStore,
        config(&manager, "probe", 1),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);
    let scratch = Scratch {
        pages: (0..64).map(PageId).collect(),
        slot: StateSlotId(0),
    };
    assert!(store.needs_characterization());
    let line = store.characterize(&scratch).unwrap().expect("measured");
    assert!(line.contains("restore source order"), "{line}");
    let ranking = store.pool().ranking();
    assert!(
        ranking
            .us_per_page(crate::characterize::Level::HostDram)
            .is_some(),
        "the manager serves its DRAM here: {line}"
    );
    assert!(
        ranking
            .us_per_page(crate::characterize::Level::NeighbourGpu)
            .is_none()
    );
    assert!(!store.needs_characterization());
    crate::HostStore::close(&mut store).unwrap();
}

// ---- the eviction cascade against the real manager ----

/// A history of its own: every record published from one is a root.
fn salted(salt: u32) -> TestHistory {
    let mut history = TestHistory::new([7; 32]);
    let tokens: Vec<u32> = (0..4000u32)
        .map(|t| t.wrapping_mul(2654435761).wrapping_add(salt))
        .collect();
    history.append(&tokens, &[]).unwrap();
    history
}

fn salted_page(salt: u32, page: usize) -> Vec<u8> {
    (0..PAGE)
        .map(|i| ((i * 13 + page * 7 + salt as usize * 31 + 5) % 251) as u8)
        .collect()
}

fn salted_row(salt: u32) -> Vec<u8> {
    (0..ROW)
        .map(|i| ((i * 11 + salt as usize * 17 + 3) % 251) as u8)
        .collect()
}

/// The checkpoint of `pages` pages that record `salt` publishes, with its
/// bytes placed in the simulated device first.
fn publication(gpu: &GpuMemory, salt: u32, pages: usize) -> (PublishedCheckpoint, TestHistory) {
    let history = salted(salt);
    let base = salt * 100;
    for page in 0..pages {
        gpu.pages
            .lock()
            .unwrap()
            .insert(base + page as u32, salted_page(salt, page));
    }
    gpu.rows.lock().unwrap().insert(salt, salted_row(salt));
    let key = history.key_at(pages as u64 * PT).unwrap();
    let checkpoint = PublishedCheckpoint {
        id: CheckpointId::new(u64::from(salt)),
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        chain_pages: (base..base + pages as u32).map(PageId).collect(),
        tail_pages: pages,
        slot: StateSlotId(salt),
    };
    (checkpoint, history)
}

/// Publish as the model worker does: store, poll until the copy-out is
/// reported, and try again after a refusal or a deferral. The device cache
/// then evicts the checkpoint, as it does under pressure.
fn publish<S: HostStore>(store: &mut S, checkpoint: &PublishedCheckpoint) {
    store_and_wait(store, checkpoint);
    store.evicted(checkpoint.key, checkpoint.id.value(), None);
}

fn store_and_wait<S: HostStore>(store: &mut S, checkpoint: &PublishedCheckpoint) {
    let deadline = Instant::now() + Duration::from_secs(20);
    'attempt: loop {
        let outcome = store.store(checkpoint).unwrap();
        if outcome == StoreOutcome::Present {
            return;
        }
        assert_ne!(outcome, StoreOutcome::Oversize);
        let mut events = Vec::new();
        while Instant::now() < deadline {
            store.poll(&mut events).unwrap();
            let mut retry = outcome != StoreOutcome::Queued;
            for event in events.drain(..) {
                match event {
                    StoreEvent::Stored { key, result, .. } if key == checkpoint.key => {
                        result.unwrap();
                        if outcome == StoreOutcome::Queued {
                            return;
                        }
                    }
                    StoreEvent::Deferred { checkpoint: id } if id == checkpoint.id => retry = true,
                    _ => {}
                }
            }
            thread::sleep(Duration::from_millis(2));
            if retry {
                continue 'attempt;
            }
        }
        panic!("timed out publishing {:?}", checkpoint.key);
    }
}

/// Poll `store` until `done`.
fn drive<S: HostStore>(store: &mut S, what: &str, mut done: impl FnMut(&mut S) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut events = Vec::new();
    while !done(store) {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        store.poll(&mut events).unwrap();
        events.clear();
        thread::sleep(Duration::from_millis(2));
    }
}

fn offered(requester: &Instance, history: &TestHistory) -> bool {
    let ticket = requester.client.begin_lookup(history, Vec::new());
    let deadline = Instant::now() + Duration::from_secs(2);
    while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(2));
    }
    matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
}

/// Restore the record `salt` of `pages` pages from the manager's store into
/// the requester and check every byte.
fn restore_salted(
    requester: &mut Instance,
    history: &TestHistory,
    salt: u32,
    pages: usize,
    evictor: &mut impl HostStore,
) {
    // The evictor's own notices must reach the manager first; it is polled
    // as the worker would.
    wait("the manager's store to be the only source", || {
        evictor.poll(&mut Vec::new()).unwrap();
        let ticket = requester.client.begin_lookup(history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        let key = history.key_at(pages as u64 * PT).unwrap();
        matches!(ticket.state(), LookupState::Ready(a)
            if a.candidates.iter().any(|c| c.plan.key == key && c.source.instance.is_empty())
                && a.candidates.iter().all(|c| c.plan.key != key || c.source.instance.is_empty()))
    });
    let scratch: Vec<u32> = (0..pages as u32).map(|p| 700 + salt * 10 + p).collect();
    let fetch = fetch_for(requester, history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * PAGE + ROW) as u64));
    let gpu = requester.gpu.pages.lock().unwrap();
    for (page, id) in scratch.iter().enumerate() {
        assert_eq!(
            gpu[id],
            salted_page(salt, page),
            "record {salt} page {page}"
        );
    }
    drop(gpu);
    assert_eq!(requester.gpu.rows.lock().unwrap()[&9], salted_row(salt));
}

/// A pooled store over a standalone memory store of `pages` pages, with its
/// own simulated device.
fn memory_instance(manager: &Manager, pages: usize) -> (PooledStore<MemoryStore>, GpuMemory) {
    let (transfers, gpu) = SimulatedGpu::shared(&geo()).unwrap();
    let pool = SlabPool::new(PAGE as u64, pages, ROW as u64, pages, true).unwrap();
    let memory = MemoryStore::new([7; 32], pool, transfers).unwrap();
    let (copier, _unused) = SimulatedGpu::shared(&geo()).unwrap();
    let store = PooledStore::connect(
        memory,
        config(manager, "evictor", 1),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);
    (store, gpu)
}

#[test]
fn an_evicted_dram_record_reaches_the_manager_store_and_is_restored_by_another_instance() {
    let manager = Manager::start();
    let mut requester = instance(&manager, "requester", 2);
    requester.ready();
    let (mut store, gpu) = memory_instance(&manager, 8);
    let (first, first_history) = publication(&gpu, 1, 4);
    let (second, second_history) = publication(&gpu, 2, 4);
    let (third, third_history) = publication(&gpu, 3, 4);
    publish(&mut store, &first);
    publish(&mut store, &second);
    // The manager hears of both once their checksums are done.
    drive(&mut store, "the records to be announced", |_| {
        offered(&requester, &first_history) && offered(&requester, &second_history)
    });

    // The third needs the room: the coldest record moves to the manager first.
    publish(&mut store, &third);
    drive(&mut store, "the first record to leave", |store| {
        store.lookup(&first_history, 0, u64::MAX).is_none()
    });
    assert!(store.lookup(&second_history, 0, u64::MAX).is_some());
    assert!(store.lookup(&third_history, 0, u64::MAX).is_some());
    // Another instance restores it from the manager's store.
    restore_salted(&mut requester, &first_history, 1, 4, &mut store);
    crate::HostStore::close(&mut store).unwrap();
}

#[test]
fn a_record_the_manager_already_holds_is_released_without_waiting_for_a_copy() {
    let manager = Manager::start();
    let mut writer = instance(&manager, "writer", 3);
    writer.ready();
    let (mut store, gpu) = memory_instance(&manager, 8);
    let (first, first_history) = publication(&gpu, 1, 4);
    let (second, second_history) = publication(&gpu, 2, 4);
    let (third, _) = publication(&gpu, 3, 4);

    // Another instance already put the very record into the manager's store.
    let data: Vec<u8> = (0..4)
        .flat_map(|page| salted_page(1, page))
        .chain(salted_row(1))
        .collect();
    let sums = (0..4)
        .map(|p| checksum(&data[p * PAGE..(p + 1) * PAGE]))
        .collect();
    let row = checksum(&data[4 * PAGE..]);
    writer
        .client
        .put(PutRequest {
            key: first.key,
            parent: None,
            bucket: first.bucket,
            chain_pages: 4,
            tail_pages: 4,
            row: true,
            generation: 1,
            source: PutSource::Host(Box::new(Bytes { data, sums, row })),
        })
        .unwrap();
    let outcome = writer.event("the put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert!(outcome.starts_with("Stored"), "{outcome}");

    publish(&mut store, &first);
    publish(&mut store, &second);
    drive(&mut store, "the records to be announced", |_| {
        offered(&writer, &second_history)
    });
    publish(&mut store, &third);
    drive(&mut store, "the first record to leave", |store| {
        store.lookup(&first_history, 0, u64::MAX).is_none()
    });
    crate::HostStore::close(&mut store).unwrap();
}

#[test]
fn a_record_the_manager_refuses_is_dropped_and_not_retried() {
    // A manager whose store cannot hold a 20-page record at all.
    let manager = Manager::with_memory(160 * 1024);
    let requester = instance(&manager, "requester", 2);
    requester.ready();
    let (mut store, gpu) = memory_instance(&manager, 24);
    let (big, big_history) = publication(&gpu, 1, 20);
    let (small, small_history) = publication(&gpu, 2, 4);
    let (third, _) = publication(&gpu, 3, 4);
    publish(&mut store, &big);
    publish(&mut store, &small);
    drive(&mut store, "the records to be announced", |_| {
        offered(&requester, &big_history) && offered(&requester, &small_history)
    });
    // The pool is full: the coldest record, the big one, is offered to the
    // manager, which refuses it. It is dropped here and the publication goes on.
    publish(&mut store, &third);
    drive(&mut store, "the big record to leave", |store| {
        store.lookup(&big_history, 0, u64::MAX).is_none()
    });
    assert!(store.lookup(&small_history, 0, u64::MAX).is_some());
    drive(&mut store, "the big record to be forgotten", |_| {
        !offered(&requester, &big_history)
    });
    crate::HostStore::close(&mut store).unwrap();
}

#[test]
fn without_a_manager_the_tier_evicts_by_itself_and_publication_goes_on() {
    let mut manager = Manager::start();
    let (mut store, gpu) = memory_instance(&manager, 8);
    let records: Vec<_> = (1..=4).map(|salt| publication(&gpu, salt, 4)).collect();
    publish(&mut store, &records[0].0);
    publish(&mut store, &records[1].0);
    manager.stop();
    drive(&mut store, "the pool to be lost", |store| {
        store.pool().status() != PoolStatus::Ready
    });
    // The pool is full and the manager is gone: nothing waits for a copy.
    publish(&mut store, &records[2].0);
    publish(&mut store, &records[3].0);
    assert!(store.lookup(&records[0].1, 0, u64::MAX).is_none());
    assert!(store.lookup(&records[3].1, 0, u64::MAX).is_some());
    crate::HostStore::close(&mut store).unwrap();
}

/// A directory for the disk tier that takes direct I/O, or `None`.
fn offload_directory(test: &str) -> Option<PathBuf> {
    let base = std::env::var_os("FLARE_OFFLOAD_TEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let directory = base.join(format!(
        "flare-offload-cascade-{test}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let probe = directory.join(".probe");
    match flare_eos::direct_file(&probe, true) {
        Ok(_) => {
            std::fs::remove_file(&probe).unwrap();
            Some(directory)
        }
        Err(error) if cfg!(unix) && error.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!(
                "skipping {test}: {} refuses direct I/O",
                directory.display()
            );
            std::fs::remove_dir_all(&directory).unwrap();
            None
        }
        Err(error) => panic!("probe {}: {error}", probe.display()),
    }
}

#[test]
fn a_record_cascades_from_memory_to_the_disk_tier_to_the_manager_store() {
    let Some(directory) = offload_directory("composite") else {
        return;
    };
    let manager = Manager::start();
    let mut requester = instance(&manager, "requester", 2);
    requester.ready();
    let geometry: Vec<CacheGeometry> = geo();
    let layout = Layout::new(&geometry).unwrap();
    let fingerprint = fingerprint(&geometry);
    // Memory holds two records of two pages; the disk tier, two files.
    let (memory_transfers, gpu) = SimulatedGpu::shared(&geometry).unwrap();
    let pool = SlabPool::new(PAGE as u64, 4, ROW as u64, 4, true).unwrap();
    let memory = MemoryStore::new_flat([7; 32], pool, memory_transfers).unwrap();
    let lock = record::DirectoryLock::acquire(&directory).unwrap();
    let scanned = record::scan(&lock, &[7; 32], &fingerprint, &geometry).unwrap();
    let file_bytes = record::Header::new(
        &geometry,
        [7; 32],
        fingerprint,
        salted(1).key_at(2 * PT).unwrap(),
        None,
        [0; 16],
        2,
        std::time::SystemTime::now(),
    )
    .unwrap()
    .length_with_row();
    let (disk_transfers, _) = SimulatedGpu::shared(&geometry).unwrap();
    let disk = DiskStore::new(
        lock,
        [7; 32],
        fingerprint,
        2 * file_bytes + file_bytes / 2,
        geometry,
        scanned,
        Box::new(Staged::new(
            Some(SlabPool::staging(&layout).unwrap()),
            disk_transfers,
            DiskThread::start("cascade-test-disk").unwrap(),
        )),
    )
    .unwrap();
    let composite = CompositeStore::new(memory, disk).unwrap();
    let (copier, _unused) = SimulatedGpu::shared(&geo()).unwrap();
    let mut store = PooledStore::connect(
        composite,
        config(&manager, "evictor", 1),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);

    let (first, first_history) = publication(&gpu, 1, 2);
    let (second, _) = publication(&gpu, 2, 2);
    let (third, _) = publication(&gpu, 3, 2);
    let (fourth, _) = publication(&gpu, 4, 2);
    // Memory and the disk tier fill: nothing is written until memory needs room.
    publish(&mut store, &first);
    publish(&mut store, &second);
    assert!(!directory.join(record::file_name(&first.key)).exists());
    // The third pushes the first to disk; the fourth finds the disk tier full
    // and pushes the coldest file on to the manager.
    publish(&mut store, &third);
    publish(&mut store, &fourth);
    drive(&mut store, "the first record's file to go", |_| {
        !directory.join(record::file_name(&first.key)).exists()
    });
    restore_salted(&mut requester, &first_history, 1, 2, &mut store);
    crate::HostStore::close(&mut store).unwrap();
    drop(store);
    let _ = std::fs::remove_dir_all(&directory);
}

// ---- instances on other hosts: the data channel ---------------------------------

/// Put a record through `writer`, restore it through `requester`, and check the level
/// the lookup gave it.
fn put_then_restore(manager: &Manager, carrier: &str) {
    let mut writer = instance(manager, "writer", 1);
    let mut requester = instance(manager, "requester", 2);
    writer.ready();
    requester.ready();
    // Neither instance has shared memory with the manager: no neighbour is offered.
    let history = history();
    let (request, _) = put_request(&history, 5);
    writer.client.put(request).unwrap();
    let outcome = writer.event("the put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert!(outcome.starts_with("Stored"), "{outcome}");
    let scratch = [200, 201, 202, 203, 204];
    let fetch = fetch_for(&mut requester, &history, 5, &scratch);
    assert_eq!(
        fetch.parts[0].candidate.plan.level,
        crate::characterize::Level::RemoteNvme,
        "a manager on another host is the remote level"
    );
    requester.client.fetch(fetch).unwrap();
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((5 * PAGE + ROW) as u64));
    let gpu = requester.gpu.pages.lock().unwrap();
    for page in 0..5 {
        assert_eq!(gpu[&scratch[page]], expected_page(page), "page {page}");
    }
    assert_eq!(
        requester.gpu.rows.lock().unwrap()[&9],
        (5 * PAGE..5 * PAGE + ROW)
            .map(|i| (i * 13 + 5) as u8)
            .collect::<Vec<u8>>()
    );
    drop(gpu);
    // The same record again is a successful deduplication, and a second restore works.
    let (request, _) = put_request(&history, 5);
    writer.client.put(request).unwrap();
    let outcome = writer.event("the second put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert_eq!(outcome, "Present");
    restore(&mut requester, &history, 5, &[300, 301, 302, 303, 304]);
    writer.client.close().unwrap();
    requester.client.close().unwrap();
    // One put and two restores of five pages and a row went by this carrier and no other.
    let (windows, bytes) = manager.moved(carrier);
    assert!(windows >= 3, "{carrier}: {windows} windows");
    assert!(
        bytes >= 3 * (5 * PAGE + ROW) as u64,
        "{carrier}: {bytes} bytes"
    );
    let other = if carrier == "stream" {
        "rdma"
    } else {
        "stream"
    };
    assert_eq!(manager.moved(other), (0, 0), "nothing went by {other}");
}

#[test]
fn a_record_put_into_a_manager_on_another_host_is_restored_over_tcp() {
    let manager = Manager::start_network(false);
    put_then_restore(&manager, "stream");
}

#[test]
fn a_record_put_into_a_manager_on_another_host_is_restored_over_tls() {
    let manager = Manager::start_network(true);
    put_then_restore(&manager, "stream");
}

#[cfg(feature = "rdma")]
#[test]
fn a_record_put_into_a_manager_on_another_host_is_restored_over_rdma() {
    let manager = Manager::start_network_with(false, true);
    put_then_restore(&manager, "rdma");
}

#[test]
fn the_remote_level_is_timed_by_the_probe_through_a_real_claim() {
    let manager = Manager::start_network(false);
    let (copier, _gpu) = SimulatedGpu::shared(&geo()).unwrap();
    let mut store = PooledStore::connect(
        NoStore,
        config(&manager, "probe", 1),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);
    let scratch = Scratch {
        pages: (0..64).map(PageId).collect(),
        slot: StateSlotId(0),
    };
    assert!(store.needs_characterization());
    let line = store.characterize(&scratch).unwrap().expect("measured");
    let ranking = store.pool().ranking();
    assert!(
        ranking
            .us_per_page(crate::characterize::Level::RemoteNvme)
            .is_some(),
        "the manager's records are the remote level: {line}"
    );
    assert!(
        ranking
            .us_per_page(crate::characterize::Level::HostDram)
            .is_none(),
        "a manager on another host serves no same-host level: {line}"
    );
    assert!(!store.needs_characterization());
    crate::HostStore::close(&mut store).unwrap();
}

#[test]
fn instances_on_other_hosts_come_back_and_copy_after_the_manager_restarts() {
    let mut manager = Manager::start_network(false);
    let mut writer = instance(&manager, "writer", 1);
    writer.ready();
    let history = history();
    let (request, _) = put_request(&history, 3);
    writer.client.put(request).unwrap();
    writer.event("the put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    manager.restart();
    wait("the instance to reconnect", || {
        writer.client.status() == PoolStatus::Ready
    });
    // The restarted manager starts empty: the put works again, and a second instance restores it.
    let (request, _) = put_request(&history, 3);
    writer.client.put(request).unwrap();
    let outcome = writer.event("the put after the restart", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert!(outcome.starts_with("Stored"), "{outcome}");
    let mut requester = instance(&manager, "requester", 2);
    requester.ready();
    restore(&mut requester, &history, 3, &[400, 401, 402]);
    writer.client.close().unwrap();
    requester.client.close().unwrap();
}

/// Pages that fill the manager's window exactly leave the row to a window of its own.
fn a_window_of_pages_with_the_row_alone(manager: &Manager) {
    let mut writer = instance(manager, "writer", 1);
    let mut requester = instance(manager, "requester", 2);
    writer.ready();
    requester.ready();
    // The manager's windows hold 4 MiB: 512 pages of 8 KiB fill one, and the row follows alone.
    let pages = 512usize;
    let history = history_of(pages as u32 * PT as u32 + 64);
    let (request, _) = put_request(&history, pages);
    writer.client.put(request).unwrap();
    let outcome = writer.event("the put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert!(outcome.starts_with("Stored"), "{outcome}");
    let scratch: Vec<u32> = (1000..1000 + pages as u32).collect();
    restore(&mut requester, &history, pages, &scratch);
    writer.client.close().unwrap();
    requester.client.close().unwrap();
}

#[test]
fn a_row_in_a_window_of_its_own_is_put_and_restored_over_tcp() {
    a_window_of_pages_with_the_row_alone(&Manager::start_network(false));
}

#[test]
fn a_row_in_a_window_of_its_own_is_put_and_restored_through_shared_memory() {
    a_window_of_pages_with_the_row_alone(&Manager::start());
}

/// The round trip of a lookup, from asking to a ready answer, against the real manager
/// over a Unix socket with an index to scan: the figure the worker's admission hold is
/// set from. Run with `-- --nocapture` to see the numbers.
#[test]
fn a_lookup_is_answered_in_well_under_a_millisecond_over_a_unix_socket() {
    let manager = Manager::start();
    let a = instance(&manager, "a", 1);
    a.ready();
    // Noise the scan must skip, and holdings the first lookups find.
    for n in 0..5000u32 {
        a.client.saved(IndexEntry {
            key: PrefixKey {
                consumed_tokens: 64 * u64::from(n % 50 + 1),
                digest: [
                    (n % 251) as u8,
                    (n / 251) as u8,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                    9,
                ],
            },
            parent: None,
            bucket: [
                (n % 251) as u8,
                (n / 251) as u8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
                8,
            ],
            tail_pages: 1,
            row: true,
            tier: Tier::Gpu,
            generation: u64::from(n) + 1,
            bytes: 1,
        });
    }
    let mut b = instance(&manager, "b", 2);
    b.ready();
    thread::sleep(Duration::from_millis(300));
    let mut times = Vec::new();
    for salt in 0..2000u32 {
        let history = salted(salt);
        let started = Instant::now();
        let ticket = b.client.begin_lookup(&history, Vec::new());
        while matches!(ticket.state(), LookupState::Pending) {
            std::hint::spin_loop();
        }
        times.push(started.elapsed());
        assert!(!matches!(ticket.state(), LookupState::Pending));
    }
    times.sort();
    let at = |q: f64| times[((times.len() as f64 * q) as usize).min(times.len() - 1)];
    eprintln!(
        "lookup round trip over {} lookups: p50 {:?}, p90 {:?}, p99 {:?}, p99.9 {:?}, max {:?}",
        times.len(),
        at(0.5),
        at(0.9),
        at(0.99),
        at(0.999),
        times[times.len() - 1]
    );
    b.poll();
    assert!(at(0.99) < Duration::from_millis(5), "p99 {:?}", at(0.99));
}

// ---- copies between instances through the manager ---------------------------------

/// A host pin that holds back every window after the first until the gate opens, and says
/// when it was dropped.
struct StallPin {
    inner: TestPin,
    gate: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

impl HostPin for StallPin {
    fn chain(&self) -> &HostChain {
        self.inner.chain()
    }
    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums> {
        let deadline = Instant::now() + Duration::from_secs(20);
        while window.first_page > 0 && !self.gate.load(Ordering::Acquire) {
            if Instant::now() > deadline {
                return Err("the gate never opened".into());
            }
            thread::sleep(Duration::from_millis(5));
        }
        self.inner.read(window, dst)
    }
}

impl Drop for StallPin {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

/// `source` saves a host record of `pages` pages; the manager offers it to `requester`.
fn offer_host_record(
    source: &Instance,
    requester: &Instance,
    history: &TestHistory,
    pages: usize,
    generation: u64,
) -> PrefixKey {
    let key = history.key_at(pages as u64 * PT).unwrap();
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Dram,
        generation,
        bytes: (pages * PAGE + ROW) as u64,
    });
    wait("the manager to offer the record", || {
        let ticket = requester.client.begin_lookup(history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    key
}

fn finish_restore(requester: &mut Instance) -> std::result::Result<u64, String> {
    requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    })
}

/// The pages and row a restore of `pages` pages of `expected_page` put on `requester`'s device.
fn check_restored(requester: &Instance, scratch: &[u32], pages: usize) {
    let gpu = requester.gpu.pages.lock().unwrap();
    for page in 0..pages {
        assert_eq!(gpu[&scratch[page]], expected_page(page), "page {page}");
    }
    assert_eq!(
        requester.gpu.rows.lock().unwrap()[&9],
        (pages * PAGE..pages * PAGE + ROW)
            .map(|i| (i * 13 + 5) as u8)
            .collect::<Vec<u8>>()
    );
}

/// A host record of `pages` pages moves from `source` to `requester` through the manager,
/// whichever of them are on the manager's host.
fn relay_host_record(manager: &Manager, source_local: bool, requester_local: bool, pages: usize) {
    let mut source = instance_on(manager, "source", 1, source_local);
    let mut requester = instance_on(manager, "requester", 2, requester_local);
    source.ready();
    requester.ready();
    let history = history_of(pages as u32 * PT as u32 + 64);
    let key = offer_host_record(&source, &requester, &history, pages, 11);
    let scratch: Vec<u32> = (0..pages as u32).map(|p| 5000 + p).collect();
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    source.client.host_pinned(export, pin_for(key, 11, pages));
    assert_eq!(
        finish_restore(&mut requester),
        Ok((pages * PAGE + ROW) as u64)
    );
    check_restored(&requester, &scratch, pages);
    source.client.close().unwrap();
    requester.client.close().unwrap();
}

#[test]
fn a_host_record_is_relayed_between_two_instances_on_other_hosts_over_tcp() {
    let manager = Manager::start_network(false);
    // Two windows of a 4 MiB manager window each way.
    relay_host_record(&manager, false, false, 700);
    let (windows, bytes) = manager.moved("stream");
    assert!(windows >= 4, "{windows} windows crossed the manager twice");
    assert!(bytes >= 2 * (700 * PAGE + ROW) as u64, "{bytes} bytes");
}

#[cfg(feature = "rdma")]
#[test]
fn a_host_record_is_relayed_between_two_instances_on_other_hosts_over_rdma() {
    let manager = Manager::start_network_with(false, true);
    relay_host_record(&manager, false, false, 700);
    let (windows, bytes) = manager.moved("rdma");
    assert!(windows >= 4, "{windows} windows crossed the manager twice");
    assert!(bytes >= 2 * (700 * PAGE + ROW) as u64, "{bytes} bytes");
    assert_eq!(
        manager.moved("stream"),
        (0, 0),
        "nothing went by the stream"
    );
}

#[test]
fn a_host_record_is_relayed_between_two_instances_on_other_hosts_over_tls() {
    let manager = Manager::start_network(true);
    relay_host_record(&manager, false, false, 5);
}

#[test]
fn a_remote_instance_serves_an_instance_on_the_managers_host() {
    let manager = Manager::start_network(false);
    relay_host_record(&manager, false, true, 700);
    let (windows, _) = manager.moved("stream");
    assert!(windows >= 2, "{windows} windows came in from the source");
}

#[test]
fn an_instance_on_the_managers_host_serves_a_remote_instance() {
    let manager = Manager::start_network(false);
    relay_host_record(&manager, true, false, 700);
    let (windows, _) = manager.moved("stream");
    assert!(windows >= 2, "{windows} windows went out to the requester");
}

#[test]
fn a_gpu_checkpoint_on_another_host_is_relayed_through_reserved_staging() {
    let manager = Manager::start_network(false);
    let mut source = instance(&manager, "source", 1);
    let mut requester = instance(&manager, "requester", 2);
    source.ready();
    requester.ready();
    let history = history();
    let pages = 3usize;
    let key = history.key_at(pages as u64 * PT).unwrap();
    for page in 0..pages {
        source
            .gpu
            .pages
            .lock()
            .unwrap()
            .insert(20 + page as u32, expected_page(page));
    }
    let row: Vec<u8> = (pages * PAGE..pages * PAGE + ROW)
        .map(|i| (i * 13 + 5) as u8)
        .collect();
    source.gpu.rows.lock().unwrap().insert(5, row);
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Gpu,
        generation: 5,
        bytes: (pages * PAGE + ROW) as u64,
    });
    wait("the manager to offer it", || {
        let ticket = requester.client.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let scratch = [300, 301, 302];
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let (export, reserved) = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some((request.export, request.reserved_bytes)),
        _ => None,
    });
    assert!(reserved >= (pages * PAGE + ROW) as u64);
    source.client.export_pinned(
        export,
        crate::ExportChain {
            nodes: vec![crate::ExportNode {
                key,
                generation: 5,
                pages: (20..20 + pages as u32).map(PageId).collect(),
            }],
            skip_pages: 0,
            row: Some(StateSlotId(5)),
        },
    );
    assert_eq!(
        finish_restore(&mut requester),
        Ok((pages * PAGE + ROW) as u64)
    );
    check_restored(&requester, &scratch, pages);
    source.event("the pin to be released", |e| match e {
        PoolEvent::ExportEnded { export: ended } if *ended == export => Some(()),
        _ => None,
    });
}

#[test]
fn a_stalled_source_fails_the_relay_and_the_same_record_relays_after() {
    let manager = Manager::start_network(false);
    let pages = 700usize;
    let mut source = instance(&manager, "source", 1);
    let mut requester = instance(&manager, "requester", 2);
    source.ready();
    requester.ready();
    let history = history_of(pages as u32 * PT as u32 + 64);
    let key = offer_host_record(&source, &requester, &history, pages, 11);
    let scratch: Vec<u32> = (0..pages as u32).map(|p| 5000 + p).collect();
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    let gate = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let TestPinParts(inner) = TestPinParts::new(key, 11, pages);
    source.client.host_pinned(
        export,
        Box::new(StallPin {
            inner,
            gate: gate.clone(),
            dropped: dropped.clone(),
        }),
    );
    // The second window never comes: the claim runs out of progress and fails the restore.
    let result = finish_restore(&mut requester);
    assert!(result.is_err(), "{result:?}");
    // The source lets go once its copy can finish, and releases the pin.
    gate.store(true, Ordering::Release);
    wait("the source to release its pin", || {
        source.poll();
        dropped.load(Ordering::Acquire)
    });
    // Nothing was left behind: the same record relays again.
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("a second export request", |e| match e {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    source.client.host_pinned(export, pin_for(key, 11, pages));
    assert_eq!(
        finish_restore(&mut requester),
        Ok((pages * PAGE + ROW) as u64)
    );
    check_restored(&requester, &scratch, pages);
}

/// A `TestPin` built the way `pin_for` builds one.
struct TestPinParts(TestPin);

impl TestPinParts {
    fn new(key: PrefixKey, generation: u64, pages: usize) -> Self {
        let data: Vec<u8> = (0..pages * PAGE + ROW)
            .map(|i| (i * 13 + 5) as u8)
            .collect();
        let sums = (0..pages)
            .map(|p| checksum(&data[p * PAGE..(p + 1) * PAGE]))
            .collect();
        let row = checksum(&data[pages * PAGE..]);
        Self(TestPin {
            chain: HostChain {
                records: vec![HostRecord {
                    key,
                    generation,
                    first_page: 0,
                    pages: pages as u32,
                    row: true,
                }],
                total_pages: pages as u64,
                row: true,
            },
            data,
            sums,
            row,
        })
    }
}
