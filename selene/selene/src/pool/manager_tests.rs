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
        PutBytes, PutRequest, PutSource, Scope, Tier,
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

const PT: u64 = 16;
const PAGE: usize = 8192;
const ROW: usize = 4096;

static SERIAL: AtomicU32 = AtomicU32::new(0);

struct OkRegistrar;
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

struct Manager {
    socket: PathBuf,
    base: PathBuf,
    config: Config,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Manager {
    fn start() -> Self {
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
        let mut manager = Self {
            socket,
            base,
            config,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
        };
        manager.run();
        manager
    }

    fn run(&mut self) {
        self.stop.store(false, Ordering::Release);
        let (config, stop) = (self.config.clone(), self.stop.clone());
        self.thread = Some(thread::spawn(move || {
            if let Err(error) = control::run(config, stop) {
                eprintln!("manager exited: {error}");
            }
        }));
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.socket.exists() {
            assert!(Instant::now() < deadline, "the manager did not start");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// Restart the manager on the same socket.
    fn restart(&mut self) {
        self.stop();
        let _ = std::fs::remove_file(&self.socket);
        self.run();
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

fn geo() -> Vec<crate::CacheGeometry> {
    vec![geometry(PT, &[("k", 4096), ("v", 4096)], 4096)]
}

fn config(manager: &Manager, name: &str, gpu: u8) -> PoolConfig {
    let geometry = geo();
    PoolConfig {
        address: PoolAddress::Unix(manager.socket.clone()),
        tenant: "t".into(),
        ca: None,
        scope: Scope::new("t", [7; 32], fingerprint(&geometry), &geometry).unwrap(),
        instance_name: name.into(),
        gpu_uuids: vec![[gpu; 16]],
        export_slots: 2,
        claim_slots: 2,
        level_order: None,
        registration_context: 0,
    }
}

struct Instance {
    client: PoolClient,
    gpu: GpuMemory,
    events: Vec<PoolEvent>,
}

fn instance(manager: &Manager, name: &str, gpu: u8) -> Instance {
    let (copier, memory) = SimulatedGpu::shared(&geo()).unwrap();
    let client =
        PoolClient::start(config(manager, name, gpu), copier, Arc::new(OkRegistrar)).unwrap();
    Instance {
        client,
        gpu: memory,
        events: Vec::new(),
    }
}

fn wait(what: &str, mut done: impl FnMut() -> bool) {
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
    let mut history = TestHistory::new([7; 32]);
    history
        .append(&(0..4000).collect::<Vec<u32>>(), &[])
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
struct TestPin {
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
    let mut requester = instance(&manager, "requester", 2);
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
    drive(&mut store, "the first record's file to go", |store| {
        !directory.join(record::file_name(&first.key)).exists()
    });
    restore_salted(&mut requester, &first_history, 1, 2, &mut store);
    crate::HostStore::close(&mut store).unwrap();
    drop(store);
    let _ = std::fs::remove_dir_all(&directory);
}
