/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 Flare community contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//! The client against the real manager, in process over a Unix socket: two
//! instances sharing records through shared memory, a put into the manager's
//! store, characterization, and a manager that restarts.

use flare_selene::shared::{Scope, Tier};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use flare_selene::{
    History, PageId, PrefixKey, Result, StateSlotId, StoreEvent, StoreOutcome,
    geometry::fingerprint,
    plan::{self, Identity, Request},
    pool::{
        Checksums, FetchPart, HostChain, HostPin, HostRecord, HostWindow, IndexEntry, LookupState,
        PoolAddress, PoolClient, PoolConfig, PoolEvent, PoolFetch, PoolStatus, PooledStore,
        PutRequest, PutSource, STARTUP_WAIT,
    },
    record::checksum,
    testing::{GpuMemory, SimulatedGpu, TestHistory, geometry},
};

use flare_selene::{
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

use flare_selene::testing::IdentityRegistrar as OkRegistrar;
use flare_selene::testing::TrackingRegistrar as CountingRegistrar;
use selene_manager::testing::free_address;
use selene_manager::testing::{Manager, wait};

fn network_manager(tls: bool) -> Manager {
    network_manager_with(tls, false)
}

fn network_manager_with(tls: bool, rdma: bool) -> Manager {
    let mut manager = Manager::start();
    manager.stop();
    let _ = std::fs::remove_file(&manager.config.socket);
    let free = free_address;
    let net = &mut manager.config;
    net.listen = Some(free());
    net.listen_data = Some(free());
    net.metrics = Some(free());
    net.rdma = rdma;
    #[cfg(feature = "rdma")]
    if rdma {
        // The carrier uses the device on the data connection's route.
        let ip = selene_manager::testing::rdma_address().expect("no local RoCE v2 address");
        net.listen_data = Some(selene_manager::testing::free_address_on(ip));
    }
    if tls {
        let generated = selene_manager::testing::certificate();
        let (cert, key) = (
            manager.base.join("server.pem"),
            manager.base.join("server-key.pem"),
        );
        std::fs::write(&cert, generated.cert.pem()).unwrap();
        std::fs::write(&key, generated.signing_key.serialize_pem()).unwrap();
        net.tls_cert = Some(cert.clone());
        net.tls_key = Some(key);
    }
    manager.run();
    manager
}

fn moved(manager: &Manager, carrier: &str) -> (u64, u64) {
    let text = manager.metrics();
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

fn remote(manager: &Manager) -> Option<(PoolAddress, Option<PathBuf>)> {
    let port = manager.config.listen?.port();
    let ca = manager.config.tls_cert.clone();
    let host = if ca.is_some() {
        "localhost"
    } else {
        "127.0.0.1"
    };
    let address = format!("{host}:{port}");
    Some((
        if ca.is_some() {
            PoolAddress::Tcp(address)
        } else {
            PoolAddress::TcpPlaintext(address)
        },
        ca,
    ))
}

fn geo() -> Vec<flare_selene::CacheGeometry> {
    vec![geometry(PT, &[("k", 4096), ("v", 4096)], 4096)]
}

fn config(manager: &Manager, gpu: u8) -> PoolConfig {
    config_on(manager, gpu, false, &geo())
}

/// As [`config`]; `local` reaches a network manager over its socket, as an instance on the
/// manager's own host does.
fn config_on(manager: &Manager, gpu: u8, local: bool, geometry: &[CacheGeometry]) -> PoolConfig {
    let (address, ca) = remote(manager)
        .filter(|_| !local)
        .unwrap_or_else(|| (PoolAddress::Unix(manager.config.socket.clone()), None));
    PoolConfig {
        address,
        ca,
        scope: Scope::new([7; 32], fingerprint(geometry), geometry).unwrap(),
        gpu_uuids: vec![[gpu; 16]],
        export_slots: 2,
        claim_slots: 2,

        registration_context: 0,
        approve: None,
    }
}

struct Instance {
    client: PoolClient,
    gpu: GpuMemory,
    events: Vec<PoolEvent>,
}

fn instance(manager: &Manager, gpu: u8) -> Instance {
    instance_on(manager, gpu, false, &geo())
}

fn instance_on(manager: &Manager, gpu: u8, local: bool, geometry: &[CacheGeometry]) -> Instance {
    let (copier, memory) = SimulatedGpu::shared(geometry).unwrap();
    let client = PoolClient::start(
        config_on(manager, gpu, local, geometry),
        None,
        copier,
        Arc::new(OkRegistrar),
        flare_selene::pool::Roots::System,
        flare_selene::pool::STARTUP_WAIT,
    )
    .unwrap();
    Instance {
        client,
        gpu: memory,
        events: Vec::new(),
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

fn history_of(tokens: u32) -> TestHistory {
    let mut history = TestHistory::new([7; 32]);
    history
        .append(&(0..tokens).collect::<Vec<u32>>(), &[])
        .unwrap();
    history
}

fn identity(geometry: &[CacheGeometry]) -> Identity {
    Identity {
        model: [7; 32],
        layout: fingerprint(geometry),
    }
}

use flare_selene::testing::{BytesPin as TestPin, RecordBytes as Bytes};

fn pin_for(key: PrefixKey, pages: usize) -> Box<dyn HostPin> {
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
                first_page: 0,
                pages: pages as u32,
                row: true,
            }],
            total_pages: pages as u64,
            row: true,
        },
        bytes: Bytes { data, sums, row, page_bytes: PAGE, row_bytes: ROW },
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
    fetch_for_geometry(instance, history, pages, scratch, &geo())
}

fn fetch_for_geometry(
    instance: &Instance,
    history: &TestHistory,
    pages: usize,
    scratch: &[u32],
    geometry: &[CacheGeometry],
) -> PoolFetch {
    let ticket = instance.client.begin_lookup(history, 0);
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
    let request = Request {
        history,
        identity: &identity(geometry),
        page_tokens: PT,
        resident: 0,
        before: u64::MAX,
    };
    let plan = plan::plan(&request, &answer.plan_candidates()).expect("a plan");
    assert_eq!(plan.pages(), pages);
    let part = plan.part.clone();
    let candidate = answer
        .candidates
        .iter()
        .find(|c| c.plan.source == part.source)
        .unwrap()
        .clone();
    PoolFetch {
        id: 0,
        page_tokens: PT,
        part: FetchPart {
            candidate,
            part,
            pages: scratch.iter().map(|p| PageId(*p)).collect(),
            slot: Some(StateSlotId(9)),
        },
    }
}

#[test]
fn a_client_lookup_includes_an_endpoint_exactly_at_the_prompt_length() {
    let manager = Manager::start();
    let source = instance(&manager, 1);
    let requester = instance(&manager, 2);
    source.ready();
    requester.ready();
    let history = history_of(65);
    let key = history.key_at(history.token_count()).unwrap();
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: 4,
        row: true,
        tier: Tier::Dram,
        bytes: (4 * PAGE + ROW) as u64,
    });
    wait("the exact prompt endpoint", || {
        let ticket = requester.client.begin_lookup(&history, 0);
        wait("the lookup reply", || {
            !matches!(ticket.state(), LookupState::Pending)
        });
        matches!(ticket.state(), LookupState::Ready(answer) if answer.candidates.iter().any(|c| c.plan.key == key))
    });
}

#[test]
fn an_instance_restores_a_neighbours_host_record_through_the_real_manager() {
    let manager = Manager::start();
    let mut source = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
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
        bytes: (pages * PAGE + ROW) as u64,
    });
    // The manager learns of it; the requester's lookup then finds it.
    wait("the manager to offer the record", || {
        let ticket = requester.client.begin_lookup(&history, 0);
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let scratch = [100, 101, 102, 103];
    // Built before the claim starts: the manager's pin deadline runs from the request, and
    // making the data in an unoptimised build on a loaded machine is not what is under test.
    let pin = pin_for(key, pages);
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    // The source is asked to export; its store (here, the test) pins the chain.
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some((request.export, request.tier)),
        _ => None,
    });
    assert_eq!(export.1, Tier::Dram);
    source.client.host_pinned(export.0, pin);
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
            ancestor_keys: Vec::new(),
            key,
            parent: None,
            bucket: history.bucket_at(key.consumed_tokens).unwrap(),
            chain_pages: pages as u64,
            tail_pages: pages as u64,
            capture_role: Default::default(),
            child_paths: 0,
            descendant_row: false,
            next_child: None,
            row: true,
            source: PutSource::Host(Box::new(Bytes { data, sums, row, page_bytes: PAGE, row_bytes: ROW })),
        },
        key,
    )
}

fn wait_store_index(requester: &Instance, history: &TestHistory) {
    // Put completion can precede the asynchronous catalog update of the pool index.
    wait("the manager's store record to be indexed", || {
        offered(requester, history)
    });
}

fn restore(requester: &mut Instance, history: &TestHistory, pages: usize, scratch: &[u32]) {
    let fetch = fetch_for(requester, history, pages, scratch);
    assert_eq!(restore_bytes(requester, fetch), (pages * PAGE + ROW) as u64);
    let gpu = requester.gpu.pages.lock().unwrap();
    for page in 0..pages {
        assert_eq!(gpu[&scratch[page]], expected_page(page), "page {page}");
    }
}

fn restore_bytes(requester: &mut Instance, fetch: PoolFetch) -> u64 {
    requester.client.fetch(fetch).unwrap();
    requester.event("the restore", |event| match event {
        PoolEvent::Fetched { result, .. } => Some(*result.as_ref().unwrap()),
        _ => None,
    })
}

fn busy_source_restores(manager: &Manager) {
    let mut source = instance(manager, 1);
    let mut requesters: Vec<_> = (2..=5).map(|id| instance(manager, id)).collect();
    source.ready();
    let histories: Vec<_> = (1..=4).map(salted).collect();
    let pages = 4;
    let scratch = [100, 101, 102, 103];
    let keys: Vec<_> = requesters
        .iter()
        .zip(&histories)
        .map(|(requester, history)| {
            requester.ready();
            offer_host_record(&source, requester, history, pages)
        })
        .collect();
    let started = Instant::now();
    let deadline = started
        + flare_selene::shared::transfer_timeout(
            flare_selene::testing::RESTORE_DEADLINE,
            (pages * PAGE + ROW) as u64,
        );
    // Occupy both export slots before the other lookups and claims arrive.
    let mut held = Vec::new();
    for i in 0..2 {
        let fetch = fetch_for(&mut requesters[i], &histories[i], pages, &scratch);
        requesters[i].client.fetch(fetch).unwrap();
        held.push(
            source.event("an occupied export slot", |event| match event {
                PoolEvent::Export(request) => Some(request.export),
                _ => None,
            }),
        );
    }
    for i in 2..4 {
        let fetch = fetch_for(&mut requesters[i], &histories[i], pages, &scratch);
        requesters[i].client.fetch(fetch).unwrap();
    }
    wait("both excess claims to encounter the busy source", || {
        manager.metrics().lines().any(|line| {
            line.starts_with("selene_pool_claims_refused_total{reason=\"SourceLimit\"} ")
                && line
                    .split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    >= 2
        })
    });
    for (export, key) in held.into_iter().zip(&keys) {
        source.client.host_pinned(export, pin_for(*key, pages));
    }
    let mut restored = 0;
    while restored < requesters.len() {
        assert!(
            Instant::now() < deadline,
            "restores exceeded their deadline"
        );
        source.poll();
        for event in std::mem::take(&mut source.events) {
            if let PoolEvent::Export(request) = event {
                assert!(keys.contains(&request.key));
                source
                    .client
                    .host_pinned(request.export, pin_for(request.key, pages));
            }
        }
        for requester in &mut requesters {
            requester.poll();
            for event in std::mem::take(&mut requester.events) {
                if let PoolEvent::Fetched { result, .. } = event {
                    assert_eq!(result.unwrap(), (pages * PAGE + ROW) as u64);
                    check_restored(requester, &scratch, pages);
                    restored += 1;
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(restored, 4, "every request restored without recomputing");
    for requester in &mut requesters {
        requester.client.close().unwrap();
    }
    source.client.close().unwrap();
}

#[test]
fn a_busy_source_restores_every_request_through_shared_memory() {
    let mut manager = Manager::start();
    manager.stop();
    manager.config.limits.pin_deadline_ms = 5_000;
    manager.config.metrics = Some(free_address());
    manager.run();
    busy_source_restores(&manager);
}

#[test]
fn a_source_keepalive_timeout_releases_exports_and_resumes_waiting_restores() {
    let mut manager = Manager::new();
    manager.config.limits.pin_deadline_ms = 100;
    manager.config.h2_keepalive = Some((Duration::from_millis(100), Duration::from_secs(1)));
    manager.config.metrics = Some(free_address());
    manager.run();
    // Keep the copier handle to block the client's runtime during the stall.
    let (copier, memory) = SimulatedGpu::shared(&geo()).unwrap();
    let client = PoolClient::start(
        config(&manager, 1),
        None,
        copier.clone(),
        Arc::new(OkRegistrar),
        flare_selene::pool::Roots::System,
        STARTUP_WAIT,
    )
    .unwrap();
    let mut source = Instance {
        client,
        gpu: memory,
        events: Vec::new(),
    };
    let mut requester = instance(&manager, 2);
    source.ready();
    requester.ready();
    let history = history();
    let pages = 4;
    let scratch = [100, 101, 102, 103];
    let key = offer_host_record(&source, &requester, &history, pages);
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);

    // Block the source's runtime, including HTTP/2 keepalive processing.
    let paused = copier.lock().unwrap();
    // Allow three 20 ms idle ticks to reach the locked copier before sending claims.
    thread::sleep(Duration::from_millis(60));
    requester.client.fetch(fetch.clone()).unwrap();
    requester.client.fetch(fetch.clone()).unwrap();
    for _ in 0..2 {
        let failure = requester.event("a timed-out source claim", |event| match event {
            PoolEvent::Fetched { result, .. } => {
                Some(result.as_ref().unwrap_err().error.to_string())
            }
            _ => None,
        });
        assert!(failure.contains("SourceTimeout"), "{failure}");
    }
    let waiting = requester.client.fetch(fetch).unwrap();
    wait("a source-slot refusal", || {
        manager.metrics().contains("selene_pool_claims_refused_total{reason=\"SourceLimit\"}")
    });
    wait("the source's keepalive timeout", || {
        manager.gauge("pool_sessions") == 1
    });
    drop(paused);

    let resumed = Instant::now();
    let failure = requester.event("the waiting restore to fail over", |event| match event {
        PoolEvent::Fetched { id, result } => {
            assert_eq!(*id, waiting);
            Some(result.as_ref().unwrap_err().error.to_string())
        }
        _ => None,
    });
    assert!(
        failure.contains("UnknownSource") || failure.contains("StaleSource"),
        "{failure}"
    );
    // Allow scheduling slack beyond the keepalive timeout, but not the 12 s fetch deadline.
    assert!(resumed.elapsed() < Duration::from_secs(3));
    wait("both sessions to reconnect", || {
        manager.gauge("pool_sessions") == 2
    });
    source.ready();
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("an export request after reconnect", |event| match event {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    source.client.host_pinned(export, pin_for(key, pages));
    assert_eq!(
        finish_restore(&mut requester),
        Ok((pages * PAGE + ROW) as u64)
    );
    check_restored(&requester, &scratch, pages);
    wait("claim records to clear", || {
        manager.gauge("pool_claim_records") == 0
    });
    source.client.close().unwrap();
    requester.client.close().unwrap();
}

#[test]
fn a_busy_source_restores_every_request_over_tcp() {
    let mut manager = network_manager(false);
    manager.stop();
    manager.config.limits.pin_deadline_ms = 5_000;
    manager.run();
    busy_source_restores(&manager);
}

#[test]
fn a_record_put_into_the_manager_store_is_restored_by_another_instance() {
    let manager = Manager::start();
    let mut writer = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
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
    wait_store_index(&requester, &history);
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
    let mut source = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
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
        bytes: (pages * PAGE + ROW) as u64,
    });
    wait("the manager to offer it", || {
        let ticket = requester.client.begin_lookup(&history, 0);
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let fetch = fetch_for(&mut requester, &history, pages, &[300, 301, 302]);
    assert_eq!(
        fetch.part.candidate.plan.level,
        flare_selene::levels::Level::NeighbourGpu
    );
    requester.client.fetch(fetch).unwrap();
    // The worker's side: pin the chain and answer, then release at the end.
    let (export, reserved) = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => {
            assert_eq!((request.tier, request.key), (Tier::Gpu, key));
            Some((request.export, request.reserved_bytes))
        }
        _ => None,
    });
    assert!(reserved >= (pages * PAGE + ROW) as u64);
    source.client.export_pinned(
        export,
        flare_selene::ExportChain {
            nodes: vec![flare_selene::ExportNode {
                key,
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
    let mut source = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
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
        bytes: (4 * PAGE + ROW) as u64,
    });
    let offered = |requester: &Instance| {
        let ticket = requester.client.begin_lookup(&history, 0);
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

#[test]
fn a_restore_through_the_real_manager_still_works_after_the_manager_restarts() {
    let mut manager = Manager::start();
    let mut source = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
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
        bytes: (pages * PAGE + ROW) as u64,
    });
    let offered = |requester: &Instance| {
        let ticket = requester.client.begin_lookup(&history, 0);
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    };
    wait("the record to be offered", || offered(&requester));
    manager.restart();
    let restarted = Instant::now();
    wait("the instances to reconnect", || {
        source.client.status() == PoolStatus::Ready
            && requester.client.status() == PoolStatus::Ready
    });
    assert!(
        restarted.elapsed() < Duration::from_secs(4),
        "reconnected {:?} after the restart",
        restarted.elapsed()
    );
    wait("the record to be offered again", || offered(&requester));
    // Round after round, the restore copies through the regions the restart reused.
    for round in 0..3u32 {
        let scratch = [100 + round * 10, 101 + round * 10, 102 + round * 10, 103 + round * 10];
        let pin = pin_for(key, pages);
        let fetch = fetch_for(&mut requester, &history, pages, &scratch);
        requester.client.fetch(fetch).unwrap();
        let export = source.event("an export request", |e| match e {
            PoolEvent::Export(request) => Some(request.export),
            _ => None,
        });
        source.client.host_pinned(export, pin);
        let result = requester.event("the restore", |e| match e {
            PoolEvent::Fetched { result, .. } => {
                Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
            }
            _ => None,
        });
        assert_eq!(result, Ok((pages * PAGE + ROW) as u64), "round {round}");
    }
}

fn counted(manager: &Manager, registrar: &Arc<CountingRegistrar>, wait: Duration) -> PoolClient {
    counted_approving(manager, registrar, wait, None).unwrap()
}

fn counted_approving(
    manager: &Manager,
    registrar: &Arc<CountingRegistrar>,
    wait: Duration,
    approve: Option<flare_selene::pool::Approve>,
) -> Result<PoolClient> {
    let (copier, _gpu) = SimulatedGpu::shared(&geo()).unwrap();
    let mut config = config(manager, 1);
    config.approve = approve;
    PoolClient::start(
        config,
        None,
        copier,
        registrar.clone(),
        flare_selene::testing::Roots::System,
        wait,
    )
}

struct SlowRegistrar {
    counting: CountingRegistrar,
    runtime_progress: AtomicBool,
}

impl SlowRegistrar {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            counting: CountingRegistrar::default(),
            runtime_progress: AtomicBool::new(false),
        })
    }
}

impl flare_selene::HostRegistrar for SlowRegistrar {
    fn register(
        &self,
        context: usize,
        address: usize,
        bytes: usize,
    ) -> Result<Box<dyn flare_selene::Registration>> {
        if self.counting.calls.lock().unwrap().is_empty() {
            let (tx, rx) = std::sync::mpsc::channel();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = tx.send(());
            });
            thread::sleep(Duration::from_secs(1));
            self.runtime_progress
                .store(rx.try_recv().is_ok(), Ordering::SeqCst);
        }
        flare_selene::HostRegistrar::register(&self.counting, context, address, bytes)
    }

    fn map_split(
        &self,
        requests: &[(usize, i32, usize)],
    ) -> Result<Box<dyn flare_selene::SplitMapping>> {
        flare_selene::HostRegistrar::map_split(&self.counting, requests)
    }
}

fn short_keepalive_manager() -> Manager {
    let mut manager = Manager::new();
    manager.config.h2_keepalive = Some((Duration::from_millis(100), Duration::from_millis(200)));
    manager.run();
    manager
}

#[test]
fn slow_registration_keeps_the_pool_connection_alive_until_open() {
    let manager = short_keepalive_manager();
    let registrar = SlowRegistrar::new();
    let (copier, _gpu) = SimulatedGpu::shared(&geo()).unwrap();
    let mut client = PoolClient::start(
        config(&manager, 1),
        None,
        copier,
        registrar.clone(),
        flare_selene::pool::Roots::System,
        STARTUP_WAIT,
    )
    .unwrap();
    wait("the pool session to be ready", || {
        client.status() == PoolStatus::Ready
    });
    assert_eq!(client.registered_regions().len(), 2);
    assert_eq!(registrar.counting.calls.lock().unwrap().len(), 2);
    assert!(registrar.runtime_progress.load(Ordering::SeqCst));
    client.close().unwrap();
}

fn inode(manager: &Manager, file: &str) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(manager.config.shm_directory().unwrap().join(file))
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
    let files = (inode(&manager, "arena"), inode(&manager, "staging"));

    manager.stop();
    wait("the old session to disconnect", || {
        client.status() != PoolStatus::Ready
    });
    manager.run();
    wait("a session with the restarted manager", || {
        client.status() == PoolStatus::Ready
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
    flare_selene::pool::Approve,
    Arc<std::sync::Mutex<Vec<(Vec<flare_selene::pool::PlannedRegion>, usize)>>>,
) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (log, registrar) = (seen.clone(), registrar.clone());
    let approve: flare_selene::pool::Approve = Arc::new(move |plan| {
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
    let client = counted_approving(&manager, &registrar, STARTUP_WAIT, Some(approve)).unwrap();
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
    manager.stop();
    wait("the old session to disconnect", || {
        client.status() != PoolStatus::Ready
    });
    manager.run();
    wait("a session with the restarted manager", || {
        client.status() == PoolStatus::Ready
    });
    assert_eq!(seen.lock().unwrap().len(), 1, "the reconnect asked nothing");
    assert_eq!(registrar.calls.lock().unwrap().len(), 2);
}

#[test]
fn a_rejected_plan_fails_startup_without_registration() {
    let manager = Manager::start();
    let registrar = Arc::new(CountingRegistrar::default());
    let (approve, seen) = recording(&registrar, Err("over the host budget".into()));
    let reason = counted_approving(&manager, &registrar, STARTUP_WAIT, Some(approve))
        .err()
        .expect("approval rejection must fail startup")
        .to_string();
    for text in [
        "over the host budget",
        "arena",
        "staging",
        "remove --prefix-cache-pool",
    ] {
        assert!(reason.contains(text), "{reason}");
    }
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(registrar.calls.lock().unwrap().is_empty());
}

#[test]
fn a_changed_staging_size_turns_the_pool_off_for_a_running_instance() {
    let mut manager = Manager::start();
    let registrar = Arc::new(CountingRegistrar::default());
    let mut client = counted(&manager, &registrar, STARTUP_WAIT);
    wait("Ready", || client.status() == PoolStatus::Ready);

    manager.stop();
    manager.config.staging *= 2;
    let _ = std::fs::remove_file(&manager.config.socket);
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
    assert!(matches!(client.status(), PoolStatus::Disabled(_)));
    assert_eq!(
        registrar.calls.lock().unwrap().len(),
        2,
        "nothing was registered late"
    );
    client.close().unwrap();
}

#[test]
fn pool_thread_unwind_during_registration_fails_startup() {
    use flare_selene::{HostRegistrar, Registration, SplitMapping};
    struct PanickingRegistrar;
    impl HostRegistrar for PanickingRegistrar {
        fn register(
            &self,
            context: usize,
            address: usize,
            bytes: usize,
        ) -> Result<Box<dyn Registration>> {
            OkRegistrar.register(context, address, bytes)
        }
        fn map_split(&self, requests: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
            OkRegistrar.map_split(requests)
        }
    }
    impl Drop for PanickingRegistrar {
        fn drop(&mut self) {
            panic!("registrar owner dropped before startup registration completed");
        }
    }
    let manager = Manager::start();
    for rank in [false, true] {
        let config = config(&manager, 1);
        let (copier, _gpu) = SimulatedGpu::shared(&geo()).unwrap();
        let (zero, peer) = flare_selene::pool::rank::memory_links();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = thread::spawn(move || {
            let result = if rank {
                let geometry = vec![geo()[0].clone(); 2];
                flare_selene::pool::RankAgent::start(flare_selene::pool::AgentConfig {
                    address: config.address,
                    ca: None,
                    scope: Scope::new([7; 32], fingerprint(&geometry), &geometry).unwrap(),
                    rank: 1,
                    world: 2,
                    link: peer,
                    copier,
                    registrar: Arc::new(PanickingRegistrar),
                    registration_context: 0,
                    gpu_uuid: [3; 16],
                    records: None,
                    approve: None,
                })
                .map(|_| ())
            } else {
                PoolClient::start(
                    config,
                    None,
                    copier,
                    Arc::new(PanickingRegistrar),
                    flare_selene::pool::Roots::System,
                    STARTUP_WAIT,
                )
                .map(|_| ())
            };
            send.send(result).unwrap();
        });
        let reason = receive
            .recv_timeout(Duration::from_secs(2))
            .expect("pool thread unwind must wake startup")
            .expect_err("a panicking pool thread must fail startup")
            .to_string();
        assert!(
            reason.contains("pool thread stopped before attachment completed"),
            "{reason}"
        );
        worker.join().unwrap();
        drop(zero);
    }
}

#[test]
fn the_weight_load_probe_reaches_hello_without_opening_a_session() {
    let mut manager = Manager::new();
    let options = flare_selene::pool::PoolOptions {
        address: manager.address(),
        ca: None,
    };
    let reason = options
        .reach(Duration::from_millis(20))
        .unwrap_err()
        .to_string();
    assert!(reason.contains("did not answer within 20ms"), "{reason}");
    assert!(reason.contains("No such file or directory"), "{reason}");
    manager.config.metrics = Some(free_address());
    manager.run();
    options.reach(Duration::from_secs(2)).unwrap();
    wait("Hello without a session", || {
        manager
            .metrics()
            .lines()
            .any(|line| line == "selene_pool_sessions 0")
    });
}

#[test]
fn an_absent_manager_returns_an_actionable_startup_error() {
    let manager = Manager::new();
    let registrar = Arc::new(CountingRegistrar::default());
    let (copier, _) = SimulatedGpu::shared(&geo()).unwrap();
    let started = Instant::now();
    let reason = PoolClient::start(
        config(&manager, 1),
        None,
        copier,
        registrar.clone(),
        flare_selene::testing::Roots::System,
        Duration::from_millis(400),
    )
    .err()
    .expect("a configured pool must attach at startup")
    .to_string();
    assert!(started.elapsed() < Duration::from_secs(5));
    for text in [
        "did not answer within 400ms",
        "No such file or directory",
        "start the manager before the instance",
        "remove --prefix-cache-pool",
    ] {
        assert!(reason.contains(text), "{reason}");
    }
    assert!(registrar.calls.lock().unwrap().is_empty());
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
        capture_role: flare_selene::CaptureRole::Endpoint,
        id: CheckpointId::new(u64::from(salt)),
        key,
        ancestors: Vec::new(),
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        chain_pages: (base..base + pages as u32).map(PageId).collect(),
        tail_pages: pages,
        slot: StateSlotId(salt),
    };
    (checkpoint, history)
}

/// Publish as the model worker does: start the copy and poll until it is
/// reported, and try again after a refusal or a deferral. The device cache
/// then evicts the checkpoint, as it does under pressure.
fn publish<S: HostStore>(store: &mut S, checkpoint: &PublishedCheckpoint) {
    store_and_wait(store, checkpoint);
    store.evicted(checkpoint.key, None);
}

fn store_and_wait<S: HostStore + ?Sized>(store: &mut S, checkpoint: &PublishedCheckpoint) {
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
                        if let Err(error) = &result {
                            assert_eq!(error.to_string(), "no room for the record on disk");
                            retry = true;
                        } else if outcome == StoreOutcome::Queued {
                            return;
                        }
                    }
                    StoreEvent::Deferred { checkpoint: id, .. } if id == checkpoint.id => {
                        retry = true
                    }
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
fn drive<S: HostStore + ?Sized>(store: &mut S, what: &str, mut done: impl FnMut(&mut S) -> bool) {
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
    let ticket = requester.client.begin_lookup(history, 0);
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
        let ticket = requester.client.begin_lookup(history, 0);
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
    assert_eq!(restore_bytes(requester, fetch), (pages * PAGE + ROW) as u64);
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

/// A pooled memory store with the specified retention slots and a simulated device.
fn memory_instance(
    manager: &Manager,
    geometry: &[CacheGeometry],
    slots: usize,
) -> (PooledStore<MemoryStore>, GpuMemory) {
    let (transfers, gpu) = SimulatedGpu::shared_mapped(geometry).unwrap();
    let layout = Layout::new(geometry).unwrap();
    let intake = flare_selene::offload::intake_slots(&layout, 1);
    let pool = SlabPool::new(layout.page_record_bytes, intake + slots, intake).unwrap();
    let memory = MemoryStore::new([7; 32], pool, transfers).unwrap();
    let (copier, _) = SimulatedGpu::shared(geometry).unwrap();
    let config = config_on(manager, 1, false, geometry);
    let store = PooledStore::connect(memory, config, copier, Arc::new(OkRegistrar)).unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);
    (store, gpu)
}

#[test]
fn an_evicted_dram_record_reaches_the_manager_store_and_is_restored_by_another_instance() {
    let manager = Manager::start();
    let mut requester = instance(&manager, 2);
    requester.ready();
    let geometry = geo();
    let layout = Layout::new(&geometry).unwrap();
    let intake = flare_selene::offload::intake_slots(&layout, 1);
    let slots = intake + 8 + 2 * layout.row_slots();
    let (mut store, gpu) = memory_instance(&manager, &geometry, slots);
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
    flare_selene::HostStore::close(&mut store).unwrap();
}

#[test]
fn a_retired_same_page_tip_restores_from_its_endpoint_parent() {
    let manager = Manager::start();
    let mut reader = instance(&manager, 2);
    reader.ready();
    let history = history_of((2 * PT + 1) as u32);
    let geometry = geo();
    let intake = flare_selene::offload::intake_slots(&Layout::new(&geometry).unwrap(), 1);
    let pool = SlabPool::new(PAGE as u64, intake + 4, intake).unwrap();
    let (transfers, gpu) = SimulatedGpu::shared_mapped(&geometry).unwrap();
    let mut memory = MemoryStore::new([7; 32], pool, transfers).unwrap();
    for page in 0..2 {
        gpu.pages
            .lock()
            .unwrap()
            .insert(page, expected_page(page as usize));
    }
    let mut ancestors = Vec::new();
    for (tokens, tail) in [(PT, 1), (2 * PT, 1), (2 * PT + 1, 0)] {
        gpu.rows
            .lock()
            .unwrap()
            .insert(tokens as u32, vec![tokens as u8; ROW]);
        let key = history.key_at(tokens).unwrap();
        store_and_wait(
            &mut memory,
            &PublishedCheckpoint {
                capture_role: flare_selene::CaptureRole::Endpoint,
                id: CheckpointId::new(tokens),
                key,
                ancestors: ancestors.clone(),
                bucket: history.bucket_at(tokens).unwrap(),
                chain_pages: (0..(tokens / PT) as u32).map(PageId).collect(),
                tail_pages: tail,
                slot: StateSlotId(tokens as u32),
            },
        );
        ancestors.insert(0, key);
    }
    assert_eq!(memory.stats().memory_records, 3);
    let (copier, _) = SimulatedGpu::shared(&geometry).unwrap();
    let mut store =
        PooledStore::connect(memory, config(&manager, 1), copier, Arc::new(OkRegistrar)).unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);
    drive(&mut store, "the entire path to retire", |store| {
        store.stats().memory_records == 0
    });
    store.close().unwrap();
    wait_store_index(&reader, &history);
    let parent_key = history.key_at(2 * PT).unwrap();
    stored_candidate(&reader, &history, parent_key);
    restore(&mut reader, &history, 2, &[100, 101]);
    assert_eq!(
        reader.gpu.rows.lock().unwrap()[&9],
        vec![(2 * PT) as u8; ROW]
    );
}

/// A leaf retires through its local root, which was published under the held `hint`.
/// With `list` the root names the prompt-end Endpoint no tier stores before it, so the
/// put carries two candidate ancestors and only the second is held.
fn retire_leaf_over_root_hint(evict_hint: bool, list: bool) {
    let manager = Manager::with_memory(128 << 10);
    let mut reader = instance(&manager, 2);
    reader.ready();
    let history = history();
    let (request, hint) = put_request(&history, 1);
    reader.client.put(request).unwrap();
    reader.event("the published ancestor to commit", |event| match event {
        PoolEvent::Put { outcome, .. } => {
            assert!(matches!(
                outcome,
                flare_selene::pool::PutOutcome::Stored { .. }
            ));
            Some(())
        }
        _ => None,
    });
    wait_store_index(&reader, &history);
    if evict_hint {
        let (request, _) = put_request(&salted(99), 8);
        reader.client.put(request).unwrap();
        reader.event("the record that evicts the hint", |event| match event {
            PoolEvent::Put { outcome, .. } => {
                assert!(matches!(
                    outcome,
                    flare_selene::pool::PutOutcome::Stored { .. }
                ));
                Some(())
            }
            _ => None,
        });
        wait("the hint to leave the manager index", || {
            !offered(&reader, &history)
        });
    }
    let geometry = geo();
    let layout = Layout::new(&geometry).unwrap();
    let (transfers, gpu) = SimulatedGpu::shared_mapped(&geometry).unwrap();
    let intake = flare_selene::offload::intake_slots(&layout, 1);
    let pool = SlabPool::new(PAGE as u64, intake + 8, intake).unwrap();
    let mut memory = MemoryStore::new([7; 32], pool, transfers).unwrap();
    for page in 0..4 {
        gpu.pages
            .lock()
            .unwrap()
            .insert(page, expected_page(page as usize));
    }
    let prompt_end = history.key_at(PT + 1).unwrap();
    let mut ancestors = if list {
        vec![prompt_end, hint]
    } else {
        vec![hint]
    };
    for pages in 2..=4 {
        let row = (pages * PAGE..pages * PAGE + ROW)
            .map(|byte| (byte * 13 + 5) as u8)
            .collect();
        gpu.rows.lock().unwrap().insert(pages as u32, row);
        let checkpoint = PublishedCheckpoint {
            capture_role: flare_selene::CaptureRole::Endpoint,
            id: CheckpointId::new(pages as u64),
            key: history.key_at(pages as u64 * PT).unwrap(),
            ancestors: ancestors.clone(),
            bucket: history.bucket_at(pages as u64 * PT).unwrap(),
            chain_pages: (0..pages as u32).map(PageId).collect(),
            tail_pages: 1,
            slot: StateSlotId(pages as u32),
        };
        store_and_wait(&mut memory, &checkpoint);
        ancestors.insert(0, checkpoint.key);
    }
    assert_eq!(memory.stats().memory_records, 3);
    let (copier, _) = SimulatedGpu::shared(&geometry).unwrap();
    let mut store =
        PooledStore::connect(memory, config(&manager, 1), copier, Arc::new(OkRegistrar)).unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);
    let leaf = ancestors[0];
    drive(&mut store, "the retired leaf to commit", |store| {
        let ticket = reader.client.begin_lookup(&history, 0);
        wait("the manager lookup", || {
            !matches!(ticket.state(), LookupState::Pending)
        });
        let LookupState::Ready(answer) = ticket.state() else {
            panic!("no lookup answer");
        };
        let Some(candidate) = answer
            .candidates
            .iter()
            .find(|candidate| candidate.plan.key == leaf && candidate.source.instance.is_empty())
        else {
            return false;
        };
        let nodes = &candidate.plan.nodes;
        let first = usize::from(!evict_hint);
        assert_eq!(nodes.len(), first + 3);
        if !evict_hint {
            assert_eq!(nodes[0].key, hint);
        }
        for (i, node) in nodes[first..].iter().enumerate() {
            assert_eq!(node.key, history.key_at((2 + i as u64) * PT).unwrap());
            assert_eq!(node.tail_pages, if evict_hint && i == 0 { 2 } else { 1 });
        }
        store.stats().memory_records < 3
    });
    store.close().unwrap();
    restore(&mut reader, &history, 4, &[100, 101, 102, 103]);
    assert_eq!(
        reader.gpu.rows.lock().unwrap()[&9],
        gpu.rows.lock().unwrap()[&4]
    );
    reader.client.close().unwrap();
}

#[test]
fn a_retired_leaf_links_to_the_published_hint_of_its_local_root() {
    retire_leaf_over_root_hint(false, false);
}

#[test]
fn a_retired_leaf_with_an_evicted_root_hint_is_stored_as_a_root() {
    retire_leaf_over_root_hint(true, false);
}

#[test]
fn a_retired_leaf_links_through_the_second_of_its_local_roots_offered_ancestors() {
    retire_leaf_over_root_hint(false, true);
}

#[test]
fn a_retired_leaf_with_every_offered_ancestor_evicted_is_stored_as_a_root() {
    retire_leaf_over_root_hint(true, true);
}

#[test]
fn a_retired_child_links_to_a_nearer_ancestor_held_only_by_the_manager() {
    let mut manager = Manager::dram_only(16 << 20);
    manager.run();
    let geometry = geo();
    let mut reader = instance(&manager, 2);
    reader.ready();

    let (mut source, gpu) = memory_instance(&manager, &geometry, 4);
    let (nearer, history) = publication(&gpu, 7, 3);
    publish(&mut source, &nearer);
    drive(&mut source, "the three-page ancestor to retire", |store| {
        store.stats().memory_records == 0
    });
    source.close().unwrap();
    wait_store_index(&reader, &history);
    stored_candidate(&reader, &history, nearer.key);

    let intake = flare_selene::offload::intake_slots(&Layout::new(&geometry).unwrap(), 1);
    let pool = SlabPool::new(PAGE as u64, intake + 6, intake).unwrap();
    let (transfers, gpu) = SimulatedGpu::shared_mapped(&geometry).unwrap();
    let mut memory = MemoryStore::new([7; 32], pool, transfers).unwrap();
    let (local, _) = publication(&gpu, 7, 1);
    store_and_wait(&mut memory, &local);
    let (mut child, _) = publication(&gpu, 7, 4);
    child.id = CheckpointId::new(8);
    child.slot = StateSlotId(8);
    gpu.rows.lock().unwrap().insert(8, salted_row(8));
    child.ancestors = vec![nearer.key, local.key];
    child.tail_pages = 1;
    store_and_wait(&mut memory, &child);
    assert_eq!(memory.index()[&child.key].parent, Some(local.key));
    assert_eq!(memory.index()[&child.key].tail_pages, 3);

    let (copier, _) = SimulatedGpu::shared(&geometry).unwrap();
    let mut store =
        PooledStore::connect(memory, config(&manager, 1), copier, Arc::new(OkRegistrar)).unwrap();
    drive(&mut store, "the local chain to retire", |store| {
        store.stats().memory_records == 0
    });
    assert_eq!(store.stats().deeper_link_records[1], 0);
    store.close().unwrap();
    let candidate = stored_candidate(&reader, &history, child.key);
    let nodes: Vec<_> = candidate.plan.nodes.iter().map(|node| (node.key, node.tail_pages)).collect();
    assert_eq!(nodes, [(nearer.key, 3), (child.key, 1)]);
    // Three pages for the nearer ancestor, one for the local parent, one for the child.
    assert_eq!(manager.gauge("put_bytes_total{part=\"pages\"}"), (5 * PAGE) as u64);

    let scratch = [100, 101, 102, 103];
    let fetch = fetch_for(&mut reader, &history, 4, &scratch);
    assert!(fetch.part.candidate.source.instance.is_empty());
    assert_eq!(restore_bytes(&mut reader, fetch), (4 * PAGE + ROW) as u64);
    for (page, dest) in scratch.iter().enumerate() {
        assert_eq!(reader.gpu.pages.lock().unwrap()[dest], salted_page(7, page));
    }
    assert_eq!(reader.gpu.rows.lock().unwrap()[&9], salted_row(8));
    reader.client.close().unwrap();
}

/// Retire the Anchor before publishing the terminal under its missing prompt end.
fn retire_anchor_and_terminal(
    manager: &Manager,
    geometry: &[CacheGeometry],
) -> (Instance, GpuMemory, flare_selene::StoreStats) {
    let layout = Layout::new(geometry).unwrap();
    let page_bytes = layout.page_record_bytes as usize;
    let row_bytes = layout.row_record_bytes as usize;
    let (mut store, gpu) = memory_instance(manager, geometry, 64 + layout.row_slots());
    let reader = instance_on(manager, 2, false, geometry);
    reader.ready();
    let history = history_of(64 * PT as u32);
    let page = |index: u32| -> Vec<u8> {
        (0..page_bytes)
            .map(|byte| (index * 13 + byte as u32 / PAGE as u32 * 71 + byte as u32 % 251) as u8)
            .collect()
    };
    let row = |salt: u32| -> Vec<u8> {
        (0..row_bytes)
            .map(|byte| (salt as usize * 17 + byte / ROW * 91 + byte % 251) as u8)
            .collect()
    };
    for index in 0..24 {
        gpu.pages.lock().unwrap().insert(index, page(index));
    }
    gpu.rows.lock().unwrap().insert(1, row(1));
    gpu.rows.lock().unwrap().insert(2, row(2));
    let anchor = history.key_at(16 * PT).unwrap();
    let prompt_end = history.key_at(16 * PT + 1).unwrap();
    let terminal = history.key_at(24 * PT).unwrap();
    assert_eq!(prompt_end.consumed_tokens / PT, anchor.consumed_tokens / PT);
    let publication = |id: u64, key: PrefixKey, ancestors: Vec<PrefixKey>| {
        let parent = ancestors.first().copied();
        let pages = (key.consumed_tokens / PT) as usize;
        PublishedCheckpoint {
            capture_role: if id == 1 {
                flare_selene::CaptureRole::Anchor
            } else {
                flare_selene::CaptureRole::Endpoint
            },
            id: CheckpointId::new(id),
            key,
            ancestors,
            bucket: history.bucket_at(key.consumed_tokens).unwrap(),
            chain_pages: (0..pages as u32).map(PageId).collect(),
            tail_pages: pages - parent.map_or(0, |p| (p.consumed_tokens / PT) as usize),
            slot: StateSlotId(id as u32),
        }
    };
    publish(&mut store, &publication(1, anchor, Vec::new()));
    drive(&mut store, "the Anchor to leave local memory", |store| {
        store.stats().memory_records == 0
    });
    wait_store_index(&reader, &history_of(16 * PT as u32));
    // With both ancestors absent locally, the terminal copies its full chain.
    store_and_wait(
        &mut store,
        &publication(2, terminal, vec![prompt_end, anchor]),
    );
    drive(&mut store, "the terminal to leave local memory", |store| {
        store.stats().memory_records == 0
    });
    let stats = store.stats();
    store.close().unwrap();
    (reader, gpu, stats)
}

#[test]
fn retired_terminal_counts_only_its_linked_tail_in_the_pool() {
    let mut manager = Manager::with_storage(4096, 64 << 20);
    manager.run();
    let geometry = [geo(), geo()].concat();
    let (mut reader, _, stats) = retire_anchor_and_terminal(&manager, &geometry);
    let layout = Layout::new(&geometry).unwrap();
    let page_bytes = layout.page_record_bytes;
    let row_bytes = layout.row_record_bytes;
    let history = history_of(24 * PT as u32);
    let anchor = history.key_at(16 * PT).unwrap();
    let terminal = history.key_at(24 * PT).unwrap();
    let directory = manager
        .config
        .root
        .as_ref()
        .unwrap()
        .join(record::directory_name(&[7; 32], &fingerprint(&geometry)));
    assert_eq!(stats.root_records[0], [1, 1]);
    assert_eq!(stats.deeper_link_records[0], 0);
    assert_eq!(stats.root_pages[0], [16, 24]);
    assert_eq!(stats.deeper_link_pages[0], 0);
    // The manager already holds the Anchor's first 16 pages.
    let stored = std::fs::read(directory.join(record::file_name(&terminal))).unwrap();
    let header = record::Header::decode(&stored[..4096]).unwrap();
    assert_eq!(header.parent, Some(anchor));
    assert_eq!(header.tail_pages, 8);
    let metrics = manager.metrics();
    for line in [
        "selene_put_links_total{parent=\"root\"} 1\n".to_string(),
        "selene_put_links_total{parent=\"linked\"} 1\n".to_string(),
        format!(
            "selene_put_bytes_total{{part=\"pages\"}} {}\n",
            24 * page_bytes
        ),
        format!(
            "selene_put_bytes_total{{part=\"rows\"}} {}\n",
            2 * row_bytes
        ),
    ] {
        assert!(metrics.contains(&line), "missing metric {line}");
    }
    assert_eq!(stats.root_records[1], [1, 0]);
    assert_eq!(stats.deeper_link_records[1], 1);
    assert_eq!(stats.root_pages[1], [16, 0]);
    assert_eq!(stats.deeper_link_pages[1], 8);
    reader.client.close().unwrap();
}

#[test]
fn restarted_manager_restores_terminal_anchor_and_branch_bytes() {
    let mut manager = Manager::with_storage(4096, 64 << 20);
    manager.run();
    let geometry = [geo(), geo()].concat();
    let (mut reader, source, _) = retire_anchor_and_terminal(&manager, &geometry);
    let layout = Layout::new(&geometry).unwrap();
    manager.restart();
    reader.ready();
    let terminal = history_of(24 * PT as u32);
    let anchor = history_of(16 * PT as u32);
    let mut branch = history_of(16 * PT as u32);
    branch
        .append(&(9000..9000 + 8 * PT as u32).collect::<Vec<_>>(), &[])
        .unwrap();
    wait_store_index(&reader, &terminal);
    for (history, pages, slot, first) in [
        (&terminal, 24, 2, 100),
        (&anchor, 16, 1, 200),
        (&branch, 16, 1, 300),
    ] {
        let scratch: Vec<_> = (first..first + pages).collect();
        let fetch = fetch_for_geometry(&reader, history, pages as usize, &scratch, &geometry);
        assert!(fetch.part.candidate.source.instance.is_empty());
        let expected_key = history.key_at(u64::from(pages) * PT).unwrap();
        assert_eq!(fetch.part.candidate.plan.key, expected_key);
        let bytes = restore_bytes(&mut reader, fetch);
        assert_eq!(
            bytes,
            u64::from(pages) * layout.page_record_bytes + layout.row_record_bytes
        );
        let restored = reader.gpu.pages.lock().unwrap();
        for (page, dest) in scratch.iter().enumerate() {
            assert_eq!(restored[dest], source.pages.lock().unwrap()[&(page as u32)]);
        }
        assert_eq!(reader.gpu.rows.lock().unwrap()[&9], source.rows.lock().unwrap()[&slot]);
    }
    reader.client.close().unwrap();
}

/// The candidate the manager's store offers for `key`, once its lookup answers.
fn stored_candidate(
    reader: &Instance,
    history: &TestHistory,
    key: PrefixKey,
) -> flare_selene::pool::PoolCandidate {
    let ticket = reader.client.begin_lookup(history, 0);
    wait("the manager's lookup", || {
        !matches!(ticket.state(), LookupState::Pending)
    });
    let LookupState::Ready(answer) = ticket.state() else {
        panic!("no lookup answer");
    };
    answer
        .candidates
        .iter()
        .find(|candidate| candidate.plan.key == key && candidate.source.instance.is_empty())
        .expect("the manager holds the record")
        .clone()
}

/// The total size of the record files in `directory`.
fn record_files_bytes(directory: &std::path::Path) -> u64 {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "ckpt")
        })
        .map(|path| std::fs::metadata(path).unwrap().len())
        .sum()
}

#[test]
fn successive_retired_conversation_turns_store_only_tails_and_restore_every_rank() {
    let mut manager = Manager::with_storage(4096, 64 << 20);
    manager.run();
    let geometry = vec![
        geometry(PT, &[("k", 4096), ("v", 4096)], 4096),
        geometry(PT, &[("k", 4096), ("v", 4096)], 4096),
    ];
    let directory = manager
        .config
        .root
        .as_ref()
        .unwrap()
        .join(record::directory_name(&[7; 32], &fingerprint(&geometry)));
    let layout = Layout::new(&geometry).unwrap();
    let page_bytes = layout.page_record_bytes as usize;
    let row_bytes = layout.row_record_bytes as usize;
    let (mut store, gpu) = memory_instance(&manager, &geometry, 64 + layout.row_slots());
    let mut reader = instance_on(&manager, 2, false, &geometry);
    reader.ready();
    let history = history_of(64 * PT as u32);
    let page = |index: u32| -> Vec<u8> {
        (0..page_bytes)
            .map(|byte| (index * 13 + byte as u32 / PAGE as u32 * 71 + byte as u32 % 251) as u8)
            .collect()
    };
    for index in 0..64 {
        gpu.pages.lock().unwrap().insert(index, page(index));
    }
    let mut ancestors = Vec::new();
    let mut expected_bytes = 0;
    let mut stored_page_bytes = 0;
    let mut stored_row_bytes = 0;
    let mut metadata_bytes = 0;
    let mut previous_pages = 0;
    for turn in 1..=4 {
        let pages = turn * 16;
        let tokens = pages as u64 * PT;
        let key = history.key_at(tokens).unwrap();
        let row: Vec<u8> = (0..row_bytes)
            .map(|byte| (turn * 17 + byte / ROW * 91 + byte % 251) as u8)
            .collect();
        gpu.rows.lock().unwrap().insert(turn as u32, row);
        let checkpoint = PublishedCheckpoint {
            capture_role: flare_selene::CaptureRole::Endpoint,
            id: CheckpointId::new(turn as u64),
            key,
            ancestors: ancestors.clone(),
            bucket: history.bucket_at(tokens).unwrap(),
            chain_pages: (0..pages as u32).map(PageId).collect(),
            tail_pages: pages - previous_pages,
            slot: StateSlotId(turn as u32),
        };
        publish(&mut store, &checkpoint);
        drive(&mut store, "the turn to leave local memory", |store| {
            store.stats().memory_records == 0
        });
        let candidate = stored_candidate(&reader, &history, key);
        let header = record::Header::new(
            &geometry,
            [7; 32],
            fingerprint(&geometry),
            key,
            ancestors.first().copied(),
            checkpoint.bucket,
            (pages - previous_pages) as u32,
            std::time::SystemTime::now(),
        )
        .unwrap();
        expected_bytes += header.length_with_row();
        metadata_bytes += u64::from(header.page_segment);
        let stored = std::fs::read(directory.join(record::file_name(&key))).unwrap();
        let stored_header = record::Header::decode(&stored[..4096]).unwrap();
        stored_page_bytes += stored_header.tail_pages as usize * page_bytes;
        stored_row_bytes += row_bytes;
        assert_eq!(record_files_bytes(&directory), expected_bytes);
        assert_eq!(stored_header.parent, ancestors.first().copied());
        assert_eq!(stored_header.tail_pages, 16);
        assert_eq!(candidate.plan.nodes.len(), turn);
        assert_eq!(candidate.plan.nodes.last().unwrap().tail_pages, 16);
        ancestors.insert(0, key);
        previous_pages = pages;
    }
    assert_eq!(stored_page_bytes, 64 * page_bytes);
    assert_eq!(stored_row_bytes, 4 * row_bytes);
    let metrics = manager.metrics();
    for line in [
        "selene_put_links_total{parent=\"root\"} 1\n".to_string(),
        "selene_put_links_total{parent=\"linked\"} 3\n".to_string(),
        format!("selene_put_bytes_total{{part=\"pages\"}} {stored_page_bytes}\n"),
        format!("selene_put_bytes_total{{part=\"rows\"}} {stored_row_bytes}\n"),
    ] {
        assert!(metrics.contains(&line), "missing metric {line}");
    }
    // Each file holds its header and checksum table, its tail pages, and its row.
    assert_eq!(
        expected_bytes,
        (stored_page_bytes + stored_row_bytes) as u64 + metadata_bytes
    );
    let identity = Identity {
        model: [7; 32],
        layout: fingerprint(&geometry),
    };
    let request = Request {
        history: &history,
        identity: &identity,
        page_tokens: PT,
        resident: 0,
        before: u64::MAX,
    };
    let candidate = stored_candidate(&reader, &history, ancestors[0]);
    let plan = plan::plan(&request, std::slice::from_ref(&candidate.plan)).unwrap();
    let part = plan.part;
    reader
        .client
        .fetch(PoolFetch {
            id: 0,
            page_tokens: PT,
            part: FetchPart {
                candidate,
                part,
                pages: (100..164).map(PageId).collect(),
                slot: Some(StateSlotId(9)),
            },
        })
        .unwrap();
    let result = reader.event("the linked restore", |event| match event {
        PoolEvent::Fetched { result, .. } => Some(
            result
                .as_ref()
                .map(|bytes| *bytes)
                .map_err(|error| error.error.to_string()),
        ),
        _ => None,
    });
    assert_eq!(result, Ok((64 * page_bytes + row_bytes) as u64));
    for index in 0..64 {
        assert_eq!(
            reader.gpu.pages.lock().unwrap()[&(100 + index)],
            page(index)
        );
    }
    assert_eq!(
        reader.gpu.rows.lock().unwrap()[&9],
        gpu.rows.lock().unwrap()[&4]
    );
    store.close().unwrap();
    reader.client.close().unwrap();
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
    let mut requester = instance(&manager, 2);
    requester.ready();
    let geometry: Vec<CacheGeometry> = geo();
    let fingerprint = fingerprint(&geometry);
    // Memory holds two records of two pages; the disk tier, two files.
    let (memory_transfers, gpu) = SimulatedGpu::shared_mapped(&geometry).unwrap();
    let intake = flare_selene::offload::intake_slots(&Layout::new(&geometry).unwrap(), 1);
    let pool = SlabPool::new(PAGE as u64, intake + 8, intake).unwrap();
    let memory = MemoryStore::new_flat([7; 32], pool, memory_transfers).unwrap();
    let lock = flare_selene::testing::directory_lock(&directory).unwrap();
    let scanned = record::scan(&lock, &[7; 32], &fingerprint, geometry[0].page_tokens, &record::rank_sizes(&geometry)).unwrap();
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
    let (disk_transfers, _) = SimulatedGpu::shared_mapped(&geometry).unwrap();
    let disk = DiskStore::new(
        lock,
        [7; 32],
        fingerprint,
        2 * file_bytes + file_bytes / 2,
        geometry,
        scanned,
        Box::new(Staged::new(
            Some(memory.pool().clone()),
            disk_transfers,
            DiskThread::start("cascade-test-disk").unwrap(),
        )),
    )
    .unwrap();
    let composite = CompositeStore::new(memory, disk).unwrap();
    let (copier, _unused) = SimulatedGpu::shared(&geo()).unwrap();
    let mut store = PooledStore::connect(
        composite,
        config(&manager, 1),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    wait("Ready", || store.pool().status() == PoolStatus::Ready);

    let (first, first_history) = publication(&gpu, 1, 2);
    let (second, _) = publication(&gpu, 2, 2);
    let (third, _) = publication(&gpu, 3, 2);
    let (fourth, _) = publication(&gpu, 4, 2);
    // The small warm tier is below the clean floor, so landed copies spill.
    publish(&mut store, &first);
    publish(&mut store, &second);
    drive(&mut store, "the first record to commit on disk", |_| {
        directory.join(record::file_name(&first.key)).exists()
    });
    // Further publications fill the disk tier and push the coldest file
    // to the manager, releasing its memory copy too.
    publish(&mut store, &third);
    publish(&mut store, &fourth);
    drive(&mut store, "the first record's file to go", |_| {
        !directory.join(record::file_name(&first.key)).exists()
    });
    restore_salted(&mut requester, &first_history, 1, 2, &mut store);
    flare_selene::HostStore::close(&mut store).unwrap();
    drop(store);
    let _ = std::fs::remove_dir_all(&directory);
}

#[test]
fn drained_records_are_announced_and_claimable_after_restart() {
    let Some(directory) = offload_directory("drained-restart") else {
        return;
    };
    let manager = Manager::start();
    let mut requester = instance(&manager, 2);
    requester.ready();
    let open = || {
        let geometry = geo();
        let fingerprint = fingerprint(&geometry);
        let lock = flare_selene::testing::directory_lock(&directory).unwrap();
        let scanned = record::scan(
            &lock,
            &[7; 32],
            &fingerprint,
            geometry[0].page_tokens,
            &record::rank_sizes(&geometry),
        )
        .unwrap();
        let (memory_copier, gpu) = SimulatedGpu::shared_mapped(&geometry).unwrap();
        let memory = MemoryStore::new_flat(
            [7; 32],
            SlabPool::new(
                PAGE as u64,
                20 + 2 * flare_selene::offload::intake_slots(&Layout::new(&geometry).unwrap(), 1),
                flare_selene::offload::intake_slots(&Layout::new(&geometry).unwrap(), 1),
            )
            .unwrap(),
            memory_copier,
        )
        .unwrap();
        let (disk_copier, _) = SimulatedGpu::shared_mapped(&geometry).unwrap();
        let disk = DiskStore::new(
            lock,
            [7; 32],
            fingerprint,
            1 << 20,
            geometry,
            scanned,
            Box::new(Staged::new(
                Some(memory.pool().clone()),
                disk_copier,
                DiskThread::start("drained-restart").unwrap(),
            )),
        )
        .unwrap();
        let mut composite = CompositeStore::new(memory, disk).unwrap();
        composite.drain_on_shutdown = Duration::MAX;
        let (copier, _) = SimulatedGpu::shared(&geo()).unwrap();
        let store = PooledStore::connect(
            composite,
            config(&manager, 1),
            copier,
            Arc::new(OkRegistrar),
        )
        .unwrap();
        (store, gpu)
    };
    let (mut source, gpu) = open();
    let records: Vec<_> = (1..=3).map(|salt| publication(&gpu, salt, 8)).collect();
    for (checkpoint, _) in &records {
        publish(&mut source, checkpoint);
    }
    drive(&mut source, "records offered before shutdown", |_| {
        records
            .iter()
            .all(|(_, history)| offered(&requester, history))
    });
    drive(&mut source, "the cold record to commit on disk", |_| {
        directory
            .join(record::file_name(&records[0].0.key))
            .exists()
    });
    let last_file = directory.join(record::file_name(&records[2].0.key));
    assert!(!last_file.exists(), "the last record is DRAM-only");
    source.close().unwrap();
    drop(source);
    assert!(last_file.exists(), "shutdown drained the last record");

    let (mut source, _) = open();
    assert_eq!(source.stats().disk_records, records.len());
    assert_eq!(source.stats().memory_records, 0);
    assert_eq!(source.pool().announcer().len(), records.len());
    wait("restarted source ready", || {
        source.pool().status() == PoolStatus::Ready
    });
    for (_, history) in &records {
        wait(
            "recovered record offered before the first source poll",
            || offered(&requester, history),
        );
    }
    for (index, (_, history)) in records.iter().enumerate() {
        let scratch: Vec<_> = (700..708).collect();
        let fetch = fetch_for(&mut requester, history, 8, &scratch);
        assert_eq!(fetch.part.candidate.tier, Tier::Nvme);
        requester.client.fetch(fetch).unwrap();
        let mut result = None;
        wait("recovered record claimed and copied", || {
            source.poll(&mut Vec::new()).unwrap();
            requester.poll();
            for event in requester.events.drain(..) {
                if let PoolEvent::Fetched { result: done, .. } = event {
                    result = Some(done.map_err(|failure| failure.error.to_string()));
                }
            }
            result.is_some()
        });
        assert_eq!(result.unwrap(), Ok((8 * PAGE + ROW) as u64));
        let salt = index as u32 + 1;
        for (page, id) in scratch.iter().enumerate() {
            assert_eq!(
                requester.gpu.pages.lock().unwrap()[id],
                salted_page(salt, page)
            );
        }
        assert_eq!(requester.gpu.rows.lock().unwrap()[&9], salted_row(salt));
    }
    source.close().unwrap();
    drop(source);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn large_recovered_nvme_records_restore_consecutively() {
    let Some(directory) = offload_directory("recovered-pair") else {
        return;
    };
    let mut manager = Manager::new();
    manager.config.staging = 128 << 20;
    manager.run();
    let mut requester = instance(&manager, 2);
    requester.ready();
    let open = || {
        let geometry = geo();
        let layout = Layout::new(&geometry).unwrap();
        let fingerprint = fingerprint(&geometry);
        let lock = flare_selene::testing::directory_lock(&directory).unwrap();
        let scanned = record::scan(&lock, &[7; 32], &fingerprint, geometry[0].page_tokens, &record::rank_sizes(&geometry)).unwrap();
        let (copier, gpu) = SimulatedGpu::shared(&geometry).unwrap();
        let store = DiskStore::new(
            lock,
            [7; 32],
            fingerprint,
            256 << 20,
            geometry,
            scanned,
            Box::new(Staged::new(
                Some(
                    SlabPool::new(
                        layout.page_record_bytes,
                        flare_selene::offload::intake_slots(&layout, 1),
                        flare_selene::offload::intake_slots(&layout, 1),
                    )
                    .unwrap(),
                ),
                copier,
                DiskThread::start("recovered-pair").unwrap(),
            )),
        )
        .unwrap();
        (store, gpu)
    };
    let (mut disk, gpu) = open();
    let pages = 110_000 / PT as usize;
    let mut histories = Vec::new();
    for salt in 1..=2u32 {
        let mut history = TestHistory::new([7; 32]);
        history.append(&vec![salt; 110_000], &[]).unwrap();
        for page in 0..pages {
            gpu.pages
                .lock()
                .unwrap()
                .insert(page as u32, salted_page(salt, page));
        }
        gpu.rows.lock().unwrap().insert(salt, salted_row(salt));
        let checkpoint = PublishedCheckpoint {
            capture_role: flare_selene::CaptureRole::Endpoint,
            id: CheckpointId::new(u64::from(salt)),
            key: history.key_at(110_000).unwrap(),
            ancestors: Vec::new(),
            bucket: history.bucket_at(110_000).unwrap(),
            chain_pages: (0..pages as u32).map(PageId).collect(),
            tail_pages: pages,
            slot: StateSlotId(salt),
        };
        store_and_wait(&mut disk, &checkpoint);
        histories.push(history);
    }
    disk.close().unwrap();
    drop(disk);
    let (disk, _) = open();
    let (copier, _) = SimulatedGpu::shared(&geo()).unwrap();
    let mut source =
        PooledStore::connect(disk, config(&manager, 1), copier, Arc::new(OkRegistrar)).unwrap();
    wait("recovered source ready", || {
        source.pool().status() == PoolStatus::Ready
    });
    source.poll(&mut Vec::new()).unwrap();
    for (index, history) in histories.iter().enumerate() {
        wait("recovered record offered", || {
            source.poll(&mut Vec::new()).unwrap();
            offered(&requester, history)
        });
        let scratch: Vec<_> = (0..pages as u32).collect();
        let fetch = fetch_for(&mut requester, history, pages, &scratch);
        assert_eq!(fetch.part.candidate.tier, Tier::Nvme);
        requester.client.fetch(fetch).unwrap();
        let mut result = None;
        wait("recovered restore", || {
            source.poll(&mut Vec::new()).unwrap();
            requester.poll();
            for event in requester.events.drain(..) {
                if let PoolEvent::Fetched { result: done, .. } = event {
                    result = Some(done.map_err(|f| f.error.to_string()));
                }
            }
            result.is_some()
        });
        assert_eq!(result.unwrap(), Ok((pages * PAGE + ROW) as u64));
        let salt = index as u32 + 1;
        let gpu = requester.gpu.pages.lock().unwrap();
        for page in 0..pages {
            assert_eq!(gpu[&(page as u32)], salted_page(salt, page));
        }
        assert_eq!(requester.gpu.rows.lock().unwrap()[&9], salted_row(salt));
    }
    source.close().unwrap();
}

// ---- instances on other hosts: the data channel ---------------------------------

/// Put a record through `writer`, restore it through `requester`, and check the level
/// the lookup gave it.
fn put_then_restore(manager: &Manager, carrier: &str) {
    let mut writer = instance(manager, 1);
    let mut requester = instance(manager, 2);
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
    wait_store_index(&requester, &history);
    let scratch = [200, 201, 202, 203, 204];
    let fetch = fetch_for(&mut requester, &history, 5, &scratch);
    assert_eq!(
        fetch.part.candidate.plan.level,
        flare_selene::levels::Level::RemoteNvme,
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
    // RDMA writes restore windows; uploads use the stream.
    let copies = if carrier == "rdma" { 2 } else { 3 };
    let (windows, bytes) = moved(manager, carrier);
    assert!(windows >= copies, "{carrier}: {windows} windows");
    assert_eq!(bytes, copies * (5 * PAGE + ROW) as u64, "{carrier}");
    if carrier == "rdma" {
        let (windows, bytes) = moved(manager, "stream");
        assert!(windows >= 1, "stream: {windows} windows");
        assert_eq!(bytes, (5 * PAGE + ROW) as u64);
    } else {
        assert_eq!(moved(manager, "rdma"), (0, 0), "nothing went by RDMA");
    }
}

#[test]
fn a_record_put_into_a_manager_on_another_host_is_restored_over_tcp() {
    let manager = network_manager(false);
    put_then_restore(&manager, "stream");
}

#[test]
fn a_record_put_into_a_manager_on_another_host_is_restored_over_tls() {
    let manager = network_manager(true);
    put_then_restore(&manager, "stream");
}

#[cfg(feature = "rdma")]
#[test]
fn a_record_put_into_a_manager_on_another_host_is_restored_over_rdma() {
    let manager = network_manager_with(false, true);
    put_then_restore(&manager, "rdma");
}

#[test]
fn instances_on_other_hosts_come_back_and_copy_after_the_manager_restarts() {
    let mut manager = network_manager(false);
    let mut writer = instance(&manager, 1);
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
    let mut requester = instance(&manager, 2);
    requester.ready();
    wait_store_index(&requester, &history);
    restore(&mut requester, &history, 3, &[400, 401, 402]);
    writer.client.close().unwrap();
    requester.client.close().unwrap();
}

/// Pages that fill the manager's window exactly leave the row to a window of its own.
fn a_window_of_pages_with_the_row_alone(manager: &Manager) {
    let mut writer = instance(manager, 1);
    let mut requester = instance(manager, 2);
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
    wait_store_index(&requester, &history);
    let scratch: Vec<u32> = (1000..1000 + pages as u32).collect();
    restore(&mut requester, &history, pages, &scratch);
    writer.client.close().unwrap();
    requester.client.close().unwrap();
}

#[test]
fn a_row_in_a_window_of_its_own_is_put_and_restored_over_tcp() {
    a_window_of_pages_with_the_row_alone(&network_manager(false));
}

#[test]
fn a_row_in_a_window_of_its_own_is_put_and_restored_through_shared_memory() {
    a_window_of_pages_with_the_row_alone(&Manager::start());
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
) -> PrefixKey {
    let key = history.key_at(pages as u64 * PT).unwrap();
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Dram,
        bytes: (pages * PAGE + ROW) as u64,
    });
    wait("the manager to offer the record", || {
        let ticket = requester.client.begin_lookup(history, 0);
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
    let mut source = instance_on(manager, 1, source_local, &geo());
    let mut requester = instance_on(manager, 2, requester_local, &geo());
    source.ready();
    requester.ready();
    let history = history_of(pages as u32 * PT as u32 + 64);
    let key = offer_host_record(&source, &requester, &history, pages);
    let scratch: Vec<u32> = (0..pages as u32).map(|p| 5000 + p).collect();
    let pin = pin_for(key, pages);
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    source.client.host_pinned(export, pin);
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
    let manager = network_manager(false);
    // Two windows of a 4 MiB manager window each way.
    relay_host_record(&manager, false, false, 700);
    let (windows, bytes) = moved(&manager, "stream");
    assert!(windows >= 4, "{windows} windows crossed the manager twice");
    assert!(bytes >= 2 * (700 * PAGE + ROW) as u64, "{bytes} bytes");
}

#[cfg(feature = "rdma")]
#[test]
fn a_host_record_is_relayed_between_two_instances_on_other_hosts_over_rdma() {
    let manager = network_manager_with(false, true);
    relay_host_record(&manager, false, false, 700);
    let (windows, bytes) = moved(&manager, "rdma");
    assert!(windows >= 4, "{windows} windows crossed the manager twice");
    assert_eq!(bytes, (700 * PAGE + ROW) as u64);
    let (windows, bytes) = moved(&manager, "stream");
    assert!(windows >= 4, "{windows} upload windows crossed the manager");
    assert_eq!(bytes, (700 * PAGE + ROW) as u64);
}

#[test]
fn a_host_record_is_relayed_between_two_instances_on_other_hosts_over_tls() {
    let manager = network_manager(true);
    relay_host_record(&manager, false, false, 5);
}

#[test]
fn a_tls_manager_relays_between_unix_and_tls_clients() {
    let manager = network_manager(true);
    relay_host_record(&manager, false, true, 5);
    relay_host_record(&manager, true, false, 5);
}

#[test]
fn a_remote_instance_serves_an_instance_on_the_managers_host() {
    let manager = network_manager(false);
    relay_host_record(&manager, false, true, 700);
    let (windows, _) = moved(&manager, "stream");
    assert!(windows >= 2, "{windows} windows came in from the source");
}

#[test]
fn an_instance_on_the_managers_host_serves_a_remote_instance() {
    let manager = network_manager(false);
    relay_host_record(&manager, true, false, 700);
    let (windows, _) = moved(&manager, "stream");
    assert!(windows >= 2, "{windows} windows went out to the requester");
}

#[test]
fn a_gpu_checkpoint_on_another_host_is_relayed_through_reserved_staging() {
    let manager = network_manager(false);
    let mut source = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
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
        bytes: (pages * PAGE + ROW) as u64,
    });
    wait("the manager to offer it", || {
        let ticket = requester.client.begin_lookup(&history, 0);
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
        flare_selene::ExportChain {
            nodes: vec![flare_selene::ExportNode {
                key,
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
    let manager = network_manager(false);
    let pages = 700usize;
    let mut source = instance(&manager, 1);
    let mut requester = instance(&manager, 2);
    source.ready();
    requester.ready();
    let history = history_of(pages as u32 * PT as u32 + 64);
    let key = offer_host_record(&source, &requester, &history, pages);
    let scratch: Vec<u32> = (0..pages as u32).map(|p| 5000 + p).collect();
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    let gate = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let TestPinParts(inner) = TestPinParts::new(key, pages);
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
    let pin = pin_for(key, pages);
    let fetch = fetch_for(&mut requester, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    let export = source.event("a second export request", |e| match e {
        PoolEvent::Export(request) => Some(request.export),
        _ => None,
    });
    source.client.host_pinned(export, pin);
    assert_eq!(
        finish_restore(&mut requester),
        Ok((pages * PAGE + ROW) as u64)
    );
    check_restored(&requester, &scratch, pages);
}

/// A `TestPin` built the way `pin_for` builds one.
struct TestPinParts(TestPin);

impl TestPinParts {
    fn new(key: PrefixKey, pages: usize) -> Self {
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
                    first_page: 0,
                    pages: pages as u32,
                    row: true,
                }],
                total_pages: pages as u64,
                row: true,
            },
            bytes: Bytes { data, sums, row, page_bytes: PAGE, row_bytes: ROW },
        })
    }
}

#[path = "client/ranks.rs"]
mod ranks;
#[path = "client/tls.rs"]
mod tls;

#[test]
fn offload_startup_and_restore_cover_every_tier_combination() {
    use flare_selene::{
        HostRegistrar, Registration, SplitMapping,
        offload::*,
        pool::{Budget, PoolBuilder, PoolOptions},
    };
    use std::cell::RefCell;

    // With one rank, the split mapping has the ordinary record layout.
    struct OneRank(CountingRegistrar);
    struct Split(SlabPool);
    impl SplitMapping for Split {
        fn address(&self) -> usize {
            self.0.slot_address(flare_selene::slab::Slot(0))
        }
        fn nbytes(&self) -> usize {
            self.0.bytes() as usize
        }
        fn part_address(&self, index: usize) -> usize {
            assert_eq!(index, 0);
            self.address()
        }
    }
    impl HostRegistrar for OneRank {
        fn register(
            &self,
            context: usize,
            address: usize,
            bytes: usize,
        ) -> Result<Box<dyn Registration>> {
            self.0.register(context, address, bytes)
        }
        fn map_split(&self, requests: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
            assert_eq!(requests.len(), 1);
            Ok(Box::new(Split(SlabPool::new(
                PAGE as u64,
                requests[0].2 / PAGE,
                0,
            )?)))
        }
    }
    #[derive(Default)]
    struct Copiers(RefCell<Vec<GpuMemory>>);
    impl CopierFactory for Copiers {
        fn mapped(
            &self,
            _: &[(usize, i32)],
            geometry: Vec<CacheGeometry>,
            _: &SlabPool,
        ) -> Result<flare_selene::SharedCopier> {
            let (copier, gpu) = SimulatedGpu::shared_mapped(&geometry)?;
            self.0.borrow_mut().push(gpu);
            Ok(copier)
        }
        fn unmapped(
            &self,
            _: &[(usize, i32)],
            geometry: Vec<CacheGeometry>,
        ) -> Result<Box<dyn flare_selene::PageCopier>> {
            let (copier, gpu) = SimulatedGpu::new(&geometry)?;
            self.0.borrow_mut().push(gpu);
            Ok(Box::new(copier))
        }
    }

    let mut geometry = geo();
    geometry[0].row.unit_bytes = 3 * PAGE as u64;
    let manager = Manager::start();
    for warm in [false, true] {
        for disk in [false, true] {
            for pooled in [false, true] {
                let name = format!("matrix-{warm}-{disk}-{pooled}");
                let Some(directory) = offload_directory(&name) else {
                    return;
                };
                let options = PrefixCacheOffload {
                    memory_bytes: Some(if warm { 512 * PAGE as u64 } else { 0 }),
                    disk: disk.then(|| directory.clone()),
                    disk_limit: Some(1 << 20),
                    pool: pooled.then(|| PoolOptions {
                        address: manager.address(),
                        ca: None,
                    }),
                    ..Default::default()
                };
                let registrar = Arc::new(OneRank(CountingRegistrar::default()));
                let factory = Copiers::default();
                let mut startup = Startup::default();
                if pooled {
                    startup
                        .start_pool(
                            PoolBuilder {
                                options: options.pool.as_ref().unwrap(),
                                identity: [7; 32],
                                geometry: &geometry,
                                gpu_uuids: vec![[1; 16]],
                                registration_context: 0,
                                budget: Budget {
                                    host: Ok(u64::MAX),
                                    devices: vec![(u64::MAX, false)],
                                },
                                peers: None,
                                disk,
                            },
                            registrar.clone(),
                            |approved| {
                                assert!(approved);
                                Ok(true)
                            },
                        )
                        .unwrap();
                }
                let preparation = prepare_with_inputs(
                    &options,
                    std::path::Path::new("unused"),
                    [7; 32],
                    geometry.clone(),
                    vec![(0, 0)],
                    8,
                    0,
                    None,
                    registrar.as_ref(),
                )
                .unwrap();
                if !warm && !disk && !pooled {
                    assert_eq!(preparation.summary(), "disabled");
                    assert!(matches!(preparation, Preparation::None(_)));
                    assert_eq!(startup.attach(preparation, &factory).unwrap(), "disabled");
                    assert!(startup.binding(0).is_none());
                    std::fs::remove_dir_all(directory).unwrap();
                    continue;
                }
                let Preparation::Store(prepared) = &preparation else {
                    panic!("{name}: disabled")
                };
                assert_eq!(prepared.warm_bytes, options.memory_bytes.unwrap());
                assert_eq!(prepared.memory_pool.is_some(), warm || disk);
                if let Some(pool) = &prepared.memory_pool {
                    assert_eq!(
                        pool.slot_count(),
                        intake_slots(&prepared.layout, 8) + usize::from(warm) * 512
                    );
                    assert_eq!(pool.rank_parts().is_some(), warm && !disk && !pooled);
                }
                // The manager's two regions are separate from the local shared mapping.
                assert_eq!(
                    registrar.0.calls.lock().unwrap().len(),
                    usize::from(pooled) * 2 + usize::from(disk || (warm && pooled))
                );
                startup.attach(preparation, &factory).unwrap();
                let mut store =
                    open(startup.binding(0).unwrap().take().unwrap(), &factory).unwrap();
                let stats = store.stats();
                let intake = if warm || disk {
                    intake_slots(&Layout::new(&geometry).unwrap(), 8)
                } else {
                    0
                };
                assert_eq!(stats.slots_used, 0, "{name}");
                assert_eq!(stats.slots_total, intake + usize::from(warm) * 512, "{name}");
                assert_eq!(stats.intake_slots, intake, "{name}");
                let salt = 1 + u32::from(warm) * 4 + u32::from(disk) * 2 + u32::from(pooled);
                let (checkpoint, history) = publication(&factory.0.borrow()[0], salt, 2);
                let row: Vec<u8> = (0..3 * PAGE)
                    .map(|i| ((i * 11 + salt as usize * 17 + 3) % 251) as u8)
                    .collect();
                for gpu in factory.0.borrow().iter() {
                    publication(gpu, salt, 2);
                    gpu.rows.lock().unwrap().insert(salt, row.clone());
                }
                store_and_wait(store.as_mut(), &checkpoint);
                if warm || disk {
                    store.evicted(checkpoint.key, None);
                    let hit = store.lookup(&history, 0, u64::MAX).unwrap();
                    store
                        .fetch(
                            hit,
                            flare_selene::InboundTicket {
                                key: checkpoint.key,
                                pages: vec![PageId(300), PageId(301)],
                                slot: StateSlotId(9),
                            },
                        )
                        .unwrap();
                    let deadline = Instant::now() + Duration::from_secs(20);
                    loop {
                        let mut events = Vec::new();
                        store.poll(&mut events).unwrap();
                        if let Some(StoreEvent::Fetched { result, .. }) = events
                            .into_iter()
                            .find(|event| matches!(event, StoreEvent::Fetched { .. }))
                        {
                            assert_eq!(
                                result.map_err(|error| error.to_string()),
                                Ok((2 * PAGE + row.len()) as u64),
                                "{name}"
                            );
                            break;
                        }
                        assert!(Instant::now() < deadline, "{name}: local restore stalled");
                        thread::sleep(Duration::from_millis(2));
                    }
                    let memories = factory.0.borrow();
                    let restored = memories
                        .iter()
                        .find(|gpu| gpu.rows.lock().unwrap().contains_key(&9))
                        .unwrap();
                    for page in 0..2 {
                        assert_eq!(
                            restored.pages.lock().unwrap()[&(300 + page as u32)],
                            salted_page(salt, page)
                        );
                    }
                    assert_eq!(restored.rows.lock().unwrap()[&9], row);
                }
                if pooled {
                    let mut options = config(&manager, 2);
                    options.scope = Scope::new([7; 32], fingerprint(&geometry), &geometry).unwrap();
                    let (copier, gpu) = SimulatedGpu::shared(&geometry).unwrap();
                    let mut requester = Instance {
                        client: PoolClient::start(
                            options,
                            None,
                            copier,
                            Arc::new(OkRegistrar),
                            flare_selene::pool::Roots::System,
                            STARTUP_WAIT,
                        )
                        .unwrap(),
                        gpu,
                        events: Vec::new(),
                    };
                    requester.ready();
                    drive(store.as_mut(), "the published record", |_| {
                        let ticket = requester.client.begin_lookup(&history, 0);
                        wait("the lookup", || {
                            !matches!(ticket.state(), LookupState::Pending)
                        });
                        matches!(ticket.state(), LookupState::Ready(answer)
                            if answer.candidates.iter().any(|candidate| candidate.plan.key == checkpoint.key)
                                && answer.candidates.iter().all(|candidate| (!warm && !disk) || candidate.tier != Tier::Gpu))
                    });
                    let fetch = fetch_for(&mut requester, &history, 2, &[300, 301]);
                    requester.client.fetch(fetch).unwrap();
                    let deadline = Instant::now() + Duration::from_secs(20);
                    loop {
                        let mut events = Vec::new();
                        store.poll(&mut events).unwrap();
                        for event in events {
                            if let StoreEvent::ExportRequested { export, key, .. } = event {
                                assert!(!warm && !disk, "{name}: evicted GPU was selected");
                                assert_eq!(key, checkpoint.key);
                                store
                                    .export_pinned(
                                        export,
                                        flare_selene::ExportChain {
                                            nodes: vec![flare_selene::ExportNode {
                                                key,
                                                pages: checkpoint.chain_pages.clone(),
                                            }],
                                            skip_pages: 0,
                                            row: Some(checkpoint.slot),
                                        },
                                    )
                                    .unwrap();
                            }
                        }
                        requester.poll();
                        if let Some(at) = requester
                            .events
                            .iter()
                            .position(|event| matches!(event, PoolEvent::Fetched { .. }))
                        {
                            let PoolEvent::Fetched { result, .. } = requester.events.remove(at)
                            else {
                                unreachable!()
                            };
                            assert_eq!(
                                result.map_err(|error| error.error.to_string()),
                                Ok((2 * PAGE + row.len()) as u64),
                                "{name}"
                            );
                            break;
                        }
                        assert!(Instant::now() < deadline, "{name}: restore stalled");
                        thread::sleep(Duration::from_millis(2));
                    }
                    for page in 0..2 {
                        assert_eq!(
                            requester.gpu.pages.lock().unwrap()[&(300 + page as u32)],
                            salted_page(salt, page)
                        );
                    }
                    assert_eq!(requester.gpu.rows.lock().unwrap()[&9], row);
                    requester.client.close().unwrap();
                }
                store.evicted(checkpoint.key, None);
                store.close().unwrap();
                drop(store);
                startup.reset();
                assert_eq!(
                    *registrar.0.drops.lock().unwrap(),
                    registrar.0.calls.lock().unwrap().len(),
                    "{name}"
                );
                std::fs::remove_dir_all(directory).unwrap();
            }
        }
    }
}
