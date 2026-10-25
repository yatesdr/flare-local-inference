//! A multi-process instance against the real manager, in process: rank 0 and the peer
//! ranks' agents, each with its own simulated GPU, restore records a single-process
//! instance of the same shape holds, slice by slice.

use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use crate::{
    CacheGeometry, History, PageId, PrefixKey, Result, StateSlotId,
    characterize::Ranking,
    geometry::fingerprint,
    plan::{self, Identity, Request},
    pool::{
        Checksums, FetchPart, HostChain, HostPin, HostRecord, HostWindow, IndexEntry, LookupState,
        PoolAddress, PoolClient, PoolConfig, PoolEvent, PoolFetch, PoolStatus, Scope, Tier,
        agent::{AgentConfig, RankAgent},
        manager_tests::{Manager, OkRegistrar, PT, history_of, wait},
        rank::{RankSet, memory_links},
    },
    record::checksum,
    testing::{GpuMemory, SimulatedGpu, TestHistory, geometry},
};

/// One rank's pages: two planes of 4 KiB, and a 4 KiB row.
fn local() -> CacheGeometry {
    geometry(PT, &[("k", 4096), ("v", 4096)], 4096)
}

fn ranks(world: usize) -> Vec<CacheGeometry> {
    vec![local(); world]
}

/// Bytes of a whole page record of a `world`-rank instance, and of its row.
fn whole(world: usize) -> (usize, usize) {
    (world * 8192, world * 4096)
}

fn scope(world: usize) -> Scope {
    let geometry = ranks(world);
    Scope::new("t", [7; 32], fingerprint(&geometry), &geometry).unwrap()
}

fn config(manager: &Manager, name: &str, world: usize, gpus: Vec<[u8; 16]>) -> PoolConfig {
    config_at(PoolAddress::Unix(manager.socket.clone()), name, world, gpus)
}

fn config_at(address: PoolAddress, name: &str, world: usize, gpus: Vec<[u8; 16]>) -> PoolConfig {
    PoolConfig {
        address,
        tenant: "t".into(),
        ca: None,
        scope: scope(world),
        instance_name: name.into(),
        gpu_uuids: gpus,
        export_slots: 2,
        claim_slots: 2,
        level_order: None,
        registration_context: 0,
        approve: None,
    }
}

fn bytes_of(world: usize, pages: usize) -> Vec<u8> {
    let (page, row) = whole(world);
    (0..pages * page + row)
        .map(|i| (i.wrapping_mul(31) + i / 4096 * 7 + 5) as u8)
        .collect()
}

/// A host pin over known whole-record bytes, serving the whole-page checksums persisted with
/// a composite record.
struct Pin {
    chain: HostChain,
    data: Vec<u8>,
    page: usize,
    row: usize,
    sums: Vec<u64>,
    row_sum: u64,
}

impl HostPin for Pin {
    fn chain(&self) -> &HostChain {
        &self.chain
    }
    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums> {
        let from = window.first_page as usize * self.page;
        let n = window.page_count as usize * self.page;
        dst[..n].copy_from_slice(&self.data[from..from + n]);
        if window.row {
            dst[n..].copy_from_slice(&self.data[self.data.len() - self.row..]);
        }
        Ok(Checksums {
            pages: self.sums[window.first_page as usize..][..window.page_count as usize].to_vec(),
            row: window.row.then_some(self.row_sum),
        })
    }
}

fn pin(world: usize, key: PrefixKey, generation: u64, pages: usize) -> Box<dyn HostPin> {
    let (page, row) = whole(world);
    let data = bytes_of(world, pages);
    let sums = (0..pages)
        .map(|p| checksum(&data[p * page..(p + 1) * page]))
        .collect();
    let row_sum = checksum(&data[pages * page..]);
    Box::new(Pin {
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
        page,
        row,
        sums,
        row_sum,
    })
}

struct Source {
    client: PoolClient,
    _gpu: GpuMemory,
}

fn source(manager: &Manager, world: usize) -> Source {
    let (copier, gpu) = SimulatedGpu::shared(&ranks(world)).unwrap();
    let client = PoolClient::start(
        config(manager, "source", world, vec![[1; 16]]),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    Source { client, _gpu: gpu }
}

/// A multi-process requester: rank 0's client and the agents of ranks `1..world`.
struct Instance {
    rank0: PoolClient,
    memory: Vec<GpuMemory>,
    agents: Vec<RankAgent>,
    events: Vec<PoolEvent>,
}

/// How each rank of an instance reaches the manager: over its Unix socket (shared memory)
/// or over the network (its own data channel).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Reach {
    Local,
    Network,
}

fn instance(manager: &Manager, world: usize) -> Instance {
    instance_reaching(manager, world, &vec![Reach::Local; world])
}

fn address_for(manager: &Manager, reach: Reach) -> PoolAddress {
    match reach {
        Reach::Local => PoolAddress::Unix(manager.socket.clone()),
        Reach::Network => manager.remote().expect("a network manager").0,
    }
}

fn instance_reaching(manager: &Manager, world: usize, reach: &[Reach]) -> Instance {
    instance_seeded(manager, world, reach, 2)
}

/// As [`instance_reaching`]; the GPUs, and so the instance, differ with `seed`.
fn instance_seeded(manager: &Manager, world: usize, reach: &[Reach], seed: u8) -> Instance {
    let mut links = Vec::new();
    let mut peer_ends = Vec::new();
    for _ in 1..world {
        let (zero, peer) = memory_links();
        links.push(zero);
        peer_ends.push(peer);
    }
    let (copier, gpu0) = SimulatedGpu::shared(&[local()]).unwrap();
    let gpus: Vec<[u8; 16]> = (0..world).map(|r| [seed + r as u8; 16]).collect();
    let rank0 = PoolClient::start_ranked(
        config_at(
            address_for(manager, reach[0]),
            "requester",
            world,
            gpus.clone(),
        ),
        RankSet { links },
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    let mut memory = vec![gpu0];
    let mut agents = Vec::new();
    for (index, link) in peer_ends.into_iter().enumerate() {
        let rank = index + 1;
        let (copier, gpu) = SimulatedGpu::shared(&[local()]).unwrap();
        memory.push(gpu);
        agents.push(
            RankAgent::start(AgentConfig {
                address: address_for(manager, reach[rank]),
                ca: None,
                scope: scope(world),
                rank,
                world,
                instance_name: format!("agent-{rank}"),
                link,
                copier,
                registrar: Arc::new(OkRegistrar),
                registration_context: 0,
                gpu_uuid: gpus[rank],
            })
            .unwrap(),
        );
    }
    Instance {
        rank0,
        memory,
        agents,
        events: Vec::new(),
    }
}

impl Instance {
    fn ready(&self) {
        wait("rank 0 to be Ready", || {
            self.rank0.status() == PoolStatus::Ready
        });
    }

    fn poll(&mut self) {
        let mut events = Vec::new();
        self.rank0.poll(&mut events).unwrap();
        self.events.extend(events);
    }

    fn event<T>(&mut self, what: &str, mut take: impl FnMut(&PoolEvent) -> Option<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(20);
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

    fn close(mut self) {
        let _ = self.rank0.close();
        for agent in &mut self.agents {
            let _ = agent.close();
        }
    }
}

fn identity(world: usize) -> Identity {
    Identity {
        tenant: "t".into(),
        model: [7; 32],
        layout: fingerprint(&ranks(world)),
    }
}

fn fetch_for(
    client: &PoolClient,
    world: usize,
    history: &TestHistory,
    pages: usize,
    scratch: &[u32],
) -> PoolFetch {
    let ticket = client.begin_lookup(history, Vec::new());
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
        identity: &identity(world),
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

/// Every rank's destination pages and row hold exactly its slice of the source record.
fn assert_slices(instance: &Instance, world: usize, pages: usize, scratch: &[u32]) {
    let data = bytes_of(world, pages);
    let (page, row) = whole(world);
    for (rank, memory) in instance.memory.iter().enumerate() {
        let landed = memory.pages.lock().unwrap();
        for (i, id) in scratch.iter().enumerate() {
            let from = i * page + rank * 8192;
            assert_eq!(
                landed.get(id).map(Vec::as_slice),
                Some(&data[from..from + 8192]),
                "rank {rank} page {i}"
            );
        }
        let rows = memory.rows.lock().unwrap();
        let from = pages * page + rank * 4096;
        assert_eq!(
            rows.get(&9).map(Vec::as_slice),
            Some(&data[from..from + 4096]),
            "rank {rank}'s row (of {row})"
        );
    }
}

fn offer(source: &Source, history: &TestHistory, world: usize, pages: usize) -> PrefixKey {
    let key = history.key_at(pages as u64 * PT).unwrap();
    let (page, row) = whole(world);
    source.client.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Dram,
        generation: 11,
        bytes: (pages * page + row) as u64,
    });
    key
}

#[test]
fn a_two_rank_instance_restores_a_single_process_record_slice_by_slice() {
    restore_through(&Manager::start(), 2, &[Reach::Local, Reach::Local]);
}

#[test]
fn a_peer_on_its_own_data_channel_gets_its_slices_while_rank_zero_reads_shared_memory() {
    let manager = Manager::start_network(false);
    restore_through(&manager, 2, &[Reach::Local, Reach::Network]);
}

#[test]
fn every_rank_on_its_own_data_channel_restores_a_record() {
    let manager = Manager::start_network(false);
    restore_through(&manager, 2, &[Reach::Network, Reach::Network]);
}

#[test]
fn three_ranks_mix_shared_memory_and_channels() {
    let manager = Manager::start_network(false);
    restore_through(&manager, 3, &[Reach::Local, Reach::Network, Reach::Local]);
}

fn restore_through(manager: &Manager, world: usize, reach: &[Reach]) {
    let mut source = source(manager, world);
    let mut requester = instance_reaching(manager, world, reach);
    wait("the source", || source.client.status() == PoolStatus::Ready);
    requester.ready();
    let history = history_of(4000);
    let pages = 4;
    let key = offer(&source, &history, world, pages);
    let scratch = [100, 101, 102, 103];
    wait("the offer to be indexed", || {
        let ticket = requester.rank0.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let fetch = fetch_for(&requester.rank0, world, &history, pages, &scratch);
    requester.rank0.fetch(fetch).unwrap();
    let export = loop {
        let mut events = Vec::new();
        source.client.poll(&mut events).unwrap();
        if let Some(request) = events.into_iter().find_map(|e| match e {
            PoolEvent::Export(request) => Some(request),
            _ => None,
        }) {
            break request;
        }
        thread::sleep(Duration::from_millis(2));
    };
    source
        .client
        .host_pinned(export.export, pin(world, key, 11, pages));
    let (page, row) = whole(world);
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * page + row) as u64));
    assert_slices(&requester, world, pages, &scratch);
    requester.close();
    let _ = source.client.close();
}

// ---- exports from a multi-process instance ---------------------------------------

use crate::{ExportChain, ExportNode};

/// Put a GPU checkpoint of `pages` pages and its row on every rank of `source`, in the
/// pages `first..` and slot 5, and announce it. Returns its key.
fn publish(source: &Instance, history: &TestHistory, world: usize, pages: usize) -> PrefixKey {
    let data = bytes_of(world, pages);
    let (page, _) = whole(world);
    for (rank, memory) in source.memory.iter().enumerate() {
        for p in 0..pages {
            let from = p * page + rank * 8192;
            memory
                .pages
                .lock()
                .unwrap()
                .insert(20 + p as u32, data[from..from + 8192].to_vec());
        }
        let from = pages * page + rank * 4096;
        memory
            .rows
            .lock()
            .unwrap()
            .insert(5, data[from..from + 4096].to_vec());
    }
    let key = history.key_at(pages as u64 * PT).unwrap();
    let (page, row) = whole(world);
    source.rank0.saved(IndexEntry {
        key,
        parent: None,
        bucket: history.bucket_at(key.consumed_tokens).unwrap(),
        tail_pages: pages as u32,
        row: true,
        tier: Tier::Gpu,
        generation: 5,
        bytes: (pages * page + row) as u64,
    });
    key
}

fn wait_offered(client: &PoolClient, history: &TestHistory) {
    wait("the offer to be indexed", || {
        let ticket = client.begin_lookup(history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
}

/// The worker's side of a GPU export: pin the chain, answer, and wait for the pin to end.
fn serve_export(source: &mut Instance, key: PrefixKey, pages: usize) {
    let export = source.event("an export request", |e| match e {
        PoolEvent::Export(request) => {
            assert_eq!((request.tier, request.generation), (Tier::Gpu, 5));
            Some(request.export)
        }
        _ => None,
    });
    source.rank0.export_pinned(
        export,
        ExportChain {
            nodes: vec![ExportNode {
                key,
                generation: 5,
                pages: (0..pages).map(|p| PageId(20 + p as u32)).collect(),
            }],
            skip_pages: 0,
            row: Some(StateSlotId(5)),
        },
    );
    source.event("the pin to be released", |e| match e {
        PoolEvent::ExportEnded { export: ended } if *ended == export => Some(()),
        _ => None,
    });
}

/// A plain single-process instance of the same shape, as a requester.
struct Plain {
    client: PoolClient,
    memory: GpuMemory,
}

fn plain(manager: &Manager, world: usize, reach: Reach) -> Plain {
    let (copier, memory) = SimulatedGpu::shared(&ranks(world)).unwrap();
    let client = PoolClient::start(
        config_at(address_for(manager, reach), "plain", world, vec![[9; 16]]),
        copier,
        Arc::new(OkRegistrar),
    )
    .unwrap();
    Plain { client, memory }
}

fn expect_fetched(client: &mut PoolClient, what: &str, want: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut events = Vec::new();
        client.poll(&mut events).unwrap();
        for event in events {
            if let PoolEvent::Fetched { result, .. } = event {
                assert_eq!(
                    result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()),
                    Ok(want),
                    "{what}"
                );
                return;
            }
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

fn export_to_plain(manager: &Manager, world: usize, source_reach: &[Reach], plain_reach: Reach) {
    let mut source = instance_reaching(manager, world, source_reach);
    let mut requester = plain(manager, world, plain_reach);
    source.ready();
    wait("the requester", || {
        requester.client.status() == PoolStatus::Ready
    });
    let history = history_of(4000);
    let pages = 4;
    let key = publish(&source, &history, world, pages);
    wait_offered(&requester.client, &history);
    let scratch = [300, 301, 302, 303];
    let fetch = fetch_for(&requester.client, world, &history, pages, &scratch);
    requester.client.fetch(fetch).unwrap();
    serve_export(&mut source, key, pages);
    let (page, row) = whole(world);
    expect_fetched(
        &mut requester.client,
        "the restore",
        (pages * page + row) as u64,
    );
    let data = bytes_of(world, pages);
    let landed = requester.memory.pages.lock().unwrap();
    for (i, id) in scratch.iter().enumerate() {
        assert_eq!(
            landed.get(id).map(Vec::as_slice),
            Some(&data[i * page..(i + 1) * page]),
            "page {i} of the whole record"
        );
    }
    assert_eq!(
        requester
            .memory
            .rows
            .lock()
            .unwrap()
            .get(&9)
            .map(Vec::as_slice),
        Some(&data[pages * page..]),
        "the whole row"
    );
    drop(landed);
    source.close();
    let _ = requester.client.close();
}

#[test]
fn a_two_rank_instance_exports_a_gpu_checkpoint_to_a_single_process_instance() {
    export_to_plain(
        &Manager::start(),
        2,
        &[Reach::Local, Reach::Local],
        Reach::Local,
    );
}

#[test]
fn a_peer_on_a_channel_writes_its_slice_of_an_export_to_the_manager() {
    let manager = Manager::start_network(false);
    export_to_plain(&manager, 2, &[Reach::Local, Reach::Network], Reach::Local);
}

#[test]
fn a_whole_instance_on_channels_exports_to_a_remote_single_process_instance() {
    let manager = Manager::start_network(false);
    export_to_plain(&manager, 3, &[Reach::Network; 3], Reach::Network);
}

#[test]
fn three_ranks_mixing_shared_memory_and_channels_export_to_a_multi_process_instance() {
    let manager = Manager::start_network(false);
    let world = 3;
    let mut source = instance_reaching(
        &manager,
        world,
        &[Reach::Local, Reach::Network, Reach::Local],
    );
    let mut requester = instance_seeded(
        &manager,
        world,
        &[Reach::Local, Reach::Local, Reach::Network],
        40,
    );
    source.ready();
    requester.ready();
    let history = history_of(4000);
    let pages = 4;
    let key = publish(&source, &history, world, pages);
    wait_offered(&requester.rank0, &history);
    let scratch = [300, 301, 302, 303];
    let fetch = fetch_for(&requester.rank0, world, &history, pages, &scratch);
    requester.rank0.fetch(fetch).unwrap();
    serve_export(&mut source, key, pages);
    let (page, row) = whole(world);
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * page + row) as u64));
    assert_slices(&requester, world, pages, &scratch);
    source.close();
    requester.close();
}

// ---- puts from a multi-process instance -------------------------------------------

use crate::pool::{PutRequest, PutSource};

/// Put the checkpoint `publish` placed on every rank of `writer` into the manager's store.
fn put_from(writer: &mut Instance, history: &TestHistory, pages: usize) -> PrefixKey {
    let key = history.key_at(pages as u64 * PT).unwrap();
    writer
        .rank0
        .put(PutRequest {
            key,
            parent: None,
            bucket: history.bucket_at(key.consumed_tokens).unwrap(),
            chain_pages: pages as u64,
            tail_pages: pages as u64,
            row: true,
            generation: 1,
            source: PutSource::Gpu {
                pages: (0..pages).map(|p| PageId(20 + p as u32)).collect(),
                slot: StateSlotId(5),
            },
        })
        .unwrap();
    key
}

/// Fill every rank's GPU as `publish` does, without announcing.
fn fill_gpus(writer: &Instance, world: usize, pages: usize) {
    let data = bytes_of(world, pages);
    let (page, _) = whole(world);
    for (rank, memory) in writer.memory.iter().enumerate() {
        for p in 0..pages {
            let from = p * page + rank * 8192;
            memory
                .pages
                .lock()
                .unwrap()
                .insert(20 + p as u32, data[from..from + 8192].to_vec());
        }
        let from = pages * page + rank * 4096;
        memory
            .rows
            .lock()
            .unwrap()
            .insert(5, data[from..from + 4096].to_vec());
    }
}

fn put_then_restore(
    manager: &Manager,
    world: usize,
    writer_reach: &[Reach],
    reader: Option<&[Reach]>,
) {
    let mut writer = instance_reaching(manager, world, writer_reach);
    writer.ready();
    let history = history_of(4000);
    let pages = 4;
    fill_gpus(&writer, world, pages);
    put_from(&mut writer, &history, pages);
    let outcome = writer.event("the put", |e| match e {
        PoolEvent::Put { outcome, .. } => Some(format!("{outcome:?}")),
        _ => None,
    });
    assert!(outcome.starts_with("Stored"), "{outcome}");
    let (page, row) = whole(world);
    let scratch = [300, 301, 302, 303];
    match reader {
        None => {
            // A single-process instance of the same shape restores the whole record, which
            // proves the checksums the record keeps are the whole-page ones.
            let mut plain = plain(manager, world, Reach::Local);
            wait("the reader", || plain.client.status() == PoolStatus::Ready);
            wait_offered(&plain.client, &history);
            let fetch = fetch_for(&plain.client, world, &history, pages, &scratch);
            plain.client.fetch(fetch).unwrap();
            expect_fetched(
                &mut plain.client,
                "the restore",
                (pages * page + row) as u64,
            );
            let data = bytes_of(world, pages);
            let landed = plain.memory.pages.lock().unwrap();
            for (i, id) in scratch.iter().enumerate() {
                assert_eq!(
                    landed.get(id).map(Vec::as_slice),
                    Some(&data[i * page..(i + 1) * page]),
                    "page {i}"
                );
            }
            assert_eq!(
                plain.memory.rows.lock().unwrap().get(&9).map(Vec::as_slice),
                Some(&data[pages * page..])
            );
            drop(landed);
            let _ = plain.client.close();
        }
        Some(reach) => {
            let mut other = instance_seeded(manager, world, reach, 60);
            other.ready();
            wait_offered(&other.rank0, &history);
            let fetch = fetch_for(&other.rank0, world, &history, pages, &scratch);
            other.rank0.fetch(fetch).unwrap();
            let result = other.event("the restore", |e| match e {
                PoolEvent::Fetched { result, .. } => {
                    Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
                }
                _ => None,
            });
            assert_eq!(result, Ok((pages * page + row) as u64));
            assert_slices(&other, world, pages, &scratch);
            other.close();
        }
    }
    writer.close();
}

#[test]
fn a_two_rank_instance_puts_a_record_a_single_process_instance_restores() {
    put_then_restore(&Manager::start(), 2, &[Reach::Local, Reach::Local], None);
}

#[test]
fn a_peer_on_a_channel_writes_its_slice_of_a_put() {
    let manager = Manager::start_network(false);
    put_then_restore(&manager, 2, &[Reach::Local, Reach::Network], None);
}

#[test]
fn three_ranks_on_channels_put_and_another_multi_process_instance_restores() {
    let manager = Manager::start_network(false);
    put_then_restore(
        &manager,
        3,
        &[Reach::Network, Reach::Network, Reach::Network],
        Some(&[Reach::Local, Reach::Network, Reach::Local]),
    );
}
