//! A multi-process instance against the real manager, in process: rank 0 and the peer
//! ranks' agents, each with its own simulated GPU, restore records a single-process
//! instance of the same shape holds, slice by slice.

use std::{
    sync::{Arc, Mutex},
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
    PoolConfig {
        address: PoolAddress::Unix(manager.socket.clone()),
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

fn instance(manager: &Manager, world: usize) -> Instance {
    let mut links = Vec::new();
    let mut peer_ends = Vec::new();
    for _ in 1..world {
        let (zero, peer) = memory_links();
        links.push(zero);
        peer_ends.push(peer);
    }
    let (copier, gpu0) = SimulatedGpu::shared(&[local()]).unwrap();
    let mut gpus: Vec<[u8; 16]> = vec![[2; 16]];
    gpus.extend((1..world).map(|r| [2 + r as u8; 16]));
    let rank0 = PoolClient::start_ranked(
        config(manager, "requester", world, gpus.clone()),
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
                address: PoolAddress::Unix(manager.socket.clone()),
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
    let manager = Manager::start();
    let mut source = source(&manager, 2);
    let mut requester = instance(&manager, 2);
    wait("the source", || source.client.status() == PoolStatus::Ready);
    requester.ready();
    let history = history_of(4000);
    let pages = 4;
    let key = offer(&source, &history, 2, pages);
    let scratch = [100, 101, 102, 103];
    wait("the offer to be indexed", || {
        let ticket = requester.rank0.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + Duration::from_secs(2);
        while matches!(ticket.state(), LookupState::Pending) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        matches!(ticket.state(), LookupState::Ready(a) if !a.candidates.is_empty())
    });
    let fetch = fetch_for(&requester.rank0, 2, &history, pages, &scratch);
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
        .host_pinned(export.export, pin(2, key, 11, pages));
    let (page, row) = whole(2);
    let result = requester.event("the restore", |e| match e {
        PoolEvent::Fetched { result, .. } => {
            Some(result.as_ref().map(|b| *b).map_err(|f| f.error.to_string()))
        }
        _ => None,
    });
    assert_eq!(result, Ok((pages * page + row) as u64));
    assert_slices(&requester, 2, pages, &scratch);
    requester.close();
    let _ = source.client.close();
}

#[allow(dead_code)]
fn unused(_: &Mutex<()>) {}
