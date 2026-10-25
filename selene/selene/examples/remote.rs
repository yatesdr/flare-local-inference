//! Qualifies a pool manager on another host from an instance: puts records into its store,
//! restores them and reports what that cost, with the pages of every restore checked.
//!
//! ```text
//! cargo run --release --features test-support --example remote -- \
//!     --address HOST:PORT [--ca FILE] [--mode MODE] [--pages N] [--page-kib K] \
//!     [--records R] [--rounds R] [--name NAME] [--gpu BYTE]
//! ```
//!
//! Modes: `check` puts the records and restores each once, comparing every page with what
//! was put; `bench` times the puts and `--rounds` restores of each record without keeping
//! the pages; `hang` starts a large restore and then waits, for a harness that kills this
//! process in the middle of it. `export` offers the records as host records of its own and
//! serves the manager's export requests for `--seconds`; `import` (or `import-check`, which
//! compares every page) restores what another instance exports, through the manager's relay.
//! The device is simulated, so the numbers are the network and the host copies, not a GPU.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use flare_selene::{
    CacheGeometry, History, HostRegistrar, Job, Layout, PageCopier, PageId, Registration, Reply,
    Result, SplitMapping, StateSlotId,
    geometry::fingerprint,
    plan::{self, Identity, Request},
    pool::{
        Checksums, FetchPart, HostChain, HostPin, HostRecord, HostWindow, IndexEntry, LookupState,
        PoolAddress, PoolClient, PoolConfig, PoolEvent, PoolFetch, PoolStatus, PutBytes,
        PutRequest, PutSource, Scope, Tier,
    },
    record::checksum,
    testing::{SimulatedGpu, TestHistory, geometry},
};

const PT: u64 = 16;
const ROW: usize = 4096;

struct Options {
    address: String,
    ca: Option<std::path::PathBuf>,
    mode: String,
    pages: usize,
    page_kib: u64,
    records: usize,
    rounds: usize,
    name: String,
    gpu: u8,
    seconds: u64,
}

fn options() -> Options {
    let mut options = Options {
        address: String::new(),
        ca: None,
        mode: "check".into(),
        pages: 16,
        page_kib: 1024,
        records: 2,
        rounds: 3,
        name: "remote".into(),
        gpu: 1,
        seconds: 3600,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--address" => options.address = value,
            "--ca" => options.ca = Some(value.into()),
            "--mode" => options.mode = value,
            "--pages" => options.pages = value.parse().unwrap(),
            "--page-kib" => options.page_kib = value.parse().unwrap(),
            "--records" => options.records = value.parse().unwrap(),
            "--rounds" => options.rounds = value.parse().unwrap(),
            "--name" => options.name = value,
            "--gpu" => options.gpu = value.parse().unwrap(),
            "--seconds" => options.seconds = value.parse().unwrap(),
            other => panic!("unknown option {other}"),
        }
    }
    assert!(
        !options.address.is_empty(),
        "--address HOST:PORT is required"
    );
    options
}

/// Registration is a no-op: nothing here is a device.
struct Registrar;
struct Registered;
impl Registration for Registered {
    fn device_address(&self) -> u64 {
        0
    }
}
impl HostRegistrar for Registrar {
    fn register(&self, _: usize, _: usize, _: usize) -> Result<Box<dyn Registration>> {
        Ok(Box::new(Registered))
    }
    fn map_split(&self, _: &[(usize, i32, usize)]) -> Result<Box<dyn SplitMapping>> {
        Err("not used".into())
    }
}

/// A device that accepts every copy and keeps nothing, for timing the network alone.
struct Null {
    layout: Layout,
    replies: Vec<Reply>,
}

impl PageCopier for Null {
    fn layout(&self) -> &Layout {
        &self.layout
    }
    fn submit(&mut self, job: Job) -> Result<()> {
        self.replies.push(Reply {
            id: job.id,
            window: job.window,
            direction: job.direction,
            result: Ok(()),
            bytes: 0,
            elapsed: Duration::from_micros(1),
        });
        Ok(())
    }
    fn poll(&mut self, replies: &mut Vec<Reply>) {
        replies.append(&mut self.replies);
    }
    fn in_flight(&self) -> usize {
        self.replies.len()
    }
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn page_bytes(record: usize, page: usize, size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| ((page * 131 + i * 13 + record * 7 + 5) & 0xff) as u8)
        .collect()
}

/// Bytes a put supplies: known pages with their checksums.
struct Record {
    record: usize,
    page: usize,
    pages: usize,
    sums: Vec<u64>,
    row: u64,
}

impl Record {
    fn new(record: usize, pages: usize, page: usize) -> Self {
        let sums = (0..pages)
            .map(|p| checksum(&page_bytes(record, p, page)))
            .collect();
        Self {
            record,
            page,
            pages,
            sums,
            row: checksum(&page_bytes(record, pages, ROW)),
        }
    }
}

impl PutBytes for Record {
    fn fill(&mut self, first: u64, count: u32, row: bool, dst: &mut [u8]) -> Result<Checksums> {
        for index in 0..count as usize {
            let at = index * self.page;
            dst[at..at + self.page].copy_from_slice(&page_bytes(
                self.record,
                first as usize + index,
                self.page,
            ));
        }
        if row {
            let at = count as usize * self.page;
            dst[at..at + ROW].copy_from_slice(&page_bytes(self.record, self.pages, ROW));
        }
        Ok(Checksums {
            pages: self.sums[first as usize..][..count as usize].to_vec(),
            row: row.then_some(self.row),
        })
    }
}

/// A host record this instance exports: the pages of `Record`, pinned for the manager.
struct ExportPin {
    chain: HostChain,
    record: Record,
}

impl HostPin for ExportPin {
    fn chain(&self) -> &HostChain {
        &self.chain
    }
    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums> {
        self.record
            .fill(window.first_page as u64, window.page_count, window.row, dst)
    }
}

fn history_for(record: usize, pages: usize) -> TestHistory {
    let mut history = TestHistory::new([7; 32]);
    let tokens: Vec<u32> = (0..pages as u64 * PT + 64)
        .map(|t| (t as u32).wrapping_add(record as u32 * 1_000_003))
        .collect();
    history.append(&tokens, &[]).unwrap();
    history
}

fn wait<T>(what: &str, limit: Duration, mut poll: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(value) = poll() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The client and the events it has produced that nobody has asked for yet.
struct Pool {
    client: PoolClient,
    events: Vec<PoolEvent>,
}

impl Pool {
    /// Wait for the event `want` accepts.
    fn event(&mut self, what: &str, mut want: impl FnMut(&PoolEvent) -> bool) -> PoolEvent {
        wait(what, Duration::from_secs(120), || {
            let mut fresh = Vec::new();
            self.client.poll(&mut fresh).unwrap();
            self.events.extend(fresh);
            self.events
                .iter()
                .position(&mut want)
                .map(|index| self.events.remove(index))
        })
    }

    fn put(&mut self, request: PutRequest) -> String {
        let id = self.client.put(request).unwrap();
        let event = self.event(
            "a put",
            |e| matches!(e, PoolEvent::Put { id: got, .. } if *got == id),
        );
        let PoolEvent::Put { outcome, .. } = event else {
            unreachable!()
        };
        format!("{outcome:?}")
    }

    /// Look up, plan and fetch one record; the seconds the lookup took and the restore took.
    fn restore(
        &mut self,
        o: &Options,
        identity: &Identity,
        record: usize,
        scratch: &[u32],
    ) -> (f64, f64) {
        let history = history_for(record, o.pages);
        let began = Instant::now();
        let ticket = self.client.begin_lookup(&history, 0);
        let answer = wait("a lookup", Duration::from_secs(10), || {
            match ticket.state() {
                LookupState::Pending => None,
                LookupState::Ready(answer) => Some(answer),
                LookupState::None => panic!("the lookup has no answer"),
            }
        });
        let lookup = began.elapsed().as_secs_f64();
        let request = Request {
            history: &history,
            identity,
            page_tokens: PT,
            resident: 0,
            before: u64::MAX,
        };
        let plan = plan::plan(&request, &answer.plan_candidates()).expect("a plan");
        let part = plan.part.clone();
        let candidate = answer
            .candidates
            .iter()
            .find(|c| c.plan.source == part.source && c.plan.generation == part.generation)
            .unwrap()
            .clone();
        let fetch = PoolFetch {
            id: 0,
            page_tokens: PT,
            parts: vec![FetchPart {
                candidate,
                part,
                pages: scratch.iter().map(|p| PageId(*p)).collect(),
                slot: Some(StateSlotId(9)),
            }],
        };
        let began = Instant::now();
        let id = self.client.fetch(fetch).unwrap();
        if o.mode == "hang" {
            println!("restoring");
            std::thread::sleep(Duration::from_secs(3600));
        }
        let event = self.event(
            "a restore",
            |e| matches!(e, PoolEvent::Fetched { id: got, .. } if *got == id),
        );
        let PoolEvent::Fetched { result, .. } = event else {
            unreachable!()
        };
        let moved = result.unwrap_or_else(|f| panic!("restore {record} failed: {}", f.error));
        assert_eq!(
            moved,
            ((o.pages * (o.page_kib as usize)) << 10) as u64 + ROW as u64
        );
        (lookup, began.elapsed().as_secs_f64())
    }
}

fn main() {
    let o = options();
    let page = (o.page_kib << 10) as usize;
    let geo: Vec<CacheGeometry> = vec![geometry(
        PT,
        &[("k", page as u64 / 2), ("v", page as u64 / 2)],
        ROW as u64,
    )];
    let keep = matches!(o.mode.as_str(), "check" | "import-check");
    let (copier, memory) = if keep {
        let (copier, memory) = SimulatedGpu::shared(&geo).unwrap();
        (copier, Some(memory))
    } else {
        let copier: flare_selene::SharedCopier = Arc::new(Mutex::new(Null {
            layout: Layout::new(&geo).unwrap(),
            replies: Vec::new(),
        }));
        (copier, None)
    };
    let identity = Identity {
        tenant: "remote".into(),
        model: [7; 32],
        layout: fingerprint(&geo),
    };
    let client = PoolClient::start(
        PoolConfig {
            address: if o.ca.is_some() {
                PoolAddress::Tcp(o.address.clone())
            } else {
                PoolAddress::TcpPlaintext(o.address.clone())
            },
            tenant: "remote".into(),
            ca: o.ca.clone(),
            scope: Scope::new("remote", [7; 32], fingerprint(&geo), &geo).unwrap(),
            instance_name: o.name.clone(),
            gpu_uuids: vec![[o.gpu; 16]],
            export_slots: 2,
            claim_slots: 2,

            registration_context: 0,
            approve: None,
        },
        copier,
        Arc::new(Registrar),
    )
    .unwrap();
    let mut pool = Pool {
        client,
        events: Vec::new(),
    };
    let started = Instant::now();
    wait(
        "the pool to be ready",
        Duration::from_secs(30),
        || match pool.client.status() {
            PoolStatus::Ready => Some(()),
            PoolStatus::Disabled(why) => panic!("the pool is disabled: {why}"),
            _ => None,
        },
    );
    println!("ready_ms={}", started.elapsed().as_millis());

    // Put every record, timing each.
    let record_bytes = (o.pages * page + ROW) as f64;
    let mut put_secs = Vec::new();
    let puts = !matches!(o.mode.as_str(), "export" | "import" | "import-check");
    for record in 0..if puts { o.records } else { 0 } {
        let history = history_for(record, o.pages);
        let key = history.key_at(o.pages as u64 * PT).unwrap();
        let request = PutRequest {
            key,
            parent: None,
            bucket: history.bucket_at(key.consumed_tokens).unwrap(),
            chain_pages: o.pages as u64,
            tail_pages: o.pages as u64,
            row: true,
            source: PutSource::Host(Box::new(Record::new(record, o.pages, page))),
        };
        let began = Instant::now();
        let outcome = pool.put(request);
        assert!(
            outcome.starts_with("Stored") || outcome == "Present",
            "put {record}: {outcome}"
        );
        put_secs.push(began.elapsed().as_secs_f64());
    }
    if !put_secs.is_empty() {
        let total: f64 = put_secs.iter().sum();
        println!(
            "put_records={} put_mib_per_s={:.1} put_ms_mean={:.1}",
            put_secs.len(),
            put_secs.len() as f64 * record_bytes / (1 << 20) as f64 / total,
            total / put_secs.len() as f64 * 1000.0
        );
    }

    let scratch: Vec<u32> = (0..o.pages as u32).collect();
    match o.mode.as_str() {
        "export" => {
            for record in 0..o.records {
                let history = history_for(record, o.pages);
                let key = history.key_at(o.pages as u64 * PT).unwrap();
                pool.client.saved(IndexEntry {
                    key,
                    parent: None,
                    bucket: history.bucket_at(key.consumed_tokens).unwrap(),
                    tail_pages: o.pages as u32,
                    row: true,
                    tier: Tier::Dram,
                    bytes: record_bytes as u64,
                });
            }
            let keys: Vec<_> = (0..o.records)
                .map(|r| history_for(r, o.pages).key_at(o.pages as u64 * PT).unwrap())
                .collect();
            println!("serving");
            let deadline = Instant::now() + Duration::from_secs(o.seconds);
            let (mut served, mut ended) = (0, 0);
            while Instant::now() < deadline {
                let mut fresh = Vec::new();
                pool.client.poll(&mut fresh).unwrap();
                for event in fresh {
                    match event {
                        PoolEvent::Export(request) => {
                            let record = keys
                                .iter()
                                .position(|k| *k == request.key)
                                .expect("an export of a record this instance offered");
                            pool.client.host_pinned(
                                request.export,
                                Box::new(ExportPin {
                                    chain: HostChain {
                                        records: vec![HostRecord {
                                            key: request.key,
                                            first_page: 0,
                                            pages: o.pages as u32,
                                            row: true,
                                        }],
                                        total_pages: o.pages as u64,
                                        row: true,
                                    },
                                    record: Record::new(record, o.pages, page),
                                }),
                            );
                            served += 1;
                        }
                        PoolEvent::ExportEnded { .. } => ended += 1,
                        _ => {}
                    }
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            println!("exports_served={served} ended={ended}");
        }
        "import" | "import-check" => {
            // Wait for the manager to offer what the other instance exports.
            let probe = history_for(0, o.pages);
            wait("a candidate", Duration::from_secs(60), || {
                let ticket = pool.client.begin_lookup(&probe, 0);
                let answer = wait("a lookup", Duration::from_secs(10), || {
                    match ticket.state() {
                        LookupState::Pending => None,
                        LookupState::Ready(answer) => Some(Some(answer)),
                        LookupState::None => Some(None),
                    }
                });
                answer.filter(|a| !a.candidates.is_empty()).map(|_| ())
            });
            let mut totals = Vec::new();
            let mut lookups = Vec::new();
            for _ in 0..o.rounds {
                for record in 0..o.records.max(1) {
                    let (lookup, total) = pool.restore(&o, &identity, record, &scratch);
                    lookups.push(lookup);
                    totals.push(total);
                    if keep {
                        let memory = memory.as_ref().unwrap();
                        let pages = memory.pages.lock().unwrap();
                        for index in 0..o.pages {
                            assert_eq!(
                                pages[&scratch[index]],
                                page_bytes(record, index, page),
                                "record {record} page {index}"
                            );
                        }
                        assert_eq!(
                            memory.rows.lock().unwrap()[&9],
                            page_bytes(record, o.pages, ROW),
                            "record {record} row"
                        );
                    }
                }
            }
            totals.sort_by(f64::total_cmp);
            let sum: f64 = totals.iter().sum();
            println!(
                "restores={} restore_mib_per_s={:.1} restore_ms_mean={:.2} restore_ms_median={:.2} restore_ms_min={:.2} restore_ms_max={:.2} lookup_ms_mean={:.2}{}",
                totals.len(),
                totals.len() as f64 * record_bytes / (1 << 20) as f64 / sum,
                sum / totals.len() as f64 * 1000.0,
                totals[totals.len() / 2] * 1000.0,
                totals[0] * 1000.0,
                totals[totals.len() - 1] * 1000.0,
                lookups.iter().sum::<f64>() / lookups.len() as f64 * 1000.0,
                if keep { " verified=true" } else { "" }
            );
        }
        "check" => {
            for record in 0..o.records {
                pool.restore(&o, &identity, record, &scratch);
                let memory = memory.as_ref().unwrap();
                let pages = memory.pages.lock().unwrap();
                for index in 0..o.pages {
                    assert_eq!(
                        pages[&scratch[index]],
                        page_bytes(record, index, page),
                        "record {record} page {index}"
                    );
                }
                assert_eq!(
                    memory.rows.lock().unwrap()[&9],
                    page_bytes(record, o.pages, ROW),
                    "record {record} row"
                );
            }
            println!("verified_records={}", o.records);
        }
        "bench" | "hang" => {
            let mut totals = Vec::new();
            let mut lookups = Vec::new();
            for _ in 0..o.rounds {
                for record in 0..o.records.max(1) {
                    let (lookup, total) = pool.restore(&o, &identity, record, &scratch);
                    lookups.push(lookup);
                    totals.push(total);
                }
            }
            totals.sort_by(f64::total_cmp);
            let sum: f64 = totals.iter().sum();
            println!(
                "restores={} restore_mib_per_s={:.1} restore_ms_mean={:.2} restore_ms_median={:.2} restore_ms_min={:.2} restore_ms_max={:.2} lookup_ms_mean={:.2}",
                totals.len(),
                totals.len() as f64 * record_bytes / (1 << 20) as f64 / sum,
                sum / totals.len() as f64 * 1000.0,
                totals[totals.len() / 2] * 1000.0,
                totals[0] * 1000.0,
                totals[totals.len() - 1] * 1000.0,
                lookups.iter().sum::<f64>() / lookups.len() as f64 * 1000.0
            );
        }
        other => panic!("unknown mode {other}"),
    }
    pool.client.close().unwrap();
    println!("done");
}
