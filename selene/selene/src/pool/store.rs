//! A host store attached to a pool.
//!
//! [`PooledStore`] wraps the local host store and a [`PoolClient`]. Local
//! behaviour is the wrapped store's. On top of it: GPU holdings are announced
//! and their evictions reported, pool lookups run while a request queues, a
//! restore may take its pages from the pool, and exports reach the model
//! worker through `poll`.
//!
//! A restore has at most two sources. Its closest part may be local (this
//! instance's DRAM or NVMe) or pool; the second part is always a pool source.
//! A local first part is copied first and the pool part follows, so the
//! endpoint's row, which the pool part carries, is written last. All parts
//! succeed or the restore fails as one: pages are never published partially.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::{
    Error, ExportChain, ExportId, ExportRefusal, History, HostHit, HostStore, InboundTicket,
    PrefixKey, PublishedCheckpoint, Result, Scratch, StoreEvent, StoreOutcome, StoreReason,
    StoreStats, Tier as Local,
    characterize::{Characterizer, Level, Pages},
    index::Match,
    plan::{self, Identity, Plan, Request, SourceId, Which},
    store::INITIAL_PREFILL_RATE,
};

use super::{
    announce::{Announcer, IndexEntry},
    cascade::Cascade,
    client::PoolClient,
    exporter::HostPin,
    lookup::{LookupState, LookupTicket},
    probe::StoreProbe,
    shared::{FetchPart, PoolEvent, PoolFetch, PutOutcome, PutRequest},
    types::{HeldRange, PoolCandidate, SourceRef, Tier},
};

/// What the store uses of the pool client. [`PoolClient`] is the implementation;
/// tests of the store's planning and sequencing substitute a recording one.
pub trait PoolPort {
    fn copies_drained(&self) -> bool {
        true
    }
    /// An instance-scoped probe namespace, when the port has a process identity.
    fn probe_model(&self) -> Option<[u8; 32]> {
        None
    }
    fn begin_lookup(&self, history: &dyn History, held: Vec<HeldRange>) -> LookupTicket;
    fn fetch(&mut self, fetch: PoolFetch) -> Result<u64>;
    fn poll(&mut self, events: &mut Vec<PoolEvent>) -> Result<()>;
    fn ranking(&self) -> crate::characterize::Ranking;
    fn set_ranking(&self, ranking: crate::characterize::Ranking);
    fn observe_restore(&self, level: Level, pages: usize, elapsed: Duration);
    fn saved(&self, entry: IndexEntry);
    fn evicted(&self, key: PrefixKey, tier: Tier, generation: u64, heir: Option<IndexEntry>);
    fn evictions_lost(&self);
    fn put(&mut self, request: PutRequest) -> Result<u64>;
    fn cancel_put(&mut self, id: u64);
    fn announcer(&self) -> Announcer;
    fn host_pinned(&mut self, export: ExportId, pin: Box<dyn HostPin>);
    fn export_pinned(&mut self, export: ExportId, chain: ExportChain);
    fn export_refused(&mut self, export: ExportId, reason: ExportRefusal);
    /// Whether a put can be accepted now: the session is open and the manager ready.
    fn ready(&self) -> bool;
    fn close(&mut self) -> Result<()>;
}

impl PoolPort for PoolClient {
    fn copies_drained(&self) -> bool {
        PoolClient::copies_drained(self)
    }
    fn probe_model(&self) -> Option<[u8; 32]> {
        Some(PoolClient::probe_model(self))
    }
    fn begin_lookup(&self, history: &dyn History, held: Vec<HeldRange>) -> LookupTicket {
        PoolClient::begin_lookup(self, history, held)
    }
    fn fetch(&mut self, fetch: PoolFetch) -> Result<u64> {
        PoolClient::fetch(self, fetch)
    }
    fn poll(&mut self, events: &mut Vec<PoolEvent>) -> Result<()> {
        PoolClient::poll(self, events)
    }
    fn ranking(&self) -> crate::characterize::Ranking {
        PoolClient::ranking(self)
    }
    fn set_ranking(&self, ranking: crate::characterize::Ranking) {
        PoolClient::set_ranking(self, ranking)
    }
    fn observe_restore(&self, level: Level, pages: usize, elapsed: Duration) {
        PoolClient::observe_restore(self, level, pages, elapsed)
    }
    fn saved(&self, entry: IndexEntry) {
        PoolClient::saved(self, entry)
    }
    fn evicted(&self, key: PrefixKey, tier: Tier, generation: u64, heir: Option<IndexEntry>) {
        PoolClient::evicted(self, key, tier, generation, heir)
    }
    fn evictions_lost(&self) {
        PoolClient::evictions_lost(self)
    }
    fn put(&mut self, request: PutRequest) -> Result<u64> {
        PoolClient::put(self, request)
    }
    fn cancel_put(&mut self, id: u64) {
        PoolClient::cancel_put(self, id)
    }
    fn announcer(&self) -> Announcer {
        PoolClient::announcer(self)
    }
    fn host_pinned(&mut self, export: ExportId, pin: Box<dyn HostPin>) {
        PoolClient::host_pinned(self, export, pin)
    }
    fn export_pinned(&mut self, export: ExportId, chain: ExportChain) {
        PoolClient::export_pinned(self, export, chain)
    }
    fn export_refused(&mut self, export: ExportId, reason: ExportRefusal) {
        PoolClient::export_refused(self, export, reason)
    }
    fn ready(&self) -> bool {
        PoolClient::status(self) == super::shared::PoolStatus::Ready
    }
    fn close(&mut self) -> Result<()> {
        PoolClient::close(self)
    }
}

/// Pages of the three sizes the characterizer times: a small, a typical and a
/// long prefix. The scratch ids must cover the long one.
pub const PROBE_PAGES: Pages = Pages {
    small: 2,
    typical: 16,
    long: 64,
};

/// The planner's name for this instance's own tiers.
const LOCAL: SourceId = SourceId(0);
/// A source that lacked headroom is not tried again for this long.
const SKIP: Duration = Duration::from_secs(1);
/// Plans remembered between a lookup and its fetch.
const PLANS: usize = 256;
/// Times a failed pool restore is replanned from other sources.
const RETRIES: u32 = 2;
/// Copies out of the GPU in flight beyond which a publication is declined.
const COPY_QUEUE: usize = 16;
/// The keys a history has at the positions a plan mentions, kept so that a
/// plan can be rebuilt after the request's history is gone.
struct KeptHistory {
    tokens: u64,
    keys: HashMap<u64, PrefixKey>,
}

impl History for KeptHistory {
    fn token_count(&self) -> u64 {
        self.tokens
    }
    fn key_at(&self, consumed_tokens: u64) -> Option<PrefixKey> {
        self.keys.get(&consumed_tokens).copied()
    }
    fn aligned_boundaries(&self) -> &[PrefixKey] {
        &[]
    }
}

struct Planned {
    plan: Plan,
    local: Option<HostHit>,
    pool: Vec<PoolCandidate>,
    history: KeptHistory,
    resident: u64,
    before: u64,
}

enum Stage {
    /// The local first part is copying; the pool part follows.
    Local { local_key: PrefixKey },
    /// A pool restore is running under this id.
    Pool { id: u64 },
    /// The pool could not restore it and this host's own copy of the same endpoint is
    /// copying into the same pages.
    Fallback { local_key: PrefixKey },
}

struct Restore {
    planned: Planned,
    ticket: InboundTicket,
    stage: Stage,
    retries: u32,
}

pub struct PooledStore<S: HostStore, P: PoolPort = PoolClient> {
    inner: S,
    pool: P,
    identity: Identity,
    page_tokens: u64,
    page_record_bytes: u64,
    row_record_bytes: u64,
    prefill_rate: f64,
    plans: HashMap<(PrefixKey, u64), Planned>,
    restores: HashMap<PrefixKey, Restore>,
    local_timings: HashMap<PrefixKey, (Level, usize, Instant)>,
    skip: HashMap<(SourceRef, Tier), Instant>,
    inner_events: Vec<StoreEvent>,
    pool_events: Vec<PoolEvent>,
    /// How the puts started through this store ended, until collected.
    puts: Vec<(u64, PutOutcome)>,
    /// Moves of the last local tier's records to the manager.
    cascade: Cascade,
    characterizer: Option<Characterizer>,
    characterized: bool,
    /// What the synthetic probe records are keyed by: the model and, once connected, this instance.
    probe_model: [u8; 32],
    /// Restores that were wholly from the pool, which the local tiers did not count.
    pool_fetched: u64,
    /// Restores that failed in the pool part.
    pool_fetch_failed: u64,
    /// Bytes the pool parts copied into the device.
    pool_bytes_in: u64,
}

impl<S: HostStore> PooledStore<S, PoolClient> {
    /// Attach `inner` to the pool named by `config`. Starts the pool client's
    /// thread; the store is usable at once and shares nothing until the
    /// session is ready. `copier` is a page copier dedicated to the pool.
    pub fn connect(
        inner: S,
        config: super::client::PoolConfig,
        copier: crate::SharedCopier,
        registrar: std::sync::Arc<dyn crate::HostRegistrar + Send + Sync>,
    ) -> Result<Self> {
        Self::attach(inner, config, None, copier, registrar)
    }

    /// Rank 0 of a multi-process instance (see [`PoolClient::start_ranked`]): `inner` is this
    /// rank's host tier, holding this rank's slices of the records, and `copier` serves this
    /// rank only. The peer ranks serve their own slices of host-tier exports and puts through
    /// their agents' [`RankRecords`](super::agent::RankRecords).
    pub fn connect_ranked(
        inner: S,
        config: super::client::PoolConfig,
        ranks: super::rank::RankSet,
        copier: crate::SharedCopier,
        registrar: std::sync::Arc<dyn crate::HostRegistrar + Send + Sync>,
    ) -> Result<Self> {
        Self::attach(inner, config, Some(ranks), copier, registrar)
    }

    fn attach(
        inner: S,
        config: super::client::PoolConfig,
        ranks: Option<super::rank::RankSet>,
        copier: crate::SharedCopier,
        registrar: std::sync::Arc<dyn crate::HostRegistrar + Send + Sync>,
    ) -> Result<Self> {
        let layout = copier.lock().unwrap().layout().clone();
        let identity = Identity {
            tenant: config.scope.tenant.clone(),
            model: config.scope.model_identity,
            layout: config.scope.layout_fingerprint,
        };
        let order = config.level_order;
        let pool = match ranks {
            Some(ranks) => PoolClient::start_ranked(config, ranks, copier, registrar)?,
            None => PoolClient::start(config, copier, registrar)?,
        };
        let store = Self::new(inner, pool, identity, &layout);
        store.with_pinned_order(order)
    }
}

impl<S: HostStore, P: PoolPort> PooledStore<S, P> {
    pub fn new(mut inner: S, pool: P, identity: Identity, layout: &crate::Layout) -> Self {
        let probe_model = pool.probe_model().unwrap_or(identity.model);
        inner.announce_to(pool.announcer());
        inner.cascade();
        Self {
            inner,
            pool,
            identity,
            page_tokens: layout.page_tokens,
            page_record_bytes: layout.page_record_bytes,
            row_record_bytes: layout.row_record_bytes,
            prefill_rate: INITIAL_PREFILL_RATE,
            plans: HashMap::new(),
            restores: HashMap::new(),
            local_timings: HashMap::new(),
            skip: HashMap::new(),
            inner_events: Vec::new(),
            pool_events: Vec::new(),
            puts: Vec::new(),
            cascade: Cascade::new(layout),
            characterizer: Characterizer::new(PROBE_PAGES, None).ok(),
            characterized: false,
            probe_model,
            pool_fetched: 0,
            pool_fetch_failed: 0,
            pool_bytes_in: 0,
        }
    }

    /// Pin the order of the copy levels instead of measuring it. `order` names
    /// the four levels closest first.
    pub fn with_pinned_order(mut self, order: Option<[Level; 4]>) -> Result<Self> {
        let pinned = order.map(|order| {
            let mut levels = vec![Level::ThisGpu];
            levels.extend(order);
            levels
        });
        self.characterizer = Some(Characterizer::new(PROBE_PAGES, pinned)?);
        Ok(self)
    }

    /// Whether the startup probe has not yet run.
    pub fn needs_characterization(&self) -> bool {
        !self.characterized
    }

    /// Measure the cost of each copy level end to end and rank them, if the
    /// startup probe has not run. Blocks for the time the transfers take,
    /// bounded: three repetitions at each of [`PROBE_PAGES`] for each level
    /// this instance can time. The `scratch` ids must be idle for the whole call
    /// and no transfer may be in flight;
    /// call it before the scheduler admits requests. Returns the one log line
    /// naming the order in use, when it measured.
    pub fn characterize(&mut self, scratch: &Scratch) -> Result<Option<String>> {
        let mut characterizer = self
            .characterizer
            .take()
            .ok_or_else(|| Error::new("the characterizer could not be built"))?;
        let measured = {
            let mut probe = StoreProbe {
                inner: &mut self.inner,
                pool: &mut self.pool,
                scratch,
                model: self.probe_model,
                page_tokens: self.page_tokens,
                page_bytes: self.page_record_bytes as usize,
                row_bytes: self.row_record_bytes as usize,
                stash: &mut self.pool_events,
            };
            characterizer.update(&mut probe)
        };
        let line = measured.then(|| {
            PoolPort::set_ranking(&self.pool, characterizer.ranking().clone());
            characterizer.ranking().log_line()
        });
        self.characterizer = Some(characterizer);
        self.characterized = true;
        Ok(line)
    }

    /// The puts that ended since the last call.
    pub fn take_puts(&mut self) -> Vec<(u64, PutOutcome)> {
        std::mem::take(&mut self.puts)
    }

    pub fn pool(&self) -> &P {
        &self.pool
    }

    /// The measured prefill rate the planner weighs transfers against.
    pub fn set_prefill_rate(&mut self, tokens_per_second: f64) {
        self.prefill_rate = tokens_per_second;
    }

    // ---- publication -----------------------------------------------------------

    /// Queue a copy into the first host tier, bounded by the copy queue.
    fn copy_out(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
        if self.inner.stats().outbound_in_flight >= COPY_QUEUE {
            return Ok(StoreOutcome::Retry(StoreReason::Queue));
        }
        self.inner.write_back(checkpoint)
    }

    // ---- lookup ---------------------------------------------------------------

    /// The local hit as a planner candidate.
    fn local_candidate(&self, hit: &HostHit) -> Option<plan::Candidate> {
        let pt = self.page_tokens;
        let mut nodes = Vec::with_capacity(hit.chain.nodes.len());
        let mut previous: Option<u64> = None;
        for (index, key) in hit.chain.nodes.iter().enumerate() {
            let end = key.consumed_tokens / pt;
            let tail = match previous {
                None => hit.chain.skip_pages as u64 + end.checked_sub(hit.resident_tokens / pt)?,
                Some(before) => end.checked_sub(before)?,
            };
            nodes.push(plan::ChainNode {
                key: *key,
                tail_pages: tail as usize,
            });
            previous = Some(end);
            let _ = index;
        }
        Some(plan::Candidate {
            level: match hit.tier {
                Local::Memory => Level::HostDram,
                Local::Disk => Level::HostNvme,
            },
            source: LOCAL,
            generation: 0,
            key: hit.key,
            begin_tokens: hit.resident_tokens,
            skip_pages: hit.chain.skip_pages,
            nodes,
            us_per_page: if hit.pages == 0 {
                0
            } else {
                (hit.estimated_seconds * 1e6 / hit.pages as f64).ceil() as u32
            },
        })
    }

    fn usable(&self, now: Instant, candidate: &PoolCandidate) -> bool {
        self.skip
            .get(&(candidate.source.clone(), candidate.tier))
            .is_none_or(|until| now >= *until)
    }

    fn request<'a>(
        &'a self,
        history: &'a dyn History,
        ranking: &'a crate::characterize::Ranking,
        resident: u64,
        before: u64,
    ) -> Request<'a> {
        Request {
            history,
            identity: &self.identity,
            page_tokens: self.page_tokens,
            resident,
            before,
            ranking,
            prefill_tokens_per_second: self.prefill_rate,
        }
    }

    fn hit_for(
        &self,
        plan: &Plan,
        local: Option<&HostHit>,
        pool: &[PoolCandidate],
        resident: u64,
        ranking: &crate::characterize::Ranking,
    ) -> HostHit {
        let endpoint = plan.endpoint();
        let pages = plan.pages();
        let mut seconds = 0.0;
        let mut nodes = Vec::new();
        for part in std::iter::once(&plan.a).chain(plan.b.as_ref()) {
            nodes.extend(part.chain.iter().copied());
            seconds += if part.source == LOCAL {
                local.map_or(0.0, |hit| hit.estimated_seconds)
            } else {
                // With no estimate at all the cost is not held against the restore: the
                // requester's own fetch policy decides.
                pool.iter()
                    .find(|c| c.plan.source == part.source && c.plan.generation == part.generation)
                    .and_then(|c| {
                        ranking.part_seconds(c.plan.level, part.pages, c.plan.us_per_page)
                    })
                    .unwrap_or(0.0)
            };
        }
        HostHit {
            key: endpoint,
            consumed_tokens: endpoint.consumed_tokens,
            resident_tokens: resident,
            pages,
            bytes: pages as u64 * self.page_record_bytes + self.row_record_bytes,
            tier: if plan.a.source == LOCAL {
                local.map_or(Local::Memory, |hit| hit.tier)
            } else {
                Local::Memory
            },
            estimated_seconds: seconds,
            chain: Match {
                key: endpoint,
                consumed_tokens: endpoint.consumed_tokens,
                resident_tokens: resident,
                nodes,
                skip_pages: plan.a.first_page,
                pages,
            },
        }
    }

    // ---- restores ---------------------------------------------------------------

    fn pool_part(
        &self,
        planned: &Planned,
        part: &plan::Part,
        ticket: &InboundTicket,
    ) -> Option<FetchPart> {
        let candidate = planned.pool.iter().find(|c| {
            c.plan.source == part.source
                && c.plan.generation == part.generation
                && c.plan.key == part.end
        })?;
        Some(FetchPart {
            candidate: candidate.clone(),
            part: part.clone(),
            pages: ticket.pages[part.dest_page..part.dest_page + part.pages].to_vec(),
            slot: part.row.then_some(ticket.slot),
        })
    }

    fn fetch_local(&mut self, hit: HostHit, ticket: InboundTicket) -> Result<()> {
        let key = hit.key;
        let level = match hit.tier {
            Local::Memory => Level::HostDram,
            Local::Disk => Level::HostNvme,
        };
        let timing = (level, ticket.pages.len(), Instant::now());
        self.inner.fetch(hit, ticket)?;
        self.local_timings.insert(key, timing);
        Ok(())
    }

    /// Start the pool parts of a plan; the ids it returns name the restore.
    fn start_pool(&mut self, endpoint: PrefixKey) -> Result<()> {
        let restore = self.restores.get(&endpoint).expect("registered");
        let parts: Vec<_> = std::iter::once(&restore.planned.plan.a)
            .chain(restore.planned.plan.b.as_ref())
            .filter(|p| p.source != LOCAL)
            .cloned()
            .collect();
        let fetch_parts = parts
            .iter()
            .map(|part| self.pool_part(&restore.planned, part, &restore.ticket))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| Error::new("a planned pool source is no longer known"))?;
        let id = PoolPort::fetch(
            &mut self.pool,
            PoolFetch {
                id: 0,
                page_tokens: self.page_tokens,
                parts: fetch_parts,
            },
        )?;
        self.restores.get_mut(&endpoint).expect("registered").stage = Stage::Pool { id };
        Ok(())
    }

    fn fail(&mut self, key: PrefixKey, error: Error, events: &mut Vec<StoreEvent>) {
        events.push(StoreEvent::Fetched {
            key,
            result: Err(error),
        });
    }

    /// A pool restore finished. A failure may be replanned from other sources
    /// when the restore is all pool; otherwise it fails as one.
    fn pool_finished(
        &mut self,
        id: u64,
        result: std::result::Result<u64, super::shared::FetchFailure>,
        events: &mut Vec<StoreEvent>,
    ) {
        let Some(key) = self
            .restores
            .iter()
            .find(|(_, r)| matches!(r.stage, Stage::Pool { id: running } if running == id))
            .map(|(key, _)| *key)
        else {
            return;
        };
        let failure = match result {
            Ok(bytes) => {
                if let Some(restore) = self.restores.remove(&key) {
                    // A restore that began in a local tier was counted by it.
                    self.pool_fetched += u64::from(restore.planned.plan.a.source != LOCAL);
                }
                self.pool_bytes_in += bytes;
                events.push(StoreEvent::Fetched {
                    key,
                    result: Ok(bytes),
                });
                return;
            }
            Err(failure) => failure,
        };
        if failure.no_headroom
            && let Some(source) = failure.source.clone()
        {
            self.skip.insert(source, Instant::now() + SKIP);
        }
        let (all_pool, retries) = {
            let restore = self.restores.get(&key).expect("found");
            (restore.planned.plan.a.source != LOCAL, restore.retries)
        };
        if all_pool
            && retries < RETRIES
            && let Some(part) = failure.part
        {
            let which = if part == 0 { Which::A } else { Which::B };
            let now = Instant::now();
            let replanned = {
                let planned = &self.restores[&key].planned;
                let usable: Vec<plan::Candidate> = planned
                    .pool
                    .iter()
                    .filter(|c| self.usable(now, c))
                    .map(|c| c.plan.clone())
                    .collect();
                let ranking = PoolPort::ranking(&self.pool);
                let request =
                    self.request(&planned.history, &ranking, planned.resident, planned.before);
                planned.plan.rebuild(&request, &usable, which)
            };
            if let Some(replanned) = replanned
                && replanned.a.source != LOCAL
            {
                let restore = self.restores.get_mut(&key).expect("found");
                restore.retries += 1;
                restore.planned.plan = replanned;
                if self.start_pool(key).is_ok() {
                    return;
                }
            }
        }
        // The pool has no source left: this host's own copy of the same endpoint restores
        // into the same pages, and only a restore with no source left is a recompute.
        let own = self.restores.get(&key).and_then(|restore| {
            let pool_only = restore.planned.plan.a.source != LOCAL;
            restore
                .planned
                .local
                .clone()
                .filter(|hit| pool_only && hit.key == key)
        });
        if let Some(hit) = own {
            let ticket = self.restores[&key].ticket.clone();
            if self.inner.fetch(hit, ticket).is_ok() {
                self.restores.get_mut(&key).expect("found").stage =
                    Stage::Fallback { local_key: key };
                self.pool_fetch_failed += 1;
                return;
            }
        }
        self.restores.remove(&key);
        self.pool_fetch_failed += 1;
        self.fail(key, failure.error, events);
    }
}

impl<S: HostStore, P: PoolPort> HostStore for PooledStore<S, P> {
    fn pooled(&self) -> bool {
        true
    }
    fn copies_drained(&self) -> bool {
        self.pool.copies_drained()
    }
    fn lookup(&mut self, history: &dyn History, resident: u64, before: u64) -> Option<HostHit> {
        self.inner.lookup(history, resident, before)
    }

    fn lookup_for(
        &mut self,
        ticket: Option<&LookupTicket>,
        history: &dyn History,
        resident: u64,
        before: u64,
    ) -> Option<HostHit> {
        let local = self.inner.lookup(history, resident, before);
        let Some(LookupState::Ready(answer)) = ticket.map(LookupTicket::state) else {
            return local;
        };
        let now = Instant::now();
        let pool: Vec<PoolCandidate> = answer
            .candidates
            .iter()
            .filter(|c| self.usable(now, c))
            .cloned()
            .collect();
        let mut candidates: Vec<plan::Candidate> = pool.iter().map(|c| c.plan.clone()).collect();
        candidates.extend(local.as_ref().and_then(|hit| self.local_candidate(hit)));
        let ranking = PoolPort::ranking(&self.pool);
        let request = self.request(history, &ranking, resident, before);
        let mut plan = plan::plan(&request, &candidates)?;
        // A pool part followed by a part of this host's own tiers cannot be run: the store
        // fetches its own part first and the pool serves the rest. Plan again from the pool's
        // candidates alone, and fall back to the local hit when they cannot cover the restore.
        if plan.b.as_ref().is_some_and(|b| b.source == LOCAL) {
            let pool_only: Vec<plan::Candidate> = candidates
                .iter()
                .filter(|c| c.source != LOCAL)
                .cloned()
                .collect();
            match plan::plan(&request, &pool_only) {
                Some(replanned) => plan = replanned,
                None => return local,
            }
        }
        if plan.a.source == LOCAL && plan.b.is_none() {
            return local;
        }
        if plan.validate(history, self.page_tokens).is_err() {
            return local;
        }
        let mut keys = HashMap::new();
        for candidate in &candidates {
            for key in candidate.nodes.iter().map(|n| n.key).chain([candidate.key]) {
                if let Some(real) = history.key_at(key.consumed_tokens) {
                    keys.insert(key.consumed_tokens, real);
                }
            }
        }
        let hit = self.hit_for(&plan, local.as_ref(), &pool, resident, &ranking);
        if self.plans.len() >= PLANS {
            self.plans.clear();
        }
        self.plans.insert(
            (hit.key, resident),
            Planned {
                plan,
                local,
                pool,
                history: KeptHistory {
                    tokens: history.token_count(),
                    keys,
                },
                resident,
                before,
            },
        );
        Some(hit)
    }

    fn begin_lookup(&mut self, history: &dyn History) -> Option<LookupTicket> {
        let mut held = Vec::new();
        if let Some(hit) = self.inner.lookup(history, 0, history.token_count()) {
            held.push(HeldRange {
                level: match hit.tier {
                    Local::Memory => Level::HostDram,
                    Local::Disk => Level::HostNvme,
                },
                end: hit.key,
            });
        }
        Some(PoolPort::begin_lookup(&self.pool, history, held))
    }

    fn store(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
        // The publication is announced as a GPU holding, so neighbours can
        // claim it; copying it to a host tier waits for `write_back`.
        PoolPort::saved(
            &self.pool,
            IndexEntry {
                key: checkpoint.key,
                parent: checkpoint.parent,
                bucket: checkpoint.bucket,
                tail_pages: checkpoint.tail_pages as u32,
                row: true,
                tier: Tier::Gpu,
                generation: checkpoint.id.value(),
                bytes: checkpoint.tail_pages as u64 * self.page_record_bytes
                    + self.row_record_bytes,
            },
        );
        Ok(StoreOutcome::Noted)
    }

    fn write_back(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
        self.copy_out(checkpoint)
    }

    fn fetch(&mut self, hit: HostHit, ticket: InboundTicket) -> Result<()> {
        let Some(planned) = self.plans.remove(&(hit.key, hit.resident_tokens)) else {
            return self.fetch_local(hit, ticket);
        };
        self.plans.retain(|(key, _), _| *key != hit.key);
        if ticket.key != hit.key || ticket.pages.len() != planned.plan.pages() {
            return Err(Error::new("the ticket does not match the planned restore"));
        }
        let endpoint = hit.key;
        let local_first = planned.plan.a.source == LOCAL;
        let stage = Stage::Pool { id: 0 };
        let local_hit = planned.local.clone();
        let a_pages = planned.plan.a.pages;
        self.restores.insert(
            endpoint,
            Restore {
                planned,
                ticket: ticket.clone(),
                stage,
                retries: 0,
            },
        );
        let started = if local_first {
            let local_hit = local_hit.ok_or_else(|| Error::new("the local part has no hit"));
            local_hit.and_then(|local_hit| {
                let local_key = local_hit.key;
                let part_ticket = InboundTicket {
                    key: local_key,
                    pages: ticket.pages[..a_pages].to_vec(),
                    slot: ticket.slot,
                };
                self.fetch_local(local_hit, part_ticket)?;
                self.restores.get_mut(&endpoint).expect("inserted").stage =
                    Stage::Local { local_key };
                Ok(())
            })
        } else {
            self.start_pool(endpoint)
        };
        if started.is_err() {
            self.restores.remove(&endpoint);
        }
        started
    }

    fn poll(&mut self, events: &mut Vec<StoreEvent>) -> Result<()> {
        let mut inner_events = std::mem::take(&mut self.inner_events);
        let inner = self.inner.poll(&mut inner_events);
        for event in inner_events.drain(..) {
            if let StoreEvent::Fetched { key, result } = &event
                && let Some((level, pages, started)) = self.local_timings.remove(key)
                && result.is_ok()
            {
                PoolPort::observe_restore(&self.pool, level, pages, started.elapsed());
            }
            match event {
                StoreEvent::Fetched { key, result }
                    if self.restores.values().any(
                        |r| matches!(r.stage, Stage::Local { local_key } if local_key == key),
                    ) =>
                {
                    let endpoint = self
                        .restores
                        .iter()
                        .find(|(_, r)| matches!(r.stage, Stage::Local { local_key } if local_key == key))
                        .map(|(endpoint, _)| *endpoint)
                        .expect("matched above");
                    match result {
                        Err(error) => {
                            self.restores.remove(&endpoint);
                            self.fail(endpoint, error, events);
                        }
                        Ok(_) => {
                            if let Err(error) = self.start_pool(endpoint) {
                                self.restores.remove(&endpoint);
                                self.fail(endpoint, error, events);
                            }
                        }
                    }
                }
                StoreEvent::Fetched { key, result }
                    if self.restores.get(&key).is_some_and(
                        |r| matches!(r.stage, Stage::Fallback { local_key } if local_key == key),
                    ) =>
                {
                    self.restores.remove(&key);
                    events.push(StoreEvent::Fetched { key, result });
                }
                other => events.push(other),
            }
        }
        self.inner_events = inner_events;
        let mut pool_events = std::mem::take(&mut self.pool_events);
        let pool = PoolPort::poll(&mut self.pool, &mut pool_events);
        for event in pool_events.drain(..) {
            match event {
                PoolEvent::Export(request) if request.tier == Tier::Gpu => {
                    events.push(StoreEvent::ExportRequested {
                        export: request.export,
                        key: request.key,
                        generation: request.generation,
                        begin_tokens: request.begin_tokens,
                        want_row: request.want_row,
                        reserved_bytes: request.reserved_bytes,
                    })
                }
                // A host tier is this store's to pin; the model worker is not involved.
                PoolEvent::Export(request) => match self.inner.pin_export(
                    request.tier,
                    request.key,
                    request.generation,
                    request.begin_tokens,
                    request.want_row,
                ) {
                    Ok(pin) => PoolPort::host_pinned(&mut self.pool, request.export, pin),
                    Err(reason) => PoolPort::export_refused(&mut self.pool, request.export, reason),
                },
                PoolEvent::ExportEnded { export } => {
                    events.push(StoreEvent::ExportEnded { export })
                }
                PoolEvent::Fetched { id, result } => self.pool_finished(id, result, events),
                PoolEvent::Put { id, outcome } => {
                    if !self.cascade.ended(&mut self.inner, id, &outcome) {
                        self.puts.push((id, outcome));
                    }
                }
                PoolEvent::Status(_) => {}
            }
        }
        self.pool_events = pool_events;
        self.cascade.pump(&mut self.inner, &mut self.pool);
        inner?;
        pool
    }

    fn stats(&self) -> StoreStats {
        let mut stats = self.inner.stats();
        stats.fetched += self.pool_fetched;
        stats.fetch_failed += self.pool_fetch_failed;
        stats.bytes_in += self.pool_bytes_in;
        stats
    }

    fn close(&mut self) -> Result<()> {
        let pool = PoolPort::close(&mut self.pool);
        let inner = self.inner.close();
        pool.and(inner)
    }

    fn export_pinned(&mut self, export: ExportId, chain: ExportChain) -> Result<()> {
        PoolPort::export_pinned(&mut self.pool, export, chain);
        Ok(())
    }

    fn export_refused(&mut self, export: ExportId, reason: ExportRefusal) {
        PoolPort::export_refused(&mut self.pool, export, reason);
    }

    fn evictions_lost(&mut self) {
        PoolPort::evictions_lost(&self.pool);
    }

    fn needs_characterization(&self) -> bool {
        PooledStore::needs_characterization(self)
    }

    fn characterize(&mut self, scratch: &Scratch) -> Result<Option<String>> {
        PooledStore::characterize(self, scratch)
    }

    fn evicted(&mut self, key: PrefixKey, generation: u64, heir: Option<&PublishedCheckpoint>) {
        PoolPort::evicted(
            &self.pool,
            key,
            Tier::Gpu,
            generation,
            heir.map(|heir| IndexEntry {
                key: heir.key,
                parent: heir.parent,
                bucket: heir.bucket,
                tail_pages: heir.tail_pages as u32,
                row: true,
                tier: Tier::Gpu,
                generation: heir.id.value(),
                bytes: heir.tail_pages as u64 * self.page_record_bytes + self.row_record_bytes,
            }),
        );
    }
}

#[cfg(test)]
mod tests;
