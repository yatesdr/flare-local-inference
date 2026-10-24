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
    store::{INITIAL_MEMORY_RATE, INITIAL_PREFILL_RATE, INITIAL_STAGED_DISK_RATE, worth_transfer},
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
    fn begin_lookup(&self, history: &dyn History, held: Vec<HeldRange>) -> LookupTicket;
    fn fetch(&mut self, fetch: PoolFetch) -> Result<u64>;
    fn poll(&mut self, events: &mut Vec<PoolEvent>) -> Result<()>;
    fn ranking(&self) -> crate::characterize::Ranking;
    fn set_ranking(&self, ranking: crate::characterize::Ranking);
    fn topology(&self) -> crate::characterize::Topology;
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
    fn topology(&self) -> crate::characterize::Topology {
        PoolClient::topology(self)
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
/// How long a record seen in a lookup answer counts as held elsewhere.
const HELD_FOR: Duration = Duration::from_secs(30);
/// Holdings remembered from lookup answers before the memory is cleared.
const HELD_MAX: usize = 4096;

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
    skip: HashMap<(SourceRef, Tier), Instant>,
    inner_events: Vec<StoreEvent>,
    pool_events: Vec<PoolEvent>,
    /// How the puts started through this store ended, until collected.
    puts: Vec<(u64, PutOutcome)>,
    /// Moves of the last local tier's records to the manager.
    cascade: Cascade,
    /// Records lookup answers showed in a host tier of the pool, with when.
    held: HashMap<PrefixKey, (Level, Instant)>,
    characterizer: Option<Characterizer>,
    characterized_for: Option<crate::characterize::Topology>,
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
        // Two instances probing at once must not write or time each other's records.
        let probe_model = super::probe::instance_probe_model(
            &config.scope.model_identity,
            &super::identity::instance_id(
                &config.scope.tenant,
                &config.scope.model_identity,
                &config.scope.layout_fingerprint,
                &config.gpu_uuids,
            ),
        );
        let pool = match ranks {
            Some(ranks) => PoolClient::start_ranked(config, ranks, copier, registrar)?,
            None => PoolClient::start(config, copier, registrar)?,
        };
        let mut store = Self::new(inner, pool, identity, &layout);
        store.probe_model = probe_model;
        store.with_pinned_order(order)
    }
}

impl<S: HostStore, P: PoolPort> PooledStore<S, P> {
    pub fn new(mut inner: S, pool: P, identity: Identity, layout: &crate::Layout) -> Self {
        let identity_model = identity.model;
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
            skip: HashMap::new(),
            inner_events: Vec::new(),
            pool_events: Vec::new(),
            puts: Vec::new(),
            cascade: Cascade::new(layout),
            held: HashMap::new(),
            characterizer: Characterizer::new(PROBE_PAGES, None).ok(),
            characterized_for: None,
            probe_model: identity_model,
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

    /// Whether the copy levels were last measured under another topology than
    /// the pool has now (or never). The caller then runs [`PooledStore::characterize`]
    /// when it has scratch ids to spare.
    pub fn needs_characterization(&self) -> bool {
        let now = PoolPort::topology(&self.pool);
        self.characterized_for != Some(now)
    }

    /// Measure the cost of each copy level end to end and rank them, if the
    /// topology changed since the last time (or never measured). Blocks for
    /// the time the transfers take, bounded: three repetitions at each of
    /// [`PROBE_PAGES`] for each level this instance can time. The `scratch`
    /// ids must be idle for the whole call and no transfer may be in flight;
    /// call it before the scheduler admits requests. Returns the one log line
    /// naming the order in use, when it measured.
    pub fn characterize(&mut self, scratch: &Scratch) -> Result<Option<String>> {
        let mut characterizer = self
            .characterizer
            .take()
            .ok_or_else(|| Error::new("the characterizer could not be built"))?;
        let topology = PoolPort::topology(&self.pool);
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
            characterizer.update(topology, &mut probe)
        };
        let line = measured.then(|| {
            PoolPort::set_ranking(&self.pool, characterizer.ranking().clone());
            characterizer.ranking().log_line()
        });
        self.characterizer = Some(characterizer);
        self.characterized_for = Some(topology);
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

    /// Remember which records the pool's host tiers held when a lookup asked.
    fn remember_held(&mut self, now: Instant, candidates: &[PoolCandidate]) {
        if self.held.len() >= HELD_MAX {
            self.held
                .retain(|_, (_, seen)| now.duration_since(*seen) < HELD_FOR);
            if self.held.len() >= HELD_MAX {
                self.held.clear();
            }
        }
        for candidate in candidates {
            let level = candidate.plan.level;
            if matches!(level, Level::HostDram | Level::HostNvme) {
                for key in candidate
                    .plan
                    .nodes
                    .iter()
                    .map(|node| node.key)
                    .chain([candidate.plan.key])
                {
                    self.held.insert(key, (level, now));
                }
            }
        }
    }

    /// Whether a host tier of the pool at least as close as the one a
    /// publication would be copied into held `key` recently.
    fn held_as_close(&self, key: &PrefixKey) -> bool {
        let Some((level, seen)) = self.held.get(key) else {
            return false;
        };
        let closest = match self.inner.closest_tier() {
            Some(Local::Memory) => Level::HostDram,
            _ => Level::HostNvme,
        };
        seen.elapsed() < HELD_FOR && (*level == Level::HostDram || closest == Level::HostNvme)
    }

    /// Whether restoring a checkpoint from the first host tier would beat
    /// recomputing it, at the measured rate of that tier or its first estimate.
    fn worth_keeping(&self, checkpoint: &PublishedCheckpoint) -> bool {
        let bytes = checkpoint.tail_pages as u64 * self.page_record_bytes + self.row_record_bytes;
        let initial = match self.inner.closest_tier() {
            Some(Local::Disk) => INITIAL_STAGED_DISK_RATE,
            _ => INITIAL_MEMORY_RATE,
        };
        let measured = self.inner.stats().inbound_rate;
        let rate = if measured > 0.0 { measured } else { initial };
        worth_transfer(
            checkpoint.tail_pages as u64 * self.page_tokens,
            bytes as f64 / rate,
            self.prefill_rate,
        )
    }

    /// Copy `checkpoint` to the first host tier now, if that is worth it:
    /// `Oversize` (never stored; the worker does not ask again) when restoring
    /// it would not beat recomputing it, `Present` when the pool already holds
    /// it in a tier at least as close, `Retry(Queue)` when too many copies are
    /// in flight, and otherwise what the tier answers.
    fn copy_out(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
        if !self.worth_keeping(checkpoint) {
            return Ok(StoreOutcome::Oversize);
        }
        if self.held_as_close(&checkpoint.key) {
            return Ok(StoreOutcome::Present);
        }
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
            restore.planned.local.clone().filter(|hit| pool_only && hit.key == key)
        });
        if let Some(hit) = own {
            let ticket = self.restores[&key].ticket.clone();
            if self.inner.fetch(hit, ticket).is_ok() {
                self.restores.get_mut(&key).expect("found").stage = Stage::Fallback { local_key: key };
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
        self.remember_held(now, &answer.candidates);
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
            let pool_only: Vec<plan::Candidate> =
                candidates.iter().filter(|c| c.source != LOCAL).cloned().collect();
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
            return self.inner.fetch(hit, ticket);
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
                self.inner.fetch(local_hit, part_ticket)?;
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
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{
        CheckpointId, Layout, PageId, Retirement, Retiring, StateSlotId,
        characterize::Ranking,
        pool::{
            Answer,
            shared::{FetchFailure, PutRefusal, PutSource},
        },
        testing::{TestHistory, geometry},
    };

    const PT: u64 = 16;

    fn layout() -> Layout {
        Layout::new(&[geometry(PT, &[("k", 4096)], 4096)]).unwrap()
    }

    fn history() -> TestHistory {
        let mut history = TestHistory::new([7; 32]);
        history
            .append(&(0..2000).collect::<Vec<u32>>(), &[])
            .unwrap();
        history
    }

    fn identity() -> Identity {
        Identity {
            tenant: "t".into(),
            model: [7; 32],
            layout: [1; 16],
        }
    }

    // ---- fakes ----

    #[derive(Default)]
    struct LocalState {
        hit: Option<HostHit>,
        fetches: Vec<(HostHit, InboundTicket)>,
        events: Vec<StoreEvent>,
        reject_fetch: bool,
        pinned: Vec<(Tier, PrefixKey)>,
        refuse_pin: Option<ExportRefusal>,
        announced: bool,
        /// Microseconds per page this store's own `tier` takes to restore.
        probe: Option<(Local, u64)>,
        /// What the last local tier offers for a move, coldest first.
        retiring: Vec<Retiring>,
        /// How each offered record's move ended, in order.
        retired: Vec<(PrefixKey, Retirement)>,
        spill: Option<bool>,
        cascaded: bool,
        /// Keys the local tiers were asked to copy, and the answer given.
        stored: Vec<PrefixKey>,
        store_outcome: Option<StoreOutcome>,
        copies_in_flight: usize,
    }

    #[derive(Clone, Default)]
    struct FakeLocal(Arc<Mutex<LocalState>>);

    impl HostStore for FakeLocal {
        fn lookup(&mut self, _: &dyn History, resident: u64, before: u64) -> Option<HostHit> {
            self.0
                .lock()
                .unwrap()
                .hit
                .clone()
                .filter(|hit| hit.resident_tokens == resident && hit.consumed_tokens < before)
        }
        fn store(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
            let mut state = self.0.lock().unwrap();
            state.stored.push(checkpoint.key);
            Ok(state.store_outcome.unwrap_or(StoreOutcome::Queued))
        }
        fn closest_tier(&self) -> Option<Local> {
            Some(Local::Memory)
        }
        fn fetch(&mut self, hit: HostHit, ticket: InboundTicket) -> Result<()> {
            let mut state = self.0.lock().unwrap();
            if state.reject_fetch {
                return Err("rejected".into());
            }
            state.fetches.push((hit, ticket));
            Ok(())
        }
        fn poll(&mut self, events: &mut Vec<StoreEvent>) -> Result<()> {
            events.append(&mut self.0.lock().unwrap().events);
            Ok(())
        }
        fn stats(&self) -> StoreStats {
            StoreStats {
                outbound_in_flight: self.0.lock().unwrap().copies_in_flight,
                ..StoreStats::default()
            }
        }
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
        fn announce_to(&mut self, _: Announcer) {
            self.0.lock().unwrap().announced = true;
        }
        fn cascade(&mut self) {
            self.0.lock().unwrap().cascaded = true;
        }
        fn set_spill(&mut self, enabled: bool) {
            self.0.lock().unwrap().spill = Some(enabled);
        }
        fn take_retiring(&mut self) -> Vec<Retiring> {
            std::mem::take(&mut self.0.lock().unwrap().retiring)
        }
        fn retired(&mut self, key: PrefixKey, _: Tier, how: Retirement) {
            self.0.lock().unwrap().retired.push((key, how));
        }
        fn probe(
            &mut self,
            tier: Local,
            _: &Scratch,
            pages: usize,
            _: bool,
        ) -> Result<Option<Duration>> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .probe
                .filter(|(held, _)| *held == tier)
                .map(|(_, per_page)| Duration::from_micros(per_page * pages as u64)))
        }
        fn pin_export(
            &mut self,
            tier: Tier,
            key: PrefixKey,
            _: u64,
            _: u64,
            _: bool,
        ) -> std::result::Result<Box<dyn HostPin>, ExportRefusal> {
            let mut state = self.0.lock().unwrap();
            state.pinned.push((tier, key));
            match state.refuse_pin {
                Some(reason) => Err(reason),
                None => Ok(Box::new(NoPin(key))),
            }
        }
    }

    struct NoPin(PrefixKey);
    impl HostPin for NoPin {
        fn chain(&self) -> &crate::pool::HostChain {
            // The chain a put asks the pin for: a single record of 10 pages.
            static CHAIN: std::sync::OnceLock<crate::pool::HostChain> = std::sync::OnceLock::new();
            CHAIN.get_or_init(|| crate::pool::HostChain {
                records: vec![crate::pool::HostRecord {
                    key: self.0,
                    generation: 1,
                    first_page: 0,
                    pages: 10,
                    row: true,
                }],
                total_pages: 10,
                row: true,
            })
        }
        fn read(
            &mut self,
            _: &crate::pool::HostWindow,
            _: &mut [u8],
        ) -> Result<crate::pool::Checksums> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct PoolState {
        ranking: Option<Ranking>,
        topology: u64,
        /// Serve the probe record at this level, as a manager would.
        manager_level: Option<Level>,
        host_pinned: Vec<ExportId>,
        refused: Vec<(ExportId, ExportRefusal)>,
        answer: Option<Arc<Answer>>,
        held: Vec<Vec<HeldRange>>,
        fetches: Vec<PoolFetch>,
        reject: bool,
        events: Vec<PoolEvent>,
        saved: Vec<IndexEntry>,
        evicted: Vec<(PrefixKey, Tier, u64, Option<IndexEntry>)>,
        lost: usize,
        ready: bool,
        /// Puts accepted, by id; the test ends them with a [`PoolEvent::Put`].
        puts: Vec<(u64, PutRequest)>,
        /// Refuse every put as a pool whose command queue is full does.
        put_error: bool,
    }

    #[derive(Clone)]
    struct FakePool(Arc<Mutex<PoolState>>, Ranking, Announcer);

    impl PoolPort for FakePool {
        fn begin_lookup(&self, history: &dyn History, held: Vec<HeldRange>) -> LookupTicket {
            let mut state = self.0.lock().unwrap();
            state.held.push(held);
            if let Some(level) = state.manager_level {
                let key = history.aligned_boundaries()[0];
                let tail = (key.consumed_tokens / PT) as usize;
                let candidate = PoolCandidate {
                    plan: plan::Candidate {
                        level,
                        source: SourceId(9),
                        generation: 1,
                        key,
                        begin_tokens: 0,
                        skip_pages: 0,
                        nodes: vec![plan::ChainNode {
                            key,
                            tail_pages: tail,
                        }],
                        us_per_page: 0,
                    },
                    source: SourceRef {
                        instance: Vec::new(),
                        session_incarnation: 0,
                    },
                    tier: Tier::Dram,
                };
                let answer = Arc::new(Answer {
                    held: Vec::new(),
                    candidates: vec![candidate],
                });
                return LookupTicket::watched(LookupState::Ready(answer), || {});
            }
            match state.answer.clone() {
                Some(answer) => LookupTicket::watched(LookupState::Ready(answer), || {}),
                None => LookupTicket::watched(LookupState::Pending, || {}),
            }
        }
        fn fetch(&mut self, fetch: PoolFetch) -> Result<u64> {
            let mut state = self.0.lock().unwrap();
            if state.manager_level.is_some() {
                let id = 78;
                state.events.push(PoolEvent::Fetched { id, result: Ok(1) });
                state.fetches.push(fetch);
                return Ok(id);
            }
            if state.reject {
                return Err("the pool is not ready".into());
            }
            state.fetches.push(fetch);
            Ok(state.fetches.len() as u64)
        }
        fn poll(&mut self, events: &mut Vec<PoolEvent>) -> Result<()> {
            events.append(&mut self.0.lock().unwrap().events);
            Ok(())
        }
        fn ranking(&self) -> Ranking {
            self.0
                .lock()
                .unwrap()
                .ranking
                .clone()
                .unwrap_or_else(|| self.1.clone())
        }
        fn saved(&self, entry: IndexEntry) {
            self.0.lock().unwrap().saved.push(entry);
        }
        fn evicted(&self, key: PrefixKey, tier: Tier, generation: u64, heir: Option<IndexEntry>) {
            self.0
                .lock()
                .unwrap()
                .evicted
                .push((key, tier, generation, heir));
        }
        fn set_ranking(&self, ranking: Ranking) {
            self.0.lock().unwrap().ranking = Some(ranking);
        }
        fn topology(&self) -> crate::characterize::Topology {
            crate::characterize::Topology(self.0.lock().unwrap().topology)
        }
        fn evictions_lost(&self) {
            self.0.lock().unwrap().lost += 1;
        }
        fn put(&mut self, request: PutRequest) -> Result<u64> {
            let mut state = self.0.lock().unwrap();
            if state.manager_level.is_none() {
                if !state.ready {
                    return Err("the fake pool takes no puts".into());
                }
                if state.put_error {
                    return Err("the pool's command queue is full".into());
                }
                let id = state.puts.len() as u64 + 1;
                state.puts.push((id, request));
                return Ok(id);
            }
            let id = 77;
            state.events.push(PoolEvent::Put {
                id,
                outcome: PutOutcome::Stored { bytes: 1 },
            });
            Ok(id)
        }
        fn cancel_put(&mut self, _: u64) {}
        fn announcer(&self) -> Announcer {
            self.2.clone()
        }
        fn host_pinned(&mut self, export: ExportId, _: Box<dyn HostPin>) {
            self.0.lock().unwrap().host_pinned.push(export);
        }
        fn export_pinned(&mut self, _: ExportId, _: ExportChain) {}
        fn export_refused(&mut self, export: ExportId, reason: ExportRefusal) {
            self.0.lock().unwrap().refused.push((export, reason));
        }
        fn ready(&self) -> bool {
            self.0.lock().unwrap().ready
        }
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct Rig {
        store: PooledStore<FakeLocal, FakePool>,
        local: FakeLocal,
        pool: Arc<Mutex<PoolState>>,
        history: TestHistory,
    }

    fn rig() -> Rig {
        rig_with(Ranking::for_test(
            Level::DEFAULT_ORDER,
            &[
                (Level::NeighbourGpu, 10.0),
                (Level::HostDram, 10.0),
                (Level::HostNvme, 100.0),
                (Level::RemoteNvme, 200.0),
            ],
        ))
    }

    fn rig_with(ranking: Ranking) -> Rig {
        let local = FakeLocal::default();
        let pool = Arc::new(Mutex::new(PoolState::default()));
        Rig {
            store: PooledStore::new(
                local.clone(),
                FakePool(
                    pool.clone(),
                    ranking,
                    Announcer::new(Arc::new(tokio::sync::Notify::new())),
                ),
                identity(),
                &layout(),
            ),
            local,
            pool,
            history: history(),
        }
    }

    impl Rig {
        fn key(&self, tokens: u64) -> PrefixKey {
            self.history.key_at(tokens).unwrap()
        }

        /// A candidate whose records end at `ends`, the first starting at page `first_start`.
        fn candidate(
            &self,
            level: Level,
            source: u8,
            begin: u64,
            first_start: u64,
            ends: &[u64],
            us: u32,
        ) -> PoolCandidate {
            let mut previous = first_start;
            let nodes: Vec<plan::ChainNode> = ends
                .iter()
                .map(|end| {
                    let page = end / PT;
                    let node = plan::ChainNode {
                        key: self.key(*end),
                        tail_pages: (page - previous) as usize,
                    };
                    previous = page;
                    node
                })
                .collect();
            PoolCandidate {
                plan: plan::Candidate {
                    level,
                    source: SourceId(u64::from(source)),
                    generation: 1,
                    key: nodes.last().unwrap().key,
                    begin_tokens: begin,
                    skip_pages: (begin / PT - first_start) as usize,
                    nodes,
                    us_per_page: us,
                },
                source: SourceRef {
                    instance: vec![source],
                    session_incarnation: 1,
                },
                tier: if level == Level::HostNvme {
                    Tier::Nvme
                } else {
                    Tier::Dram
                },
            }
        }

        fn answer(&self, candidates: Vec<PoolCandidate>) {
            self.pool.lock().unwrap().answer = Some(Arc::new(Answer {
                held: Vec::new(),
                candidates,
            }));
        }

        fn local_hit(&self, end: u64, tier: Local) {
            let key = self.key(end);
            self.local.0.lock().unwrap().hit = Some(HostHit {
                key,
                consumed_tokens: end,
                resident_tokens: 0,
                pages: (end / PT) as usize,
                bytes: 0,
                tier,
                estimated_seconds: 0.001,
                chain: Match {
                    key,
                    consumed_tokens: end,
                    resident_tokens: 0,
                    nodes: vec![key],
                    skip_pages: 0,
                    pages: (end / PT) as usize,
                },
            });
        }

        fn lookup(&mut self) -> Option<HostHit> {
            let ticket = PoolPort::begin_lookup(&self.store.pool, &self.history, Vec::new());
            let history = self.history.clone();
            self.store.lookup_for(Some(&ticket), &history, 0, u64::MAX)
        }

        fn ticket(&self, hit: &HostHit) -> InboundTicket {
            InboundTicket {
                key: hit.key,
                pages: (0..hit.pages as u32).map(PageId).collect(),
                slot: StateSlotId(3),
            }
        }

        fn poll(&mut self) -> Vec<StoreEvent> {
            let mut events = Vec::new();
            self.store.poll(&mut events).unwrap();
            events
        }
    }

    fn fetched(events: &[StoreEvent]) -> Vec<(PrefixKey, bool)> {
        events
            .iter()
            .filter_map(|e| match e {
                StoreEvent::Fetched { key, result } => Some((*key, result.is_ok())),
                _ => None,
            })
            .collect()
    }

    fn failure(part: usize, source: (SourceRef, Tier), no_headroom: bool) -> FetchFailure {
        FetchFailure {
            error: "claim refused".into(),
            part: Some(part),
            source: Some(source),
            no_headroom,
        }
    }

    // ---- two sources ----

    #[test]
    fn local_first_part_then_pool_part_with_one_row_from_the_pool() {
        let mut rig = rig();
        rig.local_hit(800, Local::Memory);
        rig.answer(vec![rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0)]);
        let hit = rig.lookup().unwrap();
        assert_eq!((hit.key, hit.pages), (rig.key(1600), 100));
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        {
            let local = rig.local.0.lock().unwrap();
            assert_eq!(local.fetches.len(), 1);
            assert_eq!(local.fetches[0].1.key, rig.key(800));
            assert_eq!(
                local.fetches[0].1.pages.len(),
                50,
                "A takes the first 50 destination pages"
            );
        }
        assert!(
            rig.pool.lock().unwrap().fetches.is_empty(),
            "the pool part follows the local part"
        );
        rig.local
            .0
            .lock()
            .unwrap()
            .events
            .push(StoreEvent::Fetched {
                key: rig.key(800),
                result: Ok(1),
            });
        assert!(
            fetched(&rig.poll()).is_empty(),
            "A done is not the restore done"
        );
        let fetch = rig.pool.lock().unwrap().fetches[0].clone();
        assert_eq!(fetch.parts.len(), 1);
        let part = &fetch.parts[0];
        assert_eq!(
            (part.part.begin_tokens, part.part.pages, part.part.row),
            (800, 50, true)
        );
        assert_eq!(part.pages.first(), Some(&PageId(50)));
        assert_eq!(part.slot, Some(StateSlotId(3)));
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 1,
            result: Ok(5),
        });
        assert_eq!(fetched(&rig.poll()), [(rig.key(1600), true)]);
    }

    #[test]
    fn a_failed_local_part_fails_the_restore_without_asking_the_pool() {
        let mut rig = rig();
        rig.local_hit(800, Local::Memory);
        rig.answer(vec![rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0)]);
        let hit = rig.lookup().unwrap();
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        rig.local
            .0
            .lock()
            .unwrap()
            .events
            .push(StoreEvent::Fetched {
                key: rig.key(800),
                result: Err("disk".into()),
            });
        assert_eq!(fetched(&rig.poll()), [(rig.key(1600), false)]);
        assert!(rig.pool.lock().unwrap().fetches.is_empty());
    }

    #[test]
    fn a_failed_pool_restore_falls_back_to_the_local_copy_of_the_same_endpoint() {
        let mut rig = rig();
        rig.local_hit(1600, Local::Disk);
        let candidate = rig.candidate(Level::HostDram, 1, 0, 0, &[1600], 0);
        let refused = (candidate.source.clone(), candidate.tier);
        rig.answer(vec![candidate]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(1600));
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        assert!(rig.local.0.lock().unwrap().fetches.is_empty());
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 1,
            result: Err(failure(0, refused, false)),
        });
        assert!(fetched(&rig.poll()).is_empty(), "the failure is not reported");
        assert_eq!(rig.local.0.lock().unwrap().fetches.len(), 1, "the local copy is fetched");
        rig.local.0.lock().unwrap().events.push(StoreEvent::Fetched {
            key: rig.key(1600),
            result: Ok(1),
        });
        assert_eq!(fetched(&rig.poll()), [(rig.key(1600), true)]);
    }

    #[test]
    fn a_pool_part_refused_after_the_local_part_fails_the_restore_once() {
        let mut rig = rig();
        rig.local_hit(800, Local::Memory);
        rig.answer(vec![rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0)]);
        let hit = rig.lookup().unwrap();
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        rig.pool.lock().unwrap().reject = true;
        rig.local
            .0
            .lock()
            .unwrap()
            .events
            .push(StoreEvent::Fetched {
                key: rig.key(800),
                result: Ok(1),
            });
        assert_eq!(fetched(&rig.poll()), [(rig.key(1600), false)]);
        assert!(fetched(&rig.poll()).is_empty());
    }

    #[test]
    fn two_pool_parts_are_one_pool_restore_with_the_row_in_the_last_part_only() {
        let mut rig = rig();
        rig.answer(vec![
            rig.candidate(Level::HostDram, 1, 0, 0, &[800], 0),
            rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0),
        ]);
        let hit = rig.lookup().unwrap();
        assert_eq!((hit.key, hit.pages), (rig.key(1600), 100));
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        let fetch = rig.pool.lock().unwrap().fetches[0].clone();
        assert_eq!(fetch.parts.len(), 2);
        let (a, b) = (&fetch.parts[0], &fetch.parts[1]);
        assert_eq!(
            (a.part.begin_tokens, a.part.pages, a.part.row),
            (0, 50, false)
        );
        assert_eq!(
            (b.part.begin_tokens, b.part.pages, b.part.row),
            (800, 50, true)
        );
        assert_eq!((a.slot, b.slot), (None, Some(StateSlotId(3))));
        assert_eq!(a.pages.len() + b.pages.len(), 100);
        assert_eq!(b.pages[0], PageId(50), "disjoint and contiguous");
        assert!(rig.local.0.lock().unwrap().fetches.is_empty());
    }

    #[test]
    fn a_split_inside_a_page_gives_a_row_only_second_part() {
        let mut rig = rig();
        rig.answer(vec![
            rig.candidate(Level::HostDram, 1, 0, 0, &[808], 0),
            rig.candidate(Level::HostNvme, 2, 808, 0, &[815], 0),
        ]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(815));
        let ticket = rig.ticket(&hit);
        assert_eq!(ticket.pages.len(), 50, "floor(815/16) pages");
        rig.store.fetch(hit, ticket).unwrap();
        let fetch = rig.pool.lock().unwrap().fetches[0].clone();
        let b = &fetch.parts[1];
        assert_eq!((b.part.pages, b.part.row, b.pages.len()), (0, true, 0));
        assert_eq!(b.slot, Some(StateSlotId(3)));
    }

    #[test]
    fn a_pool_part_is_never_planned_ahead_of_a_part_of_the_local_tiers() {
        // A neighbour's pages end at 808; this host's own disk ends at 815, in the same page.
        // The plan would take the pages from the neighbour and the row from the disk, which
        // the store cannot run in that order. It must restore from the pool alone.
        let mut rig = rig();
        rig.local_hit(815, Local::Disk);
        rig.answer(vec![rig.candidate(Level::NeighbourGpu, 1, 0, 0, &[808], 0)]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(808));
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        let fetch = rig.pool.lock().unwrap().fetches[0].clone();
        assert_eq!(fetch.parts.len(), 1);
        assert!(fetch.parts[0].part.row, "the pool part carries the row");
        assert!(rig.local.0.lock().unwrap().fetches.is_empty(), "nothing is fetched locally");
    }

    #[test]
    fn the_first_long_restore_is_priced_by_the_fitted_line_not_by_the_few_page_average() {
        // Timed at 2, 16 and 64 pages: 14 ms of latency and 0.3 ms a page, which is 7.3 ms a
        // page at the small size.
        let line = |pages: f64| 14_000.0 + 300.0 * pages;
        let mut rig = rig_with(Ranking::for_test_sized(
            Level::HostDram,
            [line(2.0) / 2.0, line(16.0) / 16.0, line(64.0) / 64.0],
        ));
        // The manager has no estimate to give yet (0): only probe-sized claims so far.
        rig.answer(vec![rig.candidate(Level::HostDram, 1, 0, 0, &[1600], 0)]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.pages, 100);
        let fitted = line(100.0) / 1e6;
        assert!(
            (hit.estimated_seconds - fitted).abs() < 1e-3,
            "{}",
            hit.estimated_seconds
        );
        // Half the prefill of 1600 tokens at 8000 tokens a second is 0.1 s: the restore stands
        // where the small-size average (0.73 s) would have turned it down.
        assert!(crate::worth_fetching(&hit, 8000.0));
        assert!(100.0 * line(2.0) / 2.0 / 1e6 > 0.1);
        // With no estimate of any kind the cost is not held against the restore.
        let mut rig = rig_with(Ranking::default());
        rig.answer(vec![rig.candidate(Level::HostDram, 1, 0, 0, &[1600], 0)]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.estimated_seconds, 0.0);
        assert!(crate::worth_fetching(&hit, 8000.0));
    }

    #[test]
    fn a_second_part_not_worth_fetching_leaves_the_local_hit_alone() {
        let mut rig = rig();
        rig.local_hit(800, Local::Memory);
        rig.answer(vec![rig.candidate(
            Level::HostNvme,
            2,
            800,
            0,
            &[1600],
            50_000_000,
        )]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(800), "the local match is the answer");
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        assert!(rig.pool.lock().unwrap().fetches.is_empty());
        assert_eq!(rig.local.0.lock().unwrap().fetches.len(), 1);
    }

    #[test]
    fn without_an_answer_the_local_lookup_is_used() {
        let mut rig = rig();
        rig.local_hit(800, Local::Disk);
        let history = rig.history.clone();
        assert_eq!(
            rig.store
                .lookup_for(None, &history, 0, u64::MAX)
                .unwrap()
                .key,
            rig.key(800)
        );
        let pending = LookupTicket::watched(LookupState::Pending, || {});
        assert_eq!(
            rig.store
                .lookup_for(Some(&pending), &history, 0, u64::MAX)
                .unwrap()
                .key,
            rig.key(800)
        );
        // And no pool restore is started when the answer holds nothing usable.
        rig.answer(Vec::new());
        assert_eq!(rig.lookup().unwrap().key, rig.key(800));
    }

    // ---- failure ----

    fn two_pool_sources(rig: &Rig) {
        rig.answer(vec![
            rig.candidate(Level::HostDram, 1, 0, 0, &[800], 0),
            rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0),
            rig.candidate(Level::RemoteNvme, 3, 800, 0, &[1600], 0),
        ]);
    }

    #[test]
    fn a_failed_second_part_is_replanned_from_another_source_of_the_same_endpoint() {
        let mut rig = rig();
        two_pool_sources(&rig);
        let hit = rig.lookup().unwrap();
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        let b = rig.pool.lock().unwrap().fetches[0].parts[1]
            .candidate
            .clone();
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 1,
            result: Err(failure(1, (b.source.clone(), b.tier), false)),
        });
        assert!(fetched(&rig.poll()).is_empty(), "replanned, not failed");
        let again = rig.pool.lock().unwrap().fetches[1].clone();
        assert_eq!(again.parts.len(), 2);
        assert_eq!(
            again.parts[1].candidate.source.instance,
            vec![3],
            "the next source"
        );
        assert_eq!(again.parts[1].part.end, rig.key(1600));
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 2,
            result: Ok(9),
        });
        assert_eq!(fetched(&rig.poll()), [(rig.key(1600), true)]);
        // A restore wholly from the pool is counted by the pool, once, with its bytes.
        let stats = rig.store.stats();
        assert_eq!(
            (stats.fetched, stats.fetch_failed, stats.bytes_in),
            (1, 0, 9)
        );
    }

    #[test]
    fn with_no_other_source_the_restore_fails_once() {
        let mut rig = rig();
        rig.answer(vec![
            rig.candidate(Level::HostDram, 1, 0, 0, &[800], 0),
            rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0),
        ]);
        let hit = rig.lookup().unwrap();
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        let b = rig.pool.lock().unwrap().fetches[0].parts[1]
            .candidate
            .clone();
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 1,
            result: Err(failure(1, (b.source.clone(), b.tier), false)),
        });
        assert_eq!(fetched(&rig.poll()), [(rig.key(1600), false)]);
        assert_eq!(rig.pool.lock().unwrap().fetches.len(), 1);
        let stats = rig.store.stats();
        assert_eq!((stats.fetched, stats.fetch_failed), (0, 1));
    }

    #[test]
    fn a_source_without_headroom_is_skipped_for_a_moment() {
        let mut rig = rig();
        two_pool_sources(&rig);
        let hit = rig.lookup().unwrap();
        let ticket = rig.ticket(&hit);
        rig.store.fetch(hit, ticket).unwrap();
        let b = rig.pool.lock().unwrap().fetches[0].parts[1]
            .candidate
            .clone();
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 1,
            result: Err(failure(1, (b.source.clone(), b.tier), true)),
        });
        rig.poll();
        rig.pool.lock().unwrap().events.push(PoolEvent::Fetched {
            id: 2,
            result: Ok(9),
        });
        rig.poll();
        // A new lookup that offers only the refused source finds no pool plan.
        rig.answer(vec![
            rig.candidate(Level::HostDram, 1, 0, 0, &[800], 0),
            rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0),
        ]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(800), "A alone: source 2 is skipped");
    }

    #[test]
    fn a_pool_that_refuses_the_fetch_leaves_no_event_behind() {
        let mut rig = rig();
        rig.answer(vec![rig.candidate(Level::HostDram, 1, 0, 0, &[800], 0)]);
        let hit = rig.lookup().unwrap();
        let ticket = rig.ticket(&hit);
        rig.pool.lock().unwrap().reject = true;
        assert!(rig.store.fetch(hit, ticket).is_err());
        assert!(rig.poll().is_empty());
    }

    // ---- announcements and events ----

    #[test]
    fn gpu_publications_are_announced_and_their_evictions_reported() {
        let mut rig = rig();
        let key = rig.key(160);
        let checkpoint = PublishedCheckpoint {
            id: CheckpointId::new(42),
            key,
            parent: Some(rig.key(64)),
            bucket: [4; 16],
            chain_pages: vec![PageId(1), PageId(2)],
            tail_pages: 2,
            slot: StateSlotId(1),
        };
        // Publishing announces the GPU holding and copies nothing.
        assert_eq!(rig.store.store(&checkpoint).unwrap(), StoreOutcome::Noted);
        assert!(rig.copies().is_empty());
        let saved = rig.pool.lock().unwrap().saved.clone();
        assert_eq!(saved.len(), 1);
        assert_eq!(
            (saved[0].generation, saved[0].tier, saved[0].tail_pages),
            (42, Tier::Gpu, 2)
        );
        assert_eq!(saved[0].parent, Some(rig.key(64)));
        let mut heir = checkpoint.clone();
        heir.id = CheckpointId::new(43);
        rig.store.evicted(key, 42, Some(&heir));
        rig.store.evicted(key, 41, None);
        rig.store.evictions_lost();
        let state = rig.pool.lock().unwrap();
        assert_eq!(state.evicted.len(), 2);
        assert_eq!(state.evicted[0].3.as_ref().map(|h| h.generation), Some(43));
        assert_eq!(state.lost, 1);
    }

    #[test]
    fn the_local_end_is_held_when_a_lookup_begins_and_export_events_reach_the_worker() {
        let mut rig = rig();
        rig.local_hit(800, Local::Memory);
        let history = rig.history.clone();
        assert!(rig.store.begin_lookup(&history).is_some());
        let held = rig.pool.lock().unwrap().held[0].clone();
        assert_eq!(
            held,
            [HeldRange {
                level: Level::HostDram,
                end: rig.key(800)
            }]
        );
        let export = ExportId(5);
        rig.pool.lock().unwrap().events.push(PoolEvent::Export(
            super::super::shared::ExportRequest {
                export,
                key: rig.key(800),
                generation: 3,
                begin_tokens: 0,
                want_row: true,
                deadline: Instant::now(),
                reserved_bytes: 1,
                tier: Tier::Gpu,
            },
        ));
        rig.pool
            .lock()
            .unwrap()
            .events
            .push(PoolEvent::ExportEnded { export });
        let events = rig.poll();
        assert!(
            matches!(events[0], StoreEvent::ExportRequested { export: e, generation: 3, .. } if e == export)
        );
        assert!(matches!(events[1], StoreEvent::ExportEnded { export: e } if e == export));
    }

    #[test]
    fn host_tier_exports_are_pinned_by_the_store_and_never_reach_the_worker() {
        let mut rig = rig();
        assert!(
            rig.local.0.lock().unwrap().announced,
            "the store announces its tiers"
        );
        let key = rig.key(800);
        let request = |export: u64, tier: Tier| {
            PoolEvent::Export(super::super::shared::ExportRequest {
                export: ExportId(export),
                tier,
                key,
                generation: 3,
                begin_tokens: 0,
                want_row: true,
                deadline: Instant::now(),
                reserved_bytes: 0,
            })
        };
        let events = vec![request(1, Tier::Dram), request(2, Tier::Nvme)];
        rig.pool.lock().unwrap().events.extend(events);
        assert!(rig.poll().is_empty());
        {
            let state = rig.pool.lock().unwrap();
            assert_eq!(state.host_pinned, [ExportId(1), ExportId(2)]);
        }
        assert_eq!(rig.local.0.lock().unwrap().pinned[1].0, Tier::Nvme);
        rig.local.0.lock().unwrap().refuse_pin = Some(ExportRefusal::NotFound);
        let refused = request(3, Tier::Dram);
        rig.pool.lock().unwrap().events.push(refused);
        assert!(rig.poll().is_empty());
        assert_eq!(
            rig.pool.lock().unwrap().refused,
            [(ExportId(3), ExportRefusal::NotFound)]
        );
    }

    #[test]
    fn a_candidate_that_disagrees_with_the_history_is_dropped_and_the_others_kept() {
        let mut rig = rig();
        let mut forged = rig.candidate(Level::HostDram, 1, 0, 0, &[1200], 0);
        forged.plan.nodes[0].key.digest[0] ^= 1;
        forged.plan.key.digest[0] ^= 1;
        let good = rig.candidate(Level::HostNvme, 2, 0, 0, &[800], 0);
        rig.answer(vec![forged, good]);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(800));
    }

    #[test]
    fn a_candidate_starting_above_what_is_held_is_discarded_and_one_at_the_local_end_is_kept() {
        let mut rig = rig();
        // Nothing local and nothing resident: a candidate that begins at 800 leaves a hole.
        rig.answer(vec![rig.candidate(Level::HostNvme, 2, 800, 0, &[1600], 0)]);
        assert!(rig.lookup().is_none());
        // With a local part ending at 800 it is the second part.
        rig.local_hit(800, Local::Memory);
        let hit = rig.lookup().unwrap();
        assert_eq!(hit.key, rig.key(1600));
    }

    fn scratch() -> Scratch {
        Scratch {
            pages: (0..64).map(PageId).collect(),
            slot: StateSlotId(1),
        }
    }

    #[test]
    fn levels_are_ranked_by_what_the_stores_and_the_manager_measure_and_only_when_the_topology_changes()
     {
        let mut rig = rig();
        rig.pool.lock().unwrap().topology = 5;
        // Own DRAM is slow here; the manager serves host NVMe quickly.
        rig.local.0.lock().unwrap().probe = Some((Local::Memory, 500));
        rig.pool.lock().unwrap().manager_level = Some(Level::HostNvme);
        assert!(rig.store.needs_characterization());
        let line = rig
            .store
            .characterize(&scratch())
            .unwrap()
            .expect("measured");
        assert!(line.contains("changed from the default"), "{line}");
        assert!(line.contains("not measured"), "{line}");
        let ranking = rig.store.pool().ranking();
        assert!(ranking.rank(Level::HostNvme) < ranking.rank(Level::HostDram));
        assert!(ranking.us_per_page(Level::HostDram).unwrap() > 400.0);
        assert!(ranking.us_per_page(Level::NeighbourGpu).is_none());
        assert!(
            ranking.us_per_page(Level::RemoteNvme).is_none(),
            "the manager serves it as host NVMe"
        );
        // Nothing moved: nothing is measured again.
        assert!(!rig.store.needs_characterization());
        assert!(rig.store.characterize(&scratch()).unwrap().is_none());
        // A different manager or link measures again.
        rig.pool.lock().unwrap().topology = 6;
        assert!(rig.store.needs_characterization());
        assert!(rig.store.characterize(&scratch()).unwrap().is_some());
        // The pool's events that were not the probe's are kept for the worker.
        assert!(rig.poll().is_empty());
    }

    #[test]
    fn a_pinned_order_is_kept_whatever_is_measured_and_without_stores_or_manager_nothing_is_measured()
     {
        let local = FakeLocal::default();
        let pool = Arc::new(Mutex::new(PoolState::default()));
        pool.lock().unwrap().topology = 1;
        let ranking = Ranking::default();
        let order = [
            Level::RemoteNvme,
            Level::HostNvme,
            Level::HostDram,
            Level::NeighbourGpu,
        ];
        let mut store = PooledStore::new(
            local.clone(),
            FakePool(
                pool.clone(),
                ranking,
                Announcer::new(Arc::new(tokio::sync::Notify::new())),
            ),
            identity(),
            &layout(),
        )
        .with_pinned_order(Some(order))
        .unwrap();
        local.0.lock().unwrap().probe = Some((Local::Memory, 10));
        let line = store.characterize(&scratch()).unwrap().unwrap();
        assert!(line.contains("pinned"), "{line}");
        assert_eq!(&store.pool().ranking().order()[1..], &order);
        // With neither tier nor manager, every level stays where it was.
        let mut bare = rig();
        let line = bare.store.characterize(&scratch()).unwrap().unwrap();
        assert!(line.contains("default order"), "{line}");
    }

    // ---- the eviction cascade ----

    impl Rig {
        fn retiring(&self, page: u64) -> Retiring {
            Retiring {
                tier: Tier::Nvme,
                key: self.key(page * PT),
                generation: page,
                parent: (page > 1).then(|| self.key((page - 1) * PT)),
                bucket: [page as u8; 16],
                tail_pages: 1,
                row: true,
            }
        }

        fn offer(&self, records: &[Retiring]) {
            self.local
                .0
                .lock()
                .unwrap()
                .retiring
                .extend(records.iter().cloned());
        }

        fn ready(&self, ready: bool) {
            self.pool.lock().unwrap().ready = ready;
        }

        /// The ids of the puts started and not yet answered, with their keys.
        fn started(&self) -> Vec<(u64, PrefixKey)> {
            self.pool
                .lock()
                .unwrap()
                .puts
                .iter()
                .map(|(id, request)| (*id, request.key))
                .collect()
        }

        fn end(&self, id: u64, outcome: PutOutcome) {
            self.pool
                .lock()
                .unwrap()
                .events
                .push(PoolEvent::Put { id, outcome });
        }

        fn retired(&self) -> Vec<(PrefixKey, Retirement)> {
            std::mem::take(&mut self.local.0.lock().unwrap().retired)
        }
    }

    #[test]
    fn a_retiring_record_is_put_and_released_only_when_the_manager_commits_it() {
        let mut rig = rig();
        assert!(rig.local.0.lock().unwrap().cascaded);
        rig.ready(true);
        let record = rig.retiring(5);
        rig.offer(std::slice::from_ref(&record));
        rig.poll();
        assert_eq!(rig.local.0.lock().unwrap().spill, Some(true));
        // The put describes the record and its pinned chain.
        {
            let state = rig.pool.lock().unwrap();
            let [(_, request)] = &state.puts[..] else {
                panic!("one put expected");
            };
            assert_eq!(
                (request.key, request.parent, request.bucket),
                (record.key, record.parent, record.bucket)
            );
            assert_eq!(
                (request.chain_pages, request.tail_pages, request.row),
                (10, 1, true)
            );
            assert_eq!(request.generation, 5);
            assert!(matches!(request.source, PutSource::Host(_)));
        }
        assert_eq!(
            rig.local.0.lock().unwrap().pinned,
            [(Tier::Nvme, record.key)]
        );
        // Until the manager answers, the source keeps the record.
        rig.poll();
        assert!(rig.retired().is_empty());
        let (id, _) = rig.started()[0];
        rig.end(id, PutOutcome::Stored { bytes: 1 });
        rig.poll();
        assert_eq!(rig.retired(), [(record.key, Retirement::Moved)]);
    }

    #[test]
    fn a_record_the_manager_already_holds_is_released_without_another_copy() {
        let mut rig = rig();
        rig.ready(true);
        let record = rig.retiring(3);
        rig.offer(std::slice::from_ref(&record));
        rig.poll();
        let (id, _) = rig.started()[0];
        rig.end(id, PutOutcome::Present);
        rig.poll();
        assert_eq!(rig.retired(), [(record.key, Retirement::Moved)]);
    }

    #[test]
    fn a_refusal_for_room_or_readiness_drops_the_record_at_once_and_is_not_retried() {
        for refusal in [
            PutRefusal::Budget,
            PutRefusal::Queue,
            PutRefusal::NotReady,
            PutRefusal::Quota,
            PutRefusal::Oversize,
        ] {
            let mut rig = rig();
            rig.ready(true);
            let record = rig.retiring(4);
            rig.offer(std::slice::from_ref(&record));
            rig.poll();
            let (id, _) = rig.started()[0];
            rig.end(id, PutOutcome::Refused(refusal));
            rig.poll();
            assert_eq!(rig.retired(), [(record.key, Retirement::Dropped)]);
            rig.poll();
            assert_eq!(rig.started().len(), 1, "{refusal:?} is not retried");
        }
    }

    #[test]
    fn a_failed_write_keeps_the_record_for_one_more_try_and_the_second_failure_drops_it() {
        for failure in [
            PutOutcome::Failed("the session was lost".into()),
            PutOutcome::Cancelled,
            PutOutcome::Refused(PutRefusal::Io),
            PutOutcome::Refused(PutRefusal::Invalid),
            PutOutcome::Refused(PutRefusal::Inflight),
        ] {
            let mut rig = rig();
            rig.ready(true);
            let record = rig.retiring(6);
            rig.offer(std::slice::from_ref(&record));
            rig.poll();
            rig.end(rig.started()[0].0, failure);
            rig.poll();
            assert_eq!(rig.retired(), [(record.key, Retirement::Failed)]);
            // The tier offers it again at its next pass.
            rig.offer(std::slice::from_ref(&record));
            rig.poll();
            let started = rig.started();
            assert_eq!(started.len(), 2);
            rig.end(started[1].0, PutOutcome::Failed("again".into()));
            rig.poll();
            assert_eq!(rig.retired(), [(record.key, Retirement::Dropped)]);
        }
        // A success in between clears the count.
        let mut rig = rig();
        rig.ready(true);
        let record = rig.retiring(6);
        rig.offer(std::slice::from_ref(&record));
        rig.poll();
        rig.end(rig.started()[0].0, PutOutcome::Cancelled);
        rig.poll();
        rig.offer(std::slice::from_ref(&record));
        rig.poll();
        rig.end(rig.started()[1].0, PutOutcome::Present);
        rig.poll();
        rig.offer(std::slice::from_ref(&record));
        rig.poll();
        rig.end(rig.started()[2].0, PutOutcome::Cancelled);
        rig.poll();
        assert_eq!(
            rig.retired(),
            [
                (record.key, Retirement::Failed),
                (record.key, Retirement::Moved),
                (record.key, Retirement::Failed)
            ]
        );
    }

    #[test]
    fn a_source_that_cannot_be_pinned_counts_as_a_failed_write() {
        let mut rig = rig();
        rig.ready(true);
        rig.local.0.lock().unwrap().refuse_pin = Some(ExportRefusal::ChainGap);
        let record = rig.retiring(2);
        for expected in [Retirement::Failed, Retirement::Dropped] {
            rig.offer(std::slice::from_ref(&record));
            rig.poll();
            assert_eq!(rig.retired(), [(record.key, expected)]);
        }
        assert!(rig.started().is_empty());
    }

    #[test]
    fn an_unavailable_manager_passes_records_on_at_once_and_the_tier_stops_spilling() {
        let mut rig = rig();
        // Not ready: nothing is put, and every offered record is dropped.
        let records: Vec<_> = (1..=3).map(|n| rig.retiring(n)).collect();
        rig.offer(&records);
        rig.poll();
        assert_eq!(rig.local.0.lock().unwrap().spill, Some(false));
        assert!(rig.started().is_empty());
        assert_eq!(
            rig.retired(),
            records
                .iter()
                .map(|r| (r.key, Retirement::Dropped))
                .collect::<Vec<_>>()
        );
        // A pool whose command queue refuses the put does the same.
        rig.ready(true);
        rig.pool.lock().unwrap().put_error = true;
        rig.offer(&records[..1]);
        rig.poll();
        assert_eq!(rig.retired(), [(records[0].key, Retirement::Dropped)]);
        assert_eq!(rig.local.0.lock().unwrap().spill, Some(true));
        // A manager that goes away while records wait drops them too.
        rig.pool.lock().unwrap().put_error = false;
        let many: Vec<_> = (1..=10).map(|n| rig.retiring(n)).collect();
        rig.offer(&many);
        rig.poll();
        assert_eq!(rig.started().len(), 4);
        rig.ready(false);
        rig.poll();
        assert_eq!(rig.retired().len(), 6);
    }

    #[test]
    fn a_full_queue_drops_the_coldest_waiting_records_and_never_blocks() {
        use super::super::cascade::{MOVES_MAX, PENDING_MAX};
        let mut rig = rig();
        rig.ready(true);
        let total = MOVES_MAX + PENDING_MAX + 3;
        let records: Vec<_> = (1..=total as u64).map(|n| rig.retiring(n)).collect();
        rig.offer(&records);
        rig.poll();
        // The coldest four were started; of those left waiting, the three
        // that overflowed the queue were the coldest.
        assert_eq!(
            rig.started()
                .iter()
                .map(|(_, key)| *key)
                .collect::<Vec<_>>(),
            records[..MOVES_MAX]
                .iter()
                .map(|r| r.key)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rig.retired(),
            records[MOVES_MAX..MOVES_MAX + 3]
                .iter()
                .map(|r| (r.key, Retirement::Dropped))
                .collect::<Vec<_>>()
        );
        assert_eq!(rig.store.cascade.in_progress(), MOVES_MAX + PENDING_MAX);
        // As moves end, the waiting records start in order.
        let first = rig.started()[0];
        rig.end(first.0, PutOutcome::Stored { bytes: 1 });
        rig.poll();
        assert_eq!(rig.started().len(), MOVES_MAX + 1);
        assert_eq!(rig.started()[MOVES_MAX].1, records[MOVES_MAX + 3].key);
    }

    #[test]
    fn puts_the_cascade_did_not_start_are_left_for_their_owner() {
        let mut rig = rig();
        rig.ready(true);
        rig.end(900, PutOutcome::Present);
        rig.poll();
        let puts = rig.store.take_puts();
        assert!(matches!(puts.as_slice(), [(900, PutOutcome::Present)]));
        assert!(rig.retired().is_empty());
    }

    // ---- selective write-through at publication ----

    impl Rig {
        fn publication(&self, end: u64) -> PublishedCheckpoint {
            let pages = (end / PT) as usize;
            PublishedCheckpoint {
                id: CheckpointId::new(5),
                key: self.key(end),
                parent: None,
                bucket: [3; 16],
                chain_pages: (0..pages as u32).map(PageId).collect(),
                tail_pages: pages,
                slot: StateSlotId(1),
            }
        }

        fn copies(&self) -> Vec<PrefixKey> {
            self.local.0.lock().unwrap().stored.clone()
        }

        fn announced_on_gpu(&self) -> usize {
            self.pool
                .lock()
                .unwrap()
                .saved
                .iter()
                .filter(|entry| entry.tier == Tier::Gpu)
                .count()
        }
    }

    #[test]
    fn a_publication_is_noted_and_announced_and_copies_nothing_until_written_back() {
        let mut rig = rig();
        let checkpoint = rig.publication(800);
        assert_eq!(rig.store.store(&checkpoint).unwrap(), StoreOutcome::Noted);
        assert!(rig.copies().is_empty());
        assert_eq!(rig.announced_on_gpu(), 1);
        assert_eq!(
            rig.store.write_back(&checkpoint).unwrap(),
            StoreOutcome::Queued
        );
        assert_eq!(rig.copies(), [checkpoint.key]);
        assert_eq!(rig.announced_on_gpu(), 1, "announced once");
    }

    #[test]
    fn a_checkpoint_cheaper_to_recompute_than_to_restore_is_never_stored() {
        let mut rig = rig();
        let checkpoint = rig.publication(800);
        // Prefill so fast that no restore can beat it.
        rig.store.set_prefill_rate(1e12);
        assert_eq!(
            rig.store.write_back(&checkpoint).unwrap(),
            StoreOutcome::Oversize
        );
        assert!(rig.copies().is_empty());
        // At the ordinary prefill rate it is worth keeping again.
        rig.store.set_prefill_rate(INITIAL_PREFILL_RATE);
        assert_eq!(
            rig.store.write_back(&checkpoint).unwrap(),
            StoreOutcome::Queued
        );
    }

    #[test]
    fn a_checkpoint_the_pools_dram_already_holds_is_present_and_not_copied_again() {
        let mut rig = rig();
        // A lookup showed another instance's DRAM holding the endpoint; an
        // NVMe holding does not count for an instance with a memory tier.
        rig.answer(vec![
            rig.candidate(Level::HostDram, 2, 0, 0, &[800], 0),
            rig.candidate(Level::HostNvme, 3, 0, 0, &[1600], 0),
        ]);
        rig.lookup();
        let held = rig.publication(800);
        assert_eq!(rig.store.write_back(&held).unwrap(), StoreOutcome::Present);
        assert!(rig.copies().is_empty());
        let farther = rig.publication(1600);
        assert_eq!(
            rig.store.write_back(&farther).unwrap(),
            StoreOutcome::Queued
        );
        assert_eq!(rig.copies(), [farther.key]);
        // A key no lookup showed is copied.
        let unknown = rig.publication(400);
        assert_eq!(
            rig.store.write_back(&unknown).unwrap(),
            StoreOutcome::Queued
        );
    }

    #[test]
    fn a_full_copy_queue_tells_the_worker_to_retry_and_the_tiers_answers_pass_through() {
        let mut rig = rig();
        let checkpoint = rig.publication(800);
        rig.local.0.lock().unwrap().copies_in_flight = 16;
        assert_eq!(
            rig.store.write_back(&checkpoint).unwrap(),
            StoreOutcome::Retry(StoreReason::Queue)
        );
        assert!(rig.copies().is_empty());
        rig.local.0.lock().unwrap().copies_in_flight = 0;
        for inner in [
            StoreOutcome::Retry(StoreReason::Queue),
            StoreOutcome::Retry(StoreReason::Slabs),
            StoreOutcome::Retry(StoreReason::Inflight),
            StoreOutcome::Retry(StoreReason::Budget),
            StoreOutcome::Present,
            StoreOutcome::Oversize,
            StoreOutcome::Queued,
        ] {
            rig.local.0.lock().unwrap().store_outcome = Some(inner);
            assert_eq!(rig.store.write_back(&checkpoint).unwrap(), inner);
        }
    }
}
