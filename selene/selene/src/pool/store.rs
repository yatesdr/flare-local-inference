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
    PrefixKey, PublishedCheckpoint, Result, StoreEvent, StoreOutcome, StoreStats, Tier as Local,
    characterize::Level,
    index::Match,
    plan::{self, Identity, Plan, Request, SourceId, Which},
    store::INITIAL_PREFILL_RATE,
};

use super::{
    announce::IndexEntry,
    client::PoolClient,
    lookup::{LookupState, LookupTicket},
    shared::{FetchPart, PoolEvent, PoolFetch},
    types::{HeldRange, PoolCandidate, SourceRef, Tier},
};

/// What the store uses of the pool client. [`PoolClient`] is the implementation;
/// tests of the store's planning and sequencing substitute a recording one.
pub trait PoolPort {
    fn begin_lookup(&self, history: &dyn History, held: Vec<HeldRange>) -> LookupTicket;
    fn fetch(&mut self, fetch: PoolFetch) -> Result<u64>;
    fn poll(&mut self, events: &mut Vec<PoolEvent>) -> Result<()>;
    fn ranking(&self) -> crate::characterize::Ranking;
    fn saved(&self, entry: IndexEntry);
    fn evicted(&self, key: PrefixKey, tier: Tier, generation: u64, heir: Option<IndexEntry>);
    fn evictions_lost(&self);
    fn export_pinned(&mut self, export: ExportId, chain: ExportChain);
    fn export_refused(&mut self, export: ExportId, reason: ExportRefusal);
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
    fn saved(&self, entry: IndexEntry) {
        PoolClient::saved(self, entry)
    }
    fn evicted(&self, key: PrefixKey, tier: Tier, generation: u64, heir: Option<IndexEntry>) {
        PoolClient::evicted(self, key, tier, generation, heir)
    }
    fn evictions_lost(&self) {
        PoolClient::evictions_lost(self)
    }
    fn export_pinned(&mut self, export: ExportId, chain: ExportChain) {
        PoolClient::export_pinned(self, export, chain)
    }
    fn export_refused(&mut self, export: ExportId, reason: ExportRefusal) {
        PoolClient::export_refused(self, export, reason)
    }
    fn close(&mut self) -> Result<()> {
        PoolClient::close(self)
    }
}

/// The planner's name for this instance's own tiers.
const LOCAL: SourceId = SourceId(0);
/// A source that lacked headroom is not tried again for this long.
const SKIP: Duration = Duration::from_secs(1);
/// Plans remembered between a lookup and its fetch.
const PLANS: usize = 256;
/// Times a failed pool restore is replanned from other sources.
const RETRIES: u32 = 2;

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
        exporter: Option<std::sync::Arc<dyn super::exporter::HostExporter>>,
    ) -> Result<Self> {
        let layout = copier.lock().unwrap().layout().clone();
        let identity = Identity {
            tenant: config.scope.tenant.clone(),
            model: config.scope.model_identity,
            layout: config.scope.layout_fingerprint,
        };
        let pool = PoolClient::start(config, copier, registrar, exporter)?;
        Ok(Self::new(inner, pool, identity, &layout))
    }
}

impl<S: HostStore, P: PoolPort> PooledStore<S, P> {
    pub fn new(inner: S, pool: P, identity: Identity, layout: &crate::Layout) -> Self {
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
        }
    }

    pub fn pool(&self) -> &P {
        &self.pool
    }

    /// The measured prefill rate the planner weighs transfers against.
    pub fn set_prefill_rate(&mut self, tokens_per_second: f64) {
        self.prefill_rate = tokens_per_second;
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
                let us = pool
                    .iter()
                    .find(|c| c.plan.source == part.source && c.plan.generation == part.generation)
                    .and_then(|c| match c.plan.us_per_page {
                        0 => ranking.us_per_page(c.plan.level),
                        measured => Some(f64::from(measured)),
                    })
                    .unwrap_or(0.0);
                part.pages as f64 * us / 1e6
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
                self.restores.remove(&key);
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
        self.restores.remove(&key);
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
        let plan = plan::plan(&request, &candidates)?;
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
        let outcome = self.inner.store(checkpoint)?;
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
        Ok(outcome)
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
                other => events.push(other),
            }
        }
        self.inner_events = inner_events;
        let mut pool_events = std::mem::take(&mut self.pool_events);
        let pool = PoolPort::poll(&mut self.pool, &mut pool_events);
        for event in pool_events.drain(..) {
            match event {
                PoolEvent::Export(request) => events.push(StoreEvent::ExportRequested {
                    export: request.export,
                    key: request.key,
                    generation: request.generation,
                    begin_tokens: request.begin_tokens,
                    want_row: request.want_row,
                    reserved_bytes: request.reserved_bytes,
                }),
                PoolEvent::ExportEnded { export } => {
                    events.push(StoreEvent::ExportEnded { export })
                }
                PoolEvent::Fetched { id, result } => self.pool_finished(id, result, events),
                PoolEvent::Status(_) => {}
            }
        }
        self.pool_events = pool_events;
        inner?;
        pool
    }

    fn stats(&self) -> StoreStats {
        self.inner.stats()
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
        CheckpointId, Layout, PageId, StateSlotId,
        characterize::Ranking,
        pool::{Answer, shared::FetchFailure},
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
        fn store(&mut self, _: &PublishedCheckpoint) -> Result<StoreOutcome> {
            Ok(StoreOutcome::Queued)
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
            StoreStats::default()
        }
        fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct PoolState {
        answer: Option<Arc<Answer>>,
        held: Vec<Vec<HeldRange>>,
        fetches: Vec<PoolFetch>,
        reject: bool,
        events: Vec<PoolEvent>,
        saved: Vec<IndexEntry>,
        evicted: Vec<(PrefixKey, Tier, u64, Option<IndexEntry>)>,
        lost: usize,
    }

    #[derive(Clone)]
    struct FakePool(Arc<Mutex<PoolState>>, Ranking);

    impl PoolPort for FakePool {
        fn begin_lookup(&self, _: &dyn History, held: Vec<HeldRange>) -> LookupTicket {
            let mut state = self.0.lock().unwrap();
            state.held.push(held);
            match state.answer.clone() {
                Some(answer) => LookupTicket::watched(LookupState::Ready(answer), || {}),
                None => LookupTicket::watched(LookupState::Pending, || {}),
            }
        }
        fn fetch(&mut self, fetch: PoolFetch) -> Result<u64> {
            let mut state = self.0.lock().unwrap();
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
            self.1.clone()
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
        fn evictions_lost(&self) {
            self.0.lock().unwrap().lost += 1;
        }
        fn export_pinned(&mut self, _: ExportId, _: ExportChain) {}
        fn export_refused(&mut self, _: ExportId, _: ExportRefusal) {}
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
        let local = FakeLocal::default();
        let pool = Arc::new(Mutex::new(PoolState::default()));
        let ranking = Ranking::for_test(
            Level::DEFAULT_ORDER,
            &[
                (Level::NeighbourGpu, 10.0),
                (Level::HostDram, 10.0),
                (Level::HostNvme, 100.0),
                (Level::RemoteNvme, 200.0),
            ],
        );
        Rig {
            store: PooledStore::new(
                local.clone(),
                FakePool(pool.clone(), ranking),
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
        assert_eq!(rig.store.store(&checkpoint).unwrap(), StoreOutcome::Queued);
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
}
