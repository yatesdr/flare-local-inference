//! State shared between the model worker and the pool's I/O thread: bounded
//! queues in both directions, the status, and the lookup registry.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::Instant,
};

use tokio::sync::Notify;

use crate::{
    Error, ExportChain, ExportId, ExportRefusal, PageId, PrefixKey, Result, StateSlotId,
    characterize::{Level, Ranking},
};

use super::{
    exporter::{HostPin, PutBytes},
    lookup::{Answer, LookupState, LookupTicket, TicketInner},
    types::{HeldRange, PoolCandidate, SourceRef, Tier},
};

/// Commands the worker may queue before new lookups and fetches are refused.
pub const COMMAND_BOUND: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolStatus {
    /// No session: not connected yet, or the manager was lost and drained.
    Local,
    /// Hello, open or the first snapshot is in progress.
    Connecting,
    /// Session open, snapshot installed and the manager READY.
    Ready,
    /// The manager is not READY: no lookups or claims; notices still flow.
    NotReady,
    /// Permanently off for this process (version or encryption mismatch).
    Disabled(String),
}

/// An export request for the model worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportRequest {
    pub export: ExportId,
    /// The tier asked to serve it: the GPU's is answered by the model worker,
    /// a host tier's by the store.
    pub tier: Tier,
    pub key: PrefixKey,
    pub generation: u64,
    pub begin_tokens: u64,
    pub want_row: bool,
    /// When the manager stops waiting for the answer.
    pub deadline: Instant,
    /// Bytes of staging the manager reserved for the whole export.
    pub reserved_bytes: u64,
}

/// Why a pool restore failed, for the caller that may try another source.
#[derive(Clone, Debug)]
pub struct FetchFailure {
    pub error: Error,
    /// The part whose claim or copy failed, if one part is to blame.
    pub part: Option<usize>,
    /// The source of that part.
    pub source: Option<(SourceRef, Tier)>,
    /// The source refused because pinning would not leave it enough room: skip
    /// it for a moment and try another source for the same endpoint.
    pub no_headroom: bool,
}

#[derive(Debug)]
pub enum PoolEvent {
    /// A GPU-tier export for the model worker.
    Export(ExportRequest),
    /// A pool restore finished and every destination write drained.
    Fetched {
        id: u64,
        result: std::result::Result<u64, FetchFailure>,
    },
    /// No copy still reads this export's pages: release its pin, once. Never
    /// produced for an export that is quarantined.
    ExportEnded {
        export: ExportId,
    },
    /// A put ended, after every copy that read its source drained.
    Put {
        id: u64,
        outcome: PutOutcome,
    },
    Status(PoolStatus),
}

/// Where a put's bytes come from.
pub enum PutSource {
    /// The GPU pages of the whole chain, ascending, and the state slot holding
    /// the endpoint row. The caller keeps them pinned until the put ends.
    Gpu {
        pages: Vec<PageId>,
        slot: StateSlotId,
    },
    /// Bytes from a host tier or a probe.
    Host(Box<dyn PutBytes>),
}

/// A record to write into the manager's store. The writer drops its own copy
/// only when the put ends `Stored` or `Present`.
pub struct PutRequest {
    pub key: PrefixKey,
    /// The key of the previous record in the chain, if any.
    pub parent: Option<PrefixKey>,
    pub bucket: [u8; 16],
    /// Pages of the whole chain up to `key`.
    pub chain_pages: u64,
    /// Pages the record owns, counted from its published parent.
    pub tail_pages: u64,
    pub row: bool,
    pub generation: u64,
    pub source: PutSource,
}

/// Why the manager did not store a record. None of them is retried in a loop:
/// the record moves to the next tier or is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutRefusal {
    /// A put for the same key is running.
    Inflight,
    /// Out of write budget or full.
    Budget,
    /// The record can never fit.
    Oversize,
    Quota,
    /// The metadata did not match what was accepted.
    Invalid,
    /// The manager has as many puts open as it allows.
    Queue,
    /// A manager device error.
    Io,
    /// The manager is not serving puts yet.
    NotReady,
}

#[derive(Debug)]
pub enum PutOutcome {
    /// Committed and indexed in the manager's store.
    Stored {
        bytes: u64,
    },
    /// Already stored there: a successful deduplication, nothing moved.
    Present,
    Refused(PutRefusal),
    /// Cancelled by the caller or by closing before it was committed.
    Cancelled,
    /// The put could not be carried out (session lost, copy failed, bad answer).
    Failed(Error),
}

/// One part of a pool restore.
#[derive(Clone, Debug)]
pub struct FetchPart {
    pub candidate: PoolCandidate,
    /// The planner's part: begin, end, first page, page count and row.
    pub part: crate::plan::Part,
    /// Destination pages of this part, in order.
    pub pages: Vec<PageId>,
    /// The state slot that receives the row when this part carries it.
    pub slot: Option<StateSlotId>,
}

/// A restore the pool performs, parts in claim order.
#[derive(Clone, Debug)]
pub struct PoolFetch {
    pub id: u64,
    pub page_tokens: u64,
    pub parts: Vec<FetchPart>,
}

pub(crate) struct LookupSend {
    pub id: u64,
    pub boundaries: Vec<PrefixKey>,
    pub before: u64,
    pub held: Vec<HeldRange>,
    pub level_order: Vec<Level>,
}

pub(crate) enum Command {
    Lookup(LookupSend),
    CancelLookup(u64),
    Fetch(PoolFetch),
    Put {
        id: u64,
        request: PutRequest,
    },
    PutCancel(u64),
    ExportPinned {
        export: ExportId,
        chain: ExportChain,
    },
    HostPinned {
        export: ExportId,
        pin: Box<dyn HostPin>,
    },
    ExportRefused {
        export: ExportId,
        reason: ExportRefusal,
    },
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CoalesceKey {
    last: PrefixKey,
    before: u64,
    held: Vec<(usize, PrefixKey)>,
}

struct Entry {
    key: CoalesceKey,
    held: Vec<HeldRange>,
    waiters: Vec<(u64, Arc<TicketInner>)>,
}

#[derive(Default)]
struct Registry {
    next_wire: u64,
    next_waiter: u64,
    by_key: HashMap<CoalesceKey, u64>,
    entries: HashMap<u64, Entry>,
}

pub(crate) struct Shared {
    pub(crate) wake: Arc<Notify>,
    status: Mutex<PoolStatus>,
    events: Mutex<VecDeque<PoolEvent>>,
    commands: Mutex<VecDeque<Command>>,
    registry: Mutex<Registry>,
    ranking: Mutex<Ranking>,
    topology: std::sync::atomic::AtomicU64,
    /// Lookups the manager accepts at once; 0 until the manager states it.
    max_lookups: AtomicU32,
    /// No lookups start before this instant after a refusal or timeout.
    paused_until: Mutex<Option<Instant>>,
    quarantined: std::sync::atomic::AtomicBool,
}

impl Shared {
    pub(crate) fn new(wake: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            wake,
            status: Mutex::new(PoolStatus::Connecting),
            events: Mutex::new(VecDeque::new()),
            commands: Mutex::new(VecDeque::new()),
            registry: Mutex::new(Registry::default()),
            ranking: Mutex::new(Ranking::default()),
            topology: std::sync::atomic::AtomicU64::new(0),
            max_lookups: AtomicU32::new(0),
            paused_until: Mutex::new(None),
            quarantined: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub(crate) fn status(&self) -> PoolStatus {
        self.status.lock().unwrap().clone()
    }

    /// Record a status and tell the worker if it changed.
    pub(crate) fn set_status(&self, status: PoolStatus) {
        let mut held = self.status.lock().unwrap();
        if *held != status {
            *held = status.clone();
            drop(held);
            let mut events = self.events.lock().unwrap();
            events.retain(|e| !matches!(e, PoolEvent::Status(_)));
            events.push_back(PoolEvent::Status(status));
        }
    }

    pub(crate) fn push_event(&self, event: PoolEvent) {
        self.events.lock().unwrap().push_back(event);
    }

    pub(crate) fn drain_events(&self, out: &mut Vec<PoolEvent>) {
        out.extend(self.events.lock().unwrap().drain(..));
    }

    /// An export request the worker has not taken yet, withdrawn.
    pub(crate) fn withdraw_export(&self, export: ExportId) -> bool {
        let mut events = self.events.lock().unwrap();
        let before = events.len();
        events.retain(|e| !matches!(e, PoolEvent::Export(r) if r.export == export));
        events.len() != before
    }

    /// Queue a command. `false` when the queue is full.
    pub(crate) fn push_command(&self, command: Command, force: bool) -> bool {
        let mut queue = self.commands.lock().unwrap();
        if !force && queue.len() >= COMMAND_BOUND {
            return false;
        }
        queue.push_back(command);
        drop(queue);
        self.wake.notify_one();
        true
    }

    pub(crate) fn take_commands(&self) -> Vec<Command> {
        self.commands.lock().unwrap().drain(..).collect()
    }

    pub(crate) fn ranking(&self) -> Ranking {
        self.ranking.lock().unwrap().clone()
    }

    pub(crate) fn set_ranking(&self, ranking: Ranking) {
        *self.ranking.lock().unwrap() = ranking;
    }

    pub(crate) fn observe_restore(&self, level: Level, pages: usize, elapsed: std::time::Duration) {
        self.ranking.lock().unwrap().observe(level, pages, elapsed);
    }

    /// Identifies the manager and link the session runs over; 0 while no session
    /// is ready. This identifies the topology timed by the startup probe.
    pub(crate) fn topology(&self) -> u64 {
        if self.status() != PoolStatus::Ready {
            return 0;
        }
        self.topology.load(Ordering::Relaxed)
    }

    pub(crate) fn set_topology(&self, topology: u64) {
        self.topology.store(topology, Ordering::Relaxed);
    }

    pub(crate) fn set_max_lookups(&self, max: u32) {
        self.max_lookups.store(max, Ordering::Relaxed);
    }

    pub(crate) fn set_quarantined(&self) {
        self.quarantined.store(true, Ordering::SeqCst);
    }

    pub(crate) fn quarantined(&self) -> bool {
        self.quarantined.load(Ordering::SeqCst)
    }

    pub(crate) fn pause_lookups(&self, until: Instant) {
        *self.paused_until.lock().unwrap() = Some(until);
    }

    /// Start a lookup, or join an identical one already on the wire. The ticket
    /// has no answer when the pool is not ready, lookups are paused or full.
    pub(crate) fn begin_lookup(
        self: &Arc<Self>,
        now: Instant,
        boundaries: Vec<PrefixKey>,
        before: u64,
        held: Vec<HeldRange>,
    ) -> LookupTicket {
        let Some(last) = boundaries.last().copied() else {
            return LookupTicket::none();
        };
        if self.status() != PoolStatus::Ready
            || self
                .paused_until
                .lock()
                .unwrap()
                .is_some_and(|until| now < until)
        {
            return LookupTicket::none();
        }
        let mut held_key: Vec<(usize, PrefixKey)> = held
            .iter()
            .map(|range| (range.level as usize, range.end))
            .collect();
        held_key.sort_by_key(|(level, end)| (*level, end.consumed_tokens));
        let key = CoalesceKey {
            last,
            before,
            held: held_key,
        };
        let inner = TicketInner::new(LookupState::Pending);
        let mut registry = self.registry.lock().unwrap();
        registry.next_waiter += 1;
        let waiter = registry.next_waiter;
        let existing = registry.by_key.get(&key).copied();
        let wire = match existing {
            Some(wire) => {
                registry
                    .entries
                    .get_mut(&wire)
                    .expect("indexed entry")
                    .waiters
                    .push((waiter, inner.clone()));
                wire
            }
            None => {
                let limit = self.max_lookups.load(Ordering::Relaxed).max(1) as usize;
                if registry.entries.len() >= limit {
                    return LookupTicket::none();
                }
                registry.next_wire += 1;
                let wire = registry.next_wire;
                let level_order = self.ranking().order().to_vec();
                let sent = self.push_command(
                    Command::Lookup(LookupSend {
                        id: wire,
                        boundaries,
                        before,
                        held: held.clone(),
                        level_order,
                    }),
                    false,
                );
                if !sent {
                    return LookupTicket::none();
                }
                registry.by_key.insert(key.clone(), wire);
                registry.entries.insert(
                    wire,
                    Entry {
                        key,
                        held,
                        waiters: vec![(waiter, inner.clone())],
                    },
                );
                wire
            }
        };
        drop(registry);
        let shared = self.clone();
        LookupTicket::new(
            inner,
            Box::new(move || shared.withdraw_lookup(wire, waiter)),
        )
    }

    fn withdraw_lookup(&self, wire: u64, waiter: u64) {
        let mut registry = self.registry.lock().unwrap();
        let Some(entry) = registry.entries.get_mut(&wire) else {
            return;
        };
        entry.waiters.retain(|(id, _)| *id != waiter);
        if entry.waiters.is_empty() {
            let entry = registry.entries.remove(&wire).expect("present");
            registry.by_key.remove(&entry.key);
            drop(registry);
            self.push_command(Command::CancelLookup(wire), true);
        }
    }

    /// The lookup finished: every waiter sees the answer, or none.
    pub(crate) fn complete_lookup(&self, wire: u64, candidates: Option<Vec<PoolCandidate>>) {
        let mut registry = self.registry.lock().unwrap();
        let Some(entry) = registry.entries.remove(&wire) else {
            return;
        };
        registry.by_key.remove(&entry.key);
        drop(registry);
        let state = match candidates {
            Some(candidates) => LookupState::Ready(Arc::new(Answer {
                held: entry.held,
                candidates,
            })),
            None => LookupState::None,
        };
        for (_, ticket) in entry.waiters {
            ticket.set(state.clone());
        }
    }

    /// Every lookup still waiting ends with no answer.
    pub(crate) fn fail_lookups(&self) {
        let wires: Vec<u64> = self
            .registry
            .lock()
            .unwrap()
            .entries
            .keys()
            .copied()
            .collect();
        for wire in wires {
            self.complete_lookup(wire, None);
        }
    }

    /// Wire lookups currently registered, for tests.
    #[cfg(test)]
    pub(crate) fn live_lookups(&self) -> usize {
        self.registry.lock().unwrap().entries.len()
    }
}

pub(crate) fn require_ready(status: &PoolStatus) -> Result<()> {
    match status {
        PoolStatus::Ready => Ok(()),
        other => Err(Error::new(format!("pool is not ready ({other:?})"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(n: u64) -> PrefixKey {
        PrefixKey {
            consumed_tokens: n * 64,
            digest: [n as u8; 16],
        }
    }

    fn ready() -> Arc<Shared> {
        let shared = Shared::new(Arc::new(Notify::new()));
        shared.set_status(PoolStatus::Ready);
        shared.set_max_lookups(8);
        shared
    }

    #[test]
    fn the_topology_is_zero_until_the_session_is_ready() {
        let shared = Shared::new(Arc::new(Notify::new()));
        shared.set_topology(7);
        assert_eq!(shared.topology(), 0);
        shared.set_status(PoolStatus::Ready);
        assert_eq!(shared.topology(), 7);
        shared.set_status(PoolStatus::Connecting);
        assert_eq!(shared.topology(), 0);
    }

    fn lookup(shared: &Arc<Shared>, n: u64) -> LookupTicket {
        shared.begin_lookup(
            Instant::now(),
            vec![key(0), key(n)],
            key(n).consumed_tokens + 64,
            Vec::new(),
        )
    }

    #[test]
    fn identical_prefixes_share_one_wire_lookup_and_dropping_one_keeps_the_other() {
        let shared = ready();
        let a = lookup(&shared, 3);
        let b = lookup(&shared, 3);
        let commands = shared.take_commands();
        assert_eq!(commands.len(), 1, "one wire lookup");
        let Command::Lookup(sent) = &commands[0] else {
            panic!("expected a lookup")
        };
        drop(a);
        assert!(shared.take_commands().is_empty(), "the other still waits");
        shared.complete_lookup(sent.id, Some(Vec::new()));
        assert!(matches!(b.state(), LookupState::Ready(_)));
        assert_eq!(shared.live_lookups(), 0);
    }

    #[test]
    fn dropping_the_last_ticket_cancels_and_a_late_reply_is_discarded() {
        let shared = ready();
        let ticket = lookup(&shared, 3);
        let Command::Lookup(sent) = shared.take_commands().remove(0) else {
            panic!()
        };
        drop(ticket);
        let commands = shared.take_commands();
        assert!(matches!(commands[..], [Command::CancelLookup(id)] if id == sent.id));
        shared.complete_lookup(sent.id, Some(Vec::new()));
        assert_eq!(shared.live_lookups(), 0);
    }

    #[test]
    fn a_pool_that_is_not_ready_or_is_paused_gives_tickets_without_answers() {
        let shared = ready();
        shared.set_status(PoolStatus::NotReady);
        assert!(matches!(lookup(&shared, 1).state(), LookupState::None));
        shared.set_status(PoolStatus::Ready);
        shared.pause_lookups(Instant::now() + std::time::Duration::from_secs(5));
        assert!(matches!(lookup(&shared, 1).state(), LookupState::None));
        assert!(shared.take_commands().is_empty());
    }

    #[test]
    fn concurrent_wire_lookups_are_bounded() {
        let shared = ready();
        shared.set_max_lookups(2);
        let tickets: Vec<_> = (1..=3).map(|n| lookup(&shared, n)).collect();
        assert!(matches!(tickets[0].state(), LookupState::Pending));
        assert!(matches!(tickets[2].state(), LookupState::None));
        assert_eq!(shared.take_commands().len(), 2);
    }
}
