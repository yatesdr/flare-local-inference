//! The announcement index: what this instance holds, in every tier, and the
//! ordered notices that tell the manager about changes.
//!
//! One mutex covers the announced set, the sequence counter and the unsent
//! queue, so a snapshot is a single cut: its watermark is the counter at that
//! moment and its entries are exactly the effect of notices up to it. Callers
//! (the worker, the host tiers) never wait on the network: a full queue is
//! dropped and a snapshot takes its place.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use tokio::sync::Notify;

use crate::PrefixKey;

use super::types::Tier;

/// Unsent notices kept before the queue is dropped in favour of a snapshot.
pub const QUEUE_BOUND: usize = 1 << 16;
/// Announced entries kept when the manager has not stated a bound.
pub const DEFAULT_MAX_ENTRIES: usize = 4_000_000;

/// One (key, tier) holding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: PrefixKey,
    pub parent: Option<PrefixKey>,
    pub bucket: [u8; 16],
    /// Pages the record owns, not the whole chain.
    pub tail_pages: u32,
    /// The record carries the state row, so it can end a restore.
    pub row: bool,
    pub tier: Tier,
    pub generation: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Notice {
    Saved(IndexEntry),
    Spilled {
        to: IndexEntry,
        from_tier: Tier,
        from_generation: u64,
    },
    Evicted {
        key: PrefixKey,
        tier: Tier,
        generation: u64,
        heir: Option<IndexEntry>,
    },
}

/// The announced set as of sequence `watermark`.
#[derive(Debug)]
pub struct Snapshot {
    pub watermark: u64,
    pub entries: Vec<IndexEntry>,
}

struct State {
    entries: HashMap<(PrefixKey, Tier), IndexEntry>,
    /// Notices are queued and numbered only while a session is open.
    active: bool,
    /// Highest sequence assigned in this session.
    sequence: u64,
    queue: VecDeque<Notice>,
    overflowed: bool,
    max_entries: usize,
}

impl State {
    /// Sequence of the notice at the front of the queue.
    fn front(&self) -> u64 {
        self.sequence - self.queue.len() as u64 + 1
    }

    fn upsert(&mut self, entry: &IndexEntry) -> bool {
        let slot = (entry.key, entry.tier);
        match self.entries.get(&slot) {
            Some(held) if held.generation > entry.generation => false,
            Some(_) => {
                self.entries.insert(slot, entry.clone());
                true
            }
            None if self.entries.len() >= self.max_entries => false,
            None => {
                self.entries.insert(slot, entry.clone());
                true
            }
        }
    }

    fn remove(&mut self, key: PrefixKey, tier: Tier, generation: u64) -> bool {
        if self
            .entries
            .get(&(key, tier))
            .is_some_and(|held| held.generation <= generation)
        {
            self.entries.remove(&(key, tier));
            return true;
        }
        false
    }

    fn push(&mut self, notice: Notice) {
        if !self.active {
            return;
        }
        if self.queue.len() >= QUEUE_BOUND {
            self.queue.clear();
            self.overflowed = true;
            return;
        }
        self.sequence += 1;
        self.queue.push_back(notice);
    }
}

/// A handle to the announcement index; cloneable and usable from any thread.
#[derive(Clone)]
pub struct Announcer {
    state: Arc<Mutex<State>>,
    wake: Arc<Notify>,
}

impl Announcer {
    pub(crate) fn new(wake: Arc<Notify>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                entries: HashMap::new(),
                active: false,
                sequence: 0,
                queue: VecDeque::new(),
                overflowed: false,
                max_entries: DEFAULT_MAX_ENTRIES,
            })),
            wake,
        }
    }

    /// A holding appeared. Never blocks and never fails the caller.
    pub fn saved(&self, entry: IndexEntry) {
        let mut state = self.state.lock().unwrap();
        if state.upsert(&entry) {
            state.push(Notice::Saved(entry));
        }
        drop(state);
        self.wake.notify_one();
    }

    /// A holding moved down the cascade: `to` is added and the `from` holding
    /// removed as one step, so a lookup never sees neither.
    pub fn spilled(&self, to: IndexEntry, from: (Tier, u64)) {
        let mut state = self.state.lock().unwrap();
        let key = to.key;
        let added = state.upsert(&to);
        let removed = state.remove(key, from.0, from.1);
        if added || removed {
            state.push(Notice::Spilled {
                to,
                from_tier: from.0,
                from_generation: from.1,
            });
        }
        drop(state);
        self.wake.notify_one();
    }

    /// A holding was removed. `heir` is the single child that took over its
    /// pages when compaction removed the node as an endpoint.
    pub fn evicted(&self, key: PrefixKey, tier: Tier, generation: u64, heir: Option<IndexEntry>) {
        let mut state = self.state.lock().unwrap();
        let removed = state.remove(key, tier, generation);
        let updated = heir.as_ref().is_some_and(|heir| state.upsert(heir));
        if removed || updated {
            state.push(Notice::Evicted {
                key,
                tier,
                generation,
                heir,
            });
        }
        drop(state);
        self.wake.notify_one();
    }

    /// Every GPU holding is forgotten and the manager's view is replaced by a
    /// new snapshot. For when eviction notices were lost: holdings that were
    /// announced and have since left the GPU cannot be told apart, so none is
    /// kept; those still resident are announced again when they are published
    /// again.
    pub fn forget_gpu(&self) {
        let mut state = self.state.lock().unwrap();
        state.entries.retain(|(_, tier), _| *tier != Tier::Gpu);
        if state.active {
            state.queue.clear();
            state.overflowed = true;
        }
        drop(state);
        self.wake.notify_one();
    }

    /// Entries announced beyond `max` are held back: they are hints only.
    pub(crate) fn set_max_entries(&self, max: usize) {
        self.state.lock().unwrap().max_entries = max;
    }

    /// A session opened: numbering restarts at 1, and the first snapshot is
    /// the cut at watermark 0.
    pub(crate) fn begin_session(&self) -> Snapshot {
        let mut state = self.state.lock().unwrap();
        state.active = true;
        state.sequence = 0;
        state.queue.clear();
        state.overflowed = false;
        Snapshot {
            watermark: 0,
            entries: state.entries.values().cloned().collect(),
        }
    }

    pub(crate) fn end_session(&self) {
        let mut state = self.state.lock().unwrap();
        state.active = false;
        state.queue.clear();
        state.overflowed = false;
    }

    /// One cut: the watermark is the counter now and the entries are exactly the
    /// effect of notices up to it. Notices up to the watermark that were still
    /// queued are discarded; later ones follow the snapshot.
    pub(crate) fn snapshot(&self) -> Snapshot {
        let mut state = self.state.lock().unwrap();
        state.queue.clear();
        state.overflowed = false;
        Snapshot {
            watermark: state.sequence,
            entries: state.entries.values().cloned().collect(),
        }
    }

    /// The unsent queue overflowed since the last snapshot.
    pub(crate) fn overflowed(&self) -> bool {
        self.state.lock().unwrap().overflowed
    }

    /// Up to `max` unsent notices and the sequence of the first.
    pub(crate) fn take_batch(&self, max: usize) -> Option<(u64, Vec<Notice>)> {
        let mut state = self.state.lock().unwrap();
        if state.queue.is_empty() || max == 0 {
            return None;
        }
        let first = state.front();
        let count = max.min(state.queue.len());
        Some((first, state.queue.drain(..count).collect()))
    }

    pub fn len(&self) -> usize {
        self.state.lock().unwrap().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    fn entry(n: u64, tier: Tier, generation: u64) -> IndexEntry {
        IndexEntry {
            key: key(n),
            parent: None,
            bucket: [0; 16],
            tail_pages: 1,
            row: true,
            tier,
            generation,
            bytes: 4096,
        }
    }

    fn announcer() -> Announcer {
        Announcer::new(Arc::new(Notify::new()))
    }

    fn drain(announcer: &Announcer) -> (u64, Vec<Notice>) {
        announcer.take_batch(usize::MAX).unwrap()
    }

    #[test]
    fn notices_are_numbered_from_one_in_each_session() {
        let a = announcer();
        a.saved(entry(1, Tier::Gpu, 1));
        a.begin_session();
        a.saved(entry(2, Tier::Gpu, 2));
        a.saved(entry(3, Tier::Gpu, 3));
        let (first, notices) = drain(&a);
        assert_eq!((first, notices.len()), (1, 2));
        a.saved(entry(4, Tier::Gpu, 4));
        assert_eq!(drain(&a).0, 3);
        a.begin_session();
        a.saved(entry(5, Tier::Gpu, 5));
        assert_eq!(drain(&a).0, 1);
    }

    #[test]
    fn the_snapshot_holds_every_tier_and_a_spill_never_leaves_neither() {
        let a = announcer();
        a.begin_session();
        a.saved(entry(1, Tier::Gpu, 5));
        a.saved(entry(1, Tier::Dram, 2));
        a.spilled(entry(1, Tier::Nvme, 3), (Tier::Dram, 2));
        let snapshot = a.snapshot();
        let mut tiers: Vec<_> = snapshot.entries.iter().map(|e| e.tier).collect();
        tiers.sort();
        assert_eq!(tiers, [Tier::Gpu, Tier::Nvme]);
        assert_eq!(snapshot.watermark, 3);
    }

    #[test]
    fn older_generations_never_replace_or_remove_newer_ones() {
        let a = announcer();
        a.begin_session();
        a.saved(entry(1, Tier::Gpu, 5));
        a.saved(entry(1, Tier::Gpu, 3));
        a.evicted(key(1), Tier::Gpu, 4, None);
        // Only the first save changed anything.
        assert_eq!(drain(&a).1.len(), 1);
        assert_eq!(a.len(), 1);
        a.evicted(key(1), Tier::Gpu, 5, None);
        assert_eq!(a.len(), 0);
        // A GPU generation below a host one for the same key does not shadow it.
        a.saved(entry(2, Tier::Dram, 9));
        a.saved(entry(2, Tier::Gpu, 1));
        a.evicted(key(2), Tier::Gpu, 1, None);
        assert_eq!(a.snapshot().entries[0].tier, Tier::Dram);
    }

    #[test]
    fn a_compaction_heir_replaces_the_child_entry_in_one_notice() {
        let a = announcer();
        a.begin_session();
        a.saved(entry(1, Tier::Gpu, 1));
        a.saved(entry(2, Tier::Gpu, 2));
        let mut heir = entry(2, Tier::Gpu, 2);
        heir.tail_pages = 2;
        a.evicted(key(1), Tier::Gpu, 1, Some(heir.clone()));
        let snapshot = a.snapshot();
        assert_eq!(snapshot.entries, [heir]);
    }

    #[test]
    fn a_full_queue_is_dropped_for_a_snapshot_that_loses_and_repeats_nothing() {
        let a = announcer();
        a.begin_session();
        for n in 0..QUEUE_BOUND as u64 {
            a.saved(entry(n, Tier::Gpu, 1));
        }
        assert!(!a.overflowed());
        a.saved(entry(QUEUE_BOUND as u64, Tier::Gpu, 1));
        assert!(a.overflowed());
        assert!(a.take_batch(10).is_none());
        let snapshot = a.snapshot();
        assert!(!a.overflowed());
        assert_eq!(snapshot.entries.len(), QUEUE_BOUND + 1);
        // The overflowing save was applied to the set, never queued; the next
        // notice is numbered after the watermark.
        a.saved(entry(u64::MAX / 128, Tier::Gpu, 1));
        let (first, notices) = drain(&a);
        assert_eq!((first, notices.len()), (snapshot.watermark + 1, 1));
    }

    #[test]
    fn gpu_holdings_whose_notices_were_dropped_converge_in_the_snapshot() {
        let a = announcer();
        a.begin_session();
        for n in 0..QUEUE_BOUND as u64 + 5 {
            a.saved(entry(n, Tier::Gpu, 1));
        }
        a.evicted(key(3), Tier::Gpu, 1, None);
        let snapshot = a.snapshot();
        assert!(snapshot.entries.iter().all(|e| e.key != key(3)));
        assert_eq!(snapshot.entries.len(), QUEUE_BOUND + 4);
    }

    #[test]
    fn forgetting_the_gpu_tier_keeps_host_holdings_and_asks_for_a_snapshot() {
        let a = announcer();
        a.begin_session();
        a.saved(entry(1, Tier::Gpu, 1));
        a.saved(entry(1, Tier::Dram, 1));
        a.saved(entry(2, Tier::Gpu, 2));
        a.forget_gpu();
        assert!(a.overflowed());
        let snapshot = a.snapshot();
        assert_eq!(snapshot.entries.len(), 1);
        assert_eq!(snapshot.entries[0].tier, Tier::Dram);
        assert!(a.take_batch(10).is_none());
    }

    #[test]
    fn entries_beyond_the_bound_are_held_back() {
        let a = announcer();
        a.set_max_entries(2);
        a.begin_session();
        a.saved(entry(1, Tier::Gpu, 1));
        a.saved(entry(2, Tier::Gpu, 1));
        a.saved(entry(3, Tier::Gpu, 1));
        assert_eq!(a.len(), 2);
        assert_eq!(drain(&a).1.len(), 2);
    }

    #[test]
    fn a_mutation_racing_the_cut_lands_exactly_once() {
        let a = announcer();
        a.begin_session();
        a.saved(entry(1, Tier::Gpu, 1));
        let before = a.snapshot();
        a.saved(entry(2, Tier::Gpu, 1));
        a.evicted(key(1), Tier::Gpu, 1, None);
        let (first, notices) = drain(&a);
        assert_eq!(first, before.watermark + 1);
        assert_eq!(notices.len(), 2);
        // The entry was in the snapshot and its eviction arrives after it.
        assert_eq!(before.entries.len(), 1);
        assert_eq!(before.watermark, 1);
    }
}
