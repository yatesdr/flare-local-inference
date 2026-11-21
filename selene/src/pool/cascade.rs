/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 Flare community contributors
 * SPDX-License-Identifier: Apache-2.0
 */

//! Budget and in-flight refusals retry the same put once. A permanent refusal
//! or an exhausted retry on an ancestor leaves one attempt to send the leaf.
//! Other failures keep the record local for a later move.
//! Manager loss unwinds waiting selections; active moves retain their pins
//! until the drained terminal event. While the manager cannot accept records,
//! the local tier's LRU is the end of the chain.

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use crate::{Error, Layout, Result, Retirement, Retiring, Tier};

use super::{
    exporter::{Checksums, HostPin, HostWindow, PutBytes},
    shared::{PutOutcome, PutRefusal, PutRequest, PutSource},
    store::PoolPort,
};

/// Records waiting for a put; selection stops at this bound.
pub(crate) const PENDING_MAX: usize = 64;
/// Puts started at once.
pub(crate) const MOVES_MAX: usize = 4;

/// Supplies a put's bytes from a pinned host chain.
struct PinBytes {
    pin: Box<dyn HostPin>,
    page_bytes: usize,
    row_bytes: usize,
}

impl PutBytes for PinBytes {
    fn fill(
        &mut self,
        first_page: u64,
        page_count: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        let records = self.pin.chain().records.clone();
        let last = records
            .len()
            .checked_sub(1)
            .ok_or_else(|| Error::new("the chain to write is empty"))?;
        let end = first_page + u64::from(page_count);
        let mut sums = Checksums {
            pages: Vec::with_capacity(page_count as usize),
            row: None,
        };
        let (mut base, mut offset) = (0u64, 0usize);
        for (index, record) in records.iter().enumerate() {
            let from = first_page.max(base);
            let to = end.min(base + u64::from(record.pages));
            let count = to.saturating_sub(from) as u32;
            let with_row = row && index == last;
            if count > 0 || with_row {
                let first = if count > 0 {
                    record.first_page + (from - base) as u32
                } else {
                    record.first_page + record.pages
                };
                let window = HostWindow {
                    record: index as u32,
                    first_page: first,
                    page_count: count,
                    row: with_row,
                };
                let len =
                    count as usize * self.page_bytes + if with_row { self.row_bytes } else { 0 };
                let chunk = dst
                    .get_mut(offset..offset + len)
                    .ok_or_else(|| Error::new("the window is smaller than its records"))?;
                let read = self.pin.read(&window, chunk)?;
                sums.pages.extend(read.pages);
                if with_row {
                    sums.row = read.row;
                }
                offset += len;
            }
            base += u64::from(record.pages);
        }
        if offset != dst.len() {
            return Err(Error::new("the window is larger than its records"));
        }
        Ok(sums)
    }
}

struct Move {
    record: Retiring,
    failures: u8,
    leaf_only: bool,
    path: VecDeque<crate::PrefixKey>,
    depth: u64,
    /// Shapes offered on this pass, checked before releasing a locally promoted row.
    offered: Vec<Retiring>,
    started: Instant,
    /// A predecessor's put has its own candidates; the leaf uses `record.ancestor_keys`.
    predecessor_candidates: Option<Vec<crate::PrefixKey>>,
}

impl Move {
    fn candidates(&self) -> &[crate::PrefixKey] {
        self.predecessor_candidates.as_deref().unwrap_or(&self.record.ancestor_keys)
    }
}

pub(crate) struct Cascade {
    /// Records chosen by the local tier, coldest first.
    pending: VecDeque<Move>,
    /// Records whose put is running, by put id.
    moving: HashMap<u64, Move>,
    retry_after: Instant,
    refused_until: Instant,
    #[cfg(test)]
    pub(crate) now: Option<Instant>,
    /// Refusal discards by DRAM/NVMe source tier.
    pub(crate) dropped: [u64; 2],
    pub(crate) refusal_drops: [u64; 5],
    page_bytes: usize,
    row_bytes: usize,
    pub(crate) paths: u64,
    pub(crate) max_depth: u64,
    pub(crate) latency_ns: u64,
    pub(crate) max_latency_ns: u64,
    pub(crate) latency_buckets: [u64; 8],
    /// Root puts the manager stored, by [`crate::RootReason`], with their pages.
    pub(crate) root_records: [u64; crate::RootReason::COUNT],
    pub(crate) root_pages: [u64; crate::RootReason::COUNT],
    pub(crate) deeper_link_records: u64,
    pub(crate) deeper_link_pages: u64,
}

impl Cascade {
    pub(crate) fn new(layout: &Layout) -> Self {
        let now = Instant::now();
        Self {
            paths: 0,
            max_depth: 0,
            latency_ns: 0,
            max_latency_ns: 0,
            latency_buckets: [0; 8],
            root_records: [0; crate::RootReason::COUNT],
            root_pages: [0; crate::RootReason::COUNT],
            deeper_link_records: 0,
            deeper_link_pages: 0,
            pending: VecDeque::new(),
            moving: HashMap::new(),
            retry_after: now,
            refused_until: now,
            #[cfg(test)]
            now: None,
            dropped: [0; 2],
            refusal_drops: [0; 5],
            page_bytes: layout.page_record_bytes as usize,
            row_bytes: layout.row_record_bytes as usize,
        }
    }

    /// Records waiting for a put or being written.
    #[cfg(test)]
    pub(crate) fn in_progress(&self) -> usize {
        self.pending.len() + self.moving.len()
    }

    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = self.now {
            return now;
        }
        Instant::now()
    }

    pub(crate) fn progress(&mut self) {
        self.retry_after = self.now().max(self.refused_until);
    }

    /// Start bounded puts; unavailable destinations leave records local.
    pub(crate) fn pump<S: crate::store::TierStore, P: PoolPort>(
        &mut self,
        inner: &mut S,
        pool: &mut P,
    ) {
        let up = pool.ready();
        let ready = up && self.now() >= self.retry_after;
        inner.set_spill(ready);
        for record in inner.take_retiring() {
            // A failed put's backoff holds the queue; it does not fail what waits in it.
            if up && self.pending.len() + self.moving.len() < PENDING_MAX + MOVES_MAX {
                let mut path = VecDeque::new();
                if record.tier == Tier::Dram && record.parent.is_some() {
                    // Each parent consumes fewer tokens, bounding the walk by the local chain depth.
                    let mut parent = record.parent;
                    while let Some(local) =
                        parent.and_then(|key| inner.retirement_predecessor(key, false))
                    {
                        path.push_front(local.key);
                        parent = local.parent;
                    }
                    // A page-only local root cannot start a manager chain. The first
                    // row-bearing descendant can send those pages from its pinned chain.
                    if path
                        .front()
                        .and_then(|key| inner.retirement_predecessor(*key, false))
                        .is_some_and(|r| r.parent.is_none() && !r.row)
                    {
                        while path
                            .front()
                            .and_then(|key| inner.retirement_predecessor(*key, false))
                            .is_some_and(|r| !r.row)
                        {
                            path.pop_front();
                        }
                    }
                }
                self.pending.push_back(Move {
                    depth: 1 + path.len() as u64,
                    started: self.now(),
                    offered: Vec::new(),
                    path,
                    record,
                    failures: 0,
                    leaf_only: false,
                    predecessor_candidates: None,
                });
            } else {
                inner.retired(record.key, record.tier, Retirement::Failed);
            }
        }
        while ready && self.moving.len() < MOVES_MAX && self.now() >= self.retry_after {
            let Some(entry) = self.pending.pop_front() else {
                break;
            };
            self.begin(inner, pool, entry);
        }
        if !up {
            for Move { record, .. } in self.pending.drain(..) {
                inner.retired(record.key, record.tier, Retirement::Failed);
            }
        }
    }

    fn begin<S: crate::store::TierStore, P: PoolPort>(
        &mut self,
        inner: &mut S,
        pool: &mut P,
        mut entry: Move,
    ) {
        let key = entry.path.front().copied().unwrap_or(entry.record.key);
        let current = (entry.record.tier == Tier::Dram)
            .then(|| inner.retirement_predecessor(key, key != entry.record.key))
            .flatten();
        let mut record = match current {
            Some(record) => record,
            None if key == entry.record.key => entry.record.clone(),
            None => return self.failed(inner, &entry.record),
        };
        let pin = match inner.pin_export(record.tier, record.key, 0, record.row) {
            Ok(pin) => pin,
            Err(_) => return self.failed(inner, &entry.record),
        };
        entry.predecessor_candidates = (key != entry.record.key)
            .then(|| std::mem::take(&mut record.ancestor_keys));
        let request = PutRequest {
            key: record.key,
            parent: record.parent,
            ancestor_keys: entry.candidates().to_vec(),
            bucket: record.bucket,
            chain_pages: pin.chain().total_pages,
            tail_pages: record.tail_pages as u64,
            capture_role: record.capture_role,
            child_paths: record.child_paths,
            descendant_row: record.descendant_row,
            next_child: entry
                .path
                .get(1)
                .copied()
                .or_else(|| entry.path.front().map(|_| entry.record.key)),
            row: record.row,
            source: PutSource::Host(Box::new(PinBytes {
                pin,
                page_bytes: self.page_bytes,
                row_bytes: self.row_bytes,
            })),
        };
        if record.tier == Tier::Dram {
            entry.offered.push(record);
        }
        match pool.put(request) {
            Ok(id) => {
                self.moving.insert(id, entry);
            }
            Err(_) => self.failed(inner, &entry.record),
        }
    }

    fn back_off(&mut self) {
        self.retry_after = self.now() + Duration::from_millis(50);
    }

    fn failed<S: crate::store::TierStore>(&mut self, inner: &mut S, record: &Retiring) {
        self.back_off();
        inner.set_spill(false);
        inner.retired(record.key, record.tier, Retirement::Failed);
    }

    /// A put ended. False when it was not one of the cascade's.
    pub(crate) fn ended<S: crate::store::TierStore>(
        &mut self,
        inner: &mut S,
        id: u64,
        outcome: &PutOutcome,
    ) -> bool {
        let Some(mut entry) = self.moving.remove(&id) else {
            return false;
        };
        if let PutOutcome::Stored { pages, linked, .. } = outcome {
            let put = entry.offered.last().unwrap_or(&entry.record);
            let candidates = entry.candidates();
            if let Some(parent) = linked {
                if candidates.iter().skip(1).any(|key| key == parent) {
                    self.deeper_link_records += 1;
                    self.deeper_link_pages += pages;
                }
            } else if put.parent.is_none() {
                let reason = crate::RootReason::from_nearest(candidates.first().copied()) as usize;
                self.root_records[reason] += 1;
                self.root_pages[reason] += pages;
            }
        }
        let record = &entry.record;
        match outcome {
            PutOutcome::Stored { .. } | PutOutcome::Present => {
                if entry.path.pop_front().is_some() {
                    entry.failures = 0;
                    self.pending.push_front(entry);
                } else {
                    if record.tier == Tier::Dram {
                        if entry.offered.iter().any(|old| {
                            inner
                                .retirement_predecessor(old.key, false)
                                .is_some_and(|current| {
                                    current.row
                                        && (!old.row
                                            || current.capture_role > old.capture_role
                                            || (old.descendant_row && !current.descendant_row)
                                            || (old.child_paths < 2 && current.child_paths >= 2))
                                })
                        }) {
                            self.failed(inner, record);
                            return true;
                        }
                        self.paths += 1;
                        self.max_depth = self.max_depth.max(entry.depth);
                        let elapsed = self.now().duration_since(entry.started).as_nanos() as u64;
                        for (count, ms) in self
                            .latency_buckets
                            .iter_mut()
                            .zip(crate::RETIREMENT_LATENCY_MS)
                        {
                            *count += u64::from(elapsed <= ms * 1_000_000);
                        }
                        self.latency_ns += elapsed;
                        self.max_latency_ns = self.max_latency_ns.max(elapsed);
                    }
                    let how = if entry.leaf_only {
                        Retirement::MovedLeaf
                    } else {
                        Retirement::Moved
                    };
                    inner.retired(record.key, record.tier, how);
                }
            }
            PutOutcome::Refused(
                reason @ (PutRefusal::Invalid
                | PutRefusal::Unlinked
                | PutRefusal::Oversize
                | PutRefusal::Budget
                | PutRefusal::Inflight),
            ) => {
                let temporary = matches!(reason, PutRefusal::Budget | PutRefusal::Inflight);
                if temporary {
                    self.back_off();
                    self.refused_until = self.retry_after;
                    if !entry.leaf_only
                        && crate::store::retry_once(&mut entry.failures) == Retirement::Failed
                    {
                        entry.offered.pop();
                        self.pending.push_back(entry);
                        return true;
                    }
                }
                let index = match reason {
                    PutRefusal::Invalid => 0,
                    PutRefusal::Unlinked => 1,
                    PutRefusal::Oversize => 2,
                    PutRefusal::Budget => 3,
                    PutRefusal::Inflight => 4,
                    _ => unreachable!(),
                };
                if temporary || entry.path.is_empty() {
                    self.refusal_drops[index] += 1;
                }
                if entry.path.is_empty() {
                    self.dropped[usize::from(record.tier == Tier::Nvme)] += 1;
                    inner.retired(record.key, record.tier, Retirement::Dropped);
                } else {
                    entry.path.clear();
                    entry.offered.clear();
                    entry.leaf_only = true;
                    entry.depth = 1;
                    self.pending.push_front(entry);
                }
            }
            _ => {
                self.failed(inner, record);
                if record.tier == Tier::Dram {
                    self.refused_until = self.retry_after;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::PrefixKey;
    use crate::pool::{
        pins::{MemoryPin, MemorySpan, host_chain},
        tiers::{RecordSums, Releases},
    };

    fn key(tokens: u64, salt: u8) -> PrefixKey {
        PrefixKey {
            consumed_tokens: tokens,
            digest: [salt; 16],
        }
    }

    #[test]
    fn put_bytes_cross_the_records_of_the_chain_and_carry_the_persisted_checksums() {
        const PAGE: usize = 64;
        const ROW: usize = 32;
        // Pages of 16 tokens: the first record owns pages 0..3, the second 3..5.
        let first: Vec<u8> = (0..3 * PAGE + ROW).map(|i| i as u8).collect();
        let second: Vec<u8> = (0..2 * PAGE + ROW).map(|i| (i * 3 + 1) as u8).collect();
        let (a, b) = (first.as_ptr() as usize, second.as_ptr() as usize);
        let releases = Releases::default();
        let nodes = [(key(48, 1), 3), (key(80, 2), 2)];
        let pin = MemoryPin {
            chain: host_chain(16, 0, true, &nodes),
            spans: vec![
                MemorySpan {
                    pages: vec![a, a + PAGE, a + 2 * PAGE],
                    row: Some(vec![a + 3 * PAGE]),
                    sums: Arc::new(RecordSums {
                        pages: vec![10, 11, 12],
                        row: Some(13),
                    }),
                },
                MemorySpan {
                    pages: vec![b, b + PAGE],
                    row: Some(vec![b + 2 * PAGE]),
                    sums: Arc::new(RecordSums {
                        pages: vec![20, 21],
                        row: Some(22),
                    }),
                },
            ],
            layout: Arc::new(crate::Layout {
                page_tokens: 16,
                page_record_bytes: PAGE as u64,
                row_record_bytes: ROW as u64,
                ranks: vec![crate::geometry::RankLayout {
                    page_offset: 0,
                    plane_offsets: vec![0],
                    plane_bytes: vec![64],
                    row_bytes: 64,
                    row_offset: 0,
                }],
            }),
            keys: vec![key(48, 1), key(80, 2)],
            releases: releases.clone(),
        };
        let mut bytes = PinBytes {
            pin: Box::new(pin),
            page_bytes: PAGE,
            row_bytes: ROW,
        };

        // A window across the boundary between the records.
        let mut dst = vec![0u8; 3 * PAGE];
        let sums = bytes.fill(2, 3, false, &mut dst).unwrap();
        assert_eq!(dst[..PAGE], first[2 * PAGE..3 * PAGE]);
        assert_eq!(dst[PAGE..], second[..2 * PAGE]);
        assert_eq!((sums.pages, sums.row), (vec![12, 20, 21], None));

        // The last window carries the endpoint's row after its pages.
        let mut dst = vec![0u8; 2 * PAGE + ROW];
        let sums = bytes.fill(3, 2, true, &mut dst).unwrap();
        assert_eq!(dst, second);
        assert_eq!((sums.pages, sums.row), (vec![20, 21], Some(22)));

        // A row-only window after every page went in earlier ones.
        let mut dst = vec![0u8; ROW];
        let sums = bytes.fill(5, 0, true, &mut dst).unwrap();
        assert_eq!(dst, second[2 * PAGE..]);
        assert_eq!((sums.pages.len(), sums.row), (0, Some(22)));

        // A window the wrong size, or beyond the chain, is refused.
        assert!(bytes.fill(0, 1, false, &mut [0u8; PAGE + 1]).is_err());
        assert!(bytes.fill(4, 3, false, &mut [0u8; 3 * PAGE]).is_err());
        drop(bytes);
        assert_eq!(releases.drain(), [key(48, 1), key(80, 2)]);
    }
}
