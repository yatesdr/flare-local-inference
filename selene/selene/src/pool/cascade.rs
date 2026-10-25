//! The pool-mode eviction cascade below this instance's own tiers.
//!
//! The last local tier (DRAM, or the disk when there is no DRAM tier) keeps a
//! record it would drop until the manager's store holds a committed copy. The
//! pooled store asks that tier for the records it wants to move, writes each
//! with a put, and tells the tier how the put ended. The manager's answer is
//! the only evidence that the copy exists: `Stored` and `Present` let the
//! tier drop its own, a refusal for want of room moves the record on to the
//! next tier, which is none, so it is dropped, and a failed write is tried
//! once more the next time the tier needs room.

use std::collections::{HashMap, VecDeque};

use crate::{Error, HostStore, Layout, PrefixKey, Result, Retirement, Retiring};

use super::{
    exporter::{Checksums, HostPin, HostWindow, PutBytes},
    shared::{PutOutcome, PutRefusal, PutRequest, PutSource},
    store::PoolPort,
    types::Tier,
};

/// Records waiting for a put; beyond this the coldest is dropped.
pub(crate) const PENDING_MAX: usize = 64;
/// Puts started at once.
pub(crate) const MOVES_MAX: usize = 4;
/// Records remembered after a failed write.
const FAILURES_MAX: usize = 1024;

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

pub(crate) struct Cascade {
    /// Records chosen by the local tier, coldest first.
    pending: VecDeque<Retiring>,
    /// Records whose put is running, by put id.
    moving: HashMap<u64, Retiring>,
    /// Failed writes so far, by record.
    failures: HashMap<(Tier, PrefixKey), u8>,
    page_bytes: usize,
    row_bytes: usize,
}

impl Cascade {
    pub(crate) fn new(layout: &Layout) -> Self {
        Self {
            pending: VecDeque::new(),
            moving: HashMap::new(),
            failures: HashMap::new(),
            page_bytes: layout.page_record_bytes as usize,
            row_bytes: layout.row_record_bytes as usize,
        }
    }

    /// Records waiting for a put or being written.
    #[cfg(test)]
    pub(crate) fn in_progress(&self) -> usize {
        self.pending.len() + self.moving.len()
    }

    /// Take what the local tier wants to move and start puts, without waiting
    /// for any. With the manager not ready every record is dropped at once.
    pub(crate) fn pump<S: HostStore, P: PoolPort>(&mut self, inner: &mut S, pool: &mut P) {
        let ready = pool.ready();
        inner.set_spill(ready);
        let chosen = inner.take_retiring();
        if !ready {
            for record in self.pending.drain(..).chain(chosen) {
                inner.retired(record.key, record.tier, Retirement::Dropped);
            }
            return;
        }
        self.pending.extend(chosen);
        while self.moving.len() < MOVES_MAX {
            let Some(record) = self.pending.pop_front() else {
                break;
            };
            self.begin(inner, pool, record);
        }
        // Records still waiting are bounded: the coldest are dropped.
        while self.pending.len() > PENDING_MAX {
            if let Some(coldest) = self.pending.pop_front() {
                inner.retired(coldest.key, coldest.tier, Retirement::Dropped);
            }
        }
    }

    fn begin<S: HostStore, P: PoolPort>(&mut self, inner: &mut S, pool: &mut P, record: Retiring) {
        if !record.row {
            // Without its row the record can end no restore.
            return inner.retired(record.key, record.tier, Retirement::Dropped);
        }
        let pin = match inner.pin_export(record.tier, record.key, 0, true) {
            Ok(pin) => pin,
            Err(_) => return self.failed(inner, record),
        };
        let request = PutRequest {
            key: record.key,
            parent: record.parent,
            bucket: record.bucket,
            chain_pages: pin.chain().total_pages,
            tail_pages: record.tail_pages as u64,
            row: true,
            source: PutSource::Host(Box::new(PinBytes {
                pin,
                page_bytes: self.page_bytes,
                row_bytes: self.row_bytes,
            })),
        };
        match pool.put(request) {
            Ok(id) => {
                self.moving.insert(id, record);
            }
            // The manager cannot take it now: there is no tier beyond it.
            Err(_) => inner.retired(record.key, record.tier, Retirement::Dropped),
        }
    }

    fn failed<S: HostStore>(&mut self, inner: &mut S, record: Retiring) {
        if self.failures.len() >= FAILURES_MAX {
            self.failures.clear();
        }
        let count = self.failures.entry((record.tier, record.key)).or_insert(0);
        *count += 1;
        let how = if *count >= 2 {
            self.failures.remove(&(record.tier, record.key));
            Retirement::Dropped
        } else {
            Retirement::Failed
        };
        inner.retired(record.key, record.tier, how);
    }

    /// A put ended. False when it was not one of the cascade's.
    pub(crate) fn ended<S: HostStore>(
        &mut self,
        inner: &mut S,
        id: u64,
        outcome: &PutOutcome,
    ) -> bool {
        let Some(record) = self.moving.remove(&id) else {
            return false;
        };
        match outcome {
            PutOutcome::Stored { .. } | PutOutcome::Present => {
                self.failures.remove(&(record.tier, record.key));
                inner.retired(record.key, record.tier, Retirement::Moved);
            }
            PutOutcome::Refused(
                PutRefusal::Budget
                | PutRefusal::Queue
                | PutRefusal::NotReady
                | PutRefusal::Quota
                | PutRefusal::Oversize,
            ) => {
                self.failures.remove(&(record.tier, record.key));
                inner.retired(record.key, record.tier, Retirement::Dropped);
            }
            PutOutcome::Refused(PutRefusal::Inflight | PutRefusal::Invalid | PutRefusal::Io)
            | PutOutcome::Cancelled
            | PutOutcome::Failed(_) => self.failed(inner, record),
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
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
                    row: Some(a + 3 * PAGE),
                    sums: Arc::new(RecordSums {
                        pages: vec![10, 11, 12],
                        row: Some(13),
                    }),
                },
                MemorySpan {
                    pages: vec![b, b + PAGE],
                    row: Some(b + 2 * PAGE),
                    sums: Arc::new(RecordSums {
                        pages: vec![20, 21],
                        row: Some(22),
                    }),
                },
            ],
            page_bytes: PAGE,
            row_bytes: ROW,
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
