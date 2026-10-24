//! Timing the copy levels: the probe the characterizer drives.
//!
//! Levels the instance holds itself (its own DRAM and NVMe) are timed by the
//! stores through their own copier and disk paths. A level only the manager
//! serves is timed through the pool: a synthetic record is put into the
//! manager's store and claimed back through the real path (lookup, claim,
//! shared window, checksum, upload). The synthetic records carry keys derived
//! with a fixed probe label, so no real request can ever name them; there is
//! one per probed size and a repeat finds it already stored. A level that
//! cannot be timed (a neighbour's GPU, or a manager that does not serve it
//! here) fails, and the characterizer leaves it at its default position.

use std::time::{Duration, Instant};

use blake2::{Blake2b, Digest, digest::consts::U16};

use crate::{
    Error, History, HostStore, PrefixKey, Result, Scratch, Tier as Local,
    characterize::{Level, Probe},
    plan,
    record::checksum,
};

use super::{
    exporter::{Checksums, PutBytes},
    lookup::LookupState,
    shared::{FetchPart, PoolEvent, PoolFetch, PutOutcome, PutRequest, PutSource},
    store::PoolPort,
};

/// How long any one step of a manager probe may take.
const STEP: Duration = Duration::from_secs(3);

/// The key of the synthetic record of `pages` pages, which no real history
/// can produce.
pub(crate) fn probe_key(model: &[u8; 32], pages: usize, page_tokens: u64) -> PrefixKey {
    let mut hash = Blake2b::<U16>::new();
    hash.update(b"selene-probe-v2");
    hash.update(model);
    hash.update((pages as u64).to_le_bytes());
    PrefixKey {
        consumed_tokens: pages as u64 * page_tokens,
        digest: hash.finalize().into(),
    }
}

/// A history of two keys, the root and the probe record, for the lookup.
struct ProbeHistory {
    root: PrefixKey,
    key: PrefixKey,
}

impl History for ProbeHistory {
    /// One more than the record, so the record lies below `before`.
    fn token_count(&self) -> u64 {
        self.key.consumed_tokens + 1
    }

    fn key_at(&self, consumed_tokens: u64) -> Option<PrefixKey> {
        match consumed_tokens {
            0 => Some(self.root),
            n if n == self.key.consumed_tokens => Some(self.key),
            _ => None,
        }
    }

    fn aligned_boundaries(&self) -> &[PrefixKey] {
        std::slice::from_ref(&self.key)
    }
}

/// Known bytes for the synthetic record, hashed as they are produced.
struct Synthetic {
    page_bytes: usize,
    row_bytes: usize,
}

impl PutBytes for Synthetic {
    fn fill(
        &mut self,
        first_page: u64,
        page_count: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        let mut pages = Vec::with_capacity(page_count as usize);
        for index in 0..page_count as usize {
            let page = &mut dst[index * self.page_bytes..(index + 1) * self.page_bytes];
            for (offset, byte) in page.iter_mut().enumerate() {
                *byte = (offset as u64 ^ (first_page + index as u64)).wrapping_mul(31) as u8;
            }
            pages.push(checksum(page));
        }
        let row = row.then(|| {
            let start = page_count as usize * self.page_bytes;
            dst[start..start + self.row_bytes].fill(0x5a);
            checksum(&dst[start..start + self.row_bytes])
        });
        Ok(Checksums { pages, row })
    }
}

/// How a put ended, for the probe's purposes.
enum PutEnd {
    Done,
    Not(String),
}

/// Drives the characterizer over a store and its pool.
pub(crate) struct StoreProbe<'a, S: HostStore, P: PoolPort> {
    pub inner: &'a mut S,
    pub pool: &'a mut P,
    pub scratch: &'a Scratch,
    pub model: [u8; 32],
    pub page_tokens: u64,
    pub page_bytes: usize,
    pub row_bytes: usize,
    /// Events the probe saw that belong to the worker, kept for its next poll.
    pub stash: &'a mut Vec<PoolEvent>,
}

impl<S: HostStore, P: PoolPort> StoreProbe<'_, S, P> {
    /// Poll the pool until `wanted` accepts an event; others are kept.
    fn wait<T>(
        &mut self,
        what: &str,
        mut wanted: impl FnMut(&PoolEvent) -> Option<T>,
    ) -> Result<T> {
        let deadline = Instant::now() + STEP;
        loop {
            let mut events = Vec::new();
            self.pool.poll(&mut events)?;
            let mut found = None;
            for event in events {
                match found.is_none().then(|| wanted(&event)).flatten() {
                    Some(value) => found = Some(value),
                    None => self.stash.push(event),
                }
            }
            if let Some(value) = found {
                return Ok(value);
            }
            if Instant::now() >= deadline {
                return Err(Error::new(format!(
                    "the pool did not answer ({what}) in time"
                )));
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    /// Time a claim of the synthetic record from the manager's store, which
    /// must serve it at `level`.
    fn through_manager(&mut self, level: Level, pages: usize) -> Result<Duration> {
        if pages == 0 || pages > self.scratch.pages.len() {
            return Err(Error::new(
                "the probe needs between one page and the scratch pages",
            ));
        }
        let key = probe_key(&self.model, pages, self.page_tokens);
        // Seed the record (a repeat finds it stored already).
        let put = self.pool.put(PutRequest {
            key,
            parent: None,
            bucket: key.digest,
            chain_pages: pages as u64,
            tail_pages: pages as u64,
            row: true,
            generation: 1,
            source: PutSource::Host(Box::new(Synthetic {
                page_bytes: self.page_bytes,
                row_bytes: self.row_bytes,
            })),
        })?;
        let end = self.wait("put", |event| match event {
            PoolEvent::Put { id, outcome } if *id == put => Some(match outcome {
                PutOutcome::Stored { .. } | PutOutcome::Present => PutEnd::Done,
                other => PutEnd::Not(format!("{other:?}")),
            }),
            _ => None,
        })?;
        if let PutEnd::Not(why) = end {
            return Err(Error::new(format!(
                "the manager would not store the probe record: {why}"
            )));
        }
        // Time lookup, claim, copy, checksum and upload.
        let started = Instant::now();
        let history = ProbeHistory {
            root: PrefixKey {
                consumed_tokens: 0,
                digest: [0; 16],
            },
            key,
        };
        let ticket = self.pool.begin_lookup(&history, Vec::new());
        let deadline = Instant::now() + STEP;
        let answer = loop {
            match ticket.state() {
                LookupState::Ready(answer) => break answer,
                LookupState::None => {
                    return Err(Error::new("the manager gave no answer to the probe lookup"));
                }
                LookupState::Pending if Instant::now() >= deadline => {
                    return Err(Error::new("the probe lookup timed out"));
                }
                LookupState::Pending => std::thread::sleep(Duration::from_micros(100)),
            }
        };
        let candidate = answer
            .candidates
            .iter()
            .find(|c| c.plan.key == key && c.source.instance.is_empty())
            .ok_or_else(|| Error::new("the manager does not offer the probe record"))?;
        if candidate.plan.level != level {
            return Err(Error::new(format!(
                "the manager serves the probe record at {:?}, not {level:?}",
                candidate.plan.level
            )));
        }
        let part = plan::Part {
            level,
            source: candidate.plan.source,
            generation: candidate.plan.generation,
            begin_tokens: 0,
            end: key,
            first_page: 0,
            dest_page: 0,
            pages,
            row: true,
            chain: vec![key],
        };
        let id = self.pool.fetch(PoolFetch {
            id: 0,
            page_tokens: self.page_tokens,
            parts: vec![FetchPart {
                candidate: candidate.clone(),
                part,
                pages: self.scratch.pages[..pages].to_vec(),
                slot: Some(self.scratch.slot),
            }],
        })?;
        self.wait("fetch", |event| match event {
            PoolEvent::Fetched { id: done, result } if *done == id => {
                Some(result.as_ref().map(|_| ()).map_err(|f| f.error.to_string()))
            }
            _ => None,
        })?
        .map_err(Error::new)?;
        Ok(started.elapsed())
    }
}

impl<S: HostStore, P: PoolPort> Probe for StoreProbe<'_, S, P> {
    fn transfer(&mut self, level: Level, pages: usize, cold: bool) -> Result<Duration> {
        let local = match level {
            Level::ThisGpu => return Err(Error::new("this GPU is not a copy")),
            Level::NeighbourGpu => {
                return Err(Error::new("there is no neighbour GPU to measure"));
            }
            Level::HostDram => Some(Local::Memory),
            Level::HostNvme => Some(Local::Disk),
            Level::RemoteNvme => None,
        };
        if let Some(tier) = local
            && let Some(elapsed) = self.inner.probe(tier, self.scratch, pages, cold)?
        {
            return Ok(elapsed);
        }
        self.through_manager(level, pages)
    }
}
