//! Fixtures for tests of this crate and of the crates that implement its seams.

use blake2::{Blake2b, Digest, digest::consts::U16};

use crate::{
    CacheGeometry, HASH_BLOCK_TOKENS, History, Job, Layout, PageCopier, Plane, PrefixKey, Reply,
    Result,
};

pub use crate::slab::rank_part;

/// A token history with the key structure the index reads: a key at every
/// hash-block boundary and at any endpoint between them. Digests chain, so two
/// histories share a key exactly while their tokens agree.
#[derive(Clone, Debug)]
pub struct TestHistory {
    identity: [u8; 32],
    boundaries: Vec<PrefixKey>,
    tokens: Vec<u32>,
}

impl TestHistory {
    pub fn new(identity: [u8; 32]) -> Self {
        let mut hash = Blake2b::<U16>::new();
        hash.update(identity);
        Self {
            identity,
            boundaries: vec![PrefixKey {
                consumed_tokens: 0,
                digest: hash.finalize().into(),
            }],
            tokens: Vec::new(),
        }
    }

    pub fn append(&mut self, tokens: &[u32], _events: &[()]) -> Result<()> {
        for token in tokens {
            self.tokens.push(*token);
            if self.tokens.len().is_multiple_of(HASH_BLOCK_TOKENS) {
                let end = self.tokens.len();
                let key = self.digest(end, self.boundaries.last().unwrap().digest);
                self.boundaries.push(key);
            }
        }
        Ok(())
    }

    fn digest(&self, end: usize, parent: [u8; 16]) -> PrefixKey {
        let start = end - end % HASH_BLOCK_TOKENS;
        let start = if end.is_multiple_of(HASH_BLOCK_TOKENS) {
            end - HASH_BLOCK_TOKENS
        } else {
            start
        };
        let mut hash = Blake2b::<U16>::new();
        hash.update(self.identity);
        hash.update(parent);
        hash.update((end as u64).to_le_bytes());
        for token in &self.tokens[start..end] {
            hash.update(token.to_le_bytes());
        }
        PrefixKey {
            consumed_tokens: end as u64,
            digest: hash.finalize().into(),
        }
    }

    /// Digest of the aligned boundary at or below `consumed_tokens`.
    pub fn bucket_at(&self, consumed_tokens: u64) -> Option<[u8; 16]> {
        let aligned = consumed_tokens - consumed_tokens % HASH_BLOCK_TOKENS as u64;
        self.key_at(aligned).map(|key| key.digest)
    }
}

impl History for TestHistory {
    fn token_count(&self) -> u64 {
        self.tokens.len() as u64
    }

    fn key_at(&self, consumed_tokens: u64) -> Option<PrefixKey> {
        let end = usize::try_from(consumed_tokens).ok()?;
        if end > self.tokens.len() {
            return None;
        }
        let boundary = self.boundaries[end / HASH_BLOCK_TOKENS];
        if end.is_multiple_of(HASH_BLOCK_TOKENS) {
            return Some(boundary);
        }
        Some(self.digest(end, boundary.digest))
    }

    fn aligned_boundaries(&self) -> &[PrefixKey] {
        &self.boundaries[1..]
    }
}

/// A cache geometry of `planes` page planes of the given unit sizes and a state row, at
/// fixed addresses.
pub fn geometry(page_tokens: u64, planes: &[(&str, u64)], row_bytes: u64) -> CacheGeometry {
    let mut address = 0x7000_0000_0000u64;
    let planes = planes
        .iter()
        .map(|(name, unit_bytes)| {
            let plane = Plane {
                name: (*name).to_owned(),
                address,
                unit_bytes: *unit_bytes,
            };
            address += unit_bytes * 4096;
            plane
        })
        .collect();
    CacheGeometry {
        snapshot_pages: Vec::new(),
        page_tokens,
        page_capacity: 4096,
        row_capacity: 64,
        row: Plane {
            name: "state".into(),
            address,
            unit_bytes: row_bytes,
        },
        planes,
    }
}

/// A copier with no rank threads: every `submit` fails and `poll` returns
/// nothing, for tests of stores that never submit a transfer or expect every
/// submission to fail.
pub struct StoppedCopier {
    layout: Layout,
}

impl StoppedCopier {
    pub fn new(geometry: &[CacheGeometry]) -> Result<Self> {
        Ok(Self {
            layout: Layout::new(geometry)?,
        })
    }

    pub fn shared(geometry: &[CacheGeometry]) -> Result<crate::SharedCopier> {
        Ok(std::sync::Arc::new(std::sync::Mutex::new(Self::new(
            geometry,
        )?)))
    }
}

impl PageCopier for StoppedCopier {
    fn layout(&self) -> &Layout {
        &self.layout
    }

    fn submit(&mut self, _: Job) -> Result<()> {
        Err("no offload threads are running".into())
    }

    fn poll(&mut self, _: &mut Vec<Reply>) {}

    fn in_flight(&self) -> usize {
        0
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
