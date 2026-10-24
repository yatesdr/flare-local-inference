//! The pool's vocabulary in selene's own types. The wire messages stay inside
//! the codec; nothing here depends on them.

use crate::PrefixKey;

/// Where a copy of a record lives, from the source's point of view. Distinct
/// from [`crate::Tier`], which names the local store a hit is read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Tier {
    Gpu,
    Dram,
    Nvme,
}

/// The copy-priority level of a candidate from the requester's perspective.
/// The numbering is the default order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Level {
    /// Never a candidate; appears only in [`HeldRange`].
    ThisGpu = 1,
    NeighbourGpu = 2,
    HostDram = 3,
    HostNvme = 4,
    RemoteNvme = 5,
}

/// The deepest committed record the requester already holds at `level` for a
/// history. Coverage is the whole prefix from token 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HeldRange {
    pub level: Level,
    pub end: PrefixKey,
}

/// An instance, or the manager's own store when `instance` is empty.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceRef {
    pub instance: Vec<u8>,
    pub session_incarnation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainNode {
    pub key: PrefixKey,
    pub tail_pages: u32,
    pub generation: u64,
    pub row: bool,
}

/// One place a request could be restored from, as the manager offered it. A
/// hint: the requester re-checks every node against its own history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub level: Level,
    pub tier: Tier,
    pub source: SourceRef,
    /// The endpoint and its generation.
    pub key: PrefixKey,
    pub generation: u64,
    /// Exact first token this candidate supplies; the physical start is its
    /// floor to a page.
    pub begin_tokens: u64,
    pub pages: u64,
    pub bytes: u64,
    pub row: bool,
    /// From the record containing `begin_tokens` up to the endpoint.
    pub nodes: Vec<ChainNode>,
    /// Pages of `nodes[0]` that precede the physical start.
    pub skip_pages: u32,
    pub est_us_per_page: u32,
}
