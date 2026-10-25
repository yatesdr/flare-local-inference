//! The pool's vocabulary in selene's own types. The wire messages stay inside
//! the codec; nothing here depends on them.

use crate::{PrefixKey, levels::Level, plan};

/// Where a copy of a record lives, from the source's point of view. Distinct
/// from [`crate::Tier`], which names the local store a hit is read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Tier {
    Gpu,
    Dram,
    Nvme,
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

/// A candidate the manager offered, in the planner's terms plus what a claim
/// on it needs. The planner's `source` names `(source, tier)` through the
/// client's source table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolCandidate {
    pub plan: plan::Candidate,
    pub source: SourceRef,
    pub tier: Tier,
}
