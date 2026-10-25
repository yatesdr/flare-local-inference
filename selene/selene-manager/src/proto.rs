#![allow(clippy::large_enum_variant)]

//! The store's internal vocabulary lives at the top of this module; the pool protocol, the
//! only protocol the manager speaks, is [`v2`].

tonic::include_proto!("selene.store");

/// The pool protocol, `selene.v2`.
pub use flare_selene::pool::wire as v2;

pub use v2::{Reason, Region};

#[derive(Clone, Copy)]
pub enum Tier {
    Dram = 1,
    Nvme = 2,
}

#[derive(Clone, Debug)]
pub enum CatalogEvent {
    SnapshotBegin,
    Entry(crate::store::EntryData),
    SnapshotEnd,
    Added(crate::store::EntryData),
    Changed(crate::store::EntryData),
    Removed(crate::Key),
}
