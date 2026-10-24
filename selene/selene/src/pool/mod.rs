//! The in-process pool client: the instance side of the pool protocol.
//!
//! Nothing in this module runs unless a pool address is configured. Without
//! one no pool type is constructed, so there is no thread, connection, mapping
//! or allocation.

mod announce;
mod cascade;
mod client;
mod codec;
mod conn;
mod engine;
mod exporter;
#[cfg(test)]
mod host_tests;
mod identity;
mod lookup;
#[cfg(test)]
mod manager_tests;
pub(crate) mod pins;
mod probe;
mod shared;
mod shm;
#[cfg(test)]
mod smoke_tests;
mod store;
pub(crate) mod tiers;
#[allow(clippy::all, dead_code)]
pub(crate) mod wire {
    tonic::include_proto!("selene.v2");
}
mod types;

pub use announce::{Announcer, IndexEntry, Notice, Snapshot};
pub use client::{PoolAddress, PoolClient, PoolConfig};
pub use exporter::{Checksums, HostChain, HostPin, HostRecord, HostWindow, PutBytes};
pub use identity::{Geometry, RankPlanes, Scope, instance_id, process_nonce, scope_id};
pub use lookup::{Answer, LookupState, LookupTicket};
pub use pins::{FilePin, MemoryPin};
pub use shared::{
    ExportRequest, FetchFailure, FetchPart, PoolEvent, PoolFetch, PoolStatus, PutOutcome,
    PutRefusal, PutRequest, PutSource,
};
pub use store::{PoolPort, PooledStore};
pub use tiers::{Releases, TierView};
pub use types::{HeldRange, PoolCandidate, SourceRef, Tier};
