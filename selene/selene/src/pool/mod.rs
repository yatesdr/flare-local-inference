//! The in-process pool client: the instance side of the pool protocol.
//!
//! Nothing in this module runs unless a pool address is configured. Without
//! one no pool type is constructed, so there is no thread, connection, mapping
//! or allocation.

mod announce;
mod client;
mod codec;
mod conn;
mod engine;
mod exporter;
mod identity;
mod lookup;
mod shared;
mod shm;
#[allow(clippy::all, dead_code)]
pub(crate) mod wire {
    tonic::include_proto!("selene.v2");
}
mod types;

pub use announce::{Announcer, IndexEntry, Notice, Snapshot};
pub use client::{PoolAddress, PoolClient, PoolConfig};
pub use exporter::{Checksums, HostChain, HostExporter, HostPin, HostRecord, HostWindow};
pub use identity::{Geometry, RankPlanes, Scope, instance_id, process_nonce, scope_id};
pub use lookup::{Answer, LookupState, LookupTicket};
pub use shared::{ExportRequest, FetchFailure, FetchPart, PoolEvent, PoolFetch, PoolStatus};
pub use types::{HeldRange, PoolCandidate, SourceRef, Tier};
