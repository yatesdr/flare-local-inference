//! The in-process pool client: the instance side of the pool protocol.
//!
//! Nothing in this module runs unless a pool address is configured. Without
//! one no pool type is constructed, so there is no thread, connection, mapping
//! or allocation.

mod agent;
mod announce;
mod cascade;
mod budget;
mod builder;
mod client;
mod codec;
mod conn;
mod engine;
mod exporter;
pub mod frame;
#[cfg(test)]
mod host_tests;
mod identity;
mod local;
mod limits;
pub use limits::{Limits, WINDOW_CREDITS, MAX_WINDOW_BYTES, MAX_CHAIN_NODES, MAX_OPEN_PUTS, PROGRESS_TIMEOUT};
mod lookup;
#[cfg(test)]
mod manager_tests;
pub(crate) mod net;
pub(crate) mod pins;
mod peers;
mod options;
mod records;
pub mod rank;
#[cfg(test)]
mod rank_tests;
pub mod rdma;
mod shared;
mod shm;
pub mod slice;
mod startup;
mod store;
pub(crate) mod tiers;
#[allow(clippy::all, dead_code)]
pub mod wire {
    tonic::include_proto!("selene.v2");
}
mod types;

pub use budget::Budget;
pub use builder::{PoolBuilder, PreparedPool};
pub use options::{PoolOptions, DEFAULT_POOL_TENANT, parse_pool_address, parse_pool_tenant};
use options::{pool_address, pool_scope};
pub use records::DeferredRecords;
pub use agent::{AgentConfig, RankAgent, RankRecords};
pub use announce::{Announcer, IndexEntry, Notice, Snapshot};
pub use client::{PoolAddress, PoolClient, PoolConfig, STARTUP_WAIT};
pub use exporter::{Checksums, HostChain, HostPin, HostRecord, HostWindow, PutBytes};
pub use identity::{Geometry, RankPlanes, Scope, instance_id, process_nonce, scope_id};
pub use lookup::{Answer, LookupState, LookupTicket};
pub use peers::{PoolOffer, PoolPeers, pool_rank_zero, pool_peer};
pub use pins::{FilePin, MemoryPin};
pub use rank::{FrameReceiver, FrameSender, RankLink, RankSet};
pub use shared::{
    ExportRequest, FetchFailure, FetchPart, PoolEvent, PoolFetch, PoolStatus, PutOutcome,
    PutRefusal, PutRequest, PutSource,
};
pub use slice::{RankSlice, SliceLayout, SliceSums};
pub use startup::{Approve, PlannedRegion, RegisteredRegion};
pub use store::{PoolPort, PooledStore};
pub use tiers::{Releases, TierView};
pub use types::{PoolCandidate, SourceRef, Tier};
