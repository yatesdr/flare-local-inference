//! The in-process pool client: the instance side of the pool protocol.
//!
//! Nothing in this module runs unless a pool address is configured. Without
//! one no pool type is constructed, so there is no thread, connection, mapping
//! or allocation.

mod identity;
mod lookup;
#[allow(clippy::all, dead_code)]
pub(crate) mod wire {
    tonic::include_proto!("selene.v2");
}
mod types;

pub use identity::{Geometry, RankPlanes, Scope, instance_id, process_nonce, scope_id};
pub use lookup::{Answer, LookupState, LookupTicket};
pub use types::{Candidate, ChainNode, HeldRange, Level, SourceRef, Tier};
