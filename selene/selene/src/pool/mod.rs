//! The in-process pool client: the instance side of the pool protocol.
//!
//! Nothing in this module runs unless a pool address is configured. Without
//! one no pool type is constructed, so there is no thread, connection, mapping
//! or allocation.

mod lookup;
mod types;

pub use lookup::{Answer, LookupState, LookupTicket};
pub use types::{Candidate, ChainNode, HeldRange, Level, SourceRef, Tier};
