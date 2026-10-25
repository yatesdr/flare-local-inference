//! Manager pool settings.

use crate::proto::v2;
pub use flare_selene::pool::Limits;

pub fn to_proto(limits: &Limits) -> v2::Limits {
    v2::Limits {
        max_concurrent_lookups: limits.max_concurrent_lookups,
        max_candidates: limits.max_candidates,
        max_chain_nodes: limits.max_chain_nodes,
        max_boundaries: limits.max_boundaries,
        max_notices_per_batch: limits.max_notices_per_batch,
        max_resync_chunk_entries: limits.max_resync_chunk_entries,
        max_resync_entries: limits.max_resync_entries,
        max_claims_in_flight: limits.max_claims_in_flight,
        max_export_slots: limits.max_export_slots,
        window_credits: limits.window_credits,
        max_window_bytes: limits.max_window_bytes,
        max_claim_deadline_ms: limits.max_claim_deadline_ms,
        pin_deadline_ms: limits.pin_deadline_ms,
        progress_timeout_ms: limits.progress_timeout_ms,
        max_open_puts: limits.max_open_puts,
    }
}

/// The pool settings a manager is started with.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PoolSettings {
    pub limits: Limits,
}
