//! The manager's pool limits. Defaults come from the protocol; every limit is
//! enforced by the manager and carried to clients in `Hello`.

use crate::proto::v2;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_concurrent_lookups: u32,
    pub max_candidates: u32,
    pub max_chain_nodes: u32,
    pub max_boundaries: u32,
    pub max_notices_per_batch: u32,
    pub max_unacked_notices: u32,
    pub max_resync_chunk_entries: u32,
    pub max_resync_entries: u32,
    pub max_resync_buffered_notices: u32,
    pub max_claims_in_flight: u32,
    pub max_export_slots: u32,
    pub window_credits: u32,
    pub max_window_bytes: u64,
    pub max_claim_deadline_ms: u32,
    pub pin_deadline_ms: u32,
    pub progress_timeout_ms: u32,
    pub max_open_puts: u32,
    pub resync_timeout_ms: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_concurrent_lookups: 8,
            max_candidates: 8,
            max_chain_nodes: 4096,
            max_boundaries: 8192,
            max_notices_per_batch: 256,
            max_unacked_notices: 16_384,
            max_resync_chunk_entries: 1024,
            max_resync_entries: 4_000_000,
            max_resync_buffered_notices: 65_536,
            max_claims_in_flight: 2,
            max_export_slots: 2,
            window_credits: 2,
            max_window_bytes: 4 << 20,
            max_claim_deadline_ms: 30_000,
            pin_deadline_ms: 250,
            progress_timeout_ms: 3000,
            max_open_puts: 4,
            resync_timeout_ms: 5000,
        }
    }
}

impl Limits {
    pub fn to_proto(&self) -> v2::Limits {
        v2::Limits {
            max_concurrent_lookups: self.max_concurrent_lookups,
            max_candidates: self.max_candidates,
            max_chain_nodes: self.max_chain_nodes,
            max_boundaries: self.max_boundaries,
            max_notices_per_batch: self.max_notices_per_batch,
            max_unacked_notices: self.max_unacked_notices,
            max_resync_chunk_entries: self.max_resync_chunk_entries,
            max_resync_entries: self.max_resync_entries,
            max_resync_buffered_notices: self.max_resync_buffered_notices,
            max_claims_in_flight: self.max_claims_in_flight,
            max_export_slots: self.max_export_slots,
            window_credits: self.window_credits,
            max_window_bytes: self.max_window_bytes,
            max_claim_deadline_ms: self.max_claim_deadline_ms,
            pin_deadline_ms: self.pin_deadline_ms,
            progress_timeout_ms: self.progress_timeout_ms,
            max_open_puts: self.max_open_puts,
            resync_timeout_ms: self.resync_timeout_ms,
        }
    }

    pub fn resync_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.resync_timeout_ms))
    }

    pub fn pin_deadline(&self) -> Duration {
        Duration::from_millis(u64::from(self.pin_deadline_ms))
    }

    pub fn progress_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.progress_timeout_ms))
    }
}

/// The pool settings a manager is started with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolSettings {
    pub limits: Limits,
    /// How long a terminal claim's reply is kept to answer duplicates.
    pub reply_retention: Duration,
    /// The expected-source barrier's bounded wait, from process start.
    pub source_wait: Duration,
    /// Instances not seen for this long are forgotten.
    pub instance_retention: Duration,
}

impl Default for PoolSettings {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            reply_retention: Duration::from_secs(60),
            source_wait: Duration::from_secs(10),
            instance_retention: Duration::from_secs(7 * 86_400),
        }
    }
}
