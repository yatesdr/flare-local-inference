//! Limits and fixed transfer sizes shared by the manager and its clients.

use super::wire;
use std::time::Duration;

pub const MAX_OPEN_PUTS: u32 = 4;
pub const PROGRESS_TIMEOUT: Duration = Duration::from_secs(3);
pub const WINDOW_CREDITS: u32 = 2;
pub const MAX_CHAIN_NODES: u32 = 4096;
pub const MAX_WINDOW_BYTES: u64 = 4 << 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_concurrent_lookups: u32,
    pub max_candidates: u32,
    pub max_chain_nodes: u32,
    pub max_boundaries: u32,
    pub max_notices_per_batch: u32,
    pub max_resync_chunk_entries: u32,
    pub max_resync_entries: u32,
    pub max_claims_in_flight: u32,
    pub max_export_slots: u32,
    pub window_credits: u32,
    pub max_window_bytes: u64,
    pub max_claim_deadline_ms: u32,
    pub pin_deadline_ms: u32,
    pub progress_timeout_ms: u32,
    pub max_open_puts: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_concurrent_lookups: 8,
            max_candidates: 8,
            max_chain_nodes: MAX_CHAIN_NODES,
            max_boundaries: 8192,
            max_notices_per_batch: 256,
            max_resync_chunk_entries: 1024,
            max_resync_entries: 4_000_000,
            max_claims_in_flight: 2,
            max_export_slots: 2,
            window_credits: WINDOW_CREDITS,
            max_window_bytes: MAX_WINDOW_BYTES,
            max_claim_deadline_ms: 30_000,
            pin_deadline_ms: 250,
            progress_timeout_ms: PROGRESS_TIMEOUT.as_millis() as u32,
            max_open_puts: MAX_OPEN_PUTS,
        }
    }
}

impl Limits {
    pub fn pin_deadline(&self) -> Duration {
        Duration::from_millis(u64::from(self.pin_deadline_ms))
    }

    pub fn progress_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.progress_timeout_ms))
    }
}

#[cfg(test)]
pub(crate) fn wire_defaults() -> wire::Limits {
    let limits = Limits::default();
    wire::Limits {
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

pub(crate) fn validate(stated: Option<wire::Limits>) -> std::result::Result<(), String> {
    let limits = stated.ok_or("the pool manager did not state its limits")?;
    if limits.max_concurrent_lookups == 0
        || limits.max_candidates == 0
        || limits.max_chain_nodes == 0
        || limits.max_boundaries == 0
        || limits.max_notices_per_batch == 0
        || limits.max_resync_chunk_entries == 0
        || limits.max_resync_entries == 0
        || limits.max_claims_in_flight == 0
        || limits.max_export_slots == 0
        || limits.window_credits == 0
        || limits.max_window_bytes == 0
        || limits.max_claim_deadline_ms == 0
        || limits.pin_deadline_ms == 0
        || limits.progress_timeout_ms == 0
        || limits.max_open_puts == 0
    {
        return Err("the pool manager stated a zero limit".into());
    }
    Ok(())
}

pub(crate) fn staging_slots(limits: &wire::Limits, claims: u32, exports: u32) -> u32 {
    (claims + limits.max_open_puts + exports).max(1) * limits.window_credits
}
