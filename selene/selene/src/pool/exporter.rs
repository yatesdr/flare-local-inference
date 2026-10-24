//! The seam through which the host tiers serve exports to other instances.

use crate::{ExportRefusal, PrefixKey, Result};

use super::types::Tier;

/// One record of a pinned host chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostRecord {
    pub key: PrefixKey,
    pub generation: u64,
    /// Record-relative page where this export's pages start.
    pub first_page: u32,
    pub pages: u32,
    /// The record carries the endpoint row this export includes.
    pub row: bool,
}

/// What a host pin holds, in restore order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostChain {
    pub records: Vec<HostRecord>,
    pub total_pages: u64,
    pub row: bool,
}

/// A span of one record to copy into a window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostWindow {
    pub record: u32,
    pub first_page: u32,
    pub page_count: u32,
    pub row: bool,
}

/// The checksums persisted with the records read, never hashes of the bytes
/// just read: a corrupted record must reach the requester as a mismatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checksums {
    pub pages: Vec<u64>,
    pub row: Option<u64>,
}

/// A host record chain held against eviction. Dropping it releases the hold.
pub trait HostPin: Send {
    fn chain(&self) -> &HostChain;
    /// Copy the span into `dst` (page records, then the row) and return its
    /// persisted checksums. May block on a device: it runs on a copy thread.
    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums>;
}

/// The host tiers of this instance, as a source of exports.
pub trait HostExporter: Send + Sync {
    /// Pin the chain ending at `key` from the page holding `begin_tokens`. The
    /// refusal names why nothing was pinned.
    fn pin(
        &self,
        tier: Tier,
        key: PrefixKey,
        generation: u64,
        begin_tokens: u64,
        want_row: bool,
    ) -> std::result::Result<Box<dyn HostPin>, ExportRefusal>;
}
