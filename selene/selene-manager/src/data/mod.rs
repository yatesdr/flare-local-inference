//! Manager-to-manager data channel.
//!
//! Control decides what to move; a `DataChannel` only moves bytes between two managers' region
//! files. Implementations split a transfer into 1 MiB pieces and echo the grant identity
//! `(transfer, index)` and `(piece, pieces)` in every frame.

use std::io;

#[cfg(feature = "rdma")]
#[path = "ibv.rs"]
pub mod ibv;

#[path = "channel.rs"]
pub mod channel;

#[path = "rdma.rs"]
pub mod rdma;

#[path = "stream.rs"]
pub mod stream;

/// Maximum payload bytes in one piece.
pub const PIECE_BYTES: usize = 1 << 20;

/// The two shared region files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Region {
    Arena,
    Staging,
}

impl Region {
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Arena => 1,
            Self::Staging => 2,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Arena),
            2 => Some(Self::Staging),
            _ => None,
        }
    }
}

/// One granted window within one transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct GrantKey {
    pub transfer: u64,
    pub index: u32,
}

/// Access to a manager's region files. Implementations expose slices so the data channel can
/// send from and receive into the mapping without an intermediate copy. The callback runs while
/// the implementation's mapping is borrowed; it must not re-enter the same region.
pub trait RegionAccess: Send + Sync {
    fn size(&self, region: Region) -> u64;
    /// The mapped base and length, for RDMA registration. `None` when the caller cannot
    /// register this region (the RDMA setup then falls back to stream).
    #[allow(dead_code)] // used by the feature-gated RDMA setup
    fn mapping(&self, _region: Region) -> Option<(*mut u8, usize)> {
        None
    }
    fn with_read(
        &self,
        region: Region,
        offset: u64,
        length: usize,
        f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()>;
    fn with_write(
        &self,
        region: Region,
        offset: u64,
        length: usize,
        f: &mut dyn FnMut(&mut [u8]) -> io::Result<()>,
    ) -> io::Result<()>;
}

/// The live windows a peer may write to or read from.
pub trait GrantTable: Send + Sync {
    /// `(region, offset, length)` of the live grant for one session and direction, or `None`
    /// when it is unknown, belongs to another session or has the wrong direction.
    fn window(&self, session: u64, key: GrantKey, put: bool) -> Option<(Region, u64, u64)>;
}

/// A manager-to-manager byte channel. Implementations are used from one peer thread only.
pub trait DataChannel: Send {
    /// Copy `len` bytes at local (`region`, `offset`) to the peer at
    /// (`remote_region`, `remote_offset`). Completion arrives through `poll`.
    #[allow(clippy::too_many_arguments)]
    fn write(
        &mut self,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()>;

    /// Ask the peer to copy `len` bytes from its (`remote_region`, `remote_offset`) into local
    /// (`region`, `offset`). Completion arrives through `poll`.
    #[allow(clippy::too_many_arguments)]
    fn read(
        &mut self,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()>;

    /// Completed or failed `(transfer, index)` windows since the last call.
    fn poll(&mut self, done: &mut Vec<(u64, u32, io::Result<()>)>) -> io::Result<()>;

    /// Stop the channel and complete every outstanding window with an error.
    fn close(&mut self) -> io::Result<()>;

    /// Whether this channel moves bytes over RDMA. A data-path failure on such a channel makes
    /// the peer stream-only until the manager restarts.
    fn is_rdma(&self) -> bool {
        false
    }
}

/// Pieces needed for `len` bytes: `ceil(len / 1 MiB)`.
pub fn pieces(len: u64) -> u32 {
    let pieces = len.div_ceil(PIECE_BYTES as u64);
    pieces.min(u32::MAX as u64) as u32
}

/// Bytes in piece `piece` of a `len`-byte transfer.
pub fn piece_len(len: u64, piece: u32) -> usize {
    let start = u64::from(piece) * PIECE_BYTES as u64;
    let remaining = len.saturating_sub(start);
    remaining.min(PIECE_BYTES as u64) as usize
}

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

/// Validate a 4096-aligned range against a backing size without overflow.
pub fn checked_range(offset: u64, length: usize, size: u64) -> Option<u64> {
    if !offset.is_multiple_of(4096) || !(length as u64).is_multiple_of(4096) {
        return None;
    }
    let end = offset.checked_add(length as u64)?;
    (end <= size).then_some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pieces_are_one_mib_and_a_short_tail() {
        assert_eq!(pieces(0), 0);
        assert_eq!(pieces(1), 1);
        assert_eq!(pieces(PIECE_BYTES as u64), 1);
        assert_eq!(pieces(PIECE_BYTES as u64 + 1), 2);
        assert_eq!(pieces(2 * PIECE_BYTES as u64 + 4096), 3);
        assert_eq!(piece_len(2 * PIECE_BYTES as u64 + 4096, 0), PIECE_BYTES);
        assert_eq!(piece_len(2 * PIECE_BYTES as u64 + 4096, 1), PIECE_BYTES);
        assert_eq!(piece_len(2 * PIECE_BYTES as u64 + 4096, 2), 4096);
        assert_eq!(piece_len(4096, 0), 4096);
    }

    #[test]
    fn region_round_trip() {
        assert_eq!(Region::from_u8(Region::Arena.as_u8()), Some(Region::Arena));
        assert_eq!(
            Region::from_u8(Region::Staging.as_u8()),
            Some(Region::Staging)
        );
        assert_eq!(Region::from_u8(0), None);
        assert_eq!(Region::from_u8(3), None);
    }

    #[test]
    fn checked_ranges_reject_misalignment_overflow_and_overrun() {
        assert_eq!(checked_range(0, 4096, 8192), Some(4096));
        assert_eq!(checked_range(4096, 4096, 8192), Some(8192));
        assert_eq!(checked_range(4096, 4096, 4095), None);
        assert_eq!(checked_range(1, 4096, 8192), None);
        assert_eq!(checked_range(0, 1, 8192), None);
        assert_eq!(checked_range(u64::MAX, 4096, u64::MAX), None);
    }
}
