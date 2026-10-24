//! `RegionAccess` over the manager's shared arena and staging mappings.
//!
//! The peer thread and the data channels outlive any borrow of `Local`, so they receive clones
//! of the two `Arena`s. Each clone holds the mapping alive, and the mapping's base address and
//! length never change, which is what `RegionAccess` promises.

use crate::arena::Arena;
use crate::data::{Region, RegionAccess};
use std::io;

/// The store's two regions, safe to share with a peer thread.
#[derive(Clone)]
pub struct LocalRegions {
    arena: Arena,
    staging: Arena,
}

impl LocalRegions {
    pub fn new(arena: Arena, staging: Arena) -> Self {
        Self { arena, staging }
    }

    #[allow(dead_code)] // used by the peer runtime, not by this module's tests
    pub fn arena(&self) -> &Arena {
        &self.arena
    }

    #[allow(dead_code)] // used by the peer runtime, not by this module's tests
    pub fn staging(&self) -> &Arena {
        &self.staging
    }

    fn region(&self, region: Region) -> &Arena {
        match region {
            Region::Arena => &self.arena,
            Region::Staging => &self.staging,
        }
    }

    /// Checks an arbitrary (not necessarily page-aligned) range.
    fn start(&self, region: Region, offset: u64, length: usize) -> io::Result<usize> {
        let start = usize::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset does not fit"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range overflow"))?;
        if end > self.region(region).capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "range outside the region",
            ));
        }
        Ok(start)
    }
}

impl RegionAccess for LocalRegions {
    fn size(&self, region: Region) -> u64 {
        self.region(region).capacity() as u64
    }

    fn mapping(&self, region: Region) -> Option<(*mut u8, usize)> {
        Some(self.region(region).mapping())
    }

    fn with_read(
        &self,
        region: Region,
        offset: u64,
        length: usize,
        f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let start = self.start(region, offset, length)?;
        let (pointer, _) = self.region(region).mapping();
        // SAFETY: the range was checked against the mapping this value owns, and the mapping
        // stays alive and at a stable address for as long as the `Arena` clone exists.
        let bytes = unsafe { std::slice::from_raw_parts(pointer.add(start), length) };
        f(bytes)
    }

    fn with_write(
        &self,
        region: Region,
        offset: u64,
        length: usize,
        f: &mut dyn FnMut(&mut [u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let start = self.start(region, offset, length)?;
        let (pointer, _) = self.region(region).mapping();
        // SAFETY: as above. The manager serialises window writes through the grant table, so a
        // window range is not concurrently borrowed by another writer.
        let bytes = unsafe { std::slice::from_raw_parts_mut(pointer.add(start), length) };
        f(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn regions() -> LocalRegions {
        LocalRegions::new(
            Arena::new(64 * 1024).unwrap(),
            Arena::new(16 * 1024).unwrap(),
        )
    }

    #[test]
    fn reads_and_writes_hit_the_mapped_bytes() {
        let regions = regions();
        regions
            .with_write(Region::Arena, 4096, 3, &mut |bytes| {
                bytes.copy_from_slice(b"abc");
                Ok(())
            })
            .unwrap();
        regions
            .with_read(Region::Arena, 4096, 3, &mut |bytes| {
                assert_eq!(bytes, b"abc");
                Ok(())
            })
            .unwrap();
        assert_eq!(regions.size(Region::Arena), 64 * 1024);
        assert_eq!(regions.size(Region::Staging), 16 * 1024);
    }

    #[test]
    fn the_mapping_is_stable_across_accesses() {
        let regions = regions();
        let first = regions.mapping(Region::Arena).unwrap();
        let second = regions.mapping(Region::Arena).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.1, 64 * 1024);
    }

    #[test]
    fn ranges_outside_a_region_are_rejected() {
        let regions = regions();
        assert!(
            regions
                .with_read(Region::Arena, 64 * 1024, 1, &mut |_| Ok(()))
                .is_err()
        );
        assert!(
            regions
                .with_write(Region::Staging, 16 * 1024, 1, &mut |_| Ok(()))
                .is_err()
        );
        assert!(
            regions
                .with_read(Region::Arena, u64::MAX, 2, &mut |_| Ok(()))
                .is_err()
        );
        assert!(
            regions
                .with_read(Region::Staging, 32 * 1024, 1, &mut |_| Ok(()))
                .is_err()
        );
    }
}
