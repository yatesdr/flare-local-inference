use super::{Checksums, RankRecords};
use crate::{
    CacheGeometry, Layout, PrefixKey, Result,
    geometry::invalid,
    record::{self, HEADER_BYTES, Header},
};
use std::{
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// Filled after cache allocation and scanning, before the peer can serve a pool command.
#[derive(Clone, Default)]
pub struct DeferredRecords(Arc<Mutex<Option<Records>>>);

struct Records {
    directory: PathBuf,
    identity: [u8; 32],
    fingerprint: [u8; 16],
    geometry: Vec<CacheGeometry>,
    layout: Layout,
    _lock: record::DirectoryLock,
}

impl DeferredRecords {
    pub fn bind(
        &self,
        directory: PathBuf,
        identity: [u8; 32],
        fingerprint: [u8; 16],
        geometry: Vec<CacheGeometry>,
        lock: record::DirectoryLock,
    ) -> Result<()> {
        *self.0.lock().unwrap() = Some(Records {
            directory,
            identity,
            fingerprint,
            layout: Layout::new(&geometry)?,
            geometry,
            _lock: lock,
        });
        Ok(())
    }
}

impl Records {
    fn header(&self, key: PrefixKey) -> Result<(std::fs::File, Header)> {
        let path = self.directory.join(record::file_name(&key));
        let mut file =
            std::fs::File::open(path).map_err(|e| record::io_failure("open a rank record", e))?;
        let mut block = [0; HEADER_BYTES];
        file.read_exact(&mut block)
            .map_err(|e| record::io_failure("read a rank record header", e))?;
        let header = Header::decode(&block)?;
        if header.key != key
            || header.identity != self.identity
            || header.fingerprint != self.fingerprint
            || !header.matches(&self.geometry)
        {
            return Err(invalid(
                "a pool rank record has a different identity or layout",
            ));
        }
        Ok((file, header))
    }

    fn read(
        &self,
        key: PrefixKey,
        first: u32,
        pages: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        let (mut file, header) = self.header(key)?;
        let page_bytes = self.layout.page_record_bytes as usize;
        let length = pages as usize * page_bytes
            + if row {
                self.layout.row_record_bytes as usize
            } else {
                0
            };
        if first
            .checked_add(pages)
            .is_none_or(|end| end > header.tail_pages)
            || dst.len() != length
        {
            return Err(invalid(
                "a pool rank record slice exceeds its record or destination",
            ));
        }
        let mut stored = vec![0; pages as usize * 8];
        file.seek(SeekFrom::Start(
            header.checksum_segment() + u64::from(first) * 8,
        ))
        .and_then(|_| file.read_exact(&mut stored))
        .map_err(|e| record::io_failure("read rank record checksums", e))?;
        let sums = Checksums {
            pages: stored
                .chunks_exact(8)
                .map(|sum| u64::from_le_bytes(sum.try_into().unwrap()))
                .collect(),
            row: row.then_some(header.row_checksum),
        };
        let page_length = pages as usize * page_bytes;
        file.seek(SeekFrom::Start(
            header.page_segment + u64::from(first) * page_bytes as u64,
        ))
        .and_then(|_| file.read_exact(&mut dst[..page_length]))
        .map_err(|e| record::io_failure("read rank record pages", e))?;
        if row {
            file.seek(SeekFrom::Start(header.row_segment))
                .and_then(|_| file.read_exact(&mut dst[page_length..]))
                .map_err(|e| record::io_failure("read a rank record row", e))?;
        }
        Ok(sums)
    }

    fn fill(
        &self,
        key: PrefixKey,
        first: u32,
        pages: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        let mut chain = Vec::new();
        let mut cursor = key;
        loop {
            let (_, header) = self.header(cursor)?;
            let parent = header.parent;
            chain.push(header);
            let Some(parent) = parent else { break };
            if parent.consumed_tokens >= cursor.consumed_tokens {
                return Err(invalid("a pool rank record chain does not decrease"));
            }
            cursor = parent;
        }
        chain.reverse();
        let end = u64::from(first) + u64::from(pages);
        let (mut base, mut offset) = (0u64, 0usize);
        let mut sums = Checksums {
            pages: Vec::new(),
            row: None,
        };
        for header in &chain {
            let from = u64::from(first).max(base);
            let to = end.min(base + u64::from(header.tail_pages));
            let count = to.saturating_sub(from) as u32;
            let with_row = row && header.key == key;
            if count > 0 || with_row {
                let local_first = if count > 0 {
                    (from - base) as u32
                } else {
                    header.tail_pages
                };
                let bytes = count as usize * self.layout.page_record_bytes as usize
                    + if with_row {
                        self.layout.row_record_bytes as usize
                    } else {
                        0
                    };
                let chunk = dst
                    .get_mut(offset..offset + bytes)
                    .ok_or_else(|| invalid("a pool rank spill destination is too small"))?;
                let read = self.read(header.key, local_first, count, with_row, chunk)?;
                sums.pages.extend(read.pages);
                if with_row {
                    sums.row = read.row;
                }
                offset += bytes;
            }
            base += u64::from(header.tail_pages);
        }
        if end > base || offset != dst.len() {
            return Err(invalid(
                "a pool rank spill exceeds its chain or destination",
            ));
        }
        Ok(sums)
    }
}

impl RankRecords for DeferredRecords {
    fn read(
        &mut self,
        key: PrefixKey,
        _generation: u64,
        first: u32,
        pages: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        // Rank zero owns generations and holds every shard until the agent's read drains.
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| invalid("pool rank records are not initialized"))?
            .read(key, first, pages, row, dst)
    }

    fn fill(
        &mut self,
        key: PrefixKey,
        first: u32,
        pages: u32,
        row: bool,
        dst: &mut [u8],
    ) -> Result<Checksums> {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| invalid("pool rank records are not initialized"))?
            .fill(key, first, pages, row, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pool::RankRecords, testing::geometry};
    use std::time::SystemTime;

    fn key(tokens: u64) -> PrefixKey {
        PrefixKey {
            consumed_tokens: tokens,
            digest: [tokens as u8; 16],
        }
    }

    fn write(
        directory: &Path,
        geometry: &[CacheGeometry],
        tokens: u64,
        parent: Option<PrefixKey>,
        values: &[u8],
    ) -> Header {
        let mut header = Header::new(
            geometry,
            [1; 32],
            [2; 16],
            key(tokens),
            parent,
            [0; 16],
            values.len() as u32,
            SystemTime::now(),
        )
        .unwrap();
        header.row_checksum = record::checksum(&vec![tokens as u8; 4096]);
        let mut bytes = vec![0; header.length_with_row() as usize];
        bytes[..HEADER_BYTES].copy_from_slice(&header.encode().unwrap());
        for (page, &value) in values.iter().enumerate() {
            let at = header.page_segment as usize + page * 4096;
            bytes[at..at + 4096].fill(value);
            let sum = record::checksum(&bytes[at..at + 4096]);
            let at = header.checksum_segment() as usize + page * 8;
            bytes[at..at + 8].copy_from_slice(&sum.to_le_bytes());
        }
        bytes[header.row_segment as usize..].fill(tokens as u8);
        std::fs::write(directory.join(record::file_name(&header.key)), bytes).unwrap();
        header
    }

    #[test]
    fn rank_records_read_slices_rows_and_chain_spills_with_persisted_sums() {
        let directory =
            std::env::temp_dir().join(format!("selene-pool-records-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let geometry = vec![geometry(64, &[("kv", 4096)], 4096)];
        let root = write(&directory, &geometry, 128, None, &[11, 22]);
        let tail = write(&directory, &geometry, 256, Some(root.key), &[33, 44]);
        let lock = record::DirectoryLock::acquire(&directory).unwrap();
        let mut records = DeferredRecords(Arc::new(Mutex::new(Some(Records {
            directory: directory.clone(),
            identity: [1; 32],
            fingerprint: [2; 16],
            layout: Layout::new(&geometry).unwrap(),
            geometry,

            _lock: lock,
        }))));
        let mut storage = vec![0; 4 * 4096];
        let offset = storage.as_ptr().align_offset(4096);
        let dst = &mut storage[offset..offset + 3 * 4096];
        let sums = records
            .read(root.key, 7, 1, 1, true, &mut dst[..8192])
            .unwrap();
        assert_eq!(&dst[..4096], vec![22; 4096]);
        assert_eq!(&dst[4096..8192], vec![128; 4096]);
        assert_eq!(sums.pages, vec![record::checksum(&vec![22; 4096])]);
        assert_eq!(sums.row, Some(root.row_checksum));
        let sums = records.fill(tail.key, 1, 2, true, dst).unwrap();
        assert_eq!(&dst[..4096], vec![22; 4096]);
        assert_eq!(&dst[4096..8192], vec![33; 4096]);
        assert_eq!(&dst[8192..], vec![0; 4096]);
        assert_eq!(sums.pages.len(), 2);
        assert_eq!(sums.row, Some(tail.row_checksum));
        assert!(
            records
                .read(root.key, 7, 2, 1, false, &mut dst[..4096])
                .is_err()
        );
        assert!(
            records
                .read(root.key, 7, 0, 1, false, &mut dst[..12])
                .is_err()
        );
        // A damaged slice still serves the persisted checksum for the manager to verify.
        let path = directory.join(record::file_name(&root.key));
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[root.page_segment as usize] ^= 1;
        std::fs::write(path, bytes).unwrap();
        let sums = records
            .read(root.key, 7, 0, 1, false, &mut dst[..4096])
            .unwrap();
        assert_eq!(sums.pages, vec![record::checksum(&vec![11; 4096])]);
        assert_ne!(sums.pages[0], record::checksum(&dst[..4096]));
        drop(records);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
