//! Exports of host-tier holdings: pinned record chains that fill windows from
//! slabs in memory or from record files.

use std::{fs::File, path::PathBuf, sync::Arc};

use flare_eos::FileReadAt;

use crate::{Error, PrefixKey, Result, record::Header};

use super::{
    exporter::{Checksums, HostChain, HostPin, HostRecord, HostWindow},
    tiers::{RecordSums, Releases},
};

/// The records of an export: the nodes from the one holding the page that
/// contains `begin_tokens` to the endpoint, each with the pages and first page
/// it supplies. `nodes` are ascending `(key, generation, tail pages)`; the
/// endpoint carries the row when `want_row`.
pub(crate) fn host_chain(
    page_tokens: u64,
    begin_tokens: u64,
    want_row: bool,
    nodes: &[(PrefixKey, u64, usize)],
) -> HostChain {
    let start_page = begin_tokens / page_tokens;
    let mut records = Vec::new();
    for (index, (key, generation, tail)) in nodes.iter().enumerate() {
        let end = key.consumed_tokens / page_tokens;
        let start = end.saturating_sub(*tail as u64);
        let from = start.max(start_page);
        let pages = end.saturating_sub(from);
        let last = index + 1 == nodes.len();
        if pages == 0 && !last {
            continue;
        }
        records.push(HostRecord {
            key: *key,
            generation: *generation,
            first_page: (from - start) as u32,
            pages: pages as u32,
            row: last && want_row,
        });
    }
    HostChain {
        total_pages: records.iter().map(|r| u64::from(r.pages)).sum(),
        row: want_row,
        records,
    }
}

fn check_window(
    chain: &HostChain,
    window: &HostWindow,
    dst: &[u8],
    page: u64,
    row: u64,
) -> Result<()> {
    let record = chain
        .records
        .get(window.record as usize)
        .ok_or_else(|| Error::new("window names no record"))?;
    let end = record.first_page + record.pages;
    if window.first_page < record.first_page
        || window.first_page + window.page_count > end
        || (window.row && !record.row)
        || dst.len() as u64
            != u64::from(window.page_count) * page + if window.row { row } else { 0 }
    {
        return Err(Error::new("window does not fit the pinned record"));
    }
    Ok(())
}

/// One pinned record in memory.
pub(crate) struct MemorySpan {
    /// Addresses of every tail page of the record.
    pub pages: Vec<usize>,
    pub row: Option<usize>,
    pub sums: Arc<RecordSums>,
}

/// A chain of records in registered host memory, held in place by pins that
/// the store releases once this is dropped.
pub struct MemoryPin {
    pub(crate) chain: HostChain,
    pub(crate) spans: Vec<MemorySpan>,
    pub(crate) page_bytes: usize,
    pub(crate) row_bytes: usize,
    pub(crate) keys: Vec<PrefixKey>,
    pub(crate) releases: Releases,
}

impl HostPin for MemoryPin {
    fn chain(&self) -> &HostChain {
        &self.chain
    }

    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums> {
        check_window(
            &self.chain,
            window,
            dst,
            self.page_bytes as u64,
            self.row_bytes as u64,
        )?;
        let span = &self.spans[window.record as usize];
        let mut offset = 0;
        let mut pages = Vec::with_capacity(window.page_count as usize);
        for page in window.first_page..window.first_page + window.page_count {
            let source = span.pages[page as usize];
            // The record is pinned, so its slab is mapped and unchanged.
            let bytes = unsafe { std::slice::from_raw_parts(source as *const u8, self.page_bytes) };
            dst[offset..offset + self.page_bytes].copy_from_slice(bytes);
            offset += self.page_bytes;
            pages.push(span.sums.pages[page as usize]);
        }
        let row = if window.row {
            let source = span
                .row
                .ok_or_else(|| Error::new("the record has no row"))?;
            let bytes = unsafe { std::slice::from_raw_parts(source as *const u8, self.row_bytes) };
            dst[offset..offset + self.row_bytes].copy_from_slice(bytes);
            span.sums.row
        } else {
            None
        };
        Ok(Checksums { pages, row })
    }
}

impl Drop for MemoryPin {
    fn drop(&mut self) {
        self.releases.push(&self.keys);
    }
}

/// One pinned record file.
pub(crate) struct FileSpan {
    pub path: PathBuf,
    pub header: Header,
}

/// A chain of record files, held in place by pins that the store releases once
/// this is dropped. Pages are read from the files and the checksums served are
/// the ones the files persist.
pub struct FilePin {
    pub(crate) chain: HostChain,
    pub(crate) spans: Vec<FileSpan>,
    pub(crate) page_bytes: u64,
    pub(crate) row_bytes: u64,
    pub(crate) keys: Vec<PrefixKey>,
    pub(crate) releases: Releases,
}

impl HostPin for FilePin {
    fn chain(&self) -> &HostChain {
        &self.chain
    }

    fn read(&mut self, window: &HostWindow, dst: &mut [u8]) -> Result<Checksums> {
        check_window(&self.chain, window, dst, self.page_bytes, self.row_bytes)?;
        let span = &self.spans[window.record as usize];
        let io = |what: &str, e: std::io::Error| {
            Error::caused_by(format!("{what} {}", span.path.display()), e)
        };
        let file = File::open(&span.path).map_err(|e| io("cannot open", e))?;
        let count = window.page_count as usize;
        let pages_bytes = count * self.page_bytes as usize;
        let first = u64::from(window.first_page);
        file.read_exact_at(
            &mut dst[..pages_bytes],
            span.header.page_segment + first * self.page_bytes,
        )
        .map_err(|e| io("cannot read pages of", e))?;
        let mut stored = vec![0u8; count * 8];
        file.read_exact_at(&mut stored, span.header.checksum_segment() + first * 8)
            .map_err(|e| io("cannot read checksums of", e))?;
        let pages = stored
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().expect("eight bytes")))
            .collect();
        let row = if window.row {
            file.read_exact_at(
                &mut dst[pages_bytes..pages_bytes + self.row_bytes as usize],
                span.header.row_segment,
            )
            .map_err(|e| io("cannot read the row of", e))?;
            // The persisted row checksum is in the file's own header.
            let mut block = vec![0u8; crate::record::HEADER_BYTES];
            file.read_exact_at(&mut block, 0)
                .map_err(|e| io("cannot read the header of", e))?;
            Some(Header::decode(&block)?.row_checksum)
        } else {
            None
        };
        Ok(Checksums { pages, row })
    }
}

impl Drop for FilePin {
    fn drop(&mut self) {
        self.releases.push(&self.keys);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(tokens: u64, salt: u8) -> PrefixKey {
        PrefixKey {
            consumed_tokens: tokens,
            digest: [salt; 16],
        }
    }

    #[test]
    fn records_start_where_the_split_falls_and_a_split_in_the_last_page_leaves_a_row_only_tail() {
        // Pages of 16 tokens: a covers pages 0..50, b pages 50..100.
        let nodes = [(key(800, 1), 5, 50), (key(1600, 2), 6, 50)];
        let chain = host_chain(16, 0, true, &nodes);
        assert_eq!(chain.total_pages, 100);
        assert_eq!(chain.records[0].first_page, 0);
        assert!(!chain.records[0].row && chain.records[1].row);
        // Starting inside b skips a entirely.
        let chain = host_chain(16, 1000, true, &nodes);
        assert_eq!(chain.records.len(), 1);
        assert_eq!(chain.records[0].key, key(1600, 2));
        assert_eq!(
            (chain.records[0].first_page, chain.records[0].pages),
            (12, 38)
        );
        // Starting at the page the endpoint ends in: its row alone.
        let chain = host_chain(16, 1600, true, &nodes);
        assert_eq!(chain.total_pages, 0);
        assert_eq!(
            (chain.records[0].first_page, chain.records[0].pages),
            (50, 0)
        );
        assert!(chain.records[0].row);
        // Without the row, no record carries one.
        assert!(
            host_chain(16, 0, false, &nodes)
                .records
                .iter()
                .all(|r| !r.row)
        );
    }

    #[test]
    fn a_memory_pin_copies_pages_and_serves_the_checksums_it_was_given_and_releases_on_drop() {
        let page = 64usize;
        let data: Vec<u8> = (0..3 * page + 32).map(|i| (i * 7) as u8).collect();
        let base = data.as_ptr() as usize;
        let releases = Releases::default();
        let sums = Arc::new(RecordSums {
            pages: vec![11, 22, 33],
            row: Some(44),
        });
        let nodes = [(key(48, 1), 1, 3)];
        let mut pin = MemoryPin {
            chain: host_chain(16, 16, true, &nodes),
            spans: vec![MemorySpan {
                pages: vec![base, base + page, base + 2 * page],
                row: Some(base + 3 * page),
                sums,
            }],
            page_bytes: page,
            row_bytes: 32,
            keys: vec![key(48, 1)],
            releases: releases.clone(),
        };
        assert_eq!(
            (
                pin.chain().records[0].first_page,
                pin.chain().records[0].pages
            ),
            (1, 2)
        );
        let window = HostWindow {
            record: 0,
            first_page: 1,
            page_count: 2,
            row: true,
        };
        let mut dst = vec![0u8; 2 * page + 32];
        let sums = pin.read(&window, &mut dst).unwrap();
        assert_eq!(dst, data[page..]);
        assert_eq!((sums.pages, sums.row), (vec![22, 33], Some(44)));
        // A window outside what was pinned is refused.
        let outside = HostWindow {
            record: 0,
            first_page: 0,
            page_count: 1,
            row: false,
        };
        assert!(pin.read(&outside, &mut vec![0u8; page]).is_err());
        assert!(pin.read(&window, &mut [0u8; 5]).is_err());
        drop(pin);
        assert_eq!(releases.drain(), [key(48, 1)]);
    }
}
