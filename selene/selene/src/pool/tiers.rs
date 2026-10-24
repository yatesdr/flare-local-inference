//! What a host tier holds, as the pool hears of it, and the pins and checksum
//! thread an export from that tier needs.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, mpsc},
    thread,
};

use crate::{
    Layout, PrefixKey, Result,
    index::{NodeEvent, NodeNotice, NodeNotices},
    record::checksum,
};

use super::{
    announce::{Announcer, IndexEntry},
    types::Tier,
};

/// Checksums of one record: one per tail page, and the row's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordSums {
    pub pages: Vec<u64>,
    pub row: Option<u64>,
}

/// Keys pinned for exports that have ended, for the store to unpin on its own
/// thread. A pin is released by dropping it, from any thread.
#[derive(Clone, Default)]
pub struct Releases(Arc<Mutex<Vec<PrefixKey>>>);

impl Releases {
    pub fn push(&self, keys: &[PrefixKey]) {
        self.0.lock().unwrap().extend_from_slice(keys);
    }

    pub fn drain(&self) -> Vec<PrefixKey> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// Announces one tier's holdings from the events of its index.
pub struct TierView {
    announcer: Announcer,
    tier: Tier,
    sink: NodeNotices,
    generations: HashMap<PrefixKey, u64>,
    page_record_bytes: u64,
    row_record_bytes: u64,
}

impl TierView {
    pub fn new(announcer: Announcer, tier: Tier, layout: &Layout) -> Self {
        Self {
            announcer,
            tier,
            sink: Default::default(),
            generations: HashMap::new(),
            page_record_bytes: layout.page_record_bytes,
            row_record_bytes: layout.row_record_bytes,
        }
    }

    /// Where the tier's index records its events.
    pub fn sink(&self) -> NodeNotices {
        self.sink.clone()
    }

    /// The events recorded since the last call.
    pub fn drain(&self) -> Vec<NodeNotice> {
        std::mem::take(&mut *self.sink.lock().unwrap())
    }

    /// The generation announced for the key in this tier.
    pub fn generation(&self, key: &PrefixKey) -> Option<u64> {
        self.generations.get(key).copied()
    }

    /// Announce a node that became ready, or whose row was dropped, with the
    /// parent the manager should chain it under.
    pub fn announce(&mut self, notice: &NodeNotice, parent: Option<PrefixKey>) {
        let generation = match notice.event {
            NodeEvent::Ready => *self
                .generations
                .entry(notice.key)
                .or_insert_with(|| self.announcer.next_generation(self.tier)),
            NodeEvent::RowDropped => match self.generations.get(&notice.key) {
                Some(generation) => *generation,
                None => return,
            },
            NodeEvent::Gone => return,
        };
        self.announcer.saved(IndexEntry {
            key: notice.key,
            parent,
            bucket: notice.bucket,
            tail_pages: notice.tail_pages as u32,
            row: notice.row,
            tier: self.tier,
            generation,
            bytes: notice.tail_pages as u64 * self.page_record_bytes
                + if notice.row { self.row_record_bytes } else { 0 },
        });
    }

    /// The node left the tier because the next tier holds it, which `to` describes:
    /// one notice, so a lookup never sees neither.
    pub fn spill(&mut self, key: &PrefixKey, to: IndexEntry) {
        if let Some(generation) = self.generations.remove(key) {
            self.announcer.spilled(to, (self.tier, generation));
        }
    }

    /// The node left the tier.
    pub fn retire(&mut self, key: &PrefixKey) {
        if let Some(generation) = self.generations.remove(key) {
            self.announcer.evicted(*key, self.tier, generation, None);
        }
    }
}

/// A record in a window of memory the hasher may read: pinned in its index
/// until the result is collected.
pub(crate) struct HashJob {
    pub key: PrefixKey,
    pub pages: Vec<usize>,
    pub page_bytes: usize,
    pub row: Option<usize>,
    pub row_bytes: usize,
}

pub(crate) struct HashDone {
    pub key: PrefixKey,
    pub sums: RecordSums,
}

/// Hashes records off the worker thread. Started when a store first announces.
pub(crate) struct Hasher {
    jobs: Option<mpsc::Sender<HashJob>>,
    done: mpsc::Receiver<HashDone>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Hasher {
    pub(crate) fn start() -> Result<Self> {
        let (jobs, rx) = mpsc::channel::<HashJob>();
        let (tx, done) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("selene-checksum".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // The record is pinned until its result is collected, so its
                    // slabs stay mapped and unchanged.
                    let read = |address: usize, len: usize| unsafe {
                        std::slice::from_raw_parts(address as *const u8, len)
                    };
                    let sums = RecordSums {
                        pages: job
                            .pages
                            .iter()
                            .map(|address| checksum(read(*address, job.page_bytes)))
                            .collect(),
                        row: job
                            .row
                            .map(|address| checksum(read(address, job.row_bytes))),
                    };
                    if tx.send(HashDone { key: job.key, sums }).is_err() {
                        break;
                    }
                }
            })
            .map_err(|e| crate::Error::caused_by("cannot start the checksum thread", e))?;
        Ok(Self {
            jobs: Some(jobs),
            done,
            thread: Some(thread),
        })
    }

    pub(crate) fn submit(&self, job: HashJob) -> bool {
        self.jobs
            .as_ref()
            .is_some_and(|jobs| jobs.send(job).is_ok())
    }

    pub(crate) fn collect(&self) -> Vec<HashDone> {
        self.done.try_iter().collect()
    }
}

impl Drop for Hasher {
    /// Wait for the job in progress, which reads slabs the store frees next.
    fn drop(&mut self) {
        self.jobs = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pool::announce::Notice, testing::geometry};
    use tokio::sync::Notify;

    fn key(n: u64) -> PrefixKey {
        PrefixKey {
            consumed_tokens: n * 64,
            digest: [n as u8; 16],
        }
    }

    fn notice(event: NodeEvent, n: u64, row: bool) -> NodeNotice {
        NodeNotice {
            event,
            key: key(n),
            parent: None,
            bucket: [0; 16],
            tail_pages: 2,
            row,
        }
    }

    #[test]
    fn a_tier_view_announces_ready_nodes_with_stable_generations_and_retires_them() {
        let announcer = Announcer::new(Arc::new(Notify::new()));
        announcer.begin_session();
        let layout = Layout::new(&[geometry(64, &[("k", 4096)], 4096)]).unwrap();
        let mut view = TierView::new(announcer.clone(), Tier::Dram, &layout);
        view.announce(&notice(NodeEvent::Ready, 1, true), Some(key(0)));
        view.announce(&notice(NodeEvent::Ready, 2, true), None);
        let first = view.generation(&key(1)).unwrap();
        assert_ne!(first, view.generation(&key(2)).unwrap());
        // Ready again keeps the generation; a dropped row updates the entry.
        view.announce(&notice(NodeEvent::RowDropped, 1, false), Some(key(0)));
        assert_eq!(view.generation(&key(1)), Some(first));
        view.retire(&key(1));
        view.retire(&key(1));
        assert_eq!(view.generation(&key(1)), None);
        // A node never announced is not retired.
        view.retire(&key(9));
        let (_, notices) = announcer.take_batch(100).unwrap();
        let kinds: Vec<_> = notices
            .iter()
            .map(|n| match n {
                Notice::Saved(e) => ("saved", e.row, e.bytes),
                Notice::Evicted { generation, .. } => ("evicted", false, *generation),
                Notice::Spilled { .. } => ("spilled", false, 0),
            })
            .collect();
        assert_eq!(kinds[0], ("saved", true, 2 * 4096 + 4096));
        assert_eq!(kinds[2], ("saved", false, 2 * 4096));
        assert_eq!(kinds[3], ("evicted", false, first));
        assert_eq!(kinds.len(), 4);
    }

    #[test]
    fn the_hasher_hashes_each_page_and_the_row_off_the_calling_thread() {
        let hasher = Hasher::start().unwrap();
        let data: Vec<u8> = (0..3 * 64).map(|i| i as u8).collect();
        let base = data.as_ptr() as usize;
        assert!(hasher.submit(HashJob {
            key: key(1),
            pages: vec![base, base + 64],
            page_bytes: 64,
            row: Some(base + 128),
            row_bytes: 64,
        }));
        let done = loop {
            let done = hasher.collect();
            if let Some(done) = done.into_iter().next() {
                break done;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        assert_eq!(done.key, key(1));
        assert_eq!(
            done.sums.pages,
            [checksum(&data[..64]), checksum(&data[64..128])]
        );
        assert_eq!(done.sums.row, Some(checksum(&data[128..])));
    }

    #[test]
    fn releases_collect_keys_from_any_thread_and_drain_once() {
        let releases = Releases::default();
        let clone = releases.clone();
        std::thread::spawn(move || clone.push(&[key(1), key(2)]))
            .join()
            .unwrap();
        assert_eq!(releases.drain(), [key(1), key(2)]);
        assert!(releases.drain().is_empty());
    }
}
