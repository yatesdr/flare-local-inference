//! The host tiers announce what they hold and serve it as exports, against the
//! real memory store and a simulated GPU.

use std::{sync::Arc, time::Duration};

use tokio::sync::Notify;

use crate::{
    CheckpointId, ExportRefusal, History, HostStore, Layout, PageId, PublishedCheckpoint,
    StateSlotId, StoreEvent, StoreOutcome,
    memory::MemoryStore,
    pool::{Announcer, HostWindow, Notice, Tier},
    record::checksum,
    slab::SlabPool,
    testing::{GpuMemory, SimulatedGpu, TestHistory, geometry},
};

const PT: u64 = 16;
const PAGE: usize = 4096 * 2;
const ROW: usize = 4096;

struct Rig {
    store: MemoryStore,
    gpu: GpuMemory,
    announcer: Announcer,
    history: TestHistory,
    next: u64,
}

fn rig(pages: usize) -> Rig {
    let geometry = geometry(PT, &[("k", 4096), ("v", 4096)], 4096);
    let layout = Layout::new(std::slice::from_ref(&geometry)).unwrap();
    assert_eq!(layout.page_record_bytes as usize, PAGE);
    let (copier, gpu) = SimulatedGpu::shared(std::slice::from_ref(&geometry)).unwrap();
    let pool = SlabPool::new(PAGE as u64, pages, ROW as u64, 4, true).unwrap();
    let mut store = MemoryStore::new([7; 32], pool, copier).unwrap();
    let announcer = Announcer::new(Arc::new(Notify::new()));
    announcer.begin_session();
    store.announce_to(announcer.clone());
    let mut history = TestHistory::new([7; 32]);
    history
        .append(&(0..4000).collect::<Vec<u32>>(), &[])
        .unwrap();
    Rig {
        store,
        gpu,
        announcer,
        history,
        next: 0,
    }
}

impl Rig {
    /// A root checkpoint of `pages` pages whose device pages hold known bytes.
    fn publish(&mut self, pages: usize, first_id: u32) -> PublishedCheckpoint {
        self.next += 1;
        let consumed = pages as u64 * PT;
        for page in 0..pages {
            self.gpu.pages.lock().unwrap().insert(
                first_id + page as u32,
                (0..PAGE)
                    .map(|i| (i as u32 * 7 + first_id + page as u32) as u8)
                    .collect(),
            );
        }
        self.gpu.rows.lock().unwrap().insert(
            first_id,
            (0..ROW).map(|i| (i + first_id as usize) as u8).collect(),
        );
        PublishedCheckpoint {
            id: CheckpointId::new(self.next),
            key: self.history.key_at(consumed).unwrap(),
            parent: None,
            bucket: self.history.bucket_at(consumed).unwrap(),
            chain_pages: (0..pages as u32).map(|p| PageId(first_id + p)).collect(),
            tail_pages: pages,
            slot: StateSlotId(first_id),
        }
    }

    fn poll(&mut self) -> Vec<StoreEvent> {
        let mut events = Vec::new();
        self.store.poll(&mut events).unwrap();
        events
    }

    /// Poll until `done` or fail.
    fn poll_until(&mut self, mut done: impl FnMut(&mut Rig) -> bool) {
        for _ in 0..2000 {
            self.poll();
            if done(self) {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("condition not reached");
    }

    fn notices(&self) -> Vec<Notice> {
        self.announcer
            .take_batch(1000)
            .map(|(_, notices)| notices)
            .unwrap_or_default()
    }
}

fn saved(notices: &[Notice]) -> Vec<(u64, u64, bool, Tier)> {
    notices
        .iter()
        .filter_map(|n| match n {
            Notice::Saved(e) => Some((e.key.consumed_tokens, e.generation, e.row, e.tier)),
            _ => None,
        })
        .collect()
}

#[test]
fn a_stored_record_is_announced_once_hashed_and_served_with_those_checksums() {
    let mut rig = rig(8);
    let checkpoint = rig.publish(3, 100);
    assert_eq!(rig.store.store(&checkpoint).unwrap(), StoreOutcome::Queued);
    let mut announced = Vec::new();
    rig.poll_until(|rig| {
        announced.extend(rig.notices());
        !announced.is_empty()
    });
    let entries = saved(&announced);
    assert_eq!(entries.len(), 1, "announced once");
    let (consumed, generation, row, tier) = entries[0];
    assert_eq!((consumed, row, tier), (3 * PT, true, Tier::Dram));
    let key = checkpoint.key;
    // The wrong generation or an unknown key is refused.
    assert_eq!(
        rig.store
            .pin_export(Tier::Dram, key, generation + 1, 0, true)
            .err(),
        Some(ExportRefusal::GenerationChanged)
    );
    let other = rig.history.key_at(5 * PT).unwrap();
    assert_eq!(
        rig.store
            .pin_export(Tier::Dram, other, generation, 0, true)
            .err(),
        Some(ExportRefusal::NotFound)
    );
    assert_eq!(
        rig.store
            .pin_export(Tier::Nvme, key, generation, 0, true)
            .err(),
        Some(ExportRefusal::NotFound)
    );
    let mut pin = rig
        .store
        .pin_export(Tier::Dram, key, generation, 0, true)
        .unwrap();
    let chain = pin.chain().clone();
    assert_eq!(
        (chain.total_pages, chain.row, chain.records.len()),
        (3, true, 1)
    );
    let window = HostWindow {
        record: 0,
        first_page: 0,
        page_count: 3,
        row: true,
    };
    let mut dst = vec![0u8; 3 * PAGE + ROW];
    let sums = pin.read(&window, &mut dst).unwrap();
    for page in 0..3 {
        let sent = rig.gpu.pages.lock().unwrap()[&(100 + page as u32)].clone();
        assert_eq!(dst[page * PAGE..(page + 1) * PAGE], sent[..]);
        assert_eq!(sums.pages[page], checksum(&sent));
    }
    let row_bytes = rig.gpu.rows.lock().unwrap()[&100].clone();
    assert_eq!(dst[3 * PAGE..], row_bytes[..]);
    assert_eq!(sums.row, Some(checksum(&row_bytes)));
    // Starting inside the record serves a suffix and its checksums.
    let mut suffix = rig
        .store
        .pin_export(Tier::Dram, key, generation, PT, false)
        .unwrap();
    assert_eq!(suffix.chain().records[0].first_page, 1);
    let mut dst = vec![0u8; 2 * PAGE];
    let part = suffix
        .read(
            &HostWindow {
                record: 0,
                first_page: 1,
                page_count: 2,
                row: false,
            },
            &mut dst,
        )
        .unwrap();
    assert_eq!(part.pages, sums.pages[1..]);
    assert_eq!(part.row, None);
}

#[test]
fn a_pinned_record_survives_pressure_until_the_pin_is_dropped_and_its_eviction_is_announced() {
    let mut rig = rig(3);
    let first = rig.publish(3, 100);
    assert_eq!(rig.store.store(&first).unwrap(), StoreOutcome::Queued);
    let mut announced = Vec::new();
    rig.poll_until(|rig| {
        announced.extend(rig.notices());
        !announced.is_empty()
    });
    let generation = saved(&announced)[0].1;
    let pin = rig
        .store
        .pin_export(Tier::Dram, first.key, generation, 0, true)
        .unwrap();
    // The pool is full, and the only record is pinned: nothing can be stored.
    let second = rig.publish(2, 200);
    assert_ne!(rig.store.store(&second).unwrap(), StoreOutcome::Queued);
    rig.poll();
    assert!(rig.notices().is_empty());
    drop(pin);
    rig.poll();
    assert_eq!(rig.store.store(&second).unwrap(), StoreOutcome::Queued);
    let mut after = Vec::new();
    rig.poll_until(|rig| {
        after.extend(rig.notices());
        after.iter().any(|n| matches!(n, Notice::Saved(_)))
    });
    let evicted: Vec<_> = after
        .iter()
        .filter_map(|n| match n {
            Notice::Evicted {
                key,
                tier,
                generation,
                ..
            } => Some((*key, *tier, *generation)),
            _ => None,
        })
        .collect();
    assert_eq!(evicted, [(first.key, Tier::Dram, generation)]);
    assert!(
        rig.store
            .pin_export(Tier::Dram, first.key, generation, 0, true)
            .is_err()
    );
}

#[test]
fn a_store_that_was_never_attached_announces_nothing_and_exports_nothing() {
    let geometry = geometry(PT, &[("k", 4096), ("v", 4096)], 4096);
    let (copier, _gpu) = SimulatedGpu::shared(std::slice::from_ref(&geometry)).unwrap();
    let pool = SlabPool::new(PAGE as u64, 4, ROW as u64, 2, true).unwrap();
    let mut store = MemoryStore::new([7; 32], pool, copier).unwrap();
    let mut events = Vec::new();
    store.poll(&mut events).unwrap();
    let key = crate::PrefixKey {
        consumed_tokens: PT,
        digest: [1; 16],
    };
    assert_eq!(
        store.pin_export(Tier::Dram, key, 1, 0, true).err(),
        Some(ExportRefusal::NotFound)
    );
}

// ---- probes -------------------------------------------------------------------

use crate::Scratch;

fn scratch(pages: usize) -> Scratch {
    Scratch {
        pages: (0..pages as u32).map(|p| PageId(1000 + p)).collect(),
        slot: StateSlotId(900),
    }
}

#[test]
fn the_memory_probe_uploads_known_pages_from_free_slabs_and_leaves_the_store_as_it_was() {
    let mut rig = rig(8);
    let before = rig.store.pool().free_pages();
    let scratch = scratch(6);
    let elapsed = rig
        .store
        .probe(crate::Tier::Memory, &scratch, 4, false)
        .unwrap()
        .expect("a memory tier measures");
    assert!(elapsed > Duration::ZERO);
    assert_eq!(
        rig.store.pool().free_pages(),
        before,
        "the slabs are released"
    );
    // The scratch ids received the synthetic pages and the row.
    assert_eq!(rig.gpu.pages.lock().unwrap().len(), 4);
    assert!(rig.gpu.rows.lock().unwrap().contains_key(&900));
    // More pages than there are free slabs is refused rather than evicting.
    let big = self::scratch(9);
    assert!(
        rig.store
            .probe(crate::Tier::Memory, &big, 9, false)
            .is_err()
    );
    // Another tier is not this store's.
    assert!(
        rig.store
            .probe(crate::Tier::Disk, &scratch, 2, false)
            .unwrap()
            .is_none()
    );
    // The probe never shows up in the pool's announcements.
    rig.poll();
    assert!(rig.notices().is_empty());
}

#[test]
fn the_disk_probe_writes_and_restores_a_record_cold_and_leaves_no_file_and_no_announcement() {
    use crate::{
        disk::DiskThread,
        disk_store::{DiskStore, Staged},
        record,
    };
    let base = std::env::var_os("FLARE_OFFLOAD_TEST_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let directory = base.join(format!("flare-selene-probe-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let probe_file = directory.join(".probe");
    match flare_eos::direct_file(&probe_file, true) {
        Ok(_) => std::fs::remove_file(&probe_file).unwrap(),
        Err(error) if cfg!(unix) && error.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipping: {} refuses direct I/O", directory.display());
            std::fs::remove_dir_all(&directory).unwrap();
            return;
        }
        Err(error) => panic!("probe {}: {error}", probe_file.display()),
    }
    let geometry = vec![geometry(PT, &[("k", 4096), ("v", 4096)], 4096)];
    let layout = Layout::new(&geometry).unwrap();
    let (copier, _gpu) = SimulatedGpu::shared(&geometry).unwrap();
    let lock = record::DirectoryLock::acquire(&directory).unwrap();
    let mut store = DiskStore::new(
        lock,
        [7; 32],
        [8; 16],
        1 << 30,
        geometry,
        Vec::new(),
        Box::new(Staged::new(
            Some(SlabPool::staging(&layout).unwrap()),
            copier,
            DiskThread::start("selene-probe-test").unwrap(),
        )),
    )
    .unwrap();
    let announcer = Announcer::new(Arc::new(Notify::new()));
    announcer.begin_session();
    store.announce_to(announcer.clone());
    let scratch = scratch(6);
    let elapsed = store
        .probe(crate::Tier::Disk, &scratch, 4, true)
        .unwrap()
        .expect("a disk tier measures");
    assert!(elapsed > Duration::ZERO);
    assert!(store.index().is_empty(), "the probe record is gone");
    let files = std::fs::read_dir(&directory)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "ckpt"))
        .count();
    assert_eq!(files, 0);
    store.flush_announcements();
    assert!(
        announcer.take_batch(10).is_none(),
        "the pool never hears of it"
    );
    assert!(
        store
            .probe(crate::Tier::Memory, &scratch, 2, true)
            .unwrap()
            .is_none()
    );
    // A record already stored is not probed over.
    drop(store);
    std::fs::remove_dir_all(&directory).unwrap();
}

#[test]
fn a_store_dropped_while_its_records_are_being_hashed_frees_no_slab_under_the_hasher() {
    // The checksum thread reads a record's slabs; the store joins it before
    // it frees them.
    for _ in 0..300 {
        let mut rig = rig(8);
        let checkpoint = rig.publish(3, 100);
        assert_eq!(rig.store.store(&checkpoint).unwrap(), StoreOutcome::Queued);
        rig.poll();
        rig.poll();
        drop(rig);
    }
}
