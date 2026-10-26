//! Pressure-driven retention of GPU checkpoints.
use crate::{
    CheckpointId, HostStore, PrefixKey, PublishedCheckpoint, Result, StoreOutcome, StoreReason,
};

pub struct Supply {
    pub free: usize,
    pub demand: usize,
    pub largest_admission: usize,
}

/// Read-only capacity queries and the existing asynchronous copy pin.
pub trait Cache {
    fn supply(&self) -> Supply;
    fn projected(&self, retained: &dyn Fn(PrefixKey) -> bool) -> usize;
    fn candidates(&self) -> Vec<CheckpointId>;
    fn publication(&self, id: CheckpointId) -> Option<PublishedCheckpoint>;
    fn resident(&self, id: CheckpointId) -> bool;
    fn can_pin(&self, id: CheckpointId) -> bool;
    fn pin(&mut self, id: CheckpointId) -> Result<()>;
}

#[derive(Default)]
pub struct WriteBack {
    // Host eviction does not change a GPU checkpoint's completed write-back.
    clean: std::collections::HashMap<PrefixKey, CheckpointId>,
    // Store progress advances on completion or capacity release, never idle polls.
    blocked: Option<u64>,
}

impl WriteBack {
    pub fn stored(&mut self, id: CheckpointId, key: PrefixKey, success: bool) {
        if success {
            self.clean.insert(key, id);
        }
    }

    pub fn pump(
        &mut self,
        store: &mut dyn HostStore,
        cache: &mut impl Cache,
        prefill_rate: f64,
        mut observed: impl FnMut(StoreOutcome),
    ) -> Result<bool> {
        self.clean.retain(|_, id| cache.resident(*id));
        if self.blocked == Some(store.stats().progress) {
            return Ok(false);
        }
        self.blocked = None;
        let needed = |clean: &std::collections::HashMap<PrefixKey, CheckpointId>,
                      store: &dyn HostStore,
                      cache: &dyn Cache| {
            let supply = cache.supply();
            let reserve = store.write_back_reserve(supply.largest_admission, prefill_rate);
            let target = supply.demand.saturating_add(reserve);
            supply.free < target
                && cache.projected(&|key| clean.contains_key(&key) || store.has_copy(key)) < target
        };
        if !needed(&self.clean, store, cache) {
            return Ok(false);
        }
        let mut progress = false;
        for id in cache.candidates() {
            let Some(checkpoint) = cache.publication(id) else {
                continue;
            };
            if self.clean.contains_key(&checkpoint.key) {
                continue;
            }
            if store.has_copy(checkpoint.key) {
                self.clean.insert(checkpoint.key, id);
                continue;
            }
            if !cache.can_pin(id) {
                continue;
            }
            if let Ok(outcome) = store.write_back(&checkpoint) {
                observed(outcome);
                if outcome == StoreOutcome::Queued {
                    cache.pin(id)?;
                }
                if outcome == StoreOutcome::Present {
                    self.clean.insert(checkpoint.key, id);
                }
                if matches!(
                    outcome,
                    StoreOutcome::Retry(StoreReason::Slabs | StoreReason::Budget)
                ) {
                    self.blocked = Some(store.stats().progress);
                }
                if matches!(outcome, StoreOutcome::Retry(_)) {
                    break;
                }
                progress |= matches!(outcome, StoreOutcome::Queued | StoreOutcome::Present);
                if outcome == StoreOutcome::Present && !needed(&self.clean, store, cache) {
                    break;
                }
            }
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PageId, StateSlotId, StoreEvent, memory::MemoryStore, slab::SlabPool, testing};

    struct Checkpoints {
        nodes: Vec<PublishedCheckpoint>,
        pins: Vec<usize>,
    }

    impl Cache for Checkpoints {
        fn supply(&self) -> Supply {
            Supply {
                free: 0,
                demand: 2,
                largest_admission: 128,
            }
        }
        fn projected(&self, retained: &dyn Fn(PrefixKey) -> bool) -> usize {
            self.nodes
                .iter()
                .enumerate()
                .filter(|(i, p)| self.pins[*i] == 0 && retained(p.key))
                .count()
        }
        fn candidates(&self) -> Vec<CheckpointId> {
            self.nodes
                .iter()
                .enumerate()
                .filter(|(i, _)| self.pins[*i] == 0)
                .map(|(_, p)| p.id)
                .collect()
        }
        fn publication(&self, id: CheckpointId) -> Option<PublishedCheckpoint> {
            self.nodes.iter().find(|p| p.id == id).cloned()
        }
        fn resident(&self, id: CheckpointId) -> bool {
            self.nodes.iter().any(|p| p.id == id)
        }
        fn can_pin(&self, _: CheckpointId) -> bool {
            true
        }
        fn pin(&mut self, id: CheckpointId) -> Result<()> {
            let i = self.nodes.iter().position(|p| p.id == id).unwrap();
            self.pins[i] += 1;
            Ok(())
        }
    }

    fn complete(store: &mut MemoryStore, cache: &mut Checkpoints, write_back: &mut WriteBack) {
        let mut events = Vec::new();
        store.poll(&mut events).unwrap();
        for event in events {
            let StoreEvent::Stored {
                checkpoint,
                key,
                result,
            } = event
            else {
                panic!("unexpected event")
            };
            assert!(result.is_ok());
            let i = cache.nodes.iter().position(|p| p.id == checkpoint).unwrap();
            assert_eq!(cache.pins[i], 1);
            cache.pins[i] -= 1;
            write_back.stored(checkpoint, key, true);
        }
    }

    #[test]
    fn small_memory_tier_stops_copying_after_host_eviction() {
        for page_tokens in [64, 128, 256] {
            let geometry = testing::geometry(page_tokens, &[("kv", 4096)], 4096);
            let (copier, _) = testing::SimulatedGpu::shared(&[geometry]).unwrap();
            let pool = SlabPool::new(4096, 2, 4096, 2, true).unwrap();
            let mut store = MemoryStore::new([0; 32], pool, copier).unwrap();
            let mut cache = Checkpoints {
                nodes: (0..4)
                    .map(|i| PublishedCheckpoint {
                        id: CheckpointId::new(i),
                        key: PrefixKey {
                            consumed_tokens: page_tokens,
                            digest: [i as u8; 16],
                        },
                        parent: None,
                        bucket: [i as u8; 16],
                        chain_pages: vec![PageId(i as u32)],
                        tail_pages: 1,
                        slot: StateSlotId(i as u32),
                    })
                    .collect(),
                pins: vec![0; 4],
            };
            let mut write_back = WriteBack::default();
            let mut outcomes = Vec::new();
            assert_eq!(store.write_back_reserve(128, 1e9), 2);
            assert!(
                write_back
                    .pump(&mut store, &mut cache, 1e9, |o| outcomes.push(o))
                    .unwrap()
            );
            assert_eq!(
                outcomes,
                [
                    StoreOutcome::Queued,
                    StoreOutcome::Queued,
                    StoreOutcome::Retry(StoreReason::Slabs)
                ]
            );
            assert!(
                !write_back
                    .pump(&mut store, &mut cache, 1e9, |_| panic!(
                        "retried without capacity"
                    ))
                    .unwrap()
            );
            complete(&mut store, &mut cache, &mut write_back);
            assert!(
                write_back
                    .pump(&mut store, &mut cache, 1e9, |o| outcomes.push(o))
                    .unwrap()
            );
            complete(&mut store, &mut cache, &mut write_back);
            assert!(!store.has_copy(cache.nodes[0].key));
            assert!(!store.has_copy(cache.nodes[1].key));
            assert_eq!(store.stats().stored, 4);
            assert_eq!(store.stats().memory_records, 2);
            assert_eq!(store.stats().outbound_in_flight, 0);
            assert_eq!(cache.pins, [0; 4]);
            assert!(
                !write_back
                    .pump(&mut store, &mut cache, 1e9, |_| panic!("copied again"))
                    .unwrap()
            );
            complete(&mut store, &mut cache, &mut write_back);
            assert!(
                !write_back
                    .pump(&mut store, &mut cache, 1e9, |_| panic!("copied again"))
                    .unwrap()
            );
            assert_eq!(store.stats().stored, 4);
            // Clean state belongs to this GPU residency, not to a future publication of the key.
            cache.nodes[0].id = CheckpointId::new(4);
            write_back
                .pump(&mut store, &mut cache, 1e9, |o| outcomes.push(o))
                .unwrap();
            assert_eq!(outcomes.last(), Some(&StoreOutcome::Queued));
            complete(&mut store, &mut cache, &mut write_back);
            store.close().unwrap();
        }
    }
}
