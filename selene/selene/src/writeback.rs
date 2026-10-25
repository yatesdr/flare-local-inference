//! Pressure-driven retention of GPU checkpoints.
use crate::{CheckpointId, HostStore, PrefixKey, PublishedCheckpoint, Result, StoreOutcome};

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
    fn can_pin(&self, id: CheckpointId) -> bool;
    fn pin(&mut self, id: CheckpointId) -> Result<()>;
}

pub fn pump(
    store: &mut dyn HostStore,
    cache: &mut impl Cache,
    prefill_rate: f64,
    mut observed: impl FnMut(StoreOutcome),
) -> Result<bool> {
    let needed = |store: &dyn HostStore, cache: &dyn Cache| {
        let supply = cache.supply();
        let reserve = store.write_back_reserve(supply.largest_admission, prefill_rate);
        let target = supply.demand.saturating_add(reserve);
        supply.free < target && cache.projected(&|key| store.has_copy(key)) < target
    };
    if !needed(store, cache) {
        return Ok(false);
    }
    let mut progress = false;
    for id in cache.candidates() {
        let Some(checkpoint) = cache.publication(id) else {
            continue;
        };
        if store.has_copy(checkpoint.key) || !cache.can_pin(id) {
            continue;
        }
        if let Ok(outcome) = store.write_back(&checkpoint) {
            observed(outcome);
            if outcome == StoreOutcome::Queued {
                cache.pin(id)?;
            }
            if matches!(outcome, StoreOutcome::Retry(_)) {
                break;
            }
            progress |= matches!(outcome, StoreOutcome::Queued | StoreOutcome::Present);
            if outcome == StoreOutcome::Present && !needed(store, cache) {
                break;
            }
        }
    }
    Ok(progress)
}
