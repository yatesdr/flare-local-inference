//! The Helios tiering interface over selene's host tiers.

use flare_selene::{
    HostHit, HostStore, InboundTicket, PublishedCheckpoint, StoreOutcome, StoreStats,
};

use super::{PrefixStore, StoreEvent};
use crate::{Result, cache::PrefixHistory};

/// Drives a selene store as the worker's [`PrefixStore`].
pub(super) struct Host {
    store: Box<dyn HostStore>,
    /// Event scratch reused across polls.
    events: Vec<flare_selene::StoreEvent>,
}

impl Host {
    pub(super) fn new(store: Box<dyn HostStore>) -> Self {
        Self {
            store,
            events: Vec::new(),
        }
    }
}

impl PrefixStore for Host {
    fn lookup(&mut self, history: &PrefixHistory, resident: u64, before: u64) -> Option<HostHit> {
        self.store.lookup(history, resident, before)
    }

    fn store(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
        Ok(self.store.store(checkpoint)?)
    }

    fn fetch(&mut self, hit: HostHit, ticket: InboundTicket) -> Result<()> {
        Ok(self.store.fetch(hit, ticket)?)
    }

    fn poll(&mut self, events: &mut Vec<StoreEvent>) -> Result<()> {
        let polled = self.store.poll(&mut self.events);
        events.extend(self.events.drain(..).filter_map(|event| match event {
            flare_selene::StoreEvent::Stored {
                checkpoint,
                key,
                result,
            } => Some(StoreEvent::Stored {
                checkpoint,
                key,
                result: result.map_err(Into::into),
            }),
            flare_selene::StoreEvent::Fetched { key, result } => Some(StoreEvent::Fetched {
                key,
                result: result.map_err(Into::into),
            }),
            flare_selene::StoreEvent::Deferred { checkpoint } => {
                Some(StoreEvent::Deferred { checkpoint })
            }
            // Only a store attached to a pool produces export events, and the
            // worker does not serve exports yet.
            flare_selene::StoreEvent::ExportRequested { .. }
            | flare_selene::StoreEvent::ExportEnded { .. } => None,
        }));
        Ok(polled?)
    }

    fn stats(&self) -> StoreStats {
        self.store.stats()
    }

    fn close(&mut self) -> Result<()> {
        Ok(self.store.close()?)
    }
}
