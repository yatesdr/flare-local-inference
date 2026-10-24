//! The Helios tiering interface over selene's host tiers.

use flare_selene::{
    ExportChain, ExportId, ExportRefusal, HostHit, HostStore, InboundTicket, PublishedCheckpoint,
    Scratch, StoreOutcome, StoreStats, pool::LookupTicket,
};

use super::{PrefixStore, StoreEvent};
use crate::{
    Result,
    cache::{PrefixHistory, PrefixKey},
};

/// Drives a selene store as the worker's [`PrefixStore`].
pub(super) struct Host {
    store: Box<dyn HostStore>,
    pooled: bool,
    /// The pool's dedicated copier, whose in-flight jobs bound when scratch ids are idle.
    pool_copier: Option<flare_selene::SharedCopier>,
    /// Event scratch reused across polls.
    events: Vec<flare_selene::StoreEvent>,
}

impl Host {
    pub(super) fn with_pool_copier(mut self, copier: flare_selene::SharedCopier) -> Self {
        self.pool_copier = Some(copier);
        self
    }

    pub(super) fn new(store: Box<dyn HostStore>, pooled: bool) -> Self {
        Self {
            store,
            pooled,
            pool_copier: None,
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

    fn write_back(&mut self, checkpoint: &PublishedCheckpoint) -> Result<StoreOutcome> {
        Ok(self.store.write_back(checkpoint)?)
    }

    fn fetch(&mut self, hit: HostHit, ticket: InboundTicket) -> Result<()> {
        Ok(self.store.fetch(hit, ticket)?)
    }

    fn poll(&mut self, events: &mut Vec<StoreEvent>) -> Result<()> {
        let polled = self.store.poll(&mut self.events);
        events.extend(self.events.drain(..).map(|event| match event {
            flare_selene::StoreEvent::Stored {
                checkpoint,
                key,
                result,
            } => StoreEvent::Stored {
                checkpoint,
                key,
                result: result.map_err(Into::into),
            },
            flare_selene::StoreEvent::Fetched { key, result } => StoreEvent::Fetched {
                key,
                result: result.map_err(Into::into),
            },
            flare_selene::StoreEvent::Deferred { checkpoint } => {
                StoreEvent::Deferred { checkpoint }
            }
            flare_selene::StoreEvent::ExportRequested {
                export,
                key,
                generation,
                begin_tokens,
                want_row,
                reserved_bytes,
            } => StoreEvent::ExportRequested {
                export,
                key,
                generation,
                begin_tokens,
                want_row,
                reserved_bytes,
            },
            flare_selene::StoreEvent::ExportEnded { export } => StoreEvent::ExportEnded { export },
        }));
        Ok(polled?)
    }

    fn pooled(&self) -> bool {
        self.pooled
    }

    fn begin_lookup(&mut self, history: &PrefixHistory) -> Option<LookupTicket> {
        self.store.begin_lookup(history)
    }

    fn lookup_for(
        &mut self,
        ticket: Option<&LookupTicket>,
        history: &PrefixHistory,
        resident: u64,
        before: u64,
    ) -> Option<HostHit> {
        self.store.lookup_for(ticket, history, resident, before)
    }

    fn export_pinned(&mut self, export: ExportId, chain: ExportChain) -> Result<()> {
        Ok(self.store.export_pinned(export, chain)?)
    }

    fn export_refused(&mut self, export: ExportId, reason: ExportRefusal) {
        self.store.export_refused(export, reason);
    }

    fn evicted(&mut self, key: PrefixKey, generation: u64, heir: Option<&PublishedCheckpoint>) {
        self.store.evicted(key, generation, heir);
    }

    fn evictions_lost(&mut self) {
        self.store.evictions_lost();
    }

    fn copies_drained(&self) -> bool {
        self.pool_copier.as_ref().is_none_or(|copier| {
            copier
                .lock()
                .map(|copier| copier.in_flight() == 0)
                .unwrap_or(false)
        })
    }

    fn needs_characterization(&self) -> bool {
        self.store.needs_characterization()
    }

    fn characterize(&mut self, scratch: &Scratch) -> Result<Option<String>> {
        Ok(self.store.characterize(scratch)?)
    }

    fn stats(&self) -> StoreStats {
        self.store.stats()
    }

    fn close(&mut self) -> Result<()> {
        Ok(self.store.close()?)
    }
}

/// The local tiers beneath a pooled store. Without any, nothing is retained
/// locally and every restore comes from the pool.
pub(super) struct Tiers(Option<Box<dyn HostStore>>);

impl Tiers {
    pub(super) fn new(tiers: Option<Box<dyn HostStore>>) -> Self {
        Self(tiers)
    }
}

impl HostStore for Tiers {
    fn lookup(
        &mut self,
        history: &dyn flare_selene::History,
        resident: u64,
        before: u64,
    ) -> Option<HostHit> {
        self.0.as_mut()?.lookup(history, resident, before)
    }

    fn store(&mut self, checkpoint: &PublishedCheckpoint) -> flare_selene::Result<StoreOutcome> {
        match &mut self.0 {
            Some(tiers) => tiers.store(checkpoint),
            None => Ok(StoreOutcome::Present),
        }
    }

    fn write_back(
        &mut self,
        checkpoint: &PublishedCheckpoint,
    ) -> flare_selene::Result<StoreOutcome> {
        match &mut self.0 {
            Some(tiers) => tiers.write_back(checkpoint),
            None => Ok(StoreOutcome::Present),
        }
    }

    fn fetch(&mut self, hit: HostHit, ticket: InboundTicket) -> flare_selene::Result<()> {
        match &mut self.0 {
            Some(tiers) => tiers.fetch(hit, ticket),
            None => Err(flare_selene::Error::new("no local tier holds this record")),
        }
    }

    fn poll(&mut self, events: &mut Vec<flare_selene::StoreEvent>) -> flare_selene::Result<()> {
        match &mut self.0 {
            Some(tiers) => tiers.poll(events),
            None => Ok(()),
        }
    }

    fn stats(&self) -> StoreStats {
        self.0
            .as_ref()
            .map(|tiers| tiers.stats())
            .unwrap_or_default()
    }

    fn close(&mut self) -> flare_selene::Result<()> {
        match &mut self.0 {
            Some(tiers) => tiers.close(),
            None => Ok(()),
        }
    }
}
