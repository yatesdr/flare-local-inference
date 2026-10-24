//! The Helios tiering interface over selene's host tiers.

use flare_selene::{
    ExportChain, ExportId, ExportRefusal, HostHit, HostStore, InboundTicket, PublishedCheckpoint,
    Scratch, StoreOutcome, StoreStats, Tier, pool::LookupTicket,
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

    // The hooks a pooled store drives on its tiers. Each must reach them: a default here
    // would leave the tiers announcing nothing, never exporting and never spilling.

    fn announce_to(&mut self, announcer: flare_selene::pool::Announcer) {
        if let Some(tiers) = &mut self.0 {
            tiers.announce_to(announcer);
        }
    }

    fn pin_export(
        &mut self,
        tier: flare_selene::pool::Tier,
        key: PrefixKey,
        generation: u64,
        begin_tokens: u64,
        want_row: bool,
    ) -> std::result::Result<Box<dyn flare_selene::pool::HostPin>, ExportRefusal> {
        match &mut self.0 {
            Some(tiers) => tiers.pin_export(tier, key, generation, begin_tokens, want_row),
            None => Err(ExportRefusal::NotFound),
        }
    }

    fn probe(
        &mut self,
        tier: Tier,
        scratch: &Scratch,
        pages: usize,
        cold: bool,
    ) -> flare_selene::Result<Option<std::time::Duration>> {
        match &mut self.0 {
            Some(tiers) => tiers.probe(tier, scratch, pages, cold),
            None => Ok(None),
        }
    }

    fn cascade(&mut self) {
        if let Some(tiers) = &mut self.0 {
            tiers.cascade();
        }
    }

    fn set_spill(&mut self, enabled: bool) {
        if let Some(tiers) = &mut self.0 {
            tiers.set_spill(enabled);
        }
    }

    fn take_retiring(&mut self) -> Vec<flare_selene::Retiring> {
        self.0
            .as_mut()
            .map(|tiers| tiers.take_retiring())
            .unwrap_or_default()
    }

    fn retired(
        &mut self,
        key: PrefixKey,
        tier: flare_selene::pool::Tier,
        how: flare_selene::Retirement,
    ) {
        if let Some(tiers) = &mut self.0 {
            tiers.retired(key, tier, how);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Records which pool hooks reached it.
    #[derive(Clone, Default)]
    struct Hooks(Arc<Mutex<Vec<String>>>);

    impl HostStore for Hooks {
        fn lookup(&mut self, _: &dyn flare_selene::History, _: u64, _: u64) -> Option<HostHit> {
            None
        }
        fn store(&mut self, _: &PublishedCheckpoint) -> flare_selene::Result<StoreOutcome> {
            Ok(StoreOutcome::Present)
        }
        fn fetch(&mut self, _: HostHit, _: InboundTicket) -> flare_selene::Result<()> {
            Ok(())
        }
        fn poll(&mut self, _: &mut Vec<flare_selene::StoreEvent>) -> flare_selene::Result<()> {
            Ok(())
        }
        fn stats(&self) -> StoreStats {
            StoreStats::default()
        }
        fn close(&mut self) -> flare_selene::Result<()> {
            Ok(())
        }
        fn cascade(&mut self) {
            self.0.lock().unwrap().push("cascade".into());
        }
        fn set_spill(&mut self, enabled: bool) {
            self.0.lock().unwrap().push(format!("spill {enabled}"));
        }
        fn take_retiring(&mut self) -> Vec<flare_selene::Retiring> {
            self.0.lock().unwrap().push("take_retiring".into());
            Vec::new()
        }
        fn retired(
            &mut self,
            _: PrefixKey,
            tier: flare_selene::pool::Tier,
            how: flare_selene::Retirement,
        ) {
            self.0
                .lock()
                .unwrap()
                .push(format!("retired {tier:?} {how:?}"));
        }
        fn pin_export(
            &mut self,
            tier: flare_selene::pool::Tier,
            _: PrefixKey,
            generation: u64,
            _: u64,
            _: bool,
        ) -> std::result::Result<Box<dyn flare_selene::pool::HostPin>, ExportRefusal> {
            self.0
                .lock()
                .unwrap()
                .push(format!("pin {tier:?} {generation}"));
            Err(ExportRefusal::GenerationChanged)
        }
        fn probe(
            &mut self,
            tier: Tier,
            _: &Scratch,
            pages: usize,
            _: bool,
        ) -> flare_selene::Result<Option<std::time::Duration>> {
            self.0
                .lock()
                .unwrap()
                .push(format!("probe {tier:?} {pages}"));
            Ok(Some(std::time::Duration::from_micros(7)))
        }
    }

    #[test]
    fn the_tiers_beneath_a_pooled_store_receive_every_pool_hook() {
        let hooks = Hooks::default();
        let mut tiers = Tiers::new(Some(Box::new(hooks.clone())));
        let key = PrefixKey {
            consumed_tokens: 64,
            digest: [1; 16],
        };
        tiers.cascade();
        tiers.set_spill(true);
        assert!(tiers.take_retiring().is_empty());
        tiers.retired(
            key,
            flare_selene::pool::Tier::Dram,
            flare_selene::Retirement::Moved,
        );
        assert!(matches!(
            tiers.pin_export(flare_selene::pool::Tier::Dram, key, 9, 0, true),
            Err(ExportRefusal::GenerationChanged)
        ));
        let scratch = Scratch {
            pages: Vec::new(),
            slot: flare_selene::StateSlotId(1),
        };
        assert_eq!(
            tiers.probe(Tier::Memory, &scratch, 4, false).unwrap(),
            Some(std::time::Duration::from_micros(7))
        );
        assert_eq!(
            *hooks.0.lock().unwrap(),
            [
                "cascade",
                "spill true",
                "take_retiring",
                "retired Dram Moved",
                "pin Dram 9",
                "probe Memory 4"
            ]
        );
        // With no tiers there is nothing to reach.
        let mut none = Tiers::new(None);
        none.cascade();
        assert!(none.take_retiring().is_empty());
        assert!(matches!(
            none.pin_export(flare_selene::pool::Tier::Dram, key, 9, 0, true),
            Err(ExportRefusal::NotFound)
        ));
        assert_eq!(none.probe(Tier::Memory, &scratch, 4, false).unwrap(), None);
    }
}
