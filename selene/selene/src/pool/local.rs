use crate::{
    ExportRefusal, HostHit, HostStore, InboundTicket, PrefixKey, PublishedCheckpoint, Scratch,
    StoreOutcome, StoreStats, Tier,
};

/// The local tiers beneath a pooled store. Without any, nothing is retained
/// locally and every restore comes from the pool.
pub struct Tiers(Option<Box<dyn HostStore>>);

impl Tiers {
    pub fn new(tiers: Option<Box<dyn HostStore>>) -> Self {
        Self(tiers)
    }
}

impl HostStore for Tiers {
    fn lookup(
        &mut self,
        history: &dyn crate::History,
        resident: u64,
        before: u64,
    ) -> Option<HostHit> {
        self.0.as_mut()?.lookup(history, resident, before)
    }

    fn store(&mut self, checkpoint: &PublishedCheckpoint) -> crate::Result<StoreOutcome> {
        match &mut self.0 {
            Some(tiers) => tiers.store(checkpoint),
            None => Ok(StoreOutcome::Present),
        }
    }

    fn write_back(&mut self, checkpoint: &PublishedCheckpoint) -> crate::Result<StoreOutcome> {
        match &mut self.0 {
            Some(tiers) => tiers.write_back(checkpoint),
            None => Ok(StoreOutcome::Present),
        }
    }

    fn fetch(&mut self, hit: HostHit, ticket: InboundTicket) -> crate::Result<()> {
        match &mut self.0 {
            Some(tiers) => tiers.fetch(hit, ticket),
            None => Err(crate::Error::new("no local tier holds this record")),
        }
    }

    fn poll(&mut self, events: &mut Vec<crate::StoreEvent>) -> crate::Result<()> {
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

    fn close(&mut self) -> crate::Result<()> {
        match &mut self.0 {
            Some(tiers) => tiers.close(),
            None => Ok(()),
        }
    }

    // The hooks a pooled store drives on its tiers. Each must reach them: a default here
    // would leave the tiers announcing nothing, never exporting and never spilling.

    fn announce_to(&mut self, announcer: crate::pool::Announcer) {
        if let Some(tiers) = &mut self.0 {
            tiers.announce_to(announcer);
        }
    }

    fn pin_export(
        &mut self,
        tier: crate::pool::Tier,
        key: PrefixKey,
        generation: u64,
        begin_tokens: u64,
        want_row: bool,
    ) -> std::result::Result<Box<dyn crate::pool::HostPin>, ExportRefusal> {
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
    ) -> crate::Result<Option<std::time::Duration>> {
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

    fn take_retiring(&mut self) -> Vec<crate::Retiring> {
        self.0
            .as_mut()
            .map(|tiers| tiers.take_retiring())
            .unwrap_or_default()
    }

    fn retired(&mut self, key: PrefixKey, tier: crate::pool::Tier, how: crate::Retirement) {
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
        fn lookup(&mut self, _: &dyn crate::History, _: u64, _: u64) -> Option<HostHit> {
            None
        }
        fn store(&mut self, _: &PublishedCheckpoint) -> crate::Result<StoreOutcome> {
            Ok(StoreOutcome::Present)
        }
        fn fetch(&mut self, _: HostHit, _: InboundTicket) -> crate::Result<()> {
            Ok(())
        }
        fn poll(&mut self, _: &mut Vec<crate::StoreEvent>) -> crate::Result<()> {
            Ok(())
        }
        fn stats(&self) -> StoreStats {
            StoreStats::default()
        }
        fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
        fn cascade(&mut self) {
            self.0.lock().unwrap().push("cascade".into());
        }
        fn set_spill(&mut self, enabled: bool) {
            self.0.lock().unwrap().push(format!("spill {enabled}"));
        }
        fn take_retiring(&mut self) -> Vec<crate::Retiring> {
            self.0.lock().unwrap().push("take_retiring".into());
            Vec::new()
        }
        fn retired(&mut self, _: PrefixKey, tier: crate::pool::Tier, how: crate::Retirement) {
            self.0
                .lock()
                .unwrap()
                .push(format!("retired {tier:?} {how:?}"));
        }
        fn pin_export(
            &mut self,
            tier: crate::pool::Tier,
            _: PrefixKey,
            generation: u64,
            _: u64,
            _: bool,
        ) -> std::result::Result<Box<dyn crate::pool::HostPin>, ExportRefusal> {
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
        ) -> crate::Result<Option<std::time::Duration>> {
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
        tiers.retired(key, crate::pool::Tier::Dram, crate::Retirement::Moved);
        assert!(matches!(
            tiers.pin_export(crate::pool::Tier::Dram, key, 9, 0, true),
            Err(ExportRefusal::GenerationChanged)
        ));
        let scratch = Scratch {
            pages: Vec::new(),
            slot: crate::StateSlotId(1),
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
            none.pin_export(crate::pool::Tier::Dram, key, 9, 0, true),
            Err(ExportRefusal::NotFound)
        ));
        assert_eq!(none.probe(Tier::Memory, &scratch, 4, false).unwrap(), None);
    }
}
