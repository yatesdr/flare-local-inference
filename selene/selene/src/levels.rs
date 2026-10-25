//! Fixed copy priority and costs observed from completed restores.
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Level {
    ThisGpu,
    NeighbourGpu,
    HostDram,
    HostNvme,
    RemoteNvme,
}
impl Level {
    pub const DEFAULT_ORDER: [Level; 5] = [
        Self::ThisGpu,
        Self::NeighbourGpu,
        Self::HostDram,
        Self::HostNvme,
        Self::RemoteNvme,
    ];
    fn index(self) -> usize {
        Self::DEFAULT_ORDER
            .iter()
            .position(|level| *level == self)
            .unwrap()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Topology(pub u64);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ranking {
    observed: [Vec<f64>; 5],
}
impl Ranking {
    pub fn order(&self) -> &[Level; 5] {
        &Level::DEFAULT_ORDER
    }
    pub fn rank(&self, level: Level) -> usize {
        level.index()
    }
    pub(crate) fn observe(&mut self, level: Level, pages: usize, elapsed: Duration) {
        if level == Level::ThisGpu || pages == 0 || elapsed.is_zero() {
            return;
        }
        let samples = &mut self.observed[level.index()];
        if samples.len() == 3 {
            samples.remove(0);
        }
        samples.push(elapsed.as_secs_f64() * 1e6 / pages as f64);
    }
    pub fn us_per_page(&self, level: Level) -> Option<f64> {
        let mut samples = self.observed[level.index()].clone();
        samples.sort_by(f64::total_cmp);
        samples.get(samples.len() / 2).copied()
    }
    pub fn part_seconds(&self, level: Level, pages: usize, reported: u32) -> Option<f64> {
        let us = if reported == 0 {
            self.us_per_page(level)?
        } else {
            f64::from(reported)
        };
        Some(pages as f64 * us / 1e6)
    }
    #[cfg(test)]
    pub(crate) fn for_test(_order: [Level; 5], costs: &[(Level, f64)]) -> Self {
        let mut ranking = Self::default();
        for (level, us) in costs {
            ranking.observed[level.index()].push(*us);
        }
        ranking
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observations_refine_costs_without_changing_copy_priority() {
        let mut rates = Ranking::default();
        for us in [4, 1, 100, 2] {
            rates.observe(Level::HostDram, 1, Duration::from_micros(us));
        }
        assert_eq!(rates.us_per_page(Level::HostDram), Some(2.0));
        assert_eq!(rates.order(), &Level::DEFAULT_ORDER);
    }
}
