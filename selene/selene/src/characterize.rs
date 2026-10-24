//! The order in which restore sources are preferred.
//!
//! The default order is this GPU, a neighbour's GPU, host DRAM, host NVMe, remote
//! NVMe. At pool startup, and again only when the topology changes, each copy
//! level is timed end to end through a [`Probe`] at three sizes: three repetitions
//! each, the median kept, NVMe read cold. The levels are ranked by the time per
//! page at the typical size. A near tie keeps the default order, a level whose
//! measurement failed keeps its default position, and an operator-pinned order
//! overrides the ranking.
//!
//! This GPU is not a copy and is not measured: unless an operator pins another
//! order it stays first.

use std::time::Duration;

use crate::{Error, Result};

/// A place a restore can read from, closest in the default order first.
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
        Level::ThisGpu,
        Level::NeighbourGpu,
        Level::HostDram,
        Level::HostNvme,
        Level::RemoteNvme,
    ];

    fn name(self) -> &'static str {
        match self {
            Level::ThisGpu => "this GPU",
            Level::NeighbourGpu => "neighbour GPU",
            Level::HostDram => "host DRAM",
            Level::HostNvme => "host NVMe",
            Level::RemoteNvme => "remote NVMe",
        }
    }

    fn index(self) -> usize {
        Self::DEFAULT_ORDER
            .iter()
            .position(|level| *level == self)
            .expect("every level is in the default order")
    }

    /// NVMe levels are timed with the data not in any cache.
    fn cold(self) -> bool {
        matches!(self, Level::HostNvme | Level::RemoteNvme)
    }
}

/// How large one timed transfer is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Size {
    Small,
    Typical,
    Long,
}

const SIZES: [Size; 3] = [Size::Small, Size::Typical, Size::Long];

/// Repetitions of each timed transfer; the median is used.
pub const REPETITIONS: usize = 3;

/// Pages moved by a transfer of each size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pages {
    pub small: usize,
    pub typical: usize,
    pub long: usize,
}

impl Pages {
    fn of(self, size: Size) -> usize {
        match size {
            Size::Small => self.small,
            Size::Typical => self.typical,
            Size::Long => self.long,
        }
    }
}

/// Runs one timed transfer of a level through the real path: lookup, claim, copy,
/// checksum and upload. The pool client implements it over its page copier and
/// the manager connection. When `cold` is set the data must not be in any cache.
pub trait Probe {
    fn transfer(&mut self, level: Level, pages: usize, cold: bool) -> Result<Duration>;
}

/// Identifies the set of peers, managers and links a measurement was taken in.
/// The pool client derives it; equal values mean the topology did not change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Topology(pub u64);

/// Microseconds per page of one level at one size.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Cost {
    /// The median of the repetitions.
    pub us_per_page: f64,
    /// The slowest repetition minus the fastest.
    pub spread: f64,
}

/// The measured costs of one level; a size that failed has none.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Measured {
    pub small: Option<Cost>,
    pub typical: Option<Cost>,
    pub long: Option<Cost>,
}

/// The order in use and what it was derived from.
#[derive(Clone, Debug, PartialEq)]
pub struct Ranking {
    order: [Level; 5],
    measured: [Measured; 5],
    pinned: bool,
}

impl Default for Ranking {
    /// The default order with nothing measured.
    fn default() -> Self {
        Self {
            order: Level::DEFAULT_ORDER,
            measured: [Measured::default(); 5],
            pinned: false,
        }
    }
}

impl Ranking {
    /// Levels from closest to farthest.
    pub fn order(&self) -> &[Level; 5] {
        &self.order
    }

    /// Position in the order in use; lower is closer.
    pub fn rank(&self, level: Level) -> usize {
        self.order
            .iter()
            .position(|candidate| *candidate == level)
            .expect("every level is in the order")
    }

    pub fn measured(&self, level: Level) -> &Measured {
        &self.measured[level.index()]
    }

    /// Microseconds per page at the typical size, if it was measured.
    pub fn us_per_page(&self, level: Level) -> Option<f64> {
        self.measured(level).typical.map(|cost| cost.us_per_page)
    }

    /// The single startup log line: the order in use and how it differs from the default.
    pub fn log_line(&self) -> String {
        let order = join(&self.order);
        if self.pinned {
            return format!("restore source order: {order} (pinned by configuration)");
        }
        let unmeasured: Vec<Level> = Level::DEFAULT_ORDER[1..]
            .iter()
            .copied()
            .filter(|level| self.us_per_page(*level).is_none())
            .collect();
        let mut line = if self.order == Level::DEFAULT_ORDER {
            format!("restore source order: {order} (default order)")
        } else {
            format!(
                "restore source order: {order} (changed from the default {})",
                join(&Level::DEFAULT_ORDER)
            )
        };
        if !unmeasured.is_empty() {
            line.push_str(&format!("; not measured: {}", join(&unmeasured)));
        }
        line
    }
}

fn join(levels: &[Level]) -> String {
    let names: Vec<&str> = levels.iter().map(|level| level.name()).collect();
    names.join(" > ")
}

/// Measures the levels and keeps the order until the topology changes.
pub struct Characterizer {
    pages: Pages,
    pinned: Option<[Level; 5]>,
    topology: Option<Topology>,
    ranking: Ranking,
}

impl Characterizer {
    /// `pinned`, when given, must name every level once; it is the order in use
    /// whatever the measurements say.
    pub fn new(pages: Pages, pinned: Option<Vec<Level>>) -> Result<Self> {
        let pinned = pinned.map(pin).transpose()?;
        let ranking = Ranking {
            pinned: pinned.is_some(),
            order: pinned.unwrap_or(Level::DEFAULT_ORDER),
            ..Ranking::default()
        };
        Ok(Self {
            pages,
            pinned,
            topology: None,
            ranking,
        })
    }

    pub fn ranking(&self) -> &Ranking {
        &self.ranking
    }

    /// Measure and rank if this is the first call or `topology` differs from the
    /// last measured one. Returns whether it measured; the caller then logs
    /// [`Ranking::log_line`].
    pub fn update(&mut self, topology: Topology, probe: &mut dyn Probe) -> bool {
        if self.topology == Some(topology) {
            return false;
        }
        let mut measured = [Measured::default(); 5];
        for level in &Level::DEFAULT_ORDER[1..] {
            measured[level.index()] = measure(*level, self.pages, probe);
        }
        self.ranking = Ranking {
            order: self.pinned.unwrap_or_else(|| rank(&measured)),
            measured,
            pinned: self.pinned.is_some(),
        };
        self.topology = Some(topology);
        true
    }
}

fn pin(levels: Vec<Level>) -> Result<[Level; 5]> {
    let order: [Level; 5] = levels
        .try_into()
        .map_err(|_| Error::new("a pinned order must name all five levels"))?;
    for level in Level::DEFAULT_ORDER {
        if order
            .iter()
            .filter(|candidate| **candidate == level)
            .count()
            != 1
        {
            return Err(Error::new(format!(
                "a pinned order must name {} exactly once",
                level.name()
            )));
        }
    }
    Ok(order)
}

/// Time one level at every size. A size with a failed repetition has no cost, and
/// the level stops at its first failure.
fn measure(level: Level, pages: Pages, probe: &mut dyn Probe) -> Measured {
    let mut measured = Measured::default();
    for size in SIZES {
        let count = pages.of(size);
        let Some(cost) = measure_size(level, count, probe) else {
            break;
        };
        match size {
            Size::Small => measured.small = Some(cost),
            Size::Typical => measured.typical = Some(cost),
            Size::Long => measured.long = Some(cost),
        }
    }
    measured
}

fn measure_size(level: Level, pages: usize, probe: &mut dyn Probe) -> Option<Cost> {
    if pages == 0 {
        return None;
    }
    let mut times = Vec::with_capacity(REPETITIONS);
    for _ in 0..REPETITIONS {
        let elapsed = probe.transfer(level, pages, level.cold()).ok()?;
        times.push(elapsed.as_secs_f64() * 1e6 / pages as f64);
    }
    times.sort_by(f64::total_cmp);
    Some(Cost {
        us_per_page: times[REPETITIONS / 2],
        spread: times[REPETITIONS - 1] - times[0],
    })
}

/// The order from measurements at the typical size.
///
/// A level without a typical measurement keeps its default position. The others
/// take the remaining positions in the default order and then insertion-sort: a
/// level moves ahead of a neighbour only when that neighbour is slower by more
/// than the larger of the two spreads, so a near tie never reorders.
fn rank(measured: &[Measured; 5]) -> [Level; 5] {
    let cost = |level: Level| measured[level.index()].typical;
    let mut order = Level::DEFAULT_ORDER;
    let slots: Vec<usize> = (0..5)
        .filter(|slot| *slot != 0 && cost(order[*slot]).is_some())
        .collect();
    let mut ranked: Vec<Level> = Vec::new();
    for slot in &slots {
        let level = order[*slot];
        let mut at = ranked.len();
        while at > 0 && clearly_slower(cost(ranked[at - 1]).unwrap(), cost(level).unwrap()) {
            at -= 1;
        }
        ranked.insert(at, level);
    }
    for (slot, level) in slots.into_iter().zip(ranked) {
        order[slot] = level;
    }
    order
}

fn clearly_slower(a: Cost, b: Cost) -> bool {
    a.us_per_page - b.us_per_page > a.spread.max(b.spread)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const PAGES: Pages = Pages {
        small: 1,
        typical: 100,
        long: 1000,
    };

    /// Per-page microseconds per level, per repetition; one entry per call.
    #[derive(Default)]
    struct Script {
        us_per_page: HashMap<Level, Vec<f64>>,
        failing: Vec<Level>,
        calls: Vec<(Level, usize, bool)>,
    }

    impl Script {
        fn fixed(costs: &[(Level, f64)]) -> Self {
            Self {
                us_per_page: costs.iter().map(|(l, c)| (*l, vec![*c])).collect(),
                ..Self::default()
            }
        }
    }

    impl Probe for Script {
        fn transfer(&mut self, level: Level, pages: usize, cold: bool) -> Result<Duration> {
            self.calls.push((level, pages, cold));
            if self.failing.contains(&level) {
                return Err("probe failed".into());
            }
            let series = self.us_per_page.get_mut(&level).ok_or("no script")?;
            let us = if series.len() > 1 {
                series.remove(0)
            } else {
                series[0]
            };
            Ok(Duration::from_secs_f64(us * pages as f64 / 1e6))
        }
    }

    fn measured(script: &mut Script) -> Ranking {
        let mut characterizer = Characterizer::new(PAGES, None).unwrap();
        assert!(characterizer.update(Topology(1), script));
        characterizer.ranking().clone()
    }

    use Level::*;

    #[test]
    fn ranks_by_the_typical_size() {
        let mut script = Script::fixed(&[
            (NeighbourGpu, 50.0),
            (HostDram, 10.0),
            (HostNvme, 200.0),
            (RemoteNvme, 100.0),
        ]);
        let ranking = measured(&mut script);
        assert_eq!(
            ranking.order(),
            &[ThisGpu, HostDram, NeighbourGpu, RemoteNvme, HostNvme]
        );
        assert_eq!(ranking.rank(HostDram), 1);
        assert_eq!(ranking.us_per_page(HostNvme), Some(200.0));
    }

    #[test]
    fn small_and_long_sizes_do_not_decide_the_rank() {
        // Dram is cheap per page only at the typical size.
        let mut script = Script::default();
        script
            .us_per_page
            .insert(NeighbourGpu, vec![5.0, 50.0, 5.0]);
        script.us_per_page.insert(HostDram, vec![1.0, 10.0, 1.0]);
        script.us_per_page.insert(HostNvme, vec![9.0, 90.0, 9.0]);
        script.us_per_page.insert(RemoteNvme, vec![9.0, 90.0, 9.0]);
        // Each size repeats three times, so spell the series out per repetition.
        for series in script.us_per_page.values_mut() {
            *series = [series[0], series[1], series[2]]
                .iter()
                .flat_map(|v| [*v; 3])
                .collect();
        }
        let ranking = measured(&mut script);
        assert_eq!(ranking.order()[1], HostDram);
    }

    #[test]
    fn runs_three_repetitions_at_three_sizes_and_reads_nvme_cold() {
        let mut script = Script::fixed(&[
            (NeighbourGpu, 1.0),
            (HostDram, 1.0),
            (HostNvme, 1.0),
            (RemoteNvme, 1.0),
        ]);
        measured(&mut script);
        assert_eq!(script.calls.len(), 4 * 3 * 3);
        assert!(!script.calls.iter().any(|(level, _, _)| *level == ThisGpu));
        for level in [NeighbourGpu, HostDram, HostNvme, RemoteNvme] {
            let sizes: Vec<usize> = script
                .calls
                .iter()
                .filter(|(l, _, _)| *l == level)
                .map(|(_, pages, _)| *pages)
                .collect();
            assert_eq!(sizes, [1, 1, 1, 100, 100, 100, 1000, 1000, 1000]);
        }
        for (level, _, cold) in &script.calls {
            assert_eq!(*cold, matches!(level, HostNvme | RemoteNvme));
        }
    }

    #[test]
    fn the_median_of_three_is_used_and_the_spread_is_the_range() {
        let mut script = Script::fixed(&[
            (NeighbourGpu, 1.0),
            (HostDram, 1.0),
            (HostNvme, 1.0),
            (RemoteNvme, 1.0),
        ]);
        // Typical-size repetitions (calls 4 to 6): 30, 10 and 12 per page.
        script.us_per_page.insert(
            HostDram,
            vec![1.0, 1.0, 1.0, 30.0, 10.0, 12.0, 1.0, 1.0, 1.0],
        );
        let ranking = measured(&mut script);
        let cost = ranking.measured(HostDram).typical.unwrap();
        assert!((cost.us_per_page - 12.0).abs() < 1e-6);
        assert!((cost.spread - 20.0).abs() < 1e-6);
    }

    #[test]
    fn a_near_tie_keeps_the_default_order() {
        // Dram is faster than the neighbour, but within the larger spread.
        let mut script = Script::fixed(&[
            (NeighbourGpu, 10.0),
            (HostDram, 9.0),
            (HostNvme, 500.0),
            (RemoteNvme, 900.0),
        ]);
        script.us_per_page.insert(
            NeighbourGpu,
            vec![10.0, 10.0, 10.0, 8.0, 10.0, 12.0, 10.0, 10.0, 10.0],
        );
        let ranking = measured(&mut script);
        assert_eq!(ranking.order(), &Level::DEFAULT_ORDER);
        // Without the spread the same means reorder.
        let mut clear = Script::fixed(&[
            (NeighbourGpu, 10.0),
            (HostDram, 5.0),
            (HostNvme, 500.0),
            (RemoteNvme, 900.0),
        ]);
        assert_eq!(measured(&mut clear).order()[1], HostDram);
    }

    #[test]
    fn a_level_that_failed_keeps_its_default_position() {
        // Host DRAM fails, so it stays third; the others sort around it.
        let mut script =
            Script::fixed(&[(NeighbourGpu, 300.0), (HostNvme, 10.0), (RemoteNvme, 100.0)]);
        script.failing.push(HostDram);
        let ranking = measured(&mut script);
        assert_eq!(
            ranking.order(),
            &[ThisGpu, HostNvme, HostDram, RemoteNvme, NeighbourGpu]
        );
        assert_eq!(ranking.us_per_page(HostDram), None);
    }

    #[test]
    fn a_failure_at_the_typical_size_leaves_the_level_unranked() {
        let mut script = Script::fixed(&[
            (NeighbourGpu, 1.0),
            (HostDram, 1.0),
            (HostNvme, 1.0),
            (RemoteNvme, 1.0),
        ]);
        // Small succeeds, the first typical repetition fails.
        struct FailTypical(Script);
        impl Probe for FailTypical {
            fn transfer(&mut self, level: Level, pages: usize, cold: bool) -> Result<Duration> {
                if level == NeighbourGpu && pages == PAGES.typical {
                    return Err("link down".into());
                }
                self.0.transfer(level, pages, cold)
            }
        }
        let mut characterizer = Characterizer::new(PAGES, None).unwrap();
        let mut probe = FailTypical(std::mem::take(&mut script));
        characterizer.update(Topology(1), &mut probe);
        let ranking = characterizer.ranking();
        assert!(ranking.measured(NeighbourGpu).small.is_some());
        assert_eq!(ranking.us_per_page(NeighbourGpu), None);
        assert_eq!(ranking.measured(NeighbourGpu).long, None);
        assert_eq!(ranking.rank(NeighbourGpu), 1);
    }

    #[test]
    fn a_pinned_order_wins_over_the_measurements() {
        let pinned = vec![ThisGpu, RemoteNvme, HostNvme, HostDram, NeighbourGpu];
        let mut script = Script::fixed(&[
            (NeighbourGpu, 1.0),
            (HostDram, 2.0),
            (HostNvme, 3.0),
            (RemoteNvme, 4.0),
        ]);
        let mut characterizer = Characterizer::new(PAGES, Some(pinned.clone())).unwrap();
        characterizer.update(Topology(1), &mut script);
        assert_eq!(characterizer.ranking().order().to_vec(), pinned);
        assert_eq!(characterizer.ranking().us_per_page(HostDram), Some(2.0));
        assert!(characterizer.ranking().log_line().contains("pinned"));
    }

    #[test]
    fn a_pinned_order_must_name_every_level_once() {
        assert!(Characterizer::new(PAGES, Some(vec![ThisGpu, HostDram])).is_err());
        let doubled = vec![ThisGpu, HostDram, HostDram, HostNvme, RemoteNvme];
        assert!(Characterizer::new(PAGES, Some(doubled)).is_err());
    }

    #[test]
    fn measures_again_only_when_the_topology_changes() {
        let mut script = Script::fixed(&[
            (NeighbourGpu, 1.0),
            (HostDram, 1.0),
            (HostNvme, 1.0),
            (RemoteNvme, 1.0),
        ]);
        let mut characterizer = Characterizer::new(PAGES, None).unwrap();
        assert!(characterizer.update(Topology(1), &mut script));
        let calls = script.calls.len();
        assert!(!characterizer.update(Topology(1), &mut script));
        assert_eq!(script.calls.len(), calls);
        assert!(characterizer.update(Topology(2), &mut script));
        assert_eq!(script.calls.len(), 2 * calls);
    }

    #[test]
    fn the_log_line_reports_the_order_and_its_difference_from_the_default() {
        let default = Ranking::default().log_line();
        assert_eq!(
            default,
            "restore source order: this GPU > neighbour GPU > host DRAM > host NVMe > remote NVMe (default order); not measured: neighbour GPU > host DRAM > host NVMe > remote NVMe"
        );
        let mut script = Script::fixed(&[
            (NeighbourGpu, 50.0),
            (HostDram, 10.0),
            (HostNvme, 200.0),
            (RemoteNvme, 100.0),
        ]);
        let line = measured(&mut script).log_line();
        assert_eq!(
            line,
            "restore source order: this GPU > host DRAM > neighbour GPU > remote NVMe > host NVMe (changed from the default this GPU > neighbour GPU > host DRAM > host NVMe > remote NVMe)"
        );
        let mut same = Script::fixed(&[
            (NeighbourGpu, 1.0),
            (HostDram, 100.0),
            (HostNvme, 1000.0),
            (RemoteNvme, 10000.0),
        ]);
        assert!(measured(&mut same).log_line().ends_with("(default order)"));
    }
}
