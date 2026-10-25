//! The order in which restore sources are preferred.
//!
//! The default order is this GPU, a neighbour's GPU, host DRAM, host NVMe, remote
//! NVMe. At pool startup, each copy level is timed end to end through a [`Probe`]
//! at three sizes: three repetitions each, the median kept, NVMe read cold.
//! Levels rank by time per page at the typical size. A near tie keeps the
//! default order. Failed measurements keep their default positions, and a pinned
//! order wins. Completed restores refine the order without probes.
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

/// Identifies the pool client's connected peers, manager and links.
/// Equal values mean the topology did not change.
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
    /// How many pages each measured size moved.
    pages: Pages,
    /// The last three successful restore costs per level, in microseconds per page.
    observed: [Vec<f64>; 5],
}

impl Default for Ranking {
    /// The default order with nothing measured.
    fn default() -> Self {
        Self {
            order: Level::DEFAULT_ORDER,
            measured: [Measured::default(); 5],
            pinned: false,
            pages: Pages {
                small: 2,
                typical: 16,
                long: 64,
            },
            observed: Default::default(),
        }
    }
}

impl Ranking {
    /// Refine the order from real restores, preserving the startup cost fits.
    pub(crate) fn observe(&mut self, level: Level, pages: usize, elapsed: Duration) {
        if level == Level::ThisGpu || pages == 0 || elapsed.is_zero() {
            return;
        }
        let samples = &mut self.observed[level.index()];
        if samples.len() == REPETITIONS {
            samples.remove(0);
        }
        samples.push(elapsed.as_secs_f64() * 1e6 / pages as f64);
        if self.pinned {
            return;
        }
        let mut measured = self.measured;
        for (index, samples) in self.observed.iter().enumerate() {
            if samples.len() == REPETITIONS {
                let mut times = samples.clone();
                times.sort_by(f64::total_cmp);
                measured[index].typical = Some(Cost {
                    us_per_page: times[REPETITIONS / 2],
                    spread: times[REPETITIONS - 1] - times[0],
                });
            }
        }
        self.order = rank(&measured);
    }

    /// An order with typical-size costs, for tests of code that consumes a ranking.
    #[cfg(test)]
    pub(crate) fn for_test(order: [Level; 5], us_per_page: &[(Level, f64)]) -> Self {
        let mut ranking = Self {
            order,
            ..Self::default()
        };
        for (level, us) in us_per_page {
            ranking.measured[level.index()].typical = Some(Cost {
                us_per_page: *us,
                spread: 0.0,
            });
        }
        ranking
    }

    /// A ranking whose `level` was timed at the three sizes (microseconds per page).
    #[cfg(test)]
    pub(crate) fn for_test_sized(level: Level, us_per_page: [f64; 3]) -> Self {
        let cost = |us_per_page| {
            Some(Cost {
                us_per_page,
                spread: 0.0,
            })
        };
        let mut ranking = Self::default();
        ranking.measured[level.index()] = Measured {
            small: cost(us_per_page[0]),
            typical: cost(us_per_page[1]),
            long: cost(us_per_page[2]),
        };
        ranking
    }

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

    /// The latency in microseconds and the microseconds per page that fit the transfers
    /// measured at `level`, from the least-squares line through total time against pages
    /// over the sizes that were timed. One size gives no latency: all of it is per page.
    pub fn cost_line(&self, level: Level) -> Option<(f64, f64)> {
        let measured = self.measured(level);
        let points: Vec<(f64, f64)> = [
            (self.pages.small, measured.small),
            (self.pages.typical, measured.typical),
            (self.pages.long, measured.long),
        ]
        .into_iter()
        .filter_map(|(pages, cost)| {
            cost.filter(|_| pages > 0)
                .map(|cost| (pages as f64, pages as f64 * cost.us_per_page))
        })
        .collect();
        match points.as_slice() {
            [] => None,
            [(pages, total)] => Some((0.0, total / pages)),
            _ => {
                let n = points.len() as f64;
                let (sx, sy) = points
                    .iter()
                    .fold((0.0, 0.0), |(x, y), p| (x + p.0, y + p.1));
                let sxx: f64 = points.iter().map(|p| p.0 * p.0).sum();
                let sxy: f64 = points.iter().map(|p| p.0 * p.1).sum();
                let slope = (n * sxy - sx * sy) / (n * sxx - sx * sx);
                let latency = (sy - slope * sx) / n;
                // Neither can be negative: a negative slope is noise around a flat cost, a
                // negative latency a cost that is all per page.
                if slope < 0.0 {
                    Some((sy / n, 0.0))
                } else if latency < 0.0 {
                    Some((0.0, sxy / sxx))
                } else {
                    Some((latency, slope))
                }
            }
        }
    }

    /// Seconds a transfer of `pages` pages from a source at `level` is expected to take, from
    /// what the source's own claims have cost (`observed_us_per_page`, 0 when the source has
    /// no estimate worth giving) and otherwise from the line fitted to the timed transfers,
    /// which counts a fixed latency once instead of per page. `None` when neither is known.
    pub fn part_seconds(
        &self,
        level: Level,
        pages: usize,
        observed_us_per_page: u32,
    ) -> Option<f64> {
        let observed = (observed_us_per_page != 0)
            .then(|| pages as f64 * f64::from(observed_us_per_page) / 1e6);
        let fitted = self
            .cost_line(level)
            .map(|(latency, per_page)| (latency + pages as f64 * per_page) / 1e6);
        observed.or(fitted)
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

/// Measures the levels once at startup.
pub struct Characterizer {
    pages: Pages,
    pinned: Option<[Level; 5]>,
    measured: bool,
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
            measured: false,
            ranking,
        })
    }

    pub fn ranking(&self) -> &Ranking {
        &self.ranking
    }

    /// Measure and rank on the first call only. Returns whether it measured; the caller logs
    /// [`Ranking::log_line`].
    pub fn update(&mut self, probe: &mut dyn Probe) -> bool {
        if self.measured {
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
            pages: self.pages,
            observed: Default::default(),
        };
        self.measured = true;
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
mod tests;
