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
    assert!(characterizer.update(script));
    characterizer.ranking().clone()
}

fn sized(level: Level, us_per_page: [f64; 3]) -> Ranking {
    let mut ranking = Ranking::default();
    let cost = |us_per_page| {
        Some(Cost {
            us_per_page,
            spread: 0.0,
        })
    };
    ranking.measured[level.index()] = Measured {
        small: cost(us_per_page[0]),
        typical: cost(us_per_page[1]),
        long: cost(us_per_page[2]),
    };
    ranking
}

#[test]
fn a_fixed_cost_per_transfer_is_a_latency_and_not_a_cost_per_page() {
    // 14 ms of latency and 0.3 ms a page, timed at 2, 16 and 64 pages.
    let line = |pages: f64| 14_000.0 + 300.0 * pages;
    let ranking = sized(
        HostDram,
        [line(2.0) / 2.0, line(16.0) / 16.0, line(64.0) / 64.0],
    );
    let (latency, per_page) = ranking.cost_line(HostDram).unwrap();
    assert!((latency - 14_000.0).abs() < 1.0 && (per_page - 300.0).abs() < 0.1);
    // The first long restore costs what the line says, not the few-page average.
    let seconds = ranking.part_seconds(HostDram, 526, 0).unwrap();
    assert!((seconds - line(526.0) / 1e6).abs() < 1e-3, "{seconds}");
    let few_page_average = 526.0 * line(2.0) / 2.0 / 1e6;
    assert!(seconds < few_page_average / 10.0);
    // A source's own estimate, when it gives one, is what its claims cost.
    let observed = ranking.part_seconds(HostDram, 526, 100).unwrap();
    assert!((observed - 526.0 * 100.0 / 1e6).abs() < 1e-9);
    // Nothing measured and nothing observed: no estimate; one observation is enough.
    assert_eq!(ranking.part_seconds(HostNvme, 526, 0), None);
    assert!((ranking.part_seconds(HostNvme, 10, 50).unwrap() - 500e-6).abs() < 1e-9);
}

#[test]
fn noise_cannot_give_a_negative_latency_or_slope() {
    // Cheaper per page at the small size: all per page.
    let ranking = sized(HostDram, [100.0, 200.0, 400.0]);
    let (latency, per_page) = ranking.cost_line(HostDram).unwrap();
    assert!(latency >= 0.0 && per_page > 0.0, "{latency} {per_page}");
    // Cheaper at the long size than a latency alone would give: flat.
    let ranking = sized(HostDram, [5_000.0, 1_000.0, 100.0]);
    let (latency, per_page) = ranking.cost_line(HostDram).unwrap();
    assert!(latency >= 0.0 && per_page >= 0.0);
    // One size is proportional, as before.
    let mut ranking = Ranking::default();
    ranking.measured[HostDram.index()].typical = Some(Cost {
        us_per_page: 50.0,
        spread: 0.0,
    });
    assert_eq!(ranking.cost_line(HostDram), Some((0.0, 50.0)));
}

use Level::*;

#[test]
fn observed_restores_rank_by_a_rolling_median_per_page_without_replacing_cost_fits() {
    let mut ranking = sized(HostDram, [10.0; 3]);
    let fit = ranking.cost_line(HostDram);
    for (pages, per_page) in [(2, 30), (16, 10), (64, 12)] {
        ranking.observe(
            NeighbourGpu,
            pages,
            Duration::from_micros(pages as u64 * per_page),
        );
    }
    // Median 12, spread 20: default order wins the near tie with DRAM at 10.
    assert_eq!(ranking.order(), &Level::DEFAULT_ORDER);
    for _ in 0..3 {
        ranking.observe(NeighbourGpu, 16, Duration::from_micros(16 * 50));
    }
    assert_eq!(ranking.order()[1], HostDram);
    assert_eq!(ranking.cost_line(HostDram), fit);
    assert_eq!(ranking.observed[NeighbourGpu.index()], [50.0; 3]);
    // New observations replace the previous window and can reverse the ranking.
    for _ in 0..3 {
        ranking.observe(NeighbourGpu, 16, Duration::from_micros(16));
    }
    assert_eq!(ranking.order(), &Level::DEFAULT_ORDER);
}

#[test]
fn observed_medians_ignore_an_outlier_and_rank_previously_unmeasured_levels() {
    let mut ranking = Ranking::default();
    for per_page in [100, 100, 100] {
        ranking.observe(NeighbourGpu, 2, Duration::from_micros(2 * per_page));
    }
    for per_page in [90, 1, 2] {
        ranking.observe(HostDram, 16, Duration::from_micros(16 * per_page));
    }
    // Median 2 and spread 89 are clearly below 100; the mean would not reorder.
    assert_eq!(ranking.order()[1], HostDram);
}

#[test]
fn observations_keep_a_pinned_order_and_ignore_row_only_restores() {
    let order = vec![ThisGpu, RemoteNvme, HostNvme, HostDram, NeighbourGpu];
    let mut ranking = Characterizer::new(PAGES, Some(order.clone()))
        .unwrap()
        .ranking;
    for _ in 0..3 {
        ranking.observe(NeighbourGpu, 16, Duration::from_micros(16));
        ranking.observe(RemoteNvme, 16, Duration::from_micros(1600));
    }
    assert_eq!(ranking.order().to_vec(), order);
    let before = ranking.clone();
    ranking.observe(HostDram, 0, Duration::from_micros(1));
    ranking.observe(HostDram, 16, Duration::ZERO);
    ranking.observe(ThisGpu, 16, Duration::from_micros(1));
    assert_eq!(ranking, before);
}

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
    let mut script = Script::fixed(&[(NeighbourGpu, 300.0), (HostNvme, 10.0), (RemoteNvme, 100.0)]);
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
    characterizer.update(&mut probe);
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
    characterizer.update(&mut script);
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
fn measures_only_once() {
    let mut script = Script::fixed(&[
        (NeighbourGpu, 1.0),
        (HostDram, 1.0),
        (HostNvme, 1.0),
        (RemoteNvme, 1.0),
    ]);
    let mut characterizer = Characterizer::new(PAGES, None).unwrap();
    assert!(characterizer.update(&mut script));
    let calls = script.calls.len();
    assert!(!characterizer.update(&mut script));
    assert_eq!(script.calls.len(), calls);
    assert!(!characterizer.update(&mut script));
    assert_eq!(script.calls.len(), calls);
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
