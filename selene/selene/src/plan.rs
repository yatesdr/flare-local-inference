//! The two-source restore plan.
//!
//! A restore reads the pages after the GPU-resident prefix from at most two
//! sources. Part A is the longest prefix held at the closest level that has any
//! of it. Part B is one source at a strictly farther level, starting exactly
//! where A ends and reaching the endpoint, kept only if its pages are worth
//! fetching against recomputing them. The parts are disjoint and contiguous from
//! the resident prefix, and the endpoint's continuation row comes from whichever
//! part ends last.
//!
//! Positions are exact token counts; pages follow the floor of the token count
//! over the page size, as the index does, so a part may start in the middle of a
//! record and B may have no pages and only the row.
//!
//! This is pure logic over the candidates a lookup returned. Holding the sources,
//! copying and checksumming belong to the pool client, which rebuilds the plan
//! with [`Plan::rebuild`] when a hold fails.

use std::cmp::Reverse;

use crate::characterize::{Level, Ranking};
use crate::{Error, History, PrefixKey, Result, worth_transfer};

/// Whose cache a prefix belongs to. It is checked by the sources; the plan carries
/// it so every part is validated against the same identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub tenant: String,
    pub model: [u8; 32],
    pub layout: [u8; 16],
}

/// Names a place that holds records: a local tier, a neighbour or a manager.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SourceId(pub u64);

/// One record of a candidate's chain: its key and the pages of its own tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainNode {
    pub key: PrefixKey,
    pub tail_pages: usize,
}

/// A source that can supply the pages from `begin_tokens` to the endpoint `key`
/// and the endpoint's row, from a local tier or from a pool lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub level: Level,
    pub source: SourceId,
    pub generation: u64,
    pub key: PrefixKey,
    /// The first token this candidate supplies, exact, not necessarily page aligned.
    pub begin_tokens: u64,
    /// Pages of `nodes[0]`'s tail before the page that holds `begin_tokens`.
    pub skip_pages: usize,
    /// Records from the one holding `begin_tokens` up to the endpoint, ascending.
    pub nodes: Vec<ChainNode>,
    /// The source's measured microseconds per page; 0 when unknown.
    pub us_per_page: u32,
}

/// What the plan is built for.
pub struct Request<'a> {
    pub history: &'a dyn History,
    pub identity: &'a Identity,
    pub page_tokens: u64,
    /// Tokens already in GPU memory.
    pub resident: u64,
    /// The endpoint must be strictly below this.
    pub before: u64,
    pub ranking: &'a Ranking,
    pub prefill_tokens_per_second: f64,
}

/// One source's share of the restore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    pub level: Level,
    pub source: SourceId,
    pub generation: u64,
    /// The first token this part supplies.
    pub begin_tokens: u64,
    /// The last record's key; the part supplies the tokens before it.
    pub end: PrefixKey,
    /// Pages of the first record in `chain` to skip.
    pub first_page: usize,
    /// Offset into the ticket's pages where this part's pages go.
    pub dest_page: usize,
    pub pages: usize,
    /// Whether this part fetches the endpoint's continuation row.
    pub row: bool,
    /// Keys of the records read, ascending, each verified against the history.
    pub chain: Vec<PrefixKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Which {
    A,
    B,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    pub identity: Identity,
    pub resident: u64,
    pub a: Part,
    pub b: Option<Part>,
}

impl Plan {
    pub fn endpoint(&self) -> PrefixKey {
        self.b.as_ref().unwrap_or(&self.a).end
    }

    /// Pages written into the ticket, over both parts.
    pub fn pages(&self) -> usize {
        self.a.pages + self.b.as_ref().map_or(0, |b| b.pages)
    }

    /// The plan again after a hold on one part failed: the same endpoint from the
    /// remaining candidates without that part's source, or `None`, a miss. An
    /// endpoint that would end earlier cannot reuse the ticket's pages.
    pub fn rebuild(
        &self,
        request: &Request,
        candidates: &[Candidate],
        failed: Which,
    ) -> Option<Plan> {
        let part = match failed {
            Which::A => &self.a,
            Which::B => self.b.as_ref()?,
        };
        let remaining: Vec<&Candidate> = candidates
            .iter()
            .filter(|c| (c.source, c.generation) != (part.source, part.generation))
            .collect();
        search(request, &remaining, Some(self.endpoint()))
    }

    /// Check the plan's invariants against the history: contiguous from the
    /// resident prefix, disjoint destinations, one row, and verified keys.
    pub fn validate(&self, history: &dyn History, page_tokens: u64) -> Result<()> {
        let pages = |tokens: u64| (tokens / page_tokens) as usize;
        let mut begin = self.resident;
        let mut dest = 0;
        let parts = std::iter::once(&self.a).chain(self.b.as_ref());
        for (index, part) in parts.enumerate() {
            let last = index == usize::from(self.b.is_some());
            let end = part.end.consumed_tokens;
            let expected = pages(end) - pages(begin);
            let ok = part.begin_tokens == begin
                && part.dest_page == dest
                && part.pages == expected
                && part.row == last
                && end >= begin
                && history.key_at(end) == Some(part.end)
                && part.chain.last() == Some(&part.end)
                && part
                    .chain
                    .iter()
                    .all(|key| history.key_at(key.consumed_tokens) == Some(*key));
            if !ok {
                return Err(Error::new(format!("restore part {index} is inconsistent")));
            }
            begin = end;
            dest += part.pages;
        }
        Ok(())
    }
}

/// Build the plan for a request from the candidates a lookup returned, or `None`
/// when no candidate continues the resident prefix without a hole.
pub fn plan(request: &Request, candidates: &[Candidate]) -> Option<Plan> {
    let all: Vec<&Candidate> = candidates.iter().collect();
    search(request, &all, None)
}

/// With `endpoint` set, only plans ending exactly there qualify.
fn search(
    request: &Request,
    candidates: &[&Candidate],
    endpoint: Option<PrefixKey>,
) -> Option<Plan> {
    let usable: Vec<&Candidate> = candidates
        .iter()
        .copied()
        .filter(|c| {
            let end = c.key.consumed_tokens;
            end > request.resident
                && end < request.before
                && endpoint.is_none_or(|e| end <= e.consumed_tokens)
                && valid(request, c)
        })
        .collect();
    let mut firsts: Vec<&Candidate> = usable
        .iter()
        .copied()
        .filter(|c| c.begin_tokens <= request.resident)
        .collect();
    firsts.sort_by_key(|c| preference(request, c));
    for first in firsts {
        let mut a = part(request, first, request.resident, 0, true);
        if endpoint == Some(first.key) {
            return Some(request.plan(a, None));
        }
        a.row = false;
        let a_end = first.key.consumed_tokens;
        let a_rank = request.ranking.rank(first.level);
        let mut seconds: Vec<&Candidate> = usable
            .iter()
            .copied()
            .filter(|c| {
                c.key.consumed_tokens > a_end
                    && c.begin_tokens <= a_end
                    && request.ranking.rank(c.level) > a_rank
                    && endpoint.is_none_or(|e| c.key == e)
            })
            .collect();
        seconds.sort_by_key(|c| preference(request, c));
        for second in seconds {
            let b = part(request, second, a_end, a.pages, true);
            if worth(request, second, &b, a_end) {
                return Some(request.plan(a, Some(b)));
            }
        }
        if endpoint.is_none() {
            a.row = true;
            return Some(request.plan(a, None));
        }
    }
    None
}

impl Request<'_> {
    fn plan(&self, a: Part, b: Option<Part>) -> Plan {
        Plan {
            identity: self.identity.clone(),
            resident: self.resident,
            a,
            b,
        }
    }
}

/// Closest level first, then the cheapest known source, then the longest match.
fn preference(request: &Request, c: &Candidate) -> (usize, bool, u32, Reverse<u64>, u64) {
    (
        request.ranking.rank(c.level),
        c.us_per_page == 0,
        c.us_per_page,
        Reverse(c.key.consumed_tokens),
        c.source.0,
    )
}

/// Whether the part's pages beat recomputing the tokens they replace. A source
/// whose cost is unknown, by its own estimate and by measurement, is not.
fn worth(request: &Request, c: &Candidate, b: &Part, from: u64) -> bool {
    let us = match c.us_per_page {
        0 => request.ranking.us_per_page(c.level),
        measured => Some(f64::from(measured)),
    };
    us.is_some_and(|us| {
        worth_transfer(
            c.key.consumed_tokens - from,
            b.pages as f64 * us / 1e6,
            request.prefill_tokens_per_second,
        )
    })
}

/// The page where each node's tail starts, if the chain is consistent.
fn tail_starts(page_tokens: u64, c: &Candidate) -> Option<Vec<usize>> {
    let mut starts = Vec::with_capacity(c.nodes.len());
    let mut previous: Option<usize> = None;
    for node in &c.nodes {
        let end = (node.key.consumed_tokens / page_tokens) as usize;
        let start = end.checked_sub(node.tail_pages)?;
        if previous.is_some_and(|p| p != start) {
            return None;
        }
        starts.push(start);
        previous = Some(end);
    }
    Some(starts)
}

/// A candidate is a hint: its keys must be the history's and its records must
/// chain page by page from the page holding `begin_tokens` to the endpoint.
fn valid(request: &Request, c: &Candidate) -> bool {
    let pt = request.page_tokens;
    let end = c.key.consumed_tokens;
    let Some(starts) = tail_starts(pt, c).filter(|starts| !starts.is_empty()) else {
        return false;
    };
    c.level != Level::ThisGpu
        && c.begin_tokens <= end
        && end <= request.history.token_count()
        && request.history.key_at(end) == Some(c.key)
        && c.nodes.last().is_some_and(|node| node.key == c.key)
        && c.nodes
            .windows(2)
            .all(|w| w[0].key.consumed_tokens < w[1].key.consumed_tokens)
        && c.nodes
            .iter()
            .all(|n| request.history.key_at(n.key.consumed_tokens) == Some(n.key))
        && starts[0] + c.skip_pages == (c.begin_tokens / pt) as usize
}

/// The part reading `c` from token `begin`, which `c` must cover.
fn part(request: &Request, c: &Candidate, begin: u64, dest_page: usize, row: bool) -> Part {
    let pt = request.page_tokens;
    let start = (begin / pt) as usize;
    let end_page = (c.key.consumed_tokens / pt) as usize;
    let starts = tail_starts(pt, c).expect("validated candidate");
    // The record holding the first page; at the end of the chain, the last record.
    let first = c
        .nodes
        .iter()
        .position(|n| (n.key.consumed_tokens / pt) as usize > start)
        .unwrap_or(c.nodes.len() - 1);
    Part {
        level: c.level,
        source: c.source,
        generation: c.generation,
        begin_tokens: begin,
        end: c.key,
        first_page: start - starts[first],
        dest_page,
        pages: end_page - start,
        row,
        chain: c.nodes[first..].iter().map(|n| n.key).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TestHistory;
    use Level::*;

    const PT: u64 = 16;
    const PREFILL: f64 = 8000.0;

    struct Fixture {
        history: TestHistory,
        identity: Identity,
        ranking: Ranking,
    }

    impl Fixture {
        fn new() -> Self {
            let mut history = TestHistory::new([7; 32]);
            let tokens: Vec<u32> = (0..2000).collect();
            history.append(&tokens, &[]).unwrap();
            Self {
                history,
                identity: Identity {
                    tenant: "t".into(),
                    model: [7; 32],
                    layout: [1; 16],
                },
                // Cheap enough that fetching 800 tokens beats recomputing them.
                ranking: Ranking::for_test(
                    Level::DEFAULT_ORDER,
                    &[
                        (NeighbourGpu, 10.0),
                        (HostDram, 10.0),
                        (HostNvme, 100.0),
                        (RemoteNvme, 200.0),
                    ],
                ),
            }
        }

        fn request(&self, resident: u64) -> Request<'_> {
            Request {
                history: &self.history,
                identity: &self.identity,
                page_tokens: PT,
                resident,
                before: u64::MAX,
                ranking: &self.ranking,
                prefill_tokens_per_second: PREFILL,
            }
        }

        fn key(&self, tokens: u64) -> PrefixKey {
            self.history.key_at(tokens).unwrap()
        }

        /// A candidate supplying `begin..` through records ending at `ends`, whose
        /// first record starts at page `first_start`.
        fn candidate(
            &self,
            level: Level,
            source: u64,
            begin: u64,
            first_start: usize,
            ends: &[u64],
            us: u32,
        ) -> Candidate {
            let mut previous = first_start;
            let nodes: Vec<ChainNode> = ends
                .iter()
                .map(|end| {
                    let page = (end / PT) as usize;
                    let node = ChainNode {
                        key: self.key(*end),
                        tail_pages: page - previous,
                    };
                    previous = page;
                    node
                })
                .collect();
            Candidate {
                level,
                source: SourceId(source),
                generation: 1,
                key: self.key(*ends.last().unwrap()),
                begin_tokens: begin,
                skip_pages: (begin / PT) as usize - first_start,
                nodes,
                us_per_page: us,
            }
        }

        fn plan(&self, resident: u64, candidates: &[Candidate]) -> Option<Plan> {
            let plan = plan(&self.request(resident), candidates)?;
            plan.validate(&self.history, PT).unwrap();
            Some(plan)
        }
    }

    #[test]
    fn local_a_then_pool_b_is_contiguous_with_one_row() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 0);
        let plan = f.plan(0, &[remote, dram]).unwrap();
        let b = plan.b.as_ref().unwrap();
        assert_eq!((plan.a.source, plan.a.level), (SourceId(1), HostDram));
        assert_eq!(
            (plan.a.begin_tokens, plan.a.dest_page, plan.a.pages),
            (0, 0, 50)
        );
        assert!(!plan.a.row);
        assert_eq!(
            (b.source, b.begin_tokens, b.dest_page, b.pages),
            (SourceId(2), 800, 50, 50)
        );
        assert_eq!(b.first_page, 0);
        assert!(b.row);
        assert_eq!(plan.endpoint(), f.key(1600));
        assert_eq!(plan.pages(), 100);
        assert_eq!(plan.identity, f.identity);
    }

    #[test]
    fn a_resident_prefix_is_skipped_from_the_first_record() {
        let f = Fixture::new();
        // The record starts at page 0 but the GPU already holds the first 10 pages.
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let plan = f.plan(160, &[dram]).unwrap();
        assert_eq!(
            (plan.a.begin_tokens, plan.a.first_page, plan.a.pages),
            (160, 10, 40)
        );
        assert!(plan.a.row && plan.b.is_none());
    }

    #[test]
    fn b_starts_inside_a_record_when_boundaries_differ() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        // The pool's chain was cut at 640 and 1600; A ends in the second record.
        let remote = f.candidate(RemoteNvme, 2, 0, 0, &[640, 1600], 0);
        let plan = f.plan(0, &[dram, remote]).unwrap();
        let b = plan.b.as_ref().unwrap();
        assert_eq!((b.first_page, b.pages, b.dest_page), (10, 50, 50));
        assert_eq!(b.chain, vec![f.key(1600)]);
        // And in the first record, with both records read.
        let remote = f.candidate(RemoteNvme, 2, 0, 0, &[1000, 1600], 0);
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let plan = f.plan(0, &[dram, remote]).unwrap();
        let b = plan.b.as_ref().unwrap();
        assert_eq!((b.first_page, b.pages), (50, 50));
        assert_eq!(b.chain, vec![f.key(1000), f.key(1600)]);
    }

    #[test]
    fn a_split_in_the_middle_of_a_page_floors_both_parts() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[808], 0);
        let remote = f.candidate(RemoteNvme, 2, 808, 50, &[1608], 0);
        let plan = f.plan(0, &[dram, remote]).unwrap();
        let b = plan.b.as_ref().unwrap();
        assert_eq!(plan.a.pages, 50);
        assert_eq!(
            (b.begin_tokens, b.first_page, b.pages, b.dest_page),
            (808, 0, 50, 50)
        );
    }

    #[test]
    fn a_row_only_tail_has_no_pages_and_the_row() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[810], 0);
        let plan = f.plan(0, &[dram, remote]).unwrap();
        let b = plan.b.as_ref().unwrap();
        assert_eq!(
            (b.pages, b.dest_page, b.first_page, b.row),
            (0, 50, 0, true)
        );
        assert_eq!(plan.endpoint(), f.key(810));
        assert!(!plan.a.row);
    }

    #[test]
    fn a_candidate_starting_above_the_resident_prefix_is_a_hole() {
        let f = Fixture::new();
        let late = f.candidate(HostDram, 1, 800, 50, &[1600], 0);
        assert!(f.plan(0, &[late.clone()]).is_none());
        // Nothing at all is a miss too.
        assert!(f.plan(0, &[]).is_none());
        // A hole after A leaves A alone, with its own row.
        let dram = f.candidate(HostDram, 2, 0, 0, &[800], 0);
        let gap = f.candidate(RemoteNvme, 3, 960, 60, &[1600], 0);
        let plan = f.plan(0, &[dram, gap]).unwrap();
        assert!(plan.b.is_none() && plan.a.row);
        assert_eq!(plan.endpoint(), f.key(800));
    }

    #[test]
    fn a_candidate_covering_less_than_a_is_not_used() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let short = f.candidate(RemoteNvme, 2, 0, 0, &[400], 0);
        let same = f.candidate(RemoteNvme, 3, 0, 0, &[800], 0);
        let plan = f.plan(0, &[short, same, dram]).unwrap();
        assert!(plan.b.is_none());
        assert_eq!(plan.a.source, SourceId(1));
    }

    #[test]
    fn nothing_is_copied_from_a_farther_level_than_a_closer_one_holds() {
        let f = Fixture::new();
        // The neighbour GPU is closest even though it holds less than DRAM.
        let near = f.candidate(NeighbourGpu, 1, 0, 0, &[400], 0);
        let dram = f.candidate(HostDram, 2, 0, 0, &[800], 0);
        let plan = f.plan(0, &[dram, near]).unwrap();
        assert_eq!(plan.a.source, SourceId(1));
        let b = plan.b.as_ref().unwrap();
        assert_eq!(
            (b.source, b.begin_tokens, b.first_page, b.pages),
            (SourceId(2), 400, 25, 25)
        );
        // A source at A's own level is never B.
        let same_level = f.candidate(NeighbourGpu, 3, 0, 0, &[800], 0);
        let plan = f
            .plan(
                0,
                &[f.candidate(NeighbourGpu, 1, 0, 0, &[400], 0), same_level],
            )
            .unwrap();
        assert!(plan.b.is_none());
    }

    #[test]
    fn a_pinned_order_changes_which_level_is_closer() {
        let mut f = Fixture::new();
        f.ranking = Ranking::for_test(
            [ThisGpu, HostDram, NeighbourGpu, HostNvme, RemoteNvme],
            &[(NeighbourGpu, 10.0)],
        );
        let near = f.candidate(NeighbourGpu, 1, 0, 0, &[400], 0);
        let dram = f.candidate(HostDram, 2, 0, 0, &[800], 0);
        let plan = f.plan(0, &[near, dram]).unwrap();
        assert_eq!((plan.a.source, plan.endpoint()), (SourceId(2), f.key(800)));
        assert!(plan.b.is_none());
    }

    #[test]
    fn within_a_level_the_cheapest_source_wins_then_the_longest() {
        let f = Fixture::new();
        let slow = f.candidate(HostDram, 1, 0, 0, &[800], 50);
        let fast = f.candidate(HostDram, 2, 0, 0, &[400], 20);
        let unknown = f.candidate(HostDram, 3, 0, 0, &[1200], 0);
        let plan = f
            .plan(0, &[slow.clone(), unknown.clone(), fast.clone()])
            .unwrap();
        assert_eq!(plan.a.source, SourceId(2));
        // Equal cost: the longer match. Unknown cost ranks after known.
        let twin = f.candidate(HostDram, 4, 0, 0, &[1200], 20);
        let plan = f.plan(0, &[fast, twin, slow, unknown]).unwrap();
        assert_eq!(plan.a.source, SourceId(4));
        // The same rule picks among B candidates.
        let dram = f.candidate(HostDram, 1, 0, 0, &[400], 0);
        let b_slow = f.candidate(RemoteNvme, 5, 400, 25, &[1600], 90);
        let b_fast = f.candidate(RemoteNvme, 6, 400, 25, &[1200], 30);
        let plan = f.plan(0, &[dram, b_slow, b_fast]).unwrap();
        assert_eq!(plan.b.unwrap().source, SourceId(6));
    }

    #[test]
    fn b_is_dropped_when_its_pages_are_not_worth_fetching() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        // 50 pages at 2000 us is 0.1 s; half the prefill of 800 tokens is 0.05 s.
        let dear = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 2000);
        let plan = f.plan(0, &[dram.clone(), dear]).unwrap();
        assert!(plan.b.is_none() && plan.a.row);
        assert_eq!(plan.endpoint(), f.key(800));
        // A cheaper source of the same extension is taken instead.
        let fair = f.candidate(RemoteNvme, 3, 800, 50, &[1600], 900);
        let dear = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 20);
        let dearer = f.candidate(HostNvme, 4, 800, 50, &[1600], 2000);
        let plan = f.plan(0, &[dram.clone(), dearer, fair, dear]).unwrap();
        assert_eq!(plan.b.unwrap().source, SourceId(2));
    }

    #[test]
    fn a_source_with_no_known_cost_is_not_worth_fetching() {
        let mut f = Fixture::new();
        f.ranking = Ranking::default();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 0);
        assert!(
            f.plan(0, &[dram.clone(), remote.clone()])
                .unwrap()
                .b
                .is_none()
        );
        // The source's own estimate is enough.
        let mut known = remote;
        known.us_per_page = 20;
        assert!(f.plan(0, &[dram, known]).unwrap().b.is_some());
    }

    #[test]
    fn the_endpoint_stays_below_the_bound() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 0);
        let mut request = f.request(0);
        request.before = 1600;
        let plan = plan(&request, &[dram.clone(), remote]).unwrap();
        assert_eq!(plan.endpoint(), f.key(800));
        request.before = 800;
        assert!(super::plan(&request, &[dram]).is_none());
    }

    #[test]
    fn candidates_that_do_not_match_the_history_are_dropped() {
        let f = Fixture::new();
        let good = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let mut wrong_key = f.candidate(NeighbourGpu, 2, 0, 0, &[400], 0);
        wrong_key.nodes[0].key.digest[0] ^= 1;
        wrong_key.key = wrong_key.nodes[0].key;
        let mut wrong_endpoint = f.candidate(NeighbourGpu, 3, 0, 0, &[400], 0);
        wrong_endpoint.key = f.key(416);
        let mut broken_tails = f.candidate(NeighbourGpu, 4, 0, 0, &[200, 400], 0);
        broken_tails.nodes[1].tail_pages += 1;
        let mut wrong_skip = f.candidate(NeighbourGpu, 5, 160, 0, &[400], 0);
        wrong_skip.skip_pages = 0;
        let mut no_nodes = f.candidate(NeighbourGpu, 6, 0, 0, &[400], 0);
        no_nodes.nodes.clear();
        let gpu = f.candidate(ThisGpu, 7, 0, 0, &[400], 0);
        let mut beyond = f.candidate(NeighbourGpu, 8, 0, 0, &[400], 0);
        beyond.key.consumed_tokens = 3200;
        beyond.nodes[0].key = beyond.key;
        beyond.nodes[0].tail_pages = 200;
        let candidates = [
            wrong_key,
            wrong_endpoint,
            broken_tails,
            wrong_skip,
            no_nodes,
            gpu,
            beyond,
            good,
        ];
        let plan = f.plan(0, &candidates).unwrap();
        assert_eq!((plan.a.source, plan.endpoint()), (SourceId(1), f.key(800)));
    }

    #[test]
    fn rebuild_after_a_failed_hold_keeps_the_endpoint() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 0);
        let other_b = f.candidate(RemoteNvme, 3, 800, 50, &[1600], 0);
        let full = f.candidate(RemoteNvme, 4, 0, 0, &[1600], 0);
        let candidates = [dram, remote, other_b, full];
        let request = f.request(0);
        let first = plan(&request, &candidates).unwrap();
        assert_eq!(
            (first.a.source, first.b.as_ref().unwrap().source),
            (SourceId(1), SourceId(2))
        );

        // B fails: another source of the same endpoint for B, A unchanged.
        let again = first.rebuild(&request, &candidates, Which::B).unwrap();
        assert_eq!(
            (again.a.source, again.b.as_ref().unwrap().source),
            (SourceId(1), SourceId(3))
        );
        again.validate(&f.history, PT).unwrap();

        // A fails: a single part from the best remaining source of the endpoint.
        let again = first.rebuild(&request, &candidates, Which::A).unwrap();
        assert!(again.b.is_none() && again.a.row);
        assert_eq!((again.a.source, again.a.pages), (SourceId(4), 100));
        again.validate(&f.history, PT).unwrap();
    }

    #[test]
    fn rebuild_without_a_source_of_the_endpoint_is_a_miss() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 0);
        let candidates = [dram, remote];
        let request = f.request(0);
        let first = plan(&request, &candidates).unwrap();
        // B has no replacement, and a plan ending at 800 cannot reuse the ticket.
        assert!(first.rebuild(&request, &candidates, Which::B).is_none());
        assert!(first.rebuild(&request, &candidates, Which::A).is_none());
        // A single-part plan has no B to fail.
        let single = plan(&request, &candidates[..1]).unwrap();
        assert!(single.rebuild(&request, &candidates, Which::B).is_none());
    }

    #[test]
    fn a_failed_generation_is_excluded_but_a_newer_one_is_not() {
        let f = Fixture::new();
        let old = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let mut newer = old.clone();
        newer.generation = 2;
        let request = f.request(0);
        let first = plan(&request, std::slice::from_ref(&old)).unwrap();
        let again = first.rebuild(&request, &[old, newer], Which::A).unwrap();
        assert_eq!(again.a.generation, 2);
    }

    #[test]
    fn validate_rejects_inconsistent_plans() {
        let f = Fixture::new();
        let dram = f.candidate(HostDram, 1, 0, 0, &[800], 0);
        let remote = f.candidate(RemoteNvme, 2, 800, 50, &[1600], 0);
        let good = f.plan(0, &[dram, remote]).unwrap();
        let mut gap = good.clone();
        gap.b.as_mut().unwrap().begin_tokens += 16;
        let mut overlap = good.clone();
        overlap.b.as_mut().unwrap().dest_page = 49;
        let mut two_rows = good.clone();
        two_rows.a.row = true;
        let mut no_row = good.clone();
        no_row.b.as_mut().unwrap().row = false;
        let mut foreign = good.clone();
        foreign.b.as_mut().unwrap().chain[0].digest[0] ^= 1;
        let mut miscounted = good.clone();
        miscounted.a.pages += 1;
        for bad in [gap, overlap, two_rows, no_row, foreign, miscounted] {
            assert!(bad.validate(&f.history, PT).is_err());
        }
    }
}
