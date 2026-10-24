//! Lookup (PROTOCOL.md section 4): candidates from the index, never from a source.

use super::{
    Checked, Violation,
    core::Core,
    index::{Entry, SourceId, Tier, walk_chain},
    key_from, key_to, tier_to,
};
use crate::{
    Key,
    proto::v2::{self, server_message::M},
};
use std::collections::VecDeque;

/// A source's running cost as microseconds and pages, each decayed by 7/8 per claim.
pub(super) type Estimate = (u64, u64);

/// Add a finished claim to a source's running cost.
pub(super) fn blend_estimate(estimate: &mut Estimate, micros: u64, pages: u64) {
    *estimate = (estimate.0 * 7 / 8 + micros, estimate.1 * 7 / 8 + pages);
}

/// The running cost per page, or 0 when nothing is known. Weighted by pages, so a claim of
/// a few pages, which is mostly fixed cost, cannot dominate one of hundreds.
pub(super) fn estimate_us_per_page(estimate: Estimate) -> u32 {
    if estimate.1 == 0 {
        return 0;
    }
    (estimate.0 / estimate.1).clamp(1, u64::from(u32::MAX)) as u32
}

/// Entries scanned per lookup at most, so one lookup cannot hold the actor.
const SCAN_BUDGET: usize = 65_536;
/// Lookups answered per pass of the clock.
const PER_PASS: usize = 64;

pub(super) struct Queued {
    pub sid: u64,
    pub request: v2::LookupRequest,
}

fn rank(order: &[v2::Level], level: v2::Level) -> i32 {
    if level == v2::Level::ThisGpu {
        return -1;
    }
    order
        .iter()
        .position(|candidate| *candidate == level)
        .map_or(i32::MAX, |position| position as i32)
}

impl Core {
    pub(super) fn lookup(&mut self, sid: u64, request: v2::LookupRequest) -> Checked<()> {
        let limits = self.cfg.limits.clone();
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        if !self.ready() {
            let retry = self.readiness_info().barrier_remaining_ms.clamp(50, 1000);
            self.lookup_reply(
                sid,
                request.lookup_id,
                v2::LookupStatus::LookupNotReady,
                vec![],
                retry,
            );
            return Ok(());
        }
        if session.lookups.contains(&request.lookup_id) {
            return Err(Violation::new("lookup_id reused while live"));
        }
        if session.lookups.len() >= limits.max_concurrent_lookups as usize {
            self.lookup_reply(
                sid,
                request.lookup_id,
                v2::LookupStatus::LookupOverloaded,
                vec![],
                50,
            );
            return Ok(());
        }
        let session = self.sessions.get_mut(&sid).expect("checked above");
        session.lookups.insert(request.lookup_id);
        self.live_lookups += 1;
        self.lookup_queue.push_back(Queued { sid, request });
        Ok(())
    }

    pub(super) fn lookup_cancel(&mut self, sid: u64, lookup_id: u64) {
        let before = self.lookup_queue.len();
        self.lookup_queue
            .retain(|queued| !(queued.sid == sid && queued.request.lookup_id == lookup_id));
        if self.lookup_queue.len() != before {
            if let Some(session) = self.sessions.get_mut(&sid) {
                session.lookups.remove(&lookup_id);
            }
            self.live_lookups = self.live_lookups.saturating_sub(1);
        }
    }

    /// Answers queued lookups. Called from the clock, so a burst arriving within one
    /// pass sees the concurrency bound.
    pub(super) fn lookup_pass(&mut self) {
        for _ in 0..PER_PASS {
            let Some(Queued { sid, request }) = self.lookup_queue.pop_front() else {
                break;
            };
            let Some(session) = self.sessions.get_mut(&sid) else {
                continue;
            };
            session.lookups.remove(&request.lookup_id);
            self.live_lookups = self.live_lookups.saturating_sub(1);
            let lookup_id = request.lookup_id;
            let (status, candidates) = self.answer_lookup(sid, request);
            self.lookup_reply(sid, lookup_id, status, candidates, 0);
        }
    }

    fn lookup_reply(
        &mut self,
        sid: u64,
        lookup_id: u64,
        status: v2::LookupStatus,
        candidates: Vec<v2::Candidate>,
        retry_after_ms: u32,
    ) {
        self.count("lookups_total", format!("status=\"{status:?}\""));
        self.send(
            sid,
            M::LookupReply(v2::LookupReply {
                lookup_id,
                status: status as i32,
                candidates,
                retry_after_ms,
            }),
        );
    }

    fn answer_lookup(
        &self,
        sid: u64,
        request: v2::LookupRequest,
    ) -> (v2::LookupStatus, Vec<v2::Candidate>) {
        use v2::LookupStatus as S;
        let limits = &self.cfg.limits;
        let Some(session) = self.sessions.get(&sid) else {
            return (S::LookupInvalid, vec![]);
        };
        let Some(state) = self.scopes.get(&session.scope) else {
            return (S::LookupInvalid, vec![]);
        };
        let geometry = &state.valid.geometry;

        // Validation.
        if request.boundaries.len() > limits.max_boundaries as usize {
            return (S::LookupInvalid, vec![]);
        }
        let mut boundaries: Vec<Key> = Vec::with_capacity(request.boundaries.len());
        for boundary in &request.boundaries {
            let Ok(key) = key_from(Some(boundary)) else {
                return (S::LookupInvalid, vec![]);
            };
            if boundaries
                .last()
                .is_some_and(|previous| previous.consumed_tokens >= key.consumed_tokens)
            {
                return (S::LookupInvalid, vec![]);
            }
            boundaries.push(key);
        }
        let mut order: Vec<v2::Level> = Vec::new();
        for value in &request.level_order {
            match v2::Level::try_from(*value) {
                Ok(v2::Level::ThisGpu) => {}
                Ok(
                    level @ (v2::Level::NeighbourGpu
                    | v2::Level::HostDram
                    | v2::Level::HostNvme
                    | v2::Level::RemoteNvme),
                ) => {
                    if order.contains(&level) {
                        return (S::LookupInvalid, vec![]);
                    }
                    order.push(level);
                }
                _ => return (S::LookupInvalid, vec![]),
            }
        }
        if order.len() != 4 {
            return (S::LookupInvalid, vec![]);
        }
        let mut held: Vec<(v2::Level, Key)> = Vec::new();
        for range in &request.held {
            let (Ok(level), Ok(end)) = (
                v2::Level::try_from(range.level),
                key_from(range.end.as_ref()),
            ) else {
                return (S::LookupInvalid, vec![]);
            };
            if level == v2::Level::Unspecified {
                return (S::LookupInvalid, vec![]);
            }
            held.push((level, end));
        }
        let max = if request.max_candidates == 0 {
            limits.max_candidates
        } else {
            request.max_candidates.min(limits.max_candidates)
        } as usize;

        // Records shorter than one page all live in the root bucket, whatever they are
        // records of, so for a history that has a block of its own they would fill the
        // candidate list with records that cannot be told from a match. Such a record
        // restores no page, so it is not offered then.
        let short = if boundaries.len() > 1 {
            geometry.page_tokens
        } else {
            0
        };

        // Candidates.
        let mut scanned = 0usize;
        let mut found: Vec<Ranked> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for boundary in &boundaries {
            for (source, key, tier) in state.index.in_bucket(&boundary.digest) {
                if scanned >= SCAN_BUDGET {
                    break;
                }
                scanned += 1;
                if !seen.insert((source, key, tier)) {
                    continue;
                }
                let Some(entry) = state.index.get(source, key, tier) else {
                    continue;
                };
                if !entry.row
                    || entry.key.consumed_tokens >= request.before_tokens
                    || entry.key.consumed_tokens < short
                {
                    continue;
                }
                let Some((level, source_ref)) =
                    self.reach(session.shm, session.instance, source, tier)
                else {
                    continue;
                };
                let closer = rank(&order, level);
                let begin = held
                    .iter()
                    .filter(|(held_level, _)| rank(&order, *held_level) < closer)
                    .map(|(_, end)| end.consumed_tokens)
                    .max()
                    .unwrap_or(0);
                if begin >= entry.key.consumed_tokens {
                    continue;
                }
                let start_page = begin / geometry.page_tokens;
                let Some(walk) = walk_chain(
                    &state.index,
                    source,
                    entry,
                    geometry.page_tokens,
                    start_page,
                    limits.max_chain_nodes as usize,
                ) else {
                    continue;
                };
                let estimate = self
                    .estimates
                    .get(&source)
                    .map_or(0, |e| estimate_us_per_page(*e));
                found.push((
                    closer,
                    estimate == 0,
                    estimate,
                    std::cmp::Reverse(entry.key.consumed_tokens),
                    source,
                    tier,
                    candidate(entry, level, source_ref, begin, &walk, geometry, estimate),
                ));
            }
        }
        found.sort_by(|a, b| (a.0, a.1, a.2, a.3, a.4, a.5).cmp(&(b.0, b.1, b.2, b.3, b.4, b.5)));
        found.truncate(max);
        (S::LookupOk, found.into_iter().map(|item| item.6).collect())
    }

    /// The level a source's entry has for a requester, and the reference a claim
    /// names. `None` when the requester cannot use that source.
    pub(super) fn reach(
        &self,
        requester_shm: bool,
        requester: super::index::InstanceId,
        source: SourceId,
        tier: Tier,
    ) -> Option<(v2::Level, v2::SourceRef)> {
        match source {
            SourceId::Instance(id) => {
                if id == requester {
                    return None;
                }
                let sid = self.instances.get(&id)?.current?;
                let source_session = self.sessions.get(&sid)?;
                if !requester_shm || !source_session.shm || source_session.export_slots == 0 {
                    return None;
                }
                let level = match tier {
                    Tier::Gpu => v2::Level::NeighbourGpu,
                    Tier::Dram => v2::Level::HostDram,
                    Tier::Nvme => v2::Level::HostNvme,
                };
                Some((
                    level,
                    v2::SourceRef {
                        instance_id: id.to_vec(),
                        session_incarnation: sid,
                    },
                ))
            }
            SourceId::Manager => {
                let level = match (requester_shm, tier) {
                    (_, Tier::Gpu) => return None,
                    (true, Tier::Dram) => v2::Level::HostDram,
                    (true, Tier::Nvme) => v2::Level::HostNvme,
                    (false, _) => v2::Level::RemoteNvme,
                };
                Some((
                    level,
                    v2::SourceRef {
                        instance_id: Vec::new(),
                        session_incarnation: 0,
                    },
                ))
            }
        }
    }
}

fn candidate(
    entry: &Entry,
    level: v2::Level,
    source: v2::SourceRef,
    begin: u64,
    walk: &super::index::ChainWalk,
    geometry: &super::ScopeGeometry,
    estimate: u32,
) -> v2::Candidate {
    v2::Candidate {
        level: level as i32,
        tier: tier_to(entry.tier) as i32,
        source: Some(source),
        key: Some(key_to(entry.key)),
        generation: entry.generation,
        begin_tokens: begin,
        pages: walk.pages,
        bytes: walk.pages * geometry.page_bytes + if entry.row { geometry.row_bytes } else { 0 },
        row: entry.row,
        nodes: walk
            .nodes
            .iter()
            .map(|node| v2::ChainNode {
                key: Some(key_to(node.key)),
                tail_pages: node.tail_pages,
                generation: node.generation,
                row: node.row,
            })
            .collect(),
        skip_pages: walk.skip_pages,
        est_us_per_page: estimate,
    }
}

/// A candidate with its sort key: level rank, unknown cost, cost, longer first, source, tier.
type Ranked = (
    i32,
    bool,
    u32,
    std::cmp::Reverse<u64>,
    SourceId,
    Tier,
    v2::Candidate,
);

pub(super) type LookupQueue = VecDeque<Queued>;

#[cfg(test)]
mod estimate_tests {
    use super::*;

    #[test]
    fn a_long_claim_outweighs_the_fixed_cost_of_short_ones() {
        let mut estimate = Estimate::default();
        assert_eq!(estimate_us_per_page(estimate), 0);
        // Probe claims: a few pages at mostly fixed cost, 4.25 ms a page.
        for _ in 0..9 {
            blend_estimate(&mut estimate, 8_500, 2);
        }
        assert!(estimate_us_per_page(estimate) >= 4_000);
        // Real restores of hundreds of pages at 0.9 ms a page.
        for _ in 0..4 {
            blend_estimate(&mut estimate, 470_000, 520);
        }
        let per_page = estimate_us_per_page(estimate);
        assert!((850..=1_000).contains(&per_page), "{per_page}");
    }
}
