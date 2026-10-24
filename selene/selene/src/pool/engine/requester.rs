//! The requester role: restores from other instances and the manager's store.
//!
//! A restore has one or two pool parts. Each part is a claim that the manager
//! brokers: the source pins first, the manager grants windows, this client
//! checks every page and the row against the checksums the source supplied,
//! copies the window into the destination pages through the page copier, and
//! returns the window only after that copy has drained. The restore reports
//! one result after every destination write has drained, whatever happened.

use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use crate::{
    Direction, Error, Job, JobId, PageId, PrefixKey, StateSlotId, plan, record::checksum,
    slab::WINDOW_PAGES,
};

use super::{
    super::{
        codec,
        shared::{FetchFailure, FetchPart, PoolEvent, PoolFetch},
        wire::{self, client_message::M as Out},
    },
    CLAIM_DEADLINE_MS, Engine, Owner, RESTORE_DEADLINE,
    coverage::Coverage,
};

/// A record the claim is expected to be granted, derived from the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExpectedRecord {
    pub key: PrefixKey,
    pub first_page: u32,
    pub pages: u32,
    pub row: bool,
}

/// The records a part must be granted: those of its chain from the page that
/// holds its first token to its end, with the first record entered at the
/// record-relative page where the part starts. A part that ends in the page it
/// starts in has no pages, and its last record is a row-only tail.
pub(crate) fn expected_records(
    page_tokens: u64,
    part: &plan::Part,
    nodes: &[plan::ChainNode],
) -> Option<Vec<ExpectedRecord>> {
    let start_page = part.begin_tokens / page_tokens;
    let mut records = Vec::new();
    let mut total = 0usize;
    for (index, key) in part.chain.iter().enumerate() {
        let node = nodes.iter().find(|node| node.key == *key)?;
        let end = key.consumed_tokens / page_tokens;
        let start = end.checked_sub(node.tail_pages as u64)?;
        let from = start.max(start_page);
        let pages = end.saturating_sub(from);
        let last = index + 1 == part.chain.len();
        if pages == 0 && !last {
            continue;
        }
        total += pages as usize;
        records.push(ExpectedRecord {
            key: *key,
            first_page: (from - start) as u32,
            pages: pages as u32,
            row: last && part.row,
        });
    }
    (total == part.pages && !records.is_empty()).then_some(records)
}

#[derive(Default)]
pub(crate) struct State {
    next_claim: u64,
    restores: HashMap<u64, Restore>,
    claims: HashMap<u64, Claim>,
}

impl State {
    pub(crate) fn is_empty(&self) -> bool {
        self.restores.is_empty() && self.claims.is_empty()
    }
}

struct PartRun {
    fetch: FetchPart,
    expected: Vec<ExpectedRecord>,
    claim: Option<u64>,
    copied: bool,
}

struct Restore {
    parts: Vec<PartRun>,
    deadline: Instant,
    failure: Option<FetchFailure>,
    bytes: u64,
}

struct Window {
    jobs_left: u32,
    released: bool,
}

struct Claim {
    restore: u64,
    part: usize,
    total_pages: u64,
    wants_row: bool,
    pages: Vec<PageId>,
    slot: Option<StateSlotId>,
    granted: bool,
    released: bool,
    terminal: Option<wire::ClaimTerminal>,
    coverage: Coverage,
    windows: BTreeMap<u32, Window>,
    jobs_outstanding: u32,
    bad: Option<wire::CopyOutcome>,
    verdict_sent: bool,
    verdict_ok: bool,
    fence_sent: bool,
}

impl Engine {
    // ---- starting a restore ---------------------------------------------------

    pub(crate) fn start_fetch(&mut self, now: Instant, fetch: PoolFetch) {
        let id = fetch.id;
        match self.build_restore(now, &fetch) {
            Ok(restore) => {
                self.requester.restores.insert(id, restore);
                self.claim_part(now, id, 0);
            }
            Err(error) => self.event(PoolEvent::Fetched {
                id,
                result: Err(FetchFailure { error, part: None }),
            }),
        }
    }

    fn build_restore(&self, now: Instant, fetch: &PoolFetch) -> Result<Restore, Error> {
        if !self.ready() || self.closing || self.quarantined() {
            return Err(Error::new("the pool is not ready"));
        }
        if self
            .sess
            .as_ref()
            .and_then(|s| s.regions.as_ref())
            .is_none()
        {
            return Err(Error::new(
                "this session has no shared memory to copy through",
            ));
        }
        if fetch.parts.is_empty() || fetch.parts.len() > 2 {
            return Err(Error::new("a pool restore has one or two parts"));
        }
        let mut bytes = 0;
        let mut parts = Vec::new();
        for part in &fetch.parts {
            let expected =
                expected_records(fetch.page_tokens, &part.part, &part.candidate.plan.nodes)
                    .ok_or_else(|| Error::new("a part's chain does not match its pages"))?;
            if part.pages.len() != part.part.pages || part.part.row != part.slot.is_some() {
                return Err(Error::new("a part's destination does not match its plan"));
            }
            bytes += part.part.pages as u64 * self.st.page_record_bytes()
                + if part.part.row {
                    self.st.row_record_bytes()
                } else {
                    0
                };
            parts.push(PartRun {
                fetch: part.clone(),
                expected,
                claim: None,
                copied: false,
            });
        }
        Ok(Restore {
            parts,
            deadline: now + RESTORE_DEADLINE,
            failure: None,
            bytes,
        })
    }

    fn claim_part(&mut self, now: Instant, restore: u64, index: usize) {
        let _ = now;
        let Some(run) = self.requester.restores.get_mut(&restore) else {
            return;
        };
        let single = run.parts.len() == 1;
        let part = &mut run.parts[index];
        self.requester.next_claim += 1;
        let id = self.requester.next_claim;
        part.claim = Some(id);
        let fetch = &part.fetch;
        let p = &fetch.part;
        let claim = wire::Claim {
            claim_id: id,
            level: codec::level_to(p.level) as i32,
            source: Some(wire::SourceRef {
                instance_id: fetch.candidate.source.instance.clone(),
                session_incarnation: fetch.candidate.source.session_incarnation,
            }),
            tier: codec::tier_to(fetch.candidate.tier) as i32,
            key: Some(codec::key_to(p.end)),
            generation: p.generation,
            begin_tokens: p.begin_tokens,
            want_row: p.row,
            mode: wire::CopyMode::SharedWindows as i32,
            deadline_ms: CLAIM_DEADLINE_MS,
            plan_id: restore,
            part: match (single, index) {
                (true, _) => wire::PlanPart::Single,
                (false, 0) => wire::PlanPart::PartA,
                (false, _) => wire::PlanPart::PartB,
            } as i32,
        };
        let expected = part.expected.clone();
        let total_pages = expected.iter().map(|r| u64::from(r.pages)).sum::<u64>();
        self.requester.claims.insert(
            id,
            Claim {
                restore,
                part: index,
                coverage: Coverage::new(expected),
                total_pages,
                wants_row: p.row,
                pages: fetch.pages.clone(),
                slot: fetch.slot,
                granted: false,
                released: false,
                terminal: None,
                windows: BTreeMap::new(),
                jobs_outstanding: 0,
                bad: None,
                verdict_sent: false,
                verdict_ok: false,
                fence_sent: false,
            },
        );
        self.emit(Out::Claim(claim));
    }

    // ---- grants and windows ---------------------------------------------------

    pub(crate) fn on_claim_granted(&mut self, now: Instant, granted: wire::ClaimGranted) {
        let id = granted.claim_id;
        // A grant for a claim this client no longer holds is never used; its
        // terminal message is answered with an immediate fence.
        let Some(claim) = self.requester.claims.get_mut(&id) else {
            return;
        };
        if claim.granted || claim.released || claim.terminal.is_some() || claim.bad.is_some() {
            return;
        }
        let verdict = if granted.channel.is_some() {
            Err("the grant names a data channel, which this client does not use")
        } else if !records_match(&claim.coverage.expected, &granted.records)
            || granted.total_pages != claim.total_pages
            || granted.row != claim.wants_row
        {
            Err("the granted records do not match the plan")
        } else {
            Ok(())
        };
        match verdict {
            Err(why) => {
                // The grant is held, so release it with a verdict, not a cancel.
                claim.granted = true;
                let restore = claim.restore;
                let part = claim.part;
                self.fail_restore(
                    restore,
                    Some(id),
                    Some(part),
                    Error::new(why),
                    wire::CopyOutcome::CopyAbandoned,
                );
            }
            Ok(()) => {
                claim.granted = true;
                let (restore, part) = (claim.restore, claim.part);
                let next = part + 1;
                if self
                    .requester
                    .restores
                    .get(&restore)
                    .is_some_and(|r| next < r.parts.len() && r.failure.is_none())
                {
                    self.claim_part(now, restore, next);
                }
            }
        }
    }

    pub(crate) fn on_window_ready(&mut self, ready: wire::WindowReady) {
        let id = ready.claim_id;
        let Some(spec) = ready.window else {
            return;
        };
        let regions = self.sess.as_ref().and_then(|s| s.regions.clone());
        let (page_bytes, row_bytes) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        let Some(claim) = self.requester.claims.get_mut(&id) else {
            return;
        };
        if !claim.granted || claim.released || claim.terminal.is_some() || claim.bad.is_some() {
            return;
        }
        let (restore, part) = (claim.restore, claim.part);
        let dest = match claim.coverage.accept(&spec, page_bytes, row_bytes) {
            Ok(dest) => dest,
            Err(why) => {
                self.fail_restore(
                    restore,
                    Some(id),
                    Some(part),
                    Error::new(why),
                    wire::CopyOutcome::CopyErrorOutcome,
                );
                return;
            }
        };
        claim.windows.insert(
            spec.index,
            Window {
                jobs_left: 0,
                released: false,
            },
        );
        let Some(region) = regions.as_ref().and_then(|r| r.get(spec.region())) else {
            self.fail_restore(
                restore,
                Some(id),
                Some(part),
                Error::new("window in an unknown region"),
                wire::CopyOutcome::CopyErrorOutcome,
            );
            return;
        };
        let Some(bytes) = region.bytes(spec.offset, spec.length) else {
            self.fail_restore(
                restore,
                Some(id),
                Some(part),
                Error::new("window outside its region"),
                wire::CopyOutcome::CopyErrorOutcome,
            );
            return;
        };
        let count = spec.page_count as usize;
        if ready.page_xxh3.len() != count || ready.has_row != spec.row {
            self.fail_restore(
                restore,
                Some(id),
                Some(part),
                Error::new("window checksums do not match its pages"),
                wire::CopyOutcome::CopyErrorOutcome,
            );
            return;
        }
        let page_len = page_bytes as usize;
        let mismatch = (0..count)
            .any(|i| checksum(&bytes[i * page_len..(i + 1) * page_len]) != ready.page_xxh3[i])
            || (spec.row && checksum(&bytes[count * page_len..]) != ready.row_xxh3);
        if mismatch {
            // Nothing was copied, so the window is free to go back at once.
            self.release_window(id, spec.index);
            self.fail_restore(
                restore,
                Some(id),
                Some(part),
                Error::new("a page or row failed its checksum"),
                wire::CopyOutcome::CopyChecksum,
            );
            return;
        }
        let base = region
            .address(spec.offset, spec.length)
            .expect("checked above");
        let claim = self.requester.claims.get_mut(&id).expect("present");
        let mut jobs = Vec::new();
        let dest = dest as usize;
        let mut first = 0;
        while first < count || (count == 0 && jobs.is_empty()) {
            let end = (first + WINDOW_PAGES).min(count);
            let pages = (first..end)
                .map(|i| (claim.pages[dest + i], base + i * page_len))
                .collect();
            let last = end == count;
            let row = (last && spec.row).then(|| {
                (
                    claim.slot.expect("a part with a row has a slot"),
                    base + count * page_len,
                )
            });
            jobs.push(Job {
                id: JobId(0),
                window: spec.index,
                direction: Direction::In,
                pages,
                row,
            });
            first = end;
            if count == 0 {
                break;
            }
        }
        let window = claim.windows.get_mut(&spec.index).expect("inserted");
        window.jobs_left = jobs.len() as u32;
        claim.jobs_outstanding += jobs.len() as u32;
        for job in jobs {
            self.queue_job(
                job,
                Owner::Claim {
                    claim: id,
                    window: spec.index,
                },
            );
        }
    }

    pub(crate) fn on_claim_job(&mut self, id: u64, window: u32, result: crate::Result<()>) {
        let Some(claim) = self.requester.claims.get_mut(&id) else {
            return;
        };
        claim.jobs_outstanding = claim.jobs_outstanding.saturating_sub(1);
        if let Some(w) = claim.windows.get_mut(&window) {
            w.jobs_left = w.jobs_left.saturating_sub(1);
        }
        let failed = result.err();
        let (restore, part) = (claim.restore, claim.part);
        if let Some(error) = failed {
            self.fail_restore(
                restore,
                Some(id),
                Some(part),
                error,
                wire::CopyOutcome::CopyErrorOutcome,
            );
        }
        let done = self
            .requester
            .claims
            .get(&id)
            .and_then(|c| c.windows.get(&window))
            .is_some_and(|w| w.jobs_left == 0 && !w.released);
        if done {
            self.release_window(id, window);
        }
        self.drive_claim(id);
    }

    fn release_window(&mut self, claim: u64, window: u32) {
        if let Some(w) = self
            .requester
            .claims
            .get_mut(&claim)
            .and_then(|c| c.windows.get_mut(&window))
            && !w.released
        {
            w.released = true;
            self.emit(Out::WindowReleased(wire::WindowReleased {
                claim_id: claim,
                index: window,
            }));
        }
    }

    pub(crate) fn on_claim_terminal(&mut self, terminal: wire::ClaimTerminal) {
        let id = terminal.claim_id;
        let Some(claim) = self.requester.claims.get_mut(&id) else {
            // Unknown here (released, restarted, never heard of): there is
            // nothing of ours to drain.
            if terminal.fence_required {
                self.emit(Out::FenceAck(wire::FenceAck {
                    subject: Some(wire::fence_ack::Subject::ClaimId(id)),
                    outcome: wire::FenceOutcome::Drained as i32,
                }));
            }
            return;
        };
        if claim.terminal.is_some() {
            return;
        }
        let state = terminal.state;
        claim.terminal = Some(terminal);
        let (restore, part, verdict_ok) = (claim.restore, claim.part, claim.verdict_ok);
        if state == wire::ClaimState::Copied as i32 && verdict_ok {
            if let Some(run) = self.requester.restores.get_mut(&restore) {
                run.parts[part].copied = true;
            }
        } else {
            let error = Error::new(format!(
                "claim ended {:?}: {:?}",
                wire::ClaimState::try_from(state),
                wire::ClaimReason::try_from(terminal.reason)
            ));
            self.fail_restore(
                restore,
                Some(id),
                Some(part),
                error,
                wire::CopyOutcome::CopyAbandoned,
            );
        }
        self.drive_claim(id);
    }

    // ---- driving a claim to its verdict, fence and removal ----------------------

    /// Once no copy of the claim is outstanding: send its verdict or fence
    /// acknowledgment, forget it when nothing more is owed, and check whether
    /// its restore is over.
    fn drive_claim(&mut self, id: u64) {
        let Some(claim) = self.requester.claims.get_mut(&id) else {
            return;
        };
        if claim.jobs_outstanding > 0 {
            return;
        }
        let mut send = Vec::new();
        let restore = claim.restore;
        if let Some(terminal) = &claim.terminal {
            if terminal.fence_required && !claim.fence_sent {
                claim.fence_sent = true;
                send.push(Out::FenceAck(wire::FenceAck {
                    subject: Some(wire::fence_ack::Subject::ClaimId(id)),
                    outcome: wire::FenceOutcome::Drained as i32,
                }));
            }
        } else if !claim.verdict_sent && claim.granted && !claim.released {
            let outcome = if let Some(bad) = claim.bad {
                Some(bad)
            } else if claim.coverage.complete() && claim.windows.values().all(|w| w.released) {
                claim.verdict_ok = true;
                Some(wire::CopyOutcome::CopyOk)
            } else {
                None
            };
            if let Some(outcome) = outcome {
                claim.verdict_sent = true;
                send.push(Out::CopyResult(wire::CopyResult {
                    claim_id: id,
                    outcome: outcome as i32,
                }));
            }
        }
        let gone = claim
            .terminal
            .as_ref()
            .is_some_and(|t| !t.fence_required || claim.fence_sent);
        if gone {
            self.requester.claims.remove(&id);
        }
        for message in send {
            self.emit(message);
        }
        self.check_restore(restore);
    }

    fn check_restore(&mut self, id: u64) {
        let Some(restore) = self.requester.restores.get(&id) else {
            return;
        };
        let drained = !self
            .requester
            .claims
            .values()
            .any(|c| c.restore == id && c.jobs_outstanding > 0);
        let result = if restore.failure.is_some() {
            if !drained {
                return;
            }
            Err(restore.failure.clone().expect("checked"))
        } else if restore.parts.iter().all(|p| p.copied) {
            Ok(restore.bytes)
        } else {
            return;
        };
        self.requester.restores.remove(&id);
        self.event(PoolEvent::Fetched { id, result });
    }

    /// The restore cannot succeed. Granted claims get a verdict once their
    /// copies drain (`culprit` carries `outcome`, the others are abandoned);
    /// claims without a grant are released; queued copies are withdrawn.
    fn fail_restore(
        &mut self,
        id: u64,
        culprit: Option<u64>,
        part: Option<usize>,
        error: Error,
        outcome: wire::CopyOutcome,
    ) {
        let Some(restore) = self.requester.restores.get_mut(&id) else {
            return;
        };
        restore.failure.get_or_insert(FetchFailure { error, part });
        let claims: Vec<u64> = self
            .requester
            .claims
            .iter()
            .filter(|(_, c)| c.restore == id)
            .map(|(claim, _)| *claim)
            .collect();
        for claim_id in claims {
            self.withdraw_queued(claim_id);
            let claim = self.requester.claims.get_mut(&claim_id).expect("listed");
            if claim.terminal.is_none() {
                if !claim.granted {
                    if !claim.released {
                        claim.released = true;
                        self.emit(Out::ClaimRelease(wire::ClaimRelease { claim_id }));
                    }
                } else {
                    let bad = if Some(claim_id) == culprit {
                        outcome
                    } else {
                        wire::CopyOutcome::CopyAbandoned
                    };
                    claim.bad.get_or_insert(bad);
                }
            }
            self.drive_claim(claim_id);
        }
        self.check_restore(id);
    }

    /// Withdraw the claim's copies that were queued but never submitted.
    fn withdraw_queued(&mut self, claim_id: u64) {
        let mut kept = std::collections::VecDeque::new();
        let mut released = Vec::new();
        for (job, owner) in std::mem::take(&mut self.pending) {
            match owner {
                Owner::Claim { claim, window } if claim == claim_id => {
                    if let Some(c) = self.requester.claims.get_mut(&claim) {
                        c.jobs_outstanding = c.jobs_outstanding.saturating_sub(1);
                        if let Some(w) = c.windows.get_mut(&window) {
                            w.jobs_left = w.jobs_left.saturating_sub(1);
                            if w.jobs_left == 0 {
                                released.push(window);
                            }
                        }
                    }
                }
                _ => kept.push_back((job, owner)),
            }
        }
        self.pending = kept;
        for window in released {
            self.release_window(claim_id, window);
        }
    }

    // ---- time, loss, close --------------------------------------------------------

    pub(crate) fn requester_tick(&mut self, now: Instant) {
        let late: Vec<u64> = self
            .requester
            .restores
            .iter()
            .filter(|(_, r)| r.failure.is_none() && now >= r.deadline)
            .map(|(id, _)| *id)
            .collect();
        for id in late {
            self.fail_restore(
                id,
                None,
                None,
                Error::new("the restore ran out of time"),
                wire::CopyOutcome::CopyAbandoned,
            );
        }
    }

    /// The session is gone: every restore fails; claims are treated as ended by
    /// the requester's loss, and each is fenced once its copies drain (the
    /// acknowledgment is owed to the next session).
    pub(crate) fn requester_lost(&mut self) {
        let ids: Vec<u64> = self.requester.restores.keys().copied().collect();
        let granted: Vec<u64> = self.requester.claims.keys().copied().collect();
        for id in granted {
            if let Some(claim) = self.requester.claims.get_mut(&id)
                && claim.terminal.is_none()
            {
                claim.terminal = Some(wire::ClaimTerminal {
                    claim_id: id,
                    state: wire::ClaimState::Failed as i32,
                    reason: wire::ClaimReason::RequesterLost as i32,
                    fence_required: claim.granted,
                });
            }
        }
        for id in ids {
            self.fail_restore(
                id,
                None,
                None,
                Error::new("the pool session was lost"),
                wire::CopyOutcome::CopyAbandoned,
            );
        }
        let all: Vec<u64> = self.requester.claims.keys().copied().collect();
        for id in all {
            self.withdraw_queued(id);
            self.drive_claim(id);
        }
    }

    pub(crate) fn requester_close(&mut self) {
        let ids: Vec<u64> = self.requester.restores.keys().copied().collect();
        for id in ids {
            self.fail_restore(
                id,
                None,
                None,
                Error::new("the pool client is closing"),
                wire::CopyOutcome::CopyAbandoned,
            );
        }
    }

    pub(crate) fn requester_quarantine(&mut self) {
        let ids: Vec<u64> = self.requester.claims.keys().copied().collect();
        for id in ids {
            self.emit(Out::FenceAck(wire::FenceAck {
                subject: Some(wire::fence_ack::Subject::ClaimId(id)),
                outcome: wire::FenceOutcome::Quarantined as i32,
            }));
        }
        // Their copies may still run: nothing is freed, nothing is reported.
        std::mem::forget(std::mem::take(&mut self.requester.claims));
        std::mem::forget(std::mem::take(&mut self.requester.restores));
    }
}

fn records_match(expected: &[ExpectedRecord], granted: &[wire::Record]) -> bool {
    expected.len() == granted.len()
        && expected.iter().zip(granted).all(|(want, got)| {
            got.key.as_ref().and_then(codec::key_from) == Some(want.key)
                && got.first_page == want.first_page
                && got.pages == want.pages
                && got.row == want.row
        })
}
