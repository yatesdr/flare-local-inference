//! Claims, exports, windows and fences (PROTOCOL.md sections 5 to 8).
//!
//! Every claim is one record, changed only by the actor, so "one authoritative
//! transition" is a plain state check. Windows are carved from the staging region
//! and are reclaimed only when every party that could have touched them has
//! drained, or is confirmed gone.

use super::{
    Checked, Violation,
    core::{Core, Nonce, ProcKey, Session},
    index::{InstanceId, SourceId, Tier, walk_chain},
    key_from, key_to,
    store_bridge::persisted_checksums,
    tier_from, tier_to,
    windows::{self, PlannedWindow, RecordShape},
};
use crate::{
    Key,
    arena::Extent,
    proto::v2::{self, server_message::M},
    store::GetAnswer,
};
use std::time::{Duration, Instant};

pub type ClaimKey = (InstanceId, Nonce, u64);

/// One party's drain obligation for a claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Party {
    /// The party never touched the claim's memory.
    Idle,
    /// It may still be accessing memory and has not said otherwise.
    Pending,
    Drained,
    Quarantined,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Planned, not yet handed to anyone.
    Unissued,
    /// The source may be writing it.
    SourceWrite,
    /// The requester may be reading it.
    RequesterRead,
    Free,
}

pub struct Win {
    pub plan: PlannedWindow,
    pub region: i32,
    pub offset: u64,
    pub phase: Phase,
    /// A window allocated on its own (streaming claims).
    pub extent: Option<Extent>,
    pub page_xxh3: Vec<u64>,
    pub row_xxh3: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportState {
    /// The claim has no source export (a refusal before any request).
    None,
    Requested,
    Pinned,
    Refused,
    Ended(Party),
}

pub struct Claim {
    pub key: ClaimKey,
    pub requester_sid: u64,
    pub source: SourceId,
    pub source_proc: Option<ProcKey>,
    pub source_sid: u64,
    pub endpoint: Key,
    pub want_row: bool,
    pub state: v2::ClaimState,
    pub reason: v2::ClaimReason,
    pub deadline: Instant,
    pub last_progress: Instant,
    pub pin_deadline: Instant,
    pub export_id: u64,
    pub export: ExportState,
    pub records: Vec<v2::Record>,
    pub total_pages: u64,
    pub row: bool,
    pub retained: bool,
    pub reserved_bytes: u64,
    pub block: Option<Extent>,
    pub windows: Vec<Win>,
    pub requester_fence: Party,
    pub source_fence: Party,
    pub terminal_at: Option<Instant>,
    pub granted_at: Option<Instant>,
    pub page_bytes: u64,
    pub row_bytes: u64,
    /// Set when the manager's own store is the source.
    pub manager: Option<ManagerGet>,
}

/// What a claim on the manager's store tracks beyond the common state.
pub struct ManagerGet {
    /// Persisted page and row checksums by record, from the store's metadata.
    pub checks: std::collections::HashMap<u32, (Vec<u64>, u64)>,
    /// A record's metadata pieces as they arrive.
    pub meta: std::collections::HashMap<u32, Vec<u8>>,
    /// The chain page at which each record's tail starts.
    pub starts: Vec<u64>,
    /// The store has finished its reads into the windows (its drain).
    pub done: bool,
    /// The requester's exact COPY_OK arrived before the store's drain; the claim
    /// completes when `done` does.
    pub copied: bool,
    pub transfer_released: bool,
}

impl Claim {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            v2::ClaimState::Copied | v2::ClaimState::Failed | v2::ClaimState::Expired
        )
    }

    fn holds_export_slot(&self, source_exited: bool) -> bool {
        match self.export {
            ExportState::Requested | ExportState::Pinned => true,
            ExportState::Ended(Party::Quarantined) => !source_exited,
            _ => false,
        }
    }

    fn unreleased(&self) -> usize {
        self.windows
            .iter()
            .filter(|window| matches!(window.phase, Phase::SourceWrite | Phase::RequesterRead))
            .count()
    }

    fn all_windows_done(&self) -> bool {
        self.windows
            .iter()
            .all(|window| window.phase == Phase::Free)
    }

    fn spec(&self, index: usize) -> v2::WindowSpec {
        let window = &self.windows[index];
        v2::WindowSpec {
            index: window.plan.index,
            region: window.region,
            offset: window.offset,
            length: window.plan.length,
            record: window.plan.record,
            first_page: window.plan.first_page,
            page_count: window.plan.page_count,
            row: window.plan.row,
        }
    }
}

fn refusal_reason(value: i32) -> v2::ClaimReason {
    use v2::ClaimReason as R;
    match R::try_from(value) {
        Ok(
            reason @ (R::NotFound
            | R::GenerationChanged
            | R::ChainGap
            | R::NoHeadroom
            | R::SourceBusy
            | R::InvalidClaim
            | R::Cancelled
            | R::StaleSource
            | R::NoWindows),
        ) => reason,
        _ => R::SourceBusy,
    }
}

impl Core {
    // ------------------------------------------------------------------ helpers

    fn session_key(&self, sid: u64) -> Option<(InstanceId, Nonce)> {
        self.sessions
            .get(&sid)
            .map(|session| (session.instance, session.nonce))
    }

    fn proc_exited_flag(&self, key: &ProcKey) -> bool {
        self.procs.get(key).is_none_or(|proc| proc.exited)
    }

    fn exports_outstanding(&self, instance: InstanceId) -> usize {
        self.claims
            .values()
            .filter(|claim| {
                claim
                    .source_proc
                    .is_some_and(|(source, _)| source == instance)
                    && claim.holds_export_slot(
                        claim
                            .source_proc
                            .as_ref()
                            .is_some_and(|proc| self.proc_exited_flag(proc)),
                    )
            })
            .count()
    }

    fn claims_in_flight(&self, requester: (InstanceId, Nonce)) -> usize {
        self.claims
            .values()
            .filter(|claim| (claim.key.0, claim.key.1) == requester && !claim.is_terminal())
            .count()
    }

    /// Bytes of windows no one may reuse yet because a party that held them has
    /// reported it cannot prove it drained, or has gone without saying.
    pub(super) fn quarantined_bytes(&self) -> u64 {
        let mut total = 0;
        for claim in self.claims.values().filter(|claim| claim.is_terminal()) {
            for window in &claim.windows {
                let (party, proc) = match window.phase {
                    Phase::SourceWrite => (claim.source_fence, claim.source_proc),
                    Phase::RequesterRead => {
                        (claim.requester_fence, Some((claim.key.0, claim.key.1)))
                    }
                    _ => continue,
                };
                let gone = proc.is_some_and(|proc| {
                    !self
                        .sessions
                        .values()
                        .any(|session| (session.instance, session.nonce) == proc)
                });
                if party == Party::Quarantined || (party == Party::Pending && gone) {
                    total += window.plan.length;
                }
            }
        }
        total
    }

    fn terminal_message(&self, claim: &Claim) -> v2::ClaimTerminal {
        v2::ClaimTerminal {
            claim_id: claim.key.2,
            state: claim.state as i32,
            reason: claim.reason as i32,
            fence_required: claim.granted_at.is_some(),
        }
    }

    // ------------------------------------------------------------------ claim

    pub(super) fn claim(&mut self, sid: u64, m: v2::Claim) -> Checked<()> {
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        let (instance, nonce) = (session.instance, session.nonce);
        let (scope_id, shm, claim_slots) = (session.scope, session.shm, session.claim_slots);
        let key: ClaimKey = (instance, nonce, m.claim_id);
        if let Some(existing) = self.claims.get(&key) {
            let replied = existing
                .terminal_at
                .is_some_and(|at| at.elapsed() < self.cfg.reply_retention);
            if existing.is_terminal() && replied {
                let message = self.terminal_message(existing);
                self.send(sid, M::ClaimTerminal(message));
            } else if existing.is_terminal() {
                // The reply is gone; the record stays for its fences. A repeat is a replay.
                self.send(
                    sid,
                    M::ClaimTerminal(v2::ClaimTerminal {
                        claim_id: m.claim_id,
                        state: v2::ClaimState::Failed as i32,
                        reason: v2::ClaimReason::InvalidClaim as i32,
                        fence_required: false,
                    }),
                );
            }
            return Ok(());
        }
        let proc = self
            .procs
            .get_mut(&(instance, nonce))
            .ok_or_else(|| Violation::new("claim from an unknown process"))?;
        if m.claim_id == 0 || m.claim_id <= proc.claim_hw {
            // A replay or a reused id: refused without a record.
            self.send(
                sid,
                M::ClaimTerminal(v2::ClaimTerminal {
                    claim_id: m.claim_id,
                    state: v2::ClaimState::Failed as i32,
                    reason: v2::ClaimReason::InvalidClaim as i32,
                    fence_required: false,
                }),
            );
            return Ok(());
        }
        proc.claim_hw = m.claim_id;

        let refuse = |core: &mut Core, reason: v2::ClaimReason| {
            core.refused_claim(sid, key, &m, reason);
            Ok(())
        };
        if !self.ready() {
            return refuse(self, v2::ClaimReason::NotReady);
        }
        if claim_slots == 0 || self.claims_in_flight((instance, nonce)) >= claim_slots as usize {
            return refuse(self, v2::ClaimReason::ClaimLimit);
        }
        let Ok(endpoint) = key_from(m.key.as_ref()) else {
            return refuse(self, v2::ClaimReason::InvalidClaim);
        };
        let tier = match tier_from(m.tier) {
            Ok(tier) => tier,
            Err(_) => return refuse(self, v2::ClaimReason::InvalidClaim),
        };
        let mode = v2::CopyMode::try_from(m.mode).unwrap_or(v2::CopyMode::Unspecified);
        let level = v2::Level::try_from(m.level).unwrap_or(v2::Level::Unspecified);
        if endpoint.consumed_tokens == 0
            || m.begin_tokens >= endpoint.consumed_tokens + u64::from(m.want_row)
            || matches!(level, v2::Level::Unspecified | v2::Level::ThisGpu)
            || mode == v2::CopyMode::Unspecified
            || (mode == v2::CopyMode::SharedWindows && !shm)
            || mode == v2::CopyMode::DataChannel
        {
            return refuse(self, v2::ClaimReason::InvalidClaim);
        }
        if m.begin_tokens > endpoint.consumed_tokens {
            return refuse(self, v2::ClaimReason::InvalidClaim);
        }

        // Resolve the source.
        let source_ref = m.source.clone().unwrap_or_default();
        let (source, source_sid, source_proc) = if source_ref.instance_id.is_empty() {
            (SourceId::Manager, 0, None)
        } else {
            let Ok(id) = InstanceId::try_from(source_ref.instance_id.as_slice()) else {
                return refuse(self, v2::ClaimReason::InvalidClaim);
            };
            if id == instance {
                return refuse(self, v2::ClaimReason::InvalidClaim);
            }
            let Some(entry) = self
                .instances
                .get(&id)
                .filter(|entry| entry.scope == scope_id)
            else {
                return refuse(self, v2::ClaimReason::UnknownSource);
            };
            let Some(current) = entry.current else {
                return refuse(self, v2::ClaimReason::UnknownSource);
            };
            if current != source_ref.session_incarnation {
                return refuse(self, v2::ClaimReason::StaleSource);
            }
            let source_session = &self.sessions[&current];
            if !shm || !source_session.shm {
                return refuse(self, v2::ClaimReason::InvalidClaim);
            }
            (
                SourceId::Instance(id),
                current,
                Some((id, source_session.nonce)),
            )
        };
        let Some(state) = self.scopes.get(&scope_id) else {
            return refuse(self, v2::ClaimReason::UnknownSource);
        };
        let Some(entry) = state.index.get(source, endpoint, tier).cloned() else {
            return refuse(self, v2::ClaimReason::UnknownSource);
        };
        if m.want_row && !entry.row {
            return refuse(self, v2::ClaimReason::InvalidClaim);
        }
        let geometry = state.valid.geometry.clone();
        if let SourceId::Instance(id) = source {
            let slots = self.sessions[&source_sid].export_slots as usize;
            if self.exports_outstanding(id) >= slots {
                return refuse(self, v2::ClaimReason::SourceLimit);
            }
        }
        // The windows the claim will need, from the index's view of the chain.
        let start_page = m.begin_tokens / geometry.page_tokens;
        let Some(walk) = walk_chain(
            &state.index,
            source,
            &entry,
            geometry.page_tokens,
            start_page,
            self.cfg.limits.max_chain_nodes as usize,
        ) else {
            return refuse(self, v2::ClaimReason::ChainGap);
        };
        let shapes = shapes_of(&walk, m.want_row);
        let Some(plan) = windows::plan(
            &shapes,
            geometry.page_bytes,
            geometry.row_bytes,
            self.cfg.limits.max_window_bytes,
        ) else {
            return refuse(self, v2::ClaimReason::InvalidClaim);
        };
        let total = windows::total_bytes(&plan);
        if source == SourceId::Manager {
            let _ = &plan;
            return self.manager_claim(sid, key, &m, endpoint, &entry, walk, &geometry);
        }

        // Retained staging: required for a GPU export, preferred for host tiers.
        let mut block = None;
        let mut retained = false;
        match usize::try_from(total)
            .ok()
            .and_then(|total| self.staging.allocate(total))
        {
            Some(extent) => {
                block = Some(extent);
                retained = true;
            }
            None if tier == Tier::Gpu => return refuse(self, v2::ClaimReason::NoWindows),
            None => {}
        }

        let now = Instant::now();
        let deadline_ms = if m.deadline_ms == 0 {
            self.cfg.limits.max_claim_deadline_ms
        } else {
            m.deadline_ms.min(self.cfg.limits.max_claim_deadline_ms)
        };
        self.next_export += 1;
        let export_id = self.next_export;
        let claim = Claim {
            key,
            requester_sid: sid,
            source,
            source_proc,
            source_sid,
            endpoint,
            want_row: m.want_row,
            state: v2::ClaimState::Requested,
            reason: v2::ClaimReason::Unspecified,
            deadline: now + Duration::from_millis(u64::from(deadline_ms)),
            last_progress: now,
            pin_deadline: now + self.cfg.limits.pin_deadline(),
            export_id,
            export: ExportState::Requested,
            records: Vec::new(),
            total_pages: 0,
            row: false,
            retained,
            reserved_bytes: if retained { total } else { 0 },
            block,
            windows: Vec::new(),
            requester_fence: Party::Idle,
            source_fence: Party::Pending,
            terminal_at: None,
            granted_at: None,
            page_bytes: geometry.page_bytes,
            row_bytes: geometry.row_bytes,
            manager: None,
        };
        let reserved = claim.reserved_bytes;
        self.claims.insert(key, claim);
        self.exports.insert(export_id, key);
        self.send(
            source_sid,
            M::ExportRequest(v2::ExportRequest {
                export_id,
                claim_id: key.2,
                requester_instance_id: instance.to_vec(),
                tier: tier_to(tier) as i32,
                key: Some(key_to(endpoint)),
                generation: m.generation,
                begin_tokens: m.begin_tokens,
                want_row: m.want_row,
                pin_deadline_ms: self.cfg.limits.pin_deadline_ms,
                session_incarnation: source_sid,
                reserved_bytes: reserved,
            }),
        );
        Ok(())
    }

    /// A claim whose source is the manager's own store: the store pins the chain
    /// itself and hands out the windows.
    #[allow(clippy::too_many_arguments)]
    fn manager_claim(
        &mut self,
        sid: u64,
        key: ClaimKey,
        m: &v2::Claim,
        endpoint: Key,
        entry: &super::index::Entry,
        walk: super::index::ChainWalk,
        geometry: &super::ScopeGeometry,
    ) -> Checked<()> {
        let refuse = |core: &mut Core, reason: v2::ClaimReason| {
            core.refused_claim(sid, key, m, reason);
            Ok(())
        };
        if entry.generation != m.generation {
            return refuse(self, v2::ClaimReason::GenerationChanged);
        }
        let (scope, pid) = {
            let session = &self.sessions[&sid];
            (session.scope, session.pid)
        };
        if self.link_for((key.0, key.1), scope, pid).is_none() {
            return refuse(self, v2::ClaimReason::SourceBusy);
        }
        let last = walk.nodes.len() - 1;
        let records: Vec<v2::Record> = walk
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let first_page = if index == 0 { walk.skip_pages } else { 0 };
                v2::Record {
                    key: Some(key_to(node.key)),
                    generation: node.generation,
                    first_page,
                    pages: node.tail_pages - first_page,
                    row: index == last && m.want_row,
                }
            })
            .collect();
        let now = Instant::now();
        let deadline_ms = if m.deadline_ms == 0 {
            self.cfg.limits.max_claim_deadline_ms
        } else {
            m.deadline_ms.min(self.cfg.limits.max_claim_deadline_ms)
        };
        let claim = Claim {
            key,
            requester_sid: sid,
            source: SourceId::Manager,
            source_proc: None,
            source_sid: 0,
            endpoint,
            want_row: m.want_row,
            state: v2::ClaimState::Requested,
            reason: v2::ClaimReason::Unspecified,
            deadline: now + Duration::from_millis(u64::from(deadline_ms)),
            last_progress: now,
            pin_deadline: now + self.cfg.limits.pin_deadline(),
            export_id: 0,
            export: ExportState::None,
            records,
            total_pages: walk.pages,
            row: m.want_row,
            retained: false,
            reserved_bytes: 0,
            block: None,
            windows: Vec::new(),
            requester_fence: Party::Idle,
            source_fence: Party::Idle,
            terminal_at: None,
            granted_at: None,
            page_bytes: geometry.page_bytes,
            row_bytes: geometry.row_bytes,
            manager: Some(ManagerGet {
                checks: Default::default(),
                meta: Default::default(),
                starts: walk
                    .nodes
                    .iter()
                    .map(|node| {
                        (node.key.consumed_tokens / geometry.page_tokens)
                            .saturating_sub(u64::from(node.tail_pages))
                    })
                    .collect(),
                done: false,
                copied: false,
                transfer_released: false,
            }),
        };
        self.claims.insert(key, claim);
        let chain: Vec<Key> = walk.nodes.iter().map(|node| node.key).collect();
        if let Err(reason) = self.manager_get(key, chain, walk.skip_pages, walk.pages, m.want_row) {
            self.finish(key, v2::ClaimState::Failed, reason);
        }
        Ok(())
    }

    /// The store's answers for a claim on the manager's own records.
    pub(super) fn get_answer(&mut self, key: ClaimKey, answer: std::io::Result<GetAnswer>) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        if claim.manager.is_none() {
            return;
        }
        let terminal = claim.is_terminal();
        match answer {
            Ok(GetAnswer::Leased { .. }) => {
                if terminal || claim.state != v2::ClaimState::Requested {
                    return;
                }
                let now = Instant::now();
                claim.state = v2::ClaimState::Granted;
                claim.granted_at = Some(now);
                claim.last_progress = now;
                claim.requester_fence = Party::Pending;
                let granted = v2::ClaimGranted {
                    claim_id: key.2,
                    records: claim.records.clone(),
                    total_pages: claim.total_pages,
                    row: claim.row,
                    channel: None,
                    window_credits: self.cfg.limits.window_credits,
                    retained: false,
                };
                let sid = claim.requester_sid;
                self.send(sid, M::ClaimGranted(granted));
            }
            Ok(GetAnswer::Meta {
                record,
                metadata_offset,
                metadata,
                ..
            }) => {
                // The store sends a record's header and checksum table in 4 KiB pieces.
                if let Some(manager) = claim.manager.as_mut() {
                    let buffer = manager.meta.entry(record).or_default();
                    if buffer.len() as u64 == metadata_offset {
                        buffer.extend_from_slice(&metadata);
                        if let Some(checks) = persisted_checksums(buffer) {
                            manager.checks.insert(record, checks);
                        }
                    }
                }
            }
            Ok(GetAnswer::Ready(grant)) => {
                if terminal {
                    return;
                }
                // The store numbers pages from the start of the chain; the protocol
                // numbers them within a record.
                let start = claim
                    .manager
                    .as_ref()
                    .and_then(|manager| manager.starts.get(grant.record as usize))
                    .copied()
                    .unwrap_or(0);
                let first = grant.page_first.saturating_sub(start) as usize;
                let count = grant.page_count as usize;
                let sums = claim
                    .manager
                    .as_ref()
                    .and_then(|manager| manager.checks.get(&grant.record))
                    .and_then(|(pages, row_sum)| {
                        pages
                            .get(first..first + count)
                            .map(|pages| (pages.to_vec(), *row_sum))
                    });
                let Some((page_xxh3, row_xxh3)) = sums else {
                    self.cancel_get_and_fail(key, v2::ClaimReason::CopyError);
                    return;
                };
                let plan = PlannedWindow {
                    index: grant.index,
                    record: grant.record,
                    first_page: first as u32,
                    page_count: grant.page_count,
                    row: grant.row,
                    length: grant.length,
                };
                let window = Win {
                    plan,
                    region: grant.region,
                    offset: grant.offset,
                    phase: Phase::RequesterRead,
                    extent: None,
                    page_xxh3: page_xxh3.clone(),
                    row_xxh3,
                };
                let index = grant.index as usize;
                while claim.windows.len() <= index {
                    // A window index the store skipped is not in anyone's hands.
                    let mut filler = Win {
                        plan,
                        region: grant.region,
                        offset: 0,
                        phase: Phase::Free,
                        extent: None,
                        page_xxh3: Vec::new(),
                        row_xxh3: 0,
                    };
                    filler.plan.index = claim.windows.len() as u32;
                    claim.windows.push(filler);
                }
                claim.windows[index] = window;
                claim.state = v2::ClaimState::Copying;
                claim.last_progress = Instant::now();
                let spec = claim.spec(index);
                let sid = claim.requester_sid;
                self.send(
                    sid,
                    M::WindowReady(v2::WindowReady {
                        claim_id: key.2,
                        window: Some(spec),
                        page_xxh3,
                        row_xxh3,
                        has_row: grant.row,
                    }),
                );
            }
            Ok(GetAnswer::Done) => {
                let mut copied = false;
                if let Some(manager) = claim.manager.as_mut() {
                    manager.done = true;
                    copied = manager.copied;
                }
                if copied && !terminal {
                    self.finish(key, v2::ClaimState::Copied, v2::ClaimReason::ReasonNone);
                }
            }
            Ok(GetAnswer::Refused(refused)) => {
                let reason = match refused.reason {
                    // BUDGET, OVERSIZE: nothing to read into.
                    3 | 4 => v2::ClaimReason::NoWindows,
                    // IO: the record could not be read intact.
                    8 => v2::ClaimReason::CopyError,
                    _ => v2::ClaimReason::NotFound,
                };
                self.finish(key, v2::ClaimState::Failed, reason);
            }
            Err(_) => self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError),
        }
    }

    fn cancel_get_and_fail(&mut self, key: ClaimKey, reason: v2::ClaimReason) {
        self.finish(key, v2::ClaimState::Failed, reason);
    }

    /// Records a claim refused before any export and tells the requester.
    fn refused_claim(&mut self, sid: u64, key: ClaimKey, m: &v2::Claim, reason: v2::ClaimReason) {
        let now = Instant::now();
        let endpoint = key_from(m.key.as_ref()).unwrap_or_default();
        let claim = Claim {
            key,
            requester_sid: sid,
            source: SourceId::Manager,
            source_proc: None,
            source_sid: 0,
            endpoint,
            want_row: m.want_row,
            state: v2::ClaimState::Failed,
            reason,
            deadline: now,
            last_progress: now,
            pin_deadline: now,
            export_id: 0,
            export: ExportState::None,
            records: Vec::new(),
            total_pages: 0,
            row: false,
            retained: false,
            reserved_bytes: 0,
            block: None,
            windows: Vec::new(),
            requester_fence: Party::Idle,
            source_fence: Party::Idle,
            terminal_at: Some(now),
            granted_at: None,
            page_bytes: 0,
            row_bytes: 0,
            manager: None,
        };
        let message = self.terminal_message(&claim);
        self.claims.insert(key, claim);
        self.send(sid, M::ClaimTerminal(message));
    }

    // ------------------------------------------------------------------ pin

    pub(super) fn pin_ack(&mut self, sid: u64, ack: v2::PinAck) -> Checked<()> {
        let Some(&key) = self.exports.get(&ack.export_id) else {
            return Ok(());
        };
        let Some(sender) = self.session_key(sid) else {
            return Ok(());
        };
        let Some(claim) = self.claims.get_mut(&key) else {
            return Ok(());
        };
        if claim.source_proc != Some(sender) || claim.export != ExportState::Requested {
            // Not this source's export, or an answer already counted.
            return Ok(());
        }
        let result = v2::PinResult::try_from(ack.result).unwrap_or(v2::PinResult::Unspecified);
        match result {
            v2::PinResult::Refused => {
                claim.export = ExportState::Refused;
                claim.source_fence = Party::Idle;
                let reason = refusal_reason(ack.refusal);
                self.finish(key, v2::ClaimState::Failed, reason);
            }
            v2::PinResult::Pinned => {
                claim.export = ExportState::Pinned;
                if claim.is_terminal() {
                    // A late pin: release it.
                    claim.source_fence = Party::Pending;
                    self.cancel_export(key, claim_reason_cancelled());
                    return Ok(());
                }
                self.grant(key, ack);
            }
            v2::PinResult::Unspecified => {
                return Err(Violation::new("PinAck without a result"));
            }
        }
        Ok(())
    }

    /// The source pinned the chain: validate what it says, carve windows, grant.
    fn grant(&mut self, key: ClaimKey, ack: v2::PinAck) {
        let limits = self.cfg.limits.clone();
        let claim = self.claims.get(&key).expect("claim exists");
        let bad = |core: &mut Core, reason: v2::ClaimReason| {
            core.finish(key, v2::ClaimState::Failed, reason);
        };
        if ack.records.is_empty() || ack.records.len() > limits.max_chain_nodes as usize {
            return bad(self, v2::ClaimReason::CopyError);
        }
        let mut shapes = Vec::with_capacity(ack.records.len());
        let mut pages = 0u64;
        for (index, record) in ack.records.iter().enumerate() {
            let last = index + 1 == ack.records.len();
            if record.row && !last || record.row != (last && claim.want_row) && last {
                return bad(self, v2::ClaimReason::CopyError);
            }
            if key_from(record.key.as_ref()).is_err() {
                return bad(self, v2::ClaimReason::CopyError);
            }
            pages += u64::from(record.pages);
            shapes.push(RecordShape {
                first_page: record.first_page,
                pages: record.pages,
                row: record.row,
            });
        }
        let last_key = key_from(ack.records.last().and_then(|record| record.key.as_ref()));
        if last_key.ok() != Some(claim.endpoint)
            || pages != ack.total_pages
            || ack.row != claim.want_row
        {
            return bad(self, v2::ClaimReason::CopyError);
        }
        let Some(plan) = windows::plan(
            &shapes,
            claim.page_bytes,
            claim.row_bytes,
            limits.max_window_bytes,
        ) else {
            return bad(self, v2::ClaimReason::CopyError);
        };
        let total = windows::total_bytes(&plan);
        let claim = self.claims.get_mut(&key).expect("claim exists");
        if claim.retained {
            let capacity = claim
                .block
                .as_ref()
                .map_or(0, |block| block.capacity() as u64);
            if total > claim.reserved_bytes || total > capacity {
                return bad(self, v2::ClaimReason::NoWindows);
            }
        }
        let base = claim
            .block
            .as_ref()
            .map_or(0, |block| block.offset() as u64);
        let mut running = 0u64;
        claim.windows = plan
            .into_iter()
            .map(|plan| {
                let offset = base + running;
                if claim.retained {
                    running += plan.length;
                }
                Win {
                    plan,
                    region: v2::Region::Staging as i32,
                    offset,
                    phase: Phase::Unissued,
                    extent: None,
                    page_xxh3: Vec::new(),
                    row_xxh3: 0,
                }
            })
            .collect();
        claim.records = ack.records.clone();
        claim.total_pages = ack.total_pages;
        claim.row = ack.row;
        claim.state = v2::ClaimState::SourcePinned;
        let now = Instant::now();
        claim.granted_at = Some(now);
        claim.last_progress = now;
        claim.requester_fence = Party::Pending;
        let requester_sid = claim.requester_sid;
        let granted = v2::ClaimGranted {
            claim_id: key.2,
            records: ack.records,
            total_pages: ack.total_pages,
            row: ack.row,
            channel: None,
            window_credits: limits.window_credits,
            retained: claim.retained,
        };
        claim.state = v2::ClaimState::Granted;
        self.send(requester_sid, M::ClaimGranted(granted));
        self.issue_windows(key);
    }

    /// Hands out windows to the source: all of them for a retained claim, as many as
    /// the credits allow otherwise.
    fn issue_windows(&mut self, key: ClaimKey) {
        let credits = self.cfg.limits.window_credits as usize;
        let staging = self.staging.clone();
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        if claim.is_terminal() {
            return;
        }
        let mut specs = Vec::new();
        let mut starved = false;
        for index in 0..claim.windows.len() {
            if claim.windows[index].phase != Phase::Unissued {
                continue;
            }
            if !claim.retained {
                if claim.unreleased() >= credits {
                    break;
                }
                let length = claim.windows[index].plan.length as usize;
                match staging.allocate(length) {
                    Some(extent) => {
                        claim.windows[index].offset = extent.offset() as u64;
                        claim.windows[index].extent = Some(extent);
                    }
                    None => {
                        starved = true;
                        break;
                    }
                }
            }
            claim.windows[index].phase = Phase::SourceWrite;
            specs.push(claim.spec(index));
        }
        let outstanding = claim.unreleased();
        let (export_id, source_sid) = (claim.export_id, claim.source_sid);
        if specs.is_empty() && starved && outstanding == 0 {
            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::NoWindows);
            return;
        }
        if !specs.is_empty() {
            self.send(
                source_sid,
                M::ExportFill(v2::ExportFill {
                    export_id,
                    windows: specs,
                }),
            );
        }
    }

    // ------------------------------------------------------------------ windows

    pub(super) fn window_filled(&mut self, sid: u64, m: v2::WindowFilled) -> Checked<()> {
        let Some(&key) = self.exports.get(&m.export_id) else {
            return Ok(());
        };
        let Some(sender) = self.session_key(sid) else {
            return Ok(());
        };
        let Some(claim) = self.claims.get_mut(&key) else {
            return Ok(());
        };
        if claim.source_proc != Some(sender) {
            return Ok(());
        }
        let index = m.index as usize;
        let terminal = claim.is_terminal();
        let Some(window) = claim.windows.get_mut(index) else {
            return Err(Violation::new(
                "WindowFilled names a window that was never issued",
            ));
        };
        match window.phase {
            Phase::SourceWrite => {}
            Phase::Unissued => {
                return Err(Violation::new("WindowFilled before the window was issued"));
            }
            // A duplicate, or a window already released.
            Phase::RequesterRead | Phase::Free => return Ok(()),
        }
        let status = v2::FillStatus::try_from(m.status).unwrap_or(v2::FillStatus::Unspecified);
        if status == v2::FillStatus::FillFailed {
            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            return Ok(());
        }
        if status != v2::FillStatus::FillOk {
            return Err(Violation::new("WindowFilled without a status"));
        }
        let plan = window.plan;
        if m.page_xxh3.len() != plan.page_count as usize || m.has_row != plan.row {
            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            return Ok(());
        }
        if terminal {
            // The source's write is over; no one read it.
            window.phase = Phase::Free;
            window.extent = None;
            self.reclaim(key);
            return Ok(());
        }
        window.phase = Phase::RequesterRead;
        window.page_xxh3 = m.page_xxh3.clone();
        window.row_xxh3 = m.row_xxh3;
        claim.state = v2::ClaimState::Copying;
        claim.last_progress = Instant::now();
        let spec = claim.spec(index);
        let requester_sid = claim.requester_sid;
        self.send(
            requester_sid,
            M::WindowReady(v2::WindowReady {
                claim_id: key.2,
                window: Some(spec),
                page_xxh3: m.page_xxh3,
                row_xxh3: m.row_xxh3,
                has_row: m.has_row,
            }),
        );
        Ok(())
    }

    pub(super) fn window_released(&mut self, sid: u64, m: v2::WindowReleased) -> Checked<()> {
        let Some((instance, nonce)) = self.session_key(sid) else {
            return Ok(());
        };
        let key = (instance, nonce, m.claim_id);
        let Some(claim) = self.claims.get_mut(&key) else {
            return Ok(());
        };
        let Some(window) = claim.windows.get_mut(m.index as usize) else {
            return Err(Violation::new(
                "WindowReleased names a window that was never issued",
            ));
        };
        match window.phase {
            Phase::RequesterRead => {
                window.phase = Phase::Free;
                window.extent = None;
            }
            Phase::Free => return Ok(()),
            Phase::SourceWrite | Phase::Unissued => {
                return Err(Violation::new(
                    "WindowReleased before the window was filled",
                ));
            }
        }
        claim.last_progress = Instant::now();
        let from_store = claim.manager.is_some();
        let terminal = claim.is_terminal();
        if from_store {
            self.store_release_window(key, m.index);
        }
        if terminal {
            self.reclaim(key);
        } else if !from_store {
            self.issue_windows(key);
        }
        Ok(())
    }

    pub(super) fn copy_result(&mut self, sid: u64, m: v2::CopyResult) -> Checked<()> {
        let Some((instance, nonce)) = self.session_key(sid) else {
            return Ok(());
        };
        let key = (instance, nonce, m.claim_id);
        let Some(claim) = self.claims.get(&key) else {
            return Ok(());
        };
        if claim.is_terminal() {
            return Ok(());
        }
        let outcome = v2::CopyOutcome::try_from(m.outcome).unwrap_or(v2::CopyOutcome::Unspecified);
        match outcome {
            v2::CopyOutcome::CopyOk => {
                // Exact coverage: every window issued, filled and released.
                let covered = claim.state == v2::ClaimState::Copying
                    || claim.state == v2::ClaimState::Granted;
                let pages: u64 = claim
                    .windows
                    .iter()
                    .map(|window| u64::from(window.plan.page_count))
                    .sum();
                let exact = pages == claim.total_pages
                    && (!claim.want_row || claim.windows.iter().any(|window| window.plan.row));
                let complete =
                    covered && exact && claim.all_windows_done() && !claim.windows.is_empty();
                let store_done = claim.manager.as_ref().is_none_or(|manager| manager.done);
                if complete && store_done {
                    self.finish(key, v2::ClaimState::Copied, v2::ClaimReason::ReasonNone);
                } else if complete {
                    // The copy is exact; the store has not yet reported that its own reads
                    // into the windows have drained. Completing waits for that.
                    if let Some(manager) = self
                        .claims
                        .get_mut(&key)
                        .and_then(|claim| claim.manager.as_mut())
                    {
                        manager.copied = true;
                    }
                } else {
                    self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
                }
            }
            v2::CopyOutcome::CopyChecksum => {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Checksum);
            }
            v2::CopyOutcome::CopyErrorOutcome => {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            }
            v2::CopyOutcome::CopyAbandoned => {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Cancelled);
            }
            v2::CopyOutcome::Unspecified => {
                return Err(Violation::new("CopyResult without an outcome"));
            }
        }
        Ok(())
    }

    pub(super) fn claim_release(&mut self, sid: u64, claim_id: u64) -> Checked<()> {
        let Some((instance, nonce)) = self.session_key(sid) else {
            return Ok(());
        };
        let key = (instance, nonce, claim_id);
        match self.claims.get(&key) {
            None => {
                self.send(
                    sid,
                    M::ClaimTerminal(v2::ClaimTerminal {
                        claim_id,
                        state: v2::ClaimState::Failed as i32,
                        reason: v2::ClaimReason::UnknownClaim as i32,
                        fence_required: false,
                    }),
                );
            }
            Some(claim) if claim.is_terminal() => {
                // Answered again while the reply is kept; a no-op afterwards.
                if claim
                    .terminal_at
                    .is_some_and(|at| at.elapsed() < self.cfg.reply_retention)
                {
                    let message = self.terminal_message(claim);
                    self.send(sid, M::ClaimTerminal(message));
                }
            }
            Some(_) => {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Cancelled);
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------ fences

    pub(super) fn fence_ack(&mut self, sid: u64, m: v2::FenceAck) -> Checked<()> {
        let outcome =
            v2::FenceOutcome::try_from(m.outcome).unwrap_or(v2::FenceOutcome::Unspecified);
        let party = match outcome {
            v2::FenceOutcome::Drained => Party::Drained,
            v2::FenceOutcome::Quarantined => Party::Quarantined,
            v2::FenceOutcome::Unspecified => {
                return Err(Violation::new("FenceAck without an outcome"));
            }
        };
        let Some((instance, nonce)) = self.session_key(sid) else {
            return Ok(());
        };
        match m.subject {
            Some(v2::fence_ack::Subject::ClaimId(claim_id)) => {
                let key = (instance, nonce, claim_id);
                if let Some(claim) = self.claims.get_mut(&key) {
                    if claim.requester_fence == Party::Pending
                        || claim.requester_fence == Party::Quarantined && party == Party::Drained
                    {
                        claim.requester_fence = party;
                    }
                    self.reclaim(key);
                }
                Ok(())
            }
            Some(v2::fence_ack::Subject::PutId(put_id)) => {
                self.put_fence(instance, nonce, put_id, party);
                Ok(())
            }
            None => Err(Violation::new("FenceAck without a subject")),
        }
    }

    pub(super) fn export_end(&mut self, sid: u64, m: v2::ExportEnd) -> Checked<()> {
        let Some(&key) = self.exports.get(&m.export_id) else {
            return Ok(());
        };
        let Some(sender) = self.session_key(sid) else {
            return Ok(());
        };
        let Some(claim) = self.claims.get_mut(&key) else {
            return Ok(());
        };
        if claim.source_proc != Some(sender) {
            return Ok(());
        }
        let fence = v2::FenceOutcome::try_from(m.fence).unwrap_or(v2::FenceOutcome::Unspecified);
        let party = match fence {
            v2::FenceOutcome::Drained => Party::Drained,
            v2::FenceOutcome::Quarantined => Party::Quarantined,
            v2::FenceOutcome::Unspecified => {
                return Err(Violation::new("ExportEnd without a fence"));
            }
        };
        match claim.export {
            ExportState::Requested | ExportState::Pinned => {}
            ExportState::Ended(Party::Quarantined) if party == Party::Drained => {}
            _ => return Ok(()),
        }
        claim.export = ExportState::Ended(party);
        claim.source_fence = party;
        if !claim.is_terminal() {
            let status =
                v2::ExportEndStatus::try_from(m.status).unwrap_or(v2::ExportEndStatus::Unspecified);
            let finished = status == v2::ExportEndStatus::ExportComplete
                && claim
                    .windows
                    .iter()
                    .all(|window| matches!(window.phase, Phase::RequesterRead | Phase::Free));
            if !finished {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
                return Ok(());
            }
        }
        self.reclaim(key);
        Ok(())
    }

    // ------------------------------------------------------------------ terminal

    /// The single authoritative transition. The first call for a claim wins.
    pub(super) fn finish(&mut self, key: ClaimKey, state: v2::ClaimState, reason: v2::ClaimReason) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        if claim.is_terminal() {
            return;
        }
        let now = Instant::now();
        claim.state = state;
        claim.reason = reason;
        claim.terminal_at = Some(now);
        if state == v2::ClaimState::Copied {
            // Update the source's cost estimate.
            if let (Some(granted), pages) = (claim.granted_at, claim.total_pages) {
                if pages > 0 {
                    let micros = (now.duration_since(granted).as_micros() as u64 / pages).max(1);
                    let sample = micros.min(u64::from(u32::MAX)) as u32;
                    let source = claim.source;
                    let entry = self.estimates.entry(source).or_insert(sample);
                    *entry = ((u64::from(*entry) * 7 + u64::from(sample)) / 8).max(1) as u32;
                }
            }
        }
        let requester_gone = self.proc_exited_flag(&(key.0, key.1));
        let source_gone = self
            .claims
            .get(&key)
            .and_then(|claim| claim.source_proc)
            .is_some_and(|proc| self.proc_exited_flag(&proc));
        let claim = self.claims.get_mut(&key).expect("claim exists");
        if claim.granted_at.is_none() {
            claim.requester_fence = Party::Idle;
        } else if requester_gone {
            claim.requester_fence = Party::Drained;
        }
        if source_gone && matches!(claim.export, ExportState::Requested | ExportState::Pinned) {
            claim.export = ExportState::Ended(Party::Drained);
            claim.source_fence = Party::Drained;
        }
        let message = self.terminal_message(self.claims.get(&key).expect("claim exists"));
        let requester_sid = self.claims[&key].requester_sid;
        self.send(requester_sid, M::ClaimTerminal(message));
        self.cancel_export(key, reason);
        if self.claims[&key].manager.is_some() {
            self.store_cancel_get(key);
        }
        self.reclaim(key);
    }

    /// Tells the source to stop and release, if its export is still open.
    pub(super) fn cancel_export(&mut self, key: ClaimKey, reason: v2::ClaimReason) {
        let Some(claim) = self.claims.get(&key) else {
            return;
        };
        if !matches!(claim.export, ExportState::Requested | ExportState::Pinned) {
            return;
        }
        let export_id = claim.export_id;
        let Some((instance, _)) = claim.source_proc else {
            return;
        };
        let Some(sid) = self
            .instances
            .get(&instance)
            .and_then(|entry| entry.current)
        else {
            return;
        };
        // Only a session of the same process can act on the export.
        let same_process = self
            .sessions
            .get(&sid)
            .is_some_and(|session| Some((session.instance, session.nonce)) == claim.source_proc);
        if same_process {
            self.send(
                sid,
                M::ExportCancel(v2::ExportCancel {
                    export_id,
                    reason: reason as i32,
                }),
            );
        }
    }

    /// Frees every window whose last toucher has drained, and drops the claim record
    /// once the reply is no longer needed.
    pub(super) fn reclaim(&mut self, key: ClaimKey) {
        let requester_gone = self.proc_exited_flag(&(key.0, key.1));
        let Some(claim) = self.claims.get(&key) else {
            return;
        };
        if !claim.is_terminal() {
            return;
        }
        let source_gone = claim
            .source_proc
            .as_ref()
            .is_none_or(|proc| self.proc_exited_flag(proc));
        let source_ok = matches!(claim.source_fence, Party::Drained | Party::Idle) || source_gone;
        let requester_ok =
            matches!(claim.requester_fence, Party::Drained | Party::Idle) || requester_gone;
        let claim = self.claims.get_mut(&key).expect("claim exists");
        for window in &mut claim.windows {
            match window.phase {
                Phase::Unissued | Phase::Free => {
                    window.phase = Phase::Free;
                    window.extent = None;
                }
                Phase::SourceWrite if source_ok => {
                    window.phase = Phase::Free;
                    window.extent = None;
                }
                Phase::RequesterRead if requester_ok => {
                    window.phase = Phase::Free;
                    window.extent = None;
                }
                _ => {}
            }
        }
        if claim
            .windows
            .iter()
            .all(|window| window.phase == Phase::Free)
        {
            claim.block = None;
        }
        let release = requester_ok
            && claim
                .manager
                .as_mut()
                .is_some_and(|manager| !std::mem::replace(&mut manager.transfer_released, true));
        if release {
            self.store_release_transfer(key);
        }
    }

    pub(super) fn claim_timers(&mut self, now: Instant) {
        let progress = self.cfg.limits.progress_timeout();
        let retention = self.cfg.reply_retention;
        let mut expire = Vec::new();
        let mut forget = Vec::new();
        for (key, claim) in &self.claims {
            if claim.is_terminal() {
                let settled = claim.block.is_none()
                    && claim
                        .windows
                        .iter()
                        .all(|window| window.phase == Phase::Free)
                    && !matches!(claim.export, ExportState::Requested | ExportState::Pinned)
                    && !matches!(claim.export, ExportState::Ended(Party::Quarantined))
                    && !matches!(claim.requester_fence, Party::Pending | Party::Quarantined)
                    && !matches!(claim.source_fence, Party::Pending);
                let source_gone = claim
                    .source_proc
                    .as_ref()
                    .is_none_or(|proc| self.proc_exited_flag(proc));
                let requester_gone = self.proc_exited_flag(&(key.0, key.1));
                let fenced_by_exit = source_gone && requester_gone && claim.block.is_none();
                if claim
                    .terminal_at
                    .is_some_and(|at| now.duration_since(at) >= retention)
                    && (settled || fenced_by_exit)
                {
                    forget.push(*key);
                }
                continue;
            }
            if now >= claim.deadline {
                expire.push((*key, v2::ClaimReason::Deadline));
            } else if claim.state == v2::ClaimState::Requested && now >= claim.pin_deadline {
                expire.push((*key, v2::ClaimReason::SourceTimeout));
            } else if matches!(
                claim.state,
                v2::ClaimState::Granted | v2::ClaimState::Copying
            ) && now.duration_since(claim.last_progress) >= progress
            {
                expire.push((*key, v2::ClaimReason::NoProgress));
            }
        }
        for (key, reason) in expire {
            let state = if reason == v2::ClaimReason::SourceTimeout {
                v2::ClaimState::Failed
            } else {
                v2::ClaimState::Expired
            };
            self.finish(key, state, reason);
        }
        for key in forget {
            if let Some(claim) = self.claims.remove(&key) {
                self.exports.remove(&claim.export_id);
            }
        }
    }

    // ------------------------------------------------------------------ processes

    /// A process is confirmed gone: nothing it did can touch memory any more.
    pub(super) fn proc_exited(&mut self, process: ProcKey) {
        let keys: Vec<ClaimKey> = self.claims.keys().copied().collect();
        for key in keys {
            let claim = self.claims.get_mut(&key).expect("claim exists");
            let mut touched = false;
            if (key.0, key.1) == process {
                if matches!(claim.requester_fence, Party::Pending | Party::Quarantined) {
                    claim.requester_fence = Party::Drained;
                }
                touched = true;
            }
            if claim.source_proc == Some(process) {
                if matches!(claim.export, ExportState::Requested | ExportState::Pinned) {
                    claim.export = ExportState::Ended(Party::Drained);
                }
                if matches!(claim.export, ExportState::Ended(Party::Quarantined)) {
                    claim.export = ExportState::Ended(Party::Drained);
                }
                if matches!(claim.source_fence, Party::Pending | Party::Quarantined) {
                    claim.source_fence = Party::Drained;
                }
                touched = true;
            }
            if touched {
                self.reclaim(key);
            }
        }
        self.puts_proc_exited(process);
    }

    /// `Goodbye{quiesced}`: the process cleared its own obligations.
    pub(super) fn party_quiesced(&mut self, process: ProcKey) {
        let keys: Vec<ClaimKey> = self.claims.keys().copied().collect();
        for key in keys {
            let claim = self.claims.get_mut(&key).expect("claim exists");
            let mut touched = false;
            if (key.0, key.1) == process && claim.requester_fence == Party::Pending {
                claim.requester_fence = Party::Drained;
                touched = true;
            }
            if claim.source_proc == Some(process) {
                if claim.source_fence == Party::Pending {
                    claim.source_fence = Party::Drained;
                    touched = true;
                }
                if matches!(claim.export, ExportState::Requested | ExportState::Pinned) {
                    claim.export = ExportState::Ended(Party::Drained);
                    touched = true;
                }
            }
            if touched {
                self.reclaim(key);
            }
        }
        self.puts_quiesced(process);
    }

    /// A session ended: its claims, as requester and as source, fail.
    pub(super) fn session_claims_lost(&mut self, sid: u64, session: &Session) {
        let keys: Vec<(ClaimKey, bool)> = self
            .claims
            .iter()
            .filter(|(_, claim)| !claim.is_terminal())
            .filter_map(|(key, claim)| {
                if claim.requester_sid == sid {
                    Some((*key, true))
                } else if claim.source_sid == sid && claim.source_sid != 0 {
                    Some((*key, false))
                } else {
                    None
                }
            })
            .collect();
        for (key, as_requester) in keys {
            if as_requester {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::RequesterLost);
            } else {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::SourceLost);
            }
        }
        // The process may already be gone.
        let process = (session.instance, session.nonce);
        if self.proc_exited_flag(&process) {
            self.proc_exited(process);
        }
    }
}

fn claim_reason_cancelled() -> v2::ClaimReason {
    v2::ClaimReason::Cancelled
}

/// Record shapes for the index's view of a chain.
fn shapes_of(walk: &super::index::ChainWalk, want_row: bool) -> Vec<RecordShape> {
    let last = walk.nodes.len() - 1;
    walk.nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let first_page = if index == 0 { walk.skip_pages } else { 0 };
            RecordShape {
                first_page,
                pages: node.tail_pages - first_page,
                row: want_row && index == last,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arena::Arena,
        pool::core::{Core, PoolConfig, Session},
    };
    use std::collections::HashSet;
    use tokio::sync::mpsc;

    const INSTANCE: InstanceId = [1; 16];
    const NONCE: Nonce = [2; 16];

    fn core() -> (Core, mpsc::Receiver<v2::ServerMessage>) {
        let (mut core, _handle, _events) = Core::new(
            PoolConfig::default(),
            v2::SharedMemory::default(),
            Arena::new(1 << 20).unwrap(),
            true,
            0,
            None,
        );
        let (tx, rx) = mpsc::channel(64);
        core.sessions.insert(
            1,
            Session {
                sid: 1,
                instance: INSTANCE,
                nonce: NONCE,
                scope: [3; 32],
                tx,
                shm: true,
                pid: 1,
                export_slots: 2,
                claim_slots: 2,
                need_resync: false,
                resync_requested: false,
                expected_next: 1,
                staged: None,
                lookups: HashSet::new(),
                goodbye: None,
                installed_once: true,
                overflowed: false,
            },
        );
        (core, rx)
    }

    /// A claim on the manager's store whose five pages and row were all delivered and
    /// released.
    fn delivered(core: &mut Core) -> ClaimKey {
        let key: ClaimKey = (INSTANCE, NONCE, 1);
        let plan = PlannedWindow {
            index: 0,
            record: 0,
            first_page: 0,
            page_count: 5,
            row: true,
            length: 6 * 4096,
        };
        let now = Instant::now();
        core.claims.insert(
            key,
            Claim {
                key,
                requester_sid: 1,
                source: SourceId::Manager,
                source_proc: None,
                source_sid: 0,
                endpoint: Key::default(),
                want_row: true,
                state: v2::ClaimState::Copying,
                reason: v2::ClaimReason::Unspecified,
                deadline: now + Duration::from_secs(30),
                last_progress: now,
                pin_deadline: now,
                export_id: 0,
                export: ExportState::None,
                records: Vec::new(),
                total_pages: 5,
                row: true,
                retained: false,
                reserved_bytes: 0,
                block: None,
                windows: vec![Win {
                    plan,
                    region: v2::Region::Staging as i32,
                    offset: 0,
                    phase: Phase::Free,
                    extent: None,
                    page_xxh3: Vec::new(),
                    row_xxh3: 0,
                }],
                requester_fence: Party::Pending,
                source_fence: Party::Idle,
                terminal_at: None,
                granted_at: Some(now),
                page_bytes: 4096,
                row_bytes: 4096,
                manager: Some(ManagerGet {
                    checks: Default::default(),
                    meta: Default::default(),
                    starts: vec![0],
                    done: false,
                    copied: false,
                    transfer_released: false,
                }),
            },
        );
        key
    }

    fn copy_ok() -> v2::CopyResult {
        v2::CopyResult {
            claim_id: 1,
            outcome: v2::CopyOutcome::CopyOk as i32,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn copy_ok_before_the_stores_drain_completes_when_it_arrives() {
        let (mut core, _out) = core();
        let key = delivered(&mut core);
        core.copy_result(1, copy_ok()).unwrap();
        assert!(
            !core.claims[&key].is_terminal(),
            "waits for the store's drain"
        );
        core.get_answer(key, Ok(GetAnswer::Done));
        assert_eq!(core.claims[&key].state, v2::ClaimState::Copied);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn copy_ok_after_the_stores_drain_completes_at_once() {
        let (mut core, _out) = core();
        let key = delivered(&mut core);
        core.get_answer(key, Ok(GetAnswer::Done));
        assert!(!core.claims[&key].is_terminal());
        core.copy_result(1, copy_ok()).unwrap();
        assert_eq!(core.claims[&key].state, v2::ClaimState::Copied);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_store_error_after_an_early_copy_ok_fails_the_claim() {
        let (mut core, _out) = core();
        let key = delivered(&mut core);
        core.copy_result(1, copy_ok()).unwrap();
        core.get_answer(
            key,
            Ok(GetAnswer::Refused(crate::store::Refused {
                reason: 8,
                detail: "disk read failed".into(),
            })),
        );
        assert_eq!(core.claims[&key].state, v2::ClaimState::Failed);
        assert_eq!(core.claims[&key].reason, v2::ClaimReason::CopyError);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_inexact_copy_ok_still_fails_whatever_the_store_says() {
        let (mut core, _out) = core();
        let key = delivered(&mut core);
        core.claims.get_mut(&key).unwrap().total_pages = 6;
        core.get_answer(key, Ok(GetAnswer::Done));
        core.copy_result(1, copy_ok()).unwrap();
        assert_eq!(core.claims[&key].state, v2::ClaimState::Failed);
    }
}
