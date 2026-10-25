//! Claims, exports, windows and fences (PROTOCOL.md sections 5 to 8).
//!
//! Every claim is one record, changed only by the actor, so "one authoritative
//! transition" is a plain state check. Windows are carved from the staging region
//! and are reclaimed only when every party that could have touched them has
//! drained, or is confirmed gone.

use super::{
    Checked, Violation,
    core::{ChanOwner, Core, Nonce, ProcKey, Session},
    index::{InstanceId, SourceId, Tier, walk_chain},
    key_from, key_to,
    mp::{MpState, Stated},
    store_bridge::persisted_checksums,
    tier_from, tier_to,
    windows::{self, PlannedWindow, RecordShape},
};
use crate::{
    Key,
    arena::Extent,
    data::channel,
    proto::v2::{self, server_message::M},
    store::GetAnswer,
};
use flare_selene::pool::{SliceSums, frame::Shape};
use std::time::{Duration, Instant};

pub type ClaimKey = (InstanceId, Nonce, u64);

/// One party's drain obligation for a claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Party {
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
    /// Readers that must finish with the window before it is free, one bit per accessor:
    /// bit 0 is the requester's session (or its own leg), bit `r` the leg of rank `r`.
    pub readers: u64,
    /// Writers that must finish with the window before it is whole, one bit per accessor like
    /// `readers`: bit 0 is the source's session (rank 0 and its shared peers) or its own leg.
    pub writers: u64,
    /// The slice checksums a multi-process source's ranks stated, by rank.
    pub stated: Vec<Option<SliceSums>>,
    /// The window's contents have been handed to the pass (or delivered): a repeat is ignored.
    pub arrived: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExportState {
    /// The claim has no source export (a refusal before any request).
    None,
    Requested,
    Pinned,
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
    /// The requester reported exact coverage; completion still waits for every drain.
    pub verdict_latched: bool,
    pub granted_at: Option<Instant>,
    pub page_bytes: u64,
    pub row_bytes: u64,
    /// Set when the manager's own store is the source.
    pub manager: Option<ManagerGet>,
    /// Set when the bytes move to the requester on the data channel instead of shared windows.
    pub channel: Option<Channel>,
    /// Set when the source has no shared memory: it writes the windows to the manager on the
    /// data channel and the manager relays them on.
    pub source_channel: Option<Channel>,
    /// What multi-process instances add: peer participants, their legs, the assembly pass.
    pub mp: MpState,
}

/// A claim's data-channel transfer. The manager's handler is the only party that
/// reads the claim's windows, so its exit is what lets them go back to the store.
pub struct Channel {
    pub transfer: u64,
    /// What the requester presents to be served this transfer.
    pub token: [u8; flare_selene::pool::frame::TOKEN_BYTES],
    /// The handler has exited (or never ran) and its connection is closed.
    pub exited: bool,
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
    pub transfer_released: bool,
}

impl Claim {
    pub fn is_terminal(&self) -> bool {
        matches!(self.state, v2::ClaimState::Copied | v2::ClaimState::Failed)
    }

    fn source_fence(&self) -> Party {
        match self.export {
            ExportState::None => Party::Drained,
            ExportState::Requested | ExportState::Pinned => Party::Pending,
            ExportState::Ended(party) => party,
        }
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

    /// Some peer rank's leg has not finished reading a window the requester has.
    pub(super) fn legs_pending(&self) -> bool {
        self.windows
            .iter()
            .any(|w| w.phase == Phase::RequesterRead && w.readers & !1 != 0)
    }

    fn all_windows_done(&self) -> bool {
        self.windows
            .iter()
            .all(|window| window.phase == Phase::Free)
    }

    pub(super) fn spec(&self, index: usize) -> v2::WindowSpec {
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
            let stuck = claim.channel.as_ref().is_some_and(|open| !open.exited);
            let stuck_source = claim
                .source_channel
                .as_ref()
                .is_some_and(|open| !open.exited);
            let stuck_rank = claim
                .mp
                .legs
                .iter()
                .any(|leg| !leg.source && !leg.ch.exited);
            let stuck_source_rank = claim.mp.legs.iter().any(|leg| leg.source && !leg.ch.exited);
            let stuck_source_peer = claim.mp.src.as_ref().is_some_and(|side| {
                side.peers.iter().any(|peer| {
                    peer.shared
                        && (peer.fence == Party::Quarantined
                            || (peer.fence == Party::Pending
                                && !self.agents.values().any(|a| a.proc() == peer.proc)
                                && !self.proc_exited_flag(&peer.proc)))
                })
            });
            // A shared-memory peer that cannot show it stopped, or that is gone unseen.
            let stuck_peer = claim.mp.req.as_ref().is_some_and(|side| {
                side.peers.iter().any(|peer| {
                    peer.shared
                        && (peer.fence == Party::Quarantined
                            || (peer.fence == Party::Pending
                                && !self.agents.values().any(|a| a.proc() == peer.proc)
                                && !self.proc_exited_flag(&peer.proc)))
                })
            });
            for window in &claim.windows {
                if (stuck || stuck_rank || stuck_peer) && window.phase == Phase::RequesterRead
                    || (stuck_source || stuck_source_rank || stuck_source_peer)
                        && window.phase == Phase::SourceWrite
                {
                    total += window.plan.length;
                    continue;
                }
                let (party, proc) = match window.phase {
                    Phase::SourceWrite => (claim.source_fence(), claim.source_proc),
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
        if !self.ranks_ready(sid) {
            return refuse(self, v2::ClaimReason::RanksDetached);
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
        {
            return refuse(self, v2::ClaimReason::InvalidClaim);
        }
        if mode == v2::CopyMode::DataChannel && self.cfg.channel.is_none() {
            // Claims carry no detail text; the manager's log says why.
            eprintln!(
                "selene: claim {} refused: this manager serves no data channel (it has no network listener)",
                m.claim_id
            );
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
            // A source or a requester without shared memory is relayed through the data
            // channel, which needs a manager that serves one.
            if !self.ranks_ready(current) {
                return refuse(self, v2::ClaimReason::RanksDetached);
            }
            let source_session = &self.sessions[&current];
            if (mode == v2::CopyMode::DataChannel || !source_session.shm)
                && self.cfg.channel.is_none()
            {
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
            return self.manager_claim(sid, key, &m, endpoint, walk, &geometry, mode);
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

        // The legs that cross the network: to a requester without shared windows, and
        // from a source without shared memory.
        let requester_slices = self.requester_slices(sid);
        let source_slices = self.requester_slices(source_sid);
        let requester_leg = if mode == v2::CopyMode::DataChannel {
            match self.open_leg(
                channel::Kind::Get,
                requester_slices.map(|layout| (layout, 0)),
            ) {
                Some(leg) => Some(leg),
                None => return refuse(self, v2::ClaimReason::NoWindows),
            }
        } else {
            None
        };
        let source_leg = if self.sessions[&source_sid].shm {
            None
        } else {
            match self.open_leg(channel::Kind::Put, source_slices.map(|layout| (layout, 0))) {
                Some(leg) => Some(leg),
                None => {
                    self.discard_leg(requester_leg);
                    return refuse(self, v2::ClaimReason::NoWindows);
                }
            }
        };
        if let Some(leg) = &requester_leg {
            self.channels.insert(leg.transfer, ChanOwner::Claim(key));
        }
        if let Some(leg) = &source_leg {
            self.channels.insert(leg.transfer, ChanOwner::Source(key));
        }
        // The requester's peer ranks, when it is a multi-process instance.
        let mut mp_legs = Vec::new();
        let sides = self
            .mp_side(
                sid,
                &|i| ChanOwner::ClaimRank(key, i),
                channel::Kind::Get,
                &mut mp_legs,
            )
            .and_then(|req| {
                self.mp_side(
                    source_sid,
                    &|i| ChanOwner::ClaimRank(key, i),
                    channel::Kind::Put,
                    &mut mp_legs,
                )
                .map(|src| (req, src))
            });
        let (req_mp, src_mp) = match sides {
            Ok(sides) => sides,
            Err(reason) => {
                for leg in [&requester_leg, &source_leg].into_iter().flatten() {
                    self.channels.remove(&leg.transfer);
                }
                self.discard_legs(&mut mp_legs);
                self.discard_leg(requester_leg);
                self.discard_leg(source_leg);
                return refuse(self, reason);
            }
        };
        let source_channel_ref = source_leg
            .as_ref()
            .zip(self.cfg.channel.as_ref())
            .map(|(leg, hub)| super::channel_ref(hub, &leg.token));

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
            requester_fence: Party::Drained,
            verdict_latched: false,
            granted_at: None,
            page_bytes: geometry.page_bytes,
            row_bytes: geometry.row_bytes,
            manager: None,
            channel: requester_leg,
            source_channel: source_leg,
            mp: MpState {
                req: req_mp,
                src: src_mp,
                legs: mp_legs,
                ..Default::default()
            },
        };
        let reserved = claim.reserved_bytes;
        self.claims.insert(key, claim);
        self.exports.insert(export_id, key);
        let source_rank_channels = self.rank_channels_of(key, true);
        self.send(
            source_sid,
            M::ExportRequest(v2::ExportRequest {
                export_id,
                claim_id: key.2,
                requester_instance_id: instance.to_vec(),
                tier: tier_to(tier) as i32,
                key: Some(key_to(endpoint)),
                begin_tokens: m.begin_tokens,
                want_row: m.want_row,
                pin_deadline_ms: self.cfg.limits.pin_deadline_ms,
                session_incarnation: source_sid,
                reserved_bytes: reserved,
                channel: source_channel_ref,
                rank_channels: source_rank_channels,
            }),
        );
        Ok(())
    }

    /// Begin a data-channel transfer for one leg of a claim.
    fn open_leg(
        &mut self,
        kind: channel::Kind,
        rank: Option<(std::sync::Arc<flare_selene::pool::SliceLayout>, usize)>,
    ) -> Option<Channel> {
        let hub = self.cfg.channel.as_ref()?;
        let opened = match rank {
            Some((layout, rank)) => hub.open_rank(kind, layout, rank),
            None => hub.open(kind),
        };
        match opened {
            Ok((transfer, token)) => Some(Channel {
                transfer,
                token,
                exited: false,
            }),
            Err(error) => {
                eprintln!("selene: no data-channel token for a claim: {error}");
                None
            }
        }
    }

    /// The slice layout of the requester's instance when it is a multi-process one.
    pub(super) fn requester_slices(
        &self,
        sid: u64,
    ) -> Option<std::sync::Arc<flare_selene::pool::SliceLayout>> {
        let session = self.sessions.get(&sid)?;
        session.ranks.as_ref()?;
        Some(self.scopes.get(&session.scope)?.valid.slices.clone())
    }

    /// Drop a leg that no one has connected to.
    fn discard_leg(&mut self, leg: Option<Channel>) {
        if let (Some(leg), Some(hub)) = (leg, self.cfg.channel.as_ref()) {
            hub.stop(leg.transfer);
            hub.forget(leg.transfer);
        }
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
        walk: super::index::ChainWalk,
        geometry: &super::ScopeGeometry,
        mode: v2::CopyMode,
    ) -> Checked<()> {
        let refuse = |core: &mut Core, reason: v2::ClaimReason| {
            core.refused_claim(sid, key, m, reason);
            Ok(())
        };
        let (scope, pid) = {
            let session = &self.sessions[&sid];
            (session.scope, session.pid)
        };
        if self.link_for((key.0, key.1), scope, pid).is_none() {
            return refuse(self, v2::ClaimReason::SourceBusy);
        }
        let slices = self.requester_slices(sid);
        let channel = if mode == v2::CopyMode::DataChannel {
            let hub = self.cfg.channel.as_ref().expect("checked by the caller");
            let opened = match &slices {
                Some(layout) => hub.open_rank(channel::Kind::Get, layout.clone(), 0),
                None => hub.open(channel::Kind::Get),
            };
            match opened {
                Ok((transfer, token)) => Some((transfer, token)),
                Err(error) => {
                    eprintln!(
                        "selene: claim {} refused: no data-channel token: {error}",
                        m.claim_id
                    );
                    return refuse(self, v2::ClaimReason::NoWindows);
                }
            }
        } else {
            None
        };
        let mut mp_legs = Vec::new();
        let req_mp = match self.mp_side(
            sid,
            &|i| ChanOwner::ClaimRank(key, i),
            channel::Kind::Get,
            &mut mp_legs,
        ) {
            Ok(side) => side,
            Err(reason) => {
                if let (Some((transfer, _)), Some(hub)) = (&channel, self.cfg.channel.as_ref()) {
                    hub.stop(*transfer);
                    hub.forget(*transfer);
                }
                return refuse(self, reason);
            }
        };
        let last = walk.nodes.len() - 1;
        let records: Vec<v2::Record> = walk
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let first_page = if index == 0 { walk.skip_pages } else { 0 };
                v2::Record {
                    key: Some(key_to(node.key)),
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
            requester_fence: Party::Drained,
            verdict_latched: false,
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
                transfer_released: false,
            }),
            channel: channel.map(|(transfer, token)| Channel {
                transfer,
                token,
                exited: false,
            }),
            source_channel: None,
            mp: MpState {
                req: req_mp,
                legs: mp_legs,
                ..Default::default()
            },
        };
        if let Some(open) = &claim.channel {
            self.channels.insert(open.transfer, ChanOwner::Claim(key));
        }
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
                let channel = claim
                    .channel
                    .as_ref()
                    .zip(self.cfg.channel.as_ref())
                    .map(|(open, hub)| super::channel_ref(hub, &open.token));
                if let Some(side) = claim.mp.req.as_mut() {
                    for peer in &mut side.peers {
                        peer.fence = Party::Pending;
                    }
                }
                let mut granted = v2::ClaimGranted {
                    claim_id: key.2,
                    records: claim.records.clone(),
                    total_pages: claim.total_pages,
                    row: claim.row,
                    channel,
                    window_credits: self.cfg.limits.window_credits,
                    retained: false,
                    rank_channels: Vec::new(),
                };
                let sid = claim.requester_sid;
                granted.rank_channels = self.rank_channels(key);
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
                    readers: 1,
                    writers: 0,
                    stated: Vec::new(),
                    arrived: true,
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
                        readers: 0,
                        writers: 0,
                        stated: Vec::new(),
                        arrived: true,
                    };
                    filler.plan.index = claim.windows.len() as u32;
                    claim.windows.push(filler);
                }
                claim.windows[index] = window;
                if claim.mp.req.is_some() {
                    // A multi-process requester gets its ranks' slices with their own
                    // checksums: the stored sums are verified and those computed in one pass.
                    let window = &mut claim.windows[index];
                    window.phase = Phase::SourceWrite;
                    window.arrived = true;
                    window.readers = 0;
                    self.window_arrived(
                        key,
                        index,
                        Stated::Whole(SliceSums {
                            pages: page_xxh3,
                            row: grant.row.then_some(row_xxh3),
                        }),
                    );
                    return;
                }
                claim.state = v2::ClaimState::Granted;
                claim.last_progress = Instant::now();
                let spec = claim.spec(index);
                let sid = claim.requester_sid;
                if let Some((open, hub)) = claim.channel.as_ref().zip(self.cfg.channel.as_ref()) {
                    // The window goes to the requester on the data channel; the claim
                    // learns it is free when the handler reports it sent.
                    let region = u8::try_from(grant.region)
                        .ok()
                        .and_then(crate::data::Region::from_u8);
                    let Some(region) = region else {
                        self.cancel_get_and_fail(key, v2::ClaimReason::CopyError);
                        return;
                    };
                    hub.push_window(
                        open.transfer,
                        channel::WindowOut {
                            shape: Shape {
                                index: grant.index,
                                record: grant.record,
                                first_page: first as u32,
                                page_count: grant.page_count,
                                row: grant.row,
                                length: grant.length,
                            },
                            page_xxh3,
                            row_xxh3,
                            region,
                            offset: grant.offset,
                        },
                    );
                    return;
                }
                self.send(
                    sid,
                    M::WindowReady(v2::WindowReady {
                        claim_id: key.2,
                        window: Some(spec),
                        page_xxh3,
                        row_xxh3,
                        has_row: grant.row,
                        rank_sums: Vec::new(),
                    }),
                );
            }
            Ok(GetAnswer::Done) => {
                if let Some(manager) = claim.manager.as_mut() {
                    manager.done = true;
                }
                // Every window is queued: the handler ends the transfer after them.
                if let Some((open, hub)) = claim.channel.as_ref().zip(self.cfg.channel.as_ref()) {
                    hub.finish(open.transfer);
                }
                if let Some(hub) = self.cfg.channel.as_ref() {
                    for leg in claim.mp.legs.iter().filter(|leg| !leg.source) {
                        hub.finish(leg.ch.transfer);
                    }
                }
                self.complete_if_latched(key);
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
            requester_fence: Party::Drained,
            verdict_latched: false,
            granted_at: None,
            page_bytes: 0,
            row_bytes: 0,
            manager: None,
            channel: None,
            source_channel: None,
            mp: MpState::default(),
        };
        let message = self.terminal_message(&claim);
        self.claims.insert(key, claim);
        self.count("claims_refused_total", format!("reason=\"{reason:?}\""));
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
                claim.export = ExportState::None;
                let reason = refusal_reason(ack.refusal);
                self.finish(key, v2::ClaimState::Failed, reason);
            }
            v2::PinResult::Pinned => {
                claim.export = ExportState::Pinned;
                if claim.is_terminal() {
                    // A late pin: release it.
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
                    readers: 0,
                    writers: 0,
                    stated: Vec::new(),
                    arrived: false,
                }
            })
            .collect();
        claim.records = ack.records.clone();
        claim.total_pages = ack.total_pages;
        claim.row = ack.row;
        let now = Instant::now();
        claim.granted_at = Some(now);
        claim.last_progress = now;
        claim.requester_fence = Party::Pending;
        let requester_sid = claim.requester_sid;
        let channel = claim
            .channel
            .as_ref()
            .zip(self.cfg.channel.as_ref())
            .map(|(open, hub)| super::channel_ref(hub, &open.token));
        let mut granted = v2::ClaimGranted {
            claim_id: key.2,
            records: ack.records,
            total_pages: ack.total_pages,
            row: ack.row,
            channel,
            window_credits: limits.window_credits,
            retained: claim.retained,
            rank_channels: Vec::new(),
        };
        claim.state = v2::ClaimState::Granted;
        for side in [claim.mp.req.as_mut(), claim.mp.src.as_mut()]
            .into_iter()
            .flatten()
        {
            for peer in &mut side.peers {
                peer.fence = Party::Pending;
            }
        }
        granted.rank_channels = self.rank_channels(key);
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
            let source_peers = claim.mp.src.is_some();
            let writers = if source_peers {
                claim
                    .mp
                    .legs
                    .iter()
                    .filter(|leg| leg.source)
                    .fold(1u64, |mask, leg| mask | 1 << leg.rank)
            } else {
                1
            };
            let world = claim.mp.src.as_ref().map_or(0, |side| side.layout.world());
            claim.windows[index].writers = writers;
            claim.windows[index].stated = vec![None; world];
            specs.push(claim.spec(index));
        }
        let outstanding = claim.unreleased();
        let (export_id, source_sid) = (claim.export_id, claim.source_sid);
        let source_leg = claim.source_channel.as_ref().map(|leg| leg.transfer);
        // A multi-process source's peers in channel mode are granted each window on their own
        // leg, with their own slice's shape.
        let peer_grants: Vec<(u64, u32)> = claim
            .mp
            .legs
            .iter()
            .filter(|leg| leg.source)
            .map(|leg| (leg.ch.transfer, leg.rank))
            .collect();
        let source_layout = claim.mp.src.as_ref().map(|side| side.layout.clone());
        if specs.is_empty() && starved && outstanding == 0 {
            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::NoWindows);
            return;
        }
        if let Some(transfer) = source_leg {
            // The source has no shared memory: it is granted each window on the channel
            // and fills it from the bytes it writes there.
            if let Some(hub) = &self.cfg.channel {
                for spec in &specs {
                    let region = u8::try_from(spec.region)
                        .ok()
                        .and_then(crate::data::Region::from_u8);
                    let Some(region) = region else { continue };
                    hub.push_grant(
                        transfer,
                        channel::GrantOut {
                            shape: shape_for(spec, source_layout.as_deref(), 0),
                            region,
                            offset: spec.offset,
                        },
                    );
                }
            }
        } else if !specs.is_empty() {
            self.send(
                source_sid,
                M::ExportFill(v2::ExportFill {
                    export_id,
                    windows: specs.clone(),
                }),
            );
        }
        if let (Some(hub), Some(layout)) = (&self.cfg.channel, &source_layout) {
            for (transfer, rank) in peer_grants {
                for spec in &specs {
                    let region = u8::try_from(spec.region)
                        .ok()
                        .and_then(crate::data::Region::from_u8);
                    let Some(region) = region else { continue };
                    hub.push_grant(
                        transfer,
                        channel::GrantOut {
                            shape: shape_for(spec, Some(layout), rank as usize),
                            region,
                            offset: spec.offset,
                        },
                    );
                }
            }
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
        if claim.source_proc != Some(sender) || claim.source_channel.is_some() {
            // A relayed source states its windows' checksums on the data channel.
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
        if claim.mp.src.is_some() {
            // A multi-process source states its ranks' slice checksums, not whole ones.
            let valid = m.page_xxh3.is_empty()
                && !m.rank_sums.is_empty()
                && m.rank_sums.iter().all(|sums| {
                    sums.page_xxh3.len() == plan.page_count as usize && sums.has_row == plan.row
                });
            if !valid {
                self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
                return Ok(());
            }
            if terminal {
                window.phase = Phase::Free;
                window.extent = None;
                self.reclaim(key);
                return Ok(());
            }
            let parts = m
                .rank_sums
                .into_iter()
                .map(|sums| {
                    (
                        sums.rank as usize,
                        SliceSums {
                            pages: sums.page_xxh3,
                            row: sums.has_row.then_some(sums.row_xxh3),
                        },
                    )
                })
                .collect();
            self.stated_part(key, index as u32, parts, 0);
            return Ok(());
        }
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
        if window.arrived {
            return Ok(());
        }
        window.arrived = true;
        self.window_arrived(
            key,
            index,
            Stated::Whole(SliceSums {
                pages: m.page_xxh3,
                row: m.has_row.then_some(m.row_xxh3),
            }),
        );
        Ok(())
    }

    /// A window of a relayed claim arrived from its source on the data channel.
    fn source_window_filled(
        &mut self,
        key: ClaimKey,
        index: u32,
        page_xxh3: Vec<u64>,
        row_xxh3: Option<u64>,
    ) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        let terminal = claim.is_terminal();
        let Some(window) = claim.windows.get_mut(index as usize) else {
            return;
        };
        if window.phase != Phase::SourceWrite {
            return;
        }
        let plan = window.plan;
        if page_xxh3.len() != plan.page_count as usize || row_xxh3.is_some() != plan.row {
            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            return;
        }
        if terminal {
            // The source's write is over; no one read it.
            window.phase = Phase::Free;
            window.extent = None;
            self.reclaim(key);
            return;
        }
        if claim.mp.src.is_some() {
            // The rank-0 leg of a multi-process source states rank 0's slice.
            let parts = vec![(
                0,
                SliceSums {
                    pages: page_xxh3,
                    row: row_xxh3,
                },
            )];
            self.stated_part(key, index, parts, 0);
            return;
        }
        if window.arrived {
            return;
        }
        window.arrived = true;
        self.window_arrived(
            key,
            index as usize,
            Stated::Whole(SliceSums {
                pages: page_xxh3,
                row: row_xxh3,
            }),
        );
    }

    /// A window is filled and checked against its plan: hand it to the requester, through
    /// shared memory or on its data channel.
    pub(super) fn deliver_window(
        &mut self,
        key: ClaimKey,
        index: usize,
        page_xxh3: Vec<u64>,
        row_xxh3: u64,
    ) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        let window = &mut claim.windows[index];
        window.phase = Phase::RequesterRead;
        window.readers = 1;
        window.arrived = true;
        window.page_xxh3 = page_xxh3.clone();
        window.row_xxh3 = row_xxh3;
        let has_row = window.plan.row;
        claim.state = v2::ClaimState::Granted;
        claim.last_progress = Instant::now();
        let spec = claim.spec(index);
        let requester_sid = claim.requester_sid;
        if let Some((open, hub)) = claim.channel.as_ref().zip(self.cfg.channel.as_ref()) {
            let region = u8::try_from(spec.region)
                .ok()
                .and_then(crate::data::Region::from_u8);
            let Some(region) = region else {
                return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            };
            hub.push_window(
                open.transfer,
                channel::WindowOut {
                    shape: shape_of(&spec),
                    page_xxh3,
                    row_xxh3,
                    region,
                    offset: spec.offset,
                },
            );
            // Once every window is on its way, the handler ends the transfer after them.
            if claim
                .windows
                .iter()
                .all(|w| !matches!(w.phase, Phase::Unissued | Phase::SourceWrite))
            {
                hub.finish(open.transfer);
            }
            return;
        }
        self.send(
            requester_sid,
            M::WindowReady(v2::WindowReady {
                claim_id: key.2,
                window: Some(spec),
                page_xxh3,
                row_xxh3,
                has_row,
                rank_sums: Vec::new(),
            }),
        );
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
            Phase::RequesterRead => {}
            Phase::Free => return Ok(()),
            Phase::SourceWrite | Phase::Unissued => {
                return Err(Violation::new(
                    "WindowReleased before the window was filled",
                ));
            }
        }
        self.reader_done(key, m.index, 0);
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
                let covered = claim.state == v2::ClaimState::Granted;
                let pages: u64 = claim
                    .windows
                    .iter()
                    .map(|window| u64::from(window.plan.page_count))
                    .sum();
                let exact = pages == claim.total_pages
                    && (!claim.want_row || claim.windows.iter().any(|window| window.plan.row));
                // On the data channel the claim's windows are freed as the handler reports
                // them sent, on another path than this message. The requester's verdict
                // proves it read them all, so windows still marked sent-or-sending count.
                let windows_done = if claim.channel.is_some() || !claim.mp.legs.is_empty() {
                    claim
                        .windows
                        .iter()
                        .all(|window| matches!(window.phase, Phase::RequesterRead | Phase::Free))
                } else {
                    claim.all_windows_done()
                };
                let complete = covered && exact && windows_done && !claim.windows.is_empty();
                if complete {
                    self.claims.get_mut(&key).unwrap().verdict_latched = true;
                    self.complete_if_latched(key);
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
            Some(claim) if claim.is_terminal() => {}
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
                    if claim.requester_fence == Party::Pending {
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
            _ => return Ok(()),
        }
        claim.export = ExportState::Ended(party);
        if !claim.is_terminal() {
            let status =
                v2::ExportEndStatus::try_from(m.status).unwrap_or(v2::ExportEndStatus::Unspecified);
            // A relayed source's last windows may still be on the channel when its
            // ExportEnd arrives; the channel reports any that never come. So may the last
            // windows of a multi-process source's channel ranks: rank 0 ends the export once
            // every rank has finished, and a leg's bytes can still be in the manager's
            // handler. The leg's close fails the claim if any window never arrived.
            let finished = status == v2::ExportEndStatus::ExportComplete
                && (claim.source_channel.is_some()
                    || claim.mp.legs.iter().any(|leg| leg.source && !leg.ch.exited)
                    || claim.windows.iter().all(|window| {
                        // A window in the assembly pass has been filled, too.
                        window.arrived || matches!(window.phase, Phase::RequesterRead | Phase::Free)
                    }));
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
        self.count(
            "claims_total",
            format!("state=\"{state:?}\",reason=\"{reason:?}\""),
        );
        let requester_gone = self.proc_exited_flag(&(key.0, key.1));
        let source_gone = self
            .claims
            .get(&key)
            .and_then(|claim| claim.source_proc)
            .is_some_and(|proc| self.proc_exited_flag(&proc));
        let claim = self.claims.get_mut(&key).expect("claim exists");
        if claim.granted_at.is_none() {
            claim.requester_fence = Party::Drained;
        } else if requester_gone {
            claim.requester_fence = Party::Drained;
        }
        if source_gone && matches!(claim.export, ExportState::Requested | ExportState::Pinned) {
            claim.export = ExportState::Ended(Party::Drained);
        }
        self.settle_peers(key);
        let message = self.terminal_message(self.claims.get(&key).expect("claim exists"));
        let requester_sid = self.claims[&key].requester_sid;
        self.send(requester_sid, M::ClaimTerminal(message));
        self.cancel_export(key, reason);
        if self.claims[&key].manager.is_some() {
            self.store_cancel_get(key);
        }
        self.stop_channel(key);
        self.reclaim(key);
    }

    /// The claim is over: end its data-channel transfers. The claim's windows go back
    /// only when the handlers have exited, which `reclaim` checks.
    fn stop_channel(&mut self, key: ClaimKey) {
        let Some(hub) = self.cfg.channel.clone() else {
            // Without a hub there never was a transfer.
            if let Some(claim) = self.claims.get_mut(&key) {
                for leg in [claim.channel.as_mut(), claim.source_channel.as_mut()]
                    .into_iter()
                    .flatten()
                    .chain(claim.mp.legs.iter_mut().map(|leg| &mut leg.ch))
                {
                    leg.exited = true;
                }
            }
            return;
        };
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        let mut quiet = Vec::new();
        for leg in [claim.channel.as_mut(), claim.source_channel.as_mut()]
            .into_iter()
            .flatten()
            .chain(claim.mp.legs.iter_mut().map(|leg| &mut leg.ch))
        {
            if leg.exited {
                continue;
            }
            if hub.stop(leg.transfer) {
                leg.exited = true;
                quiet.push(leg.transfer);
            }
        }
        for transfer in quiet {
            self.channels.remove(&transfer);
        }
    }

    // ------------------------------------------------------------------ data channel

    /// What a data-channel handler reported.
    pub(super) fn channel_event(&mut self, event: channel::Event) {
        use channel::Event as E;
        match event {
            E::Sent { transfer, index } => match self.channels.get(&transfer).copied() {
                Some(ChanOwner::Claim(key)) => self.reader_done(key, index, 0),
                Some(ChanOwner::ClaimRank(key, leg)) => {
                    let rank = self
                        .claims
                        .get(&key)
                        .and_then(|claim| claim.mp.legs.get(leg))
                        .map(|leg| leg.rank);
                    if let Some(rank) = rank {
                        self.reader_done(key, index, rank);
                    }
                }
                _ => {}
            },
            E::Filled {
                transfer,
                index,
                row_xxh3,
                page_xxh3,
            } => match self.channels.get(&transfer).copied() {
                Some(ChanOwner::ClaimRank(key, leg)) => {
                    let rank = self
                        .claims
                        .get(&key)
                        .and_then(|claim| claim.mp.legs.get(leg))
                        .filter(|leg| leg.source)
                        .map(|leg| leg.rank);
                    if let Some(rank) = rank {
                        let parts = vec![(
                            rank as usize,
                            SliceSums {
                                pages: page_xxh3,
                                row: row_xxh3,
                            },
                        )];
                        self.stated_part(key, index, parts, rank);
                    }
                }
                Some(ChanOwner::Put(key)) => {
                    if self.puts.get(&key).is_some_and(|rec| rec.mp.is_some()) {
                        // Rank 0's leg of a multi-process writer states rank 0's slice.
                        let parts = vec![(
                            0,
                            SliceSums {
                                pages: page_xxh3,
                                row: row_xxh3,
                            },
                        )];
                        self.put_stated_part(key, index, parts, 0);
                    } else {
                        self.put_window_filled(key, index, page_xxh3, row_xxh3)
                    }
                }
                Some(ChanOwner::PutRank(key, leg)) => {
                    let rank = self
                        .puts
                        .get(&key)
                        .and_then(|rec| rec.mp.as_ref())
                        .and_then(|mp| mp.legs.get(leg))
                        .map(|leg| leg.rank);
                    if let Some(rank) = rank {
                        let parts = vec![(
                            rank as usize,
                            SliceSums {
                                pages: page_xxh3,
                                row: row_xxh3,
                            },
                        )];
                        self.put_stated_part(key, index, parts, rank);
                    }
                }
                Some(ChanOwner::Source(key)) => {
                    self.source_window_filled(key, index, page_xxh3, row_xxh3)
                }
                _ => {}
            },
            E::Failed { transfer, reason } => match self.channels.get(&transfer).copied() {
                Some(ChanOwner::ClaimRank(key, _)) => {
                    eprintln!(
                        "selene: claim {}: a peer rank's data channel failed: {reason}",
                        key.2
                    );
                    self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
                }
                Some(ChanOwner::Claim(key)) => {
                    eprintln!(
                        "selene: claim {} on the data channel failed: {reason}",
                        key.2
                    );
                    self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
                }
                Some(ChanOwner::Source(key)) => {
                    eprintln!(
                        "selene: claim {}: its source's data channel failed: {reason}",
                        key.2
                    );
                    self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::SourceLost);
                }
                Some(ChanOwner::Put(key)) => self.put_channel_failed(key, &reason),
                Some(ChanOwner::PutRank(key, _)) => self.put_channel_failed(key, &reason),
                None => {}
            },
            E::Closed { transfer } => {
                if let Some(hub) = &self.cfg.channel {
                    hub.forget(transfer);
                }
                match self.channels.remove(&transfer) {
                    Some(ChanOwner::ClaimRank(key, leg)) => {
                        if let Some(open) = self
                            .claims
                            .get_mut(&key)
                            .and_then(|claim| claim.mp.legs.get_mut(leg))
                        {
                            open.ch.exited = true;
                        }
                        // A leg that ends before the claim is over, with windows it never
                        // read or wrote, fails the claim; one that finished is simply done.
                        let owed = self.claims.get(&key).is_some_and(|claim| {
                            !claim.is_terminal()
                                && claim.mp.legs.get(leg).is_some_and(|open| {
                                    if open.source {
                                        claim
                                            .windows
                                            .iter()
                                            .any(|w| !w.arrived && w.phase != Phase::Free)
                                    } else {
                                        claim.windows.iter().any(|w| {
                                            w.phase == Phase::RequesterRead
                                                && w.readers & (1 << open.rank) != 0
                                        })
                                    }
                                })
                        });
                        if owed {
                            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
                        }
                        self.reclaim(key);
                    }
                    Some(ChanOwner::Claim(key)) => {
                        if let Some(open) = self
                            .claims
                            .get_mut(&key)
                            .and_then(|claim| claim.channel.as_mut())
                        {
                            open.exited = true;
                        }
                        self.reclaim(key);
                    }
                    Some(ChanOwner::Source(key)) => {
                        if let Some(open) = self
                            .claims
                            .get_mut(&key)
                            .and_then(|claim| claim.source_channel.as_mut())
                        {
                            open.exited = true;
                        }
                        // A source that closes its channel before the claim is over has
                        // given up the windows it owed.
                        let owed = self.claims.get(&key).is_some_and(|claim| {
                            !claim.is_terminal()
                                && claim.windows.iter().any(|w| {
                                    matches!(w.phase, Phase::Unissued | Phase::SourceWrite)
                                })
                        });
                        if owed {
                            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::SourceLost);
                        }
                        self.reclaim(key);
                    }
                    Some(ChanOwner::Put(key)) => self.put_channel_closed(key),
                    Some(ChanOwner::PutRank(key, leg)) => {
                        if let Some(open) = self
                            .puts
                            .get_mut(&key)
                            .and_then(|rec| rec.mp.as_mut())
                            .and_then(|mp| mp.legs.get_mut(leg))
                        {
                            open.ch.exited = true;
                        }
                        self.puts_resettle(key);
                    }
                    None => {}
                }
            }
        }
    }

    /// One accessor of a window is done reading it: slot 0 is the requester's session (or its
    /// own leg), slot `r` the leg of peer rank `r`. The window is free when every accessor
    /// has finished; an acknowledgment counts once per accessor, whatever its repeats.
    pub(super) fn reader_done(&mut self, key: ClaimKey, index: u32, slot: u32) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        let Some(window) = claim.windows.get_mut(index as usize) else {
            return;
        };
        if window.phase != Phase::RequesterRead || window.readers & (1 << slot) == 0 {
            return;
        }
        window.readers &= !(1 << slot);
        if window.readers != 0 {
            return self.complete_if_latched(key);
        }
        window.phase = Phase::Free;
        window.extent = None;
        claim.last_progress = Instant::now();
        let terminal = claim.is_terminal();
        let from_store = claim.manager.is_some();
        if from_store {
            self.store_release_window(key, index);
        }
        if terminal {
            self.reclaim(key);
        } else if !from_store {
            // A relayed claim's next window can take the freed room.
            self.issue_windows(key);
        }
        self.complete_if_latched(key);
    }

    /// The requester's COPY_OK was exact but waited for a leg or the store; finish the claim
    /// now if nothing is outstanding.
    fn complete_if_latched(&mut self, key: ClaimKey) {
        let Some(claim) = self.claims.get(&key) else {
            return;
        };
        if claim.is_terminal() || !claim.verdict_latched || claim.legs_pending() {
            return;
        }
        if claim.manager.as_ref().is_some_and(|manager| !manager.done) {
            return;
        }
        self.finish(key, v2::ClaimState::Copied, v2::ClaimReason::ReasonNone);
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
        // A source without shared memory touches the claim's windows only through the
        // manager's handler, so that handler's exit is its fence.
        let source_ok = match &claim.source_channel {
            Some(leg) => leg.exited,
            None => matches!(claim.source_fence(), Party::Drained) || source_gone,
        };
        // On the data channel the requester never touches the claim's windows: the
        // manager's handler does, and its exit is the fence.
        let requester_ok = match &claim.channel {
            Some(open) => open.exited,
            None => matches!(claim.requester_fence, Party::Drained) || requester_gone,
        };
        // A multi-process requester's peers: a shared-memory one answers for itself (its own
        // attestation, or its process's exit); one on a leg is fenced by the handler's exit.
        let peers_ok = claim.mp.req.as_ref().is_none_or(|side| {
            side.peers.iter().all(|peer| {
                !peer.shared
                    || matches!(peer.fence, Party::Drained)
                    || self.proc_exited_flag(&peer.proc)
            })
        }) && claim
            .mp
            .legs
            .iter()
            .filter(|leg| !leg.source)
            .all(|leg| leg.ch.exited);
        // A multi-process source's peers likewise.
        let source_peers_ok = claim.mp.src.as_ref().is_none_or(|side| {
            side.peers.iter().all(|peer| {
                !peer.shared
                    || matches!(peer.fence, Party::Drained)
                    || self.proc_exited_flag(&peer.proc)
            })
        }) && claim
            .mp
            .legs
            .iter()
            .filter(|leg| leg.source)
            .all(|leg| leg.ch.exited);
        // The assembly pass reads manager memory too.
        let quiet = claim.mp.passes == 0;
        let (source_ok, requester_ok) = (
            source_ok && source_peers_ok && quiet,
            requester_ok && peers_ok && quiet,
        );
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
        let mut expire = Vec::new();
        let mut forget = Vec::new();
        for (key, claim) in &self.claims {
            if claim.is_terminal() {
                let peers_settled = claim.mp.passes == 0
                    && claim.mp.legs.iter().all(|leg| leg.ch.exited)
                    && [&claim.mp.req, &claim.mp.src]
                        .into_iter()
                        .flatten()
                        .flat_map(|side| side.peers.iter())
                        .all(|peer| {
                            !matches!(peer.fence, Party::Pending | Party::Quarantined)
                                || self.proc_exited_flag(&peer.proc)
                        });
                let drained = peers_settled
                    && [&claim.channel, &claim.source_channel]
                        .into_iter()
                        .flatten()
                        .all(|leg| leg.exited)
                    && claim
                        .windows
                        .iter()
                        .all(|window| window.phase == Phase::Free);
                let settled = claim.block.is_none()
                    && drained
                    && !matches!(claim.export, ExportState::Requested | ExportState::Pinned)
                    && !matches!(claim.export, ExportState::Ended(Party::Quarantined))
                    && (claim.channel.is_some()
                        || !matches!(claim.requester_fence, Party::Pending | Party::Quarantined))
                    && !matches!(claim.source_fence(), Party::Pending);
                let source_gone = claim
                    .source_proc
                    .as_ref()
                    .is_none_or(|proc| self.proc_exited_flag(proc));
                let requester_gone = self.proc_exited_flag(&(key.0, key.1));
                let fenced_by_exit =
                    source_gone && requester_gone && claim.block.is_none() && drained;
                if settled || fenced_by_exit {
                    forget.push(*key);
                }
                continue;
            }
            if now >= claim.deadline {
                expire.push((*key, v2::ClaimReason::Deadline));
            } else if claim.state == v2::ClaimState::Requested && now >= claim.pin_deadline {
                expire.push((*key, v2::ClaimReason::SourceTimeout));
            } else if matches!(claim.state, v2::ClaimState::Granted)
                && now.duration_since(claim.last_progress) >= progress
            {
                expire.push((*key, v2::ClaimReason::NoProgress));
            }
        }
        for (key, reason) in expire {
            self.finish(key, v2::ClaimState::Failed, reason);
        }
        for key in forget {
            if let Some(claim) = self.claims.remove(&key) {
                self.exports.remove(&claim.export_id);
            }
        }
    }

    // ------------------------------------------------------------------ rank agents

    /// An agent attested that it drained one transfer.
    pub(super) fn agent_fence(&mut self, agent: ProcKey, kind: i32, id: u64, party: Party) {
        use v2::rank_fence::Kind;
        let key = match Kind::try_from(kind) {
            Ok(Kind::Claim) => {
                // The claim is named by rank 0's id; the agent's instance is its rank 0's.
                let rank0 = self
                    .agents
                    .values()
                    .find(|a| a.proc() == agent)
                    .map(|a| a.rank0);
                rank0.map(|nonce| (agent.0, nonce, id))
            }
            Ok(Kind::Export) => self.exports.get(&id).copied(),
            Ok(Kind::Put) => {
                let rank0 = self
                    .agents
                    .values()
                    .find(|a| a.proc() == agent)
                    .map(|a| a.rank0);
                if let Some(nonce) = rank0 {
                    self.put_agent_fence((agent.0, nonce, id), agent, party);
                }
                return;
            }
            _ => None,
        };
        let Some(key) = key else { return };
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        let mut touched = false;
        for side in [claim.mp.req.as_mut(), claim.mp.src.as_mut()]
            .into_iter()
            .flatten()
        {
            for peer in side.peers.iter_mut().filter(|p| p.proc == agent) {
                if peer.fence == Party::Pending {
                    peer.fence = party;
                    touched = true;
                }
            }
        }
        if touched {
            self.reclaim(key);
        }
    }

    /// An attached agent is gone or replaced: the transfers it takes part in fail. What it
    /// may still be touching stays its own obligation.
    pub(super) fn agent_gone(&mut self, agent: ProcKey) {
        self.puts_peer_gone(agent);
        let keys: Vec<ClaimKey> = self
            .claims
            .iter()
            .filter(|(_, claim)| !claim.is_terminal())
            .filter(|(_, claim)| claim.mp.peer_procs().any(|proc| proc == agent))
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::RankLost);
        }
    }

    // ------------------------------------------------------------------ processes

    /// A process is confirmed gone: nothing it did can touch memory any more.
    pub(super) fn proc_exited(&mut self, process: ProcKey) {
        self.puts_peer_changed(process, true);
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
                touched = true;
            }
            for side in [claim.mp.req.as_mut(), claim.mp.src.as_mut()]
                .into_iter()
                .flatten()
            {
                for peer in side.peers.iter_mut().filter(|p| p.proc == process) {
                    if matches!(peer.fence, Party::Pending | Party::Quarantined) {
                        peer.fence = Party::Drained;
                    }
                    touched = true;
                }
            }
            if touched {
                self.reclaim(key);
            }
        }
        self.puts_proc_exited(process);
    }

    /// `Goodbye{quiesced}`: the process cleared its own obligations.
    pub(super) fn party_quiesced(&mut self, process: ProcKey) {
        self.puts_peer_changed(process, false);
        let keys: Vec<ClaimKey> = self.claims.keys().copied().collect();
        for key in keys {
            let claim = self.claims.get_mut(&key).expect("claim exists");
            let mut touched = false;
            if (key.0, key.1) == process && claim.requester_fence == Party::Pending {
                claim.requester_fence = Party::Drained;
                touched = true;
            }
            if claim.source_proc == Some(process) {
                if matches!(claim.export, ExportState::Requested | ExportState::Pinned) {
                    claim.export = ExportState::Ended(Party::Drained);
                    touched = true;
                }
            }
            for side in [claim.mp.req.as_mut(), claim.mp.src.as_mut()]
                .into_iter()
                .flatten()
            {
                for peer in side.peers.iter_mut().filter(|p| p.proc == process) {
                    if peer.fence == Party::Pending {
                        peer.fence = Party::Drained;
                        touched = true;
                    }
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
            if !as_requester {
                // A source without shared memory reaches the claim's windows only through
                // the manager's handler, whose exit is the fence. Its session ending
                // clears what it owed for the export itself.
                if let Some(claim) = self.claims.get_mut(&key) {
                    if claim.source_channel.is_some() {
                        claim.export = ExportState::Ended(Party::Drained);
                    }
                }
            }
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

/// The shape a window has on one rank's leg: its slice's length when the instance is a
/// multi-process one, else the whole window's.
fn shape_for(
    spec: &v2::WindowSpec,
    layout: Option<&flare_selene::pool::SliceLayout>,
    rank: usize,
) -> Shape {
    let mut shape = shape_of(spec);
    if let Some(layout) = layout {
        shape.length = layout.slice_bytes(rank, spec.page_count, spec.row);
    }
    shape
}

fn shape_of(spec: &v2::WindowSpec) -> Shape {
    Shape {
        index: spec.index,
        record: spec.record,
        first_page: spec.first_page,
        page_count: spec.page_count,
        row: spec.row,
        length: spec.length,
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
        core_with(PoolConfig::default())
    }

    fn core_with(config: PoolConfig) -> (Core, mpsc::Receiver<v2::ServerMessage>) {
        let (mut core, _handle, _events) = Core::new(
            config,
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
                syncing: false,
                lookups: HashSet::new(),
                goodbye: None,
                overflowed: false,
                ranks: None,
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
                state: v2::ClaimState::Granted,
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
                    readers: 0,
                    writers: 0,
                    stated: Vec::new(),
                    arrived: false,
                }],
                requester_fence: Party::Pending,
                verdict_latched: false,
                granted_at: Some(now),
                page_bytes: 4096,
                row_bytes: 4096,
                manager: Some(ManagerGet {
                    checks: Default::default(),
                    meta: Default::default(),
                    starts: vec![0],
                    done: false,
                    transfer_released: false,
                }),
                channel: None,
                source_channel: None,
                mp: MpState::default(),
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

    /// A handler whose exit is never seen: the claim's windows stay out of service, are
    /// counted as quarantined while the handler remains live, and return only when the exit is seen.
    #[tokio::test(flavor = "multi_thread")]
    async fn windows_of_a_handler_that_cannot_be_seen_to_exit_are_quarantined() {
        use crate::{arena::Arena, data::channel::Hub, regions::LocalRegions};
        use flare_selene::pool::frame::{Carrier, ToManager};
        use tokio::io::AsyncWriteExt;
        let regions = std::sync::Arc::new(LocalRegions::new(
            Arena::new(1 << 20).unwrap(),
            Arena::new(1 << 20).unwrap(),
        ));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let (hub, _events) = Hub::start(
            &tokio::runtime::Handle::current(),
            listener,
            regions,
            None,
            64,
            None,
        )
        .unwrap();
        hub.delay_closed(Duration::from_secs(60));
        let config = PoolConfig {
            channel: Some(hub.clone()),
            ..PoolConfig::default()
        };
        let (mut core, _out) = core_with(config);
        let key = delivered(&mut core);
        let (transfer, token) = hub.open(channel::Kind::Get).unwrap();
        // The instance is connected and quiet: a handler is attached.
        let mut socket = tokio::net::TcpStream::connect(hub.address()).await.unwrap();
        socket.write_all(&token).await.unwrap();
        ToManager::Hello(Carrier::Stream)
            .write(&mut socket)
            .await
            .unwrap();
        while hub.handlers() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let claim = core.claims.get_mut(&key).unwrap();
        claim.windows[0].phase = Phase::RequesterRead;
        claim.channel = Some(Channel {
            transfer,
            token,
            exited: false,
        });
        core.channels.insert(transfer, ChanOwner::Claim(key));
        core.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Cancelled);
        let claim = &core.claims[&key];
        assert!(!claim.channel.as_ref().unwrap().exited);
        assert_eq!(
            claim.windows[0].phase,
            Phase::RequesterRead,
            "the handler may still read the window"
        );
        core.claim_timers(Instant::now());
        assert_eq!(core.quarantined_bytes(), 6 * 4096);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::RequesterRead);
        // Seeing the exit is the proof that nothing reads the window any more.
        core.channel_event(channel::Event::Closed { transfer });
        assert_eq!(core.quarantined_bytes(), 0);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::Free);
        assert!(core.channels.is_empty());
    }

    /// The same for the leg a source without shared memory writes its windows on.
    #[tokio::test(flavor = "multi_thread")]
    async fn windows_of_a_source_leg_that_cannot_be_seen_to_exit_are_quarantined() {
        use crate::{arena::Arena, data::channel::Hub, regions::LocalRegions};
        use flare_selene::pool::frame::{Carrier, ToManager};
        use tokio::io::AsyncWriteExt;
        let regions = std::sync::Arc::new(LocalRegions::new(
            Arena::new(1 << 20).unwrap(),
            Arena::new(1 << 20).unwrap(),
        ));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let (hub, _events) = Hub::start(
            &tokio::runtime::Handle::current(),
            listener,
            regions,
            None,
            64,
            None,
        )
        .unwrap();
        hub.delay_closed(Duration::from_secs(60));
        let config = PoolConfig {
            channel: Some(hub.clone()),
            ..PoolConfig::default()
        };
        let (mut core, _out) = core_with(config);
        let key = delivered(&mut core);
        let (transfer, token) = hub.open(channel::Kind::Put).unwrap();
        let mut socket = tokio::net::TcpStream::connect(hub.address()).await.unwrap();
        socket.write_all(&token).await.unwrap();
        ToManager::Hello(Carrier::Stream)
            .write(&mut socket)
            .await
            .unwrap();
        while hub.handlers() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let claim = core.claims.get_mut(&key).unwrap();
        claim.windows[0].phase = Phase::SourceWrite;
        claim.source_channel = Some(Channel {
            transfer,
            token,
            exited: false,
        });
        core.channels.insert(transfer, ChanOwner::Source(key));
        core.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Cancelled);
        assert_eq!(
            core.claims[&key].windows[0].phase,
            Phase::SourceWrite,
            "the handler may still write the window"
        );
        core.claim_timers(Instant::now());
        assert_eq!(core.quarantined_bytes(), 6 * 4096);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::SourceWrite);
        core.channel_event(channel::Event::Closed { transfer });
        assert_eq!(core.quarantined_bytes(), 0);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::Free);
        assert!(core.channels.is_empty());
    }

    // ---- multi-process requesters -------------------------------------------------

    use super::super::mp::{MpPeer, MpSide, RankLeg};

    fn three_ranks() -> std::sync::Arc<flare_selene::pool::SliceLayout> {
        use flare_selene::pool::{Geometry, RankPlanes};
        let rank = RankPlanes {
            plane_bytes: vec![4096],
            row_bytes: 4096,
        };
        std::sync::Arc::new(
            flare_selene::pool::SliceLayout::new(&Geometry {
                page_tokens: 64,
                ranks: vec![rank; 3],
                page_record_bytes: 3 * 4096,
                row_record_bytes: 3 * 4096,
            })
            .unwrap(),
        )
    }

    const AGENT_1: ProcKey = ([1; 16], [11; 16]);
    const AGENT_2: ProcKey = ([1; 16], [12; 16]);

    /// A claim from a three-rank requester with one window read by rank 0's session, the
    /// shared peer rank 1 (a participant) and the leg of rank 2.
    fn ranked(core: &mut Core, window_phase: Phase) -> ClaimKey {
        let key = delivered(core);
        for agent in [AGENT_1, AGENT_2] {
            core.procs.insert(
                agent,
                crate::pool::core::Proc {
                    pid: None,
                    pidfd: None,
                    exited: false,
                    claim_hw: 0,
                    put_hw: 0,
                    idle_since: None,
                    link: None,
                },
            );
        }
        let claim = core.claims.get_mut(&key).unwrap();
        claim.windows[0].phase = window_phase;
        claim.windows[0].readers = 0b101;
        claim.mp.req = Some(MpSide {
            layout: three_ranks(),
            peers: vec![
                MpPeer {
                    rank: 1,
                    proc: AGENT_1,
                    shared: true,
                    fence: Party::Pending,
                },
                MpPeer {
                    rank: 2,
                    proc: AGENT_2,
                    shared: false,
                    fence: Party::Drained,
                },
            ],
        });
        claim.mp.legs = vec![RankLeg {
            rank: 2,
            source: false,
            ch: Channel {
                transfer: 99,
                token: [0; flare_selene::pool::frame::TOKEN_BYTES],
                exited: false,
            },
        }];
        claim.manager.as_mut().unwrap().done = true;
        key
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_window_is_free_only_when_every_accessor_finished_and_repeats_are_not_votes() {
        let (mut core, _out) = core();
        let key = ranked(&mut core, Phase::RequesterRead);
        core.reader_done(key, 0, 0);
        core.reader_done(key, 0, 0);
        core.reader_done(key, 0, 0);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::RequesterRead);
        assert_eq!(core.claims[&key].windows[0].readers, 0b100);
        // A slot that was never required is no vote either.
        core.reader_done(key, 0, 1);
        core.reader_done(key, 0, 5);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::RequesterRead);
        core.reader_done(key, 0, 2);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::Free);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_exact_copy_ok_waits_for_the_legs_and_completes_when_the_last_one_reports() {
        let (mut core, _out) = core();
        let key = ranked(&mut core, Phase::RequesterRead);
        core.reader_done(key, 0, 0);
        core.copy_result(1, copy_ok()).unwrap();
        assert!(
            !core.claims[&key].is_terminal(),
            "rank 2's leg has not finished reading"
        );
        assert!(core.claims[&key].verdict_latched);
        core.reader_done(key, 0, 2);
        assert_eq!(core.claims[&key].state, v2::ClaimState::Copied);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_shared_peers_window_is_released_by_its_own_fence_and_not_by_anyone_elses() {
        let (mut core, _out) = core();
        let key = ranked(&mut core, Phase::RequesterRead);
        core.claims.get_mut(&key).unwrap().windows[0].readers = 0;
        core.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Cancelled);
        // The requester's session fenced itself (the fixture's party is Pending).
        core.claims.get_mut(&key).unwrap().requester_fence = Party::Drained;
        core.claims.get_mut(&key).unwrap().mp.legs[0].ch.exited = true;
        core.reclaim(key);
        assert_eq!(
            core.claims[&key].windows[0].phase,
            Phase::RequesterRead,
            "peer 1 may still be reading"
        );
        // A different agent (a replacement) attesting does nothing for peer 1.
        core.agent_fence(
            AGENT_2,
            v2::rank_fence::Kind::Claim as i32,
            1,
            Party::Drained,
        );
        core.reclaim(key);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::RequesterRead);
        // Peer 1's own process exiting is its proof.
        core.proc_exited(AGENT_1);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::Free);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_peer_that_attests_quarantined_keeps_the_window_until_its_process_exits() {
        let (mut core, _out) = core();
        let key = ranked(&mut core, Phase::RequesterRead);
        core.claims.get_mut(&key).unwrap().windows[0].readers = 0;
        core.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Cancelled);
        let claim = core.claims.get_mut(&key).unwrap();
        claim.requester_fence = Party::Drained;
        claim.mp.legs[0].ch.exited = true;
        claim.mp.req.as_mut().unwrap().peers[0].fence = Party::Quarantined;
        core.reclaim(key);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::RequesterRead);
        assert_eq!(core.quarantined_bytes(), 6 * 4096);
        core.agent_fence(
            AGENT_1,
            v2::rank_fence::Kind::Claim as i32,
            1,
            Party::Drained,
        );
        assert_eq!(core.claims[&key].windows[0].phase, Phase::RequesterRead);
        core.proc_exited(AGENT_1);
        assert_eq!(core.claims[&key].windows[0].phase, Phase::Free);
        assert_eq!(core.quarantined_bytes(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn losing_an_attached_peer_fails_the_claims_it_takes_part_in() {
        let (mut core, _out) = core();
        let key = ranked(&mut core, Phase::RequesterRead);
        core.agent_gone(AGENT_2);
        assert!(
            !core.claims[&key].is_terminal(),
            "rank 2 is on a leg, not a participant"
        );
        core.agent_gone(AGENT_1);
        assert_eq!(core.claims[&key].state, v2::ClaimState::Failed);
        assert_eq!(core.claims[&key].reason, v2::ClaimReason::RankLost);
    }

    fn ending_source(core: &mut Core, leg_exited: bool) -> ClaimKey {
        let key = ranked(core, Phase::SourceWrite);
        let claim = core.claims.get_mut(&key).unwrap();
        claim.source_proc = Some((INSTANCE, NONCE));
        claim.export_id = 7;
        claim.export = ExportState::Pinned;
        claim.windows[0].readers = 0;
        // Rank 2's slices are still to come from its data channel.
        claim.windows[0].writers = 0b100;
        claim.mp.legs[0].source = true;
        claim.mp.legs[0].ch.exited = leg_exited;
        core.exports.insert(7, key);
        key
    }

    fn complete_export() -> v2::ExportEnd {
        v2::ExportEnd {
            export_id: 7,
            status: v2::ExportEndStatus::ExportComplete as i32,
            fence: v2::FenceOutcome::Drained as i32,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_export_that_ends_before_a_rank_legs_last_bytes_are_handled_does_not_fail_the_claim()
    {
        let (mut core, _out) = core();
        let key = ending_source(&mut core, false);
        core.export_end(1, complete_export()).unwrap();
        assert!(
            !core.claims[&key].is_terminal(),
            "the leg's channel reports any window that never comes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_export_that_ends_with_a_window_missing_and_no_leg_left_to_deliver_it_fails_the_claim()
     {
        let (mut core, _out) = core();
        let key = ending_source(&mut core, true);
        core.export_end(1, complete_export()).unwrap();
        assert_eq!(core.claims[&key].reason, v2::ClaimReason::CopyError);
    }
}
