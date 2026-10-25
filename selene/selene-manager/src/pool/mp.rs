//! Multi-process instances in claims: peer participants, their data-channel legs, the
//! assembly pass and the in-order delivery of windows.
//!
//! The bookkeeping that is the same for every claim stays in `claims.rs`; what a rank adds
//! lives here. A claim whose requester is a multi-process instance has one *leg* per peer
//! rank in channel mode, which carries only that rank's slices, and one *participant* per
//! peer rank in shared mode, which reads its slices out of the manager's windows in place
//! and answers for itself under its own process identity.

use super::{
    claims::{Channel, ClaimKey, Party, Phase},
    core::{ChanOwner, Core, Event, ProcKey},
    pass::{PassError, PassJob, PassOut},
};
use crate::{
    data::{Region, channel},
    proto::v2::{self, server_message::M},
};
use flare_selene::pool::{SliceLayout, SliceSums, frame::Shape};
use std::{collections::BTreeMap, sync::Arc, time::Instant};

/// A peer rank that reads or writes the manager's windows in place.
pub struct MpPeer {
    pub rank: u32,
    /// The agent's own identity: what it may still be touching is its own obligation.
    pub proc: ProcKey,
    pub shared: bool,
    pub fence: Party,
}

/// One side (requester or source) of a claim that is a multi-process instance.
pub struct MpSide {
    pub layout: Arc<SliceLayout>,
    /// Ranks `1..world`, in order.
    pub peers: Vec<MpPeer>,
}

/// A peer rank's data-channel leg.
pub struct RankLeg {
    pub rank: u32,
    /// The leg of the claim's source (a put-kind transfer) rather than its requester.
    pub source: bool,
    pub ch: Channel,
}

/// What the pass produced for one window, held until its turn.
pub struct Ready {
    pub whole: Option<SliceSums>,
    pub slices: Option<Vec<SliceSums>>,
}

/// Windows are delivered in index order whatever order their passes finish in.
#[derive(Default)]
pub struct Delivery {
    pub next: u32,
    pub ready: BTreeMap<u32, Ready>,
}

/// What a source states about a window.
pub enum Stated {
    /// Whole-page and row checksums (a single-process source, or the store).
    Whole(SliceSums),
    /// Every rank's slice checksums (a multi-process source), by rank.
    Ranks(Vec<SliceSums>),
}

/// What a claim tracks for multi-process instances.
#[derive(Default)]
pub struct MpState {
    pub req: Option<MpSide>,
    pub src: Option<MpSide>,
    pub legs: Vec<RankLeg>,
    pub deliver: Delivery,
    /// Passes running on the claim's windows: they read manager memory, so the windows are
    /// not reclaimed until they are over.
    pub passes: u32,
    /// The requester's exact COPY_OK arrived while some leg had not finished reading.
    pub copy_ok: bool,
}

impl MpState {
    /// Bit `rank` of a window's reader mask: the legs that must read it before it is free.
    pub fn leg_readers(&self) -> u64 {
        self.legs
            .iter()
            .filter(|leg| !leg.source)
            .fold(0, |mask, leg| mask | 1 << leg.rank)
    }

    pub fn peer_procs(&self) -> impl Iterator<Item = ProcKey> + '_ {
        [&self.req, &self.src]
            .into_iter()
            .flatten()
            .flat_map(|side| side.peers.iter().filter(|p| p.shared).map(|p| p.proc))
    }
}

impl Core {
    /// The multi-process side of a claim for the session `sid`: one participant per peer
    /// rank, and a leg opened for each that is in channel mode. `Ok(None)` for an ordinary
    /// session.
    pub(super) fn mp_side(
        &mut self,
        sid: u64,
        key: ClaimKey,
        kind: channel::Kind,
        legs: &mut Vec<RankLeg>,
    ) -> Result<Option<MpSide>, v2::ClaimReason> {
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(None);
        };
        let Some(ranks) = session.ranks.clone() else {
            return Ok(None);
        };
        let (instance, rank0, scope) = (session.instance, session.nonce, session.scope);
        let layout = self.scopes[&scope].valid.slices.clone();
        let mut peers = Vec::new();
        for rank in 1..ranks.world {
            let Some(agent) = self.agents.values().find(|a| {
                a.instance == instance && a.rank0 == rank0 && a.rank == rank && !a.drain_only
            }) else {
                self.discard_legs(legs);
                return Err(v2::ClaimReason::RanksDetached);
            };
            let shared = agent.mode == v2::RankMode::RankShared;
            let proc = agent.proc();
            if !shared {
                let opened = self
                    .cfg
                    .channel
                    .as_ref()
                    .map(|hub| hub.open_rank(kind, layout.clone(), rank as usize));
                match opened {
                    Some(Ok((transfer, token))) => {
                        self.channels
                            .insert(transfer, ChanOwner::ClaimRank(key, legs.len()));
                        legs.push(RankLeg {
                            rank,
                            source: kind == channel::Kind::Put,
                            ch: Channel {
                                transfer,
                                token,
                                exited: false,
                                stopped_at: None,
                                quarantined: false,
                            },
                        });
                    }
                    _ => {
                        self.discard_legs(legs);
                        return Err(v2::ClaimReason::NoWindows);
                    }
                }
            }
            peers.push(MpPeer {
                rank,
                proc,
                shared,
                fence: Party::Idle,
            });
        }
        Ok(Some(MpSide { layout, peers }))
    }

    /// Drop legs that no one has connected to.
    pub(super) fn discard_legs(&mut self, legs: &mut Vec<RankLeg>) {
        for leg in legs.drain(..) {
            self.channels.remove(&leg.ch.transfer);
            if let Some(hub) = &self.cfg.channel {
                hub.stop(leg.ch.transfer);
                hub.forget(leg.ch.transfer);
            }
        }
    }

    /// The channels the requester's peers in channel mode dial, for `ClaimGranted`.
    pub(super) fn rank_channels(&self, key: ClaimKey) -> Vec<v2::RankChannel> {
        let (Some(claim), Some(hub)) = (self.claims.get(&key), self.cfg.channel.as_ref()) else {
            return Vec::new();
        };
        claim
            .mp
            .legs
            .iter()
            .filter(|leg| !leg.source)
            .map(|leg| v2::RankChannel {
                rank: leg.rank,
                channel: Some(super::channel_ref(hub, &leg.ch.token)),
            })
            .collect()
    }

    /// A window is whole in the manager's memory and its source has stated its checksums.
    /// An ordinary claim hands it on at once; one with a multi-process side goes through
    /// the assembly pass first.
    pub(super) fn window_arrived(&mut self, key: ClaimKey, index: usize, stated: Stated) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        if claim.mp.req.is_none() && claim.mp.src.is_none() {
            let Stated::Whole(sums) = stated else {
                return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            };
            return self.deliver_window(key, index, sums.pages, sums.row.unwrap_or(0));
        }
        let Some(regions) = self.cfg.regions.clone() else {
            eprintln!("selene: a multi-process claim needs the manager's regions");
            return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
        };
        let window = &claim.windows[index];
        let Some(region) = u8::try_from(window.region).ok().and_then(Region::from_u8) else {
            return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
        };
        let layout = claim
            .mp
            .req
            .as_ref()
            .or(claim.mp.src.as_ref())
            .map(|side| side.layout.clone())
            .expect("a multi-process side");
        let (pages, row) = (window.plan.page_count, window.plan.row);
        let requester_mp = claim.mp.req.is_some();
        let (stated_slices, stated_whole) = match stated {
            Stated::Ranks(slices) => (Some(slices), None),
            Stated::Whole(sums) => (None, Some(sums)),
        };
        let job = PassJob {
            regions,
            region,
            offset: window.offset,
            pages,
            row,
            layout,
            stated_slices,
            stated_whole,
            want_whole: !requester_mp,
            want_slices: requester_mp,
        };
        claim.mp.passes += 1;
        let events = self.self_tx.clone();
        let index = index as u32;
        tokio::task::spawn_blocking(move || {
            let result = super::pass::run(job);
            let _ = events.blocking_send(Event::Pass { key, index, result });
        });
    }

    /// A pass finished.
    pub(super) fn pass_done(
        &mut self,
        key: ClaimKey,
        index: u32,
        result: Result<PassOut, PassError>,
    ) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        claim.mp.passes = claim.mp.passes.saturating_sub(1);
        if claim.is_terminal() {
            // Nobody wants the window now; its memory is free once the pass is over.
            return self.reclaim(key);
        }
        match result {
            Ok(out) => {
                claim.mp.deliver.ready.insert(
                    index,
                    Ready {
                        whole: out.whole,
                        slices: out.slices,
                    },
                );
            }
            Err(PassError::Mismatch(why)) => {
                eprintln!("selene: claim {}: {why}", key.2);
                return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::Checksum);
            }
            Err(PassError::Io(why)) => {
                eprintln!("selene: claim {}: the assembly pass failed: {why}", key.2);
                return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
            }
        }
        loop {
            let Some(claim) = self.claims.get_mut(&key) else {
                return;
            };
            if claim.is_terminal() {
                return;
            }
            let next = claim.mp.deliver.next;
            let Some(ready) = claim.mp.deliver.ready.remove(&next) else {
                return;
            };
            claim.mp.deliver.next += 1;
            self.deliver_ready(key, next as usize, ready);
        }
    }

    /// Hand a window that went through the pass to the requester.
    fn deliver_ready(&mut self, key: ClaimKey, index: usize, ready: Ready) {
        let Some(claim) = self.claims.get_mut(&key) else {
            return;
        };
        let Some(side) = claim.mp.req.as_ref() else {
            // A single-process requester: whole sums, delivered as ever.
            let sums = ready.whole.unwrap_or_default();
            return self.deliver_window(key, index, sums.pages, sums.row.unwrap_or(0));
        };
        let Some(slices) = ready.slices else {
            return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
        };
        let layout = side.layout.clone();
        let shared_ranks: Vec<u32> = std::iter::once(0)
            .chain(side.peers.iter().filter(|p| p.shared).map(|p| p.rank))
            .collect();
        let spec = claim.spec(index);
        let region = u8::try_from(spec.region).ok().and_then(Region::from_u8);
        let Some(region) = region else {
            return self.finish(key, v2::ClaimState::Failed, v2::ClaimReason::CopyError);
        };
        let window = &mut claim.windows[index];
        window.phase = Phase::RequesterRead;
        window.readers = 1 | claim.mp.leg_readers();
        claim.state = v2::ClaimState::Copying;
        claim.last_progress = Instant::now();
        let requester_sid = claim.requester_sid;
        let rank0_leg = claim.channel.as_ref().map(|leg| leg.transfer);
        let legs: Vec<(u64, u32)> = claim
            .mp
            .legs
            .iter()
            .filter(|leg| !leg.source)
            .map(|leg| (leg.ch.transfer, leg.rank))
            .collect();
        let shape_of = |rank: usize| Shape {
            index: spec.index,
            record: spec.record,
            first_page: spec.first_page,
            page_count: spec.page_count,
            row: spec.row,
            length: layout.slice_bytes(rank, spec.page_count, spec.row),
        };
        if let Some(hub) = self.cfg.channel.as_ref() {
            let out = |rank: usize| channel::WindowOut {
                shape: shape_of(rank),
                page_xxh3: slices[rank].pages.clone(),
                row_xxh3: slices[rank].row.unwrap_or(0),
                region,
                offset: spec.offset,
            };
            if let Some(transfer) = rank0_leg {
                hub.push_window(transfer, out(0));
            }
            for (transfer, rank) in &legs {
                hub.push_window(*transfer, out(*rank as usize));
            }
        }
        if rank0_leg.is_none() {
            self.send(
                requester_sid,
                M::WindowReady(v2::WindowReady {
                    claim_id: key.2,
                    window: Some(spec),
                    rank_sums: shared_ranks
                        .iter()
                        .map(|rank| v2::RankSums {
                            rank: *rank,
                            page_xxh3: slices[*rank as usize].pages.clone(),
                            row_xxh3: slices[*rank as usize].row.unwrap_or(0),
                            has_row: slices[*rank as usize].row.is_some(),
                        })
                        .collect(),
                    ..Default::default()
                }),
            );
        }
        self.finish_req_legs_if_delivered(key);
    }

    /// Once every window is on its way, each leg ends its transfer after the ones queued.
    pub(super) fn finish_req_legs_if_delivered(&mut self, key: ClaimKey) {
        let Some(claim) = self.claims.get(&key) else {
            return;
        };
        let all = claim
            .windows
            .iter()
            .all(|w| !matches!(w.phase, Phase::Unissued | Phase::SourceWrite));
        // A claim on the store's records ends its transfers when the store is done.
        if !all || claim.windows.is_empty() || claim.manager.is_some() {
            return;
        }
        if let Some(hub) = &self.cfg.channel {
            for leg in claim.mp.legs.iter().filter(|leg| !leg.source) {
                hub.finish(leg.ch.transfer);
            }
            if let Some(open) = &claim.channel {
                hub.finish(open.transfer);
            }
        }
    }

    /// A claim is over: peers that never took part, or whose process has gone, owe nothing.
    pub(super) fn settle_peers(&mut self, key: ClaimKey) {
        let exited: Vec<ProcKey> = match self.claims.get(&key) {
            Some(claim) => claim
                .mp
                .peer_procs()
                .filter(|proc| self.procs.get(proc).is_none_or(|p| p.exited))
                .collect(),
            None => return,
        };
        let claim = self.claims.get_mut(&key).expect("claim exists");
        let granted = claim.granted_at.is_some();
        for side in [claim.mp.req.as_mut(), claim.mp.src.as_mut()]
            .into_iter()
            .flatten()
        {
            for peer in &mut side.peers {
                if !granted {
                    peer.fence = Party::Idle;
                } else if exited.contains(&peer.proc) {
                    peer.fence = Party::Drained;
                }
            }
        }
    }

    /// The requester's peer legs, for the end of a manager-store claim.
    pub(super) fn finish_peer_legs(&mut self, key: ClaimKey) {
        let (Some(claim), Some(hub)) = (self.claims.get(&key), self.cfg.channel.as_ref()) else {
            return;
        };
        for leg in claim.mp.legs.iter().filter(|leg| !leg.source) {
            hub.finish(leg.ch.transfer);
        }
    }
}
