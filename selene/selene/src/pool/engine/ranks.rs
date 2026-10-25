//! Rank 0's side of a multi-process instance: the links to the peer ranks' agents and the
//! transfers it fans out over them.
//!
//! Rank 0 decides; an agent only moves its own slice. Every transfer is bound to the
//! manager incarnation and attachment generation the agent last reported, and an agent
//! ends each transfer with exactly one `Done` after it has drained. Nothing is released on
//! rank 0 (destination pages, pins) until every rank's `Done` is in: a rank whose drain can
//! never be established is quarantined and stops the pool instead.

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Notify;

use crate::{Error, Result};

use super::super::{
    rank::{Incoming, LinkEnd, Msg, RankSet, link},
    slice::{SliceLayout, SliceSums},
    wire,
};

/// What the agents say that the engine acts on.
#[derive(Debug)]
pub(crate) enum RankEvent {
    WindowDone {
        rank: usize,
        xfer: u64,
        index: u32,
        result: std::result::Result<(), String>,
        sums: Option<SliceSums>,
    },
    Done {
        rank: usize,
        xfer: u64,
        result: std::result::Result<(), String>,
        quarantined: bool,
    },
    /// The rank attached to the manager.
    Attached,
    /// The attachment of a rank changed or ended; its open transfers are cancelled by the
    /// agent and their `Done` follow.
    Detached,
    /// The link failed: nothing more will come from this rank, drained or not.
    Lost { rank: usize, reason: String },
}

/// A peer rank's part of a claim.
pub(crate) struct PeerPart {
    pub(crate) rank: usize,
    pub(crate) xfer: u64,
    pub(crate) shared: bool,
    /// The agent's `Done`: the rank has drained and covered its slice (or failed).
    pub(crate) done: Option<std::result::Result<(), String>>,
    /// The agent could not prove it drained.
    pub(crate) quarantined: bool,
    pub(crate) cancelled: bool,
}

/// A window shared peers have not all drained, and the slice checksums of those that did.
pub(crate) struct PeerWindow {
    pub(crate) left: std::collections::BTreeSet<usize>,
    pub(crate) sums: Vec<(usize, crate::pool::SliceSums)>,
}

/// Who a transfer belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum XferOwner {
    Claim(u64),
    Export(u64),
    Put(u64),
}

#[derive(Debug)]
enum State {
    /// Hello sent, waiting for the agent's.
    Greeting,
    /// Bound; waiting for the agent's attachment to the manager.
    Bound,
    Attached {
        incarnation: u64,
        generation: u64,
        mode: wire::RankMode,
    },
    Detached,
    Lost,
}

struct Peer {
    end: LinkEnd,
    state: State,
}

/// What rank 0 tells its peers about the instance.
pub(crate) struct Identity {
    pub secret: [u8; 32],
    pub nonce: [u8; 16],
    pub instance_id: [u8; 16],
    pub fingerprint: [u8; 16],
    /// The GPUs of the instance; an agent's must be one of them.
    pub gpus: Vec<[u8; 16]>,
}

pub(crate) struct Ranks {
    pub layout: Arc<SliceLayout>,
    secret: [u8; 32],
    instance_id: [u8; 16],
    fingerprint: [u8; 16],
    /// The GPUs of the instance; an agent's must be one of them.
    gpus: Vec<[u8; 16]>,
    peers: Vec<Peer>,
    next_xfer: u64,
    /// Open transfers: owner and rank.
    xfers: HashMap<u64, (XferOwner, usize)>,
}

impl Ranks {
    /// Start the links. `set.links[i]` reaches rank `i + 1`; Hello goes out at once.
    pub(crate) fn new(
        set: RankSet,
        layout: Arc<SliceLayout>,
        who: Identity,
        wake: Arc<Notify>,
    ) -> Result<Self> {
        let Identity {
            secret,
            nonce,
            instance_id,
            fingerprint,
            gpus,
        } = who;
        let world = set.links.len() + 1;
        if layout.world() != world {
            return Err(Error::new(
                "the scope's rank count does not match the rank links",
            ));
        }
        let mut peers = Vec::with_capacity(set.links.len());
        for (index, link) in set.links.into_iter().enumerate() {
            let end = LinkEnd::start(link, wake.clone())
                .map_err(|e| Error::caused_by("cannot start a rank link", e))?;
            end.send(Msg::Hello(link::Hello {
                rank: (index + 1) as u32,
                world: world as u32,
                rank0_nonce: nonce.to_vec(),
                layout: fingerprint.to_vec(),
            }));
            peers.push(Peer {
                end,
                state: State::Greeting,
            });
        }
        Ok(Self {
            layout,
            secret,
            instance_id,
            fingerprint,
            gpus,
            peers,
            next_xfer: 0,
            xfers: HashMap::new(),
        })
    }

    pub(crate) fn world(&self) -> usize {
        self.peers.len() + 1
    }

    /// Every peer is attached to the manager incarnation `incarnation`.
    pub(crate) fn attached(&self, incarnation: u64) -> bool {
        self.peers.iter().all(
            |peer| matches!(peer.state, State::Attached { incarnation: i, .. } if i == incarnation),
        )
    }

    /// The mode the manager gave `rank`'s agent, once attached.
    pub(crate) fn mode(&self, rank: usize) -> Option<wire::RankMode> {
        match self.peers.get(rank.checked_sub(1)?)?.state {
            State::Attached { mode, .. } => Some(mode),
            _ => None,
        }
    }

    /// Start a transfer on `rank`: the next id, bound to the agent's current attachment.
    /// `None` when the rank is not attached.
    pub(crate) fn open(
        &mut self,
        rank: usize,
        owner: XferOwner,
        mut open: link::Open,
    ) -> Option<u64> {
        let peer = self.peers.get(rank.checked_sub(1)?)?;
        let State::Attached {
            incarnation,
            generation,
            ..
        } = peer.state
        else {
            return None;
        };
        self.next_xfer += 1;
        let xfer = self.next_xfer;
        open.xfer = xfer;
        open.manager_incarnation = incarnation;
        open.generation = generation;
        if !self.peers[rank - 1].end.send(Msg::Open(open)) {
            return None;
        }
        self.xfers.insert(xfer, (owner, rank));
        Some(xfer)
    }

    pub(crate) fn send_window(&self, rank: usize, window: link::Window) -> bool {
        self.peers
            .get(rank - 1)
            .is_some_and(|peer| peer.end.send(Msg::Window(window)))
    }

    pub(crate) fn cancel(&self, xfer: u64) {
        if let Some((_, rank)) = self.xfers.get(&xfer) {
            self.peers[rank - 1]
                .end
                .send(Msg::Cancel(link::Cancel { xfer }));
        }
    }

    /// The transfer is over (its `Done` arrived or its rank is lost).
    pub(crate) fn forget(&mut self, xfer: u64) {
        self.xfers.remove(&xfer);
    }

    pub(crate) fn owner(&self, xfer: u64) -> Option<(XferOwner, usize)> {
        self.xfers.get(&xfer).copied()
    }

    /// Open transfers of `rank`.
    pub(crate) fn open_on(&self, rank: usize) -> Vec<u64> {
        self.xfers
            .iter()
            .filter(|(_, (_, r))| *r == rank)
            .map(|(x, _)| *x)
            .collect()
    }

    /// Every open transfer, on any rank.
    pub(crate) fn idle(&self) -> bool {
        self.xfers.is_empty()
    }

    /// Collect what the links delivered since the last call.
    pub(crate) fn poll(&mut self) -> Vec<RankEvent> {
        let mut events = Vec::new();
        for index in 0..self.peers.len() {
            let rank = index + 1;
            let mut incoming = Vec::new();
            self.peers[index].end.poll(&mut incoming);
            for item in incoming {
                match item {
                    Incoming::Closed(reason) => {
                        if !matches!(self.peers[index].state, State::Lost) {
                            self.peers[index].state = State::Lost;
                            events.push(RankEvent::Lost { rank, reason });
                        }
                    }
                    Incoming::Message(message) => self.message(index, message, &mut events),
                }
            }
        }
        events
    }

    fn lose(&mut self, index: usize, reason: String, events: &mut Vec<RankEvent>) {
        self.peers[index].state = State::Lost;
        events.push(RankEvent::Lost {
            rank: index + 1,
            reason,
        });
    }

    fn message(&mut self, index: usize, message: Msg, events: &mut Vec<RankEvent>) {
        let rank = index + 1;
        match message {
            Msg::AgentHello(hello) => {
                let fine = matches!(self.peers[index].state, State::Greeting)
                    && hello.rank as usize == rank
                    && hello.world as usize == self.world()
                    && hello.layout == self.fingerprint
                    && hello.agent_nonce.len() == 16
                    && self
                        .gpus
                        .iter()
                        .any(|gpu| gpu.as_slice() == hello.gpu_uuid.as_slice());
                if !fine {
                    return self.lose(
                        index,
                        format!("rank {rank}'s agent disagrees about the instance"),
                        events,
                    );
                }
                self.peers[index].state = State::Bound;
                self.peers[index].end.send(Msg::Bind(link::Bind {
                    rank_secret: self.secret.to_vec(),
                    instance_id: self.instance_id.to_vec(),
                }));
            }
            Msg::Attached(attached) => {
                if matches!(self.peers[index].state, State::Greeting | State::Lost) {
                    return self.lose(
                        index,
                        format!("rank {rank} attached before its hello"),
                        events,
                    );
                }
                let mode =
                    wire::RankMode::try_from(attached.mode).unwrap_or(wire::RankMode::Unspecified);
                self.peers[index].state = State::Attached {
                    incarnation: attached.manager_incarnation,
                    generation: attached.generation,
                    mode,
                };
                events.push(RankEvent::Attached);
            }
            Msg::Detached(_) => {
                if !matches!(self.peers[index].state, State::Lost) {
                    self.peers[index].state = State::Detached;
                    events.push(RankEvent::Detached);
                }
            }
            Msg::WindowDone(done) => {
                let result = if done.ok { Ok(()) } else { Err(done.error) };
                let sums = done.sums.map(|sums| SliceSums {
                    pages: sums.pages,
                    row: sums.has_row.then_some(sums.row),
                });
                events.push(RankEvent::WindowDone {
                    rank,
                    xfer: done.xfer,
                    index: done.index,
                    result,
                    sums,
                });
            }
            Msg::Done(done) => {
                events.push(RankEvent::Done {
                    rank,
                    xfer: done.xfer,
                    result: if done.ok { Ok(()) } else { Err(done.error) },
                    quarantined: done.quarantined,
                });
            }
            _ => self.lose(
                index,
                format!("rank {rank} sent a message only rank 0 may"),
                events,
            ),
        }
    }
}

impl super::Engine {
    /// Start every peer rank's part of an export about to be pinned. The pages are the
    /// chain's, in the order the records are covered.
    pub(super) fn open_export_peers(
        &mut self,
        id: u64,
        records: &[wire::Record],
        chain: Option<&crate::ExportChain>,
        rank_channels: &[wire::RankChannel],
    ) -> std::result::Result<(), String> {
        let Some(ranks) = self.ranks.as_mut() else {
            return Ok(());
        };
        // `None`: a host-tier export, whose peers read their own record files.
        let mut pages = Vec::new();
        for (index, node) in chain.iter().flat_map(|c| c.nodes.iter().enumerate()) {
            let chain = chain.expect("a chain has nodes");
            let first = if index == 0 { chain.skip_pages } else { 0 } as usize;
            let count = records.get(index).map_or(0, |record| record.pages as usize);
            let Some(slice) = node.pages.get(first..first + count) else {
                return Err("a chain node has fewer pages than its record".into());
            };
            pages.extend(slice.iter().map(|page| page.0));
        }
        let (page_bytes, row_bytes) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        let mut opened: Vec<PeerPart> = Vec::new();
        for rank in 1..ranks.world() {
            let channel = rank_channels
                .iter()
                .find(|c| c.rank as usize == rank)
                .and_then(|c| c.channel.clone());
            let shared = channel.is_none();
            let expected = if shared {
                wire::RankMode::RankShared
            } else {
                wire::RankMode::RankChannel
            };
            if ranks.mode(rank) != Some(expected) {
                return Err(format!("rank {rank}'s attachment does not match its mode"));
            }
            let open = link::Open {
                dir: link::Dir::Out as i32,
                kind: wire::rank_fence::Kind::Export as i32,
                fence_id: id,
                records: records.to_vec(),
                pages: pages.clone(),
                has_slot: chain.is_some_and(|c| c.row.is_some()),
                slot: chain.and_then(|c| c.row).map_or(0, |slot| slot.0),
                channel,
                page_bytes,
                row_bytes,
                host: chain.is_none(),
                ..Default::default()
            };
            let Some(xfer) = ranks.open(rank, XferOwner::Export(id), open) else {
                // Cancel the ones already started.
                for peer in &opened {
                    ranks.cancel(peer.xfer);
                }
                return Err(format!("rank {rank} is not attached"));
            };
            opened.push(PeerPart {
                rank,
                xfer,
                shared,
                done: None,
                quarantined: false,
                cancelled: false,
            });
        }
        self.source.set_peers(id, opened);
        Ok(())
    }

    /// Stop the peers of an export that is over or never started.
    pub(super) fn cancel_export_peers(&mut self, id: u64) {
        let cancels = self.source.take_uncancelled(id);
        if let Some(ranks) = &self.ranks {
            for xfer in cancels {
                ranks.cancel(xfer);
            }
        }
    }

    /// A shared-memory peer drained its slice of one window of an export.
    pub(crate) fn on_peer_export_window(
        &mut self,
        export: u64,
        rank: usize,
        index: u32,
        result: std::result::Result<(), String>,
        sums: Option<SliceSums>,
    ) {
        if !self
            .source
            .peer_window_done(export, rank, index, result, sums)
        {
            return;
        }
        let (spec, failed) = self.source.window_after_peers(export, index);
        if failed {
            self.fail_export_window(export, index);
        } else if let Some(spec) = spec {
            self.fill_done_if_idle(export, index, spec);
        }
        self.drive_export_now(export);
    }

    /// A peer rank ended its part of an export.
    pub(crate) fn on_peer_export_done(
        &mut self,
        export: u64,
        rank: usize,
        result: std::result::Result<(), String>,
        quarantined: bool,
    ) {
        self.source.peer_done(export, rank, result, quarantined);
        if quarantined {
            return self.quarantine();
        }
        self.drive_export_now(export);
    }

    /// Start every peer rank's part of an accepted put. The record's pages are the writer's
    /// chain pages from the first the manager needs.
    pub(super) fn open_put_peers(
        &mut self,
        id: u64,
        pages: u32,
        rank_channels: &[wire::RankChannel],
    ) -> std::result::Result<(), String> {
        let Some(setup) = self.writer.peer_setup(id, pages) else {
            return Err("the put's source cannot be set up for the peer ranks".into());
        };
        let Some(ranks) = self.ranks.as_mut() else {
            return Ok(());
        };
        let (page_bytes, row_bytes) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        let mut opened: Vec<PeerPart> = Vec::new();
        for rank in 1..ranks.world() {
            let channel = rank_channels
                .iter()
                .find(|c| c.rank as usize == rank)
                .and_then(|c| c.channel.clone());
            let shared = channel.is_none();
            let expected = if shared {
                wire::RankMode::RankShared
            } else {
                wire::RankMode::RankChannel
            };
            if ranks.mode(rank) != Some(expected) {
                for peer in &opened {
                    ranks.cancel(peer.xfer);
                }
                return Err(format!("rank {rank}'s attachment does not match its mode"));
            }
            let open = link::Open {
                dir: link::Dir::Out as i32,
                kind: wire::rank_fence::Kind::Put as i32,
                fence_id: id,
                records: vec![setup.record.clone()],
                pages: setup.pages.clone(),
                has_slot: setup.row && !setup.host,
                slot: setup.slot,
                channel,
                page_bytes,
                row_bytes,
                host: setup.host,
                chain_first: setup.chain_first,
                ..Default::default()
            };
            let Some(xfer) = ranks.open(rank, XferOwner::Put(id), open) else {
                for peer in &opened {
                    ranks.cancel(peer.xfer);
                }
                return Err(format!("rank {rank} is not attached"));
            };
            opened.push(PeerPart {
                rank,
                xfer,
                shared,
                done: None,
                quarantined: false,
                cancelled: false,
            });
        }
        self.writer.set_peers(id, opened);
        Ok(())
    }

    /// Whether shared peers still owe their slice of one window of a put.
    pub(super) fn put_peers_left(&self, put: u64, window: u32) -> bool {
        self.writer.peers_left(put, window)
    }

    /// Stop the peers of a put that is over.
    pub(super) fn cancel_put_peers_if_over(&mut self, put: u64) {
        let cancels = self.writer.take_uncancelled(put);
        if let Some(ranks) = &self.ranks {
            for xfer in cancels {
                ranks.cancel(xfer);
            }
        }
    }

    /// A shared-memory peer drained its slice of one window of a put.
    pub(crate) fn on_peer_put_window(
        &mut self,
        put: u64,
        rank: usize,
        index: u32,
        result: std::result::Result<(), String>,
        sums: Option<SliceSums>,
    ) {
        if !self.writer.peer_window_done(put, rank, index, result, sums) {
            return;
        }
        self.put_after_peer_window(put, index);
    }

    /// A peer rank ended its part of a put.
    pub(crate) fn on_peer_put_done(
        &mut self,
        put: u64,
        rank: usize,
        result: std::result::Result<(), String>,
        quarantined: bool,
    ) {
        self.writer.peer_done(put, rank, result, quarantined);
        if quarantined {
            return self.quarantine();
        }
        self.put_drive_now(put);
    }
}
