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
    Attached { rank: usize },
    /// The attachment of a rank changed or ended; its open transfers are cancelled by the
    /// agent and their `Done` follow.
    Detached { rank: usize },
    /// The link failed: nothing more will come from this rank, drained or not.
    Lost { rank: usize, reason: String },
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
    agent_nonce: Vec<u8>,
    gpu_uuid: Vec<u8>,
}

pub(crate) struct Ranks {
    pub layout: Arc<SliceLayout>,
    secret: [u8; 32],
    nonce: [u8; 16],
    instance_id: [u8; 16],
    fingerprint: [u8; 16],
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
        secret: [u8; 32],
        nonce: [u8; 16],
        instance_id: [u8; 16],
        fingerprint: [u8; 16],
        wake: Arc<Notify>,
    ) -> Result<Self> {
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
                agent_nonce: Vec::new(),
                gpu_uuid: Vec::new(),
            });
        }
        Ok(Self {
            layout,
            secret,
            nonce,
            instance_id,
            fingerprint,
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

    /// Whether a link failed for good.
    pub(crate) fn any_lost(&self) -> bool {
        self.peers.iter().any(|p| matches!(p.state, State::Lost))
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

    /// GPU uuids of the peers, in rank order, as they reported them.
    pub(crate) fn gpu_uuids(&self) -> Vec<Vec<u8>> {
        self.peers.iter().map(|p| p.gpu_uuid.clone()).collect()
    }

    /// Tell every agent the instance is closing.
    pub(crate) fn close_all(&self) {
        for peer in &self.peers {
            peer.end.send(Msg::Close(link::Close {}));
        }
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
                    && hello.agent_nonce.len() == 16;
                if !fine {
                    return self.lose(
                        index,
                        format!("rank {rank}'s agent disagrees about the instance"),
                        events,
                    );
                }
                self.peers[index].agent_nonce = hello.agent_nonce;
                self.peers[index].gpu_uuid = hello.gpu_uuid;
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
                events.push(RankEvent::Attached { rank });
            }
            Msg::Detached(_) => {
                if !matches!(self.peers[index].state, State::Lost) {
                    self.peers[index].state = State::Detached;
                    events.push(RankEvent::Detached { rank });
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

    pub(crate) fn nonce(&self) -> [u8; 16] {
        self.nonce
    }
}

impl super::Engine {
    pub(crate) fn on_peer_export_window(
        &mut self,
        _export: u64,
        _rank: usize,
        _index: u32,
        _result: std::result::Result<(), String>,
        _sums: Option<SliceSums>,
    ) {
    }

    pub(crate) fn on_peer_export_done(
        &mut self,
        _export: u64,
        _rank: usize,
        _result: std::result::Result<(), String>,
        _quarantined: bool,
    ) {
    }

    pub(crate) fn on_peer_put_window(
        &mut self,
        _put: u64,
        _rank: usize,
        _index: u32,
        _result: std::result::Result<(), String>,
        _sums: Option<SliceSums>,
    ) {
    }

    pub(crate) fn on_peer_put_done(
        &mut self,
        _put: u64,
        _rank: usize,
        _result: std::result::Result<(), String>,
        _quarantined: bool,
    ) {
    }
}
