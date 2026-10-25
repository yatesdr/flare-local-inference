//! The writer role: spilling a record into the manager's store.
//!
//! A put asks the manager to accept a record, fills the windows the manager
//! grants (from GPU pages by D2H copies, or from bytes a store or probe
//! supplies on the host copy thread), reports each window's checksums after the
//! copy drained, and commits the record's header and checksum segment. The
//! writer's own copy may be dropped only when the put ends `Stored` or
//! `Present`. A put ends, and reports once, only after every copy that read
//! its source has drained; windows left unfilled stay leased to it until the
//! fence acknowledgment that follows a refusal, a cancel or a failure.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use crate::{
    Direction, Error, Job, JobId, PrefixKey,
    record::{Header, checksum},
    slab::WINDOW_PAGES,
};

use super::{
    super::{
        codec,
        exporter::{Checksums, PutBytes},
        frame,
        net::{Channel, Filled, Id},
        shared::{PoolEvent, PutOutcome, PutRefusal, PutRequest, PutSource},
        wire::{self, client_message::M as Out},
    },
    Engine, Owner,
    coverage::Coverage,
    hostfill::{FillJob, FillOwner},
    ranks::{PeerPart, PeerWindow},
    requester::ExpectedRecord,
};

/// A put that has not ended by then is abandoned.
pub const PUT_DEADLINE: Duration = Duration::from_secs(30);

enum Source {
    Gpu {
        pages: Vec<crate::PageId>,
        slot: crate::StateSlotId,
    },
    Host(Arc<Mutex<Box<dyn PutBytes>>>),
}

#[derive(PartialEq, Eq)]
enum Phase {
    Begun,
    Accepted,
    Committed,
}

struct Put {
    key: PrefixKey,
    bucket: [u8; 16],
    chain_pages: u64,
    row: bool,
    source: Source,
    phase: Phase,
    first_page: u64,
    parent: Option<PrefixKey>,
    coverage: Option<Coverage>,
    /// Checksums of the accepted record's pages, in order, and of its row.
    sums: Vec<u64>,
    row_sum: Option<u64>,
    /// Windows granted and not yet reported filled, with the claim-order index
    /// of their first page.
    windows: HashMap<u32, (wire::WindowSpec, u64)>,
    jobs_outstanding: u32,
    failed: Option<Error>,
    cancelled: bool,
    cancel_sent: bool,
    refused: Option<PutOutcome>,
    stored: Option<u64>,
    fence_sent: bool,
    deadline: Instant,
    /// Windows go to the manager over the data channel (no shared memory).
    via_channel: bool,
    /// The channel's task, until it has reported that it is gone.
    channel: Option<Channel>,
    channel_open: bool,
    /// The staging slot of each granted window until the window has been written.
    slots: HashMap<u32, u32>,
    /// Filled windows the channel has not yet written out.
    sends_outstanding: u32,
    /// The peer ranks' parts of this put.
    peers: Vec<PeerPart>,
    /// Shared-memory peers that have not yet drained their slice of a window.
    peer_windows: HashMap<u32, PeerWindow>,
}

#[derive(Default)]
pub(crate) struct State {
    puts: HashMap<u64, Put>,
}

/// What a put's peers are told to copy.
pub(super) struct PeerSetup {
    pub record: wire::Record,
    pub pages: Vec<u32>,
    pub row: bool,
    pub slot: u32,
}

impl State {
    /// The record and pages a put's peers are opened with, for a GPU put.
    pub(super) fn peer_setup(&self, put: u64, pages: u32) -> Option<PeerSetup> {
        let p = self.puts.get(&put)?;
        let Source::Gpu { pages: chain, slot } = &p.source else {
            return None;
        };
        let from = p.first_page as usize;
        let mine = chain.get(from..from + pages as usize)?;
        Some(PeerSetup {
            record: wire::Record {
                key: Some(codec::key_to(p.key)),
                generation: 0,
                first_page: 0,
                pages,
                row: p.row,
            },
            pages: mine.iter().map(|page| page.0).collect(),
            row: p.row,
            slot: slot.0,
        })
    }

    pub(super) fn peers_left(&self, put: u64, window: u32) -> bool {
        self.puts
            .get(&put)
            .and_then(|p| p.peer_windows.get(&window))
            .is_some_and(|w| !w.left.is_empty())
    }

    /// A shared peer reported one window. `false` when it was not expected.
    pub(super) fn peer_window_done(
        &mut self,
        put: u64,
        rank: usize,
        index: u32,
        result: std::result::Result<(), String>,
        sums: Option<crate::pool::SliceSums>,
    ) -> bool {
        let Some(p) = self.puts.get_mut(&put) else {
            return false;
        };
        let Some(window) = p.peer_windows.get_mut(&index) else {
            return false;
        };
        if !window.left.remove(&rank) {
            return false;
        }
        match (result, sums) {
            (Ok(()), Some(sums)) => window.sums.push((rank, sums)),
            (Err(why), _) => {
                p.failed
                    .get_or_insert(Error::new(format!("rank {rank}: {why}")));
            }
            (Ok(()), None) => {
                p.failed
                    .get_or_insert(Error::new(format!("rank {rank} stated no checksums")));
            }
        }
        true
    }

    pub(super) fn peer_done(
        &mut self,
        put: u64,
        rank: usize,
        result: std::result::Result<(), String>,
        quarantined: bool,
    ) {
        let Some(p) = self.puts.get_mut(&put) else {
            return;
        };
        if let Some(peer) = p
            .peers
            .iter_mut()
            .find(|peer| peer.rank == rank && peer.done.is_none())
        {
            peer.done = Some(result.clone());
            peer.quarantined = quarantined;
        }
        if let Err(why) = result {
            p.failed
                .get_or_insert(Error::new(format!("rank {rank}: {why}")));
        }
    }

    pub(super) fn set_peers(&mut self, put: u64, peers: Vec<PeerPart>) {
        if let Some(p) = self.puts.get_mut(&put) {
            p.peers = peers;
        }
    }

    pub(super) fn take_uncancelled(&mut self, put: u64) -> Vec<u64> {
        self.puts
            .get_mut(&put)
            .map(|p| {
                p.peers
                    .iter_mut()
                    .filter(|peer| peer.done.is_none() && !peer.cancelled)
                    .map(|peer| {
                        peer.cancelled = true;
                        peer.xfer
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.puts.is_empty()
    }

    pub(crate) fn busy(&self) -> bool {
        self.puts.values().any(|p| p.jobs_outstanding > 0)
    }
}

fn refusal(reason: i32) -> PutOutcome {
    use wire::Reason as R;
    match R::try_from(reason) {
        Ok(R::Present) => PutOutcome::Present,
        Ok(R::Inflight) => PutOutcome::Refused(PutRefusal::Inflight),
        Ok(R::Budget) => PutOutcome::Refused(PutRefusal::Budget),
        Ok(R::Oversize) => PutOutcome::Refused(PutRefusal::Oversize),
        Ok(R::Quota) => PutOutcome::Refused(PutRefusal::Quota),
        Ok(R::Invalid) => PutOutcome::Refused(PutRefusal::Invalid),
        Ok(R::Queue) => PutOutcome::Refused(PutRefusal::Queue),
        Ok(R::Io) => PutOutcome::Refused(PutRefusal::Io),
        Ok(R::PutNotReady) => PutOutcome::Refused(PutRefusal::NotReady),
        _ => PutOutcome::Failed(Error::new("the manager refused the put without a reason")),
    }
}

impl Engine {
    pub(crate) fn start_put(&mut self, now: Instant, id: u64, request: PutRequest) {
        let fail = |engine: &Engine, why: &str| {
            engine.event(PoolEvent::Put {
                id,
                outcome: PutOutcome::Failed(Error::new(why)),
            })
        };
        if !self.ready() || self.closing || self.quarantined() {
            return fail(self, "the pool is not ready");
        }
        if self.memory().is_none() {
            return fail(
                self,
                "this session has neither shared memory nor a data channel to write through",
            );
        }
        let gpu_matches = match &request.source {
            PutSource::Gpu { pages, .. } => pages.len() as u64 == request.chain_pages,
            PutSource::Host(_) => true,
        };
        if request.tail_pages > request.chain_pages || !gpu_matches {
            return fail(self, "the put does not describe a consistent record");
        }
        self.emit(Out::PutBegin(wire::PutBegin {
            put_id: id,
            key: Some(codec::key_to(request.key)),
            chain_pages: request.chain_pages,
            row: request.row,
            has_published_parent: request.parent.is_some(),
            published_parent: request.parent.map(codec::key_to),
            bucket: request.bucket.to_vec(),
            published_tail_pages: request.tail_pages,
            mode: if self.dial.is_some() {
                wire::CopyMode::DataChannel
            } else {
                wire::CopyMode::SharedWindows
            } as i32,
            generation: request.generation,
        }));
        self.writer.puts.insert(
            id,
            Put {
                key: request.key,
                bucket: request.bucket,
                chain_pages: request.chain_pages,
                row: request.row,
                source: match request.source {
                    PutSource::Gpu { pages, slot } => Source::Gpu { pages, slot },
                    PutSource::Host(bytes) => Source::Host(Arc::new(Mutex::new(bytes))),
                },
                phase: Phase::Begun,
                first_page: 0,
                parent: None,
                coverage: None,
                sums: Vec::new(),
                row_sum: None,
                windows: HashMap::new(),
                jobs_outstanding: 0,
                failed: None,
                cancelled: false,
                cancel_sent: false,
                refused: None,
                stored: None,
                fence_sent: false,
                deadline: now + PUT_DEADLINE,
                via_channel: self.dial.is_some(),
                channel: None,
                channel_open: false,
                slots: HashMap::new(),
                sends_outstanding: 0,
                peers: Vec::new(),
                peer_windows: HashMap::new(),
            },
        );
    }

    /// Stop a put that has not been committed. It ends `Cancelled` once its
    /// copies drained; one already committed runs to the manager's answer.
    pub(crate) fn cancel_put(&mut self, id: u64) {
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        put.cancelled = true;
        self.send_cancel(id);
        self.drive_put(id);
    }

    fn send_cancel(&mut self, id: u64) {
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        if put.phase != Phase::Committed && !put.cancel_sent {
            put.cancel_sent = true;
            self.emit(Out::PutCancel(wire::PutCancel { put_id: id }));
        }
    }

    fn fail_put(&mut self, id: u64, error: Error) {
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        put.failed.get_or_insert(error);
        self.send_cancel(id);
        self.drive_put(id);
    }

    /// The record the manager accepted, as a header and a checksum segment.
    fn put_header(&self, put: &Put) -> crate::Result<Header> {
        let scope = &self.st.scope;
        Header::new(
            &scope.record_geometry(),
            scope.model_identity,
            scope.layout_fingerprint,
            put.key,
            put.parent,
            put.bucket,
            (put.chain_pages - put.first_page) as u32,
            SystemTime::now(),
        )
    }

    pub(crate) fn on_put_accept(&mut self, accept: wire::PutAccept) {
        let id = accept.put_id;
        let Some(put) = self.writer.puts.get(&id) else {
            return;
        };
        if put.phase != Phase::Begun || put.cancelled || put.failed.is_some() {
            return;
        }
        let pages = put.chain_pages.checked_sub(accept.first_page);
        let parent = accept
            .parent
            .as_ref()
            .and_then(codec::key_from)
            .filter(|_| accept.has_parent);
        let via_channel = put.via_channel;
        let Some(pages) = pages.filter(|_| accept.channel.is_some() == via_channel) else {
            return self.fail_put(
                id,
                Error::new("the manager accepted a put this client cannot carry out"),
            );
        };
        if pages == 0 && !put.row {
            return self.fail_put(
                id,
                Error::new("the manager accepted a put with nothing to write"),
            );
        }
        let put = self.writer.puts.get_mut(&id).expect("present");
        put.first_page = accept.first_page;
        put.parent = parent;
        let expected = match self.put_header(self.writer.puts.get(&id).expect("present")) {
            Ok(header) => {
                if self.writer.puts[&id].row {
                    header.length_with_row()
                } else {
                    header.row_segment
                }
            }
            Err(error) => return self.fail_put(id, error),
        };
        if accept.record_bytes != expected {
            return self.fail_put(
                id,
                Error::new(format!(
                    "the manager expects a {}-byte record, this client writes {expected}",
                    accept.record_bytes
                )),
            );
        }
        let put = self.writer.puts.get_mut(&id).expect("present");
        put.coverage = Some(Coverage::new(vec![ExpectedRecord {
            key: put.key,
            first_page: 0,
            pages: pages as u32,
            row: put.row,
        }]));
        put.sums = vec![0; pages as usize];
        put.phase = Phase::Accepted;
        if self.ranks.is_some()
            && let Err(why) = self.open_put_peers(id, pages as u32, &accept.rank_channels)
        {
            return self.fail_put(id, Error::new(why));
        }
        if let Some(reference) = &accept.channel {
            match self.open_channel(Id::Put(id), reference) {
                Ok(channel) => {
                    let put = self.writer.puts.get_mut(&id).expect("present");
                    put.channel = Some(channel);
                    put.channel_open = true;
                }
                Err(error) => self.fail_put(id, error),
            }
        }
    }

    pub(crate) fn on_put_grant(&mut self, grant: wire::PutGrant) {
        let Some(spec) = grant.window else {
            return;
        };
        self.put_window(grant.put_id, spec, None);
    }

    /// The manager granted a window over the data channel; its bytes go in `slot`.
    pub(crate) fn on_net_grant(&mut self, put: u64, shape: frame::Shape, slot: u32) {
        let Some(staging) = self.net.clone() else {
            return;
        };
        let spec = wire::WindowSpec {
            index: shape.index,
            region: wire::Region::Staging as i32,
            offset: staging.offset(slot),
            length: shape.length,
            record: shape.record,
            first_page: shape.first_page,
            page_count: shape.page_count,
            row: shape.row,
        };
        if !self.put_window(put, spec, Some(slot)) {
            self.free_slot(slot);
        }
    }

    /// A window of a put to fill. `false` when it was not taken up and its slot,
    /// if any, is still the caller's.
    fn put_window(&mut self, id: u64, spec: wire::WindowSpec, slot: Option<u32>) -> bool {
        let (whole_page, whole_row) = (self.st.page_record_bytes(), self.st.row_record_bytes());
        // A window of this rank's own leg holds only its slices.
        let (page_bytes, row_bytes) = match (&self.ranks, slot) {
            (Some(ranks), Some(_)) => (ranks.layout.rank(0).page_len, ranks.layout.rank(0).row_len),
            _ => (whole_page, whole_row),
        };
        let memory = self.memory();
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return false;
        };
        if put.phase != Phase::Accepted || put.cancelled || put.failed.is_some() {
            return false;
        }
        let dest = match put
            .coverage
            .as_mut()
            .expect("accepted")
            .accept(&spec, page_bytes, row_bytes)
        {
            Ok(dest) => dest,
            Err(why) => {
                self.fail_put(id, Error::new(why));
                return false;
            }
        };
        let address = memory
            .as_ref()
            .and_then(|m| m.region(spec.region()))
            .and_then(|region| region.address(spec.offset, spec.length));
        let put = self.writer.puts.get_mut(&id).expect("present");
        if let Some(slot) = slot {
            put.slots.insert(spec.index, slot);
        }
        let Some(address) = address else {
            self.fail_put(id, Error::new("the granted window lies outside its region"));
            return true;
        };
        let first = put.first_page + dest;
        let count = spec.page_count as usize;
        put.windows.insert(spec.index, (spec, dest));
        // The shared-memory peers write their own slices of this window in step.
        let shared: Vec<(usize, u64)> = put
            .peers
            .iter()
            .filter(|p| p.shared && p.done.is_none())
            .map(|p| (p.rank, p.xfer))
            .collect();
        if slot.is_none() && !shared.is_empty() {
            put.peer_windows.insert(
                spec.index,
                PeerWindow {
                    left: shared.iter().map(|(rank, _)| *rank).collect(),
                    sums: Vec::new(),
                },
            );
        }
        match &put.source {
            Source::Gpu { pages, slot } => {
                let mut jobs = Vec::new();
                let mut from = 0;
                while from < count || (count == 0 && jobs.is_empty()) {
                    let end = (from + WINDOW_PAGES).min(count);
                    let job_pages = (from..end)
                        .map(|i| (pages[first as usize + i], address + i * page_bytes as usize))
                        .collect();
                    let row = (end == count && spec.row)
                        .then(|| (*slot, address + count * page_bytes as usize));
                    jobs.push(Job {
                        id: JobId(0),
                        window: spec.index,
                        direction: Direction::Out,
                        pages: job_pages,
                        row,
                    });
                    from = end;
                    if count == 0 {
                        break;
                    }
                }
                put.jobs_outstanding += jobs.len() as u32;
                for job in jobs {
                    self.queue_job(
                        job,
                        Owner::Put {
                            put: id,
                            window: spec.index,
                        },
                    );
                }
                for (rank, xfer) in shared {
                    let sent = self.ranks.as_ref().is_some_and(|ranks| {
                        ranks.send_window(
                            rank,
                            super::super::rank::link::Window {
                                xfer,
                                spec: Some(spec),
                                sums: None,
                            },
                        )
                    });
                    if !sent {
                        self.fail_put(id, Error::new(format!("rank {rank}'s link is down")));
                        break;
                    }
                }
            }
            Source::Host(_) if self.ranks.is_some() => {
                self.fail_put(
                    id,
                    Error::new("a multi-process instance writes GPU pages only"),
                );
                return true;
            }
            Source::Host(bytes) => {
                let bytes = bytes.clone();
                put.jobs_outstanding += 1;
                let (row, count) = (spec.row, spec.page_count);
                let job = FillJob {
                    owner: FillOwner::Put(id),
                    window: spec.index,
                    address,
                    length: spec.length as usize,
                    work: Box::new(move |dst| bytes.lock().unwrap().fill(first, count, row, dst)),
                };
                if !self.submit_fill(job) {
                    let put = self.writer.puts.get_mut(&id).expect("present");
                    put.jobs_outstanding -= 1;
                    self.fail_put(id, Error::new("the host copy thread cannot start"));
                }
            }
        }
        true
    }

    /// After a shared peer reported a window: finish the window if its other copies are in.
    pub(super) fn put_after_peer_window(&mut self, id: u64, window: u32) {
        let failed = self
            .writer
            .puts
            .get(&id)
            .is_some_and(|put| put.failed.is_some());
        if failed {
            self.send_cancel(id);
        } else if !self.put_peers_left(id, window)
            && self
                .writer
                .puts
                .get(&id)
                .is_some_and(|put| put.windows.contains_key(&window))
        {
            let running = {
                let owner_is = |o: &Owner| matches!(o, Owner::Put { put: a, window: b } if *a == id && *b == window);
                self.jobs.values().any(owner_is) || self.pending.iter().any(|(_, o)| owner_is(o))
            };
            if !running {
                self.window_filled(id, window, None);
            }
        }
        self.drive_put(id);
    }

    pub(super) fn put_drive_now(&mut self, id: u64) {
        self.drive_put(id);
    }

    /// A D2H job of a put drained.
    pub(crate) fn on_put_job(&mut self, id: u64, window: u32, result: crate::Result<()>) {
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        put.jobs_outstanding = put.jobs_outstanding.saturating_sub(1);
        match result {
            Err(error) => self.fail_put(id, error),
            Ok(()) => {
                let owner = Owner::Put { put: id, window };
                let running = self
                    .jobs
                    .values()
                    .any(|o| matches!((o, owner), (Owner::Put { put: a, window: b }, Owner::Put { put: c, window: d }) if *a == c && *b == d))
                    || self.pending.iter().any(|(_, o)| matches!(o, Owner::Put { put: a, window: b } if *a == id && *b == window))
                    || self.put_peers_left(id, window);
                if !running {
                    self.window_filled(id, window, None);
                }
            }
        }
        self.drive_put(id);
    }

    /// A host fill of a put finished.
    pub(crate) fn on_put_fill(
        &mut self,
        owner: FillOwner,
        window: u32,
        result: crate::Result<Checksums>,
    ) {
        let FillOwner::Put(id) = owner else {
            return;
        };
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        put.jobs_outstanding = put.jobs_outstanding.saturating_sub(1);
        match result {
            Err(error) => self.fail_put(id, error),
            Ok(sums) => self.window_filled(id, window, Some(sums)),
        }
        self.drive_put(id);
    }

    /// The window's bytes are in place: report it with the checksums to commit
    /// (the source's own when it persists them, otherwise of what was written).
    fn window_filled(&mut self, id: u64, window: u32, supplied: Option<Checksums>) {
        let page_bytes = self.st.page_record_bytes() as usize;
        // What this rank hashes: a multi-process instance's rank states checksums of its own
        // slices, which start each record of a shared window and fill a channel's slot.
        let on_channel = self.dial.is_some();
        let (stride, hash_len, row_len) = match &self.ranks {
            Some(ranks) => {
                let mine = ranks.layout.rank(0);
                (
                    if on_channel {
                        mine.page_len as usize
                    } else {
                        page_bytes
                    },
                    mine.page_len as usize,
                    mine.row_len as usize,
                )
            }
            None => (page_bytes, page_bytes, self.st.row_record_bytes() as usize),
        };
        let memory = self.memory();
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        if put.failed.is_some() || put.cancelled || put.refused.is_some() || put.stored.is_some() {
            return;
        }
        let Some((spec, dest)) = put.windows.remove(&window) else {
            return;
        };
        let sums = supplied.or_else(|| {
            let region = memory.as_ref()?.region(spec.region())?;
            let bytes = region.bytes(spec.offset, spec.length)?;
            let count = spec.page_count as usize;
            Some(Checksums {
                pages: (0..count)
                    .map(|i| checksum(&bytes[i * stride..i * stride + hash_len]))
                    .collect(),
                row: spec
                    .row
                    .then(|| checksum(&bytes[count * stride..count * stride + row_len])),
            })
        });
        let Some(sums) = sums.filter(|s| s.pages.len() == spec.page_count as usize) else {
            return self.fail_put(id, Error::new("a window's checksums are missing"));
        };
        // A multi-process writer cannot state whole-page checksums; the manager computes the
        // ones the record keeps.
        if self.ranks.is_none() {
            for (i, sum) in sums.pages.iter().enumerate() {
                put.sums[dest as usize + i] = *sum;
            }
            if spec.row {
                put.row_sum = sums.row;
            }
        }
        if put.via_channel {
            // The window is written to the manager on the channel, from its slot.
            let Some(slot) = put.slots.get(&window).copied() else {
                return self.fail_put(id, Error::new("a filled window has no staging slot"));
            };
            let sent = put.channel.as_ref().is_some_and(|channel| {
                channel.send(Filled {
                    index: window,
                    row_xxh3: sums.row,
                    page_xxh3: sums.pages,
                    slot,
                    length: spec.length,
                })
            });
            if sent {
                put.sends_outstanding += 1;
            } else {
                self.fail_put(id, Error::new("the data channel is gone"));
            }
            return;
        }
        if self.ranks.is_some() {
            // Rank 0 attests its own slice and those of the shared peers.
            let peers = self
                .writer
                .puts
                .get_mut(&id)
                .and_then(|put| put.peer_windows.remove(&window));
            let mut rank_sums = vec![wire::RankSums {
                rank: 0,
                page_xxh3: sums.pages,
                row_xxh3: sums.row.unwrap_or(0),
                has_row: sums.row.is_some(),
            }];
            for (rank, peer) in peers.map(|p| p.sums).unwrap_or_default() {
                rank_sums.push(wire::RankSums {
                    rank: rank as u32,
                    page_xxh3: peer.pages,
                    row_xxh3: peer.row.unwrap_or(0),
                    has_row: peer.row.is_some(),
                });
            }
            return self.emit(Out::PutFilled(wire::PutFilled {
                put_id: id,
                index: window,
                rank_sums,
                ..Default::default()
            }));
        }
        self.emit(Out::PutFilled(wire::PutFilled {
            put_id: id,
            index: window,
            page_xxh3: sums.pages,
            row_xxh3: sums.row.unwrap_or(0),
            has_row: sums.row.is_some(),
            rank_sums: Vec::new(),
        }));
    }

    /// A filled window has left its slot.
    pub(crate) fn on_net_sent(&mut self, put: u64, index: u32, slot: u32) {
        // The slot is free whatever became of the put: the task is done with it.
        if let Some(open) = self.writer.puts.get_mut(&put) {
            open.sends_outstanding = open.sends_outstanding.saturating_sub(1);
            open.slots.remove(&index);
        }
        self.free_slot(slot);
        self.drive_put(put);
    }

    /// A put's transfer failed.
    pub(crate) fn net_put_failed(&mut self, put: u64, error: String) {
        if self.writer.puts.contains_key(&put) {
            self.fail_put(put, Error::new(error));
        }
    }

    /// A put's task is gone and its connection closed.
    pub(crate) fn net_put_closed(&mut self, put: u64) {
        let Some(open) = self.writer.puts.get_mut(&put) else {
            return;
        };
        open.channel = None;
        open.channel_open = false;
        // Nothing sends from a slot any more.
        open.sends_outstanding = 0;
        let slots: Vec<u32> = open.slots.drain().map(|(_, slot)| slot).collect();
        let early = open.stored.is_none()
            && open.refused.is_none()
            && open.failed.is_none()
            && !open.cancelled
            && open.phase != Phase::Committed;
        for slot in slots {
            self.free_slot(slot);
        }
        if early {
            self.fail_put(
                put,
                Error::new("the data channel closed before the put was committed"),
            );
        }
        self.drive_put(put);
    }

    pub(crate) fn on_stored(&mut self, stored: wire::Stored) {
        if let Some(put) = self.writer.puts.get_mut(&stored.put_id) {
            put.stored = Some(stored.bytes);
            self.drive_put(stored.put_id);
        }
    }

    pub(crate) fn on_put_refused(&mut self, refused: wire::PutRefused) {
        if let Some(put) = self.writer.puts.get_mut(&refused.put_id) {
            put.refused.get_or_insert(refusal(refused.reason));
            self.drive_put(refused.put_id);
        }
    }

    /// Commit when every window is filled; end the put when it is over and its
    /// source is no longer read.
    fn drive_put(&mut self, id: u64) {
        let Some(put) = self.writer.puts.get_mut(&id) else {
            return;
        };
        let over =
            put.stored.is_some() || put.refused.is_some() || put.failed.is_some() || put.cancelled;
        if !over {
            let ready = put.phase == Phase::Accepted
                && put.coverage.as_ref().is_some_and(|c| c.complete())
                && put.windows.is_empty()
                && put.peer_windows.is_empty()
                && put.jobs_outstanding == 0
                && put.sends_outstanding == 0;
            if ready {
                return self.commit_put(id);
            }
            return;
        }
        // The put is over: the task stops, and nothing is acknowledged or freed
        // while a copy reads a window or the task may still use a slot.
        if let Some(channel) = &put.channel {
            channel.stop();
        }
        if put.jobs_outstanding > 0 || put.channel_open {
            return;
        }
        // The peer ranks end their parts too: after their copies drained.
        self.cancel_put_peers_if_over(id);
        let put = self.writer.puts.get_mut(&id).expect("present");
        if put.peers.iter().any(|peer| peer.done.is_none()) {
            return;
        }
        let fenced = put.stored.is_some() || put.phase == Phase::Begun || put.fence_sent;
        if !fenced {
            put.fence_sent = true;
            self.emit(Out::FenceAck(wire::FenceAck {
                subject: Some(wire::fence_ack::Subject::PutId(id)),
                outcome: wire::FenceOutcome::Drained as i32,
            }));
        }
        let put = self.writer.puts.remove(&id).expect("present");
        let outcome = if let Some(bytes) = put.stored {
            PutOutcome::Stored { bytes }
        } else if let Some(outcome) = put.refused {
            outcome
        } else if put.cancelled {
            PutOutcome::Cancelled
        } else {
            PutOutcome::Failed(put.failed.unwrap_or_else(|| Error::new("the put failed")))
        };
        self.event(PoolEvent::Put { id, outcome });
    }

    fn commit_put(&mut self, id: u64) {
        let put = &self.writer.puts[&id];
        let metadata = self.put_header(put).and_then(|mut header| {
            header.row_checksum = put.row_sum.unwrap_or(0);
            let mut bytes = header.encode()?.to_vec();
            let mut sums = vec![0u8; header.checksum_bytes() as usize];
            for (i, sum) in put.sums.iter().enumerate() {
                sums[i * 8..i * 8 + 8].copy_from_slice(&sum.to_le_bytes());
            }
            bytes.extend(sums);
            Ok(bytes)
        });
        match metadata {
            Err(error) => self.fail_put(id, error),
            Ok(metadata) => {
                self.writer.puts.get_mut(&id).expect("present").phase = Phase::Committed;
                self.emit(Out::PutCommit(wire::PutCommit {
                    put_id: id,
                    metadata_offset: 0,
                    metadata,
                }));
            }
        }
    }

    pub(crate) fn writer_tick(&mut self, now: Instant) {
        let late: Vec<u64> = self
            .writer
            .puts
            .iter()
            .filter(|(_, p)| {
                now >= p.deadline && p.failed.is_none() && p.stored.is_none() && p.refused.is_none()
            })
            .map(|(id, _)| *id)
            .collect();
        for id in late {
            self.fail_put(id, Error::new("the put ran out of time"));
        }
    }

    /// The session is gone: every put fails once its copies drained, and the
    /// fence for its windows is owed to the next session.
    pub(crate) fn writer_lost(&mut self) {
        let ids: Vec<u64> = self.writer.puts.keys().copied().collect();
        for id in ids {
            self.fail_put(id, Error::new("the pool session was lost"));
        }
    }

    pub(crate) fn writer_close(&mut self) {
        let ids: Vec<u64> = self.writer.puts.keys().copied().collect();
        for id in ids {
            self.cancel_put(id);
        }
    }

    pub(crate) fn writer_quarantine(&mut self) {
        let ids: Vec<u64> = self.writer.puts.keys().copied().collect();
        for id in ids {
            self.emit(Out::FenceAck(wire::FenceAck {
                subject: Some(wire::fence_ack::Subject::PutId(id)),
                outcome: wire::FenceOutcome::Quarantined as i32,
            }));
        }
        std::mem::forget(std::mem::take(&mut self.writer.puts));
    }
}
