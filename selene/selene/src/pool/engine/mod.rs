//! The session engine: every protocol decision of the pool client, with no I/O.
//!
//! The connection task feeds it server messages, worker commands and the
//! passage of time, and sends what it queues. It owns the claim and export
//! state machines, the resync and notice flow, and the drain-before-release
//! rules, so each can be tested by scripting the manager's messages.

pub(crate) mod coverage;
mod hostfill;
#[cfg(test)]
mod net_tests;
mod netio;
pub(crate) mod ranks;
pub(crate) mod requester;
mod source;
#[cfg(test)]
mod tests;
mod writer;

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{CLOSE_TIMEOUT, HostRegistrar, Job, JobId, SharedCopier};

use super::{
    announce::Announcer,
    codec::{self, Sources},
    identity::Scope,
    net::{self, Dial, Staging},
    shared::{Command, PoolEvent, PoolStatus, Shared},
    shm::Regions,
    wire::{self, client_message::M as Out, server_message::M as In},
};

pub(crate) use ranks::Ranks;

/// The protocol version this client speaks.
pub const PROTOCOL: u32 = 2;
/// How long a lookup may wait for its reply.
pub const LOOKUP_TIMEOUT: Duration = Duration::from_millis(250);
/// Deadline asked of the manager for one claim.
pub const CLAIM_DEADLINE_MS: u32 = 10_000;
/// A restore that has not finished by then is abandoned.
pub const RESTORE_DEADLINE: Duration = Duration::from_secs(12);

/// What identifies and bounds this process's sessions.
pub(crate) struct Settings {
    pub instance_id: [u8; 16],
    pub nonce: [u8; 16],
    pub name: String,
    pub scope: Scope,
    pub scope_id: [u8; 32],
    pub export_slots: u32,
    pub claim_slots: u32,
    /// The context made current when registering the shared regions.
    pub context: usize,
    /// Set for rank 0 of a multi-process instance: the secret its peers attach with.
    pub rank_secret: Option<[u8; 32]>,
}

impl Settings {
    pub(crate) fn page_record_bytes(&self) -> u64 {
        self.scope.geometry.page_record_bytes
    }
    pub(crate) fn row_record_bytes(&self) -> u64 {
        self.scope.geometry.row_record_bytes
    }
}

struct Resync {
    id: u64,
    watermark: u64,
    chunks: VecDeque<Vec<wire::IndexEntry>>,
    sent_chunks: u32,
    entries: u64,
    begun: bool,
    ended: bool,
}

pub(crate) struct Session {
    header: wire::Header,
    pub(crate) limits: wire::Limits,
    pub(crate) regions: Option<Arc<Regions>>,
    installed: bool,
    ready: bool,
    resync: Option<Resync>,
    next_resync: u64,
    acked: u64,
    sent_through: u64,
    pub(crate) export_slots: u32,
}

/// Who is waiting for a copier job.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Owner {
    Claim { claim: u64, window: u32 },
    Export { export: u64, window: u32 },
    Put { put: u64, window: u32 },
}

/// Why the engine stopped serving a session.
#[derive(Debug)]
pub(crate) enum Ended {
    Closing(String),
    Violation(String),
}

pub(crate) struct Engine {
    pub(crate) st: Arc<Settings>,
    pub(crate) shared: Arc<Shared>,
    announce: Announcer,
    copier: SharedCopier,
    registrar: Arc<dyn HostRegistrar + Send + Sync>,
    sources: Sources,
    /// What the first connect attached to; `None` until then.
    pub(crate) attachment: Option<super::shm::Attachment>,
    pub(crate) sess: Option<Session>,
    /// Regions of a lost session, kept mapped until its copies have drained.
    draining: Vec<Arc<Regions>>,
    out: Vec<wire::ClientMessage>,
    /// Fence-class messages owed from before a reconnect, sent on the next session.
    owed: Vec<Out>,
    next_job: u64,
    jobs: HashMap<JobId, Owner>,
    pending: VecDeque<(Job, Owner)>,
    lookups: HashMap<u64, Instant>,
    pub(crate) requester: requester::State,
    pub(crate) source: source::State,
    pub(crate) writer: writer::State,
    quarantined: bool,
    drain_started: Option<Instant>,
    pub(crate) closing: bool,
    filler: Option<hostfill::HostFiller>,
    /// The registered slots windows of a session without shared memory pass through.
    pub(crate) net: Option<Arc<Staging>>,
    pub(crate) net_events: Arc<net::Events>,
    /// An RDMA transfer failed: the rest of this process's transfers use the stream.
    pub(crate) rdma_broken: Arc<std::sync::atomic::AtomicBool>,
    /// How the open session opens data-channel transfers, when it has no shared memory.
    pub(crate) dial: Option<Arc<dyn Dial>>,
    /// The peer ranks of a multi-process instance.
    pub(crate) ranks: Option<ranks::Ranks>,
}

impl Engine {
    pub(crate) fn new(
        st: Arc<Settings>,
        shared: Arc<Shared>,
        announce: Announcer,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
    ) -> Self {
        let net_events = net::Events::new(shared.wake.clone());
        Self {
            st,
            shared,
            announce,
            copier,
            registrar,
            sources: Sources::default(),
            sess: None,
            draining: Vec::new(),
            out: Vec::new(),
            owed: Vec::new(),
            next_job: 0,
            jobs: HashMap::new(),
            pending: VecDeque::new(),
            lookups: HashMap::new(),
            requester: Default::default(),
            source: Default::default(),
            writer: Default::default(),
            quarantined: false,
            drain_started: None,
            closing: false,
            filler: None,
            net: None,
            attachment: None,
            net_events,
            rdma_broken: Arc::default(),
            dial: None,
            ranks: None,
        }
    }

    /// Make this the rank 0 of a multi-process instance.
    pub(crate) fn set_ranks(&mut self, ranks: ranks::Ranks) {
        self.ranks = Some(ranks);
    }

    pub(crate) fn registrar(&self) -> &dyn HostRegistrar {
        &*self.registrar
    }

    /// Registration is over: from here every attempt to register host memory fails.
    pub(crate) fn close_registration(&mut self) {
        self.registrar = Arc::new(super::startup::ClosedRegistrar);
    }

    /// Copies are queued or running, so the copier needs polling soon.
    pub(crate) fn busy(&self) -> bool {
        !self.jobs.is_empty()
            || !self.pending.is_empty()
            || self.source.busy()
            || self.writer.busy()
    }

    pub(crate) fn quarantined(&self) -> bool {
        self.quarantined
    }

    /// Queue a message for the open session. Fence-class messages sent with no
    /// session are owed to the next one; anything else is dropped.
    pub(crate) fn emit(&mut self, message: Out) {
        match &self.sess {
            Some(sess) => self.out.push(wire::ClientMessage {
                header: Some(sess.header),
                m: Some(message),
            }),
            None => {
                if matches!(
                    message,
                    Out::FenceAck(_) | Out::ExportEnd(_) | Out::WindowReleased(_)
                ) {
                    self.owed.push(message);
                }
            }
        }
    }

    /// Everything queued for the wire since the last call.
    pub(crate) fn take_out(&mut self) -> Vec<wire::ClientMessage> {
        std::mem::take(&mut self.out)
    }

    // ---- session ----------------------------------------------------------

    /// The manager opened a session. `regions` are the mapped and registered
    /// shared-memory regions when the connection is same-host.
    pub(crate) fn session_opened(
        &mut self,
        manager_incarnation: u64,
        opened: &wire::SessionOpened,
        limits: wire::Limits,
        regions: Option<Arc<Regions>>,
    ) {
        let export_slots = if regions.is_some() || self.dial.is_some() {
            opened.export_slots.min(self.st.export_slots)
        } else {
            0
        };
        self.announce
            .set_max_entries(limits.max_resync_entries as usize);
        self.shared.set_max_lookups(limits.max_concurrent_lookups);
        self.sess = Some(Session {
            header: wire::Header {
                manager_incarnation,
                session_incarnation: opened.session_incarnation,
            },
            limits,
            regions,
            installed: false,
            ready: opened
                .readiness
                .as_ref()
                .is_some_and(|r| r.state == wire::ReadinessState::Ready as i32),
            resync: None,
            next_resync: 0,
            acked: 0,
            sent_through: 0,
            export_slots,
        });
        self.shared.set_status(PoolStatus::Connecting);
        for message in std::mem::take(&mut self.owed) {
            self.emit(message);
        }
        let snapshot = self.announce.begin_session();
        self.begin_resync(snapshot);
    }

    fn begin_resync(&mut self, snapshot: super::announce::Snapshot) {
        let Some(sess) = self.sess.as_mut() else {
            return;
        };
        sess.next_resync += 1;
        let per_chunk = (sess.limits.max_resync_chunk_entries as usize).max(1);
        let entries: Vec<wire::IndexEntry> = snapshot.entries.iter().map(codec::entry_to).collect();
        let total = entries.len() as u64;
        sess.resync = Some(Resync {
            id: sess.next_resync,
            watermark: snapshot.watermark,
            chunks: entries.chunks(per_chunk).map(|c| c.to_vec()).collect(),
            sent_chunks: 0,
            entries: total,
            begun: false,
            ended: false,
        });
        sess.installed = false;
        // The snapshot covers every notice up to its watermark.
        sess.sent_through = sess.sent_through.max(snapshot.watermark);
        sess.acked = sess.acked.max(snapshot.watermark);
        self.update_status();
    }

    /// The stream ended or the manager closed it: stop submitting, fail what
    /// depends on the session, and let the copies drain before anything is
    /// released. Callers keep ticking until [`Engine::quiet`].
    pub(crate) fn session_lost(&mut self, now: Instant) {
        let Some(sess) = self.sess.take() else {
            return;
        };
        self.dial = None;
        self.announce.end_session();
        self.out.clear();
        if let Some(regions) = sess.regions {
            self.draining.push(regions);
        }
        for (_, _) in self.lookups.drain() {}
        self.shared.fail_lookups();
        self.requester_lost();
        self.source_lost();
        self.writer_lost();
        self.drain_started.get_or_insert(now);
        self.shared.set_status(PoolStatus::Local);
        self.pump(now);
    }

    fn update_status(&self) {
        let ranks_attached = self.ranks_attached();
        let status = match &self.sess {
            None => return,
            Some(s) if !s.installed || !ranks_attached => PoolStatus::Connecting,
            Some(s) if s.ready => PoolStatus::Ready,
            Some(_) => PoolStatus::NotReady,
        };
        self.shared.set_status(status);
    }

    /// No copy, claim, export or queued job remains: memory of a lost session
    /// may be released.
    pub(crate) fn quiet(&self) -> bool {
        self.jobs.is_empty()
            && self.pending.is_empty()
            && self.ranks.as_ref().is_none_or(|r| r.idle())
            && self.requester.is_empty()
            && self.source.is_empty()
            && self.writer.is_empty()
    }

    // ---- server messages --------------------------------------------------

    pub(crate) fn handle(
        &mut self,
        now: Instant,
        message: wire::ServerMessage,
    ) -> std::result::Result<(), Ended> {
        let Some(sess) = &self.sess else {
            return Ok(());
        };
        if let Some(header) = &message.header
            && (header.manager_incarnation != sess.header.manager_incarnation
                || header.session_incarnation != sess.header.session_incarnation)
        {
            return Ok(());
        }
        let Some(m) = message.m else {
            return Ok(());
        };
        match m {
            In::Opened(_) => return Err(Ended::Violation("second SessionOpened".into())),
            In::Closing(closing) => {
                return Err(Ended::Closing(format!(
                    "manager closed the session ({:?}): {}",
                    wire::ClosingReason::try_from(closing.reason),
                    closing.detail
                )));
            }
            In::Readiness(info) => {
                if let Some(sess) = self.sess.as_mut() {
                    sess.ready = info.state == wire::ReadinessState::Ready as i32;
                }
                self.update_status();
            }
            In::ResyncRequired(_) => {
                let snapshot = self.announce.snapshot();
                self.begin_resync(snapshot);
            }
            In::ResyncInstalled(installed) => {
                if let Some(sess) = self.sess.as_mut()
                    && sess
                        .resync
                        .as_ref()
                        .is_some_and(|r| r.id == installed.resync_id)
                {
                    sess.resync = None;
                    sess.installed = true;
                    sess.acked = sess.acked.max(installed.applied_through);
                }
                self.update_status();
            }
            In::NoticeAck(ack) => {
                if let Some(sess) = self.sess.as_mut() {
                    sess.acked = sess.acked.max(ack.applied_through);
                }
            }
            In::LookupReply(reply) => self.on_lookup_reply(now, reply),
            In::ClaimGranted(granted) => self.on_claim_granted(now, granted),
            In::WindowReady(ready) => self.on_window_ready(ready),
            In::ClaimTerminal(terminal) => self.on_claim_terminal(terminal),
            In::ExportRequest(request) => self.on_export_request(now, request),
            In::ExportFill(fill) => self.on_export_fill(fill),
            In::ExportCancel(cancel) => self.on_export_cancel(cancel),
            In::PutAccept(accept) => self.on_put_accept(accept),
            In::PutGrant(grant) => self.on_put_grant(grant),
            In::Stored(stored) => self.on_stored(stored),
            In::PutRefused(refused) => self.on_put_refused(refused),
            // Room is only a hint for later eviction attempts.
            In::Room(_) => {}
        }
        self.pump(now);
        Ok(())
    }

    fn on_lookup_reply(&mut self, now: Instant, reply: wire::LookupReply) {
        if self.lookups.remove(&reply.lookup_id).is_none() {
            return;
        }
        let ok = reply.status == wire::LookupStatus::LookupOk as i32;
        if !ok {
            let retry = Duration::from_millis(u64::from(reply.retry_after_ms).max(100));
            self.shared.pause_lookups(now + retry);
            self.shared.complete_lookup(reply.lookup_id, None);
            return;
        }
        let max = self
            .sess
            .as_ref()
            .map_or(8, |s| s.limits.max_candidates as usize);
        let candidates = reply
            .candidates
            .iter()
            .take(max)
            .filter_map(|c| self.sources.candidate(c))
            .collect();
        self.shared
            .complete_lookup(reply.lookup_id, Some(candidates));
    }

    // ---- worker commands --------------------------------------------------

    pub(crate) fn command(&mut self, now: Instant, command: Command) {
        match command {
            Command::Lookup(lookup) => self.send_lookup(now, lookup),
            Command::CancelLookup(id) => {
                if self.lookups.remove(&id).is_some() {
                    self.emit(Out::LookupCancel(wire::LookupCancel { lookup_id: id }));
                }
            }
            Command::Fetch(fetch) => self.start_fetch(now, fetch),
            Command::ExportPinned { export, chain } => self.on_export_pinned(export, chain),
            Command::HostPinned { export, pin } => self.on_host_pinned(export, pin),
            Command::Put { id, request } => self.start_put(now, id, request),
            Command::PutCancel(id) => self.cancel_put(id),
            Command::ExportRefused { export, reason } => self.on_export_refused(export, reason),
        }
    }

    fn send_lookup(&mut self, now: Instant, lookup: super::shared::LookupSend) {
        let ready = self.sess.as_ref().is_some_and(|s| s.installed && s.ready);
        if !ready {
            self.shared.complete_lookup(lookup.id, None);
            return;
        }
        let limits = &self.sess.as_ref().unwrap().limits;
        let mut boundaries = lookup.boundaries;
        let cap = limits.max_boundaries as usize;
        if cap > 0 && boundaries.len() > cap {
            // The root and the deepest boundaries.
            let root = boundaries[0];
            let tail = boundaries.split_off(boundaries.len() - (cap - 1));
            boundaries = std::iter::once(root).chain(tail).collect();
        }
        let request = wire::LookupRequest {
            lookup_id: lookup.id,
            boundaries: boundaries.into_iter().map(codec::key_to).collect(),
            before_tokens: lookup.before,
            held: lookup
                .held
                .iter()
                .map(|range| wire::HeldRange {
                    level: codec::level_to(range.level) as i32,
                    end: Some(codec::key_to(range.end)),
                })
                .collect(),
            level_order: lookup
                .level_order
                .iter()
                .map(|level| codec::level_to(*level) as i32)
                .collect(),
            max_candidates: 0,
        };
        self.lookups.insert(lookup.id, now + LOOKUP_TIMEOUT);
        self.emit(Out::Lookup(request));
    }

    // ---- time and pumping -------------------------------------------------

    /// Advance timers and the copier, then flush what is queued.
    pub(crate) fn tick(&mut self, now: Instant) {
        let expired: Vec<u64> = self
            .lookups
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.lookups.remove(&id);
            self.emit(Out::LookupCancel(wire::LookupCancel { lookup_id: id }));
            self.shared.pause_lookups(now + Duration::from_millis(500));
            self.shared.complete_lookup(id, None);
        }
        self.requester_tick(now);
        self.source_tick(now);
        self.writer_tick(now);
        self.pump(now);
        if self.quiet() {
            self.drain_started = None;
            if self.sess.is_none() {
                self.draining.clear();
            }
        } else if self.sess.is_none()
            && self
                .drain_started
                .is_some_and(|start| now.duration_since(start) > CLOSE_TIMEOUT)
        {
            self.quarantine();
        }
    }

    /// Drain cannot be established: keep every pin, mapping and registration
    /// for the life of the process and report it.
    fn quarantine(&mut self) {
        if self.quarantined {
            return;
        }
        self.quarantined = true;
        self.shared.set_quarantined();
        for regions in self.draining.drain(..) {
            std::mem::forget(regions);
        }
        if let Some(staging) = self.net.take() {
            std::mem::forget(staging);
        }
        self.source_quarantine();
        self.requester_quarantine();
        self.writer_quarantine();
        self.shared.set_status(PoolStatus::Disabled(
            "a copy could not be drained; memory is quarantined".into(),
        ));
    }

    /// Poll the copier, submit queued jobs, and send resync chunks and notices.
    /// Returns whether another call has more to do right away.
    pub(crate) fn pump(&mut self, now: Instant) -> bool {
        self.collect_net();
        self.collect_ranks();
        let mut replies = Vec::new();
        self.copier.lock().unwrap().poll(&mut replies);
        for reply in replies {
            if let Some(owner) = self.jobs.remove(&reply.id) {
                match owner {
                    Owner::Claim { claim, window } => {
                        self.on_claim_job(claim, window, reply.result)
                    }
                    Owner::Export { export, window } => {
                        self.on_export_job(export, window, reply.result)
                    }
                    Owner::Put { put, window } => self.on_put_job(put, window, reply.result),
                }
            }
        }
        self.submit_pending();
        let more = self.flow_index();
        let _ = now;
        more
    }

    fn submit_pending(&mut self) {
        while let Some((job, owner)) = self.pending.pop_front() {
            let id = job.id;
            let result = self.copier.lock().unwrap().submit(job.clone());
            match result {
                Ok(()) => {
                    self.jobs.insert(id, owner);
                }
                Err(error) => {
                    let busy = self.copier.lock().unwrap().in_flight() > 0;
                    if busy {
                        self.pending.push_front((job, owner));
                        return;
                    }
                    // Rejected with nothing in flight: the job cannot run.
                    match owner {
                        Owner::Claim { claim, window } => {
                            self.on_claim_job(claim, window, Err(error))
                        }
                        Owner::Export { export, window } => {
                            self.on_export_job(export, window, Err(error))
                        }
                        Owner::Put { put, window } => self.on_put_job(put, window, Err(error)),
                    }
                }
            }
        }
    }

    pub(crate) fn queue_job(&mut self, mut job: Job, owner: Owner) -> JobId {
        self.next_job += 1;
        job.id = JobId(self.next_job);
        let id = job.id;
        self.pending.push_back((job, owner));
        id
    }

    /// Resync chunks (one per call) and unsent notices.
    fn flow_index(&mut self) -> bool {
        if self.sess.is_none() {
            return false;
        }
        let overflow = self.announce.overflowed()
            || self.sess.as_ref().is_some_and(|s| {
                s.limits.max_unacked_notices > 0
                    && s.sent_through.saturating_sub(s.acked)
                        >= u64::from(s.limits.max_unacked_notices)
            });
        if overflow {
            let snapshot = self.announce.snapshot();
            self.begin_resync(snapshot);
        }
        let mut more = false;
        let mut messages = Vec::new();
        {
            let sess = self.sess.as_mut().unwrap();
            if let Some(resync) = sess.resync.as_mut() {
                if !resync.begun {
                    resync.begun = true;
                    messages.push(Out::ResyncBegin(wire::ResyncBegin {
                        resync_id: resync.id,
                        watermark: resync.watermark,
                    }));
                } else if let Some(entries) = resync.chunks.pop_front() {
                    messages.push(Out::ResyncChunk(wire::ResyncChunk {
                        resync_id: resync.id,
                        chunk_index: resync.sent_chunks,
                        entries,
                    }));
                    resync.sent_chunks += 1;
                } else if !resync.ended {
                    resync.ended = true;
                    messages.push(Out::ResyncEnd(wire::ResyncEnd {
                        resync_id: resync.id,
                        chunks: resync.sent_chunks,
                        entries: resync.entries,
                    }));
                }
                more = !resync.ended;
            }
        }
        let begun = self
            .sess
            .as_ref()
            .is_some_and(|s| s.resync.as_ref().is_none_or(|r| r.begun));
        for message in messages {
            self.emit(message);
        }
        if begun {
            let max = self
                .sess
                .as_ref()
                .map_or(1, |s| s.limits.max_notices_per_batch.max(1) as usize);
            if let Some((first, notices)) = self.announce.take_batch(max) {
                let count = notices.len() as u64;
                self.emit(Out::Notices(wire::NoticeBatch {
                    first_sequence: first,
                    notices: notices.iter().map(codec::notice_to).collect(),
                }));
                if let Some(sess) = self.sess.as_mut() {
                    sess.sent_through = sess.sent_through.max(first + count - 1);
                }
                more = true;
            }
        }
        more
    }

    /// Whether the claim limit allows another restore: the pool must be ready.
    pub(crate) fn ready(&self) -> bool {
        self.sess
            .as_ref()
            .is_some_and(|s| s.installed && s.ready && self.ranks_attached())
    }

    /// Every peer rank is attached to the manager this session is with.
    fn ranks_attached(&self) -> bool {
        match (&self.ranks, &self.sess) {
            (None, _) => true,
            (Some(ranks), Some(sess)) => ranks.attached(sess.header.manager_incarnation),
            (Some(_), None) => false,
        }
    }

    /// Apply what the peer ranks said since the last call.
    fn collect_ranks(&mut self) {
        use ranks::{RankEvent, XferOwner};
        let Some(ranks) = self.ranks.as_mut() else {
            return;
        };
        let events = ranks.poll();
        let any = !events.is_empty();
        for event in events {
            match event {
                RankEvent::WindowDone {
                    rank,
                    xfer,
                    index,
                    result,
                    sums,
                } => {
                    let owner = self.ranks.as_ref().and_then(|r| r.owner(xfer));
                    match owner {
                        Some((XferOwner::Claim(claim), _)) => {
                            self.on_peer_window_done(claim, rank, index, result)
                        }
                        Some((XferOwner::Export(export), _)) => {
                            self.on_peer_export_window(export, rank, index, result, sums)
                        }
                        Some((XferOwner::Put(put), _)) => {
                            self.on_peer_put_window(put, rank, index, result, sums)
                        }
                        None => {}
                    }
                }
                RankEvent::Done {
                    rank,
                    xfer,
                    result,
                    quarantined,
                } => {
                    let owner = self.ranks.as_mut().and_then(|r| {
                        let owner = r.owner(xfer);
                        r.forget(xfer);
                        owner
                    });
                    match owner {
                        Some((XferOwner::Claim(claim), _)) => {
                            self.on_peer_done(claim, rank, result, quarantined)
                        }
                        Some((XferOwner::Export(export), _)) => {
                            self.on_peer_export_done(export, rank, result, quarantined)
                        }
                        Some((XferOwner::Put(put), _)) => {
                            self.on_peer_put_done(put, rank, result, quarantined)
                        }
                        None => {}
                    }
                }
                RankEvent::Attached | RankEvent::Detached => {}
                RankEvent::Lost { rank, reason } => self.rank_lost(rank, &reason),
            }
        }
        if any {
            self.update_status();
        }
    }

    /// A peer rank's link failed: whatever it was doing, nothing can show it stopped.
    fn rank_lost(&mut self, rank: usize, reason: &str) {
        eprintln!("warning: the link to rank {rank} failed ({reason}); the pool is off");
        let open = self
            .ranks
            .as_ref()
            .map(|ranks| ranks.open_on(rank))
            .unwrap_or_default();
        if open.is_empty() {
            self.shared
                .set_status(PoolStatus::Disabled(format!("rank {rank}'s link failed")));
            self.shared.fail_lookups();
            return;
        }
        self.quarantine();
    }

    // ---- shutdown -----------------------------------------------------------

    /// Stop submitting new work and release what has drained. Returns the
    /// `quiesced` flag for Goodbye: true only if nothing could still be
    /// touched by this process.
    pub(crate) fn begin_close(&mut self, now: Instant) {
        self.closing = true;
        self.lookups.clear();
        self.shared.fail_lookups();
        self.requester_close();
        self.source_close();
        self.writer_close();
        self.pump(now);
    }

    /// Run a window fill on the host copy thread, starting it on first use.
    pub(crate) fn submit_fill(&mut self, job: hostfill::FillJob) -> bool {
        if self.filler.is_none() {
            match hostfill::HostFiller::start(self.shared.wake.clone()) {
                Ok(filler) => self.filler = Some(filler),
                Err(_) => return false,
            }
        }
        self.filler
            .as_ref()
            .is_some_and(|filler| filler.submit(job))
    }

    pub(crate) fn collect_fills(&self) -> Vec<hostfill::FillDone> {
        let mut done = Vec::new();
        if let Some(filler) = &self.filler {
            filler.collect(&mut done);
        }
        done
    }

    pub(crate) fn goodbye(&mut self) {
        let quiesced = self.quiet() && !self.quarantined;
        self.emit(Out::Goodbye(wire::Goodbye { quiesced }));
    }

    pub(crate) fn event(&self, event: PoolEvent) {
        self.shared.push_event(event);
    }
}
