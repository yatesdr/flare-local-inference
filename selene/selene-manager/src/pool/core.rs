//! The pool actor: sessions, readiness and the clock.

use super::{
    Violation,
    barrier::Remembered,
    claims::{Claim, ClaimKey},
    index::{InstanceId, ScopeIndex, SourceId, Staged},
    limits::Limits,
    store_bridge::{CatalogFeed, Link, PutKey, PutRec, StoreHandle},
    validate_scope,
};
use crate::{
    arena::Arena,
    data::channel::{self, Hub},
    local::{Pidfd, open_pidfd, pidfd_exited},
    proto::v2::{self, server_message::M},
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    os::fd::OwnedFd,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{mpsc, oneshot};

/// What a data-channel transfer belongs to.
#[derive(Clone, Copy, Debug)]
pub(super) enum ChanOwner {
    Claim(ClaimKey),
    Put(PutKey),
}

pub type Nonce = [u8; 16];
pub type ProcKey = (InstanceId, Nonce);
pub type ScopeId = [u8; 32];

/// The context an export is sized against when the staging region is checked: Flare's
/// usual per-request limit. The manager does not know a model's real maximum.
const REFERENCE_CONTEXT_TOKENS: u64 = 262_144;

/// Tokens of context one export can hold in `staging` bytes: whole pages and the row.
fn export_tokens(staging: u64, page_bytes: u64, row_bytes: u64, page_tokens: u64) -> u64 {
    staging.saturating_sub(row_bytes) / page_bytes.max(1) * page_tokens
}

#[cfg(test)]
mod staging_tests {
    use super::*;

    #[test]
    fn staging_holds_whole_pages_and_the_row() {
        // 4 GiB, 2 MiB pages of 64 tokens, a 112 MiB row.
        assert_eq!(
            export_tokens(4 << 30, 2 << 20, 112 << 20, 64),
            ((4 << 30) - (112 << 20)) / (2 << 20) * 64
        );
        assert_eq!(export_tokens(1 << 20, 2 << 20, 0, 64), 0);
        assert_eq!(export_tokens(10, 0, 0, 64), 10 * 64);
    }
}

pub struct PoolConfig {
    pub limits: Limits,
    /// How long a terminal claim's reply is kept to answer duplicates.
    pub reply_retention: Duration,
    /// The expected-source barrier's bounded wait, from process start.
    pub source_wait: Duration,
    /// Instances not seen for this long are forgotten.
    pub instance_retention: Duration,
    /// The manager's store directory; the barrier list lives here.
    pub store_directory: Option<PathBuf>,
    pub encryption: v2::Encryption,
    pub transports: Vec<v2::DataTransport>,
    /// The data channel instances without shared memory copy through, when the
    /// manager listens for other hosts.
    pub channel: Option<Hub>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            reply_retention: Duration::from_secs(60),
            source_wait: Duration::from_secs(10),
            instance_retention: Duration::from_secs(7 * 86_400),
            store_directory: None,
            encryption: v2::Encryption::Plaintext,
            transports: vec![v2::DataTransport::Tcp],
            channel: None,
        }
    }
}

/// What `Hello` reads without asking the actor.
pub struct Shared {
    pub incarnation: u64,
    pub limits: Limits,
    pub readiness: Mutex<v2::ReadinessInfo>,
    /// Prometheus text for the pool's own state, refreshed on the manager's clock.
    pub metrics: Mutex<String>,
    pub shm: v2::SharedMemory,
    pub host_id: [u8; 16],
    pub encryption: v2::Encryption,
    pub transports: Vec<v2::DataTransport>,
    pub uid: u32,
    pub gid: u32,
}

/// What the connection said about its peer.
#[derive(Clone, Copy, Debug)]
pub struct OpenPeer {
    /// A Unix-domain connection (the only kind whose credentials are meaningful).
    pub unix: bool,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}

pub enum Event {
    Open {
        peer: OpenPeer,
        open: Box<v2::OpenSession>,
        tx: mpsc::Sender<v2::ServerMessage>,
        reply: oneshot::Sender<Option<u64>>,
    },
    Message {
        sid: u64,
        message: Box<v2::ClientMessage>,
    },
    Closed {
        sid: u64,
    },
    PutAnswer {
        process: ProcKey,
        put_id: u64,
        answer: std::io::Result<crate::store::PutAnswer>,
    },
    GetAnswer {
        key: super::claims::ClaimKey,
        answer: std::io::Result<crate::store::GetAnswer>,
    },
    Catalog {
        scope: ScopeId,
        event: crate::proto::CatalogEvent,
    },
    /// A data-channel handler reported.
    Channel(channel::Event),
    Shutdown {
        done: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct PoolHandle {
    pub events: mpsc::Sender<Event>,
    pub shared: Arc<Shared>,
}

pub(super) struct Session {
    pub sid: u64,
    pub instance: InstanceId,
    pub nonce: Nonce,
    pub scope: ScopeId,
    pub tx: mpsc::Sender<v2::ServerMessage>,
    pub shm: bool,
    pub pid: u32,
    pub export_slots: u32,
    pub claim_slots: u32,
    /// The manager holds no entries for this session until a snapshot installs.
    pub need_resync: bool,
    /// `ResyncRequired` was sent for the current need and not yet answered.
    pub resync_requested: bool,
    pub expected_next: u64,
    pub staged: Option<Staged>,
    pub lookups: HashSet<u64>,
    /// `Goodbye` with this value was received.
    pub goodbye: Option<bool>,
    pub installed_once: bool,
    /// The outbound queue overflowed; the session is closed at the next turn.
    pub overflowed: bool,
}

pub(super) struct Instance {
    pub current: Option<u64>,
    pub scope: ScopeId,
}

pub(super) struct Proc {
    pub pid: Option<u32>,
    pub pidfd: Option<OwnedFd>,
    pub exited: bool,
    pub claim_hw: u64,
    pub put_hw: u64,
    /// When the process last had no session; a process nobody can watch is forgotten after
    /// an hour of that.
    pub idle_since: Option<Instant>,
    /// The process's connection to the manager's store, opened on first use.
    pub link: Option<Link>,
}

pub(super) struct ScopeState {
    pub valid: super::ValidScope,
    pub index: ScopeIndex,
}

pub struct Core {
    pub(super) cfg: PoolConfig,
    pub(super) store: Option<StoreHandle>,
    pub(super) self_tx: mpsc::Sender<Event>,
    pub(super) puts: HashMap<PutKey, PutRec>,
    /// Open data-channel transfers by id.
    pub(super) channels: HashMap<u64, ChanOwner>,
    pub(super) catalogs: HashMap<ScopeId, CatalogFeed>,
    pub(super) shared: Arc<Shared>,
    pub(super) incarnation: u64,
    pub(super) staging: Arena,
    pub(super) started: Instant,
    pub(super) sessions: HashMap<u64, Session>,
    pub(super) next_session: u64,
    pub(super) instances: HashMap<InstanceId, Instance>,
    pub(super) procs: HashMap<ProcKey, Proc>,
    pub(super) scopes: HashMap<ScopeId, ScopeState>,
    pub(super) claims: HashMap<super::claims::ClaimKey, Claim>,
    pub(super) exports: HashMap<u64, super::claims::ClaimKey>,
    pub(super) next_export: u64,
    /// Smoothed microseconds per page by source, from completed claims.
    pub(super) estimates: HashMap<SourceId, u32>,
    pub(super) live_lookups: usize,
    /// Counters by metric name and label, for the metrics text.
    pub(super) counters: std::collections::BTreeMap<(&'static str, String), u64>,
    pub(super) lookup_queue: super::lookup::LookupQueue,
    pub(super) state: v2::ReadinessState,
    pub(super) disk_recovered: bool,
    pub(super) recovered_records: u64,
    pub(super) remembered: Remembered,
    pub(super) expected: BTreeSet<InstanceId>,
    pub(super) synced: BTreeSet<InstanceId>,
    pub(super) barrier_dirty: bool,
    pub(super) barrier_saved: Instant,
    pub(super) shutting_down: Option<oneshot::Sender<()>>,
    pub(super) euid: u32,
    pub(super) egid: u32,
}

pub fn random_incarnation() -> u64 {
    use std::io::Read;
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .expect("read /dev/urandom");
    u64::from_le_bytes(bytes) | 1
}

fn host_id() -> [u8; 16] {
    use blake2::{Blake2b, Digest, digest::consts::U16};
    let mut hash = Blake2b::<U16>::new();
    hash.update(b"selene-host-v2");
    hash.update(
        std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap_or_default()
            .trim()
            .as_bytes(),
    );
    hash.finalize().into()
}

impl Core {
    /// Builds the actor and its handle. `shm` describes the region files the
    /// store opened; `staging` is a handle on the staging region the claims
    /// carve windows from.
    pub fn new(
        cfg: PoolConfig,
        shm: v2::SharedMemory,
        staging: Arena,
        disk_recovered: bool,
        recovered_records: u64,
        store: Option<mpsc::Sender<crate::store::StoreCommand>>,
    ) -> (Self, PoolHandle, mpsc::Receiver<Event>) {
        let incarnation = random_incarnation();
        let euid = unsafe { libc::geteuid() };
        let egid = unsafe { libc::getegid() };
        let shared = Arc::new(Shared {
            incarnation,
            limits: cfg.limits.clone(),
            readiness: Mutex::new(v2::ReadinessInfo::default()),
            metrics: Mutex::new(String::new()),
            shm,
            host_id: host_id(),
            encryption: cfg.encryption,
            transports: cfg.transports.clone(),
            uid: euid,
            gid: egid,
        });
        let remembered = match &cfg.store_directory {
            Some(directory) => {
                Remembered::load(directory, cfg.instance_retention, SystemTime::now())
                    .unwrap_or_else(|error| {
                        eprintln!("selene: warning: cannot read the instance list: {error}");
                        Remembered::default()
                    })
            }
            None => Remembered::default(),
        };
        let expected: BTreeSet<InstanceId> = remembered.ids().collect();
        let (events, receiver) = mpsc::channel(4096);
        let mut core = Self {
            cfg,
            store: store.map(|commands| StoreHandle {
                commands,
                next_connection: 1 << 40,
            }),
            self_tx: events.clone(),
            puts: HashMap::new(),
            channels: HashMap::new(),
            catalogs: HashMap::new(),
            shared: shared.clone(),
            incarnation,
            staging,
            started: Instant::now(),
            sessions: HashMap::new(),
            next_session: 0,
            instances: HashMap::new(),
            procs: HashMap::new(),
            scopes: HashMap::new(),
            claims: HashMap::new(),
            exports: HashMap::new(),
            next_export: 0,
            estimates: HashMap::new(),
            live_lookups: 0,
            counters: Default::default(),
            lookup_queue: Default::default(),
            state: v2::ReadinessState::Recovering,
            disk_recovered,
            recovered_records,
            remembered,
            expected,
            synced: BTreeSet::new(),
            barrier_dirty: false,
            barrier_saved: Instant::now(),
            shutting_down: None,
            euid,
            egid,
        };
        core.update_readiness();
        (core, PoolHandle { events, shared }, receiver)
    }

    /// Says once per scope, when its geometry first arrives, whether the staging region
    /// can hold one export of a long context. A GPU export reserves staging for the
    /// whole export, so a longer context than this is refused and recomputed.
    fn check_staging(&self, scope: &super::ValidScope) {
        let geometry = &scope.geometry;
        let staging = self.staging.capacity() as u64;
        let tokens = export_tokens(
            staging,
            geometry.page_bytes,
            geometry.row_bytes,
            geometry.page_tokens,
        );
        if tokens < REFERENCE_CONTEXT_TOKENS {
            eprintln!(
                "selene: warning: --staging {} holds one export of about {tokens} tokens for the \
                 scope that just opened (a page is {} bytes, the row {}); longer contexts are \
                 refused as NO_WINDOWS and recomputed. Use at least {} for {REFERENCE_CONTEXT_TOKENS} tokens.",
                staging,
                geometry.page_bytes,
                geometry.row_bytes,
                REFERENCE_CONTEXT_TOKENS.div_ceil(geometry.page_tokens.max(1)) * geometry.page_bytes
                    + geometry.row_bytes,
            );
        }
    }

    /// Runs until `Shutdown` has drained the claims in flight.
    pub async fn run(mut self, mut events: mpsc::Receiver<Event>) {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else { break };
                    self.handle(event);
                    // Answer lookups as soon as no more work is waiting, so a lone lookup
                    // does not wait for the clock; a burst still queues and meets the
                    // concurrency bound before the first of it is answered.
                    while events.is_empty() && !self.lookup_queue.is_empty() {
                        self.lookup_pass();
                    }
                }
                _ = tick.tick() => self.tick(),
            }
            if self.shutting_down.is_some() && self.drained() {
                break;
            }
        }
        self.finish_shutdown();
    }

    pub(super) fn handle(&mut self, event: Event) {
        match event {
            Event::Open {
                peer,
                open,
                tx,
                reply,
            } => {
                let sid = self.open(peer, *open, tx);
                let _ = reply.send(sid);
            }
            Event::Message { sid, message } => self.message(sid, *message),
            Event::Closed { sid } => self.session_lost(sid),
            Event::PutAnswer {
                process,
                put_id,
                answer,
            } => self.put_answer(process, put_id, answer),
            Event::GetAnswer { key, answer } => self.get_answer(key, answer),
            Event::Catalog { scope, event } => self.catalog_event(scope, event),
            Event::Channel(event) => self.channel_event(event),
            Event::Shutdown { done } => self.begin_shutdown(done),
        }
        self.flush_overflowed();
    }

    // ------------------------------------------------------------------ sending

    pub(super) fn send(&mut self, sid: u64, m: M) {
        let incarnation = self.incarnation;
        let Some(session) = self.sessions.get_mut(&sid) else {
            return;
        };
        let message = v2::ServerMessage {
            header: Some(v2::Header {
                manager_incarnation: incarnation,
                session_incarnation: sid,
            }),
            m: Some(m),
        };
        match session.tx.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => session.overflowed = true,
            // The client has gone; its reader reports the end of the stream.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// A session that cannot keep up with its queue is closed rather than allowed to
    /// grow the manager.
    fn flush_overflowed(&mut self) {
        let stuck: Vec<u64> = self
            .sessions
            .values()
            .filter(|session| session.overflowed)
            .map(|session| session.sid)
            .collect();
        for sid in stuck {
            eprintln!("selene: closing session {sid}: its outbound queue is full");
            self.session_lost(sid);
        }
    }

    pub(super) fn close_session(&mut self, sid: u64, reason: v2::ClosingReason, detail: &str) {
        self.send(
            sid,
            M::Closing(v2::SessionClosing {
                reason: reason as i32,
                detail: detail.to_string(),
            }),
        );
        self.session_lost(sid);
    }

    pub(super) fn violation(&mut self, sid: u64, violation: Violation) {
        eprintln!(
            "selene: protocol violation on session {sid}: {}",
            violation.0
        );
        self.close_session(
            sid,
            v2::ClosingReason::ClosingProtocolViolation,
            &violation.0,
        );
    }

    // ------------------------------------------------------------------ open

    fn open(
        &mut self,
        peer: OpenPeer,
        open: v2::OpenSession,
        tx: mpsc::Sender<v2::ServerMessage>,
    ) -> Option<u64> {
        let refuse = |status: v2::OpenStatus, core: &Core| {
            let _ = tx.try_send(v2::ServerMessage {
                header: Some(v2::Header {
                    manager_incarnation: core.incarnation,
                    session_incarnation: 0,
                }),
                m: Some(M::Opened(v2::SessionOpened {
                    status: status as i32,
                    ..Default::default()
                })),
            });
            None
        };
        if self.shutting_down.is_some() || self.state == v2::ReadinessState::Draining {
            return refuse(v2::OpenStatus::ShuttingDown, self);
        }
        if open.manager_incarnation != self.incarnation {
            return refuse(v2::OpenStatus::StaleManager, self);
        }
        let Some(scope) = validate_scope(open.scope.as_ref()) else {
            return refuse(v2::OpenStatus::InvalidScope, self);
        };
        let (Ok(instance), Ok(nonce)) = (
            InstanceId::try_from(open.instance_id.as_slice()),
            Nonce::try_from(open.process_nonce.as_slice()),
        ) else {
            return refuse(v2::OpenStatus::InvalidScope, self);
        };
        let limits = &self.cfg.limits;
        let shm = peer.unix && peer.uid == self.euid && peer.gid == self.egid;
        let export_slots = if shm {
            open.export_slots.min(limits.max_export_slots)
        } else {
            0
        };
        let claim_slots = open.claim_slots.min(limits.max_claims_in_flight);

        if let Some(old) = self.instances.get(&instance).and_then(|i| i.current) {
            self.close_session(
                old,
                v2::ClosingReason::ClosingSuperseded,
                "a newer session opened",
            );
        }

        self.next_session += 1;
        let sid = self.next_session;
        let proc_key = (instance, nonce);
        let started_pid = peer.unix.then_some(peer.pid).filter(|pid| *pid != 0);
        let entry = self.procs.entry(proc_key).or_insert_with(|| Proc {
            pid: None,
            pidfd: None,
            exited: false,
            claim_hw: 0,
            put_hw: 0,
            idle_since: None,
            link: None,
        });
        entry.idle_since = None;
        if entry.pid.is_none() {
            if let Some(pid) = started_pid {
                entry.pid = Some(pid);
                match open_pidfd(pid) {
                    Pidfd::Open(fd) => entry.pidfd = Some(fd),
                    Pidfd::Exited => entry.exited = true,
                    Pidfd::Unknown => {}
                }
            }
        }
        let fresh = !self.scopes.contains_key(&scope.id);
        self.scopes.entry(scope.id).or_insert_with(|| ScopeState {
            valid: scope.clone(),
            index: ScopeIndex::default(),
        });
        if fresh {
            self.watch_catalog(scope.id);
            self.check_staging(&scope);
        }
        self.instances.insert(
            instance,
            Instance {
                current: Some(sid),
                scope: scope.id,
            },
        );
        self.sessions.insert(
            sid,
            Session {
                sid,
                instance,
                nonce,
                scope: scope.id,
                tx,
                shm,
                pid: peer.pid,
                export_slots,
                claim_slots,
                need_resync: true,
                resync_requested: false,
                expected_next: 1,
                staged: None,
                lookups: HashSet::new(),
                goodbye: None,
                installed_once: false,
                overflowed: false,
            },
        );
        if self.remembered.touch(instance, SystemTime::now()) {
            self.barrier_dirty = true;
        }
        self.save_barrier(true);
        let readiness = self.readiness_info();
        let scope_id = scope.id.to_vec();
        self.send(
            sid,
            M::Opened(v2::SessionOpened {
                status: v2::OpenStatus::OpenOk as i32,
                session_incarnation: sid,
                scope_id,
                export_slots,
                claim_slots,
                readiness: Some(readiness),
                resync_required: true,
            }),
        );
        self.update_readiness();
        Some(sid)
    }

    // ------------------------------------------------------------------ lost

    /// The session's stream ended or it was closed. Everything it owned is lost.
    pub(super) fn session_lost(&mut self, sid: u64) {
        let Some(session) = self.sessions.remove(&sid) else {
            return;
        };
        let proc_key = (session.instance, session.nonce);
        if !self
            .sessions
            .values()
            .any(|other| (other.instance, other.nonce) == proc_key)
        {
            if let Some(proc) = self.procs.get_mut(&proc_key) {
                proc.idle_since = Some(Instant::now());
            }
        }
        if session.goodbye == Some(true) {
            self.party_quiesced(proc_key);
        }
        for _ in &session.lookups {
            self.live_lookups = self.live_lookups.saturating_sub(1);
        }
        let is_current = self
            .instances
            .get(&session.instance)
            .is_some_and(|instance| instance.current == Some(sid));
        if is_current {
            if let Some(instance) = self.instances.get_mut(&session.instance) {
                instance.current = None;
            }
            if let Some(scope) = self.scopes.get_mut(&session.scope) {
                scope.index.clear(SourceId::Instance(session.instance));
            }
            self.synced.remove(&session.instance);
        }
        self.session_claims_lost(sid, &session);
        self.puts_lost(&session);
        if self.remembered.touch(session.instance, SystemTime::now()) {
            self.barrier_dirty = true;
        }
        self.save_barrier(true);
        self.update_readiness();
    }

    // ------------------------------------------------------------------ messages

    fn message(&mut self, sid: u64, message: v2::ClientMessage) {
        let Some(header) = message.header else {
            return self.violation(sid, Violation::new("message without a header"));
        };
        if header.manager_incarnation != self.incarnation {
            // A message from before a manager restart: dropped.
            return;
        }
        let Some(body) = message.m else {
            return self.violation(sid, Violation::new("empty message"));
        };
        let fence_class = matches!(
            body,
            v2::client_message::M::FenceAck(_)
                | v2::client_message::M::ClaimRelease(_)
                | v2::client_message::M::WindowReleased(_)
                | v2::client_message::M::ExportEnd(_)
        );
        let current = self.sessions.get(&sid).is_some_and(|session| {
            self.instances
                .get(&session.instance)
                .is_some_and(|instance| instance.current == Some(sid))
        });
        if !self.sessions.contains_key(&sid) {
            return;
        }
        if !current && !fence_class {
            return self.close_session(
                sid,
                v2::ClosingReason::ClosingStaleSession,
                "superseded session",
            );
        }
        if header.session_incarnation != sid && !fence_class {
            return self.close_session(
                sid,
                v2::ClosingReason::ClosingStaleSession,
                "wrong session incarnation",
            );
        }
        use v2::client_message::M as C;
        let result = match body {
            C::Open(_) => Err(Violation::new("a second open on a session")),
            C::Goodbye(goodbye) => {
                if let Some(session) = self.sessions.get_mut(&sid) {
                    session.goodbye = Some(goodbye.quiesced);
                }
                Ok(())
            }
            C::ReadinessQuery(_) => {
                let info = self.readiness_info();
                self.send(sid, M::Readiness(info));
                Ok(())
            }
            C::ResyncBegin(m) => self.resync_begin(sid, m),
            C::ResyncChunk(m) => self.resync_chunk(sid, m),
            C::ResyncEnd(m) => self.resync_end(sid, m),
            C::Notices(m) => self.notices(sid, m),
            C::Lookup(m) => self.lookup(sid, m),
            C::LookupCancel(m) => {
                self.lookup_cancel(sid, m.lookup_id);
                Ok(())
            }
            C::Claim(m) => self.claim(sid, m),
            C::ClaimRelease(m) => self.claim_release(sid, m.claim_id),
            C::WindowReleased(m) => self.window_released(sid, m),
            C::CopyResult(m) => self.copy_result(sid, m),
            C::FenceAck(m) => self.fence_ack(sid, m),
            C::PinAck(m) => self.pin_ack(sid, m),
            C::WindowFilled(m) => self.window_filled(sid, m),
            C::ExportEnd(m) => self.export_end(sid, m),
            C::PutBegin(m) => self.put_begin(sid, m),
            C::PutFilled(m) => self.put_filled(sid, m),
            C::PutCommit(m) => self.put_commit(sid, m),
            C::PutCancel(m) => self.put_cancel(sid, m),
        };
        if let Err(violation) = result {
            self.violation(sid, violation);
        }
    }

    // ------------------------------------------------------------------ readiness

    pub(super) fn sources_connected(&self) -> u32 {
        self.instances
            .values()
            .filter(|instance| instance.current.is_some())
            .count() as u32
    }

    pub(super) fn readiness_info(&self) -> v2::ReadinessInfo {
        let waited = self.started.elapsed();
        let remaining = self.cfg.source_wait.saturating_sub(waited);
        let pending = self
            .expected
            .iter()
            .filter(|id| !self.synced.contains(*id))
            .count() as u32;
        v2::ReadinessInfo {
            state: self.state as i32,
            disk_recovered: self.disk_recovered,
            recovered_records: self.recovered_records,
            sources_connected: self.sources_connected(),
            sources_synced: self.synced.len() as u32,
            barrier_remaining_ms: if pending == 0 {
                0
            } else {
                remaining.as_millis() as u32
            },
            sources_expected: pending,
            quarantined_bytes: self.quarantined_bytes(),
        }
    }

    pub(super) fn ready(&self) -> bool {
        self.state == v2::ReadinessState::Ready
    }

    /// Recomputes the readiness state and pushes it when it changed.
    pub(super) fn update_readiness(&mut self) {
        let next = if self.shutting_down.is_some() {
            v2::ReadinessState::Draining
        } else if !self.disk_recovered {
            v2::ReadinessState::Recovering
        } else if self.expected.iter().all(|id| self.synced.contains(id))
            || self.started.elapsed() >= self.cfg.source_wait
        {
            v2::ReadinessState::Ready
        } else {
            v2::ReadinessState::AwaitingSources
        };
        let changed = next != self.state;
        self.state = next;
        let info = self.readiness_info();
        *self.shared.readiness.lock().unwrap() = info;
        if changed {
            let sids: Vec<u64> = self.sessions.keys().copied().collect();
            for sid in sids {
                self.send(sid, M::Readiness(info));
            }
        }
    }

    pub(super) fn mark_synced(&mut self, instance: InstanceId) {
        self.synced.insert(instance);
        self.update_readiness();
    }

    fn save_barrier(&mut self, force: bool) {
        let Some(directory) = self.cfg.store_directory.clone() else {
            return;
        };
        if !self.barrier_dirty && !force {
            return;
        }
        self.remembered
            .forget_older_than(self.cfg.instance_retention, SystemTime::now());
        if let Err(error) = self.remembered.save(&directory) {
            eprintln!("selene: warning: cannot write the instance list: {error}");
        }
        self.barrier_dirty = false;
        self.barrier_saved = Instant::now();
    }

    // ------------------------------------------------------------------ clock

    pub(super) fn tick(&mut self) {
        let now = Instant::now();
        self.poll_exits();
        self.lookup_pass();
        self.claim_timers(now);
        self.prune_procs();
        self.resync_timers(now);
        // Keep live instances fresh in the persisted list.
        if now.duration_since(self.barrier_saved) >= Duration::from_secs(5) {
            let live: Vec<InstanceId> = self
                .instances
                .iter()
                .filter(|(_, instance)| instance.current.is_some())
                .map(|(id, _)| *id)
                .collect();
            for id in live {
                self.remembered.touch(id, SystemTime::now());
                self.barrier_dirty = true;
            }
            self.save_barrier(false);
        }
        self.update_readiness();
        self.refresh_metrics();
        self.flush_overflowed();
    }

    /// Forgets processes that are gone and that nothing refers to any more.
    fn prune_procs(&mut self) {
        let referenced: std::collections::HashSet<ProcKey> = self
            .sessions
            .values()
            .map(|session| (session.instance, session.nonce))
            .chain(self.claims.keys().map(|key| (key.0, key.1)))
            .chain(self.claims.values().filter_map(|claim| claim.source_proc))
            .chain(self.puts.keys().map(|key| (key.0, key.1)))
            .collect();
        let now = Instant::now();
        let mut forget = Vec::new();
        let mut unlink = Vec::new();
        for (key, proc) in &self.procs {
            if referenced.contains(key) {
                continue;
            }
            let watched = proc.pidfd.is_some() || proc.pid.is_some();
            if !watched && proc.link.is_some() {
                // A remote process's store connection has nothing left on it.
                unlink.push(*key);
            }
            let keep = if watched {
                !proc.exited
            } else {
                // A remote process cannot be watched; forget it after an hour of silence.
                proc.idle_since
                    .is_none_or(|since| now.duration_since(since) < Duration::from_secs(3600))
            };
            if !keep {
                forget.push(*key);
            }
        }
        for key in unlink {
            self.drop_link(key);
        }
        for key in forget {
            self.drop_link(key);
            self.procs.remove(&key);
        }
    }

    fn poll_exits(&mut self) {
        let mut exited = Vec::new();
        for (key, proc) in &mut self.procs {
            if !proc.exited {
                if let Some(fd) = &proc.pidfd {
                    if pidfd_exited(fd) {
                        proc.exited = true;
                        exited.push(*key);
                    }
                }
            }
        }
        for key in exited {
            self.proc_exited(key);
        }
    }

    // ------------------------------------------------------------------ metrics

    pub(super) fn count(&mut self, name: &'static str, label: impl Into<String>) {
        *self.counters.entry((name, label.into())).or_insert(0) += 1;
    }

    fn refresh_metrics(&mut self) {
        let info = self.readiness_info();
        let in_flight = self
            .claims
            .values()
            .filter(|claim| !claim.is_terminal())
            .count();
        let entries: usize = self.scopes.values().map(|scope| scope.index.len()).sum();
        let mut text = String::new();
        let mut gauge =
            |name: &str, value: u64| text.push_str(&format!("cm_pool_{name} {value}\n"));
        gauge("state", info.state as u64);
        gauge("ready", u64::from(self.ready()));
        gauge("sessions", self.sessions.len() as u64);
        gauge("sources_synced", u64::from(info.sources_synced));
        gauge("sources_expected", u64::from(info.sources_expected));
        gauge("index_entries", entries as u64);
        gauge("claims_in_flight", in_flight as u64);
        gauge("claim_records", self.claims.len() as u64);
        gauge("puts_open", self.puts.len() as u64);
        gauge("lookups_queued", self.lookup_queue.len() as u64);
        gauge("quarantined_bytes", info.quarantined_bytes);
        for ((name, label), value) in &self.counters {
            text.push_str(&format!("cm_pool_{name}{{{label}}} {value}\n"));
        }
        if let Some(hub) = &self.cfg.channel {
            let moved = hub.moved();
            for (index, carrier) in ["stream", "rdma"].into_iter().enumerate() {
                text.push_str(&format!(
                    "cm_pool_channel_windows_total{{carrier=\"{carrier}\"}} {}\ncm_pool_channel_bytes_total{{carrier=\"{carrier}\"}} {}\n",
                    moved.windows[index], moved.bytes[index]
                ));
            }
        }
        *self.shared.metrics.lock().unwrap() = text;
    }

    // ------------------------------------------------------------------ shutdown

    fn begin_shutdown(&mut self, done: oneshot::Sender<()>) {
        self.shutting_down = Some(done);
        self.update_readiness();
    }

    fn drained(&self) -> bool {
        self.claims.values().all(|claim| claim.is_terminal()) && self.channels.is_empty()
    }

    fn finish_shutdown(&mut self) {
        if let Some(hub) = &self.cfg.channel {
            hub.shutdown();
        }
        let processes: Vec<ProcKey> = self.procs.keys().copied().collect();
        for process in processes {
            self.drop_link(process);
        }
        let sids: Vec<u64> = self.sessions.keys().copied().collect();
        for sid in sids {
            self.close_session(
                sid,
                v2::ClosingReason::ClosingShutdown,
                "the manager is shutting down",
            );
        }
        if let Some(done) = self.shutting_down.take() {
            let _ = done.send(());
        }
    }
}
