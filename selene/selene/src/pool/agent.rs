//! The rank agent: the part of a multi-process instance that runs in each peer rank.
//!
//! Rank 0 of such an instance decides everything. A peer rank runs a [`RankAgent`], which
//! decides nothing: it is told, over the rank link, which of this rank's GPU pages to copy
//! to or from which window of the manager, does that for its own slice of each window
//! (shared memory in place, or its own data channel), and says when it has drained.
//!
//! Every transfer is bound to the manager incarnation and attachment generation this agent
//! reported to rank 0, and keeps the mapping it was opened against until it ends, so a
//! delayed command can never resolve against a newer mapping. A transfer ends with exactly
//! one `Done`, sent after exact coverage and after every accepted copy drained (and, for a
//! channel, the socket closed). What this agent may still be touching is its own
//! obligation to the manager: it attests it with a `RankFence` of its own.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    CLOSE_TIMEOUT, Direction, Error, HostRegistrar, Job, JobId, PageId, Result, SharedCopier,
    StateSlotId, record::checksum, slab::WINDOW_PAGES,
};

use super::{
    client::PoolAddress,
    codec, conn,
    engine::{coverage::Coverage, requester::ExpectedRecord},
    identity::Scope,
    net::{self, Channel, Dial, Event, Id, Staging, Tcp},
    rank::{Incoming, LinkEnd, Msg, RankLink, link},
    shared::PoolStatus,
    shm::Regions,
    slice::{SliceLayout, SliceSums},
    wire::{self, rank_client_message as rc, rank_server_message as rs},
};

const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const BUSY_TICK: Duration = Duration::from_millis(1);
const IDLE_TICK: Duration = Duration::from_millis(20);
const ATTACH_TIMEOUT: Duration = Duration::from_secs(5);
/// Finished transfers remembered, so a late command for one is recognised.
const TOMBSTONES: usize = 1024;
/// Slots of this agent's own staging when its slices travel on a channel.
const STAGING_SLOTS: u32 = 16;

/// What a peer rank's agent is built from. The pool address, CA and tenant are this
/// process's own flags; `scope` is the instance's, identical to rank 0's.
pub struct AgentConfig {
    pub address: PoolAddress,
    pub ca: Option<PathBuf>,
    pub scope: Scope,
    pub rank: usize,
    pub world: usize,
    pub instance_name: String,
    /// The link to rank 0.
    pub link: RankLink,
    /// This rank's page copier, built for its own one-rank geometry.
    pub copier: SharedCopier,
    pub registrar: Arc<dyn HostRegistrar + Send + Sync>,
    /// The device context made current when registering memory.
    pub registration_context: usize,
    pub gpu_uuid: [u8; 16],
}

/// A running agent.
pub struct RankAgent {
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    status: Arc<Mutex<PoolStatus>>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl RankAgent {
    /// Start the agent's thread and return at once.
    pub fn start(config: AgentConfig) -> Result<RankAgent> {
        Self::start_with(config, super::net::Roots::System)
    }

    pub(crate) fn start_with(config: AgentConfig, roots: net::Roots) -> Result<RankAgent> {
        let layout = Arc::new(SliceLayout::new(&config.scope.geometry)?);
        if config.world != layout.world() || config.rank == 0 || config.rank >= config.world {
            return Err(Error::new("the rank does not fit the scope's rank count"));
        }
        {
            let copier = config.copier.lock().unwrap();
            let mine = layout.rank(config.rank);
            if copier.layout().page_record_bytes != mine.page_len
                || copier.layout().row_record_bytes != mine.row_len
            {
                return Err(Error::new(
                    "the pool scope does not match the copier's layout for this rank",
                ));
            }
        }
        let wake = Arc::new(Notify::new());
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new(PoolStatus::Connecting));
        let end = LinkEnd::start(config.link, wake.clone())
            .map_err(|e| Error::caused_by("cannot start the rank link", e))?;
        let agent = Agent {
            address: config.address,
            ca: config.ca,
            roots,
            fingerprint: config.scope.layout_fingerprint,
            layout,
            rank: config.rank,
            world: config.world,
            name: config.instance_name,
            gpu_uuid: config.gpu_uuid,
            context: config.registration_context,
            copier: config.copier,
            registrar: config.registrar,
            wake: wake.clone(),
            stop: stop.clone(),
            status: status.clone(),
            end,
            agent_nonce: super::identity::process_nonce()?,
            bound: None,
            att: None,
            staging: None,
            net_events: net::Events::new(wake.clone()),
            dial: None,
            xfers: HashMap::new(),
            high: 0,
            tombstones: VecDeque::new(),
            jobs: HashMap::new(),
            pending: VecDeque::new(),
            next_job: 0,
            owed: Vec::new(),
            quarantined: false,
            closing: false,
        };
        let thread = std::thread::Builder::new()
            .name("selene-rank-agent".into())
            .spawn(move || run(agent))
            .map_err(|e| Error::caused_by("cannot start the rank agent thread", e))?;
        Ok(RankAgent {
            stop,
            wake,
            status,
            thread: Some(thread),
        })
    }

    pub fn status(&self) -> PoolStatus {
        self.status.lock().unwrap().clone()
    }

    /// Stop, drain every transfer, attest it to the manager and end. `Err` means copies
    /// may still be running: the caller must keep the memory they touch.
    pub fn close(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        self.wake.notify_one();
        match self.thread.take() {
            Some(thread) => thread
                .join()
                .map_err(|_| Error::new("the rank agent thread panicked"))?,
            None => Ok(()),
        }
    }
}

impl Drop for RankAgent {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

// ----------------------------------------------------------------------------------

/// The manager attachment of the running session.
struct Att {
    incarnation: u64,
    generation: u64,
    mode: wire::RankMode,
    regions: Option<Arc<Regions>>,
    tx: mpsc::Sender<wire::RankClientMessage>,
}

struct Bound {
    rank0_nonce: Vec<u8>,
    secret: Vec<u8>,
    instance_id: Vec<u8>,
}

/// One window of a transfer.
struct Win {
    jobs_left: u32,
    spec: wire::WindowSpec,
    /// The staging slot a channel window lives in.
    slot: Option<u32>,
}

struct Xfer {
    out: bool,
    kind: i32,
    fence_id: u64,
    coverage: Coverage,
    pages: Vec<PageId>,
    slot: Option<StateSlotId>,
    shared: bool,
    /// The mapping this transfer was opened against, kept until it ends.
    regions: Option<Arc<Regions>>,
    channel: Option<Channel>,
    channel_open: bool,
    ended: bool,
    windows: BTreeMap<u32, Win>,
    jobs_outstanding: u32,
    failed: Option<String>,
    cancelled: bool,
    page_bytes: u64,
    row_bytes: u64,
}

struct Agent {
    address: PoolAddress,
    ca: Option<PathBuf>,
    roots: net::Roots,
    fingerprint: [u8; 16],
    layout: Arc<SliceLayout>,
    rank: usize,
    world: usize,
    name: String,
    gpu_uuid: [u8; 16],
    context: usize,
    copier: SharedCopier,
    registrar: Arc<dyn HostRegistrar + Send + Sync>,
    wake: Arc<Notify>,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<PoolStatus>>,
    end: LinkEnd,
    agent_nonce: [u8; 16],
    bound: Option<Bound>,
    att: Option<Att>,
    staging: Option<Arc<Staging>>,
    net_events: Arc<net::Events>,
    dial: Option<Arc<Tcp>>,
    xfers: HashMap<u64, Xfer>,
    /// Transfers at or below this have been opened; a lower id is a replay.
    high: u64,
    tombstones: VecDeque<u64>,
    jobs: HashMap<JobId, (u64, u32)>,
    pending: VecDeque<(Job, (u64, u32))>,
    next_job: u64,
    /// Fences for the manager not yet delivered: (kind, id, drained).
    owed: Vec<(i32, u64, bool)>,
    quarantined: bool,
    closing: bool,
}

fn run(mut agent: Agent) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::caused_by("cannot start the rank agent runtime", e))?;
    runtime.block_on(async {
        let result = agent.serve().await;
        agent.finish().await;
        let status = agent.status.clone();
        let mut status = status.lock().unwrap();
        if !matches!(*status, PoolStatus::Disabled(_)) {
            *status = PoolStatus::Disabled("the agent stopped".into());
        }
        result
    })
}

impl Agent {
    fn set_status(&self, status: PoolStatus) {
        *self.status.lock().unwrap() = status;
    }

    fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    async fn serve(&mut self) -> Result<()> {
        if !self.greet().await {
            return Ok(());
        }
        let mut backoff = FIRST_BACKOFF;
        let mut warned = false;
        loop {
            if self.stopping() || self.end.failed() {
                return Ok(());
            }
            let started = Instant::now();
            match self.attempt().await {
                Attempt::Disabled(why) => {
                    eprintln!("warning: rank {}: {why}", self.rank);
                    self.set_status(PoolStatus::Disabled(why));
                    return Ok(());
                }
                Attempt::Retry(why) => {
                    if !warned {
                        warned = true;
                        eprintln!(
                            "warning: rank {}: cannot attach to the pool manager ({why}); will keep trying",
                            self.rank
                        );
                    }
                }
                Attempt::Ended => {
                    warned = false;
                    if started.elapsed() > MAX_BACKOFF {
                        backoff = FIRST_BACKOFF;
                    }
                }
            }
            // Whatever the attachment was doing is cancelled and drained before the next one.
            self.cancel_all();
            if !self.drain().await {
                return Err(Error::new("the rank agent could not drain its transfers"));
            }
            if self.att.take().is_some() {
                self.end.send(Msg::Detached(link::Detached {
                    reason: "the manager attachment ended".into(),
                }));
            }
            if self.stopping() || self.end.failed() {
                return Ok(());
            }
            self.set_status(PoolStatus::Local);
            let until = Instant::now() + backoff;
            while Instant::now() < until && !self.stopping() && !self.end.failed() {
                self.events();
                let _ = tokio::time::timeout(IDLE_TICK, self.wake.notified()).await;
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// The last act: the link says goodbye.
    async fn finish(&mut self) {
        self.end.send(Msg::Close(link::Close {}));
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.end.close();
    }

    /// Stop work: cancel, drain, attest to the manager what was touched, and, if
    /// `goodbye`, say whether everything stopped. The stream must still be open.
    async fn wind_down(&mut self, goodbye: bool) {
        self.cancel_all();
        let drained = self.drain().await;
        self.deliver_owed();
        if goodbye && let Some(att) = &self.att {
            let quiesced = drained && self.xfers.is_empty() && !self.quarantined;
            let _ = att
                .tx
                .send(wire::RankClientMessage {
                    m: Some(rc::M::Goodbye(wire::RankGoodbye { quiesced })),
                })
                .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The link handshake with rank 0: our hello, theirs, then the secret.
    async fn greet(&mut self) -> bool {
        self.end.send(Msg::AgentHello(link::AgentHello {
            rank: self.rank as u32,
            world: self.world as u32,
            agent_nonce: self.agent_nonce.to_vec(),
            layout: self.fingerprint.to_vec(),
            gpu_uuid: self.gpu_uuid.to_vec(),
        }));
        let mut hello = None;
        while !self.stopping() {
            let mut incoming = Vec::new();
            self.end.poll(&mut incoming);
            for item in incoming {
                match item {
                    Incoming::Closed(why) => {
                        self.set_status(PoolStatus::Disabled(format!(
                            "the rank link failed: {why}"
                        )));
                        return false;
                    }
                    Incoming::Message(Msg::Hello(h)) if hello.is_none() => hello = Some(h),
                    Incoming::Message(Msg::Bind(bind)) => {
                        let Some(h) = hello.take() else {
                            self.set_status(PoolStatus::Disabled("bind before hello".into()));
                            return false;
                        };
                        let fine = h.rank as usize == self.rank
                            && h.world as usize == self.world
                            && h.layout == self.fingerprint
                            && bind.rank_secret.len() == 32;
                        if !fine {
                            self.set_status(PoolStatus::Disabled(
                                "rank 0 disagrees about the instance".into(),
                            ));
                            return false;
                        }
                        self.bound = Some(Bound {
                            rank0_nonce: h.rank0_nonce,
                            secret: bind.rank_secret,
                            instance_id: bind.instance_id,
                        });
                        return true;
                    }
                    Incoming::Message(Msg::Close(_)) => return false,
                    Incoming::Message(_) => {}
                }
            }
            let _ = tokio::time::timeout(IDLE_TICK, self.wake.notified()).await;
        }
        false
    }

    /// Reach the manager, attach, and serve until the attachment ends.
    async fn attempt(&mut self) -> Attempt {
        let link = conn::Link {
            address: self.address.clone(),
            ca: self.ca.clone(),
            roots: self.roots.clone(),
            wake: self.wake.clone(),
            stop: self.stop.clone(),
            startup: Arc::new(super::startup::Startup::new()),
            approve: None,
        };
        let (channel, connected_with) = match conn::connect(&link).await {
            Ok(connected) => connected,
            Err(conn::Unreachable::Retry(why)) => return Attempt::Retry(why),
            Err(conn::Unreachable::Untrusted(why)) => {
                return Attempt::Disabled(format!(
                    "cannot verify the pool manager over TLS ({why})"
                ));
            }
        };
        let mut client = wire::pool_client::PoolClient::new(channel);
        let hello = tokio::time::timeout(
            ATTACH_TIMEOUT,
            client.hello(wire::HelloRequest {
                protocol: super::engine::PROTOCOL,
                client_version: env!("CARGO_PKG_VERSION").into(),
                connected_with: connected_with as i32,
            }),
        )
        .await;
        let hello = match hello {
            Ok(Ok(reply)) => reply.into_inner(),
            Ok(Err(status)) => return Attempt::Retry(format!("Hello failed: {status}")),
            Err(_) => return Attempt::Retry("Hello timed out".into()),
        };
        if let Err(why) = conn::check_hello(&hello, connected_with) {
            return Attempt::Disabled(why);
        }
        let Some(bound) = self.bound.as_ref() else {
            return Attempt::Disabled("not bound to rank 0".into());
        };
        let (tx, rx) = mpsc::channel::<wire::RankClientMessage>(256);
        let attach = wire::RankAttach {
            manager_incarnation: hello.manager_incarnation,
            instance_id: bound.instance_id.clone(),
            rank0_nonce: bound.rank0_nonce.clone(),
            rank: self.rank as u32,
            agent_nonce: self.agent_nonce.to_vec(),
            rank_secret: bound.secret.clone(),
            drain_only: false,
            name: self.name.clone(),
        };
        let _ = tx
            .send(wire::RankClientMessage {
                m: Some(rc::M::Attach(attach)),
            })
            .await;
        let response =
            tokio::time::timeout(ATTACH_TIMEOUT, client.rank(ReceiverStream::new(rx))).await;
        let mut stream = match response {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => return Attempt::Retry(format!("attach failed: {status}")),
            Err(_) => return Attempt::Retry("attaching timed out".into()),
        };
        let attached = match tokio::time::timeout(ATTACH_TIMEOUT, stream.message()).await {
            Ok(Ok(Some(wire::RankServerMessage {
                m: Some(rs::M::Attached(attached)),
            }))) => attached,
            Ok(Ok(other)) => return Attempt::Retry(format!("unexpected answer {other:?}")),
            Ok(Err(status)) => return Attempt::Retry(format!("attach failed: {status}")),
            Err(_) => return Attempt::Retry("the manager did not answer the attach".into()),
        };
        use wire::RankAttachStatus as S;
        match S::try_from(attached.status) {
            Ok(S::RankOk) => {}
            Ok(S::RankNoSession | S::RankStaleManager) => {
                return Attempt::Retry("rank 0 has no session yet".into());
            }
            Ok(S::RankMixedMode) => {
                return Attempt::Disabled(
                    "the manager has no data listener this rank can reach".into(),
                );
            }
            other => {
                return Attempt::Disabled(format!("the manager refused the attach ({other:?})"));
            }
        }
        let mode = wire::RankMode::try_from(attached.mode).unwrap_or(wire::RankMode::Unspecified);
        let regions = match (mode, hello.shared_memory.as_ref()) {
            (wire::RankMode::RankShared, Some(shared)) => match self.map_regions(shared) {
                Ok(regions) => Some(Arc::new(regions)),
                Err(error) => {
                    return Attempt::Disabled(format!(
                        "cannot use the pool's shared memory: {error}"
                    ));
                }
            },
            (wire::RankMode::RankShared, None) => {
                return Attempt::Disabled(
                    "the manager gave shared mode but no shared memory".into(),
                );
            }
            _ => None,
        };
        if mode == wire::RankMode::RankChannel
            && let Err(error) = self.make_dial(&link, &hello)
        {
            return Attempt::Disabled(format!("cannot use the pool's data channel: {error}"));
        }
        self.att = Some(Att {
            incarnation: hello.manager_incarnation,
            generation: attached.generation,
            mode,
            regions,
            tx,
        });
        self.set_status(PoolStatus::Ready);
        self.deliver_owed();
        self.end.send(Msg::Attached(link::Attached {
            manager_incarnation: hello.manager_incarnation,
            generation: attached.generation,
            mode: mode as i32,
        }));
        self.attached_loop(&mut stream).await;
        Attempt::Ended
    }

    fn map_regions(&self, shared: &wire::SharedMemory) -> Result<Regions> {
        let mut regions = Regions::map(shared)?;
        regions.arena.register(&*self.registrar, self.context)?;
        regions.staging.register(&*self.registrar, self.context)?;
        Ok(regions)
    }

    fn make_dial(&mut self, link: &conn::Link, hello: &wire::HelloReply) -> Result<()> {
        let limits = conn::limits_or_default(hello.limits);
        let (host, tls) = match &self.address {
            PoolAddress::Tcp(address) | PoolAddress::TcpPlaintext(address) => {
                let host = address
                    .rsplit_once(':')
                    .map_or(address.as_str(), |(host, _)| host)
                    .to_string();
                let tls = if matches!(self.address, PoolAddress::Tcp(_)) {
                    Some(net::tls_trusting(
                        net::trusted(link.ca.as_deref(), &link.roots)?,
                        &host,
                    )?)
                } else {
                    None
                };
                (host, tls)
            }
            PoolAddress::Unix(_) => ("127.0.0.1".to_string(), None),
        };
        if self.staging.is_none() {
            let mine = self.layout.rank(self.rank);
            let slot_bytes = limits.max_window_bytes.max(mine.page_len).max(mine.row_len);
            self.staging = Some(Arc::new(Staging::new(
                &*self.registrar,
                self.context,
                STAGING_SLOTS,
                slot_bytes,
            )?));
        }
        self.dial = Some(Arc::new(Tcp {
            host,
            tls,
            staging: self.staging.clone().expect("made above"),
            events: self.net_events.clone(),
            rdma_offered: false,
            rdma_broken: Arc::default(),
        }));
        Ok(())
    }

    /// Serve the link, the channel tasks and the copier while the attachment lasts.
    async fn attached_loop(&mut self, stream: &mut tonic::Streaming<wire::RankServerMessage>) {
        loop {
            self.events();
            if self.stopping() || self.end.failed() {
                self.wind_down(true).await;
                return;
            }
            let tick = if self.busy() { BUSY_TICK } else { IDLE_TICK };
            tokio::select! {
                message = stream.message() => match message {
                    Ok(Some(wire::RankServerMessage { m: Some(rs::M::Closing(closing)) })) => {
                        eprintln!(
                            "warning: rank {}: the manager closed the attachment: {}",
                            self.rank, closing.detail
                        );
                        self.wind_down(true).await;
                        return;
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => return,
                },
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(tick) => {}
            }
        }
    }

    fn busy(&self) -> bool {
        !self.jobs.is_empty() || !self.pending.is_empty() || !self.xfers.is_empty()
    }

    /// One non-blocking pass: the link, the channel tasks, the copier.
    fn events(&mut self) {
        let mut incoming = Vec::new();
        self.end.poll(&mut incoming);
        for item in incoming {
            match item {
                Incoming::Closed(why) => {
                    eprintln!("warning: rank {}: the rank link failed ({why})", self.rank);
                    self.closing = true;
                    self.cancel_all();
                }
                Incoming::Message(message) => self.on_link(message),
            }
        }
        for event in self.net_events.take() {
            self.on_net(event);
        }
        self.poll_copier();
        self.submit_pending();
        let ids: Vec<u64> = self.xfers.keys().copied().collect();
        for xfer in ids {
            self.drive(xfer);
        }
    }

    // ---- link ---------------------------------------------------------------------

    fn on_link(&mut self, message: Msg) {
        match message {
            Msg::Open(open) => self.on_open(open),
            Msg::Window(window) => self.on_window(window),
            Msg::Cancel(cancel) => self.cancel(cancel.xfer),
            Msg::Close(_) => {
                self.closing = true;
                self.cancel_all();
            }
            _ => {}
        }
    }

    fn reject(&self, xfer: u64, why: &str) {
        self.end.send(Msg::Done(link::Done {
            xfer,
            ok: false,
            error: why.into(),
            quarantined: false,
        }));
    }

    fn on_open(&mut self, open: link::Open) {
        let xfer = open.xfer;
        let bound = self.att.as_ref().is_some_and(|att| {
            att.incarnation == open.manager_incarnation && att.generation == open.generation
        });
        if !bound {
            return self.reject(xfer, "the transfer is bound to another attachment");
        }
        if xfer <= self.high || self.xfers.contains_key(&xfer) {
            return self.reject(xfer, "the transfer id was already used");
        }
        if self.closing || self.quarantined {
            return self.reject(xfer, "the agent is closing");
        }
        self.high = xfer;
        let (shared, out) = (open.channel.is_none(), open.dir == link::Dir::Out as i32);
        let att = self.att.as_ref().expect("bound");
        if shared != (att.mode == wire::RankMode::RankShared) {
            return self.reject(xfer, "the transfer's mode is not this attachment's");
        }
        let mut expected = Vec::new();
        for record in &open.records {
            let Some(key) = record.key.as_ref().and_then(codec::key_from) else {
                return self.reject(xfer, "a record names no key");
            };
            expected.push(ExpectedRecord {
                key,
                first_page: record.first_page,
                pages: record.pages,
                row: record.row,
            });
        }
        let total: u64 = expected.iter().map(|r| u64::from(r.pages)).sum();
        if expected.is_empty() || total != open.pages.len() as u64 {
            return self.reject(xfer, "the transfer's pages do not match its records");
        }
        if open.page_bytes != self.layout.page_record_bytes()
            || open.row_bytes != self.layout.row_record_bytes()
        {
            return self.reject(xfer, "the transfer's record sizes are not this instance's");
        }
        let mut run = Xfer {
            out,
            kind: open.kind,
            fence_id: open.fence_id,
            coverage: Coverage::new(expected),
            pages: open.pages.iter().map(|id| PageId(*id)).collect(),
            slot: open.has_slot.then_some(StateSlotId(open.slot)),
            shared,
            regions: att.regions.clone(),
            channel: None,
            channel_open: false,
            ended: false,
            windows: BTreeMap::new(),
            jobs_outstanding: 0,
            failed: None,
            cancelled: false,
            page_bytes: open.page_bytes,
            row_bytes: open.row_bytes,
        };
        if let Some(reference) = &open.channel {
            let id = if out {
                Id::Export(xfer)
            } else {
                Id::Claim(xfer)
            };
            match self.open_channel(id, reference) {
                Ok(channel) => {
                    run.channel = Some(channel);
                    run.channel_open = true;
                }
                Err(error) => return self.reject(xfer, &error.to_string()),
            }
        }
        self.xfers.insert(xfer, run);
    }

    fn open_channel(&self, id: Id, reference: &wire::DataChannelRef) -> Result<Channel> {
        let dial = self
            .dial
            .as_ref()
            .ok_or_else(|| Error::new("this agent has no data channel"))?;
        dial.open(net::Open {
            id,
            reference: reference.clone(),
        })
        .map_err(|e| Error::caused_by("cannot open the data channel", e))
    }

    /// A shared window of a transfer.
    fn on_window(&mut self, window: link::Window) {
        let xfer = window.xfer;
        let Some(spec) = window.spec else { return };
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return;
        };
        if !run.shared || run.cancelled || run.failed.is_some() {
            return;
        }
        let dest = match run.coverage.accept(&spec, run.page_bytes, run.row_bytes) {
            Ok(dest) => dest as usize,
            Err(why) => return self.fail(xfer, why),
        };
        let Some(region) = run
            .regions
            .as_ref()
            .and_then(|regions| regions.get(spec.region()))
        else {
            return self.fail(xfer, "window in an unknown region".into());
        };
        let Some(base) = region.address(spec.offset, spec.length) else {
            return self.fail(xfer, "window outside its region".into());
        };
        let (count, row) = (spec.page_count as usize, spec.row);
        let rank = self.rank;
        if !run.out {
            // Verify this rank's slices against the transfer checksums before copying.
            let Some(sums) = window.sums else {
                return self.fail(xfer, "a window arrived without checksums".into());
            };
            let bytes = region
                .bytes(spec.offset, spec.length)
                .expect("checked above");
            if sums.pages.len() != count || sums.has_row != row {
                return self.fail(xfer, "window checksums do not match its pages".into());
            }
            let mine = self.layout.rank(rank);
            let bad = (0..count).any(|i| {
                let at = self.layout.page_at(rank, i as u32) as usize;
                checksum(&bytes[at..at + mine.page_len as usize]) != sums.pages[i]
            }) || (row && {
                let at = self.layout.row_at(rank, count as u32) as usize;
                checksum(&bytes[at..at + mine.row_len as usize]) != sums.row
            });
            if bad {
                return self.fail(xfer, "a page or row failed its checksum".into());
            }
        }
        let layout = self.layout.clone();
        let pages = run.pages.clone();
        let slot = run.slot;
        let direction = if run.out {
            Direction::Out
        } else {
            Direction::In
        };
        run.windows.insert(
            spec.index,
            Win {
                jobs_left: 0,
                spec,
                slot: None,
            },
        );
        let jobs = jobs_of(
            direction,
            spec.index,
            count,
            row,
            |i| pages[dest + i],
            slot,
            |i| base + layout.page_at(rank, i as u32) as usize,
            base + layout.row_at(rank, count as u32) as usize,
        );
        self.queue(xfer, spec.index, jobs);
    }

    fn queue(&mut self, xfer: u64, window: u32, jobs: Vec<Job>) {
        if let Some(run) = self.xfers.get_mut(&xfer) {
            run.jobs_outstanding += jobs.len() as u32;
            if let Some(win) = run.windows.get_mut(&window) {
                win.jobs_left = jobs.len() as u32;
            }
        }
        for mut job in jobs {
            self.next_job += 1;
            job.id = JobId(self.next_job);
            self.pending.push_back((job, (xfer, window)));
        }
    }

    // ---- channel ------------------------------------------------------------------

    fn on_net(&mut self, event: Event) {
        match event {
            Event::Window {
                claim,
                window,
                slot,
            } => self.on_net_window(claim, window, slot),
            Event::End { claim } => {
                if let Some(run) = self.xfers.get_mut(&claim) {
                    run.ended = true;
                    if !run.coverage.complete() && run.failed.is_none() && !run.cancelled {
                        self.fail(
                            claim,
                            "the data channel ended before every window arrived".into(),
                        );
                    }
                }
            }
            Event::Granted { id, shape, slot } => match id {
                Id::Export(xfer) => self.on_net_grant(xfer, shape, slot),
                _ => self.release_slot(slot),
            },
            Event::Sent { id, index, slot } => {
                self.release_slot(slot);
                if let Id::Export(xfer) = id
                    && let Some(run) = self.xfers.get_mut(&xfer)
                {
                    run.windows.remove(&index);
                }
            }
            Event::Failed { id, error } => {
                let (Id::Claim(xfer) | Id::Export(xfer) | Id::Put(xfer)) = id;
                if self.xfers.contains_key(&xfer) {
                    self.fail(xfer, error);
                }
            }
            Event::Closed { id } => {
                let (Id::Claim(xfer) | Id::Export(xfer) | Id::Put(xfer)) = id;
                if let Some(run) = self.xfers.get_mut(&xfer) {
                    run.channel = None;
                    run.channel_open = false;
                    let finished = run.coverage.complete()
                        && (!run.out || run.windows.is_empty())
                        && (run.ended || run.out);
                    if !finished && run.failed.is_none() && !run.cancelled {
                        self.fail(
                            xfer,
                            "the data channel closed before the transfer was complete".into(),
                        );
                    }
                }
            }
        }
    }

    fn release_slot(&self, slot: u32) {
        if let Some(staging) = &self.staging {
            staging.release(slot);
        }
    }

    /// A window of an incoming claim arrived whole in a slot of this agent's staging.
    fn on_net_window(&mut self, xfer: u64, window: super::frame::Window, slot: u32) {
        let Some(staging) = self.staging.clone() else {
            return;
        };
        let rank = self.rank;
        let mine = *self.layout.rank(rank);
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return self.release_slot(slot);
        };
        if run.shared || run.out || run.cancelled || run.failed.is_some() {
            return self.release_slot(slot);
        }
        let shape = window.shape;
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
        let dest = match run.coverage.accept(&spec, mine.page_len, mine.row_len) {
            Ok(dest) => dest as usize,
            Err(why) => {
                self.release_slot(slot);
                return self.fail(xfer, why);
            }
        };
        let (count, row) = (spec.page_count as usize, spec.row);
        let bytes = staging
            .region()
            .bytes(spec.offset, spec.length)
            .expect("a slot of the staging");
        let page_len = mine.page_len as usize;
        let bad = window.page_xxh3.len() != count
            || (0..count)
                .any(|i| checksum(&bytes[i * page_len..(i + 1) * page_len]) != window.page_xxh3[i])
            || (row && checksum(&bytes[count * page_len..]) != window.row_xxh3);
        if bad {
            self.release_slot(slot);
            return self.fail(xfer, "a page or row failed its checksum".into());
        }
        let base = staging
            .region()
            .address(spec.offset, spec.length)
            .expect("a slot of the staging");
        let pages = run.pages.clone();
        let rowslot = run.slot;
        run.windows.insert(
            spec.index,
            Win {
                jobs_left: 0,
                spec,
                slot: Some(slot),
            },
        );
        let jobs = jobs_of(
            Direction::In,
            spec.index,
            count,
            row,
            |i| pages[dest + i],
            rowslot,
            |i| base + i * page_len,
            base + count * page_len,
        );
        self.queue(xfer, spec.index, jobs);
    }

    /// The manager granted a window of an outgoing transfer; its slice is copied into a slot.
    fn on_net_grant(&mut self, xfer: u64, shape: super::frame::Shape, slot: u32) {
        let Some(staging) = self.staging.clone() else {
            return;
        };
        let mine = *self.layout.rank(self.rank);
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return self.release_slot(slot);
        };
        if run.shared || !run.out || run.cancelled || run.failed.is_some() {
            return self.release_slot(slot);
        }
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
        let dest = match run.coverage.accept(&spec, mine.page_len, mine.row_len) {
            Ok(dest) => dest as usize,
            Err(why) => {
                self.release_slot(slot);
                return self.fail(xfer, why);
            }
        };
        let (count, row) = (spec.page_count as usize, spec.row);
        let base = staging
            .region()
            .address(spec.offset, spec.length)
            .expect("a slot of the staging");
        let page_len = mine.page_len as usize;
        let pages = run.pages.clone();
        let rowslot = run.slot;
        run.windows.insert(
            spec.index,
            Win {
                jobs_left: 0,
                spec,
                slot: Some(slot),
            },
        );
        let jobs = jobs_of(
            Direction::Out,
            spec.index,
            count,
            row,
            |i| pages[dest + i],
            rowslot,
            |i| base + i * page_len,
            base + count * page_len,
        );
        self.queue(xfer, spec.index, jobs);
    }

    // ---- copier -------------------------------------------------------------------

    fn submit_pending(&mut self) {
        while let Some((job, owner)) = self.pending.pop_front() {
            let id = job.id;
            let result = self.copier.lock().unwrap().submit(job.clone());
            match result {
                Ok(()) => {
                    self.jobs.insert(id, owner);
                }
                Err(error) => {
                    if self.copier.lock().unwrap().in_flight() > 0 {
                        self.pending.push_front((job, owner));
                        return;
                    }
                    self.job_done(owner, Err(error.to_string()));
                }
            }
        }
    }

    fn poll_copier(&mut self) {
        let mut replies = Vec::new();
        self.copier.lock().unwrap().poll(&mut replies);
        for reply in replies {
            if let Some(owner) = self.jobs.remove(&reply.id) {
                self.job_done(owner, reply.result.map_err(|e| e.to_string()));
            }
        }
    }

    fn job_done(&mut self, (xfer, window): (u64, u32), result: std::result::Result<(), String>) {
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return;
        };
        run.jobs_outstanding = run.jobs_outstanding.saturating_sub(1);
        let Some(win) = run.windows.get_mut(&window) else {
            return;
        };
        win.jobs_left = win.jobs_left.saturating_sub(1);
        let finished = win.jobs_left == 0;
        if let Err(why) = result {
            return self.fail(xfer, why);
        }
        if !finished {
            return;
        }
        self.window_drained(xfer, window);
    }

    /// Every copy of one window drained.
    fn window_drained(&mut self, xfer: u64, index: u32) {
        let rank = self.rank;
        let mine = *self.layout.rank(rank);
        let Some(run) = self.xfers.get(&xfer) else {
            return;
        };
        let Some(win) = run.windows.get(&index) else {
            return;
        };
        let (spec, slot) = (win.spec, win.slot);
        let (count, row) = (spec.page_count as usize, spec.row);
        let over = run.failed.is_some() || run.cancelled;
        let (shared, out) = (run.shared, run.out);
        if over {
            // Nothing is reported for a window of a transfer that is already over; the
            // slot, if any, goes back.
            if let Some(slot) = slot {
                self.release_slot(slot);
            }
            if let Some(run) = self.xfers.get_mut(&xfer) {
                run.windows.remove(&index);
            }
            return;
        }
        match (shared, out) {
            (true, false) => {
                self.end.send(Msg::WindowDone(link::WindowDone {
                    xfer,
                    index,
                    ok: true,
                    error: String::new(),
                    sums: None,
                }));
                self.forget_window(xfer, index);
            }
            (true, true) => {
                // The slice this rank wrote into the window, hashed after the drain.
                let region = run
                    .regions
                    .as_ref()
                    .and_then(|regions| regions.get(spec.region()));
                let Some(bytes) = region.and_then(|r| r.bytes(spec.offset, spec.length)) else {
                    return self.fail(xfer, "window outside its region".into());
                };
                let sums = SliceSums {
                    pages: (0..count)
                        .map(|i| {
                            let at = self.layout.page_at(rank, i as u32) as usize;
                            checksum(&bytes[at..at + mine.page_len as usize])
                        })
                        .collect(),
                    row: row.then(|| {
                        let at = self.layout.row_at(rank, count as u32) as usize;
                        checksum(&bytes[at..at + mine.row_len as usize])
                    }),
                };
                self.end.send(Msg::WindowDone(link::WindowDone {
                    xfer,
                    index,
                    ok: true,
                    error: String::new(),
                    sums: Some(link::SliceSums {
                        pages: sums.pages,
                        row: sums.row.unwrap_or(0),
                        has_row: sums.row.is_some(),
                    }),
                }));
                self.forget_window(xfer, index);
            }
            (false, false) => {
                // A channel window leaves its slot once the copy drained.
                if let Some(slot) = slot {
                    self.release_slot(slot);
                }
                self.forget_window(xfer, index);
            }
            (false, true) => {
                // The filled slot goes to the manager; `Sent` returns it.
                let Some(slot) = slot else { return };
                let Some(staging) = self.staging.clone() else {
                    return;
                };
                let bytes = staging
                    .region()
                    .bytes(spec.offset, spec.length)
                    .expect("a slot of the staging");
                let page_len = mine.page_len as usize;
                let page_xxh3: Vec<u64> = (0..count)
                    .map(|i| checksum(&bytes[i * page_len..(i + 1) * page_len]))
                    .collect();
                let row_xxh3 = row.then(|| checksum(&bytes[count * page_len..]));
                let sent = run.channel.as_ref().is_some_and(|channel| {
                    channel.send(net::Filled {
                        index,
                        row_xxh3,
                        page_xxh3,
                        slot,
                        length: spec.length,
                    })
                });
                if !sent {
                    self.release_slot(slot);
                    self.fail(xfer, "the data channel is gone".into());
                }
            }
        }
    }

    fn forget_window(&mut self, xfer: u64, index: u32) {
        if let Some(run) = self.xfers.get_mut(&xfer) {
            run.windows.remove(&index);
        }
    }

    // ---- ending a transfer -----------------------------------------------------------

    fn fail(&mut self, xfer: u64, why: String) {
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return;
        };
        if run.failed.is_none() {
            eprintln!("warning: rank {}: transfer {xfer} failed: {why}", self.rank);
            run.failed = Some(why);
        }
        if let Some(channel) = &run.channel {
            channel.stop();
        }
    }

    fn cancel(&mut self, xfer: u64) {
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return;
        };
        run.cancelled = true;
        if let Some(channel) = &run.channel {
            channel.stop();
        }
    }

    fn cancel_all(&mut self) {
        let ids: Vec<u64> = self.xfers.keys().copied().collect();
        for xfer in ids {
            self.cancel(xfer);
        }
    }

    /// Send `Done` for a transfer when it is over and drained.
    fn drive(&mut self, xfer: u64) {
        let Some(run) = self.xfers.get_mut(&xfer) else {
            return;
        };
        // An outgoing channel transfer whose last window was sent ends its task.
        if run.out
            && !run.shared
            && run.coverage.complete()
            && run.windows.is_empty()
            && let Some(channel) = &run.channel
        {
            channel.stop();
        }
        if run.jobs_outstanding > 0 || run.channel_open {
            return;
        }
        let over = run.failed.is_some()
            || run.cancelled
            || (run.coverage.complete()
                && run.windows.is_empty()
                && (run.shared || run.out || run.ended));
        if !over {
            return;
        }
        // A cancelled or failed transfer waits for its slots and queued jobs as well.
        if self.pending.iter().any(|(_, (x, _))| *x == xfer) {
            return;
        }
        let run = self.xfers.remove(&xfer).expect("present");
        for win in run.windows.values() {
            if let Some(slot) = win.slot {
                self.release_slot(slot);
            }
        }
        let ok = run.failed.is_none() && !run.cancelled && run.coverage.complete();
        let error = match (&run.failed, run.cancelled) {
            (Some(why), _) => why.clone(),
            (None, true) => "cancelled".into(),
            (None, false) if !ok => "the transfer did not cover its records".into(),
            _ => String::new(),
        };
        self.end.send(Msg::Done(link::Done {
            xfer,
            ok,
            error,
            quarantined: false,
        }));
        self.attest(run.kind, run.fence_id, true);
        self.tombstones.push_back(xfer);
        while self.tombstones.len() > TOMBSTONES {
            self.tombstones.pop_front();
        }
    }

    /// Tell the manager this agent stopped touching a transfer's memory.
    fn attest(&mut self, kind: i32, id: u64, drained: bool) {
        self.owed.push((kind, id, drained));
        self.deliver_owed();
    }

    fn deliver_owed(&mut self) {
        let Some(att) = &self.att else { return };
        let mut kept = Vec::new();
        for (kind, id, drained) in std::mem::take(&mut self.owed) {
            let fence = wire::RankFence {
                kind,
                id,
                outcome: if drained {
                    wire::FenceOutcome::Drained
                } else {
                    wire::FenceOutcome::Quarantined
                } as i32,
            };
            if att
                .tx
                .try_send(wire::RankClientMessage {
                    m: Some(rc::M::Fence(fence)),
                })
                .is_err()
            {
                kept.push((kind, id, drained));
            }
        }
        self.owed = kept;
    }

    /// Wait for every transfer to end. `false` when stop was requested or the time ran
    /// out; in the latter case the agent cannot show its copies stopped.
    async fn drain(&mut self) -> bool {
        let started = Instant::now();
        loop {
            self.events();
            if self.xfers.is_empty() && self.jobs.is_empty() && self.pending.is_empty() {
                return true;
            }
            if started.elapsed() > CLOSE_TIMEOUT {
                self.quarantine();
                return false;
            }
            let _ = tokio::time::timeout(BUSY_TICK, self.wake.notified()).await;
        }
    }

    /// Drain cannot be shown: keep every mapping and registration for the life of the
    /// process and say so.
    fn quarantine(&mut self) {
        if self.quarantined {
            return;
        }
        self.quarantined = true;
        let ids: Vec<u64> = self.xfers.keys().copied().collect();
        for xfer in ids {
            if let Some(run) = self.xfers.remove(&xfer) {
                self.end.send(Msg::Done(link::Done {
                    xfer,
                    ok: false,
                    error: "the agent could not drain".into(),
                    quarantined: true,
                }));
                self.owed.push((run.kind, run.fence_id, false));
                std::mem::forget(run);
            }
        }
        self.deliver_owed();
        if let Some(att) = &self.att {
            std::mem::forget(att.regions.clone());
        }
        if let Some(staging) = self.staging.take() {
            std::mem::forget(staging);
        }
        self.set_status(PoolStatus::Disabled(
            "a copy could not be drained; memory is quarantined".into(),
        ));
    }
}

enum Attempt {
    Retry(String),
    Disabled(String),
    Ended,
}

/// Copier jobs for one window: pages and then the row, at most [`WINDOW_PAGES`] pages each.
#[allow(clippy::too_many_arguments)]
fn jobs_of(
    direction: Direction,
    window: u32,
    count: usize,
    row: bool,
    page_id: impl Fn(usize) -> PageId,
    slot: Option<StateSlotId>,
    page_address: impl Fn(usize) -> usize,
    row_address: usize,
) -> Vec<Job> {
    let mut jobs = Vec::new();
    let mut first = 0;
    while first < count || (count == 0 && jobs.is_empty()) {
        let end = (first + WINDOW_PAGES).min(count);
        let pages = (first..end)
            .map(|i| (page_id(i), page_address(i)))
            .collect();
        let last = end == count;
        let row =
            (last && row).then(|| (slot.expect("a window with a row has a slot"), row_address));
        jobs.push(Job {
            id: JobId(0),
            window,
            direction,
            pages,
            row,
        });
        first = end;
        if count == 0 {
            break;
        }
    }
    jobs
}
