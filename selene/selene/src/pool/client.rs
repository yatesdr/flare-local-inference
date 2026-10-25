//! The pool client the host store owns.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use tokio::sync::Notify;

use crate::{
    Error, ExportChain, ExportId, ExportRefusal, History, HostRegistrar, Result, SharedCopier,
    characterize::{Level, Pages, Ranking},
};

use super::{
    announce::{Announcer, IndexEntry},
    conn,
    engine::{Engine, Settings},
    exporter::HostPin,
    identity::{self, Scope},
    lookup::{LookupState, LookupTicket},
    rank::RankSet,
    shared::{Command, PoolEvent, PoolFetch, PoolStatus, PutRequest, Shared},
    startup::{Approve, RegisteredRegion, Startup},
    types::{HeldRange, Tier},
};

/// How long [`PoolClient::start`] waits for the manager at startup. A manager that has not
/// answered by then is not used by this process at all: it runs on its local tiers, with one
/// warning, and never attaches later (see [`PoolClient::start`]).
pub const STARTUP_WAIT: Duration = Duration::from_secs(10);

/// Where the manager listens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolAddress {
    /// A Unix-domain socket on this host.
    Unix(PathBuf),
    /// `host:port` of a manager on the network, reached over TLS. The manager's certificate
    /// is checked against the CA file when one is configured and the host's trust store
    /// otherwise.
    Tcp(String),
    /// `host:port` of a manager on the network, reached without encryption.
    TcpPlaintext(String),
}

/// Built from the flags. `start` never blocks on the network.
pub struct PoolConfig {
    pub address: PoolAddress,
    pub tenant: String,
    /// A CA certificate file. When set the connection must be TLS.
    pub ca: Option<PathBuf>,
    /// Model identity, layout fingerprint and record geometry.
    pub scope: Scope,
    pub instance_name: String,
    /// The instance's GPUs; the instance id is derived from them and is stable
    /// across restarts.
    pub gpu_uuids: Vec<[u8; 16]>,
    /// Exports served at once (default 2).
    pub export_slots: u32,
    /// Claims in flight at once (default 2).
    pub claim_slots: u32,
    /// An operator-pinned order of the four copy levels, closest first.
    pub level_order: Option<[Level; 4]>,
    /// The device context made current when registering the shared regions.
    pub registration_context: usize,
    /// Called once at startup with everything the client is about to register (see
    /// [`PlannedRegion`]), after the manager's first Hello and before the first registration.
    /// `Err(reason)` registers nothing: the pool is off for this process, with one warning naming
    /// the sizes and the reason. `None` approves.
    pub approve: Option<Approve>,
}

/// The in-process pool client. Built only when a pool address is configured.
pub struct PoolClient {
    shared: Arc<Shared>,
    announce: Announcer,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
    next_fetch: u64,
    next_put: u64,
    registered: Vec<RegisteredRegion>,
}

impl PoolClient {
    /// Attach to the manager, once, at startup.
    ///
    /// Registering host memory with the GPU consumes device memory, so a serving instance does
    /// it only before it admits requests. This call is that time. It spawns the I/O thread and
    /// waits up to [`STARTUP_WAIT`] for the manager's first Hello; on that Hello the thread maps
    /// the manager's shared regions and registers each through `registrar` (for a manager on
    /// another host, this process's own data-channel staging), and `start` returns with the
    /// ranges in [`PoolClient::registered_regions`]. From then on the client never calls the
    /// registrar again: a reconnect after a manager restart continues with the same
    /// registrations when the manager reports the same regions, and a manager that reports
    /// different ones (it recreated them, so running instances must restart) turns the pool off
    /// for this process, with one warning. A manager not reached in time, or a pool refused at
    /// startup (version, encryption, scope or an untrusted certificate), leaves the process on
    /// its local tiers, with one warning, for good: the client does not attach later. Start the
    /// manager before the instance. The returned client is in [`PoolStatus::Connecting`] while
    /// its session opens, or [`PoolStatus::Disabled`] when it will not attach.
    pub fn start(
        config: PoolConfig,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
    ) -> Result<PoolClient> {
        Self::start_trusting(config, copier, registrar, super::net::Roots::System)
    }

    /// Rank 0 of a multi-process instance: the tensor-parallel ranks are separate
    /// processes, `ranks.links[i]` reaches rank `i + 1`, whose
    /// [`RankAgent`](super::agent::RankAgent) moves that rank's slices. `copier` serves
    /// this rank only (its layout is the scope's rank-0 slice); `config.scope` and
    /// `config.gpu_uuids` describe the whole instance.
    pub fn start_ranked(
        config: PoolConfig,
        ranks: RankSet,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
    ) -> Result<PoolClient> {
        Self::start_waiting(
            config,
            Some(ranks),
            copier,
            registrar,
            super::net::Roots::System,
            STARTUP_WAIT,
        )
    }

    /// The ranges registered at startup, in the order they were registered. Empty when the
    /// client did not attach.
    pub fn registered_regions(&self) -> &[RegisteredRegion] {
        &self.registered
    }

    /// [`PoolClient::start`] with the trust store named, so a test can give one of its own.
    pub(crate) fn start_trusting(
        config: PoolConfig,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
        roots: super::net::Roots,
    ) -> Result<PoolClient> {
        Self::start_waiting(config, None, copier, registrar, roots, STARTUP_WAIT)
    }

    /// [`PoolClient::start_trusting`] with the startup wait named, so a test need not wait ten
    /// seconds for a manager that is not there, and with the ranks of a multi-process instance.
    pub(crate) fn start_waiting(
        config: PoolConfig,
        ranks: Option<RankSet>,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
        roots: super::net::Roots,
        wait: Duration,
    ) -> Result<PoolClient> {
        let geometry = &config.scope.geometry;
        let slices = Arc::new(super::slice::SliceLayout::new(geometry)?);
        {
            let layout = copier.lock().unwrap().layout().clone();
            // A rank of a multi-process instance copies only its own slice.
            let (page, row) = match &ranks {
                Some(_) => (slices.rank(0).page_len, slices.rank(0).row_len),
                None => (geometry.page_record_bytes, geometry.row_record_bytes),
            };
            if layout.page_record_bytes != page || layout.row_record_bytes != row {
                return Err(Error::new(
                    "the pool scope does not match the copier's layout",
                ));
            }
        }
        if let Some(set) = &ranks
            && set.links.len() + 1 != slices.world()
        {
            return Err(Error::new(
                "the number of rank links does not match the scope's rank count",
            ));
        }
        if config.tenant.is_empty() || config.tenant != config.scope.tenant {
            return Err(Error::new(
                "the pool tenant must be set and equal the scope's",
            ));
        }
        if config.ca.is_some() && !matches!(config.address, PoolAddress::Tcp(_)) {
            return Err(Error::new(
                "a CA certificate applies to an encrypted network address",
            ));
        }
        let scope_id = identity::scope_id(
            &config.scope.tenant,
            &config.scope.model_identity,
            &config.scope.layout_fingerprint,
            &config.scope.geometry,
        )?;
        let instance_id = identity::instance_id(
            &config.scope.tenant,
            &config.scope.model_identity,
            &config.scope.layout_fingerprint,
            &config.gpu_uuids,
        );
        let wake = Arc::new(Notify::new());
        let shared = Shared::new(wake.clone());
        shared.set_status(PoolStatus::Connecting);
        if let Some(order) = config.level_order {
            let mut levels = vec![Level::ThisGpu];
            levels.extend(order);
            let pages = Pages {
                small: 1,
                typical: 1,
                long: 1,
            };
            shared.set_ranking(
                crate::characterize::Characterizer::new(pages, Some(levels))?
                    .ranking()
                    .clone(),
            );
        }
        let announce = Announcer::new(wake.clone());
        let gpu_uuids = config.gpu_uuids.clone();
        let rank_secret = match &ranks {
            Some(_) => Some(identity::secret()?),
            None => None,
        };
        let fingerprint = config.scope.layout_fingerprint;
        let settings = Arc::new(Settings {
            instance_id,
            nonce: identity::process_nonce()?,
            name: config.instance_name,
            scope: config.scope,
            scope_id,
            export_slots: config.export_slots,
            claim_slots: config.claim_slots,
            context: config.registration_context,
            rank_secret,
        });
        let mut engine = Engine::new(
            settings.clone(),
            shared.clone(),
            announce.clone(),
            copier,
            registrar,
        );
        if let (Some(set), Some(secret)) = (ranks, rank_secret) {
            engine.set_ranks(super::engine::Ranks::new(
                set,
                slices,
                super::engine::ranks::Identity {
                    secret,
                    nonce: settings.nonce,
                    instance_id: settings.instance_id,
                    fingerprint,
                    gpus: gpu_uuids,
                },
                wake.clone(),
            )?);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let startup = Arc::new(Startup::new());
        let link = conn::Link {
            address: config.address,
            ca: config.ca,
            roots,
            wake,
            stop: stop.clone(),
            startup: startup.clone(),
            approve: config.approve,
        };
        let thread = std::thread::Builder::new()
            .name("selene-pool".into())
            .spawn(move || conn::run(engine, link))
            .map_err(|e| Error::caused_by("cannot start the pool thread", e))?;
        let registered = match startup.wait(wait) {
            Ok(registered) => registered,
            Err(timed_out) => {
                if timed_out {
                    let reason = format!(
                        "the pool manager did not answer within {wait:?} of startup; this instance \
                         uses its local tiers only and will not attach to the pool later \
                         (start the manager before the instance)",
                    );
                    eprintln!("warning: {reason}");
                    shared.set_status(PoolStatus::Disabled(reason));
                }
                Vec::new()
            }
        };
        Ok(PoolClient {
            shared,
            announce,
            stop,
            thread: Some(thread),
            next_fetch: 0,
            next_put: 0,
            registered,
        })
    }

    pub fn status(&self) -> PoolStatus {
        self.shared.status()
    }

    /// A handle for the host tiers to announce holdings from their own threads.
    pub fn announcer(&self) -> Announcer {
        self.announce.clone()
    }

    pub fn saved(&self, entry: IndexEntry) {
        self.announce.saved(entry);
    }

    /// Eviction notices were lost: forget the GPU holdings and resync.
    pub fn evictions_lost(&self) {
        self.announce.forget_gpu();
    }

    pub fn spilled(&self, to: IndexEntry, from: (Tier, u64)) {
        self.announce.spilled(to, from);
    }

    pub fn evicted(
        &self,
        key: crate::PrefixKey,
        tier: Tier,
        generation: u64,
        heir: Option<IndexEntry>,
    ) {
        self.announce.evicted(key, tier, generation, heir);
    }

    /// The order in use for restore sources.
    pub fn ranking(&self) -> Ranking {
        self.shared.ranking()
    }

    /// Identifies the manager and link of the current session (zero before any
    /// session opened); a change means the copy levels may cost something else.
    pub fn topology(&self) -> crate::characterize::Topology {
        crate::characterize::Topology(self.shared.topology())
    }

    pub fn set_ranking(&self, ranking: Ranking) {
        self.shared.set_ranking(ranking);
    }

    pub fn observe_restore(&self, level: Level, pages: usize, elapsed: std::time::Duration) {
        self.shared.observe_restore(level, pages, elapsed);
    }

    /// Start a lookup for a queued request. Identical prefixes share one wire
    /// lookup. The ticket has no answer when the pool is not ready.
    pub fn begin_lookup(&self, history: &dyn History, held: Vec<HeldRange>) -> LookupTicket {
        let mut boundaries: Vec<_> = history.key_at(0).into_iter().collect();
        boundaries.extend_from_slice(history.aligned_boundaries());
        self.shared.begin_lookup(
            std::time::Instant::now(),
            boundaries,
            history.token_count(),
            held,
        )
    }

    /// Never waits for a reply.
    pub fn lookup_state(&self, ticket: &LookupTicket) -> LookupState {
        ticket.state()
    }

    /// Begin a restore from the pool. An error means nothing was accepted and
    /// no event follows; otherwise exactly one [`PoolEvent::Fetched`] with the
    /// returned id follows, after every destination write has drained.
    pub fn fetch(&mut self, mut fetch: PoolFetch) -> Result<u64> {
        crate::pool::shared::require_ready(&self.shared.status())?;
        self.next_fetch += 1;
        fetch.id = self.next_fetch;
        let id = fetch.id;
        if !self.shared.push_command(Command::Fetch(fetch), false) {
            return Err(Error::new("the pool's command queue is full"));
        }
        Ok(id)
    }

    /// Append events for the worker. Never waits. `Err` means the pool can no
    /// longer account for its transfers: copies may still be running and the
    /// worker must stop rather than free the pages and pins they hold.
    pub fn poll(&mut self, events: &mut Vec<PoolEvent>) -> Result<()> {
        self.shared.drain_events(events);
        if self.shared.quarantined() {
            return Err(Error::new("the pool could not drain its transfers"));
        }
        Ok(())
    }

    /// The worker pinned the checkpoint for `export`.
    pub fn export_pinned(&mut self, export: ExportId, chain: ExportChain) {
        self.shared
            .push_command(Command::ExportPinned { export, chain }, true);
    }

    /// Write a record into the manager's store. An error means nothing was
    /// accepted and no event follows; otherwise exactly one [`PoolEvent::Put`]
    /// with the returned id follows, after every copy that read the source
    /// drained. The source must stay valid (GPU pages pinned) until then, and
    /// the caller's own copy may be dropped only on `Stored` or `Present`.
    pub fn put(&mut self, request: PutRequest) -> Result<u64> {
        crate::pool::shared::require_ready(&self.shared.status())?;
        self.next_put += 1;
        let id = self.next_put;
        if !self
            .shared
            .push_command(Command::Put { id, request }, false)
        {
            return Err(Error::new("the pool's command queue is full"));
        }
        Ok(id)
    }

    /// Withdraw a put that has not been committed.
    pub fn cancel_put(&mut self, id: u64) {
        self.shared.push_command(Command::PutCancel(id), true);
    }

    /// A host tier pinned a record chain for `export`.
    pub fn host_pinned(&mut self, export: ExportId, pin: Box<dyn HostPin>) {
        self.shared
            .push_command(Command::HostPinned { export, pin }, true);
    }

    /// The worker did not pin the checkpoint for `export`.
    pub fn export_refused(&mut self, export: ExportId, reason: ExportRefusal) {
        self.shared
            .push_command(Command::ExportRefused { export, reason }, true);
    }

    /// Stop submitting, drain, release pins and staging, and say Goodbye
    /// `quiesced` only if everything drained. Export pins are released through
    /// [`PoolEvent::ExportEnded`] events the caller polls afterwards. On
    /// failure the caller must keep what the pool may still be reading.
    pub fn close(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        self.shared.wake.notify_one();
        match self.thread.take() {
            Some(thread) => thread
                .join()
                .unwrap_or_else(|_| Err(Error::new("the pool thread panicked"))),
            None => Ok(()),
        }
    }
}

impl Drop for PoolClient {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
