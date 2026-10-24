//! The pool client the host store owns.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
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
    shared::{Command, PoolEvent, PoolFetch, PoolStatus, PutRequest, Shared},
    types::{HeldRange, Tier},
};

/// Where the manager listens.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolAddress {
    /// A Unix-domain socket on this host.
    Unix(PathBuf),
    /// `host:port` of a manager on the network.
    Tcp(String),
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
}

/// The in-process pool client. Built only when a pool address is configured.
pub struct PoolClient {
    shared: Arc<Shared>,
    announce: Announcer,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
    next_fetch: u64,
    next_put: u64,
}

impl PoolClient {
    /// Spawn the I/O thread and return at once, in [`PoolStatus::Connecting`].
    pub fn start(
        config: PoolConfig,
        copier: SharedCopier,
        registrar: Arc<dyn HostRegistrar + Send + Sync>,
    ) -> Result<PoolClient> {
        let geometry = &config.scope.geometry;
        {
            let layout = copier.lock().unwrap().layout().clone();
            if layout.page_record_bytes != geometry.page_record_bytes
                || layout.row_record_bytes != geometry.row_record_bytes
            {
                return Err(Error::new(
                    "the pool scope does not match the copier's layout",
                ));
            }
        }
        if config.tenant.is_empty() || config.tenant != config.scope.tenant {
            return Err(Error::new(
                "the pool tenant must be set and equal the scope's",
            ));
        }
        if config.ca.is_some() && matches!(config.address, PoolAddress::Unix(_)) {
            return Err(Error::new(
                "a CA certificate applies to a network address, not a socket path",
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
        let settings = Arc::new(Settings {
            instance_id,
            nonce: identity::process_nonce()?,
            name: config.instance_name,
            scope: config.scope,
            scope_id,
            export_slots: config.export_slots,
            claim_slots: config.claim_slots,
            context: config.registration_context,
        });
        let engine = Engine::new(
            settings,
            shared.clone(),
            announce.clone(),
            copier,
            registrar,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let link = conn::Link {
            address: config.address,
            ca: config.ca,
            wake,
            stop: stop.clone(),
        };
        let thread = std::thread::Builder::new()
            .name("selene-pool".into())
            .spawn(move || conn::run(engine, link))
            .map_err(|e| Error::caused_by("cannot start the pool thread", e))?;
        Ok(PoolClient {
            shared,
            announce,
            stop,
            thread: Some(thread),
            next_fetch: 0,
            next_put: 0,
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
