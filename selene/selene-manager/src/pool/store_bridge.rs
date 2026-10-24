//! The manager's own store as a participant in the pool protocol.
//!
//! The store keeps its own sessions, windows and records. The pool talks to it
//! the way a local client does: one store connection per instance process (so a
//! stream that drops while the process lives keeps its windows leased), one
//! catalog subscription per scope that feeds the manager's entries into the
//! index, and the store's put and get operations behind PutBegin and claims.

use super::{
    Checked,
    claims::{ClaimKey, Party},
    core::{Core, Event, Nonce, ProcKey, ScopeId},
    index::{Entry, InstanceId, SourceId, Tier},
    key_from, key_to,
};
use crate::{
    Key,
    proto::{self, v2, v2::server_message::M},
    record::{Descriptor, HEADER_BYTES, Header, NamespaceId},
    store::{GetAnswer, PutAnswer, Reply, StoreCommand},
};
use std::io;
use tokio::sync::{mpsc, oneshot};

/// A process's connection to the store.
#[derive(Clone, Copy, Debug)]
pub(super) struct Link {
    pub connection: u64,
    pub generation: u64,
    pub namespace: NamespaceId,
}

pub(super) struct StoreHandle {
    pub commands: mpsc::Sender<StoreCommand>,
    pub next_connection: u64,
}

pub(super) type PutKey = (InstanceId, Nonce, u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PutPhase {
    Open,
    Committed,
    Finished,
}

pub(super) struct PutRec {
    pub phase: PutPhase,
    pub link: Link,
    /// Windows granted and not yet filled: the writer may still be writing them.
    pub unfilled: u32,
    pub fence: Party,
    pub refused: bool,
    pub key: Key,
    pub chain_pages: u64,
    /// What the store accepted: the parent it will link to and the first page it needs.
    pub accepted: Option<(Option<Key>, u32)>,
}

/// Per-scope catalog feed from the store.
#[derive(Default)]
pub(super) struct CatalogFeed {
    pub snapshot: Option<Vec<Entry>>,
}

impl Core {
    /// Asks the store and waits for its answer. The store thread never waits on the
    /// pool, so this returns promptly; the multi-thread runtime keeps serving others.
    fn store_call<T: Send>(
        &self,
        make: impl FnOnce(Reply<io::Result<T>>) -> StoreCommand,
    ) -> io::Result<T> {
        let Some(store) = &self.store else {
            return Err(io::Error::other("the store is not available"));
        };
        let (reply, answer) = oneshot::channel();
        let command = make(reply);
        tokio::task::block_in_place(|| {
            store
                .commands
                .blocking_send(command)
                .map_err(|_| io::Error::other("the store stopped"))?;
            answer
                .blocking_recv()
                .map_err(|_| io::Error::other("the store dropped the request"))?
        })
    }

    fn store_send(&self, command: StoreCommand) {
        if let Some(store) = &self.store {
            // A full queue means the store is busy; the pool retries nothing here because
            // every use is a release the store also performs when the connection ends.
            let _ = store.commands.try_send(command);
        }
    }

    fn new_store_connection(&mut self) -> Option<u64> {
        let store = self.store.as_mut()?;
        store.next_connection += 1;
        Some(store.next_connection)
    }

    // ------------------------------------------------------------------ catalog

    /// Subscribes to the store's catalog for a scope so its records appear in the
    /// index as the manager's entries.
    pub(super) fn watch_catalog(&mut self, scope: ScopeId) {
        if self.store.is_none() || self.catalogs.contains_key(&scope) {
            return;
        }
        let Some(descriptor) = self.scopes.get(&scope).map(|s| s.valid.descriptor.clone()) else {
            return;
        };
        let Some(connection) = self.new_store_connection() else {
            return;
        };
        let namespace = descriptor.namespace_id();
        let hello = self.store_call(|reply| StoreCommand::Hello {
            connection,
            replica: "pool".into(),
            pid: std::process::id(),
            descriptor: descriptor.clone(),
            reply,
        });
        let hello = match hello {
            Ok(hello) => hello,
            Err(error) => {
                eprintln!("selene: warning: the store would not open a catalog link: {error}");
                return;
            }
        };
        let (events, mut feed) = mpsc::channel::<proto::CatalogEvent>(65_536);
        let subscribed = self.store_call(|reply| StoreCommand::Subscribe {
            connection,
            namespace,
            generation: hello.generation,
            descriptor,
            events,
            reply,
        });
        if let Err(error) = subscribed {
            eprintln!("selene: warning: the store would not stream its catalog: {error}");
            return;
        }
        self.catalogs.insert(scope, CatalogFeed::default());
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            while let Some(event) = feed.recv().await {
                if tx.send(Event::Catalog { scope, event }).await.is_err() {
                    return;
                }
            }
        });
    }

    pub(super) fn catalog_event(&mut self, scope: ScopeId, event: proto::CatalogEvent) {
        use proto::catalog_event::Event as E;
        let sequence = event.sequence;
        let Some(feed) = self.catalogs.get_mut(&scope) else {
            return;
        };
        let to_entry = |entry: &proto::CatalogEntry| -> Option<Entry> {
            if entry.unreadable {
                return None;
            }
            let key = key_of(entry.key.as_ref()?)?;
            let parent = if entry.has_parent {
                Some(key_of(entry.parent.as_ref()?)?)
            } else {
                None
            };
            let tier = match proto::Tier::try_from(entry.tier).ok()? {
                proto::Tier::Dram => Tier::Dram,
                proto::Tier::Nvme => Tier::Nvme,
                _ => return None,
            };
            Some(Entry {
                key,
                parent,
                bucket: entry.bucket.as_slice().try_into().ok()?,
                tail_pages: u32::try_from(entry.tail_pages).ok()?,
                row: entry.row,
                tier,
                generation: sequence,
                bytes: entry.bytes,
            })
        };
        let Some(state) = self.scopes.get_mut(&scope) else {
            return;
        };
        let manager = SourceId::Manager;
        match event.event {
            Some(E::SnapshotBegin(_)) => feed.snapshot = Some(Vec::new()),
            Some(E::Entry(entry)) => {
                if let (Some(snapshot), Some(entry)) = (feed.snapshot.as_mut(), to_entry(&entry)) {
                    snapshot.push(entry);
                }
            }
            Some(E::SnapshotEnd(_)) => {
                if let Some(snapshot) = feed.snapshot.take() {
                    state.index.replace(manager, snapshot);
                }
            }
            Some(E::Added(entry)) | Some(E::Changed(entry)) => {
                if let Some(entry) = to_entry(&entry) {
                    let other = match entry.tier {
                        Tier::Dram => Tier::Nvme,
                        _ => Tier::Dram,
                    };
                    state.index.remove(manager, entry.key, other, u64::MAX);
                    state.index.upsert(manager, entry);
                }
            }
            Some(E::Removed(key)) => {
                if let Some(key) = key_of(&key) {
                    state.index.remove(manager, key, Tier::Dram, u64::MAX);
                    state.index.remove(manager, key, Tier::Nvme, u64::MAX);
                }
            }
            Some(E::Room(_)) | None => {}
        }
    }

    // ------------------------------------------------------------------ links

    /// The process's store connection, opened on first use.
    pub(super) fn link_for(&mut self, process: ProcKey, scope: ScopeId, pid: u32) -> Option<Link> {
        if let Some(link) = self.procs.get(&process).and_then(|p| p.link) {
            return Some(link);
        }
        let descriptor: Descriptor = self.scopes.get(&scope)?.valid.descriptor.clone();
        if pid == 0 {
            return None;
        }
        let connection = self.new_store_connection()?;
        let namespace = descriptor.namespace_id();
        let replica = super::barrier::hex(&process.0);
        let hello = self
            .store_call(|reply| StoreCommand::Hello {
                connection,
                replica,
                pid,
                descriptor,
                reply,
            })
            .ok()?;
        let link = Link {
            connection,
            generation: hello.generation,
            namespace,
        };
        if let Some(proc) = self.procs.get_mut(&process) {
            proc.link = Some(link);
        }
        Some(link)
    }

    /// The process is gone: its store connection and everything on it goes too.
    pub(super) fn drop_link(&mut self, process: ProcKey) {
        if let Some(link) = self.procs.get_mut(&process).and_then(|p| p.link.take()) {
            self.store_send(StoreCommand::ConnectionClosed {
                connection: link.connection,
            });
        }
    }

    fn proc_session(&self, process: ProcKey) -> Option<u64> {
        let sid = self.instances.get(&process.0)?.current?;
        let session = self.sessions.get(&sid)?;
        (session.nonce == process.1).then_some(sid)
    }

    // ------------------------------------------------------------------ put

    fn put_refusal(&mut self, sid: u64, put_id: u64, reason: v2::Reason, detail: &str) {
        self.send(
            sid,
            M::PutRefused(v2::PutRefused {
                put_id,
                reason: reason as i32,
                detail: detail.to_string(),
            }),
        );
    }

    pub(super) fn put_begin(&mut self, sid: u64, m: v2::PutBegin) -> Checked<()> {
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        let process = (session.instance, session.nonce);
        let (scope, shm) = (session.scope, session.shm);
        let put_id = m.put_id;
        let key = (process.0, process.1, put_id);
        let Some(proc) = self.procs.get_mut(&process) else {
            return Ok(());
        };
        if put_id == 0 || put_id <= proc.put_hw {
            self.put_refusal(sid, put_id, v2::Reason::Invalid, "put ids must increase");
            return Ok(());
        }
        proc.put_hw = put_id;
        if !self.ready() || self.store.is_none() {
            self.put_refusal(
                sid,
                put_id,
                v2::Reason::PutNotReady,
                "the manager is not ready",
            );
            return Ok(());
        }
        let open = self.puts.keys().filter(|k| (k.0, k.1) == process).count();
        if open >= self.cfg.limits.max_open_puts as usize {
            self.put_refusal(sid, put_id, v2::Reason::Queue, "too many open puts");
            return Ok(());
        }
        let mode = v2::CopyMode::try_from(m.mode).unwrap_or(v2::CopyMode::Unspecified);
        if mode == v2::CopyMode::DataChannel {
            self.put_refusal(
                sid,
                put_id,
                v2::Reason::Invalid,
                "the data channel is not served by this build",
            );
            return Ok(());
        }
        if mode != v2::CopyMode::SharedWindows || !shm {
            self.put_refusal(
                sid,
                put_id,
                v2::Reason::Invalid,
                "shared windows need a Unix-socket session of the manager's own user",
            );
            return Ok(());
        }
        let Ok(record_key) = key_from(m.key.as_ref()) else {
            self.put_refusal(sid, put_id, v2::Reason::Invalid, "bad key");
            return Ok(());
        };
        let parent = if m.has_published_parent {
            match key_from(m.published_parent.as_ref()) {
                Ok(parent) => Some(parent),
                Err(_) => {
                    self.put_refusal(sid, put_id, v2::Reason::Invalid, "bad parent");
                    return Ok(());
                }
            }
        } else {
            None
        };
        let Ok(bucket) = <[u8; 16]>::try_from(m.bucket.as_slice()) else {
            self.put_refusal(sid, put_id, v2::Reason::Invalid, "bucket must be 16 bytes");
            return Ok(());
        };
        let (Ok(chain_pages), Ok(tail_pages)) = (
            u32::try_from(m.chain_pages),
            u32::try_from(m.published_tail_pages),
        ) else {
            self.put_refusal(sid, put_id, v2::Reason::Invalid, "page counts");
            return Ok(());
        };
        let pid = self.sessions[&sid].pid;
        let Some(link) = self.link_for(process, scope, pid) else {
            let detail = if pid == 0 {
                "the peer's process id is not visible; share a PID namespace with the manager"
            } else {
                "no connection to the manager's store"
            };
            self.put_refusal(sid, put_id, v2::Reason::Io, detail);
            return Ok(());
        };
        let descriptor = self.scopes[&scope].valid.descriptor.clone();
        let (events, mut answers) = mpsc::channel::<io::Result<PutAnswer>>(32);
        let begun = self.store_call(|reply| StoreCommand::PutBegin {
            connection: link.connection,
            transfer: put_id,
            generation: link.generation,
            namespace: link.namespace,
            descriptor,
            key: record_key,
            published_parent: parent,
            bucket,
            chain_pages,
            published_tail_pages: tail_pages,
            row: m.row,
            events,
            reply,
        });
        if let Err(error) = begun {
            self.put_refusal(sid, put_id, v2::Reason::Invalid, &error.to_string());
            return Ok(());
        }
        self.puts.insert(
            key,
            PutRec {
                phase: PutPhase::Open,
                link,
                unfilled: 0,
                fence: Party::Idle,
                refused: false,
                key: record_key,
                chain_pages: m.chain_pages,
                accepted: None,
            },
        );
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            while let Some(answer) = answers.recv().await {
                let event = Event::PutAnswer {
                    process,
                    put_id,
                    answer,
                };
                if tx.send(event).await.is_err() {
                    return;
                }
            }
        });
        Ok(())
    }

    pub(super) fn put_answer(
        &mut self,
        process: ProcKey,
        put_id: u64,
        answer: io::Result<PutAnswer>,
    ) {
        let key = (process.0, process.1, put_id);
        let Some(sid) = self.proc_session(process) else {
            // The writer is gone; the store finishes or abandons the operation itself.
            if matches!(
                answer,
                Ok(PutAnswer::Stored { .. }) | Ok(PutAnswer::Refused(_)) | Err(_)
            ) {
                self.puts_finish(key);
            }
            return;
        };
        let Some(rec) = self.puts.get_mut(&key) else {
            return;
        };
        match answer {
            Ok(PutAnswer::Accept(accept)) => {
                rec.accepted = Some((accept.parent, accept.first_page));
                self.send(
                    sid,
                    M::PutAccept(v2::PutAccept {
                        put_id,
                        first_page: u64::from(accept.first_page),
                        has_parent: accept.parent.is_some(),
                        parent: accept.parent.map(key_to),
                        record_bytes: accept.record_bytes,
                        channel: None,
                    }),
                );
            }
            Ok(PutAnswer::Grant(grant)) => {
                rec.unfilled += 1;
                rec.fence = Party::Pending;
                let window = v2::WindowSpec {
                    index: grant.index,
                    region: grant.region,
                    offset: grant.offset,
                    length: grant.length,
                    record: grant.record,
                    first_page: grant.page_first as u32,
                    page_count: grant.page_count,
                    row: grant.row,
                };
                self.send(
                    sid,
                    M::PutGrant(v2::PutGrant {
                        put_id,
                        window: Some(window),
                    }),
                );
            }
            Ok(PutAnswer::Stored { bytes }) => {
                rec.phase = PutPhase::Finished;
                self.send(sid, M::Stored(v2::Stored { put_id, bytes }));
                self.puts_settle(key);
            }
            Ok(PutAnswer::Refused(refused)) => {
                rec.phase = PutPhase::Finished;
                let already = rec.refused;
                rec.refused = true;
                if !already {
                    let reason = v2::Reason::try_from(i32::from(refused.reason))
                        .unwrap_or(v2::Reason::Invalid);
                    self.put_refusal(sid, put_id, reason, &refused.detail);
                }
                self.puts_settle(key);
            }
            Err(error) => {
                rec.phase = PutPhase::Finished;
                let already = rec.refused;
                rec.refused = true;
                if !already {
                    self.put_refusal(sid, put_id, v2::Reason::Io, &error.to_string());
                }
                self.puts_settle(key);
            }
        }
    }

    /// A finished put with no window the writer may still hold needs no record.
    fn puts_settle(&mut self, key: PutKey) {
        let Some(rec) = self.puts.get(&key) else {
            return;
        };
        if rec.phase == PutPhase::Finished && rec.unfilled == 0 {
            self.puts.remove(&key);
        }
    }

    fn puts_finish(&mut self, key: PutKey) {
        if let Some(rec) = self.puts.get_mut(&key) {
            rec.phase = PutPhase::Finished;
        }
        self.puts_settle(key);
    }

    pub(super) fn put_filled(&mut self, sid: u64, m: v2::PutFilled) -> Checked<()> {
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        let process = (session.instance, session.nonce);
        let key = (process.0, process.1, m.put_id);
        let Some(rec) = self.puts.get(&key) else {
            return Ok(());
        };
        if rec.phase != PutPhase::Open {
            return Ok(());
        }
        let link = rec.link;
        let row = m.has_row.then_some(m.row_xxh3);
        let result = self.store_call(|reply| StoreCommand::WindowFilled {
            connection: link.connection,
            generation: link.generation,
            transfer: m.put_id,
            index: m.index,
            page_xxh3: m.page_xxh3.clone(),
            row_xxh3: row,
            reply,
        });
        match result {
            Ok(()) => {
                if let Some(rec) = self.puts.get_mut(&key) {
                    rec.unfilled = rec.unfilled.saturating_sub(1);
                    if rec.unfilled == 0 && rec.fence == Party::Pending {
                        rec.fence = Party::Idle;
                    }
                }
            }
            Err(error) => {
                self.put_fail(sid, key, v2::Reason::Invalid, &error.to_string());
            }
        }
        Ok(())
    }

    fn put_fail(&mut self, sid: u64, key: PutKey, reason: v2::Reason, detail: &str) {
        let Some(rec) = self.puts.get_mut(&key) else {
            return;
        };
        let link = rec.link;
        let already = rec.refused;
        rec.refused = true;
        rec.phase = PutPhase::Finished;
        self.store_send(StoreCommand::Cancel {
            connection: link.connection,
            generation: link.generation,
            transfer: key.2,
        });
        if !already {
            self.put_refusal(sid, key.2, reason, detail);
        }
        self.puts_settle(key);
    }

    pub(super) fn put_commit(&mut self, sid: u64, m: v2::PutCommit) -> Checked<()> {
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        let process = (session.instance, session.nonce);
        let key = (process.0, process.1, m.put_id);
        let Some(rec) = self.puts.get_mut(&key) else {
            return Ok(());
        };
        if rec.phase != PutPhase::Open {
            return Ok(());
        }
        // The metadata must describe exactly the record that was accepted, in this
        // scope: its identity, geometry, key, parent and length.
        let descriptor = &self.scopes[&session.scope].valid.descriptor;
        let valid = m.metadata.len() >= HEADER_BYTES
            && Header::decode(&m.metadata).is_ok_and(|header| {
                let (parent, first_page) = rec.accepted.unwrap_or((None, 0));
                header.identity == descriptor.identity
                    && header.fingerprint == descriptor.fingerprint
                    && header.matches(descriptor)
                    && header.key == rec.key
                    && header.parent == parent
                    && u64::from(header.tail_pages) + u64::from(first_page) == rec.chain_pages
            });
        if !valid {
            self.put_fail(
                sid,
                key,
                v2::Reason::Invalid,
                "record metadata does not match the put",
            );
            return Ok(());
        }
        rec.phase = PutPhase::Committed;
        let link = rec.link;
        let result = self.store_call(|reply| StoreCommand::PutCommit {
            connection: link.connection,
            generation: link.generation,
            transfer: m.put_id,
            metadata_offset: m.metadata_offset,
            metadata: m.metadata.clone(),
            reply,
        });
        if let Err(error) = result {
            self.put_fail(sid, key, v2::Reason::Invalid, &error.to_string());
        }
        Ok(())
    }

    pub(super) fn put_cancel(&mut self, sid: u64, m: v2::PutCancel) -> Checked<()> {
        let Some(session) = self.sessions.get(&sid) else {
            return Ok(());
        };
        let key = (session.instance, session.nonce, m.put_id);
        let Some(rec) = self.puts.get_mut(&key) else {
            return Ok(());
        };
        if rec.phase != PutPhase::Open {
            return Ok(());
        }
        rec.phase = PutPhase::Finished;
        rec.refused = true;
        let link = rec.link;
        self.store_send(StoreCommand::Cancel {
            connection: link.connection,
            generation: link.generation,
            transfer: m.put_id,
        });
        self.puts_settle(key);
        Ok(())
    }

    /// The writer's fence for a put: its writes to the leased windows have stopped.
    pub(super) fn put_fence(
        &mut self,
        instance: InstanceId,
        nonce: Nonce,
        put_id: u64,
        party: Party,
    ) {
        let key = (instance, nonce, put_id);
        let Some(rec) = self.puts.get_mut(&key) else {
            return;
        };
        if party == Party::Drained {
            let link = rec.link;
            self.puts.remove(&key);
            self.store_send(StoreCommand::ReleaseTransfer {
                connection: link.connection,
                generation: link.generation,
                transfer: put_id,
            });
        } else {
            rec.fence = party;
        }
    }

    /// The stream ended: puts that have not committed cannot continue; their leased
    /// windows wait for the writer's fence or its exit.
    pub(super) fn puts_lost(&mut self, session: &super::core::Session) {
        let process = (session.instance, session.nonce);
        let keys: Vec<PutKey> = self
            .puts
            .iter()
            .filter(|(k, rec)| (k.0, k.1) == process && rec.phase == PutPhase::Open)
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            if let Some(rec) = self.puts.get_mut(&key) {
                rec.phase = PutPhase::Finished;
                rec.refused = true;
                let link = rec.link;
                self.store_send(StoreCommand::Cancel {
                    connection: link.connection,
                    generation: link.generation,
                    transfer: key.2,
                });
            }
            self.puts_settle(key);
        }
    }

    pub(super) fn puts_proc_exited(&mut self, process: ProcKey) {
        self.puts.retain(|k, _| (k.0, k.1) != process);
        self.drop_link(process);
    }

    /// `Goodbye{quiesced}`: the writer has stopped touching every window it was leased.
    pub(super) fn puts_quiesced(&mut self, process: ProcKey) {
        let keys: Vec<PutKey> = self
            .puts
            .keys()
            .filter(|k| (k.0, k.1) == process)
            .copied()
            .collect();
        for key in keys {
            let Some(rec) = self.puts.get(&key) else {
                continue;
            };
            let (link, finished) = (rec.link, rec.phase == PutPhase::Finished);
            self.store_send(StoreCommand::ReleaseTransfer {
                connection: link.connection,
                generation: link.generation,
                transfer: key.2,
            });
            if finished {
                self.puts.remove(&key);
            } else if let Some(rec) = self.puts.get_mut(&key) {
                rec.fence = Party::Drained;
            }
        }
    }

    // ------------------------------------------------------------------ get (manager claims)

    /// Starts the store's read of a chain for a claim whose source is the manager.
    pub(super) fn manager_get(
        &mut self,
        key: ClaimKey,
        chain: Vec<Key>,
        skip_pages: u32,
        pages: u64,
        row: bool,
    ) -> Result<(), v2::ClaimReason> {
        let process = (key.0, key.1);
        let Some(claim) = self.claims.get(&key) else {
            return Err(v2::ClaimReason::UnknownClaim);
        };
        let _ = claim;
        let Some(link) = self.procs.get(&process).and_then(|p| p.link) else {
            return Err(v2::ClaimReason::SourceBusy);
        };
        let Ok(expected) = u32::try_from(pages) else {
            return Err(v2::ClaimReason::InvalidClaim);
        };
        let (events, mut answers) = mpsc::channel::<io::Result<GetAnswer>>(64);
        let begun = self.store_call(|reply| StoreCommand::GetBegin {
            connection: link.connection,
            transfer: key.2,
            generation: link.generation,
            namespace: link.namespace,
            chain,
            skip_pages,
            expected_pages: expected,
            row,
            events,
            reply,
        });
        if begun.is_err() {
            return Err(v2::ClaimReason::SourceBusy);
        }
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            while let Some(answer) = answers.recv().await {
                if tx.send(Event::GetAnswer { key, answer }).await.is_err() {
                    return;
                }
            }
        });
        Ok(())
    }

    pub(super) fn store_cancel_get(&mut self, key: ClaimKey) {
        if let Some(link) = self.procs.get(&(key.0, key.1)).and_then(|p| p.link) {
            self.store_send(StoreCommand::Cancel {
                connection: link.connection,
                generation: link.generation,
                transfer: key.2,
            });
        }
    }

    pub(super) fn store_release_window(&mut self, key: ClaimKey, index: u32) {
        if let Some(link) = self.procs.get(&(key.0, key.1)).and_then(|p| p.link) {
            let _ = self.store_call(|reply| StoreCommand::WindowReleased {
                connection: link.connection,
                generation: link.generation,
                transfer: key.2,
                index,
                reply,
            });
        }
    }

    pub(super) fn store_release_transfer(&mut self, key: ClaimKey) {
        if let Some(link) = self.procs.get(&(key.0, key.1)).and_then(|p| p.link) {
            self.store_send(StoreCommand::ReleaseTransfer {
                connection: link.connection,
                generation: link.generation,
                transfer: key.2,
            });
        }
    }
}

fn key_of(key: &proto::Key) -> Option<Key> {
    let digest: [u8; 16] = key.digest.as_slice().try_into().ok()?;
    (key.consumed_tokens != 0).then_some(Key {
        consumed_tokens: key.consumed_tokens,
        digest,
    })
}

/// The checksums persisted with a record: one per page, and the row's.
pub(super) fn persisted_checksums(metadata: &[u8]) -> Option<(Vec<u64>, u64)> {
    let header = Header::decode(metadata).ok()?;
    let pages = header.tail_pages as usize;
    // The table is padded to a block; wait for all of it before reading.
    if (metadata.len() as u64) < HEADER_BYTES as u64 + header.checksum_bytes() {
        return None;
    }
    let table = metadata.get(HEADER_BYTES..HEADER_BYTES + pages * 8)?;
    let sums = table
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    Some((sums, header.row_checksum))
}
