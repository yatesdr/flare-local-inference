//! The manager's own store as a participant in the pool protocol.
//!
//! The store keeps its own sessions, windows and records. The pool talks to it
//! the way a local client does: one store connection per instance process (so a
//! stream that drops while the process lives keeps its windows leased), one
//! catalog subscription per scope that feeds the manager's entries into the
//! index, and the store's put and get operations behind PutBegin and claims.

use super::{
    Checked, Violation,
    claims::{ClaimKey, Party},
    core::{ChanOwner, Core, Event, Nonce, ProcKey, ScopeId},
    index::{Entry, InstanceId, SourceId, Tier},
    key_from, key_to,
};
use crate::{
    Key,
    data::channel,
    proto::{self, v2, v2::server_message::M},
    record::{Descriptor, HEADER_BYTES, Header, NamespaceId},
    store::{GetAnswer, PutAnswer, Reply, StoreCommand},
};
use flare_selene::pool::frame::Shape;
use std::{io, time::Instant};
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
    /// Set when the writer's bytes arrive on the data channel.
    pub channel: Option<PutChannel>,
    /// A commit that arrived before the data channel delivered every window: the
    /// channel and the session are separate streams, so it waits for the last fill.
    pub deferred_commit: Option<(u64, v2::PutCommit)>,
}

/// A put's data-channel transfer. Its windows are written only by the manager's
/// handler, so the handler's exit is what lets them go back to the store.
pub(super) struct PutChannel {
    pub transfer: u64,
    pub token: [u8; flare_selene::pool::frame::TOKEN_BYTES],
    pub exited: bool,
    pub stopped_at: Option<Instant>,
    pub quarantined: bool,
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
        let connection = self.new_store_connection()?;
        let namespace = descriptor.namespace_id();
        let replica = super::barrier::hex(&process.0);
        let generation = if pid == 0 {
            // A process on another host has no pid to watch. It never touches the
            // manager's memory: the manager's own data-channel handler does.
            self.store_call(|reply| StoreCommand::RemoteHello {
                connection,
                node: replica,
                reply,
            })
            .ok()?
        } else {
            self.store_call(|reply| StoreCommand::Hello {
                connection,
                replica,
                pid,
                descriptor,
                reply,
            })
            .ok()?
            .generation
        };
        let link = Link {
            connection,
            generation,
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
        eprintln!("selene: put {put_id} refused ({reason:?}): {detail}");
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
        match mode {
            v2::CopyMode::SharedWindows if shm => {}
            v2::CopyMode::SharedWindows => {
                self.put_refusal(
                    sid,
                    put_id,
                    v2::Reason::Invalid,
                    "shared windows need a Unix-socket session of the manager's own user",
                );
                return Ok(());
            }
            v2::CopyMode::DataChannel if self.cfg.channel.is_some() => {}
            v2::CopyMode::DataChannel => {
                self.put_refusal(
                    sid,
                    put_id,
                    v2::Reason::Invalid,
                    "this manager serves no data channel (it has no network listener)",
                );
                return Ok(());
            }
            _ => {
                self.put_refusal(sid, put_id, v2::Reason::Invalid, "bad mode");
                return Ok(());
            }
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
        let hub = self.cfg.channel.clone();
        let channel = match (mode, hub) {
            (v2::CopyMode::DataChannel, Some(hub)) => match hub.open(channel::Kind::Put) {
                Ok((transfer, token)) => Some(PutChannel {
                    transfer,
                    token,
                    exited: false,
                    stopped_at: None,
                    quarantined: false,
                }),
                Err(error) => {
                    self.store_send(StoreCommand::Cancel {
                        connection: link.connection,
                        generation: link.generation,
                        transfer: put_id,
                    });
                    self.put_refusal(sid, put_id, v2::Reason::Io, &error.to_string());
                    return Ok(());
                }
            },
            _ => None,
        };
        if let Some(open) = &channel {
            self.channels.insert(open.transfer, ChanOwner::Put(key));
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
                channel,
                deferred_commit: None,
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
                let channel = rec
                    .channel
                    .as_ref()
                    .zip(self.cfg.channel.as_ref())
                    .map(|(open, hub)| super::channel_ref(hub, &open.token));
                self.send(
                    sid,
                    M::PutAccept(v2::PutAccept {
                        put_id,
                        first_page: u64::from(accept.first_page),
                        has_parent: accept.parent.is_some(),
                        parent: accept.parent.map(key_to),
                        record_bytes: accept.record_bytes,
                        channel,
                        rank_channels: Vec::new(),
                    }),
                );
            }
            Ok(PutAnswer::Grant(grant)) => {
                rec.unfilled += 1;
                if let Some((open, hub)) = rec.channel.as_ref().zip(self.cfg.channel.as_ref()) {
                    // The window is filled by the manager's handler from what the
                    // instance writes; no one else holds it.
                    let region = u8::try_from(grant.region)
                        .ok()
                        .and_then(crate::data::Region::from_u8);
                    let Some(region) = region else {
                        self.put_fail_by_key(key, v2::Reason::Io, "a window in an unknown region");
                        return;
                    };
                    hub.push_grant(
                        open.transfer,
                        channel::GrantOut {
                            shape: Shape {
                                index: grant.index,
                                record: grant.record,
                                first_page: grant.page_first as u32,
                                page_count: grant.page_count,
                                row: grant.row,
                                length: grant.length,
                            },
                            region,
                            offset: grant.offset,
                        },
                    );
                    return;
                }
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
    /// One on the data channel is held until its handler has exited.
    fn puts_settle(&mut self, key: PutKey) {
        let Some(rec) = self.puts.get(&key) else {
            return;
        };
        if rec.phase != PutPhase::Finished {
            return;
        }
        if rec.channel.is_some() {
            return self.put_channel_end(key);
        }
        if rec.unfilled == 0 {
            self.puts.remove(&key);
        }
    }

    /// The put is over: stop its transfer, and once the handler is gone return
    /// the windows to the store and drop the record.
    fn put_channel_end(&mut self, key: PutKey) {
        let Some(rec) = self.puts.get_mut(&key) else {
            return;
        };
        let Some(open) = rec.channel.as_mut() else {
            return;
        };
        if !open.exited {
            open.stopped_at.get_or_insert_with(Instant::now);
            let quiet = self
                .cfg
                .channel
                .as_ref()
                .is_none_or(|hub| hub.stop(open.transfer));
            if !quiet {
                // `Closed` follows when the handler has dropped its connection.
                return;
            }
            open.exited = true;
            self.channels.remove(&open.transfer);
        }
        let link = rec.link;
        self.puts.remove(&key);
        self.store_send(StoreCommand::ReleaseTransfer {
            connection: link.connection,
            generation: link.generation,
            transfer: key.2,
        });
    }

    /// A put's handler has exited.
    pub(super) fn put_channel_closed(&mut self, key: PutKey) {
        let Some(open) = self.puts.get_mut(&key).and_then(|rec| rec.channel.as_mut()) else {
            return;
        };
        open.exited = true;
        open.quarantined = false;
        self.puts_settle(key);
    }

    /// The put's windows that have been filled and written are accounted for; a
    /// handler that reports an error ends the put.
    pub(super) fn put_channel_failed(&mut self, key: PutKey, reason: &str) {
        eprintln!("selene: put {} on the data channel failed: {reason}", key.2);
        self.put_fail_by_key(key, v2::Reason::Io, reason);
    }

    /// Fails a put, telling its writer when its session is still there.
    fn put_fail_by_key(&mut self, key: PutKey, reason: v2::Reason, detail: &str) {
        match self.proc_session((key.0, key.1)) {
            Some(sid) => self.put_fail(sid, key, reason, detail),
            None => {
                if let Some(rec) = self.puts.get_mut(&key) {
                    let link = rec.link;
                    rec.refused = true;
                    rec.phase = PutPhase::Finished;
                    self.store_send(StoreCommand::Cancel {
                        connection: link.connection,
                        generation: link.generation,
                        transfer: key.2,
                    });
                }
                self.puts_settle(key);
            }
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
        let key = (session.instance, session.nonce, m.put_id);
        // Windows of a put on the data channel are filled there, never reported here.
        if self.puts.get(&key).is_some_and(|rec| rec.channel.is_some()) {
            return Err(Violation::new("PutFilled for a put on the data channel"));
        }
        self.put_window_filled(key, m.index, m.page_xxh3, m.has_row.then_some(m.row_xxh3));
        Ok(())
    }

    /// The writer filled window `index`: the store checks and consumes it.
    pub(super) fn put_window_filled(
        &mut self,
        key: PutKey,
        index: u32,
        page_xxh3: Vec<u64>,
        row_xxh3: Option<u64>,
    ) {
        let Some(rec) = self.puts.get(&key) else {
            return;
        };
        if rec.phase != PutPhase::Open {
            return;
        }
        let link = rec.link;
        let result = self.store_call(|reply| StoreCommand::WindowFilled {
            connection: link.connection,
            generation: link.generation,
            transfer: key.2,
            index,
            page_xxh3,
            row_xxh3,
            reply,
        });
        match result {
            Ok(()) => {
                let mut deferred = None;
                if let Some(rec) = self.puts.get_mut(&key) {
                    rec.unfilled = rec.unfilled.saturating_sub(1);
                    if rec.unfilled == 0 && rec.fence == Party::Pending {
                        rec.fence = Party::Idle;
                    }
                    if rec.unfilled == 0 {
                        deferred = rec.deferred_commit.take();
                    }
                }
                if let Some((sid, commit)) = deferred {
                    if let Err(violation) = self.put_commit(sid, commit) {
                        self.violation(sid, violation);
                    }
                }
            }
            Err(error) => {
                self.put_fail_by_key(key, v2::Reason::Invalid, &error.to_string());
            }
        }
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
        if rec.channel.is_some() && rec.unfilled > 0 {
            rec.deferred_commit = Some((sid, m));
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
        if rec.channel.is_some() {
            // The writer touches none of the manager's windows; its handler's exit
            // is what returns them.
            return;
        }
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
        let keys: Vec<PutKey> = self
            .puts
            .keys()
            .filter(|k| (k.0, k.1) == process)
            .copied()
            .collect();
        let mut waiting = false;
        for key in keys {
            let on_channel = self.puts.get(&key).is_some_and(|rec| rec.channel.is_some());
            if !on_channel {
                self.puts.remove(&key);
                continue;
            }
            if let Some(rec) = self.puts.get_mut(&key) {
                let link = rec.link;
                rec.refused = true;
                rec.phase = PutPhase::Finished;
                self.store_send(StoreCommand::Cancel {
                    connection: link.connection,
                    generation: link.generation,
                    transfer: key.2,
                });
            }
            self.puts_settle(key);
            waiting |= self.puts.contains_key(&key);
        }
        // A handler still running holds windows of this connection: it goes last.
        if !waiting {
            self.drop_link(process);
        }
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
            if rec.channel.is_some() {
                // Its handler's exit, not the writer's word, returns the windows.
                continue;
            }
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
