//! Near-tier peer runtime: upstream placement, write-through forwarding, catalog events and
//! remote fetches.
//!
//! The store owns all state and runs on its own thread; this module owns everything that talks
//! to upstream managers. Catalog events are applied by the store, not here, and a remote Get is
//! fetched into local DRAM on request and then adopted by the store.

use crate::arena::Extent;
use crate::config;
use crate::peer::{Action, Forwarder};
use crate::peer_client::{
    FetchRequest, ForwardBytes, ForwardRecord, PeerSet, TlsSettings, descriptor_to_proto,
};
use crate::record;
use crate::record::NamespaceId;
use crate::regions::LocalRegions;
use crate::store::{ForwardJob, PeerEvent, RemotePlan, StoreCommand};
use crate::{Key, proto};
use std::collections::BTreeMap;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

pub type FetchReply = oneshot::Sender<io::Result<Fetched>>;

/// A completed fetch: the store reservation that protects the chain until the serving Get
/// plans its own pins, and the adopted (record, extent, row) triples.
pub struct Fetched {
    pub reservation: u64,
    pub records: Vec<(Key, Arc<Extent>, bool)>,
}

/// Work handed from the store or a control handler to the peer runtime.
pub enum PeerCommand {
    /// A namespace was registered locally; mirror its upstream catalog.
    Subscribe {
        namespace: [u8; 32],
        descriptor: proto::Descriptor,
    },
    /// A local record was stored; forward it to the upstream that owns its chain.
    Forward(Box<ForwardJob>),
    /// A catalog stream ended or skipped a sequence; replace that one subscription.
    Restart {
        node: String,
        namespace: [u8; 32],
        epoch: u64,
    },
    /// Fetch a chain from its owner into local DRAM and return the extents.
    Fetch {
        namespace: [u8; 32],
        plan: RemotePlan,
        /// The local connection whose Get asked for this fetch; its close releases the pins.
        connection: u64,
        reply: FetchReply,
    },
}

struct Forwarding {
    job: ForwardJob,
}

type ForwardId = (NamespaceId, Key);

/// A catalog subscription that failed and must be retried.
struct Retry {
    attempts: u32,
    due: Instant,
}

struct PeerRuntime {
    set: PeerSet,
    forwarder: Forwarder<ForwardId>,
    jobs: BTreeMap<ForwardId, Forwarding>,
    descriptors: BTreeMap<[u8; 32], proto::Descriptor>,
    epochs: BTreeMap<(String, [u8; 32]), Arc<AtomicU64>>,
    retries: BTreeMap<(String, NamespaceId), Retry>,
    regions: Arc<LocalRegions>,
    commands: mpsc::Sender<StoreCommand>,
    tx: mpsc::Sender<PeerCommand>,
}

/// Start the peer runtime. Returns the command sender and the thread.
pub fn spawn(
    config: &config::Config,
    regions: Arc<LocalRegions>,
    commands: mpsc::Sender<StoreCommand>,
    stop: Arc<AtomicBool>,
) -> io::Result<(mpsc::Sender<PeerCommand>, thread::JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel(256);
    let tls = load_tls(config)?;
    let upstreams = config.upstreams.clone();
    let connections = config.stream_connections;
    let transport = config.peer_transport.to_string();
    let command_tx = commands.clone();
    let task_tx = tx.clone();
    let handle = thread::Builder::new()
        .name("selene-peer".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("selene: cannot start the peer runtime: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                // A near manager still serves local records while its upstream is down, so a
                // failed first connection retries instead of disabling peer support.
                let set = loop {
                    // Each upstream has its own deadline inside `PeerSet::connect`; a silent
                    // one must not discard a healthy connection made earlier in the batch.
                    let result = tokio::select! {
                        result = PeerSet::connect(
                            &upstreams,
                            regions.clone(),
                            connections,
                            tls.clone(),
                            &transport,
                        ) => result,
                        _ = wait_for_stop(&stop) => return,
                    };
                    match result {
                        Ok(set) => break set,
                        Err(error) => eprintln!("selene: no upstream peer connected: {error}"),
                    }
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                };
                let mut peer = PeerRuntime {
                    set,
                    forwarder: Forwarder::new(crate::peer::FORWARD_QUEUE_LIMIT),
                    jobs: BTreeMap::new(),
                    descriptors: BTreeMap::new(),
                    epochs: BTreeMap::new(),
                    retries: BTreeMap::new(),
                    regions,
                    commands: command_tx,
                    tx: task_tx,
                };
                peer.run(rx, stop).await;
            });
        })
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok((tx, handle))
}

impl PeerRuntime {
    async fn run(&mut self, mut rx: mpsc::Receiver<PeerCommand>, stop: Arc<AtomicBool>) {
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !stop.load(Ordering::Acquire) {
            tokio::select! {
                command = rx.recv() => match command {
                    Some(command) => self.command(command).await,
                    None => break,
                },
                _ = tick.tick() => {}
            }
            self.tick().await;
        }
        // Drop leases and stop forwarding before the store reclaims its shared regions.
        self.jobs.clear();
        let _ = self.set.reconnect(Instant::now()).await;
    }

    async fn command(&mut self, command: PeerCommand) {
        match command {
            PeerCommand::Subscribe {
                namespace,
                descriptor,
            } => {
                if self.descriptors.contains_key(&namespace) {
                    return;
                }
                self.descriptors.insert(namespace, descriptor.clone());
                self.subscribe_all(namespace, descriptor).await;
            }
            PeerCommand::Forward(job) => self.submit(*job).await,
            PeerCommand::Restart {
                node,
                namespace,
                epoch,
            } => {
                if self.current(&node, &namespace) == epoch
                    && let Some(descriptor) = self.descriptors.get(&namespace).cloned()
                {
                    self.subscribe_pair(node, namespace, descriptor).await;
                }
            }
            PeerCommand::Fetch {
                namespace,
                plan,
                connection,
                reply,
            } => {
                let result = self.fetch(namespace, plan, connection).await;
                let _ = reply.send(result);
            }
        }
    }

    fn current(&self, node: &str, namespace: &[u8; 32]) -> u64 {
        self.epochs
            .get(&(node.to_string(), *namespace))
            .map(|epoch| epoch.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    async fn subscribe_all(&mut self, namespace: [u8; 32], descriptor: proto::Descriptor) {
        let names: Vec<String> = self.set.connected().map(str::to_string).collect();
        for name in names {
            self.subscribe_pair(name, namespace, descriptor.clone())
                .await;
        }
    }

    async fn subscribe_pair(
        &mut self,
        node: String,
        namespace: [u8; 32],
        descriptor: proto::Descriptor,
    ) {
        let epoch = Arc::new(AtomicU64::new(self.current(&node, &namespace) + 1));
        let value = epoch.load(Ordering::Acquire);
        self.epochs.insert((node.clone(), namespace), epoch.clone());
        let key = (node.clone(), namespace);
        let label = record::hex(&namespace);
        let mut stream = match self
            .set
            .subscribe_client(&node, namespace, descriptor)
            .await
        {
            Ok(stream) => {
                self.retries.remove(&key);
                stream
            }
            Err(error) => {
                // A refused namespace will not improve on retry, but every other failure may:
                // a dead channel is dropped so the reconnect puts it on fresh connections.
                if error.kind() != io::ErrorKind::InvalidInput {
                    self.drop_upstream(&node).await;
                }
                let first = !self.retries.contains_key(&key);
                if first {
                    eprintln!("selene: peer '{node}' subscription {label} failed: {error}");
                }
                let now = Instant::now();
                let retry = self.retries.entry(key).or_insert(Retry {
                    attempts: 0,
                    due: now,
                });
                retry.attempts = retry.attempts.saturating_add(1);
                retry.due = now + crate::peer_client::backoff(retry.attempts.saturating_sub(1));
                return;
            }
        };
        let commands = self.commands.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut expected: Option<u64> = None;
            let mut snapshot = false;
            loop {
                if epoch.load(Ordering::Acquire) != value {
                    return;
                }
                let event = match stream.message().await {
                    Ok(Some(event)) => event,
                    _ => break,
                };
                let sequence = event.sequence;
                let Some(body) = &event.event else {
                    break;
                };
                let first = expected.is_none();
                let valid = match expected {
                    None => matches!(body, proto::catalog_event::Event::SnapshotBegin(_)),
                    Some(previous) => sequence == previous.wrapping_add(1),
                };
                if !valid {
                    break;
                }
                if let proto::catalog_event::Event::SnapshotBegin(begin) = body
                    && first
                    && sequence != begin.seq0.wrapping_add(1)
                {
                    break;
                }
                match body {
                    proto::catalog_event::Event::SnapshotBegin(_) => snapshot = true,
                    proto::catalog_event::Event::SnapshotEnd(_) => {
                        if !snapshot {
                            break;
                        }
                        snapshot = false;
                    }
                    proto::catalog_event::Event::Entry(_) if !snapshot => break,
                    proto::catalog_event::Event::Added(_)
                    | proto::catalog_event::Event::Changed(_)
                    | proto::catalog_event::Event::Removed(_)
                        if snapshot =>
                    {
                        break;
                    }
                    _ => {}
                }
                expected = Some(sequence);
                if commands
                    .send(StoreCommand::Peer(PeerEvent::Catalog {
                        node: node.clone(),
                        namespace,
                        event,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = tx
                .send(PeerCommand::Restart {
                    node,
                    namespace,
                    epoch: value,
                })
                .await;
        });
    }

    async fn fetch(
        &mut self,
        namespace: [u8; 32],
        plan: RemotePlan,
        connection: u64,
    ) -> io::Result<Fetched> {
        let Some(first) = plan.records.first() else {
            return Ok(Fetched {
                reservation: 0,
                records: Vec::new(),
            });
        };
        let mut fetched = Vec::new();
        let root_owner = self.set.owner(&namespace, &plan.root).map(str::to_string);
        // The run starts at the first record that advertises it; otherwise the chain owner
        // serves it.
        let upstream = first
            .source
            .clone()
            .or_else(|| root_owner.clone())
            .ok_or_else(|| io::Error::other("no upstream owns the chain"))?;
        // Destinations are sized from each record's full shape, not from the local charge
        // (which may be a power-of-two arena class or a stale size). The store allocates them
        // so eviction can make room and admission sees the bytes while the transfer runs.
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(StoreCommand::ReserveFetch {
                namespace,
                chain: plan.records.iter().map(|record| record.key).collect(),
                lengths: plan.records.iter().map(|record| record.length).collect(),
                connection,
                reply,
            })
            .await
            .map_err(|_| io::Error::other("store stopped during a remote fetch"))?;
        let (reservation, extents) = answer
            .await
            .map_err(|_| io::Error::other("store dropped the fetch reservation"))?
            .map_err(|error| io::Error::other(error.to_string()))?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "the fetched chain does not fit in DRAM",
                )
            })?;
        let destinations: Vec<proto::GetDestination> = extents
            .iter()
            .enumerate()
            .map(|(index, extent)| proto::GetDestination {
                record: index as u32,
                region: proto::Region::Arena as i32,
                offset: extent.offset() as u64,
                bytes: extent.capacity() as u64,
            })
            .collect();
        let request = FetchRequest {
            descriptor: descriptor_to_proto(&plan.descriptor, &plan.descriptor.tenant),
            namespace,
            chain: plan.records.iter().map(|record| record.key).collect(),
            skip_pages: 0,
            expected_pages: plan
                .records
                .iter()
                .map(|record| u64::from(record.tail_pages))
                .sum(),
            row: true,
            destinations,
        };
        let Some(client) = self.set.client_mut(&upstream) else {
            let _ = self
                .commands
                .send(StoreCommand::ReleaseFetch { token: reservation })
                .await;
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "the source upstream is not connected",
            ));
        };
        if let Err(error) = client.fetch(request).await {
            // A refusal leaves the connection usable; a transport or session failure does not.
            if reconnect_after(&error) {
                self.drop_upstream(&upstream).await;
            }
            let _ = self
                .commands
                .send(StoreCommand::ReleaseFetch { token: reservation })
                .await;
            return Err(error);
        }
        let last = extents.len().saturating_sub(1);
        for (index, (record, extent)) in plan.records.into_iter().zip(extents).enumerate() {
            // Only the endpoint is delivered with its row.
            fetched.push((record.key, extent, index == last));
        }
        Ok(Fetched {
            reservation,
            records: fetched,
        })
    }

    async fn submit(&mut self, job: ForwardJob) {
        let namespace = job.namespace;
        let key = job.key;
        let id = (namespace, key);
        let root = job.ancestors.first().copied().unwrap_or(key);
        let parent = job.parent.map(|parent| (namespace, parent));
        let ancestors: Vec<ForwardId> = job
            .ancestors
            .iter()
            .map(|ancestor| (namespace, *ancestor))
            .collect();
        let Some(upstream) = self.set.owner(&namespace, &root).map(str::to_string) else {
            self.report(id, Err(io::Error::other("no upstream owns the record")))
                .await;
            return;
        };
        self.jobs.insert(id, Forwarding { job });
        let now = Instant::now();
        for action in self.forwarder.submit(now, &upstream, id, parent, ancestors) {
            self.apply(action).await;
        }
    }

    async fn tick(&mut self) {
        let now = Instant::now();
        let due: Vec<(String, [u8; 32], proto::Descriptor)> = self
            .retries
            .iter()
            .filter(|(_, retry)| retry.due <= now)
            .filter_map(|((node, namespace), _)| {
                self.descriptors
                    .get(namespace)
                    .map(|descriptor| (node.clone(), *namespace, descriptor.clone()))
            })
            .collect();
        for (node, namespace, descriptor) in due {
            self.subscribe_pair(node, namespace, descriptor).await;
        }
        let ready = self.forwarder.ready(now);
        for action in ready {
            if let Action::Send { upstream, key } = action {
                self.send(now, &upstream, key).await;
            }
        }
        let reconnected = self.set.reconnect(now).await;
        for name in reconnected {
            for (namespace, descriptor) in self.descriptors.clone() {
                self.subscribe_pair(name.clone(), namespace, descriptor)
                    .await;
            }
        }
    }

    async fn send(&mut self, now: Instant, upstream: &str, id: ForwardId) {
        let prepared = match self.jobs.get_mut(&id) {
            Some(state) => prepare(&state.job, &self.regions),
            None => Err(io::Error::other("forward job vanished")),
        };
        let record = match prepared {
            Ok(record) => record,
            Err(error) => {
                self.fail(now, id).await;
                if !self.jobs.contains_key(&id) {
                    self.report(id, Err(error)).await;
                }
                return;
            }
        };
        let Some(client) = self.set.client_mut(upstream) else {
            // The upstream is in reconnect backoff: the attempt still failed on the transport.
            let _ = self
                .commands
                .send(StoreCommand::Peer(PeerEvent::ForwardFailed {
                    namespace: id.0,
                    key: id.1,
                }))
                .await;
            self.fail(now, id).await;
            return;
        };
        match client.forward(record).await {
            Ok(_) => {
                for action in self.forwarder.complete(id) {
                    self.apply(action).await;
                }
                self.report(id, Ok(())).await;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // The upstream lacks an ancestor: restore it before retrying the child.
                for action in self.forwarder.missing_ancestry(now, id) {
                    self.apply(action).await;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                // Any other protocol refusal is retried later.
                self.fail(now, id).await;
            }
            Err(_) => {
                let _ = self
                    .commands
                    .send(StoreCommand::Peer(PeerEvent::ForwardFailed {
                        namespace: id.0,
                        key: id.1,
                    }))
                    .await;
                self.drop_upstream(upstream).await;
                self.fail(now, id).await;
            }
        }
    }

    /// Remove an upstream that failed on the transport and tell the store its records are gone.
    /// A dropped peer is excluded from placement and reopened by the reconnect backoff; without
    /// this a dead channel would be reused for every later call.
    async fn drop_upstream(&mut self, node: &str) {
        if self.set.drop_client(node) {
            let _ = self
                .commands
                .send(StoreCommand::Peer(PeerEvent::Disconnected {
                    node: node.to_string(),
                }))
                .await;
        }
    }

    async fn fail(&mut self, now: Instant, id: ForwardId) {
        for action in self.forwarder.failed(now, id) {
            self.apply(action).await;
        }
    }

    async fn apply(&mut self, action: Action<ForwardId>) {
        match action {
            Action::Pin(_) | Action::Unpin(_) => {}
            Action::Skipped(key) => {
                self.report(key, Err(io::Error::other("forward queue is full")))
                    .await;
            }
            Action::Abandoned { upstream, key } => {
                // Retries exhausted: the connection is likely stalled (unreleased upstream
                // window credits), so drop it rather than letting it block the next attempt.
                self.set.drop_client(&upstream);
                self.report(
                    key,
                    Err(io::Error::other("forward abandoned after retries")),
                )
                .await;
            }
            Action::MissingAncestry(parent) => {
                if !self.jobs.contains_key(&parent) {
                    let (reply, answer) = oneshot::channel();
                    if self
                        .commands
                        .send(StoreCommand::PrepareForward {
                            namespace: parent.0,
                            key: parent.1,
                            reply,
                        })
                        .await
                        .is_ok()
                        && let Ok(Ok(job)) = answer.await
                    {
                        Box::pin(self.submit(job)).await;
                    }
                }
            }
            Action::Send { .. } => {}
        }
    }

    async fn report(&mut self, id: ForwardId, result: io::Result<()>) {
        self.jobs.remove(&id);
        let _ = self
            .commands
            .send(StoreCommand::Peer(PeerEvent::Forwarded {
                namespace: id.0,
                key: id.1,
                result,
            }))
            .await;
    }
}

/// Build the wire form of one forward step. A memory record addresses the arena directly. A
/// disk-backed record is paged through one bounded staging window as grants arrive, so the
/// forward never has to stage the whole record.
fn prepare(job: &ForwardJob, regions: &LocalRegions) -> io::Result<ForwardRecord> {
    let header = &job.record.header;
    let page_segment = usize::try_from(header.page_segment)
        .map_err(|_| io::Error::other("page segment does not fit"))?;
    let length = usize::try_from(header.length(job.row))
        .map_err(|_| io::Error::other("record length does not fit"))?;
    let (source, metadata) = if let Some(extent) = job.record.memory() {
        if extent.bytes().len() < length {
            return Err(io::Error::other("memory lease is shorter than the record"));
        }
        (
            ForwardBytes::Shared {
                region: crate::data::Region::Arena,
                offset: extent.offset() as u64,
            },
            extent.bytes()[..page_segment].to_vec(),
        )
    } else if let Some(disk) = job.record.disk() {
        let file = disk.reader()?;
        // One window is at most a page window (a single page may exceed the configured
        // maximum) or the endpoint row.
        let window = length.min(
            header
                .page_record_bytes()
                .max(header.row_record_bytes())
                .max(crate::store::DEFAULT_MAX_WINDOW) as usize,
        );
        let staging = regions
            .staging()
            .allocate(window)
            .ok_or_else(|| io::Error::other("staging allocation failed"))?;
        // The file is opened O_DIRECT, so the metadata read needs a page-aligned buffer and a
        // block-sized length. Round up into the first page: every record carries at least one
        // page after its metadata, so the extra bytes are still inside it.
        let aligned = page_segment
            .max(1)
            .next_multiple_of(crate::record::BLOCK_BYTES as usize);
        let buffer_bytes = aligned.next_power_of_two();
        let arena = crate::arena::Arena::new(buffer_bytes)?;
        let mut buffer = arena
            .allocate(buffer_bytes)
            .ok_or_else(|| io::Error::other("metadata buffer allocation failed"))?;
        crate::disk::read_at(
            &file,
            &mut buffer.bytes_mut()[..aligned],
            crate::disk::ENVELOPE_BYTES,
        )?;
        let metadata = buffer.bytes()[..page_segment].to_vec();
        drop(buffer);
        drop(arena);
        (
            ForwardBytes::Disk {
                file,
                offset: crate::disk::ENVELOPE_BYTES,
                staging,
            },
            metadata,
        )
    } else {
        return Err(io::Error::other("forward lease has no bytes"));
    };
    let record = ForwardRecord {
        namespace: job.namespace,
        key: job.key,
        chain_pages: job.chain_pages,
        published_parent: job.parent,
        bucket: job.bucket,
        published_tail_pages: job.tail_pages,
        row: job.row,
        descriptor: descriptor_to_proto(&job.descriptor, &job.descriptor.tenant),
        source,
        header: metadata[..record::HEADER_BYTES].to_vec(),
        checksum_segment: metadata[record::HEADER_BYTES..].to_vec(),
    };
    Ok(record)
}

/// Whether a peer call failed in a way that needs a fresh connection. A protocol refusal
/// (`InvalidInput`) or a miss (`NotFound`) leaves the channel usable; anything else is a
/// transport or session failure.
fn reconnect_after(error: &io::Error) -> bool {
    !matches!(
        error.kind(),
        io::ErrorKind::InvalidInput | io::ErrorKind::NotFound
    )
}

/// Completes when the manager is asked to stop.
async fn wait_for_stop(stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn load_tls(config: &config::Config) -> io::Result<Option<TlsSettings>> {
    let (Some(cert), Some(key), Some(ca)) = (&config.tls_cert, &config.tls_key, &config.tls_ca)
    else {
        return Ok(None);
    };
    Ok(Some(TlsSettings {
        ca: std::fs::read(ca)?,
        cert: std::fs::read(cert)?,
        key: std::fs::read(key)?,
        domain: String::new(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn only_a_broken_channel_needs_reconnection() {
        assert!(!reconnect_after(&io::Error::from(
            io::ErrorKind::InvalidInput
        )));
        assert!(!reconnect_after(&io::Error::from(io::ErrorKind::NotFound)));
        assert!(reconnect_after(&io::Error::from(io::ErrorKind::Other)));
        assert!(reconnect_after(&io::Error::from(io::ErrorKind::TimedOut)));
        assert!(reconnect_after(&io::Error::from(io::ErrorKind::BrokenPipe)));
    }

    #[test]
    fn a_disk_record_reads_metadata_through_the_direct_reader() {
        // The record file is opened O_DIRECT, so a plain heap buffer is not always accepted.
        let base = std::env::var_os("SELENE_NVME_SCRATCH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("selene-forward-{}", std::process::id()));
        std::fs::create_dir_all(&base).unwrap();
        let path = base.join("record");
        let value = crate::record::Descriptor {
            identity: [7; 32],
            fingerprint: [3; 16],
            page_tokens: 64,
            ranks: vec![crate::record::RankSizes {
                planes: vec![4000],
                row_bytes: 1000,
            }],
            tenant: String::new(),
        };
        let key = Key {
            consumed_tokens: 2048 * 64,
            digest: [9; 16],
        };
        let header = crate::record::Header::new(&value, key, None, [0; 16], 2048).unwrap();
        let page_segment = header.page_segment as usize;
        let length = header.length(true) as usize;
        let mut bytes = vec![0u8; length];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = (index % 251) as u8;
        }
        {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&path).unwrap();
            file.write_all(&vec![0u8; crate::disk::ENVELOPE_BYTES as usize])
                .unwrap();
            file.write_all(&bytes).unwrap();
        }
        let disk = Arc::new(crate::disk::DiskExtent::new(path));
        let job = ForwardJob {
            namespace: [0u8; 32],
            descriptor: value,
            key,
            parent: None,
            ancestors: vec![key],
            chain_pages: 2048,
            tail_pages: 2048,
            bucket: [0; 16],
            row: true,
            record: crate::store::ForwardRecord::for_test_disk(header, disk),
        };
        let regions = LocalRegions::new(
            crate::arena::Arena::new(16 << 20).unwrap(),
            crate::arena::Arena::new(8 << 20).unwrap(),
        );
        let prepared = prepare(&job, &regions).unwrap();
        assert_eq!(prepared.header, bytes[..record::HEADER_BYTES].to_vec());
        assert_eq!(
            prepared.checksum_segment,
            bytes[record::HEADER_BYTES..page_segment].to_vec()
        );
        match &prepared.source {
            ForwardBytes::Disk { staging, .. } => {
                assert!(staging.capacity() < length);
                assert_eq!(
                    staging.capacity(),
                    crate::store::DEFAULT_MAX_WINDOW as usize
                );
            }
            _ => panic!("expected a disk source"),
        }
        drop(prepared);
        drop(job);
        let _ = std::fs::remove_dir_all(base);
    }
}
