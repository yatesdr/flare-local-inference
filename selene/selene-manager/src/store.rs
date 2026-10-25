use crate::{
    Key, Result,
    arena::{Arena, Extent, ExtentSlice},
    disk::{self, DiskExtent, RecordData, Task},
    invalid,
    local::{GrantError, Local, Region, WindowShape},
    proto,
    record::{self, Descriptor, Header, NamespaceId},
    tree::{Eviction, Tree},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

fn key_to_proto(key: &Key) -> proto::Key {
    proto::Key {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.to_vec(),
    }
}

fn entry_to_proto(entry: &EntryData) -> proto::CatalogEntry {
    proto::CatalogEntry {
        key: Some(key_to_proto(&entry.key)),
        parent: entry.parent.map(|parent| key_to_proto(&parent)),
        has_parent: entry.parent.is_some(),
        bucket: entry.bucket.to_vec(),
        tail_pages: u64::from(entry.tail_pages),
        row: entry.row,
        tier: match entry.tier {
            0 => proto::Tier::Dram as i32,
            _ => proto::Tier::Nvme as i32,
        },
        durable: entry.durable,
        bytes: entry.bytes,
        unreadable: entry.unreadable,
    }
}

/// Store connections: one per instance process, plus one catalog link per scope.
pub const MAX_CONNECTIONS: usize = 1024;
pub const OPEN_PUTS: usize = flare_selene::pool::MAX_OPEN_PUTS as usize;
pub const OPEN_GETS: usize = 4;
pub use flare_selene::pool::WINDOW_CREDITS;
pub const MAX_CHAIN: usize = flare_selene::pool::MAX_CHAIN_NODES as usize;
pub use flare_selene::pool::PROGRESS_TIMEOUT as NO_PROGRESS;

/// Whether an operation that has been idle for `idle` has stalled. A put whose client has
/// delivered everything (`delivered`) waits on this manager's own disk, whose jobs queue behind
/// each other, so it has stalled only if the disk made no progress either (`disk_idle`, `None`
/// when no job is outstanding). Otherwise the idle time alone decides.
fn stalled(idle: Duration, delivered: bool, disk_idle: Option<Duration>) -> bool {
    idle >= NO_PROGRESS && !(delivered && disk_idle.is_some_and(|disk| disk < NO_PROGRESS))
}
pub use flare_selene::pool::MAX_WINDOW_BYTES as DEFAULT_MAX_WINDOW;

/// The staging of a store built in code (an embedding or a test). The command line always sets it
/// from `--staging`, whose default is larger.
pub const EMBEDDED_STAGING: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub root: Option<PathBuf>,
    pub socket: PathBuf,
    pub memory: usize,
    pub nvme: u64,
    pub staging: usize,
    pub metrics: Option<String>,
    pub faults: disk::Faults,
    pub max_window_bytes: u64,
    pub pool: crate::pool::limits::PoolSettings,
}

impl Config {
    pub fn new(root: PathBuf, socket: PathBuf, memory: usize, nvme: u64) -> Self {
        Self {
            name: "default".into(),
            root: Some(root),
            socket,
            memory,
            nvme,
            staging: EMBEDDED_STAGING,
            metrics: None,
            faults: disk::Faults::default(),
            max_window_bytes: DEFAULT_MAX_WINDOW,
            pool: Default::default(),
        }
    }

    pub fn dram_only(socket: PathBuf, memory: usize) -> Self {
        Self {
            name: "default".into(),
            root: None,
            socket,
            memory,
            nvme: 0,
            staging: EMBEDDED_STAGING,
            metrics: None,
            faults: disk::Faults::default(),
            max_window_bytes: DEFAULT_MAX_WINDOW,
            pool: Default::default(),
        }
    }
}

/// Physical charge of a record: the 4096-byte manager envelope plus the arena
/// extent's allocator size class (or the padded record for disk-only records).
pub fn record_charge(header: &Header, in_memory: bool) -> u64 {
    let length = header.length_with_row();
    if in_memory {
        disk::ENVELOPE_BYTES + length.max(4096).next_power_of_two()
    } else {
        disk::ENVELOPE_BYTES + length
    }
}

#[derive(Clone)]
pub struct Location {
    pub header: Header,
    pub memory: Option<Arc<Extent>>,
    pub disk: Option<Arc<DiskExtent>>,
    pub durable: bool,
    pub writing: bool,
    /// The disk job currently transforming these bytes, so a stale completion is ignored.
    pub job: Option<u64>,
    pub retry: Instant,
    pub bytes: u64,
}

/// What a finished Put should do to the tree entry for its key.
enum PublishTarget {
    /// No entry exists; index a new record.
    Insert,
    /// A page-only record is present; take its place.
    Replace,
    /// A local copy won the race; the Put is a duplicate.
    Present,
    /// The existing entry describes a different record under the same key.
    Conflict,
}

impl Location {
    pub fn reading(&self) -> bool {
        self.memory
            .as_ref()
            .is_some_and(|extent| extent.references() > 1)
            || self
                .disk
                .as_ref()
                .is_some_and(|extent| extent.references() > 1)
    }
}

pub struct Namespace {
    pub directory: Option<PathBuf>,
    pub descriptor: Descriptor,
    pub tree: Tree<Location>,
    pub seq: u64,
    pub disk: Option<disk::Worker>,
    pub inflight: HashSet<Key>,
    /// Publications from an older epoch are discarded.
    pub epoch: u64,
}

impl Namespace {
    fn path(&self, key: Key) -> Option<PathBuf> {
        self.directory
            .as_ref()
            .map(|directory| directory.join(record::file_name(&key)))
    }
    fn temporary(&self, id: u64) -> Option<Arc<DiskExtent>> {
        let directory = self.directory.as_ref()?;
        let extent = Arc::new(DiskExtent::new(directory.join(format!(".write-{id}.tmp"))));
        extent.retire();
        Some(extent)
    }
    fn parent_durable(&self, parent: Option<Key>) -> bool {
        parent.is_none_or(|key| self.tree[&key].location.durable)
    }
}

#[derive(Clone, Copy)]
struct PlannedGrant {
    record_offset: u64,
    length: u64,
    pages: u32,
    row: bool,
    page_first: u64,
}

struct FilledWindows {
    granted: HashMap<u32, PlannedGrant>,
    next: usize,
    pages: Vec<Option<u64>>,
    row: Option<u64>,
    pages_filled: u32,
    row_filled: bool,
}

/// Physical resources a Put occupies, for the shape (arena or streamed) it
/// will actually get. The arena shape is one buddy-rounded record extent plus
/// the disk envelope; the streaming shape is the disk record plus one fixed
/// staging hold of two of its largest windows. `unit` keeps the largest
/// indivisible window separate so an oversized window is never mistaken for a
/// whole-record requirement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PutRequirement {
    streaming: bool,
    /// Charge against the overall budget (`charge_total`).
    charge: u64,
    /// Portion retained until the Put finishes (arena extent or disk record).
    reservation: u64,
    /// Region bytes the shape needs in total.
    allocation: u64,
    /// Largest indivisible region allocation (one window, or the extent).
    unit: u64,
}

impl PutRequirement {
    fn arena(extent: u64) -> Self {
        let charge = disk::ENVELOPE_BYTES + extent;
        Self {
            streaming: false,
            charge,
            reservation: charge,
            allocation: extent,
            unit: extent,
        }
    }

    /// A streamed Put reserves one fixed hold of two of its largest windows.
    /// Live windows are slices of that hold, so the staging headroom the
    /// transfer will need cannot be taken by a concurrent operation.
    fn streaming(disk: u64, unit: u64) -> Self {
        let allocation = unit.saturating_mul(2);
        Self {
            streaming: true,
            charge: disk.saturating_add(allocation),
            reservation: disk,
            allocation,
            unit,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct HoldSlot {
    index: u32,
    /// Chunk write still reading this slot; the slot is reusable once it
    /// completes.
    job: Option<u64>,
}

fn set_slot_job(put: &mut Put, index: u32, job: u64) {
    for slot in put.hold_slots.iter_mut().flatten() {
        if slot.index == index {
            slot.job = Some(job);
        }
    }
}

fn release_slot_job(put: &mut Put, job: u64) {
    for slot in &mut put.hold_slots {
        if slot.is_some_and(|slot| slot.job == Some(job)) {
            *slot = None;
        }
    }
}

enum RootChoice {
    Store(Extent),
    Wait,
}

pub struct Put {
    namespace: NamespaceId,
    epoch: u64,
    header: Header,
    first_page: u32,
    grants: Vec<PlannedGrant>,
    windows: FilledWindows,
    memory: Option<Arc<Extent>>,
    temporary: Option<Arc<DiskExtent>>,
    streaming: bool,
    commit_bytes: Vec<u8>,
    metadata: Option<Arc<Vec<u8>>>,
    committed: bool,
    finalizing: bool,
    cancelled: bool,
    terminal: bool,
    finished: bool,
    pending: usize,
    blocked: VecDeque<(u64, u64, ExtentSlice)>,
    pins: VecDeque<Key>,
    reservation: u64,
    requirement: PutRequirement,
    /// Fixed staging reservation for a streamed Put. Live windows are
    /// slices of it, so concurrent operations cannot consume the headroom
    /// the transfer still needs.
    staging_hold: Option<Arc<Extent>>,
    /// Slot size inside `staging_hold`.
    hold_unit: usize,
    /// Window occupying each hold slot, and the write draining it.
    hold_slots: [Option<HoldSlot>; 2],
    events: tokio::sync::mpsc::Sender<Result<PutAnswer>>,
    outbox: VecDeque<Result<PutAnswer>>,
}

impl Put {
    fn owns_payload(&self) -> bool {
        self.windows.pages_filled as usize == self.header.tail_pages as usize
            && (self.windows.row_filled || self.header.row_record_bytes() == 0)
    }
}

#[derive(Clone, Copy)]
struct PlannedGetGrant {
    record: u32,
    record_offset: u64,
    length: u64,
    pages: u32,
    row: bool,
    page_first: u64,
}

struct GetRecord {
    header: Header,
    row: bool,
    memory: Option<Arc<Extent>>,
    file: Option<Arc<File>>,
    meta: Option<Vec<u8>>,
    meta_sent: bool,
}

pub struct Get {
    namespace: NamespaceId,
    pins: VecDeque<Key>,
    records: Vec<GetRecord>,
    grants: Vec<PlannedGetGrant>,
    next_grant: usize,
    next_meta: usize,
    outstanding: HashMap<u32, PlannedGetGrant>,
    released: HashSet<u32>,
    ready_windows: VecDeque<(u32, PlannedGetGrant, Arc<Extent>)>,
    window_reads: usize,
    pending_reads: usize,
    cancelled: bool,
    terminal: bool,
    finished: bool,
    leased_sent: bool,
    events: tokio::sync::mpsc::Sender<Result<GetAnswer>>,
    outbox: VecDeque<Result<GetAnswer>>,
}

pub enum Operation {
    Put(Box<Put>),
    Get(Box<Get>),
}

pub struct Open {
    pub operation: Operation,
    pub progress: Instant,
}

pub struct Session {
    pub generation: u64,
    pub namespace: Option<NamespaceId>,
    pub ops: HashMap<u64, Open>,
    /// A manager-to-manager session. It has no local pidfd and may bind its namespace on first
    /// use (Subscribe, PutBegin or GetBegin) instead of at Hello.
    pub remote: bool,
}

const SNAPSHOT_BUFFER: usize = 65_536;

#[derive(Clone)]
enum LiveEvent {
    Catalog {
        seq: u64,
        kind: u8,
        key: Key,
        entry: Option<EntryData>,
    },
}

struct Subscription {
    namespace: NamespaceId,
    sender: tokio::sync::mpsc::Sender<proto::CatalogEvent>,
    sequence: u64,
    seq0: u64,
    snapshot: Option<VecDeque<EntryData>>,
    begin_pending: bool,
    buffered: VecDeque<LiveEvent>,
    restarts: usize,
}

enum ReadKind {
    Meta { length: usize },
    Fill,
    Window { grant: u32, length: usize },
}

enum Job {
    Write {
        namespace: NamespaceId,
        key: Key,
    },
    Chunk {
        connection: u64,
        transfer: u64,
    },
    Finish {
        connection: u64,
        transfer: u64,
        final_extent: Arc<DiskExtent>,
    },
    Compact {
        namespace: NamespaceId,
        key: Key,
        bytes: u64,
    },
    GetRead {
        connection: u64,
        transfer: u64,
        record: u32,
        kind: ReadKind,
    },
}

#[derive(Default)]
pub struct Metrics {
    pub dropped: u64,
    pub cancels: u64,
    pub puts_ok: u64,
    pub puts_error: u64,
    pub present: u64,
    pub retry_queue: u64,
    pub retry_budget: u64,
    pub retry_inflight: u64,
    pub oversize: u64,
    pub gets_ok: u64,
    pub gets_error: u64,
    pub evictions: u64,
    pub compactions: u64,
    pub nvme_bytes: u64,
    pub nvme_seconds: f64,
    pub nvme_errors: u64,
    pub hits: HashMap<(String, &'static str), u64>,
}

#[derive(Clone, Debug)]
pub struct EntryData {
    pub key: Key,
    pub parent: Option<Key>,
    pub bucket: [u8; 16],
    pub tail_pages: u32,
    pub row: bool,
    /// 0 = DRAM, 1 = NVMe.
    pub tier: u8,
    pub durable: bool,
    pub bytes: u64,
    /// A shape anchor without a readable local or remote source.
    pub unreadable: bool,
}

pub enum StoreEvent {
    Catalog {
        namespace: NamespaceId,
        seq: u64,
        kind: u8,
        key: Key,
        entry: Option<EntryData>,
    },
}

struct PutDrain {
    connection: u64,
    transfer: u64,
    namespace: NamespaceId,
    key: Key,
    pins: VecDeque<Key>,
    pending: usize,
    reservation: u64,
    _temporary: Option<Arc<DiskExtent>>,
    _staging_hold: Option<Arc<Extent>>,
}

struct GetDrain {
    connection: u64,
    transfer: u64,
    namespace: NamespaceId,
    pins: VecDeque<Key>,
    pending: usize,
}

struct PendingPut {
    connection: u64,
    transfer: u64,
    request: PutRequest,
    events: tokio::sync::mpsc::Sender<Result<PutAnswer>>,
}

struct PendingGet {
    connection: u64,
    transfer: u64,
    request: GetRequest,
    events: tokio::sync::mpsc::Sender<Result<GetAnswer>>,
}

pub struct PutRequest {
    pub namespace: NamespaceId,
    pub key: Key,
    pub published_parent: Option<Key>,
    pub bucket: [u8; 16],
    pub chain_pages: u32,
    pub published_tail_pages: u32,
    pub row: bool,
}

pub struct GetRequest {
    pub namespace: NamespaceId,
    pub keys: Vec<Key>,
    pub skip_pages: u32,
    pub expected_pages: u32,
    pub row: bool,
}

pub struct Store {
    config: Config,
    local: Local,
    memory: Arena,
    staging: Arena,
    lock: Option<Arc<disk::RootLock>>,
    namespaces: HashMap<NamespaceId, Namespace>,
    sessions: HashMap<u64, Session>,
    jobs: HashMap<u64, Job>,
    job_counter: u64,
    /// The last time any disk job finished, or the last tick with none outstanding.
    disk_progress: Instant,
    clock: u64,
    boot: u64,
    counter: u64,
    metrics: Metrics,
    charged: u64,
    /// The highest `charge_total` seen at an admission or a tick.
    reserved_peak: u64,
    deferred_disk: Vec<Arc<DiskExtent>>,
    retired_puts: Vec<PutDrain>,
    retired_gets: Vec<GetDrain>,
    retired_records: Vec<(NamespaceId, Key, Location)>,
    events: Vec<StoreEvent>,
    pending_puts: HashMap<u64, VecDeque<PendingPut>>,
    pending_gets: HashMap<u64, VecDeque<PendingGet>>,
    put_order: VecDeque<u64>,
    get_order: VecDeque<u64>,
    subscriptions: HashMap<(u64, NamespaceId), Subscription>,
    next_transfer: u64,
    read_backlog: VecDeque<(NamespaceId, u64, Task)>,
    detached_puts: HashMap<(u64, u64), Put>,
}

/// Names the startup step and path an error came from.
fn step_error(step: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{step} at {}: {error}", path.display()),
    )
}

impl Store {
    pub fn open(config: Config) -> Result<Self> {
        if config.memory < 4096 || config.staging < 8 * 1024 * 1024 {
            return Err(invalid("memory >= 4096 and staging >= 8 MiB required"));
        }
        if config.root.is_some() && config.nvme < 8192 {
            return Err(invalid("nvme >= 8192 required with --root"));
        }
        if config.root.is_none() && config.nvme != 0 {
            return Err(invalid("nvme must be 0 without --root"));
        }
        let lock = config
            .root
            .as_ref()
            .map(|root| {
                disk::RootLock::acquire(root)
                    .map(Arc::new)
                    .map_err(|error| step_error("lock the root", root, error))
            })
            .transpose()?;
        let recovered = match (&config.root, &lock) {
            (Some(root), Some(_)) => {
                disk::recover(root).map_err(|error| step_error("recover", root, error))?
            }
            _ => Vec::new(),
        };
        let local = Local::open(
            &config.name,
            config.memory,
            config.staging,
            config.max_window_bytes,
        )
        .map_err(|error| {
            step_error(
                "open shared memory in",
                Path::new(crate::local::SHM_ROOT),
                error,
            )
        })?;
        let memory = local.arena();
        let staging = local.staging();
        let mut nonce = [0; 8];
        File::open("/dev/urandom")?.read_exact(&mut nonce)?;
        let mut store = Self {
            config: config.clone(),
            local,
            memory,
            staging,
            lock,
            namespaces: HashMap::new(),
            sessions: HashMap::new(),
            jobs: HashMap::new(),
            job_counter: 0,
            disk_progress: Instant::now(),
            clock: 0,
            boot: u64::from_le_bytes(nonce),
            counter: 0,
            metrics: Metrics::default(),
            charged: 0,
            reserved_peak: 0,
            deferred_disk: Vec::new(),
            retired_puts: Vec::new(),
            retired_gets: Vec::new(),
            retired_records: Vec::new(),
            events: Vec::new(),
            pending_puts: HashMap::new(),
            pending_gets: HashMap::new(),
            put_order: VecDeque::new(),
            get_order: VecDeque::new(),
            subscriptions: HashMap::new(),
            next_transfer: 1,
            read_backlog: VecDeque::new(),
            detached_puts: HashMap::new(),
        };
        let mut recovered_age = Vec::new();
        for recovered in recovered {
            let mut namespace = store.new_namespace(recovered.descriptor);
            let id = namespace.descriptor.namespace_id();
            for (header, row, extent) in recovered.records {
                let modified = fs::metadata(&extent.path)?
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                recovered_age.push((modified, id, header.key));
                let bytes = record_charge(&header, false);
                namespace.tree.insert(
                    header.key,
                    header.parent,
                    header.bucket,
                    header.tail_pages as usize,
                    row,
                    Location {
                        header,
                        memory: None,
                        disk: Some(extent),
                        durable: false,
                        writing: false,
                        job: None,
                        retry: Instant::now(),
                        bytes,
                    },
                )?;
                store.charged += bytes;
            }
            store.namespaces.insert(id, namespace);
            store.refresh_durability(id);
        }
        recovered_age.sort_unstable();
        for (_, id, key) in recovered_age {
            store.clock += 1;
            let clock = store.clock;
            store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .access_at(&key, clock);
        }
        Ok(store)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }
    /// What the pool service needs from the store: the shared region files, a handle on the
    /// staging region, and how many records recovery found.
    pub fn pool_view(&self) -> (crate::proto::v2::SharedMemory, crate::arena::Arena, u64) {
        let records = self
            .namespaces
            .values()
            .map(|namespace| namespace.tree.len() as u64)
            .sum();
        (
            crate::proto::v2::SharedMemory {
                arena_path: self.local.arena_path().display().to_string(),
                arena_bytes: self.local.arena_size(),
                staging_path: self.local.staging_path().display().to_string(),
                staging_bytes: self.local.staging_size(),
                regions_generation: self.local.regions_generation(),
            },
            self.local.staging(),
            records,
        )
    }

    pub fn shm_directory(&self) -> PathBuf {
        self.local.directory().to_path_buf()
    }
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }
    pub fn next_transfer(&mut self) -> u64 {
        let transfer = self.next_transfer;
        self.next_transfer += 1;
        transfer
    }

    fn new_namespace(&self, descriptor: Descriptor) -> Namespace {
        let directory = self
            .config
            .root
            .as_ref()
            .map(|root| root.join(descriptor.name()));
        let disk = match (&directory, &self.lock) {
            (Some(_), Some(lock)) => Some(disk::Worker::start(
                self.config.faults.clone(),
                lock.clone(),
            )),
            _ => None,
        };
        Namespace {
            directory,
            tree: Tree::new(descriptor.page_tokens),
            descriptor,
            seq: 0,
            disk,
            inflight: HashSet::new(),
            epoch: 0,
        }
    }

    pub fn register_namespace(&mut self, descriptor: Descriptor) -> Result<NamespaceId> {
        descriptor.validate()?;
        let id = descriptor.namespace_id();
        if let Some(stored) = self.namespaces.get(&id) {
            if stored.descriptor != descriptor {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "namespace descriptor mismatch",
                ));
            }
            return Ok(id);
        }
        if let Some(root) = &self.config.root {
            disk::descriptor(root, &descriptor)?;
        }
        let namespace = self.new_namespace(descriptor);
        self.namespaces.insert(id, namespace);
        Ok(id)
    }

    pub fn namespace_id(&self) -> Option<NamespaceId> {
        self.namespaces.keys().next().copied()
    }

    pub fn session(&self, connection: u64) -> Option<&Session> {
        self.sessions.get(&connection)
    }
    pub fn hello(
        &mut self,
        connection: u64,
        replica: String,
        pid: u32,
        descriptor: Descriptor,
    ) -> Result<u64> {
        if !self.sessions.contains_key(&connection) && self.sessions.len() >= MAX_CONNECTIONS {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "connection limit reached",
            ));
        }
        if replica.is_empty() || replica.len() > 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "replica name must be 1..=256 bytes",
            ));
        }
        let id = self.register_namespace(descriptor)?;
        self.local.accept(connection, pid)?;
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| invalid("connection counter exhausted"))?;
        let generation = self.boot ^ self.counter;
        self.sessions.insert(
            connection,
            Session {
                generation,
                namespace: Some(id),
                ops: HashMap::new(),
                remote: false,
            },
        );
        Ok(generation)
    }

    /// Register the store connection of an instance on another host. The namespace is bound by
    /// the first Subscribe or PutBegin, because `RemoteHello` carries no descriptor.
    pub fn remote_hello(&mut self, connection: u64, node: &str) -> Result<u64> {
        if node.is_empty() || node.len() > 256 {
            return Err(invalid("remote node name must be 1..=256 bytes"));
        }
        if self.sessions.contains_key(&connection) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "connection already greeted",
            ));
        }
        self.local.accept_remote(connection)?;
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| invalid("connection counter exhausted"))?;
        let generation = self.boot ^ self.counter;
        self.sessions.insert(
            connection,
            Session {
                generation,
                namespace: None,
                ops: HashMap::new(),
                remote: true,
            },
        );
        Ok(generation)
    }

    /// The arena and staging mappings, for the data channels.
    pub fn regions(&self) -> crate::regions::LocalRegions {
        crate::regions::LocalRegions::new(self.memory.clone(), self.staging.clone())
    }

    /// Removes the evictable node `key` and the rowless ancestors the tree cascades with it,
    /// releasing the charge of every node that owned local bytes and retiring its disk file.
    /// This is the only caller of the tree's cascading removal; `remove_node` is the
    /// non-cascading one.
    fn evict_accounted(&mut self, id: NamespaceId, key: Key) -> bool {
        let eligible = self
            .namespaces
            .get(&id)
            .and_then(|namespace| namespace.tree.node(&key))
            .is_some_and(|node| node.children() == 0 && !node.pinned());
        if !eligible {
            return false;
        }
        let removed = self
            .namespaces
            .get_mut(&id)
            .and_then(|namespace| namespace.tree.evict(true, |node| node.key == key));
        let Some(Eviction::Leaves(nodes)) = removed else {
            return false;
        };
        for node in nodes {
            if node.location.memory.is_some() || node.location.disk.is_some() {
                self.charged = self.charged.saturating_sub(node.location.bytes);
            }
            if let Some(disk) = &node.location.disk {
                disk.retire();
                self.deferred_disk.push(disk.clone());
            }
            self.metrics.evictions += 1;
            self.delta(id, 2, node.key);
        }
        true
    }

    pub fn snapshot(&self, id: NamespaceId) -> Option<(u64, Vec<EntryData>)> {
        let namespace = self.namespaces.get(&id)?;
        let mut keys: Vec<Key> = namespace.tree.nodes().map(|node| node.key).collect();
        keys.sort_unstable();
        let entries = keys
            .into_iter()
            .map(|key| Self::entry(namespace, key))
            .collect();
        Some((namespace.seq, entries))
    }

    fn entry(namespace: &Namespace, key: Key) -> EntryData {
        let node = &namespace.tree[&key];
        EntryData {
            key,
            parent: node.parent,
            bucket: node.bucket,
            tail_pages: node.tail_pages as u32,
            row: node.row,
            tier: u8::from(node.location.memory.is_none()),
            durable: node.location.durable,
            bytes: node.location.bytes,
            unreadable: node.location.memory.is_none() && node.location.disk.is_none(),
        }
    }

    fn delta(&mut self, id: NamespaceId, kind: u8, key: Key) {
        let namespace = self.namespaces.get_mut(&id).unwrap();
        namespace.seq += 1;
        let seq = namespace.seq;
        let entry = (kind != 2).then(|| Self::entry(namespace, key));
        self.events.push(StoreEvent::Catalog {
            namespace: id,
            seq,
            kind,
            key,
            entry,
        });
    }

    /// Recomputes chain-closed durability parents-first, emitting `changed` on flips.
    fn refresh_durability(&mut self, id: NamespaceId) {
        let namespace = &self.namespaces[&id];
        let mut keys: Vec<Key> = namespace.tree.nodes().map(|node| node.key).collect();
        keys.sort_unstable_by_key(|key| key.consumed_tokens);
        let mut flipped = Vec::new();
        for key in keys {
            let parent_ok = self.namespaces[&id].tree[&key]
                .parent
                .is_none_or(|parent| self.namespaces[&id].tree[&parent].location.durable);
            let node = &mut self.namespaces.get_mut(&id).unwrap().tree[&key];
            let durable = node.location.disk.is_some() && parent_ok;
            if node.location.durable != durable {
                node.location.durable = durable;
                flipped.push((key, durable));
            }
        }
        for (key, _) in flipped {
            self.delta(id, 1, key);
        }
    }

    /// What the NVMe limit (`--nvme`) bounds: the durable reservations (every record's
    /// envelope and file, or its arena extent and envelope, and what an unfinished Put will
    /// leave on disk) plus the staging hold of each unfinished streamed Put, which stands for
    /// the disk record in flight. Windows the pool leases from the staging region for claims
    /// and exports are not in it: they are bounded by `--staging`, and never become records.
    fn charge_total(&self) -> u64 {
        self.charged.saturating_add(self.put_staging_held())
    }

    /// Staging held by unfinished streamed Puts, those still on a connection, detached from
    /// it, or draining their last chunk writes.
    fn put_staging_held(&self) -> u64 {
        fn held(hold: &Option<Arc<Extent>>) -> u64 {
            hold.as_ref().map_or(0, |hold| hold.capacity() as u64)
        }
        let mut total = 0u64;
        for session in self.sessions.values() {
            for open in session.ops.values() {
                if let Operation::Put(put) = &open.operation {
                    total = total.saturating_add(held(&put.staging_hold));
                }
            }
        }
        for put in self.detached_puts.values() {
            total = total.saturating_add(held(&put.staging_hold));
        }
        for drain in &self.retired_puts {
            total = total.saturating_add(held(&drain._staging_hold));
        }
        total
    }

    /// Remember the most the limit's measure has been.
    fn note_reserved(&mut self) {
        let now = self.charge_total();
        self.reserved_peak = self.reserved_peak.max(now);
    }

    pub fn submit_put(
        &mut self,
        connection: u64,
        transfer: u64,
        request: PutRequest,
        events: tokio::sync::mpsc::Sender<Result<PutAnswer>>,
    ) {
        let open = self
            .sessions
            .get(&connection)
            .map(Self::put_count)
            .unwrap_or(0);
        let queued = self
            .pending_puts
            .get(&connection)
            .map_or(0, |queue| queue.len());
        if open + queued >= OPEN_PUTS {
            self.metrics.retry_queue += 1;
            let _ = events.try_send(Ok(PutAnswer::Refused(Refused {
                reason: proto::Reason::Queue as u8,
                detail: "open Put limit".into(),
            })));
            return;
        }
        let queue = self.pending_puts.entry(connection).or_default();
        let was_empty = queue.is_empty();
        queue.push_back(PendingPut {
            connection,
            transfer,
            request,
            events,
        });
        if was_empty {
            self.put_order.push_back(connection);
        }
    }

    pub fn submit_get(
        &mut self,
        connection: u64,
        transfer: u64,
        request: GetRequest,
        events: tokio::sync::mpsc::Sender<Result<GetAnswer>>,
    ) {
        let open = self
            .sessions
            .get(&connection)
            .map(Self::get_count)
            .unwrap_or(0);
        let queued = self
            .pending_gets
            .get(&connection)
            .map_or(0, |queue| queue.len());
        if open + queued >= OPEN_GETS {
            self.metrics.gets_error += 1;
            let _ = events.try_send(Ok(GetAnswer::Refused(Refused {
                reason: proto::Reason::Invalid as u8,
                detail: "open Get limit".into(),
            })));
            return;
        }
        let queue = self.pending_gets.entry(connection).or_default();
        let was_empty = queue.is_empty();
        queue.push_back(PendingGet {
            connection,
            transfer,
            request,
            events,
        });
        if was_empty {
            self.get_order.push_back(connection);
        }
    }

    fn put_count(session: &Session) -> usize {
        session
            .ops
            .values()
            .filter(|open| matches!(open.operation, Operation::Put(_)))
            .count()
    }
    fn get_count(session: &Session) -> usize {
        session
            .ops
            .values()
            .filter(|open| matches!(open.operation, Operation::Get(_)))
            .count()
    }

    fn pop_get(&mut self) -> Option<PendingGet> {
        while let Some(connection) = self.get_order.pop_front() {
            let Some(queue) = self.pending_gets.get_mut(&connection) else {
                continue;
            };
            let Some(pending) = queue.pop_front() else {
                self.pending_gets.remove(&connection);
                continue;
            };
            if queue.is_empty() {
                self.pending_gets.remove(&connection);
            } else {
                self.get_order.push_back(connection);
            }
            return Some(pending);
        }
        None
    }

    fn pop_put(&mut self) -> Option<PendingPut> {
        while let Some(connection) = self.put_order.pop_front() {
            let Some(queue) = self.pending_puts.get_mut(&connection) else {
                continue;
            };
            let Some(pending) = queue.pop_front() else {
                self.pending_puts.remove(&connection);
                continue;
            };
            if queue.is_empty() {
                self.pending_puts.remove(&connection);
            } else {
                self.put_order.push_back(connection);
            }
            return Some(pending);
        }
        None
    }

    #[cfg(test)]
    fn dispatch_trace(&mut self) -> Vec<&'static str> {
        let mut trace = Vec::new();
        let mut gets = 0;
        for _ in 0..4 {
            let Some(pending) = self.pop_get() else {
                break;
            };
            gets += 1;
            self.begin_get(pending);
        }
        trace.extend(std::iter::repeat_n("get", gets));
        if let Some(pending) = self.pop_put() {
            trace.push("put");
            self.begin_put(pending);
        }
        trace
    }

    fn dispatch(&mut self) {
        for _ in 0..4 {
            let Some(pending) = self.pop_get() else {
                break;
            };
            self.begin_get(pending);
        }
        if let Some(pending) = self.pop_put() {
            self.begin_put(pending);
        }
    }

    // ---------------------------------------------------------------- Put begin

    fn reject_put(&mut self, events: &tokio::sync::mpsc::Sender<Result<PutAnswer>>, reason: u8) {
        self.reject_put_detail(events, reason, &format!("reason {reason}"));
    }

    fn reject_put_detail(
        &mut self,
        events: &tokio::sync::mpsc::Sender<Result<PutAnswer>>,
        reason: u8,
        detail: &str,
    ) {
        let _ = events.try_send(Ok(PutAnswer::Refused(Refused {
            reason,
            detail: detail.to_string(),
        })));
    }

    /// A transfer id is unusable while any operation, queued request or drain with that
    /// (connection, transfer) is still alive, in either direction.
    fn transfer_in_use(&self, connection: u64, transfer: u64) -> bool {
        if self
            .sessions
            .get(&connection)
            .is_some_and(|session| session.ops.contains_key(&transfer))
        {
            return true;
        }
        if self
            .pending_puts
            .get(&connection)
            .is_some_and(|queue| queue.iter().any(|pending| pending.transfer == transfer))
            || self
                .pending_gets
                .get(&connection)
                .is_some_and(|queue| queue.iter().any(|pending| pending.transfer == transfer))
        {
            return true;
        }
        self.retired_puts
            .iter()
            .any(|drain| drain.connection == connection && drain.transfer == transfer)
            || self
                .retired_gets
                .iter()
                .any(|drain| drain.connection == connection && drain.transfer == transfer)
    }

    fn begin_put(&mut self, pending: PendingPut) {
        let PendingPut {
            connection,
            transfer,
            request,
            events,
        } = pending;
        let Some(id) = self.sessions.get(&connection).map(|_| request.namespace) else {
            return;
        };
        let Some(descriptor) = self
            .namespaces
            .get(&id)
            .map(|namespace| namespace.descriptor.clone())
        else {
            self.reject_put(&events, proto::Reason::Invalid as u8);
            return;
        };
        let invalid_geometry = request.key.consumed_tokens == 0
            || request.key.consumed_tokens / descriptor.page_tokens
                != u64::from(request.chain_pages)
            || request.published_tail_pages > request.chain_pages
            || (request.published_parent.is_none()
                && request.published_tail_pages != request.chain_pages)
            || request.published_parent.is_some_and(|parent| {
                parent.consumed_tokens >= request.key.consumed_tokens
                    || request.key.consumed_tokens / descriptor.page_tokens
                        - parent.consumed_tokens / descriptor.page_tokens
                        != u64::from(request.published_tail_pages)
            });
        if invalid_geometry {
            self.metrics.puts_error += 1;
            self.reject_put(&events, proto::Reason::Invalid as u8);
            return;
        }
        let initial_old = self.namespaces[&id].tree.node(&request.key).cloned();
        if self.namespaces[&id].inflight.contains(&request.key)
            || initial_old
                .as_ref()
                .is_some_and(|node| !node.row && node.pinned())
        {
            self.metrics.retry_inflight += 1;
            self.reject_put(&events, proto::Reason::Inflight as u8);
            return;
        }
        if initial_old.as_ref().is_some_and(|node| {
            node.row && (node.location.memory.is_some() || node.location.disk.is_some())
        }) {
            self.metrics.present += 1;
            self.reject_put(&events, proto::Reason::Present as u8);
            return;
        }
        if self.sessions[&connection].remote
            && request.published_parent.is_some_and(|parent| {
                !self.namespaces[&id]
                    .tree
                    .node(&parent)
                    .is_some_and(|node| node.row)
            })
        {
            // A remote session names the exact ancestor its record continues, and the writer
            // stores that ancestor first. A local Put only hints at its parent and falls back
            // to a root below when the hint is unusable.
            self.metrics.puts_error += 1;
            self.reject_put_detail(
                &events,
                proto::Reason::Invalid as u8,
                "published parent is not stored",
            );
            return;
        }
        if Self::put_count(&self.sessions[&connection]) >= OPEN_PUTS {
            self.metrics.retry_queue += 1;
            self.reject_put(&events, proto::Reason::Queue as u8);
            return;
        }
        let initial_bucket = match &initial_old {
            Some(node) => {
                if node.bucket != request.bucket {
                    self.reject_put(&events, proto::Reason::Invalid as u8);
                    return;
                }
                node.bucket
            }
            None => request.bucket,
        };
        let initial_parent = match &initial_old {
            Some(node) => node.parent,
            None => request.published_parent.filter(|parent| {
                self.namespaces[&id]
                    .tree
                    .node(parent)
                    .is_some_and(|node| node.row)
            }),
        };
        let initial_first =
            initial_parent.map_or(0, |parent| parent.consumed_tokens / descriptor.page_tokens);
        let Some(initial_tail) = request
            .chain_pages
            .checked_sub(u32::try_from(initial_first).unwrap_or(u32::MAX))
        else {
            self.reject_put(&events, proto::Reason::Invalid as u8);
            return;
        };
        let child_header = match crate::record::new_header(
            &descriptor,
            request.key,
            initial_parent,
            initial_bucket,
            initial_tail,
        ) {
            Ok(header) => header,
            Err(_) => {
                self.reject_put(&events, proto::Reason::Invalid as u8);
                return;
            }
        };
        let root_header = match crate::record::new_header(
            &descriptor,
            request.key,
            None,
            initial_bucket,
            request.chain_pages,
        ) {
            Ok(header) => header,
            Err(_) => {
                self.reject_put(&events, proto::Reason::Invalid as u8);
                return;
            }
        };
        let rootless = self.config.root.is_none();
        let replacement = initial_old.is_some();
        let arena_bytes = self.memory.capacity() as u64;
        // The arena rounds every record extent up to a power of two. Without a
        // root there is no disk to stream to, so a record whose own extent
        // cannot fit the arena can never be stored. Whether it fits beside
        // what else must stay resident is a capacity question.
        if rootless && child_header.length_with_row().max(4096).next_power_of_two() > arena_bytes {
            self.metrics.oversize += 1;
            self.reject_put(&events, proto::Reason::Oversize as u8);
            return;
        }
        let selected_pins: Vec<Key> = initial_parent
            .into_iter()
            .chain(replacement.then_some(request.key))
            .collect();
        // Admission charges the durable budget with the shape's full
        // reservation: the envelope plus the rounded extent for a record the
        // arena keeps, and for a streamed record the envelope, the record and
        // its mandatory two-window staging hold. Only the record's own charge
        // can make it OVERSIZE; a shortage caused by what else must stay
        // resident is a Budget refusal.
        let extent = child_header.length_with_row().max(4096).next_power_of_two();
        let charge = if extent <= arena_bytes {
            disk::ENVELOPE_BYTES.saturating_add(extent)
        } else {
            self.put_requirement(&child_header, 0).charge
        };
        if !rootless && charge > self.config.nvme {
            self.metrics.oversize += 1;
            self.reject_put(&events, proto::Reason::Oversize as u8);
            return;
        }
        // Pick the shape by asking the arena allocator. A failed allocation
        // falls back to streaming; a rootless manager instead tries to evict
        // the parent and store the root. `memory` holds the extent the shape
        // was admitted with.
        let selected = self.put_requirement(&child_header, arena_bytes);
        let mut memory: Option<Arc<Extent>> = None;
        let (header, parent, pins, requirement): (Header, Option<Key>, Vec<Key>, PutRequirement) =
            if !selected.streaming {
                match self.memory_room_excluding(
                    child_header.length_with_row() as usize,
                    !rootless,
                    &selected_pins,
                ) {
                    Some(extent) => {
                        let requirement = PutRequirement::arena(extent.capacity() as u64);
                        memory = Some(Arc::new(extent));
                        (child_header, initial_parent, selected_pins, requirement)
                    }
                    None if rootless => {
                        let root_pins: Vec<Key> =
                            replacement.then_some(request.key).into_iter().collect();
                        match self.rootless_fallback(
                            &root_header,
                            root_pins,
                            arena_bytes,
                        ) {
                            RootChoice::Store(extent) => {
                                let requirement = PutRequirement::arena(extent.capacity() as u64);
                                memory = Some(Arc::new(extent));
                                let root_pins: Vec<Key> =
                                    replacement.then_some(request.key).into_iter().collect();
                                (root_header, None, root_pins, requirement)
                            }
                            RootChoice::Wait => {
                                self.metrics.retry_budget += 1;
                                self.reject_put(&events, proto::Reason::Budget as u8);
                                return;
                            }
                        }
                    }
                    None => {
                        let requirement = self.put_requirement(&child_header, 0);
                        (child_header, initial_parent, selected_pins, requirement)
                    }
                }
            } else {
                let requirement = self.put_requirement(&child_header, 0);
                (child_header, initial_parent, selected_pins, requirement)
            };
        let length = header.length_with_row();
        // A streamed record reserves two windows in staging. When that hold
        // cannot fit the whole region it can never be admitted: if the record
        // has no arena shape either, refuse it as OVERSIZE. A record that still
        // fits an arena extent can be tried again after space is freed.
        if requirement.streaming && requirement.allocation > self.staging.capacity() as u64 {
            if selected.streaming {
                self.metrics.oversize += 1;
                self.reject_put(&events, proto::Reason::Oversize as u8);
                return;
            }
            self.metrics.retry_budget += 1;
            self.reject_put(&events, proto::Reason::Budget as u8);
            return;
        }
        for key in &pins {
            if self.namespaces.get_mut(&id).unwrap().tree.pin(key).is_err() {
                for key in &pins {
                    let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
                }
                self.reject_put(&events, proto::Reason::Invalid as u8);
                return;
            }
        }
        if !self.make_room_with(Some(id), requirement.charge, &pins) {
            drop(memory);
            for key in &pins {
                let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
            }
            self.metrics.retry_budget += 1;
            self.reject_put(&events, proto::Reason::Budget as u8);
            return;
        }
        let staging_hold = if requirement.streaming {
            match self.staging.allocate(requirement.allocation as usize) {
                Some(extent) => Some(Arc::new(extent)),
                None => {
                    drop(memory);
                    for key in &pins {
                        let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
                    }
                    self.metrics.retry_budget += 1;
                    self.reject_put(&events, proto::Reason::Budget as u8);
                    return;
                }
            }
        } else {
            None
        };
        let temporary = if requirement.streaming {
            self.job_counter += 1;
            let job = self.job_counter;
            match self.namespaces[&id].temporary(job) {
                Some(extent) => Some(extent),
                None => {
                    drop(memory);
                    drop(staging_hold);
                    for key in &pins {
                        let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
                    }
                    self.reject_put(&events, proto::Reason::Io as u8);
                    return;
                }
            }
        } else {
            None
        };
        self.charged += requirement.reservation;
        // The hold is allocated, and counts from here.
        let held_now = staging_hold
            .as_ref()
            .map_or(0, |hold| hold.capacity() as u64);
        self.reserved_peak = self
            .reserved_peak
            .max(self.charge_total().saturating_add(held_now));
        let first =
            parent.map_or(0, |parent| parent.consumed_tokens / descriptor.page_tokens) as u32;
        let grants = plan_put_grants(&header, first, self.config.max_window_bytes);
        let pages = vec![None; header.tail_pages as usize];
        let epoch = self.namespaces[&id].epoch;
        let mut put = Put {
            namespace: id,
            epoch,
            header: header.clone(),
            first_page: first,
            grants,
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages,
                row: None,
                pages_filled: 0,
                row_filled: header.row_record_bytes() == 0,
            },
            memory,
            temporary,
            streaming: requirement.streaming,
            commit_bytes: Vec::new(),
            metadata: None,
            committed: false,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: false,
            pending: 0,
            blocked: VecDeque::new(),
            pins: pins.into(),
            reservation: requirement.reservation,
            requirement,
            staging_hold,
            hold_unit: requirement.unit as usize,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        let namespace = self.namespaces.get_mut(&id).unwrap();
        namespace.inflight.insert(request.key);
        put.outbox.push_back(Ok(PutAnswer::Accept(PutAccept {
            transfer,
            first_page: first,
            parent,
            record_bytes: length,
        })));
        self.sessions.get_mut(&connection).unwrap().ops.insert(
            transfer,
            Open {
                progress: Instant::now(),
                operation: Operation::Put(Box::new(put)),
            },
        );
    }

    fn oldest(
        &self,
        eligible: impl Fn(&crate::tree::Node<Location>) -> bool,
    ) -> Option<(NamespaceId, Key)> {
        self.namespaces
            .iter()
            .flat_map(|(id, namespace)| {
                namespace
                    .tree
                    .nodes()
                    .filter(|node| {
                        !node.pinned()
                            && !node.location.writing
                            && !node.location.reading()
                            && eligible(node)
                    })
                    .map(move |node| (node.accessed(), node.key, *id))
            })
            .min_by_key(|(accessed, key, _)| (*accessed, key.consumed_tokens))
            .map(|(_, key, id)| (id, key))
    }

    fn memory_room(&mut self, bytes: usize, require_durable: bool) -> Option<Extent> {
        self.memory_room_excluding(bytes, require_durable, &[])
    }

    /// Attempt the arena allocation, evicting eligible leaves one at a time
    /// until it succeeds. Asking the allocator rather than summing free bytes
    /// handles buddy fragmentation and chains whose parent only becomes a
    /// leaf after its child is evicted. `excluded` keys must survive.
    fn memory_room_excluding(
        &mut self,
        bytes: usize,
        require_durable: bool,
        excluded: &[Key],
    ) -> Option<Extent> {
        loop {
            if let Some(extent) = self.memory.allocate(bytes) {
                return Some(extent);
            }
            let (id, key) = self.oldest(|node| {
                Self::memory_evictable(node, require_durable) && !excluded.contains(&node.key)
            })?;
            if !self.evict_memory(id, key) {
                return None;
            }
        }
    }

    /// Shared eligibility for reclaiming an arena extent. A rooted manager may
    /// only drop records that have a disk copy; a rootless one may drop true
    /// leaves outright.
    fn memory_evictable(node: &crate::tree::Node<Location>, require_durable: bool) -> bool {
        node.location.memory.is_some()
            && (!require_durable || node.location.durable)
            && (if require_durable {
                node.children() == 0 || (node.row && node.children() == 1)
            } else {
                node.children() == 0
            })
    }

    /// Try the whole chain when the arena cannot hold the child beside its parent.
    fn rootless_fallback(
        &mut self,
        root: &Header,
        root_pins: Vec<Key>,
        arena_bytes: u64,
    ) -> RootChoice {
        let requirement = self.put_requirement(root, arena_bytes);
        if requirement.streaming {
            return RootChoice::Wait;
        }
        match self.memory_room_excluding(root.length_with_row() as usize, false, &root_pins) {
            Some(extent) => RootChoice::Store(extent),
            None => RootChoice::Wait,
        }
    }

    /// Returns whether the candidate was actually evicted. A node rejected by
    /// `Tree::remove` (because it gained children) is left untouched.
    fn evict_memory(&mut self, id: NamespaceId, key: Key) -> bool {
        let (disk, old) = {
            let node = &mut self.namespaces.get_mut(&id).unwrap().tree[&key];
            (node.location.disk.is_some(), node.location.bytes)
        };
        if disk {
            let node = &mut self.namespaces.get_mut(&id).unwrap().tree[&key];
            node.location.memory = None;
            node.location.bytes = record_charge(&node.location.header, false);
            self.charged = self.charged - old + node.location.bytes;
            self.delta(id, 1, key);
            return true;
        }
        self.evict_accounted(id, key)
    }

    /// The physical requirement for this Put shape. A record whose
    /// buddy-rounded extent fits `arena_bytes` is stored in one arena extent;
    /// otherwise it streams to disk behind a fixed staging hold of two of its
    /// largest windows. `arena_bytes` is the arena capacity for the shape
    /// decision; admission confirms it by allocating.
    fn put_requirement(&self, header: &Header, arena_bytes: u64) -> PutRequirement {
        let length = header.length_with_row();
        let extent = length.max(4096).next_power_of_two();
        if extent <= arena_bytes {
            return PutRequirement::arena(extent);
        }
        let first = header
            .parent
            .map_or(0, |parent| parent.consumed_tokens / header.page_tokens)
            as u32;
        let unit = plan_put_grants(header, first, self.config.max_window_bytes)
            .iter()
            .map(|grant| grant.length.max(4096).next_power_of_two())
            .max()
            .unwrap_or(4096);
        PutRequirement::streaming(disk::ENVELOPE_BYTES + length, unit)
    }

    /// Reserve budget space while the shape's protected keys stay put, so an
    /// eviction cannot invalidate the shape the reservation was computed for.
    fn make_room_with(
        &mut self,
        namespace: Option<NamespaceId>,
        bytes: u64,
        protected: &[Key],
    ) -> bool {
        if self.config.root.is_none() {
            return true;
        }
        let mut pinned = Vec::new();
        if let Some(id) = namespace {
            for key in protected {
                if let Some(namespace) = self.namespaces.get_mut(&id) {
                    if namespace.tree.pin(key).is_ok() {
                        pinned.push(*key);
                    }
                }
            }
        }
        let result = self.make_room_evict(bytes);
        if let Some(id) = namespace {
            for key in pinned {
                let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(&key);
            }
        }
        result
    }

    fn make_room_evict(&mut self, bytes: u64) -> bool {
        if self.config.root.is_none() {
            return true;
        }
        while self.charge_total().saturating_add(bytes) > self.config.nvme {
            let candidate = self.oldest(|node| {
                node.location.durable
                    && (node.children() == 0 || (node.row && node.children() == 1))
            });
            let Some((id, key)) = candidate else {
                return false;
            };
            let namespace = self.namespaces.get_mut(&id).unwrap();
            match namespace.tree.evict(true, |node| node.key == key) {
                Some(Eviction::Leaves(nodes)) => {
                    for node in nodes {
                        self.metrics.evictions += 1;
                        if let Some(extent) = node.location.disk {
                            extent.retire();
                            self.deferred_disk.push(extent);
                        }
                        self.charged -= node.location.bytes;
                        self.delta(id, 2, node.key);
                    }
                }
                Some(Eviction::Compacted(key)) => {
                    let node = &self.namespaces[&id].tree[&key];
                    let Some(source) = node.location.disk.clone() else {
                        return false;
                    };
                    if source.reader().is_err() {
                        self.namespaces.get_mut(&id).unwrap().tree[&key].row = true;
                        return false;
                    }
                    let header = node.location.header.clone();
                    let freed = node
                        .location
                        .bytes
                        .saturating_sub(disk::ENVELOPE_BYTES + header.row_segment);
                    self.job_counter += 1;
                    let job = self.job_counter;
                    let temporary = Arc::new(DiskExtent::new(
                        source.path.with_file_name(format!(".compact-{job}.tmp")),
                    ));
                    temporary.retire();
                    let task = Task::Compact {
                        id: job,
                        source,
                        temporary,
                        header,
                    };
                    let Some(worker) = &self.namespaces[&id].disk else {
                        return false;
                    };
                    match worker.sender.try_send(task) {
                        Ok(()) => {
                            self.jobs.insert(
                                job,
                                Job::Compact {
                                    namespace: id,
                                    key,
                                    bytes: freed,
                                },
                            );
                            let node = &mut self.namespaces.get_mut(&id).unwrap().tree[&key];
                            node.location.writing = true;
                            node.location.job = Some(job);
                            self.namespaces
                                .get_mut(&id)
                                .unwrap()
                                .tree
                                .pin(&key)
                                .unwrap();
                            self.metrics.compactions += 1;
                            self.delta(id, 1, key);
                        }
                        Err(_) => {
                            self.namespaces.get_mut(&id).unwrap().tree[&key].row = true;
                            return false;
                        }
                    }
                    return false;
                }
                None => return false,
            }
        }
        true
    }

    // ----------------------------------------------------------- Window grants

    fn pump_grants(&mut self) {
        let connections: Vec<u64> = self.sessions.keys().copied().collect();
        for connection in connections {
            loop {
                if !self.get_credit(connection) {
                    break;
                }
                let mut progressed = false;
                let mut transfers: Vec<u64> = self.sessions[&connection]
                    .ops
                    .iter()
                    .filter(|(_, open)| matches!(open.operation, Operation::Get(_)))
                    .map(|(transfer, _)| *transfer)
                    .collect();
                transfers.sort_unstable();
                for transfer in transfers {
                    if self.send_get_meta(connection, transfer) {
                        progressed = true;
                    }
                    if self.send_get_grant(connection, transfer) {
                        progressed = true;
                    }
                    if !self.get_credit(connection) {
                        break;
                    }
                }
                if !progressed {
                    break;
                }
            }
            loop {
                if !self.put_credit(connection) {
                    break;
                }
                let mut transfers: Vec<u64> = self.sessions[&connection]
                    .ops
                    .iter()
                    .filter(|(_, open)| matches!(open.operation, Operation::Put(_)))
                    .map(|(transfer, _)| *transfer)
                    .collect();
                transfers.sort_unstable();
                let mut progressed = false;
                for transfer in transfers {
                    if self.send_put_grant(connection, transfer) {
                        progressed = true;
                    }
                    if !self.put_credit(connection) {
                        break;
                    }
                }
                if !progressed {
                    break;
                }
            }
        }
    }

    fn get_credit(&self, connection: u64) -> bool {
        self.local
            .stats(connection)
            .is_none_or(|stats| stats.get_windows < WINDOW_CREDITS)
    }
    fn put_credit(&self, connection: u64) -> bool {
        self.local
            .stats(connection)
            .is_none_or(|stats| stats.put_windows < WINDOW_CREDITS)
    }

    fn send_put_grant(&mut self, connection: u64, transfer: u64) -> bool {
        let prepared = {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .unwrap()
                .ops
                .get_mut(&transfer)
            else {
                return false;
            };
            let Operation::Put(put) = &mut open.operation else {
                return false;
            };
            if put.cancelled || put.terminal || put.windows.next >= put.grants.len() {
                return false;
            }
            let index = put.windows.next as u32;
            let plan = put.grants[put.windows.next];
            let header = put.header.clone();
            let memory = put.memory.clone();
            let hold = put.staging_hold.clone();
            let hold_unit = put.hold_unit;
            let slot = if memory.is_some() {
                None
            } else {
                put.hold_slots.iter().position(Option::is_none)
            };
            (
                index,
                plan,
                header,
                memory,
                hold,
                hold_unit,
                slot,
            )
        };
        let (index, plan, header, memory, hold, hold_unit, slot) = prepared;
        if memory.is_none() && slot.is_none() {
            // Both hold slots are still live; retry once one is released.
            return false;
        }
        let shape = match WindowShape::new(
            plan.pages,
            plan.row,
            if plan.pages == 0 {
                0
            } else {
                header.page_record_bytes()
            },
            if plan.row {
                header.row_record_bytes()
            } else {
                0
            },
        ) {
            Ok(shape) => shape,
            Err(error) => {
                self.fail_put(connection, transfer, proto::Reason::Invalid as u8, error);
                return false;
            }
        };
        let (region, extent, within) = match &memory {
            Some(memory) => (Region::Arena, memory.clone(), plan.record_offset as usize),
            None => {
                let Some(hold) = hold else {
                    self.fail_put(
                        connection,
                        transfer,
                        proto::Reason::Invalid as u8,
                        invalid("missing staging hold"),
                    );
                    return false;
                };
                (Region::Staging, hold, slot.unwrap() * hold_unit)
            }
        };
        let result = self
            .local
            .grant_put_extent(connection, transfer, index, region, shape, &extent, within);
        match result {
            Ok(grant) => {
                if let Some(open) = self
                    .sessions
                    .get_mut(&connection)
                    .and_then(|session| session.ops.get_mut(&transfer))
                {
                    let Operation::Put(put) = &mut open.operation else {
                        return false;
                    };
                    put.windows.next += 1;
                    if let Some(slot) = slot {
                        put.hold_slots[slot] = Some(HoldSlot { index, job: None });
                    }
                    put.windows.granted.insert(index, plan);
                    put.outbox.push_back(Ok(PutAnswer::Grant(WindowGrant {
                        transfer,
                        index,
                        record: 0,
                        record_offset: plan.record_offset,
                        length: plan.length,
                        region: region_wire(grant.region),
                        offset: grant.offset,
                        page_first: plan.page_first,
                        page_count: plan.pages,
                        row: plan.row,
                    })));
                    open.progress = Instant::now();
                }
                true
            }
            Err(GrantError::Credits) => false,
            Err(GrantError::Budget) => {
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Budget as u8,
                    invalid("window budget"),
                );
                false
            }
            Err(GrantError::Invalid(detail)) => {
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Invalid as u8,
                    invalid(detail),
                );
                false
            }
        }
    }

    fn fail_put(&mut self, connection: u64, transfer: u64, reason: u8, error: io::Error) {
        self.metrics.puts_error += 1;
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        {
            let Operation::Put(put) = &mut open.operation else {
                return;
            };
            put.terminal = true;
            // Ownership is ending; never submit unsent chunks afterwards.
            put.blocked.clear();
            put.outbox.push_back(Ok(PutAnswer::Refused(Refused {
                reason,
                detail: error.to_string(),
            })));
            if !put.finished && put.pending == 0 {
                put.finished = true;
                let id = put.namespace;
                let pins = std::mem::take(&mut put.pins);
                let key = put.header.key;
                let reservation = put.reservation;
                let namespace = self.namespaces.get_mut(&id).unwrap();
                for key in &pins {
                    let _ = namespace.tree.unpin(key);
                }
                namespace.inflight.remove(&key);
                self.charged = self.charged.saturating_sub(reservation);
            }
        }
    }

    fn send_get_meta(&mut self, connection: u64, transfer: u64) -> bool {
        let mut progressed = false;
        loop {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return progressed;
            };
            let Operation::Get(get) = &mut open.operation else {
                return progressed;
            };
            if get.cancelled || get.terminal || get.next_meta >= get.records.len() {
                return progressed;
            }
            let record = get.next_meta as u32;
            if get.records[get.next_meta].meta_sent {
                get.next_meta += 1;
                continue;
            }
            let Some(metadata) = get.records[get.next_meta].meta.clone() else {
                return progressed;
            };
            let source = if get.records[get.next_meta].memory.is_some() {
                proto::Tier::Dram as i32
            } else {
                proto::Tier::Nvme as i32
            };
            for (index, chunk) in metadata.chunks(4096).enumerate() {
                get.outbox.push_back(Ok(GetAnswer::Meta {
                    record,
                    metadata_offset: (index * 4096) as u64,
                    metadata: chunk.to_vec(),
                    source,
                }));
            }
            get.records[get.next_meta].meta_sent = true;
            get.next_meta += 1;
            open.progress = Instant::now();
            progressed = true;
        }
    }

    fn get_credit_usage(&self, connection: u64) -> usize {
        let granted = self
            .local
            .stats(connection)
            .map(|stats| stats.get_windows as usize)
            .unwrap_or(0);
        let reserved: usize = self
            .sessions
            .get(&connection)
            .map(|session| {
                session
                    .ops
                    .values()
                    .filter_map(|open| match &open.operation {
                        Operation::Get(get) => Some(get.window_reads + get.ready_windows.len()),
                        Operation::Put(_) => None,
                    })
                    .sum()
            })
            .unwrap_or(0);
        granted + reserved
    }

    fn send_get_grant(&mut self, connection: u64, transfer: u64) -> bool {
        let granted_windows = self
            .local
            .stats(connection)
            .map(|stats| stats.get_windows as usize)
            .unwrap_or(0);
        if granted_windows < WINDOW_CREDITS as usize {
            if let Some((index, plan, extent)) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
                .and_then(|open| match &mut open.operation {
                    Operation::Get(get) => get.ready_windows.pop_front(),
                    Operation::Put(_) => None,
                })
            {
                let header = self
                    .sessions
                    .get(&connection)
                    .and_then(|session| session.ops.get(&transfer))
                    .and_then(|open| match &open.operation {
                        Operation::Get(get) => {
                            Some(get.records[plan.record as usize].header.clone())
                        }
                        Operation::Put(_) => None,
                    });
                let Some(header) = header else {
                    return false;
                };
                let shape = match WindowShape::new(
                    plan.pages,
                    plan.row,
                    if plan.pages == 0 {
                        0
                    } else {
                        header.page_record_bytes()
                    },
                    if plan.row {
                        header.row_record_bytes()
                    } else {
                        0
                    },
                ) {
                    Ok(shape) => shape,
                    Err(error) => {
                        self.fail_get(connection, transfer, proto::Reason::Invalid as u8, error);
                        return false;
                    }
                };
                match self.local.grant_get_extent(
                    connection,
                    transfer,
                    index,
                    Region::Staging,
                    shape,
                    &extent,
                    0,
                ) {
                    Ok(granted) => {
                        if let Some(open) = self
                            .sessions
                            .get_mut(&connection)
                            .and_then(|session| session.ops.get_mut(&transfer))
                        {
                            let Operation::Get(get) = &mut open.operation else {
                                return false;
                            };
                            get.outstanding.insert(index, plan);
                            get.outbox.push_back(Ok(GetAnswer::Ready(WindowGrant {
                                transfer,
                                index,
                                record: plan.record,
                                record_offset: plan.record_offset,
                                length: plan.length,
                                region: proto::Region::Staging as i32,
                                offset: granted.offset,
                                page_first: plan.page_first,
                                page_count: plan.pages,
                                row: plan.row,
                            })));
                            open.progress = Instant::now();
                        }
                        return true;
                    }
                    Err(GrantError::Credits) => {
                        if let Some(open) = self
                            .sessions
                            .get_mut(&connection)
                            .and_then(|session| session.ops.get_mut(&transfer))
                        {
                            if let Operation::Get(get) = &mut open.operation {
                                get.ready_windows.push_front((index, plan, extent));
                            }
                        }
                        return false;
                    }
                    Err(GrantError::Budget) => {
                        self.fail_get(
                            connection,
                            transfer,
                            proto::Reason::Budget as u8,
                            invalid("staging grant"),
                        );
                        return false;
                    }
                    Err(GrantError::Invalid(detail)) => {
                        self.fail_get(
                            connection,
                            transfer,
                            proto::Reason::Invalid as u8,
                            invalid(detail),
                        );
                        return false;
                    }
                }
            }
        }
        if self.get_credit_usage(connection) >= WINDOW_CREDITS as usize {
            return false;
        }
        let Some(ready) = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .and_then(|open| match &open.operation {
                Operation::Get(get) => Some((
                    get.namespace,
                    get.next_grant,
                    get.grants.get(get.next_grant).map(|grant| PlannedGetGrant {
                        record: grant.record,
                        record_offset: grant.record_offset,
                        length: grant.length,
                        pages: grant.pages,
                        row: grant.row,
                        page_first: grant.page_first,
                    }),
                )),
                Operation::Put(_) => None,
            })
        else {
            return false;
        };
        let (namespace, position, Some(plan)) = ready else {
            return false;
        };
        enum Next {
            Stop,
            Fail(proto::Reason, io::Error),
            Lease { header: Header, memory: Arc<Extent> },
            Read { file: Arc<File> },
        }
        let action = {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return false;
            };
            let Operation::Get(get) = &mut open.operation else {
                return false;
            };
            if get.cancelled || get.terminal || !get.records[plan.record as usize].meta_sent {
                Next::Stop
            } else {
                let record = &get.records[plan.record as usize];
                if let Some(memory) = &record.memory {
                    Next::Lease {
                        header: record.header.clone(),
                        memory: memory.clone(),
                    }
                } else if let Some(file) = &record.file {
                    Next::Read { file: file.clone() }
                } else {
                    Next::Fail(proto::Reason::Io, invalid("record file closed"))
                }
            }
        };
        let schedule = match action {
            Next::Stop => return false,
            Next::Fail(reason, error) => {
                self.fail_get(connection, transfer, reason as u8, error);
                return false;
            }
            Next::Lease { header, memory } => {
                let shape = match WindowShape::new(
                    plan.pages,
                    plan.row,
                    if plan.pages == 0 {
                        0
                    } else {
                        header.page_record_bytes()
                    },
                    if plan.row {
                        header.row_record_bytes()
                    } else {
                        0
                    },
                ) {
                    Ok(shape) => shape,
                    Err(error) => {
                        self.fail_get(connection, transfer, proto::Reason::Invalid as u8, error);
                        return false;
                    }
                };
                match self.local.grant_get_lease(
                    connection,
                    transfer,
                    position as u32,
                    shape,
                    &memory,
                    plan.record_offset as usize,
                ) {
                    Ok(grant) => {
                        if let Some(open) = self
                            .sessions
                            .get_mut(&connection)
                            .and_then(|session| session.ops.get_mut(&transfer))
                        {
                            let Operation::Get(get) = &mut open.operation else {
                                return false;
                            };
                            get.outstanding.insert(position as u32, plan);
                            get.next_grant += 1;
                            get.outbox.push_back(Ok(GetAnswer::Ready(WindowGrant {
                                transfer,
                                index: position as u32,
                                record: plan.record,
                                record_offset: plan.record_offset,
                                length: plan.length,
                                region: proto::Region::Arena as i32,
                                offset: grant.offset,
                                page_first: plan.page_first,
                                page_count: plan.pages,
                                row: plan.row,
                            })));
                            open.progress = Instant::now();
                        }
                        return true;
                    }
                    Err(GrantError::Credits) => return false,
                    Err(GrantError::Budget) => {
                        self.fail_get(
                            connection,
                            transfer,
                            proto::Reason::Budget as u8,
                            invalid("window budget"),
                        );
                        return false;
                    }
                    Err(GrantError::Invalid(detail)) => {
                        self.fail_get(
                            connection,
                            transfer,
                            proto::Reason::Invalid as u8,
                            invalid(detail),
                        );
                        return false;
                    }
                }
            }
            Next::Read { file } => {
                let length = usize::try_from(plan.length).unwrap_or(usize::MAX);
                // Restore windows are transient staging traffic: reads do not
                // grow the durable set, so they are bounded by the staging
                // region alone and never by the NVMe budget.
                let Some(extent) = self.staging.allocate(length) else {
                    self.fail_get(
                        connection,
                        transfer,
                        proto::Reason::Budget as u8,
                        invalid("staging budget"),
                    );
                    return false;
                };
                if let Some(open) = self
                    .sessions
                    .get_mut(&connection)
                    .and_then(|session| session.ops.get_mut(&transfer))
                {
                    let Operation::Get(get) = &mut open.operation else {
                        return false;
                    };
                    get.next_grant += 1;
                    get.window_reads += 1;
                    get.pending_reads += 1;
                }
                Some((namespace, file, extent))
            }
        };
        let Some((namespace, file, extent)) = schedule else {
            self.fail_get(
                connection,
                transfer,
                proto::Reason::Io as u8,
                invalid("record file closed"),
            );
            return false;
        };
        self.job_counter += 1;
        let job = self.job_counter;
        let task = Task::Read {
            id: job,
            file,
            offset: disk::ENVELOPE_BYTES + plan.record_offset,
            buffer: extent,
        };
        self.jobs.insert(
            job,
            Job::GetRead {
                connection,
                transfer,
                record: plan.record,
                kind: ReadKind::Window {
                    grant: position as u32,
                    length: plan.length as usize,
                },
            },
        );
        self.submit_read(namespace, job, task);
        true
    }

    fn fail_get(&mut self, connection: u64, transfer: u64, reason: u8, error: io::Error) {
        self.metrics.gets_error += 1;
        let namespace = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .and_then(|open| match &open.operation {
                Operation::Get(get) => Some(get.namespace),
                Operation::Put(_) => None,
            });
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        {
            let Operation::Get(get) = &mut open.operation else {
                return;
            };
            get.terminal = true;
            get.cancelled = true;
            get.outbox.push_back(Ok(GetAnswer::Refused(Refused {
                reason,
                detail: error.to_string(),
            })));
            if !get.finished {
                get.finished = true;
                if get.pending_reads == 0 {
                    let pins = std::mem::take(&mut get.pins);
                    if let Some(id) = namespace {
                        let state = self.namespaces.get_mut(&id).unwrap();
                        for key in &pins {
                            let _ = state.tree.unpin(key);
                        }
                    }
                } else if let Some(id) = namespace {
                    let drain = GetDrain {
                        connection,
                        transfer,
                        namespace: id,
                        pins: std::mem::take(&mut get.pins),
                        pending: get.pending_reads,
                    };
                    self.retired_gets.push(drain);
                }
            }
        }
    }

    fn submit_read(&mut self, namespace: NamespaceId, job: u64, task: Task) {
        let Some(worker) = &self.namespaces[&namespace].disk else {
            if let Some(Job::GetRead {
                connection,
                transfer,
                ..
            }) = self.jobs.remove(&job)
            {
                self.fail_get(
                    connection,
                    transfer,
                    proto::Reason::Io as u8,
                    invalid("no disk"),
                );
            }
            return;
        };
        if let Err(std::sync::mpsc::TrySendError::Full(task)) = worker.read_sender.try_send(task) {
            self.read_backlog.push_back((namespace, job, task));
        }
    }

    // --------------------------------------------------------------- Put frames

    fn window_filled(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        page_xxh3: Vec<u64>,
        row_xxh3: Option<u64>,
    ) -> Result<()> {
        struct Prepared {
            plan: PlannedGrant,
            memory: Option<Arc<Extent>>,
            hold: Option<Arc<Extent>>,
            within: usize,
            header: Header,
            first_page: u32,
            temporary: Option<Arc<DiskExtent>>,
            streaming: bool,
        }
        let prepared = {
            let Some(open) = self
                .sessions
                .get(&connection)
                .and_then(|session| session.ops.get(&transfer))
            else {
                return Err(invalid("unknown Put transfer"));
            };
            let Operation::Put(put) = &open.operation else {
                return Err(invalid("transfer is not a Put"));
            };
            if put.cancelled || put.terminal {
                return Ok(());
            }
            let slot = put
                .hold_slots
                .iter()
                .position(|slot| slot.is_some_and(|slot| slot.index == index));
            put.windows
                .granted
                .get(&index)
                .copied()
                .map(|plan| Prepared {
                    plan,
                    memory: put.memory.clone(),
                    hold: put.staging_hold.clone(),
                    within: slot.map_or(0, |slot| slot * put.hold_unit),
                    header: put.header.clone(),
                    first_page: put.first_page,
                    temporary: put.temporary.clone(),
                    streaming: put.streaming,
                })
        };
        let Some(prepared) = prepared else {
            self.fail_put(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("filled for an ungranted window"),
            );
            return Ok(());
        };
        let plan = prepared.plan;
        if page_xxh3.len() != plan.pages as usize
            || (plan.row && row_xxh3.is_none())
            || (!plan.row && row_xxh3.is_some())
            || plan.pages > 64
        {
            self.abort_put_window(connection, transfer, index);
            self.fail_put(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("window hash shape"),
            );
            return Ok(());
        }
        // The window bytes stay in the arena extent or staging hold; a chunk
        // keeps this slice alive until its write completes, so no copy is
        // buffered in the manager.
        let data = if let Some(memory) = &prepared.memory {
            ExtentSlice::new(
                memory.clone(),
                plan.record_offset as usize,
                plan.length as usize,
            )
        } else if let Some(hold) = &prepared.hold {
            ExtentSlice::new(hold.clone(), prepared.within, plan.length as usize)
        } else {
            self.fail_put(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("window vanished"),
            );
            return Ok(());
        };
        let bytes = data.bytes();
        let page_bytes = prepared.header.page_record_bytes() as usize;
        let mut verified = Vec::with_capacity(page_xxh3.len());
        for (position, hash) in page_xxh3.iter().enumerate() {
            let start = position * page_bytes;
            if record::checksum(&bytes[start..start + page_bytes]) != *hash {
                self.abort_put_window(connection, transfer, index);
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Invalid as u8,
                    invalid("page checksum"),
                );
                return Ok(());
            }
            verified.push(*hash);
        }
        if plan.row {
            let row_bytes = prepared.header.row_record_bytes() as usize;
            let start = bytes.len() - row_bytes;
            if record::checksum(&bytes[start..]) != row_xxh3.unwrap() {
                self.abort_put_window(connection, transfer, index);
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Invalid as u8,
                    invalid("row checksum"),
                );
                return Ok(());
            }
        }
        {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return Ok(());
            };
            let Operation::Put(put) = &mut open.operation else {
                return Ok(());
            };
            put.windows.granted.remove(&index);
            // The slot stays occupied until the chunk write drains.
            for (position, hash) in verified.iter().enumerate() {
                let relative =
                    (plan.page_first - u64::from(prepared.first_page)) as usize + position;
                put.windows.pages[relative] = Some(*hash);
                put.windows.pages_filled += 1;
            }
            if plan.row {
                put.windows.row = row_xxh3;
                put.windows.row_filled = true;
            }
            open.progress = Instant::now();
        }
        let _ = self.local.release(connection, transfer, index);
        if prepared.streaming {
            let Some(temporary) = prepared.temporary else {
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Io as u8,
                    invalid("missing temporary file"),
                );
                return Ok(());
            };
            let Some(namespace) = self.sessions.get(&connection).and_then(|session| {
                session
                    .ops
                    .get(&transfer)
                    .and_then(|open| match &open.operation {
                        Operation::Put(put) => Some(put.namespace),
                        Operation::Get(_) => None,
                    })
            }) else {
                return Ok(());
            };
            self.job_counter += 1;
            let job = self.job_counter;
            let task = Task::Chunk {
                id: job,
                temporary,
                offset: plan.record_offset,
                data,
            };
            enum ChunkAction {
                Sent,
                Blocked(ExtentSlice),
                Failed,
            }
            let action = match &self.namespaces[&namespace].disk {
                Some(worker) => match worker.sender.try_send(task) {
                    Ok(()) => ChunkAction::Sent,
                    Err(std::sync::mpsc::TrySendError::Full(task)) => {
                        if let Task::Chunk { data, .. } = task {
                            ChunkAction::Blocked(data)
                        } else {
                            ChunkAction::Failed
                        }
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => ChunkAction::Failed,
                },
                None => ChunkAction::Failed,
            };
            match action {
                ChunkAction::Sent => {
                    self.jobs.insert(
                        job,
                        Job::Chunk {
                            connection,
                            transfer,
                        },
                    );
                    if let Some(open) = self
                        .sessions
                        .get_mut(&connection)
                        .and_then(|session| session.ops.get_mut(&transfer))
                    {
                        if let Operation::Put(put) = &mut open.operation {
                            put.pending += 1;
                            set_slot_job(put, index, job);
                        }
                    }
                    self.config.faults.checkpoint("data");
                }
                ChunkAction::Blocked(data) => {
                    if let Some(open) = self
                        .sessions
                        .get_mut(&connection)
                        .and_then(|session| session.ops.get_mut(&transfer))
                    {
                        if let Operation::Put(put) = &mut open.operation {
                            put.blocked.push_back((job, plan.record_offset, data));
                            set_slot_job(put, index, job);
                        }
                    }
                }
                ChunkAction::Failed => {
                    self.fail_put(
                        connection,
                        transfer,
                        proto::Reason::Io as u8,
                        invalid("disk worker stopped"),
                    );
                }
            }
        }
        Ok(())
    }

    fn abort_put_window(&mut self, connection: u64, transfer: u64, index: u32) {
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        {
            if let Operation::Put(put) = &mut open.operation {
                if let Some(slot) = put
                    .hold_slots
                    .iter_mut()
                    .find(|slot| slot.is_some_and(|slot| slot.index == index))
                {
                    *slot = None;
                }
            }
        }
        let _ = self.local.release(connection, transfer, index);
    }

    fn put_commit(
        &mut self,
        connection: u64,
        transfer: u64,
        metadata_offset: u64,
        metadata: Vec<u8>,
    ) -> Result<()> {
        enum Action {
            None,
            Fail(proto::Reason, io::Error),
            Publish,
        }
        let namespace = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .and_then(|open| match &open.operation {
                Operation::Put(put) => Some(put.namespace),
                Operation::Get(_) => None,
            });
        let Some(namespace) = namespace else {
            return Err(invalid("unknown Put transfer"));
        };
        let descriptor = self.namespaces[&namespace].descriptor.clone();
        let action = {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return Err(invalid("unknown Put transfer"));
            };
            let Operation::Put(put) = &mut open.operation else {
                return Err(invalid("transfer is not a Put"));
            };
            if put.cancelled || put.terminal || put.committed {
                return Ok(());
            }
            if !put.owns_payload() {
                Action::Fail(
                    proto::Reason::Invalid,
                    invalid("commit before every payload range"),
                )
            } else {
                let header_page_segment = put.header.page_segment as usize;
                if metadata_offset as usize != put.commit_bytes.len()
                    || metadata.is_empty()
                    || !metadata.len().is_multiple_of(4096)
                    || put.commit_bytes.len() + metadata.len() > header_page_segment
                    || !(metadata_offset as usize).is_multiple_of(4096)
                {
                    Action::Fail(proto::Reason::Invalid, invalid("commit offset or length"))
                } else {
                    put.commit_bytes.extend_from_slice(&metadata);
                    if put.commit_bytes.len() < header_page_segment {
                        Action::None
                    } else if let Err(error) = validate_commit(put, &descriptor) {
                        Action::Fail(proto::Reason::Invalid, error)
                    } else {
                        if let Some(memory) = &mut put.memory {
                            let checksum_segment =
                                put.commit_bytes[4096..header_page_segment].to_vec();
                            let bytes = Arc::get_mut(memory).unwrap().bytes_mut();
                            bytes[..4096].copy_from_slice(&put.commit_bytes[..4096]);
                            bytes[4096..header_page_segment].copy_from_slice(&checksum_segment);
                            put.committed = true;
                            Action::Publish
                        } else {
                            put.metadata = Some(Arc::new(std::mem::take(&mut put.commit_bytes)));
                            put.committed = true;
                            Action::None
                        }
                    }
                }
            }
        };
        match action {
            Action::None => {}
            Action::Fail(reason, error) => {
                self.fail_put(connection, transfer, reason as u8, error);
            }
            Action::Publish => {
                self.publish_put(connection, transfer, None, false);
            }
        }
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        {
            open.progress = Instant::now();
        }
        Ok(())
    }

    /// Admission records whether the key existed when the Put began, but the merged catalog
    /// can add or drop a remote-only entry while the Put is in flight. Decide from the tree
    /// as it is when the publication lands.
    fn publish_target(&self, id: NamespaceId, header: &Header) -> PublishTarget {
        let Some(namespace) = self.namespaces.get(&id) else {
            return PublishTarget::Conflict;
        };
        let Some(node) = namespace.tree.node(&header.key) else {
            return PublishTarget::Insert;
        };
        if node.parent != header.parent
            || node.bucket != header.bucket
            || node.tail_pages != header.tail_pages as usize
        {
            return PublishTarget::Conflict;
        }
        // A complete local copy wins; a page-only record (its row was compacted away) is
        // exactly what a replacement Put restores.
        if node.row && (node.location.memory.is_some() || node.location.disk.is_some()) {
            return PublishTarget::Present;
        }
        PublishTarget::Replace
    }

    fn publish_put(
        &mut self,
        connection: u64,
        transfer: u64,
        disk: Option<Arc<DiskExtent>>,
        durable: bool,
    ) {
        let Some((id, epoch, header)) = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .and_then(|open| match &open.operation {
                Operation::Put(put) if !put.finished => {
                    Some((put.namespace, put.epoch, put.header.clone()))
                }
                _ => None,
            })
        else {
            return;
        };
        if self
            .namespaces
            .get(&id)
            .is_none_or(|namespace| namespace.epoch != epoch)
        {
            if let Some(disk) = &disk {
                disk.retire();
            }
            self.fail_put(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("namespace was purged while publishing"),
            );
            return;
        }
        let target = self.publish_target(id, &header);
        match target {
            PublishTarget::Present => {
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Present as u8,
                    invalid("record was published concurrently"),
                );
                return;
            }
            PublishTarget::Conflict => {
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Invalid as u8,
                    invalid("published record conflicts with the catalog entry"),
                );
                return;
            }
            PublishTarget::Insert | PublishTarget::Replace => {}
        }
        let prepared = {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return;
            };
            let Operation::Put(put) = &mut open.operation else {
                return;
            };
            if put.finished {
                return;
            }
            let header = put.header.clone();
            let memory = put.memory.clone();
            let bytes = record_charge(&header, memory.is_some());
            let reservation = put.reservation;
            let pins = std::mem::take(&mut put.pins);
            let record_bytes = header.length_with_row();
            let key = header.key;
            let already_terminal = put.terminal;
            put.terminal = true;
            (
                header,
                memory,
                bytes,
                reservation,
                pins,
                key,
                already_terminal,
                record_bytes,
            )
        };
        let (header, memory, bytes, reservation, pins, key, already_terminal, record_bytes) =
            prepared;
        let replace = matches!(target, PublishTarget::Replace);
        let location = Location {
            header: header.clone(),
            memory,
            disk,
            durable: false,
            writing: false,
            job: None,
            retry: Instant::now(),
            bytes,
        };
        let inserted = {
            let namespace = self.namespaces.get_mut(&id).unwrap();
            let inserted = if replace {
                let node = namespace.tree.node_mut(&key).unwrap();
                let old = std::mem::replace(&mut node.location, location);
                node.row = true;
                self.retired_records.push((id, key, old));
                true
            } else {
                namespace
                    .tree
                    .insert(
                        key,
                        header.parent,
                        header.bucket,
                        header.tail_pages as usize,
                        true,
                        location,
                    )
                    .is_ok()
            };
            for key in &pins {
                let _ = namespace.tree.unpin(key);
            }
            namespace.inflight.remove(&key);
            inserted
        };
        if !inserted {
            self.fail_put(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("publish insert"),
            );
            return;
        }
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
            && let Operation::Put(put) = &mut open.operation
            && !put.finished
        {
            put.finished = true;
            put.pending = 0;
            if !already_terminal {
                put.outbox.push_back(Ok(PutAnswer::Stored {
                    bytes: record_bytes,
                }));
            }
        }
        self.clock += 1;
        let clock = self.clock;
        self.namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .access_at(&key, clock);
        self.charged = self.charged.saturating_sub(reservation) + bytes;
        if durable {
            if let Some(node) = self.namespaces.get_mut(&id).unwrap().tree.node_mut(&key) {
                node.location.durable = true;
            }
        }
        self.refresh_durability(id);
        self.metrics.puts_ok += 1;
        self.delta(id, if replace { 1 } else { 0 }, key);
    }

    fn begin_get(&mut self, pending: PendingGet) {
        let PendingGet {
            connection,
            transfer,
            request,
            events,
        } = pending;
        let Some(id) = self.sessions.get(&connection).map(|_| request.namespace) else {
            return;
        };
        let parsed = self.plan_get(id, &request);
        let (records, grants) = match parsed {
            Ok(value) => value,
            Err(error) => {
                self.metrics.gets_error += 1;
                let _ = events.try_send(Ok(GetAnswer::Refused(Refused {
                    reason: proto::Reason::Invalid as u8,
                    detail: error.to_string(),
                })));
                return;
            }
        };
        let pages: Vec<Key> = request.keys.clone();
        {
            let namespace = self.namespaces.get_mut(&id).unwrap();
            for key in &pages {
                if namespace.tree.pin(key).is_err() {
                    let _ = events.try_send(Ok(GetAnswer::Refused(Refused {
                        reason: proto::Reason::Invalid as u8,
                        detail: "pin".into(),
                    })));
                    return;
                }
                self.clock += 1;
                namespace.tree.access_at(key, self.clock);
            }
        }
        let record_bytes: Vec<u64> = records
            .iter()
            .enumerate()
            .map(|(index, record)| record.header.length(index + 1 == records.len()))
            .collect();
        let get = Get {
            namespace: id,
            pins: pages.into(),
            records,
            grants,
            next_grant: 0,
            next_meta: 0,
            outstanding: HashMap::new(),
            released: HashSet::new(),
            ready_windows: VecDeque::new(),
            window_reads: 0,
            pending_reads: 0,
            cancelled: false,
            terminal: false,
            finished: false,
            leased_sent: false,
            events,
            outbox: VecDeque::new(),
        };
        let mut get = get;
        get.outbox.push_back(Ok(GetAnswer::Leased {
            transfer,
            record_bytes,
        }));
        get.leased_sent = true;
        self.sessions.get_mut(&connection).unwrap().ops.insert(
            transfer,
            Open {
                progress: Instant::now(),
                operation: Operation::Get(Box::new(get)),
            },
        );
        self.prepare_get_records(connection, transfer, id);
    }

    fn plan_get(
        &mut self,
        id: NamespaceId,
        request: &GetRequest,
    ) -> Result<(Vec<GetRecord>, Vec<PlannedGetGrant>)> {
        if request.keys.is_empty() || request.keys.len() > MAX_CHAIN || !request.row {
            return Err(invalid("Get node count or row"));
        }
        let namespace = self
            .namespaces
            .get(&id)
            .ok_or_else(|| invalid("Get namespace is not registered"))?;
        let mut total = 0u64;
        let mut records = Vec::new();
        let mut grants = Vec::new();
        for (index, key) in request.keys.iter().enumerate() {
            let node = namespace
                .tree
                .node(key)
                .ok_or_else(|| invalid("Get key missing"))?;
            if (index > 0 && node.parent != Some(request.keys[index - 1]))
                || (index + 1 == request.keys.len() && !node.row)
            {
                return Err(invalid("Get chain"));
            }
            if index == 0 && u64::from(request.skip_pages) > node.tail_pages as u64 {
                return Err(invalid("Get skip"));
            }
            total += node.tail_pages as u64;
            let header = node.location.header.clone();
            let skip = if index == 0 { request.skip_pages } else { 0 };
            let first_page = node.parent.map_or(0, |parent| {
                parent.consumed_tokens / namespace.descriptor.page_tokens
            }) as u32;
            plan_get_record_grants(
                &header,
                first_page,
                skip,
                index + 1 == request.keys.len(),
                self.config.max_window_bytes,
                index as u32,
                &mut grants,
            );
            records.push(GetRecord {
                memory: node.location.memory.clone(),
                file: node
                    .location
                    .disk
                    .as_ref()
                    .map(|extent| extent.reader())
                    .transpose()?,
                header,
                row: node.row,
                meta: None,
                meta_sent: false,
            });
        }
        if total - u64::from(request.skip_pages) != u64::from(request.expected_pages) {
            return Err(invalid("Get page count"));
        }
        for record in &records {
            let key = record.header.key;
            *self
                .metrics
                .hits
                .entry((
                    record::hex(&id),
                    if record.memory.is_some() {
                        "memory"
                    } else {
                        "disk"
                    },
                ))
                .or_default() += 1;
            let _ = key;
        }
        Ok((records, grants))
    }

    fn prepare_get_records(&mut self, connection: u64, transfer: u64, id: NamespaceId) {
        let records = {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return;
            };
            let Operation::Get(get) = &mut open.operation else {
                return;
            };
            get.records.len()
        };
        for index in 0..records {
            let (memory, file, header, length) = {
                let Some(open) = self
                    .sessions
                    .get(&connection)
                    .and_then(|session| session.ops.get(&transfer))
                else {
                    return;
                };
                let Operation::Get(get) = &open.operation else {
                    return;
                };
                let record = &get.records[index];
                (
                    record.memory.clone(),
                    record.file.clone(),
                    record.header.clone(),
                    record.header.length(record.row),
                )
            };
            if let Some(memory) = memory {
                let meta = memory.bytes()[..header.page_segment as usize].to_vec();
                if let Some(open) = self
                    .sessions
                    .get_mut(&connection)
                    .and_then(|session| session.ops.get_mut(&transfer))
                {
                    let Operation::Get(get) = &mut open.operation else {
                        return;
                    };
                    get.records[index].meta = Some(meta);
                }
                continue;
            }
            let Some(file) = file else {
                self.fail_get(
                    connection,
                    transfer,
                    proto::Reason::Io as u8,
                    invalid("missing file"),
                );
                return;
            };
            let meta_length = header.page_segment as usize;
            let fill = self.memory_room(length as usize, self.config.root.is_some());
            let (offset, buffer, kind) = if let Some(extent) = fill {
                (disk::ENVELOPE_BYTES, extent, ReadKind::Fill)
            } else {
                // Metadata windows are staging traffic too; a full durable
                // budget must not refuse a restore.
                let Some(extent) = self.staging.allocate(meta_length) else {
                    self.fail_get(
                        connection,
                        transfer,
                        proto::Reason::Budget as u8,
                        invalid("staging budget"),
                    );
                    return;
                };
                (
                    disk::ENVELOPE_BYTES,
                    extent,
                    ReadKind::Meta {
                        length: meta_length,
                    },
                )
            };
            self.job_counter += 1;
            let job = self.job_counter;
            let task = Task::Read {
                id: job,
                file,
                offset,
                buffer,
            };
            if let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            {
                let Operation::Get(get) = &mut open.operation else {
                    return;
                };
                get.pending_reads += 1;
            }
            self.jobs.insert(
                job,
                Job::GetRead {
                    connection,
                    transfer,
                    record: index as u32,
                    kind,
                },
            );
            self.submit_read(id, job, task);
        }
    }

    fn drain_get_read(&mut self, connection: u64, transfer: u64) {
        for drain in &mut self.retired_gets {
            if drain.connection == connection && drain.transfer == transfer {
                drain.pending = drain.pending.saturating_sub(1);
            }
        }
    }

    fn get_read_done(
        &mut self,
        connection: u64,
        transfer: u64,
        record: u32,
        kind: ReadKind,
        data: Option<Extent>,
    ) {
        let terminal = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .is_none_or(|open| {
                matches!(&open.operation, Operation::Get(get) if get.terminal || get.finished)
            });
        if terminal {
            if let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            {
                if let Operation::Get(get) = &mut open.operation {
                    get.pending_reads = get.pending_reads.saturating_sub(1);
                    if matches!(kind, ReadKind::Window { .. }) {
                        get.window_reads = get.window_reads.saturating_sub(1);
                    }
                }
            }
            self.drain_get_read(connection, transfer);
            return;
        }
        let Some(extent) = data else {
            self.read_failed(connection, transfer);
            return;
        };
        let state = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .and_then(|open| match &open.operation {
                Operation::Get(get) => Some((
                    get.namespace,
                    usize::try_from(record).is_ok_and(|index| index < get.records.len()),
                )),
                Operation::Put(_) => None,
            });
        let Some((id, in_range)) = state else {
            return;
        };
        if !in_range {
            self.read_failed(connection, transfer);
            return;
        }
        match kind {
            ReadKind::Fill => {
                let (header, row) = {
                    let Some(open) = self
                        .sessions
                        .get(&connection)
                        .and_then(|session| session.ops.get(&transfer))
                    else {
                        return;
                    };
                    let Operation::Get(get) = &open.operation else {
                        return;
                    };
                    let record = &get.records[record as usize];
                    (record.header.clone(), record.row)
                };
                if record::verify_record(extent.bytes(), &header, row).is_err() {
                    self.read_failed(connection, transfer);
                    return;
                }
                let meta = extent.bytes()[..header.page_segment as usize].to_vec();
                let extent = Arc::new(extent);
                self.get_filled(id, &header, extent.clone());
                if let Some(open) = self
                    .sessions
                    .get_mut(&connection)
                    .and_then(|session| session.ops.get_mut(&transfer))
                {
                    let Operation::Get(get) = &mut open.operation else {
                        return;
                    };
                    get.records[record as usize].memory = Some(extent);
                    get.records[record as usize].meta = Some(meta);
                    get.pending_reads -= 1;
                }
            }
            ReadKind::Meta { length } => {
                let meta = extent.bytes()[..length].to_vec();
                if let Some(open) = self
                    .sessions
                    .get_mut(&connection)
                    .and_then(|session| session.ops.get_mut(&transfer))
                {
                    let Operation::Get(get) = &mut open.operation else {
                        return;
                    };
                    get.records[record as usize].meta = Some(meta);
                    get.pending_reads -= 1;
                }
            }
            ReadKind::Window { grant, length } => {
                let (header, plan) = {
                    let Some(open) = self
                        .sessions
                        .get_mut(&connection)
                        .and_then(|session| session.ops.get_mut(&transfer))
                    else {
                        return;
                    };
                    let Operation::Get(get) = &mut open.operation else {
                        return;
                    };
                    get.pending_reads = get.pending_reads.saturating_sub(1);
                    get.window_reads = get.window_reads.saturating_sub(1);
                    let Some(plan) = get.grants.get(grant as usize) else {
                        return;
                    };
                    (
                        get.records[plan.record as usize].header.clone(),
                        PlannedGetGrant {
                            record: plan.record,
                            record_offset: plan.record_offset,
                            length: plan.length,
                            pages: plan.pages,
                            row: plan.row,
                            page_first: plan.page_first,
                        },
                    )
                };
                let shape = match WindowShape::new(
                    plan.pages,
                    plan.row,
                    if plan.pages == 0 {
                        0
                    } else {
                        header.page_record_bytes()
                    },
                    if plan.row {
                        header.row_record_bytes()
                    } else {
                        0
                    },
                ) {
                    Ok(shape) => shape,
                    Err(error) => {
                        self.fail_get(connection, transfer, proto::Reason::Invalid as u8, error);
                        return;
                    }
                };
                let extent = Arc::new(extent);
                match self.local.grant_get_extent(
                    connection,
                    transfer,
                    grant,
                    Region::Staging,
                    shape,
                    &extent,
                    0,
                ) {
                    Ok(granted) => {
                        if let Some(open) = self
                            .sessions
                            .get_mut(&connection)
                            .and_then(|session| session.ops.get_mut(&transfer))
                        {
                            let Operation::Get(get) = &mut open.operation else {
                                return;
                            };
                            get.outstanding.insert(grant, plan);
                            get.outbox.push_back(Ok(GetAnswer::Ready(WindowGrant {
                                transfer,
                                index: grant,
                                record: plan.record,
                                record_offset: plan.record_offset,
                                length: plan.length,
                                region: proto::Region::Staging as i32,
                                offset: granted.offset,
                                page_first: plan.page_first,
                                page_count: plan.pages,
                                row: plan.row,
                            })));
                        }
                    }
                    Err(GrantError::Credits) => {
                        // The read is complete; keep the extent reserved until a
                        // connection credit frees rather than failing or leaking it.
                        if let Some(open) = self
                            .sessions
                            .get_mut(&connection)
                            .and_then(|session| session.ops.get_mut(&transfer))
                        {
                            if let Operation::Get(get) = &mut open.operation {
                                get.ready_windows.push_back((grant, plan, extent));
                            }
                        }
                    }
                    Err(GrantError::Invalid(detail)) => {
                        self.fail_get(
                            connection,
                            transfer,
                            proto::Reason::Invalid as u8,
                            invalid(detail),
                        );
                    }
                    Err(GrantError::Budget) => {
                        self.fail_get(
                            connection,
                            transfer,
                            proto::Reason::Budget as u8,
                            invalid("staging grant"),
                        );
                    }
                }
                let _ = length;
            }
        }
    }

    fn read_failed(&mut self, connection: u64, transfer: u64) {
        let terminal = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .is_none_or(|open| {
                matches!(&open.operation, Operation::Get(get) if get.terminal || get.finished)
            });
        if terminal {
            if let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            {
                if let Operation::Get(get) = &mut open.operation {
                    get.pending_reads = get.pending_reads.saturating_sub(1);
                    get.window_reads = get.window_reads.saturating_sub(1);
                }
            }
            self.drain_get_read(connection, transfer);
            return;
        }
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        {
            if let Operation::Get(get) = &mut open.operation {
                get.pending_reads = get.pending_reads.saturating_sub(1);
                get.window_reads = get.window_reads.saturating_sub(1);
            }
        }
        self.fail_get(
            connection,
            transfer,
            proto::Reason::Io as u8,
            invalid("disk read failed"),
        );
    }

    fn get_filled(&mut self, id: NamespaceId, header: &Header, extent: Arc<Extent>) {
        let Some(node) = self
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&header.key)
        else {
            return;
        };
        if node.location.memory.is_some() || node.location.header != *header {
            return;
        }
        let old = node.location.bytes;
        node.location.memory = Some(extent);
        node.location.bytes = record_charge(header, true);
        let new = node.location.bytes;
        self.charged = self.charged - old + new;
        self.delta(id, 1, header.key);
    }

    fn window_released(&mut self, connection: u64, transfer: u64, index: u32) -> Result<()> {
        let mut bad = false;
        let mut known = false;
        if let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        {
            if let Operation::Get(get) = &mut open.operation {
                known = true;
                if get.outstanding.remove(&index).is_some() {
                    let _ = self.local.release(connection, transfer, index);
                    get.released.insert(index);
                    open.progress = Instant::now();
                } else if get.released.contains(&index) {
                    self.metrics.dropped += 1;
                } else {
                    bad = true;
                }
            }
        }
        if bad {
            self.fail_get(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("release of an ungranted window"),
            );
        }
        if known {
            Ok(())
        } else {
            Err(invalid("unknown Get transfer"))
        }
    }

    fn maybe_finish_get(&mut self, connection: u64, transfer: u64) {
        let done = {
            let Some(open) = self
                .sessions
                .get(&connection)
                .and_then(|session| session.ops.get(&transfer))
            else {
                return;
            };
            let Operation::Get(get) = &open.operation else {
                return;
            };
            !get.terminal
                && !get.cancelled
                && get.next_grant >= get.grants.len()
                && get.outstanding.is_empty()
                && get.ready_windows.is_empty()
                && get.window_reads == 0
                && get.pending_reads == 0
                && get.next_meta >= get.records.len()
        };
        if done {
            let (namespace, pins) = {
                let Some(open) = self
                    .sessions
                    .get_mut(&connection)
                    .and_then(|session| session.ops.get_mut(&transfer))
                else {
                    return;
                };
                let Operation::Get(get) = &mut open.operation else {
                    return;
                };
                get.terminal = true;
                get.finished = true;
                get.outbox.push_back(Ok(GetAnswer::Done));
                (get.namespace, std::mem::take(&mut get.pins))
            };
            self.metrics.gets_ok += 1;
            self.release_pins(namespace, pins);
        }
    }

    fn release_pins(&mut self, id: NamespaceId, pins: VecDeque<Key>) {
        let namespace = self.namespaces.get_mut(&id).unwrap();
        for key in pins {
            let _ = namespace.tree.unpin(&key);
        }
    }

    // ----------------------------------------------------------- Cancel, close

    fn cancel(&mut self, connection: u64, transfer: u64) {
        self.metrics.cancels += 1;
        // A Begin that has not reached dispatch yet must be withdrawn too.
        let mut removed_pending = false;
        if let Some(queue) = self.pending_puts.get_mut(&connection) {
            if let Some(index) = queue.iter().position(|put| put.transfer == transfer) {
                queue.remove(index);
                removed_pending = true;
            }
            if queue.is_empty() {
                self.pending_puts.remove(&connection);
                self.put_order.retain(|id| *id != connection);
            }
        }
        if let Some(queue) = self.pending_gets.get_mut(&connection) {
            if let Some(index) = queue.iter().position(|get| get.transfer == transfer) {
                queue.remove(index);
                removed_pending = true;
            }
            if queue.is_empty() {
                self.pending_gets.remove(&connection);
                self.get_order.retain(|id| *id != connection);
            }
        }
        if removed_pending {
            return;
        }
        let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.remove(&transfer))
        else {
            return;
        };
        match open.operation {
            Operation::Put(mut put) => {
                if put.committed {
                    // Commit won: keep the operation and its completion ownership.
                    self.sessions.get_mut(&connection).unwrap().ops.insert(
                        transfer,
                        Open {
                            operation: Operation::Put(put),
                            progress: Instant::now(),
                        },
                    );
                    return;
                }
                put.cancelled = true;
                put.blocked.clear();
                if put.pending == 0 {
                    self.finish_put_drain(connection, transfer, *put);
                } else {
                    let id = put.namespace;
                    put.finished = true;
                    self.retired_puts.push(PutDrain {
                        connection,
                        transfer,
                        namespace: id,
                        key: put.header.key,
                        pins: put.pins,
                        pending: put.pending,
                        reservation: put.reservation,
                        _temporary: put.temporary,
                        _staging_hold: put.staging_hold,
                    });
                }
            }
            Operation::Get(mut get) => {
                if get.finished {
                    // A failure already transferred the pending reads to a drain.
                    return;
                }
                get.cancelled = true;
                if get.pending_reads == 0 {
                    let pins = std::mem::take(&mut get.pins);
                    self.release_pins(get.namespace, pins);
                } else {
                    let id = get.namespace;
                    self.retired_gets.push(GetDrain {
                        connection,
                        transfer,
                        namespace: id,
                        pins: get.pins,
                        pending: get.pending_reads,
                    });
                }
            }
        }
    }

    fn finish_put_drain(&mut self, _connection: u64, _transfer: u64, put: Put) {
        let id = put.namespace;
        let namespace = self.namespaces.get_mut(&id).unwrap();
        for key in &put.pins {
            let _ = namespace.tree.unpin(key);
        }
        namespace.inflight.remove(&put.header.key);
        self.charged = self.charged.saturating_sub(put.reservation);
    }

    fn connection_closed(&mut self, connection: u64) {
        self.local.close(connection);
        self.subscriptions.retain(|(id, _), _| *id != connection);
        self.pending_gets.remove(&connection);
        self.pending_puts.remove(&connection);
        self.get_order.retain(|id| *id != connection);
        self.put_order.retain(|id| *id != connection);
        let Some(session) = self.sessions.remove(&connection) else {
            return;
        };
        let id = session.namespace;
        for (transfer, open) in session.ops {
            match open.operation {
                Operation::Put(mut put) => {
                    if put.committed {
                        put.terminal = true;
                        self.detached_puts.insert((connection, transfer), *put);
                        continue;
                    }
                    let mut put = *put;
                    put.blocked.clear();
                    if put.pending == 0 {
                        self.finish_detached_put(id, put);
                    } else {
                        put.finished = true;
                        self.retired_puts.push(PutDrain {
                            connection,
                            transfer,
                            namespace: id.unwrap(),
                            key: put.header.key,
                            pins: put.pins,
                            pending: put.pending,
                            reservation: put.reservation,
                            _temporary: put.temporary,
                            _staging_hold: put.staging_hold,
                        });
                    }
                }
                Operation::Get(get) => {
                    if get.finished {
                        continue;
                    }
                    if get.pending_reads == 0 {
                        if let Some(id) = id {
                            let namespace = self.namespaces.get_mut(&id).unwrap();
                            for key in &get.pins {
                                let _ = namespace.tree.unpin(key);
                            }
                        }
                    } else {
                        self.retired_gets.push(GetDrain {
                            connection,
                            transfer,
                            namespace: id.unwrap(),
                            pins: get.pins,
                            pending: get.pending_reads,
                        });
                    }
                }
            }
        }
    }

    fn finish_detached_put(&mut self, id: Option<NamespaceId>, put: Put) {
        if let Some(id) = id {
            let namespace = self.namespaces.get_mut(&id).unwrap();
            for key in &put.pins {
                let _ = namespace.tree.unpin(key);
            }
            namespace.inflight.remove(&put.header.key);
        }
        self.charged = self.charged.saturating_sub(put.reservation);
    }

    fn publish_detached_put(&mut self, connection: u64, transfer: u64, disk: Arc<DiskExtent>) {
        let Some(put) = self.detached_puts.remove(&(connection, transfer)) else {
            return;
        };
        let id = put.namespace;
        if self
            .namespaces
            .get(&id)
            .is_none_or(|namespace| namespace.epoch != put.epoch)
        {
            // The worker renamed the file into place before the epoch was checked.
            disk.retire();
            self.finish_detached_put(Some(id), put);
            return;
        }
        let key = put.header.key;
        let memory = put.memory.clone();
        let header = put.header.clone();
        let bytes = record_charge(&header, memory.is_some());
        let pins = put.pins.clone();
        let reservation = put.reservation;
        let target = self.publish_target(id, &header);
        let replace = match target {
            PublishTarget::Insert => false,
            PublishTarget::Replace => true,
            PublishTarget::Present | PublishTarget::Conflict => {
                self.finish_detached_put(Some(id), put);
                return;
            }
        };
        let location = Location {
            header: header.clone(),
            memory,
            disk: Some(disk),
            durable: true,
            writing: false,
            job: None,
            retry: Instant::now(),
            bytes,
        };
        let inserted = {
            let namespace = self.namespaces.get_mut(&id).unwrap();
            let inserted = if replace {
                let node = namespace.tree.node_mut(&key).unwrap();
                let old = std::mem::replace(&mut node.location, location);
                node.row = true;
                self.retired_records.push((id, key, old));
                true
            } else {
                namespace
                    .tree
                    .insert(
                        key,
                        header.parent,
                        header.bucket,
                        header.tail_pages as usize,
                        true,
                        location,
                    )
                    .is_ok()
            };
            for key in &pins {
                let _ = namespace.tree.unpin(key);
            }
            namespace.inflight.remove(&key);
            inserted
        };
        self.charged = self.charged.saturating_sub(reservation) + bytes;
        if inserted {
            self.clock += 1;
            let clock = self.clock;
            self.namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .access_at(&key, clock);
            self.refresh_durability(id);
            self.metrics.puts_ok += 1;
            self.delta(id, if replace { 1 } else { 0 }, key);
        } else {
            self.finish_detached_put(Some(id), put);
        }
    }

    // ------------------------------------------------------------- Maintenance

    fn flush_outboxes(&mut self) {
        let connections: Vec<u64> = self.sessions.keys().copied().collect();
        let mut remove = Vec::new();
        for connection in connections {
            let Some(session) = self.sessions.get_mut(&connection) else {
                continue;
            };
            for (transfer, open) in &mut session.ops {
                match &mut open.operation {
                    Operation::Put(put) => {
                        while let Some(answer) = put.outbox.pop_front() {
                            match put.events.try_send(answer) {
                                Ok(()) => {}
                                Err(tokio::sync::mpsc::error::TrySendError::Full(answer)) => {
                                    put.outbox.push_front(answer);
                                    break;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                            }
                        }
                        if put.terminal && put.outbox.is_empty() && put.pending == 0 {
                            remove.push((connection, *transfer));
                        }
                    }
                    Operation::Get(get) => {
                        while let Some(answer) = get.outbox.pop_front() {
                            match get.events.try_send(answer) {
                                Ok(()) => {}
                                Err(tokio::sync::mpsc::error::TrySendError::Full(answer)) => {
                                    get.outbox.push_front(answer);
                                    break;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                            }
                        }
                        if get.terminal && get.outbox.is_empty() {
                            remove.push((connection, *transfer));
                        }
                    }
                }
            }
        }
        for (connection, transfer) in remove {
            if let Some(session) = self.sessions.get_mut(&connection) {
                session.ops.remove(&transfer);
            }
        }
    }

    fn settle_terminal_puts(&mut self) {
        let mut ops = Vec::new();
        for (connection, session) in &self.sessions {
            for (transfer, open) in &session.ops {
                if matches!(&open.operation, Operation::Put(put) if put.terminal && !put.finished && put.pending == 0)
                {
                    ops.push((*connection, *transfer));
                }
            }
        }
        for (connection, transfer) in ops {
            self.settle_put(connection, transfer);
        }
    }

    fn cleanup_turn(&mut self) {
        self.retired_puts.retain(|drain| {
            if drain.pending > 0 {
                return true;
            }
            let namespace = self.namespaces.get_mut(&drain.namespace).unwrap();
            for key in &drain.pins {
                let _ = namespace.tree.unpin(key);
            }
            namespace.inflight.remove(&drain.key);
            self.charged = self.charged.saturating_sub(drain.reservation);
            false
        });
        self.retired_gets.retain(|drain| {
            if drain.pending > 0 {
                return true;
            }
            let namespace = self.namespaces.get_mut(&drain.namespace).unwrap();
            for key in &drain.pins {
                let _ = namespace.tree.unpin(key);
            }
            false
        });
        self.deferred_disk.retain(|extent| extent.references() > 1);
        self.retired_records.retain(|(id, key, location)| {
            let replaced = self.namespaces[id]
                .tree
                .node(key)
                .is_none_or(|node| node.location.durable);
            if !replaced || location.reading() {
                true
            } else {
                self.charged = self.charged.saturating_sub(location.bytes);
                false
            }
        });
    }

    fn disk_events(&mut self) {
        let mut completions = Vec::new();
        for namespace in self.namespaces.values() {
            let Some(worker) = &namespace.disk else {
                continue;
            };
            for _ in 0..16 {
                let Ok(completion) = worker.receiver.try_recv() else {
                    break;
                };
                completions.push(completion);
            }
        }
        if !completions.is_empty() {
            self.disk_progress = Instant::now();
        }
        for completion in completions {
            self.metrics.nvme_bytes += completion.bytes;
            self.metrics.nvme_seconds += completion.elapsed.as_secs_f64();
            let failed = completion.result.is_err();
            if failed {
                self.metrics.nvme_errors += 1;
            }
            let Some(job) = self.jobs.remove(&completion.id) else {
                continue;
            };
            match job {
                Job::Write { namespace, key } => {
                    let path = self.namespaces[&namespace].path(key);
                    let Some(node) = self
                        .namespaces
                        .get_mut(&namespace)
                        .and_then(|namespace| namespace.tree.node_mut(&key))
                    else {
                        // A purge removed the record while its write drained; the fresh file
                        // is retired and never enters the tree.
                        if !failed && let Some(path) = path {
                            let extent = Arc::new(DiskExtent::new(path));
                            extent.retire();
                            self.deferred_disk.push(extent);
                        }
                        continue;
                    };
                    if node.location.job != Some(completion.id) {
                        // The record this write belonged to was replaced. The replacement owns
                        // the key's file now, so the stale completion must not touch either the
                        // node or that path.
                        continue;
                    }
                    node.location.job = None;
                    node.location.writing = false;
                    if !failed {
                        if let Some(path) = path {
                            node.location.disk = Some(Arc::new(DiskExtent::new(path)));
                        }
                        node.location.retry = Instant::now();
                    } else {
                        node.location.retry = Instant::now() + Duration::from_millis(100);
                    }
                    self.refresh_durability(namespace);
                }
                Job::Chunk {
                    connection,
                    transfer,
                } => {
                    for drain in &mut self.retired_puts {
                        if drain.connection == connection && drain.transfer == transfer {
                            drain.pending = drain.pending.saturating_sub(1);
                        }
                    }
                    let mut attached = false;
                    if let Some(open) = self
                        .sessions
                        .get_mut(&connection)
                        .and_then(|session| session.ops.get_mut(&transfer))
                    {
                        if let Operation::Put(put) = &mut open.operation {
                            put.pending = put.pending.saturating_sub(1);
                            release_slot_job(put, completion.id);
                            open.progress = Instant::now();
                            attached = true;
                        }
                    }
                    let mut detached_drained = false;
                    if let Some(put) = self.detached_puts.get_mut(&(connection, transfer)) {
                        put.pending = put.pending.saturating_sub(1);
                        release_slot_job(put, completion.id);
                        if failed {
                            put.cancelled = true;
                            put.blocked.clear();
                        }
                        detached_drained = put.pending == 0 && put.cancelled;
                    }
                    if failed {
                        if attached {
                            self.fail_put(
                                connection,
                                transfer,
                                proto::Reason::Io as u8,
                                invalid("streaming disk write failed"),
                            );
                            self.settle_put(connection, transfer);
                        } else if detached_drained {
                            if let Some(put) = self.detached_puts.remove(&(connection, transfer)) {
                                let id = put.namespace;
                                self.finish_detached_put(Some(id), put);
                            }
                        }
                    } else if attached {
                        self.settle_put(connection, transfer);
                    } else if detached_drained {
                        if let Some(put) = self.detached_puts.remove(&(connection, transfer)) {
                            let id = put.namespace;
                            self.finish_detached_put(Some(id), put);
                        }
                    }
                }
                Job::Finish {
                    connection,
                    transfer,
                    final_extent,
                } => {
                    let mut attached = false;
                    if let Some(open) = self
                        .sessions
                        .get_mut(&connection)
                        .and_then(|session| session.ops.get_mut(&transfer))
                    {
                        if let Operation::Put(put) = &mut open.operation {
                            put.pending = put.pending.saturating_sub(1);
                            release_slot_job(put, completion.id);
                            open.progress = Instant::now();
                            attached = true;
                        }
                    }
                    if let Some(put) = self.detached_puts.get_mut(&(connection, transfer)) {
                        put.pending = put.pending.saturating_sub(1);
                    }
                    if failed {
                        self.metrics.puts_error += 1;
                        if attached {
                            self.fail_put(
                                connection,
                                transfer,
                                proto::Reason::Io as u8,
                                invalid("streaming disk write failed"),
                            );
                            self.settle_put(connection, transfer);
                        } else if let Some(put) = self.detached_puts.remove(&(connection, transfer))
                        {
                            let id = put.namespace;
                            self.finish_detached_put(Some(id), put);
                        }
                    } else {
                        // Check the epoch before the extent is made permanent: a Put whose
                        // namespace was purged must not leave a never-referenced file behind.
                        let purged = self
                            .sessions
                            .get(&connection)
                            .and_then(|session| session.ops.get(&transfer))
                            .and_then(|open| match &open.operation {
                                Operation::Put(put) => Some((put.namespace, put.epoch)),
                                Operation::Get(_) => None,
                            })
                            .or_else(|| {
                                self.detached_puts
                                    .get(&(connection, transfer))
                                    .map(|put| (put.namespace, put.epoch))
                            })
                            .is_some_and(|(namespace, epoch)| {
                                self.namespaces
                                    .get(&namespace)
                                    .is_none_or(|namespace| namespace.epoch != epoch)
                            });
                        if purged {
                            // The publication was renamed into place before the epoch was
                            // checked; remove the file so recovery cannot resurrect it.
                            final_extent.retire();
                            if attached {
                                self.fail_put(
                                    connection,
                                    transfer,
                                    proto::Reason::Invalid as u8,
                                    invalid("namespace was purged while publishing"),
                                );
                                self.settle_put(connection, transfer);
                            } else if let Some(put) =
                                self.detached_puts.remove(&(connection, transfer))
                            {
                                let id = put.namespace;
                                self.finish_detached_put(Some(id), put);
                            }
                            continue;
                        }
                        final_extent.keep();
                        if attached {
                            self.publish_put(connection, transfer, Some(final_extent), true);
                        } else {
                            self.publish_detached_put(connection, transfer, final_extent);
                        }
                    }
                }
                Job::Compact {
                    namespace,
                    key,
                    bytes,
                } => {
                    let path = self.namespaces[&namespace].path(key);
                    let _ = self
                        .namespaces
                        .get_mut(&namespace)
                        .unwrap()
                        .tree
                        .unpin(&key);
                    let Some(node) = self
                        .namespaces
                        .get_mut(&namespace)
                        .and_then(|namespace| namespace.tree.node_mut(&key))
                    else {
                        continue;
                    };
                    if node.location.job != Some(completion.id) {
                        // The record was replaced; its successor owns this path.
                        continue;
                    }
                    node.location.job = None;
                    node.location.writing = false;
                    if !failed {
                        node.location.bytes -= bytes;
                        self.charged -= bytes;
                        node.location.memory = None;
                        if let Some(path) = path {
                            node.location.disk = Some(Arc::new(DiskExtent::new(path)));
                        }
                        self.delta(namespace, 1, key);
                    } else {
                        node.row = true;
                        self.delta(namespace, 1, key);
                    }
                    self.refresh_durability(namespace);
                }
                Job::GetRead {
                    connection,
                    transfer,
                    record,
                    kind,
                } => {
                    if failed {
                        self.read_failed(connection, transfer);
                    } else {
                        self.get_read_done(connection, transfer, record, kind, completion.data);
                    }
                }
            }
        }
    }

    fn settle_put(&mut self, connection: u64, transfer: u64) {
        let released = {
            let Some(open) = self
                .sessions
                .get_mut(&connection)
                .and_then(|session| session.ops.get_mut(&transfer))
            else {
                return;
            };
            let Operation::Put(put) = &mut open.operation else {
                return;
            };
            if !put.terminal || put.finished || put.pending > 0 {
                return;
            }
            put.finished = true;
            put.blocked.clear();
            Some((
                put.namespace,
                std::mem::take(&mut put.pins),
                put.header.key,
                put.reservation,
            ))
        };
        let Some((id, pins, key, reservation)) = released else {
            return;
        };
        let namespace = self.namespaces.get_mut(&id).unwrap();
        for key in &pins {
            let _ = namespace.tree.unpin(key);
        }
        namespace.inflight.remove(&key);
        self.charged = self.charged.saturating_sub(reservation);
    }

    fn pump_reads(&mut self) {
        let pending = std::mem::take(&mut self.read_backlog);
        for (namespace, job, task) in pending {
            let Some(worker) = &self.namespaces[&namespace].disk else {
                continue;
            };
            if let Err(std::sync::mpsc::TrySendError::Full(task)) =
                worker.read_sender.try_send(task)
            {
                self.read_backlog.push_back((namespace, job, task));
            }
        }
    }

    fn pump_chunks(&mut self) {
        let connections: Vec<u64> = self.sessions.keys().copied().collect();
        for connection in connections {
            let transfers: Vec<u64> = self.sessions[&connection]
                .ops
                .iter()
                .filter(|(_, open)| matches!(open.operation, Operation::Put(_)))
                .map(|(transfer, _)| *transfer)
                .collect();
            for transfer in transfers {
                loop {
                    let next = {
                        let Some(open) = self
                            .sessions
                            .get_mut(&connection)
                            .and_then(|session| session.ops.get_mut(&transfer))
                        else {
                            break;
                        };
                        let Operation::Put(put) = &mut open.operation else {
                            break;
                        };
                        let Some((job, offset, data)) = put.blocked.pop_front() else {
                            break;
                        };
                        put.temporary
                            .clone()
                            .map(|temporary| (job, offset, data, temporary))
                    };
                    let Some((job, offset, data, temporary)) = next else {
                        break;
                    };
                    let namespace = self
                        .sessions
                        .get(&connection)
                        .and_then(|session| session.ops.get(&transfer))
                        .and_then(|open| match &open.operation {
                            Operation::Put(put) => Some(put.namespace),
                            Operation::Get(_) => None,
                        });
                    let Some(namespace) = namespace else {
                        break;
                    };
                    let Some(worker) = &self.namespaces[&namespace].disk else {
                        self.fail_put(
                            connection,
                            transfer,
                            proto::Reason::Io as u8,
                            invalid("no disk"),
                        );
                        break;
                    };
                    let task = Task::Chunk {
                        id: job,
                        temporary,
                        offset,
                        data,
                    };
                    match worker.sender.try_send(task) {
                        Ok(()) => {
                            self.jobs.insert(
                                job,
                                Job::Chunk {
                                    connection,
                                    transfer,
                                },
                            );
                            if let Some(open) = self
                                .sessions
                                .get_mut(&connection)
                                .and_then(|session| session.ops.get_mut(&transfer))
                            {
                                if let Operation::Put(put) = &mut open.operation {
                                    put.pending += 1;
                                }
                            }
                        }
                        Err(std::sync::mpsc::TrySendError::Full(task)) => {
                            if let Task::Chunk { data, .. } = task {
                                if let Some(open) = self
                                    .sessions
                                    .get_mut(&connection)
                                    .and_then(|session| session.ops.get_mut(&transfer))
                                {
                                    if let Operation::Put(put) = &mut open.operation {
                                        put.blocked.push_front((job, offset, data));
                                    }
                                }
                            }
                            break;
                        }
                        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                            self.fail_put(
                                connection,
                                transfer,
                                proto::Reason::Io as u8,
                                invalid("disk worker stopped"),
                            );
                            break;
                        }
                    }
                }
            }
        }
        let detached: Vec<(u64, u64)> = self.detached_puts.keys().copied().collect();
        for (connection, transfer) in detached {
            loop {
                let next = {
                    let Some(put) = self.detached_puts.get_mut(&(connection, transfer)) else {
                        break;
                    };
                    if put.cancelled {
                        break;
                    }
                    let Some((job, offset, data)) = put.blocked.pop_front() else {
                        break;
                    };
                    put.temporary
                        .clone()
                        .map(|temporary| (job, offset, data, temporary, put.namespace))
                };
                let Some((job, offset, data, temporary, namespace)) = next else {
                    break;
                };
                let Some(worker) = &self.namespaces[&namespace].disk else {
                    if let Some(put) = self.detached_puts.remove(&(connection, transfer)) {
                        self.finish_detached_put(Some(namespace), put);
                    }
                    break;
                };
                let task = Task::Chunk {
                    id: job,
                    temporary,
                    offset,
                    data,
                };
                match worker.sender.try_send(task) {
                    Ok(()) => {
                        self.jobs.insert(
                            job,
                            Job::Chunk {
                                connection,
                                transfer,
                            },
                        );
                        if let Some(put) = self.detached_puts.get_mut(&(connection, transfer)) {
                            put.pending += 1;
                        }
                    }
                    Err(std::sync::mpsc::TrySendError::Full(task)) => {
                        if let Task::Chunk { data, .. } = task {
                            if let Some(put) = self.detached_puts.get_mut(&(connection, transfer)) {
                                put.blocked.push_front((job, offset, data));
                            }
                        }
                        break;
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        if let Some(put) = self.detached_puts.remove(&(connection, transfer)) {
                            self.finish_detached_put(Some(namespace), put);
                        }
                        break;
                    }
                }
            }
        }
    }

    fn schedule(&mut self) {
        let ids: Vec<NamespaceId> = self.namespaces.keys().copied().collect();
        for id in ids {
            if self.namespaces[&id].disk.is_none() {
                continue;
            }
            let namespace = &self.namespaces[&id];
            let mut keys: Vec<Key> = namespace
                .tree
                .nodes()
                .filter(|node| {
                    !node.location.durable
                        && !node.location.writing
                        && node.location.memory.is_some()
                        && node.location.retry <= Instant::now()
                        && namespace.parent_durable(node.parent)
                })
                .map(|node| node.key)
                .collect();
            keys.sort_unstable();
            for key in keys {
                self.job_counter += 1;
                let job = self.job_counter;
                let namespace = &self.namespaces[&id];
                let node = &namespace.tree[&key];
                let Some(memory) = node.location.memory.clone() else {
                    continue;
                };
                let Some(temporary) = namespace.temporary(job) else {
                    continue;
                };
                let Some(path) = namespace.path(key) else {
                    continue;
                };
                let task = Task::Publish {
                    id: job,
                    temporary,
                    destination: Arc::new(DiskExtent::new(path)),
                    data: RecordData::Memory(memory),
                    header: node.location.header.clone(),
                    row: node.row,
                };
                let Some(worker) = &namespace.disk else {
                    continue;
                };
                if worker.sender.try_send(task).is_ok() {
                    self.jobs.insert(job, Job::Write { namespace: id, key });
                    let location = &mut self.namespaces.get_mut(&id).unwrap().tree[&key].location;
                    location.writing = true;
                    location.job = Some(job);
                } else {
                    break;
                }
            }
        }
        let connections: Vec<u64> = self.sessions.keys().copied().collect();
        for connection in connections {
            let ops: Vec<(u64, NamespaceId)> = self.sessions[&connection]
                .ops
                .iter()
                .filter_map(|(transfer, open)| match &open.operation {
                    Operation::Put(put)
                        if put.committed
                            && put.metadata.is_some()
                            && put.pending == 0
                            && put.blocked.is_empty()
                            && put.temporary.is_some()
                            && !put.terminal
                            && !put.finalizing =>
                    {
                        Some((*transfer, put.namespace))
                    }
                    _ => None,
                })
                .collect();
            for (transfer, id) in ops {
                self.schedule_finish(connection, transfer, id);
            }
        }
        let detached: Vec<(u64, u64)> = self.detached_puts.keys().copied().collect();
        for (connection, transfer) in detached {
            let Some(put) = self.detached_puts.get(&(connection, transfer)) else {
                continue;
            };
            if put.committed && put.metadata.is_some() && put.pending == 0 && put.blocked.is_empty()
            {
                let id = put.namespace;
                self.schedule_detached_finish(connection, transfer, id);
            }
        }
    }

    fn schedule_finish(&mut self, connection: u64, transfer: u64, id: NamespaceId) {
        let Some((epoch, parent, terminal)) = self
            .sessions
            .get(&connection)
            .and_then(|session| session.ops.get(&transfer))
            .and_then(|open| match &open.operation {
                Operation::Put(put) => Some((put.epoch, put.header.parent, put.terminal)),
                Operation::Get(_) => None,
            })
        else {
            return;
        };
        if terminal {
            return;
        }
        if self.namespaces[&id].epoch != epoch {
            // The namespace was purged; never rename this publication into place.
            self.fail_put(
                connection,
                transfer,
                proto::Reason::Invalid as u8,
                invalid("namespace was purged while publishing"),
            );
            self.settle_put(connection, transfer);
            return;
        }
        if !self.namespaces[&id].parent_durable(parent) {
            return;
        }
        let Some(open) = self
            .sessions
            .get_mut(&connection)
            .and_then(|session| session.ops.get_mut(&transfer))
        else {
            return;
        };
        let Operation::Put(put) = &mut open.operation else {
            return;
        };
        let Some(metadata) = put.metadata.clone() else {
            return;
        };
        let Some(temporary) = put.temporary.clone() else {
            return;
        };
        self.job_counter += 1;
        let job = self.job_counter;
        let Some(path) = self.namespaces[&id].path(put.header.key) else {
            return;
        };
        let final_extent = Arc::new(DiskExtent::new(path));
        let task = Task::Publish {
            id: job,
            temporary,
            destination: final_extent.clone(),
            data: RecordData::Metadata(metadata),
            header: put.header.clone(),
            row: true,
        };
        let Some(worker) = &self.namespaces[&id].disk else {
            return;
        };
        if worker.sender.try_send(task).is_ok() {
            put.finalizing = true;
            put.pending += 1;
            self.jobs.insert(
                job,
                Job::Finish {
                    connection,
                    transfer,
                    final_extent,
                },
            );
        }
    }

    fn schedule_detached_finish(&mut self, connection: u64, transfer: u64, id: NamespaceId) {
        let Some(put) = self.detached_puts.get(&(connection, transfer)) else {
            return;
        };
        if self.namespaces[&id].epoch != put.epoch {
            // The namespace was purged; never rename this publication into place.
            if let Some(put) = self.detached_puts.remove(&(connection, transfer)) {
                self.finish_detached_put(Some(id), put);
            }
            return;
        }
        let Some(put) = self.detached_puts.get_mut(&(connection, transfer)) else {
            return;
        };
        let Some(metadata) = put.metadata.clone() else {
            return;
        };
        let Some(temporary) = put.temporary.clone() else {
            return;
        };
        if !self.namespaces[&id].parent_durable(put.header.parent) {
            return;
        }
        self.job_counter += 1;
        let job = self.job_counter;
        let Some(path) = self.namespaces[&id].path(put.header.key) else {
            return;
        };
        let final_extent = Arc::new(DiskExtent::new(path));
        let task = Task::Publish {
            id: job,
            temporary,
            destination: final_extent.clone(),
            data: RecordData::Metadata(metadata),
            header: put.header.clone(),
            row: true,
        };
        let Some(worker) = &self.namespaces[&id].disk else {
            return;
        };
        if worker.sender.try_send(task).is_ok() {
            put.finalizing = true;
            put.pending += 1;
            self.jobs.insert(
                job,
                Job::Finish {
                    connection,
                    transfer,
                    final_extent,
                },
            );
        }
    }

    fn tick(&mut self) {
        self.note_reserved();
        let now = Instant::now();
        if self.jobs.is_empty() {
            self.disk_progress = now;
        }
        self.timeouts();
    }

    fn timeouts(&mut self) {
        let mut expired = Vec::new();
        let disk_idle = (!self.jobs.is_empty()).then(|| self.disk_progress.elapsed());
        for (connection, session) in &self.sessions {
            for (transfer, open) in &session.ops {
                let delivered = matches!(&open.operation, Operation::Put(put) if put.committed);
                if stalled(open.progress.elapsed(), delivered, disk_idle) {
                    expired.push((*connection, *transfer));
                }
            }
        }
        for (connection, transfer) in expired {
            let is_put = self
                .sessions
                .get(&connection)
                .and_then(|session| session.ops.get(&transfer))
                .is_some_and(|open| matches!(open.operation, Operation::Put(_)));
            if is_put {
                self.fail_put(
                    connection,
                    transfer,
                    proto::Reason::Invalid as u8,
                    invalid("no progress"),
                );
            } else {
                self.fail_get(
                    connection,
                    transfer,
                    proto::Reason::Invalid as u8,
                    invalid("no progress"),
                );
            }
        }
    }

    pub fn pump(&mut self) {
        self.pump_reads();
        self.pump_chunks();
        self.disk_events();
        self.dispatch();
        self.pump_grants();
        self.cleanup_turn();
        self.settle_terminal_puts();
        self.schedule();
        self.tick();
        self.maybe_finish_gets();
        self.flush_outboxes();
        self.flush_events();
        self.pump_subscriptions();
        self.local_exits();
    }

    fn maybe_finish_gets(&mut self) {
        let mut ops = Vec::new();
        for (connection, session) in &self.sessions {
            for (transfer, open) in &session.ops {
                if matches!(open.operation, Operation::Get(_)) {
                    ops.push((*connection, *transfer));
                }
            }
        }
        for (connection, transfer) in ops {
            self.maybe_finish_get(connection, transfer);
        }
    }

    fn local_exits(&mut self) {
        let mut exited = Vec::new();
        self.local.poll_exits(&mut exited);
        for connection in exited {
            if let Some(session) = self.sessions.get(&connection) {
                if session.ops.is_empty() {
                    self.connection_closed(connection);
                }
            }
        }
    }

    fn flush_events(&mut self) {
        let events = std::mem::take(&mut self.events);
        for event in events {
            match event {
                StoreEvent::Catalog {
                    namespace,
                    seq,
                    kind,
                    key,
                    entry,
                } => {
                    let mut subscribers: Vec<(u64, NamespaceId)> = self
                        .subscriptions
                        .iter()
                        .filter(|(_, subscription)| subscription.namespace == namespace)
                        .map(|(id, _)| *id)
                        .collect();
                    subscribers.sort_unstable();
                    for connection in subscribers {
                        let live = LiveEvent::Catalog {
                            seq,
                            kind,
                            key,
                            entry: entry.clone(),
                        };
                        if self.buffer_or_send(connection, live) {
                            self.restart_snapshot(connection);
                        }
                    }
                }
            }
        }
    }

    /// Queues a live event behind an in-progress snapshot, or sends it when the
    /// snapshot has ended. Returns true when the snapshot buffer overflowed.
    fn buffer_or_send(&mut self, connection: (u64, NamespaceId), live: LiveEvent) -> bool {
        let seq = match &live {
            LiveEvent::Catalog { seq, .. } => Some(*seq),
        };
        let action = {
            let Some(subscription) = self.subscriptions.get_mut(&connection) else {
                return false;
            };
            // Once a snapshot is in progress, or buffered mutations remain
            // after SnapshotEnd, everything queues behind them.
            if subscription.snapshot.is_some() || !subscription.buffered.is_empty() {
                if seq.is_some_and(|seq| seq <= subscription.seq0) {
                    // Already represented in the snapshot boundary.
                    return false;
                }
                if subscription.buffered.len() >= SNAPSHOT_BUFFER {
                    Some(None)
                } else {
                    subscription.buffered.push_back(live);
                    None
                }
            } else {
                Some(Some(live))
            }
        };
        match action {
            None => false,
            Some(None) => true,
            Some(Some(live)) => {
                self.send_live(connection, live);
                false
            }
        }
    }

    fn send_live(&mut self, connection: (u64, NamespaceId), live: LiveEvent) {
        let mut remove = false;
        {
            let Some(subscription) = self.subscriptions.get_mut(&connection) else {
                return;
            };
            let sequence = subscription.sequence;
            if let Some(value) = live_event(&live, sequence) {
                subscription.sequence += 1;
                if subscription.sender.try_send(value).is_err() {
                    remove = true;
                }
            }
        }
        if remove {
            self.subscriptions.remove(&connection);
        }
    }

    fn restart_snapshot(&mut self, connection: (u64, NamespaceId)) {
        let Some(namespace) = self
            .subscriptions
            .get(&connection)
            .map(|subscription| subscription.namespace)
        else {
            return;
        };
        let Some((seq0, entries)) = self.snapshot(namespace) else {
            return;
        };
        if let Some(subscription) = self.subscriptions.get_mut(&connection) {
            subscription.seq0 = seq0;
            subscription.sequence = seq0 + 1;
            subscription.snapshot = Some(entries.into());
            subscription.begin_pending = true;
            subscription.buffered.clear();
            subscription.restarts += 1;
        }
    }

    fn pump_subscriptions(&mut self) {
        let connections: Vec<(u64, NamespaceId)> = self.subscriptions.keys().copied().collect();
        for connection in connections {
            let mut remove = false;
            {
                let Some(subscription) = self.subscriptions.get_mut(&connection) else {
                    continue;
                };
                let mut blocked = false;
                if subscription.begin_pending {
                    let value = proto::CatalogEvent {
                        sequence: subscription.sequence,
                        event: Some(proto::catalog_event::Event::SnapshotBegin(
                            proto::SnapshotBegin {
                                seq0: subscription.seq0,
                            },
                        )),
                    };
                    match subscription.sender.try_send(value) {
                        Ok(()) => {
                            subscription.sequence += 1;
                            subscription.begin_pending = false;
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => blocked = true,
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => remove = true,
                    }
                }
                if !blocked {
                    if let Some(queue) = &mut subscription.snapshot {
                        while let Some(entry) = queue.pop_front() {
                            let value = proto::CatalogEvent {
                                sequence: subscription.sequence,
                                event: Some(proto::catalog_event::Event::Entry(entry_to_proto(
                                    &entry,
                                ))),
                            };
                            match subscription.sender.try_send(value) {
                                Ok(()) => subscription.sequence += 1,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    queue.push_front(entry);
                                    blocked = true;
                                    break;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    remove = true;
                                    break;
                                }
                            }
                        }
                        if !blocked && queue.is_empty() {
                            let value = proto::CatalogEvent {
                                sequence: subscription.sequence,
                                event: Some(proto::catalog_event::Event::SnapshotEnd(
                                    proto::SnapshotEnd {},
                                )),
                            };
                            match subscription.sender.try_send(value) {
                                Ok(()) => {
                                    subscription.sequence += 1;
                                    subscription.snapshot = None;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    blocked = true;
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    remove = true;
                                }
                            }
                        }
                    }
                }
                if !blocked && !remove {
                    while let Some(live) = subscription.buffered.pop_front() {
                        let Some(value) = live_event(&live, subscription.sequence) else {
                            continue;
                        };
                        match subscription.sender.try_send(value) {
                            Ok(()) => subscription.sequence += 1,
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                subscription.buffered.push_front(live);
                                break;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                remove = true;
                                break;
                            }
                        }
                    }
                }
            }
            if remove {
                self.subscriptions.remove(&connection);
            }
        }
    }

    pub fn subscribe(
        &mut self,
        connection: u64,
        namespace: NamespaceId,
        generation: u64,
        descriptor: Descriptor,
        sender: tokio::sync::mpsc::Sender<proto::CatalogEvent>,
    ) -> Result<()> {
        descriptor.validate()?;
        if descriptor.namespace_id() != namespace {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "descriptor does not match namespace",
            ));
        }
        let session = self
            .sessions
            .get(&connection)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "Subscribe before Hello"))?;
        let bound = session.namespace;
        if session.generation != generation || (!session.remote && bound != Some(namespace)) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Subscribe generation or namespace",
            ));
        }
        if bound.is_some_and(|bound| bound != namespace) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a session may not change its namespace",
            ));
        }
        match self.namespaces.get(&namespace) {
            Some(stored) if stored.descriptor != descriptor => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "namespace descriptor mismatch",
                ));
            }
            Some(_) => {}
            None => {
                let registered = self.register_namespace(descriptor)?;
                if registered != namespace {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "descriptor does not match namespace",
                    ));
                }
            }
        }

        let (seq0, entries) = self
            .snapshot(namespace)
            .ok_or_else(|| invalid("namespace"))?;
        self.subscriptions.insert(
            (connection, namespace),
            Subscription {
                namespace,
                sender,
                sequence: seq0 + 1,
                seq0,
                snapshot: Some(entries.into()),
                begin_pending: true,
                buffered: VecDeque::new(),
                restarts: 0,
            },
        );
        Ok(())
    }

    pub fn metrics_text(&self) -> String {
        let mut text = String::new();
        for (name, value) in [
            ("unknown_frames_total", self.metrics.dropped),
            ("cancels_total", self.metrics.cancels),
            ("evictions_total", self.metrics.evictions),
            ("compactions_total", self.metrics.compactions),
            ("nvme_bytes_total", self.metrics.nvme_bytes),
            ("nvme_errors_total", self.metrics.nvme_errors),
            ("connections", self.sessions.len() as u64),
            (
                "leases",
                self.sessions
                    .values()
                    .flat_map(|session| session.ops.values())
                    .filter(|open| matches!(open.operation, Operation::Get(_)))
                    .count() as u64,
            ),
            (
                "open_ops",
                self.sessions
                    .values()
                    .map(|session| session.ops.len() as u64)
                    .sum(),
            ),
            ("memory_allocated_bytes", self.local.arena_used() as u64),
            ("staging_allocated_bytes", self.local.staging_used() as u64),
            ("nvme_reserved_bytes", self.charge_total()),
            (
                "nvme_reserved_peak_bytes",
                self.reserved_peak.max(self.charge_total()),
            ),
            (
                "cleanup_records",
                self.retired_gets
                    .iter()
                    .map(|drain| drain.pins.len() as u64)
                    .sum::<u64>()
                    + self
                        .retired_puts
                        .iter()
                        .map(|drain| drain.pins.len() as u64)
                        .sum::<u64>(),
            ),
            (
                "pending_puts",
                self.pending_puts.values().map(|q| q.len() as u64).sum(),
            ),
            (
                "pending_gets",
                self.pending_gets.values().map(|q| q.len() as u64).sum(),
            ),
        ] {
            text.push_str(&format!("selene_{name} {value}\n"));
        }
        text.push_str(&format!(
            "selene_nvme_seconds_total {}\n",
            self.metrics.nvme_seconds
        ));
        for (outcome, count) in [
            ("ok", self.metrics.puts_ok),
            ("error", self.metrics.puts_error),
            ("present", self.metrics.present),
            ("queue", self.metrics.retry_queue),
            ("budget", self.metrics.retry_budget),
            ("inflight", self.metrics.retry_inflight),
            ("oversize", self.metrics.oversize),
        ] {
            text.push_str(&format!(
                "selene_puts_total{{outcome=\"{outcome}\"}} {count}\n"
            ));
        }
        for (outcome, count) in [
            ("ok", self.metrics.gets_ok),
            ("error", self.metrics.gets_error),
        ] {
            text.push_str(&format!(
                "selene_gets_total{{outcome=\"{outcome}\"}} {count}\n"
            ));
        }
        let mut ids: Vec<&NamespaceId> = self.namespaces.keys().collect();
        ids.sort_unstable();
        for id in ids {
            let name = record::hex(id);
            let namespace = &self.namespaces[id];
            for tier in ["memory", "disk"] {
                let mut count = 0;
                let mut bytes = 0;
                for node in namespace.tree.nodes() {
                    let retained = if tier == "memory" {
                        node.location.memory.is_some()
                    } else {
                        node.location.durable
                    };
                    if retained {
                        count += 1;
                        bytes += if tier == "memory" {
                            node.location.header.length(node.row)
                        } else {
                            node.location.bytes
                        };
                    }
                }
                text.push_str(&format!("selene_records{{namespace=\"{name}\",tier=\"{tier}\"}} {count}\nselene_bytes{{namespace=\"{name}\",tier=\"{tier}\"}} {bytes}\n"));
                let hits = self
                    .metrics
                    .hits
                    .get(&(name.clone(), tier))
                    .copied()
                    .unwrap_or(0);
                text.push_str(&format!(
                    "selene_hits_total{{namespace=\"{name}\",tier=\"{tier}\"}} {hits}\n"
                ));
            }
        }
        text
    }
}

pub type Reply<T> = tokio::sync::oneshot::Sender<T>;

pub struct HelloReply {
    pub generation: u64,
    pub namespace: NamespaceId,
    pub arena_path: String,
    pub arena_bytes: u64,
    pub staging_path: String,
    pub staging_bytes: u64,
    pub max_window_bytes: u64,
}

pub struct PutAccept {
    pub transfer: u64,
    pub first_page: u32,
    pub parent: Option<Key>,
    pub record_bytes: u64,
}

pub struct WindowGrant {
    pub transfer: u64,
    pub index: u32,
    pub record: u32,
    pub record_offset: u64,
    pub length: u64,
    pub region: i32,
    pub offset: u64,
    pub page_first: u64,
    pub page_count: u32,
    pub row: bool,
}

pub struct Refused {
    pub reason: u8,
    pub detail: String,
}

pub enum PutAnswer {
    Accept(PutAccept),
    Grant(WindowGrant),
    Stored { bytes: u64 },
    Refused(Refused),
}

pub enum GetAnswer {
    Leased {
        transfer: u64,
        record_bytes: Vec<u64>,
    },
    Meta {
        record: u32,
        metadata_offset: u64,
        metadata: Vec<u8>,
        source: i32,
    },
    Ready(WindowGrant),
    Done,
    Refused(Refused),
}

pub enum StoreCommand {
    Hello {
        connection: u64,
        replica: String,
        pid: u32,
        descriptor: Descriptor,
        reply: Reply<Result<HelloReply>>,
    },
    Subscribe {
        connection: u64,
        namespace: NamespaceId,
        generation: u64,
        descriptor: Descriptor,
        events: tokio::sync::mpsc::Sender<proto::CatalogEvent>,
        reply: Reply<Result<()>>,
    },
    PutBegin {
        connection: u64,
        transfer: u64,
        generation: u64,
        namespace: NamespaceId,
        descriptor: Descriptor,
        key: Key,
        published_parent: Option<Key>,
        bucket: [u8; 16],
        chain_pages: u32,
        published_tail_pages: u32,
        row: bool,
        events: tokio::sync::mpsc::Sender<Result<PutAnswer>>,
        reply: Reply<Result<()>>,
    },
    WindowFilled {
        connection: u64,
        generation: u64,
        transfer: u64,
        index: u32,
        page_xxh3: Vec<u64>,
        row_xxh3: Option<u64>,
        reply: Reply<Result<()>>,
    },
    PutCommit {
        connection: u64,
        generation: u64,
        transfer: u64,
        metadata_offset: u64,
        metadata: Vec<u8>,
        reply: Reply<Result<()>>,
    },
    GetBegin {
        connection: u64,
        transfer: u64,
        generation: u64,
        namespace: NamespaceId,
        chain: Vec<Key>,
        skip_pages: u32,
        expected_pages: u32,
        row: bool,
        events: tokio::sync::mpsc::Sender<Result<GetAnswer>>,
        reply: Reply<Result<()>>,
    },
    WindowReleased {
        connection: u64,
        generation: u64,
        transfer: u64,
        index: u32,
        reply: Reply<Result<()>>,
    },
    Cancel {
        connection: u64,
        generation: u64,
        transfer: u64,
    },
    ConnectionClosed {
        connection: u64,
    },
    ProcessExited {
        connection: u64,
    },
    RemoteHello {
        connection: u64,
        node: String,
        reply: Reply<Result<u64>>,
    },
    /// Release the windows of one transfer once the party that held them has drained.
    ReleaseTransfer {
        connection: u64,
        generation: u64,
        transfer: u64,
    },
    Tick,
    Shutdown,
}

fn live_event(live: &LiveEvent, sequence: u64) -> Option<proto::CatalogEvent> {
    match live {
        LiveEvent::Catalog {
            kind, key, entry, ..
        } => {
            let event = if *kind == 2 {
                proto::catalog_event::Event::Removed(key_to_proto(key))
            } else {
                let entry = entry.as_ref()?;
                let proto = entry_to_proto(entry);
                if *kind == 0 {
                    proto::catalog_event::Event::Added(proto)
                } else {
                    proto::catalog_event::Event::Changed(proto)
                }
            };
            Some(proto::CatalogEvent {
                sequence,
                event: Some(event),
            })
        }
    }
}

fn region_wire(region: Region) -> i32 {
    match region {
        Region::Arena => proto::Region::Arena as i32,
        Region::Staging => proto::Region::Staging as i32,
    }
}

fn plan_put_grants(header: &Header, first_page: u32, max_window: u64) -> Vec<PlannedGrant> {
    let mut grants = Vec::new();
    plan_record_grants(
        header,
        first_page,
        0,
        true,
        max_window,
        &mut |record_offset, length, pages, row, page_first| {
            grants.push(PlannedGrant {
                record_offset,
                length,
                pages,
                row,
                page_first,
            });
        },
    );
    grants
}

fn plan_get_record_grants(
    header: &Header,
    first_page: u32,
    skip: u32,
    include_row: bool,
    max_window: u64,
    record: u32,
    grants: &mut Vec<PlannedGetGrant>,
) {
    plan_record_grants(
        header,
        first_page,
        skip,
        include_row,
        max_window,
        &mut |record_offset, length, pages, row, page_first| {
            grants.push(PlannedGetGrant {
                record,
                record_offset,
                length,
                pages,
                row,
                page_first,
            });
        },
    );
}

fn plan_record_grants(
    header: &Header,
    first_page: u32,
    skip: u32,
    include_row: bool,
    max_window: u64,
    push: &mut dyn FnMut(u64, u64, u32, bool, u64),
) {
    let page_bytes = header.page_record_bytes();
    let row_bytes = header.row_record_bytes();
    let per_grant = if page_bytes > max_window {
        1
    } else {
        ((max_window / page_bytes) as u32).clamp(1, 64)
    };
    let mut page = skip;
    let mut row_done = !include_row || row_bytes == 0;
    while page < header.tail_pages {
        let count = (header.tail_pages - page).min(per_grant).max(1);
        let pages_length = u64::from(count) * page_bytes;
        let last = page + count == header.tail_pages;
        let with_row = last && !row_done && pages_length + row_bytes <= max_window;
        push(
            header.page_segment + u64::from(page) * page_bytes,
            pages_length + if with_row { row_bytes } else { 0 },
            count,
            with_row,
            u64::from(first_page) + u64::from(page),
        );
        page += count;
        if with_row {
            row_done = true;
        }
    }
    if !row_done {
        push(header.row_segment, row_bytes, 0, true, 0);
    }
}

impl Store {
    pub fn handle(&mut self, command: StoreCommand) {
        match command {
            StoreCommand::Hello {
                connection,
                replica,
                pid,
                descriptor,
                reply,
            } => {
                if self.sessions.contains_key(&connection) {
                    self.connection_closed(connection);
                }
                let result = self
                    .hello(connection, replica, pid, descriptor)
                    .map(|generation| HelloReply {
                        generation,
                        namespace: self.sessions[&connection].namespace.unwrap(),
                        arena_path: self.local.arena_path().to_string_lossy().into_owned(),
                        arena_bytes: self.local.arena_size(),
                        staging_path: self.local.staging_path().to_string_lossy().into_owned(),
                        staging_bytes: self.local.staging_size(),
                        max_window_bytes: self.config.max_window_bytes,
                    });
                let _ = reply.send(result);
            }
            StoreCommand::Subscribe {
                connection,
                namespace,
                generation,
                descriptor,
                events,
                reply,
            } => {
                let result = self.subscribe(connection, namespace, generation, descriptor, events);
                let _ = reply.send(result);
            }
            StoreCommand::PutBegin {
                connection,
                transfer,
                generation,
                namespace,
                descriptor,
                key,
                published_parent,
                bucket,
                chain_pages,
                published_tail_pages,
                row,
                events,
                reply,
            } => {
                let session = self.sessions.get(&connection);
                let session_ok = session.is_some_and(|session| {
                    session.generation == generation
                        && (session.namespace == Some(namespace) || session.remote)
                });
                if session.is_some_and(|session| session.generation != generation) {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "generation superseded",
                    )));
                    return;
                }
                let valid = session_ok
                    && descriptor.namespace_id() == namespace
                    && self.register_namespace(descriptor).is_ok();
                if !valid {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "PutBegin namespace or descriptor",
                    )));
                    return;
                }
                if self.transfer_in_use(connection, transfer) {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Put transfer id is already open on this connection",
                    )));
                    return;
                }
                self.submit_put(
                    connection,
                    transfer,
                    PutRequest {
                        namespace,
                        key,
                        published_parent,
                        bucket,
                        chain_pages,
                        published_tail_pages,
                        row,
                    },
                    events,
                );
                let _ = reply.send(Ok(()));
            }
            StoreCommand::WindowFilled {
                connection,
                generation,
                transfer,
                index,
                page_xxh3,
                row_xxh3,
                reply,
            } => {
                let result = if self
                    .sessions
                    .get(&connection)
                    .is_some_and(|session| session.generation == generation)
                {
                    self.window_filled(connection, transfer, index, page_xxh3, row_xxh3)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "generation superseded",
                    ))
                };
                let _ = reply.send(result);
            }
            StoreCommand::PutCommit {
                connection,
                generation,
                transfer,
                metadata_offset,
                metadata,
                reply,
            } => {
                let result = if self
                    .sessions
                    .get(&connection)
                    .is_some_and(|session| session.generation == generation)
                {
                    self.put_commit(connection, transfer, metadata_offset, metadata)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "generation superseded",
                    ))
                };
                let _ = reply.send(result);
            }
            StoreCommand::GetBegin {
                connection,
                transfer,
                generation,
                namespace,
                chain,
                skip_pages,
                expected_pages,
                row,
                events,
                reply,
            } => {
                let session = self.sessions.get(&connection);
                if session.is_none_or(|session| session.generation != generation) {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "generation superseded",
                    )));
                    return;
                }
                let known = self.namespaces.contains_key(&namespace);
                let Some(session) = session else {
                    return;
                };
                let allowed = session.namespace == Some(namespace) || (session.remote && known);
                if !allowed {
                    // a Get for a namespace the session has not registered is a
                    // terminal operation refusal.
                    let _ = reply.send(Ok(()));
                    let _ = events.try_send(Ok(GetAnswer::Refused(Refused {
                        reason: proto::Reason::Invalid as u8,
                        detail: "Get namespace is not registered".into(),
                    })));
                    return;
                }
                if self.transfer_in_use(connection, transfer) {
                    let _ = reply.send(Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Get transfer id is already open on this connection",
                    )));
                    return;
                }
                self.submit_get(
                    connection,
                    transfer,
                    GetRequest {
                        namespace,
                        keys: chain,
                        skip_pages,
                        expected_pages,
                        row,
                    },
                    events,
                );
                let _ = reply.send(Ok(()));
            }
            StoreCommand::WindowReleased {
                connection,
                generation,
                transfer,
                index,
                reply,
            } => {
                let result = if self
                    .sessions
                    .get(&connection)
                    .is_some_and(|session| session.generation == generation)
                {
                    self.window_released(connection, transfer, index)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "generation superseded",
                    ))
                };
                let _ = reply.send(result);
            }
            StoreCommand::Cancel {
                connection,
                generation,
                transfer,
            } => {
                if self
                    .sessions
                    .get(&connection)
                    .is_some_and(|session| session.generation == generation)
                {
                    self.cancel(connection, transfer);
                }
            }
            StoreCommand::ConnectionClosed { connection }
            | StoreCommand::ProcessExited { connection } => self.connection_closed(connection),
            StoreCommand::RemoteHello {
                connection,
                node,
                reply,
            } => {
                let _ = reply.send(self.remote_hello(connection, &node));
            }
            StoreCommand::ReleaseTransfer {
                connection,
                generation,
                transfer,
            } => {
                if self
                    .sessions
                    .get(&connection)
                    .is_some_and(|session| session.generation == generation)
                {
                    self.local.release_transfer(connection, transfer);
                }
            }

            StoreCommand::Tick => {}
            StoreCommand::Shutdown => {}
        }
    }
}

fn validate_commit(put: &mut Put, descriptor: &Descriptor) -> Result<()> {
    let header = Header::decode(&put.commit_bytes[..4096]).map_err(|e| invalid(e.to_string()))?;
    let planned = &put.header;
    let row_ok = match put.windows.row {
        Some(hash) => header.row_checksum == hash,
        None => header.row_checksum == planned.row_checksum,
    };
    if header.key != planned.key
        || header.parent != planned.parent
        || header.bucket != planned.bucket
        || header.tail_pages != planned.tail_pages
        || header.page_segment != planned.page_segment
        || header.row_segment != planned.row_segment
        || header.flags != 0
        || !row_ok
        || !descriptor.matches(&header)
    {
        return Err(invalid("commit header disagrees with accept"));
    }
    let page_segment = planned.page_segment as usize;
    let checksum_segment = &put.commit_bytes[4096..page_segment];
    for (index, hash) in put.windows.pages.iter().enumerate() {
        let Some(hash) = hash else {
            return Err(invalid("missing page hash"));
        };
        if checksum_segment[index * 8..index * 8 + 8] != hash.to_le_bytes() {
            return Err(invalid("commit checksum table"));
        }
    }
    put.header = header;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_delivered_put_waits_while_the_disk_makes_progress() {
        use super::{NO_PROGRESS, stalled};
        use std::time::Duration;
        let long = NO_PROGRESS + Duration::from_secs(1);
        let short = Duration::from_millis(10);
        // Waiting on the client for too long is a stall.
        assert!(stalled(long, false, Some(short)));
        assert!(!stalled(short, false, None));
        // Everything delivered and the disk busy but moving: not a stall, however long.
        assert!(!stalled(long, true, Some(short)));
        // The disk has made no progress either, or nothing is queued for it.
        assert!(stalled(long, true, Some(long)));
        assert!(stalled(long, true, None));
    }

    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn descriptor() -> Descriptor {
        Descriptor {
            identity: [7; 32],
            fingerprint: [3; 16],
            page_tokens: 64,
            ranks: vec![record::RankSizes {
                planes: vec![4000],
                row_bytes: 1000,
            }],
            tenant: String::new(),
        }
    }

    fn store(label: &str) -> (Store, PathBuf) {
        dram_store(label, 1024 * 1024)
    }

    fn dram_store(label: &str, memory: usize) -> (Store, PathBuf) {
        let name = format!(
            "selene-unit-{}-{label}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut config =
            Config::dram_only(std::env::temp_dir().join(format!("{name}.sock")), memory);
        config.name = name.clone();
        config.staging = 8 * 1024 * 1024;
        let store = Store::open(config).unwrap();
        (store, PathBuf::from("/dev/shm/selene").join(name))
    }

    fn store_with_root(label: &str, memory: usize) -> (Store, PathBuf, PathBuf) {
        let name = format!(
            "selene-unit-{}-{label}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let base = std::env::temp_dir().join(&name);
        let mut config = Config::new(
            base.join("records"),
            base.join("manager.sock"),
            memory,
            64 << 20,
        );
        config.name = name.clone();
        config.staging = 8 * 1024 * 1024;
        let store = Store::open(config).unwrap();
        let directory = PathBuf::from("/dev/shm/selene").join(name);
        (store, base, directory)
    }

    #[test]
    fn put_requirement_matches_the_shape_the_put_gets() {
        let (store, directory) = store("requirement");
        let value = descriptor();
        let key = |tokens| Key {
            consumed_tokens: tokens,
            digest: [0; 16],
        };
        let one_page = crate::record::new_header(&value, key(64), None, [0; 16], 1).unwrap();
        // 16 KiB arena: the 16 KiB extent plus envelope is 20 KiB, not the
        // 28 KiB max of the arena and streaming paths.
        let arena = store.put_requirement(&one_page, 16 * 1024);
        assert!(!arena.streaming);
        assert_eq!(arena.charge, 20 * 1024);
        assert_eq!(arena.reservation, 20 * 1024);
        assert_eq!(arena.allocation, 16 * 1024);
        assert_eq!(arena.unit, 16 * 1024);
        // 128 pages and a 4 KiB row: 528 KiB of disk record plus a fixed
        // hold of two 512 KiB windows.
        let many = crate::record::new_header(&value, key(128 * 64), None, [0; 16], 128).unwrap();
        let stream = store.put_requirement(&many, 0);
        assert!(stream.streaming);
        assert_eq!(stream.reservation, 528 * 1024);
        assert_eq!(stream.allocation, 2 * 512 * 1024);
        assert_eq!(stream.unit, 512 * 1024);
        assert_eq!(stream.charge, (528 + 2 * 512) * 1024);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    fn insert_memory_node(
        store: &mut Store,
        id: NamespaceId,
        value: &Descriptor,
        key: Key,
        parent: Option<Key>,
        tail: u32,
        extent: Extent,
    ) {
        let header = crate::record::new_header(value, key, parent, [0; 16], tail).unwrap();
        let bytes = record_charge(&header, true);
        store.charged += bytes;
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                key,
                parent,
                [0; 16],
                tail as usize,
                true,
                Location {
                    header,
                    memory: Some(Arc::new(extent)),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    retry: Instant::now(),
                    bytes,
                },
            )
            .unwrap();
    }

    #[test]
    fn terminal_get_completions_transfer_to_one_drain() {
        let (mut store, directory) = store("get-drain");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let get = Get {
            namespace: id,
            pins: VecDeque::new(),
            records: Vec::new(),
            grants: Vec::new(),
            next_grant: 0,
            next_meta: 0,
            outstanding: HashMap::new(),
            released: HashSet::new(),
            ready_windows: VecDeque::new(),
            window_reads: 0,
            pending_reads: 2,
            cancelled: false,
            terminal: true,
            finished: true,
            leased_sent: true,
            events,
            outbox: VecDeque::new(),
        };
        store.sessions.get_mut(&1).unwrap().ops.insert(
            1,
            Open {
                operation: Operation::Get(Box::new(get)),
                progress: Instant::now(),
            },
        );
        store.retired_gets.push(GetDrain {
            connection: 1,
            transfer: 1,
            namespace: id,
            pins: VecDeque::new(),
            pending: 2,
        });
        for expected in [1, 0] {
            let extent = store.staging.allocate(4096).unwrap();
            store.get_read_done(1, 1, 0, ReadKind::Meta { length: 0 }, Some(extent));
            assert_eq!(store.retired_gets[0].pending, expected);
            let Operation::Get(get) = &store.sessions[&1].ops[&1].operation else {
                panic!("expected Get");
            };
            assert_eq!(get.pending_reads, expected);
        }
        store.cleanup_turn();
        assert!(store.retired_gets.is_empty());
        // Closing the connection must not create a second drain from the
        // already transferred count.
        store.connection_closed(1);
        assert!(store.retired_gets.is_empty());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn namespace_registration_is_first_sight_and_rejects_mismatch() {
        let (mut store, directory) = store("register");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        assert_eq!(id, value.namespace_id());
        assert!(store.namespaces.contains_key(&id));
        assert_eq!(store.register_namespace(value.clone()).unwrap(), id);
        let mut mismatch = value;
        mismatch.ranks[0].row_bytes += 1;
        assert_eq!(mismatch.namespace_id(), id);
        let error = store.register_namespace(mismatch).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    fn insert_node(
        store: &mut Store,
        id: NamespaceId,
        value: &Descriptor,
        key: Key,
        parent: Option<Key>,
        tail: u32,
    ) {
        let header = crate::record::new_header(value, key, parent, [0; 16], tail).unwrap();
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                key,
                parent,
                [0; 16],
                tail as usize,
                true,
                Location {
                    header,
                    memory: None,
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    retry: Instant::now(),
                    bytes: 0,
                },
            )
            .unwrap();
    }

    #[test]
    fn snapshot_drops_mutations_at_or_before_the_boundary() {
        let (mut store, directory) = store("snapshot-boundary");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let generation = store.sessions[&1].generation;
        let key = Key {
            consumed_tokens: 128,
            digest: [1; 16],
        };
        insert_node(&mut store, id, &value, key, None, 2);
        store.delta(id, 0, key);
        assert_eq!(store.namespaces[&id].seq, 1);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        store.subscribe(1, id, generation, value, sender).unwrap();
        store.flush_events();
        let mut entries = 0;
        let mut added = 0;
        let mut ended = false;
        while !ended {
            store.pump_subscriptions();
            while let Ok(event) = receiver.try_recv() {
                match event.event.unwrap() {
                    proto::catalog_event::Event::Entry(_) => entries += 1,
                    proto::catalog_event::Event::Added(_) => added += 1,
                    proto::catalog_event::Event::SnapshotEnd(_) => ended = true,
                    _ => {}
                }
            }
        }
        assert_eq!(entries, 1);
        assert_eq!(
            added, 0,
            "an event at the snapshot boundary must not replay"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn live_events_do_not_bypass_buffered_mutations() {
        let (mut store, directory) = store("snapshot-order");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let generation = store.sessions[&1].generation;
        let first = Key {
            consumed_tokens: 64,
            digest: [1; 16],
        };
        let second = Key {
            consumed_tokens: 128,
            digest: [2; 16],
        };
        let third = Key {
            consumed_tokens: 192,
            digest: [3; 16],
        };
        insert_node(&mut store, id, &value, first, None, 1);
        insert_node(&mut store, id, &value, second, Some(first), 1);
        insert_node(&mut store, id, &value, third, Some(second), 1);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        store.subscribe(1, id, generation, value, sender).unwrap();
        // Two mutations happen while the snapshot is draining.
        store.delta(id, 1, first);
        store.delta(id, 1, second);
        store.flush_events();
        let mut events = Vec::new();
        while !events.iter().any(|event: &proto::CatalogEvent| {
            matches!(
                event.event,
                Some(proto::catalog_event::Event::SnapshotEnd(_))
            )
        }) {
            store.pump_subscriptions();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
        }
        let subscription = &store.subscriptions[&(1, id)];
        assert!(subscription.snapshot.is_none());
        assert_eq!(subscription.buffered.len(), 2);
        // A newer mutation must queue behind the older buffered ones.
        store.delta(id, 1, third);
        store.flush_events();
        assert_eq!(store.subscriptions[&(1, id)].buffered.len(), 3);
        loop {
            store.pump_subscriptions();
            while let Ok(event) = receiver.try_recv() {
                events.push(event);
            }
            let subscription = &store.subscriptions[&(1, id)];
            if subscription.buffered.is_empty() && subscription.snapshot.is_none() {
                break;
            }
        }
        let live: Vec<u64> = events
            .iter()
            .filter_map(|event| match &event.event {
                Some(proto::catalog_event::Event::Changed(entry)) => {
                    Some(entry.key.as_ref().unwrap().consumed_tokens)
                }
                _ => None,
            })
            .collect();
        assert_eq!(live, vec![64, 128, 192]);
        let mut sequence = 0;
        for event in &events {
            assert_eq!(event.sequence, sequence + 1);
            sequence = event.sequence;
        }
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn large_snapshot_streams_incrementally_and_restarts_on_overflow() {
        let (mut store, directory) = store("snapshot");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let generation = store.sessions[&1].generation;
        let total = 70_000u64;
        let mut parent = None;
        for index in 0..total {
            let key = Key {
                consumed_tokens: (index + 1) * 64,
                digest: (index as u128).to_le_bytes(),
            };
            let header = crate::record::new_header(&value, key, parent, [0; 16], 1).unwrap();
            store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .insert(
                    key,
                    parent,
                    [0; 16],
                    1,
                    true,
                    Location {
                        header,
                        memory: None,
                        disk: None,
                        durable: false,
                        writing: false,
                        job: None,
                        retry: Instant::now(),
                        bytes: 0,
                    },
                )
                .unwrap();
            parent = Some(key);
        }
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
        store
            .subscribe(1, id, generation, value.clone(), sender)
            .unwrap();
        // The channel is not filled by subscribe; the snapshot is pumped.
        assert!(receiver.try_recv().is_err());
        let mut begin = 0;
        let mut entries = 0;
        let mut end = 0;
        let mut sequence = 0;
        while end == 0 {
            store.pump_subscriptions();
            while let Ok(event) = receiver.try_recv() {
                assert_eq!(event.sequence, sequence + 1);
                sequence = event.sequence;
                match event.event.unwrap() {
                    proto::catalog_event::Event::SnapshotBegin(_) => begin += 1,
                    proto::catalog_event::Event::Entry(_) => entries += 1,
                    proto::catalog_event::Event::SnapshotEnd(_) => end += 1,
                    _ => {}
                }
            }
        }
        assert_eq!(begin, 1);
        assert_eq!(entries, total as usize);
        assert_eq!(end, 1);
        // A live event after SnapshotEnd continues the sequence.
        store.delta(id, 0, parent.unwrap());
        store.flush_events();
        store.pump_subscriptions();
        let live = receiver.try_recv().unwrap();
        assert_eq!(live.sequence, sequence + 1);

        // More mutations than the buffer can hold restart the snapshot at a new
        // boundary rather than dropping events.
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        store
            .subscribe(1, id, generation, value.clone(), sender)
            .unwrap();
        for _ in 0..=SNAPSHOT_BUFFER {
            store.delta(id, 0, parent.unwrap());
        }
        store.flush_events();
        let subscription = &store.subscriptions[&(1, id)];
        assert!(subscription.restarts >= 1);
        assert!(subscription.begin_pending);
        assert!(subscription.buffered.len() <= SNAPSHOT_BUFFER);
        let mut end = 0;
        let mut entries = 0;
        while end == 0 {
            store.pump_subscriptions();
            while let Ok(event) = receiver.try_recv() {
                match event.event.unwrap() {
                    proto::catalog_event::Event::Entry(_) => entries += 1,
                    proto::catalog_event::Event::SnapshotEnd(_) => end += 1,
                    _ => {}
                }
            }
        }
        assert_eq!(entries, total as usize);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_put_naming_an_absent_parent_asks_for_ancestry_repair() {
        let (mut store, directory) = store("parent");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store.remote_hello(1, "near").unwrap();
        let parent = Key {
            consumed_tokens: 64,
            digest: [1; 16],
        };
        let key = Key {
            consumed_tokens: 128,
            digest: [9; 16],
        };
        let (events, mut receiver) = tokio::sync::mpsc::channel(4);
        let (reply, _answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::PutBegin {
            connection: 1,
            transfer: 1,
            generation: store.sessions[&1].generation,
            namespace: id,
            descriptor: value.clone(),
            key,
            published_parent: Some(parent),
            bucket: [0; 16],
            chain_pages: 2,
            published_tail_pages: 1,
            row: true,
            events,
            reply,
        });
        store.pump();
        match receiver.try_recv().unwrap().unwrap() {
            PutAnswer::Refused(refused) => assert!(
                refused.detail.contains("published parent"),
                "unexpected detail: {}",
                refused.detail
            ),
            _ => panic!("expected a refusal"),
        }
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_local_put_with_an_unusable_parent_falls_back_to_a_root() {
        let (mut store, directory) = store("local-parent");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let parent = Key {
            consumed_tokens: 64,
            digest: [1; 16],
        };
        let key = Key {
            consumed_tokens: 128,
            digest: [9; 16],
        };
        let (events, mut receiver) = tokio::sync::mpsc::channel(4);
        let (reply, _answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::PutBegin {
            connection: 1,
            transfer: 1,
            generation: store.sessions[&1].generation,
            namespace: id,
            descriptor: value.clone(),
            key,
            published_parent: Some(parent),
            bucket: [0; 16],
            chain_pages: 2,
            published_tail_pages: 1,
            row: true,
            events,
            reply,
        });
        store.pump();
        match receiver.try_recv().unwrap().unwrap() {
            PutAnswer::Accept(accept) => assert!(
                accept.parent.is_none(),
                "a local Put whose parent hint is unusable must be accepted as a root"
            ),
            PutAnswer::Refused(refused) => panic!("expected an accept: {}", refused.detail),
            _ => panic!("expected an accept"),
        }
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_page_only_record_is_replaced_by_a_complete_put() {
        let (mut store, directory) = store("publish-target");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [6; 16],
        };
        let header = crate::record::new_header(&value, key, None, [0; 16], 1).unwrap();
        let extent = Arc::new(
            store
                .memory
                .allocate(header.length_with_row() as usize)
                .unwrap(),
        );
        let bytes = record_charge(&header, true);
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                key,
                None,
                [0; 16],
                1,
                true,
                Location {
                    header: header.clone(),
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    retry: Instant::now(),
                    bytes,
                },
            )
            .unwrap();
        assert!(
            matches!(store.publish_target(id, &header), PublishTarget::Present),
            "a complete local record is a duplicate"
        );
        // Compaction drops the row, keeping the pages as a replacement target.
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&key)
            .unwrap()
            .row = false;
        assert!(
            matches!(store.publish_target(id, &header), PublishTarget::Replace),
            "a page-only record must be replaced, not treated as present"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_get_without_a_row_is_refused() {
        let (mut store, directory) = store("get-row");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [7; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        let refused = store.plan_get(
            id,
            &GetRequest {
                namespace: id,
                keys: vec![key],
                skip_pages: 0,
                expected_pages: 1,
                row: false,
            },
        );
        assert!(
            refused.is_err(),
            "the Get contract requires the endpoint row"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_stale_disk_write_does_not_attach_to_a_replacement() {
        let (mut store, directory, _root) = store_with_root("stale-write", 1 << 20);
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [12; 16],
        };
        let header = crate::record::new_header(&value, key, None, [0; 16], 1).unwrap();
        let extent = Arc::new(
            store
                .memory
                .allocate(header.length_with_row() as usize)
                .unwrap(),
        );
        let bytes = record_charge(&header, true);
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                key,
                None,
                [0; 16],
                1,
                true,
                Location {
                    header,
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    retry: Instant::now(),
                    bytes,
                },
            )
            .unwrap();
        store.pump();
        let job = store.namespaces[&id]
            .tree
            .node(&key)
            .unwrap()
            .location
            .job
            .expect("the write-back was submitted");
        // Pretend the record these bytes belong to was replaced while the write drained.
        let replacement = job + 1000;
        {
            let location = &mut store.namespaces.get_mut(&id).unwrap().tree[&key].location;
            location.job = Some(replacement);
            location.writing = true;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.jobs.contains_key(&job) {
            assert!(Instant::now() < deadline, "the disk write never completed");
            store.pump();
            std::thread::sleep(Duration::from_millis(5));
        }
        let location = &store.namespaces[&id].tree[&key].location;
        assert!(
            location.disk.is_none(),
            "a stale write attached its file to the replacement record"
        );
        assert_eq!(location.job, Some(replacement));
        assert!(location.writing, "a stale completion cleared the new write");
        drop(store);
        let _ = fs::remove_dir_all(directory);
        // `_root` is the instance's shared-memory directory.
        let _ = fs::remove_dir_all(_root);
    }

    #[test]
    fn a_draining_or_opposite_transfer_id_is_fenced() {
        let (mut store, directory) = store("fence");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store.remote_hello(1, "near").unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [6; 16],
        };
        let (get_events, _get_receiver) = tokio::sync::mpsc::channel(4);
        let (put_events, _put_receiver) = tokio::sync::mpsc::channel(4);
        let get = |store: &mut Store, transfer: u64| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::GetBegin {
                connection: 1,
                transfer,
                generation: store.sessions[&1].generation,
                namespace: id,
                chain: vec![key],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                events: get_events.clone(),
                reply,
            });
            answer.blocking_recv().unwrap()
        };
        let put = |store: &mut Store, transfer: u64| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::PutBegin {
                connection: 1,
                transfer,
                generation: store.sessions[&1].generation,
                namespace: id,
                descriptor: value.clone(),
                key,
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 1,
                published_tail_pages: 1,
                row: true,
                events: put_events.clone(),
                reply,
            });
            answer.blocking_recv().unwrap()
        };
        assert!(get(&mut store, 7).is_ok());
        // The opposite direction is fenced while the Get is pending.
        assert!(put(&mut store, 7).is_err());
        // A draining Get keeps its id reserved until the disk copy drains.
        store.retired_gets.push(GetDrain {
            connection: 1,
            transfer: 8,
            namespace: id,
            pins: VecDeque::new(),
            pending: 1,
        });
        assert!(get(&mut store, 8).is_err());
        assert!(put(&mut store, 8).is_err());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn one_connection_can_subscribe_two_namespaces() {
        let (mut store, directory) = store("multi-sub");
        let first = descriptor();
        let mut second = descriptor();
        second.tenant = "tenant-b".to_string();
        store.remote_hello(1, "near").unwrap();
        let generation = store.sessions[&1].generation;
        let (sender_a, mut receiver_a) = tokio::sync::mpsc::channel(8);
        store
            .subscribe(1, first.namespace_id(), generation, first.clone(), sender_a)
            .unwrap();
        let (sender_b, mut receiver_b) = tokio::sync::mpsc::channel(8);
        store
            .subscribe(
                1,
                second.namespace_id(),
                generation,
                second.clone(),
                sender_b,
            )
            .unwrap();
        assert_eq!(store.subscriptions.len(), 2);
        for receiver in [&mut receiver_a, &mut receiver_b] {
            loop {
                store.pump_subscriptions();
                match receiver.try_recv() {
                    Ok(event)
                        if matches!(
                            event.event,
                            Some(proto::catalog_event::Event::SnapshotEnd(_))
                        ) =>
                    {
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        }
        let key = Key {
            consumed_tokens: 64,
            digest: [5; 16],
        };
        insert_node(&mut store, first.namespace_id(), &first, key, None, 1);
        store.delta(first.namespace_id(), 0, key);
        store.flush_events();
        store.pump_subscriptions();
        assert!(
            receiver_a.try_recv().is_ok(),
            "the subscribed namespace sees it"
        );
        assert!(
            receiver_b.try_recv().is_err(),
            "another namespace must not receive it"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_reused_get_transfer_id_is_refused() {
        let (mut store, directory) = store("dup");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store.remote_hello(1, "near").unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [4; 16],
        };
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let begin = |store: &mut Store| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::GetBegin {
                connection: 1,
                transfer: 7,
                generation: store.sessions[&1].generation,
                namespace: id,
                chain: vec![key],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                events: events.clone(),
                reply,
            });
            answer.blocking_recv().unwrap()
        };
        assert!(begin(&mut store).is_ok());
        assert!(begin(&mut store).is_err());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn rootless_eviction_only_removes_leaves_and_accounts_them() {
        let (mut store, directory) = store("evict");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let root_key = Key {
            consumed_tokens: 64,
            digest: [2; 16],
        };
        let child_key = Key {
            consumed_tokens: 128,
            digest: [3; 16],
        };
        let root_header = crate::record::new_header(&value, root_key, None, [0; 16], 1).unwrap();
        let child_header = crate::record::new_header(&value, child_key, Some(root_key), [0; 16], 1).unwrap();
        for header in [&root_header, &child_header] {
            let extent = Arc::new(store.memory.allocate(4096).unwrap());
            let bytes = record_charge(header, true);
            store.charged += bytes;
            store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .insert(
                    header.key,
                    header.parent,
                    [0; 16],
                    1,
                    true,
                    Location {
                        header: header.clone(),
                        memory: Some(extent),
                        disk: None,
                        durable: false,
                        writing: false,
                        job: None,
                        retry: Instant::now(),
                        bytes,
                    },
                )
                .unwrap();
        }
        let charged = store.charged;
        assert!(!store.evict_memory(id, root_key));
        assert_eq!(store.charged, charged);
        assert!(store.namespaces[&id].tree.node(&root_key).is_some());
        assert!(store.evict_memory(id, child_key));
        assert!(store.namespaces[&id].tree.node(&child_key).is_none());
        assert!(store.evict_memory(id, root_key));
        assert_eq!(store.charged, 0);
        assert!(store.namespaces[&id].tree.node(&root_key).is_none());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_chain_admits_the_root_by_successive_leaf_eviction() {
        // A full 64 KiB rootless arena: 32 -> 16 -> 16 KiB. The new record
        // needs the arena only after three successive leaf evictions.
        let (mut store, directory) = dram_store("chain", 64 * 1024);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let id = store.sessions[&1].namespace.unwrap();
        let root = store.memory.allocate(32 * 1024).unwrap();
        let child = store.memory.allocate(16 * 1024).unwrap();
        let leaf = store.memory.allocate(16 * 1024).unwrap();
        let (r, c, l) = (
            Key {
                consumed_tokens: 128,
                digest: [1; 16],
            },
            Key {
                consumed_tokens: 192,
                digest: [2; 16],
            },
            Key {
                consumed_tokens: 256,
                digest: [3; 16],
            },
        );
        insert_memory_node(&mut store, id, &value, r, None, 2, root);
        insert_memory_node(&mut store, id, &value, c, Some(r), 1, child);
        insert_memory_node(&mut store, id, &value, l, Some(c), 1, leaf);
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: id,
                key: Key {
                    consumed_tokens: 384,
                    digest: [4; 16],
                },
                published_parent: Some(l),
                bucket: [0; 16],
                chain_pages: 6,
                published_tail_pages: 2,
                row: true,
            },
            events,
        );
        store.dispatch_trace();
        let open = &store.sessions[&1].ops[&1];
        let Operation::Put(put) = &open.operation else {
            panic!("expected Put");
        };
        assert!(!put.streaming);
        assert!(put.memory.is_some());
        assert_eq!(put.header.parent, None);
        assert_eq!(put.header.tail_pages, 6);
        let tree = &store.namespaces[&id].tree;
        assert!(tree.node(&r).is_none());
        assert!(tree.node(&c).is_none());
        assert!(tree.node(&l).is_none());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn cancel_removes_a_queued_begin() {
        let (mut store, directory) = store("cancel-queued");
        let value = descriptor();
        store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let (put_events, _put_receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: store.namespace_id().unwrap(),
                key: Key {
                    consumed_tokens: 128,
                    digest: [8; 16],
                },
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 2,
                published_tail_pages: 2,
                row: true,
            },
            put_events,
        );
        store.cancel(1, 1);
        assert!(store.dispatch_trace().is_empty());
        let (get_events, _get_receiver) = tokio::sync::mpsc::channel(4);
        store.submit_get(
            1,
            2,
            GetRequest {
                namespace: store.namespace_id().unwrap(),
                keys: vec![Key {
                    consumed_tokens: 64,
                    digest: [4; 16],
                }],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
            },
            get_events,
        );
        store.cancel(1, 2);
        assert!(store.dispatch_trace().is_empty());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn cancelled_detached_puts_do_not_submit_blocked_chunks() {
        let (mut store, base, directory) = store_with_root("detached-blocked", 4096);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let id = store.sessions[&1].namespace.unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: id,
                key: Key {
                    consumed_tokens: 128,
                    digest: [10; 16],
                },
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 2,
                published_tail_pages: 2,
                row: true,
            },
            events,
        );
        store.dispatch_trace();
        let open = store.sessions.get_mut(&1).unwrap().ops.remove(&1).unwrap();
        let Operation::Put(put) = open.operation else {
            panic!("expected Put");
        };
        let mut put = *put;
        put.cancelled = true;
        let slice = ExtentSlice::new(Arc::new(store.staging.allocate(4096).unwrap()), 0, 4096);
        put.blocked.push_back((777, 0, slice));
        store.detached_puts.insert((1, 1), put);
        store.pump_chunks();
        assert!(!store.jobs.contains_key(&777));
        assert_eq!(store.detached_puts[&(1, 1)].blocked.len(), 1);
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn dispatch_prioritises_gets() {
        let (mut store, directory) = store("dispatch");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let (get_events, _get_receiver) = tokio::sync::mpsc::channel(4);
        let (put_events, _put_receiver) = tokio::sync::mpsc::channel(4);
        for index in 0..4 {
            store.submit_get(
                1,
                100 + index,
                GetRequest {
                    namespace: id,
                    keys: vec![Key {
                        consumed_tokens: 64,
                        digest: [4; 16],
                    }],
                    skip_pages: 0,
                    expected_pages: 1,
                    row: true,
                },
                get_events.clone(),
            );
        }
        for index in 0..4 {
            store.submit_put(
                1,
                200 + index,
                PutRequest {
                    namespace: id,
                    key: Key {
                        consumed_tokens: 128 + index * 64,
                        digest: [5; 16],
                    },
                    published_parent: None,
                    bucket: [0; 16],
                    chain_pages: (128 + index * 64) as u32 / 64,
                    published_tail_pages: (128 + index * 64) as u32 / 64,
                    row: true,
                },
                put_events.clone(),
            );
        }
        assert_eq!(
            store.dispatch_trace(),
            vec!["get", "get", "get", "get", "put"]
        );
        // One more round: the fifth get still precedes the queued Put.
        store.submit_get(
            1,
            300,
            GetRequest {
                namespace: id,
                keys: vec![Key {
                    consumed_tokens: 64,
                    digest: [4; 16],
                }],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
            },
            get_events,
        );
        assert_eq!(store.dispatch_trace(), vec!["get", "put"]);
        let _ = id;
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn failed_puts_drop_unsent_chunks() {
        let (mut store, base, directory) = store_with_root("failed-chunks", 4096);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: store.namespace_id().unwrap(),
                key: Key {
                    consumed_tokens: 128,
                    digest: [9; 16],
                },
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 2,
                published_tail_pages: 2,
                row: true,
            },
            events,
        );
        store.dispatch_trace();
        {
            let operation = &mut store
                .sessions
                .get_mut(&1)
                .unwrap()
                .ops
                .get_mut(&1)
                .unwrap()
                .operation;
            let Operation::Put(put) = operation else {
                panic!("expected Put");
            };
            let slice = ExtentSlice::new(Arc::new(store.staging.allocate(4096).unwrap()), 0, 4096);
            put.blocked.push_back((999, 0, slice));
        }
        store.fail_put(1, 1, proto::Reason::Io as u8, invalid("test"));
        let operation = &store.sessions[&1].ops[&1].operation;
        let Operation::Put(put) = operation else {
            panic!("expected Put");
        };
        assert!(put.blocked.is_empty());
        store.pump_chunks();
        assert!(store.jobs.keys().all(|job| *job != 999));
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn fragmented_arena_holes_fall_back_to_streaming() {
        // 64 KiB arena with 16 KiB live at 0 and 32 KiB; the freed buddies
        // leave 32 KiB free as two separated 16 KiB holes.
        let (mut store, base, directory) = store_with_root("fragmented", 64 * 1024);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let id = store.sessions[&1].namespace.unwrap();
        let first = store.memory.allocate(16 * 1024).unwrap();
        let second = store.memory.allocate(16 * 1024).unwrap();
        let third = store.memory.allocate(16 * 1024).unwrap();
        let fourth = store.memory.allocate(16 * 1024).unwrap();
        assert_eq!(first.offset(), 0);
        assert_eq!(second.offset(), 16 * 1024);
        assert_eq!(third.offset(), 32 * 1024);
        assert_eq!(fourth.offset(), 48 * 1024);
        drop(second);
        drop(fourth);
        insert_memory_node(
            &mut store,
            id,
            &value,
            Key {
                consumed_tokens: 64,
                digest: [1; 16],
            },
            None,
            1,
            first,
        );
        insert_memory_node(
            &mut store,
            id,
            &value,
            Key {
                consumed_tokens: 128,
                digest: [2; 16],
            },
            Some(Key {
                consumed_tokens: 64,
                digest: [1; 16],
            }),
            1,
            third,
        );
        // A 32 KiB record cannot use the holes. With no evictable leaf (the
        // occupants are not durable) the Put streams instead of refusing.
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: id,
                key: Key {
                    consumed_tokens: 192,
                    digest: [3; 16],
                },
                published_parent: Some(Key {
                    consumed_tokens: 64,
                    digest: [1; 16],
                }),
                bucket: [0; 16],
                chain_pages: 3,
                published_tail_pages: 2,
                row: true,
            },
            events,
        );
        store.dispatch_trace();
        let open = &store.sessions[&1].ops[&1];
        let Operation::Put(put) = &open.operation else {
            panic!("expected Put");
        };
        assert!(put.streaming);
        assert!(put.memory.is_none());
        assert_eq!(put.requirement.allocation, 2 * 16 * 1024);
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn queued_get_overtakes_an_earlier_put() {
        let (mut store, directory) = store("batch");
        let value = descriptor();
        store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let (get_events, _get_receiver) = tokio::sync::mpsc::channel(4);
        let (put_events, _put_receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: store.namespace_id().unwrap(),
                key: Key {
                    consumed_tokens: 128,
                    digest: [5; 16],
                },
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 2,
                published_tail_pages: 2,
                row: true,
            },
            put_events,
        );
        store.submit_get(
            1,
            2,
            GetRequest {
                namespace: store.namespace_id().unwrap(),
                keys: vec![Key {
                    consumed_tokens: 64,
                    digest: [4; 16],
                }],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
            },
            get_events,
        );
        assert_eq!(store.dispatch_trace(), vec!["get", "put"]);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn the_nvme_measure_counts_put_holds_and_reservations_not_windows_leased_for_claims() {
        let (mut store, base, directory) = store_with_root("nvme-measure", 4096);
        assert_eq!(store.charge_total(), 0);
        // Windows the pool leases from the staging region for claims and exports are not
        // records and never reach the disk: they do not use the limit.
        let leased = store
            .local
            .staging()
            .allocate(1 << 20)
            .expect("a window of staging");
        assert_eq!(store.local.staging_used(), 1 << 20);
        assert_eq!(store.charge_total(), 0);
        assert!(
            store
                .metrics_text()
                .contains("selene_nvme_reserved_bytes 0\n")
        );
        drop(leased);
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn streaming_admission_uses_the_hold() {
        let name = format!(
            "selene-unit-{}-budget-staging-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let base = std::env::temp_dir().join(&name);
        let mut config = Config::new(
            base.join("records"),
            base.join("manager.sock"),
            4096,
            64 << 20,
        );
        config.name = name.clone();
        config.staging = 8 * 1024 * 1024;
        let mut store = Store::open(config).unwrap();
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let id = store.sessions[&1].namespace.unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: store.namespace_id().unwrap(),
                key: Key {
                    consumed_tokens: 128,
                    digest: [7; 16],
                },
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 2,
                published_tail_pages: 2,
                row: true,
            },
            events.clone(),
        );
        store.dispatch_trace();
        // The hold is two 16 KiB windows (32 KiB), not two negotiated 4 MiB
        // maxima, so both Puts stay in budget.
        assert!(store.sessions[&1].ops.contains_key(&1));
        store.submit_put(
            1,
            2,
            PutRequest {
                namespace: id,
                key: Key {
                    consumed_tokens: 192,
                    digest: [8; 16],
                },
                published_parent: None,
                bucket: [0; 16],
                chain_pages: 3,
                published_tail_pages: 3,
                row: true,
            },
            events,
        );
        store.dispatch_trace();
        assert!(store.sessions[&1].ops.contains_key(&2));
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(PathBuf::from("/dev/shm/selene").join(name));
    }

}
