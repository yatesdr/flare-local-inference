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
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

pub const MAX_CONNECTIONS: usize = 64;
pub const OPEN_PUTS: usize = 4;
pub const OPEN_GETS: usize = 4;
pub const WINDOW_CREDITS: u32 = 2;
pub const MAX_CHAIN: usize = 65_536;
pub const NO_PROGRESS: Duration = Duration::from_secs(3);
pub const DEFAULT_MAX_WINDOW: u64 = 4 * 1024 * 1024;
pub const DEFAULT_REMOTE_CATALOG: usize = 1_000_000;

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
    pub write_budget: u64,
    pub max_window_bytes: u64,
    /// Cap on remote-only records per namespace in the merged catalog.
    pub remote_catalog_entries: usize,
    /// Keep a fetched remote record after the Get completes.
    pub retain_remote: bool,
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
            staging: 64 * 1024 * 1024,
            metrics: None,
            faults: disk::Faults::default(),
            write_budget: 0,
            max_window_bytes: DEFAULT_MAX_WINDOW,
            remote_catalog_entries: DEFAULT_REMOTE_CATALOG,
            retain_remote: true,
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
            staging: 64 * 1024 * 1024,
            metrics: None,
            faults: disk::Faults::default(),
            write_budget: 0,
            max_window_bytes: DEFAULT_MAX_WINDOW,
            remote_catalog_entries: DEFAULT_REMOTE_CATALOG,
            retain_remote: true,
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

/// Where a record entered this manager.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Local,
    Peer,
}

#[derive(Clone)]
pub struct Location {
    pub header: Header,
    pub origin: Origin,
    pub memory: Option<Arc<Extent>>,
    pub disk: Option<Arc<DiskExtent>>,
    /// A copy on an upstream manager, kept in the merged catalog for lookups and fetches.
    pub remote: Option<RemoteSource>,
    pub durable: bool,
    pub writing: bool,
    /// The disk job currently transforming these bytes, so a stale completion is ignored.
    pub job: Option<u64>,
    /// The disk copy is stale (a fetched row completed it) and must be rewritten.
    pub rewrite: bool,
    pub retry: Instant,
    pub bytes: u64,
}

/// Where an upstream manager keeps one record, as advertised in its catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteSource {
    pub node: String,
    pub bytes: u64,
    pub row: bool,
}

/// What a finished Put should do to the tree entry for its key.
enum PublishTarget {
    /// No entry exists; index a new record.
    Insert,
    /// A catalog entry without local bytes is present; take its place and keep its source.
    Replace(Option<RemoteSource>),
    /// A local copy won the race; the Put is a duplicate.
    Present,
    /// The existing entry describes a different record under the same key.
    Conflict,
}

/// What a local Get needs from the peer runtime before it can be served.
pub struct RemotePlan {
    pub descriptor: Descriptor,
    /// The whole chain's root; placement hashes this, never a fetched suffix.
    pub root: Key,
    /// Records to fetch in one Get: from the first missing record to the chain endpoint. The
    /// endpoint carries a row, so the chain is a valid Get.
    pub records: Vec<ChainRecord>,
}

/// One record of a requested chain, as the local merged catalog knows it.
#[derive(Clone, Debug)]
pub struct ChainRecord {
    pub key: Key,
    pub bytes: u64,
    pub tail_pages: u32,
    pub row: bool,
    pub remote: bool,
    pub local: bool,
    /// The upstream that advertised this record, when it is a remote source.
    pub source: Option<String>,
    /// True for the last record of the requested chain, whose row a Get delivers.
    pub endpoint: bool,
    /// The record's full shape, including its row, used to size a fetch destination.
    pub length: u64,
}

fn remote_key(key: Option<proto::Key>) -> Option<Key> {
    let key = key?;
    if key.consumed_tokens == 0 || key.digest.len() != 16 {
        return None;
    }
    let digest: [u8; 16] = key.digest.as_slice().try_into().ok()?;
    Some(Key {
        consumed_tokens: key.consumed_tokens,
        digest,
    })
}

fn remote_entry(entry: &proto::CatalogEntry) -> Option<(Key, RemoteEntry)> {
    let key = remote_key(entry.key.clone())?;
    let parent = if entry.has_parent {
        Some(remote_key(entry.parent.clone())?)
    } else {
        None
    };
    let bucket: [u8; 16] = entry.bucket.as_slice().try_into().ok()?;
    let tail_pages = u32::try_from(entry.tail_pages).ok()?;
    Some((
        key,
        RemoteEntry {
            parent,
            bucket,
            tail_pages,
            row: entry.row,
            bytes: entry.bytes,
            unreadable: entry.unreadable,
        },
    ))
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
    /// Incremented by a purge; publications from an older epoch are discarded.
    pub epoch: u64,
    /// True while a purge's retired disk extents are still draining.
    pub purging: bool,
    /// Retired extents that keep the namespace closed to new Puts until they are released.
    pub purge_drain: Vec<std::sync::Weak<crate::disk::DiskExtent>>,
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
    Wait(PutRequirement, Vec<Key>),
}

pub struct Put {
    namespace: NamespaceId,
    epoch: u64,
    remote: bool,
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

pub struct TokenBucket {
    rate: u64,
    burst: u64,
    balance: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: u64, now: Instant) -> Self {
        let burst = rate.max(1024 * 1024);
        Self {
            rate,
            burst,
            balance: burst as f64,
            last: now,
        }
    }
    fn refill(&mut self, now: Instant) {
        if self.rate == 0 {
            self.balance = self.burst as f64;
            return;
        }
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.balance = (self.balance + elapsed * self.rate as f64).min(self.burst as f64);
    }
    fn admit(&mut self, charge: u64, now: Instant) -> bool {
        if self.rate == 0 {
            return true;
        }
        self.refill(now);
        if self.balance < 0.0 {
            return false;
        }
        self.balance -= charge as f64;
        true
    }
    pub fn balance(&self) -> i64 {
        self.balance as i64
    }
}

pub struct Session {
    pub generation: u64,
    pub namespace: Option<NamespaceId>,
    pub ops: HashMap<u64, Open>,
    pub budget: TokenBucket,
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
    Room {
        bytes: u64,
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
    pub forwarded: u64,
    pub forward_failed: u64,
    pub forward_retries: u64,
    pub remote_conflicts: u64,
    pub remote_refused: u64,
    pub remote_fetched: u64,
    pub hits: HashMap<(String, &'static str), u64>,
}

#[derive(Clone, Debug)]
pub struct EntryData {
    pub key: Key,
    pub parent: Option<Key>,
    pub bucket: [u8; 16],
    pub tail_pages: u32,
    pub row: bool,
    /// 0 = DRAM, 1 = NVMe, 2 = only an upstream manager has it.
    pub tier: u8,
    pub durable: bool,
    pub bytes: u64,
    /// A shape anchor without a readable local or remote source.
    pub unreadable: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardEvent {
    Stored { namespace: NamespaceId, key: Key },
    Durable { namespace: NamespaceId, key: Key },
}

/// A leased view of a stored record for the write-through forwarder. The
/// allocation cannot be freed until the lease is dropped.
pub struct ForwardRecord {
    pub header: Header,
    memory: Option<Arc<Extent>>,
    disk: Option<Arc<DiskExtent>>,
}

impl Drop for ForwardRecord {
    fn drop(&mut self) {
        if let Some(extent) = &self.memory {
            extent.release();
        }
        if let Some(extent) = &self.disk {
            extent.release();
        }
    }
}

impl ForwardRecord {
    /// The lease when the record is readable in memory; its bytes start at the extent.
    pub fn memory(&self) -> Option<&Arc<Extent>> {
        self.memory.as_ref()
    }

    /// The lease when the record is durable. Disk files carry a 4096-byte envelope.
    pub fn disk(&self) -> Option<&Arc<DiskExtent>> {
        self.disk.as_ref()
    }

    /// A disk-only lease, for tests that drive the forwarder without a store.
    #[cfg(test)]
    pub fn for_test_disk(header: Header, disk: Arc<DiskExtent>) -> Self {
        Self {
            header,
            memory: None,
            disk: Some(disk),
        }
    }
}

/// One write-through step: the exact stored shape and a lease on its bytes.
pub struct ForwardJob {
    pub namespace: NamespaceId,
    pub descriptor: Descriptor,
    pub key: Key,
    pub parent: Option<Key>,
    /// The chain from its root to `key`, root first.
    pub ancestors: Vec<Key>,
    pub chain_pages: u64,
    pub tail_pages: u64,
    pub bucket: [u8; 16],
    pub row: bool,
    pub record: ForwardRecord,
}

pub enum StoreEvent {
    Catalog {
        namespace: NamespaceId,
        seq: u64,
        kind: u8,
        key: Key,
        entry: Option<EntryData>,
    },
    Room {
        connection: u64,
        bytes: u64,
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

#[derive(Clone, Debug)]
struct ArmedRoom {
    requirement: PutRequirement,
    /// Arena shape the same Put could take once space frees, when the refused
    /// streaming shape cannot fit its hold.
    alternative: Option<PutRequirement>,
    /// Keys the refused shape needs to survive the retry.
    protected: Vec<Key>,
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
    clock: u64,
    boot: u64,
    counter: u64,
    metrics: Metrics,
    charged: u64,
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
    armed: HashMap<u64, ArmedRoom>,
    forward_events: Vec<ForwardEvent>,
    /// Ancestor chains pinned while a forward is pending.
    forward_pins: HashMap<(NamespaceId, Key), Vec<Key>>,
    /// Chains pinned by a remote fetch reservation until the serving Get takes over.
    fetch_pins: HashMap<u64, FetchPin>,
    fetch_counter: u64,
    /// Remote-only anchors waiting for the lease that kept them to end.
    remote_orphans: Vec<(NamespaceId, Key)>,
    quotas: HashMap<NamespaceId, Quota>,
    /// The live remote view of each (namespace, upstream), keyed by record.
    remote_views: HashMap<(NamespaceId, String), BTreeMap<Key, RemoteEntry>>,
    /// Snapshots being assembled for the same pair.
    remote_snapshots: HashMap<(NamespaceId, String), BTreeMap<Key, RemoteEntry>>,
    /// Remote entries whose parent was not indexed yet; retried when a parent arrives.
    remote_deferred: HashMap<(NamespaceId, String), BTreeMap<Key, RemoteEntry>>,
}

/// One record as an upstream catalog describes it.
#[derive(Clone)]
struct RemoteEntry {
    parent: Option<Key>,
    bucket: [u8; 16],
    tail_pages: u32,
    row: bool,
    bytes: u64,
    unreadable: bool,
}

/// A remote fetch's DRAM destinations, pinned until the serving Get plans its own pins.
struct FetchPin {
    connection: u64,
    namespace: NamespaceId,
    keys: Vec<Key>,
}

#[derive(Clone, Copy, Default)]
struct Quota {
    dram: u64,
    nvme: u64,
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
            clock: 0,
            boot: u64::from_le_bytes(nonce),
            counter: 0,
            metrics: Metrics::default(),
            charged: 0,
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
            armed: HashMap::new(),
            forward_events: Vec::new(),
            forward_pins: HashMap::new(),
            fetch_pins: HashMap::new(),
            fetch_counter: 0,
            remote_orphans: Vec::new(),
            quotas: HashMap::new(),
            remote_views: HashMap::new(),
            remote_snapshots: HashMap::new(),
            remote_deferred: HashMap::new(),
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
                        origin: Origin::Local,
                        remote: None,
                        header,
                        memory: None,
                        disk: Some(extent),
                        durable: false,
                        writing: false,
                        job: None,
                        rewrite: false,
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
            purging: false,
            purge_drain: Vec::new(),
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
    pub fn session_generation(&self, connection: u64) -> Option<u64> {
        self.sessions
            .get(&connection)
            .map(|session| session.generation)
    }
    pub fn has_op(&self, connection: u64, transfer: u64) -> bool {
        self.sessions
            .get(&connection)
            .is_some_and(|session| session.ops.contains_key(&transfer))
    }
    pub fn budget_balance(&self, connection: u64) -> Option<i64> {
        self.sessions
            .get(&connection)
            .map(|session| session.budget.balance())
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
                budget: TokenBucket::new(self.config.write_budget, Instant::now()),
                remote: false,
            },
        );
        Ok(generation)
    }

    /// Register a manager-to-manager connection. The namespace is bound by the first
    /// Subscribe or PutBegin, because `PeerHello` carries no descriptor.
    pub fn peer_hello(&mut self, connection: u64, node: &str) -> Result<u64> {
        if node.is_empty() || node.len() > 256 {
            return Err(invalid("peer node name must be 1..=256 bytes"));
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
                budget: TokenBucket::new(self.config.write_budget, Instant::now()),
                remote: true,
            },
        );
        Ok(generation)
    }

    /// The namespaces currently registered, for upstream subscription.
    pub fn descriptors(&self) -> Vec<(NamespaceId, Descriptor)> {
        self.namespaces
            .iter()
            .map(|(id, namespace)| (*id, namespace.descriptor.clone()))
            .collect()
    }

    /// The arena and staging mappings, for the peer data channels.
    pub fn regions(&self) -> crate::regions::LocalRegions {
        crate::regions::LocalRegions::new(self.memory.clone(), self.staging.clone())
    }

    /// The chain from its root to `key`, root first, for parent-first forwarding.
    pub fn ancestors_of(&self, id: NamespaceId, key: Key) -> Vec<Key> {
        let Some(namespace) = self.namespaces.get(&id) else {
            return Vec::new();
        };
        let Some(node) = namespace.tree.node(&key) else {
            return Vec::new();
        };
        let mut chain = vec![key];
        let mut parent = node.parent;
        while let Some(current) = parent {
            chain.push(current);
            parent = namespace.tree.node(&current).and_then(|node| node.parent);
        }
        chain.reverse();
        chain
    }

    /// Everything a forward step needs: the exact stored shape, the pinned record lease and its
    /// ancestor chain, root first. The ancestors stay pinned until `PeerEvent::Forwarded`.
    pub fn prepare_forward(&mut self, namespace: NamespaceId, key: Key) -> Result<ForwardJob> {
        let state = self
            .namespaces
            .get(&namespace)
            .ok_or_else(|| invalid("forward namespace is not registered"))?;
        let node = state
            .tree
            .node(&key)
            .ok_or_else(|| invalid("forward source is not stored"))?;
        let mut ancestors = vec![key];
        let mut parent = node.parent;
        while let Some(current) = parent {
            ancestors.push(current);
            parent = state.tree.node(&current).and_then(|node| node.parent);
        }
        ancestors.reverse();
        let chain_pages = key.consumed_tokens / state.descriptor.page_tokens;
        let job = ForwardJob {
            namespace,
            descriptor: state.descriptor.clone(),
            key,
            parent: node.parent,
            ancestors: ancestors.clone(),
            chain_pages,
            tail_pages: node.tail_pages as u64,
            bucket: node.bucket,
            row: node.row,
            record: self.forward_source(namespace, key)?,
        };
        let state = self.namespaces.get_mut(&namespace).unwrap();
        for ancestor in &ancestors {
            let _ = state.tree.pin(ancestor);
        }
        self.forward_pins.insert((namespace, key), ancestors);
        Ok(job)
    }

    fn forward_finished(&mut self, namespace: NamespaceId, key: Key) {
        let Some(ancestors) = self.forward_pins.remove(&(namespace, key)) else {
            return;
        };
        let Some(state) = self.namespaces.get_mut(&namespace) else {
            return;
        };
        for ancestor in &ancestors {
            let _ = state.tree.unpin(ancestor);
        }
    }

    /// Releases a prepared forward that never reached the peer thread.
    pub fn forward_dropped(&mut self, namespace: NamespaceId, key: Key) {
        self.metrics.forward_failed += 1;
        self.forward_finished(namespace, key);
    }

    /// Remove every record of a namespace, canceling its transfers first. Records are removed
    /// even when pinned or leased: their extents stay alive through the leases, and disk files
    /// are retired once the last reader drops.
    fn purge_namespace(&mut self, id: NamespaceId) -> (u64, u64) {
        let Some(namespace) = self.namespaces.get_mut(&id) else {
            return (0, 0);
        };
        namespace.epoch = namespace.epoch.wrapping_add(1);
        let mut ops = Vec::new();
        for (connection, session) in &self.sessions {
            for (transfer, open) in &session.ops {
                let namespace = match &open.operation {
                    Operation::Put(put) => Some(put.namespace),
                    Operation::Get(get) => Some(get.namespace),
                };
                if namespace == Some(id) {
                    ops.push((*connection, *transfer));
                }
            }
        }
        for (connection, transfer) in ops {
            self.cancel(connection, transfer);
        }
        for queue in self.pending_puts.values_mut() {
            queue.retain(|pending| {
                if pending.request.namespace != id {
                    return true;
                }
                let _ = pending.events.try_send(Ok(PutAnswer::Refused(Refused {
                    reason: proto::Reason::Invalid as u8,
                    detail: "namespace was purged".into(),
                })));
                false
            });
        }
        for queue in self.pending_gets.values_mut() {
            queue.retain(|pending| {
                if pending.request.namespace != id {
                    return true;
                }
                let _ = pending.events.try_send(Ok(GetAnswer::Refused(Refused {
                    reason: proto::Reason::Invalid as u8,
                    detail: "namespace was purged".into(),
                })));
                false
            });
        }
        let nodes = self.namespaces.get_mut(&id).unwrap().tree.take_all();
        let mut records = 0u64;
        let mut bytes = 0u64;
        for node in nodes {
            records += 1;
            bytes = bytes.saturating_add(node.location.bytes);
            if node.location.memory.is_some() || node.location.disk.is_some() {
                self.charged = self.charged.saturating_sub(node.location.bytes);
            }
            if let Some(disk) = &node.location.disk {
                disk.retire();
                self.namespaces
                    .get_mut(&id)
                    .unwrap()
                    .purge_drain
                    .push(Arc::downgrade(disk));
                self.deferred_disk.push(disk.clone());
            }
            self.delta(id, 2, node.key);
        }
        // Only publications that are waiting (no submitted job) can be dropped here. Ones
        // with outstanding Chunk/Finish jobs keep their ownership and charge until those
        // complete; their epoch mismatch retires the renamed file.
        let detached: Vec<(u64, u64)> = self
            .detached_puts
            .iter()
            .filter(|(_, put)| put.namespace == id && put.pending == 0)
            .map(|(key, _)| *key)
            .collect();
        for key in detached {
            if let Some(put) = self.detached_puts.remove(&key) {
                self.finish_detached_put(Some(id), put);
            }
        }
        let draining = self.purge_busy(id);
        self.namespaces.get_mut(&id).unwrap().purging = draining;
        self.metrics.evictions += records;
        (records, bytes)
    }

    fn namespace_charge(&self, id: NamespaceId, memory_only: bool) -> u64 {
        self.namespaces
            .get(&id)
            .map(|state| {
                state
                    .tree
                    .nodes()
                    .filter(|node| {
                        if memory_only {
                            node.location.memory.is_some()
                        } else {
                            node.location.memory.is_some() || node.location.disk.is_some()
                        }
                    })
                    .map(|node| node.location.bytes)
                    .sum()
            })
            .unwrap_or(0)
    }

    fn pending_reserved(&self, id: NamespaceId) -> u64 {
        let mut total = 0u64;
        for session in self.sessions.values() {
            for open in session.ops.values() {
                if let Operation::Put(put) = &open.operation
                    && put.namespace == id
                    && !put.finished
                {
                    total = total.saturating_add(put.reservation);
                }
            }
        }
        for put in self.detached_puts.values() {
            if put.namespace == id {
                total = total.saturating_add(put.reservation);
            }
        }
        for drain in &self.retired_puts {
            if drain.namespace == id {
                total = total.saturating_add(drain.reservation);
            }
        }
        total
    }

    /// Capacity held by unfinished Puts of this namespace on the memory side: a streaming
    /// Put's staging hold, or a non-streaming Put's arena reservation. A streaming Put's disk
    /// reservation belongs to the durable side and is counted by `pending_reserved`.
    fn pending_memory(&self, id: NamespaceId) -> u64 {
        fn held(put: &Put) -> u64 {
            put.staging_hold
                .as_ref()
                .map_or(put.reservation, |hold| hold.capacity() as u64)
        }
        let mut total = 0u64;
        for session in self.sessions.values() {
            for open in session.ops.values() {
                if let Operation::Put(put) = &open.operation
                    && put.namespace == id
                    && !put.finished
                {
                    total = total.saturating_add(held(put));
                }
            }
        }
        for put in self.detached_puts.values() {
            if put.namespace == id {
                total = total.saturating_add(held(put));
            }
        }
        for drain in &self.retired_puts {
            if drain.namespace == id {
                total = total.saturating_add(
                    drain
                        ._staging_hold
                        .as_ref()
                        .map_or(drain.reservation, |hold| hold.capacity() as u64),
                );
            }
        }
        total
    }

    fn quota_excess(&self, id: NamespaceId) -> u64 {
        let quota = self.quotas.get(&id).copied().unwrap_or_default();
        let pending = self.pending_reserved(id);
        let pending_memory = self.pending_memory(id);
        let physical = self.namespace_charge(id, false).saturating_add(pending);
        let memory = self
            .namespace_charge(id, true)
            .saturating_add(pending_memory);
        let nvme = if quota.nvme == 0 {
            0
        } else {
            physical.saturating_sub(quota.nvme)
        };
        let dram = if quota.dram == 0 {
            0
        } else {
            memory.saturating_sub(quota.dram)
        };
        nvme.max(dram)
    }

    /// Evict this namespace's eligible leaves until both limits hold. Pinned or leased records
    /// stay and are reported as excess.
    fn enforce_quota(&mut self, id: NamespaceId) {
        loop {
            let Some(quota) = self.quotas.get(&id).copied() else {
                return;
            };
            let pending = self.pending_reserved(id);
            let pending_memory = self.pending_memory(id);
            let physical = self.namespace_charge(id, false).saturating_add(pending);
            let memory = self
                .namespace_charge(id, true)
                .saturating_add(pending_memory);
            let over_nvme = quota.nvme != 0 && physical > quota.nvme;
            let over_dram = quota.dram != 0 && memory > quota.dram;
            if !over_nvme && !over_dram {
                return;
            }
            let memory_only = over_dram && !over_nvme;
            let Some(key) = self.quota_candidate(id, memory_only) else {
                return;
            };
            let before = (
                self.namespace_charge(id, false),
                self.namespace_charge(id, true),
            );
            if memory_only {
                self.evict_memory_of(id, key);
            } else {
                self.evict_accounted(id, key);
            }
            let after = (
                self.namespace_charge(id, false),
                self.namespace_charge(id, true),
            );
            if after == before {
                // Nothing eligible left to free; the remaining excess is blocked.
                return;
            }
        }
    }

    fn quota_candidate(&self, id: NamespaceId, memory_only: bool) -> Option<Key> {
        let namespace = self.namespaces.get(&id)?;
        namespace
            .tree
            .nodes()
            .filter(|node| {
                !node.pinned()
                    && node.children() == 0
                    && (node.location.memory.is_some() || node.location.disk.is_some())
                    && (!memory_only || node.location.memory.is_some())
            })
            .min_by_key(|node| (node.accessed(), node.key.consumed_tokens))
            .map(|node| node.key)
    }

    fn evict_memory_of(&mut self, id: NamespaceId, key: Key) {
        let disk = self.namespaces[&id]
            .tree
            .node(&key)
            .is_some_and(|node| node.location.disk.is_some());
        if disk {
            let node = self
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&key)
                .unwrap();
            let old = node.location.bytes;
            node.location.memory = None;
            node.location.bytes = record_charge(&node.location.header, false);
            self.charged = self.charged - old + node.location.bytes;
            self.delta(id, 1, key);
        } else {
            self.evict_accounted(id, key);
        }
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

    /// `nvme_charge` is the incoming record's durable reservation; `memory_charge` is what it
    /// holds in memory (its arena extent, or only the staging hold for a streamed record).
    fn quota_allows(&self, id: NamespaceId, nvme_charge: u64, memory_charge: u64) -> bool {
        let Some(quota) = self.quotas.get(&id).copied() else {
            return true;
        };
        let pending = self.pending_reserved(id);
        let pending_memory = self.pending_memory(id);
        let physical = self
            .namespace_charge(id, false)
            .saturating_add(pending)
            .saturating_add(nvme_charge);
        if quota.nvme != 0 && physical > quota.nvme {
            return false;
        }
        let retained = self
            .namespace_charge(id, true)
            .saturating_add(pending_memory)
            .saturating_add(memory_charge);
        !(quota.dram != 0 && retained > quota.dram)
    }

    fn admin(&mut self, command: AdminCommand) {
        match command {
            AdminCommand::Stats(reply) => {
                let _ = reply.send(Ok(self.metrics_text()));
            }
            AdminCommand::Pin {
                namespace,
                keys,
                pin,
                reply,
            } => {
                let result = (|| -> Result<u32> {
                    let id: NamespaceId = namespace
                        .as_slice()
                        .try_into()
                        .map_err(|_| invalid("namespace_id must be 32 bytes"))?;
                    let state = self
                        .namespaces
                        .get_mut(&id)
                        .ok_or_else(|| invalid("unknown namespace"))?;
                    let mut changed = 0;
                    for key in keys {
                        if let Ok(transition) = state.tree.admin_pin(&key, pin) {
                            changed += transition;
                        }
                    }
                    Ok(changed)
                })();
                let _ = reply.send(result);
            }
            AdminCommand::Purge {
                namespace,
                all_namespaces,
                reply,
            } => {
                let result = (|| -> Result<(u64, u64)> {
                    let ids: Vec<NamespaceId> = if all_namespaces {
                        self.namespaces.keys().copied().collect()
                    } else {
                        vec![
                            namespace
                                .as_slice()
                                .try_into()
                                .map_err(|_| invalid("namespace_id must be 32 bytes"))?,
                        ]
                    };
                    let mut records = 0u64;
                    let mut bytes = 0u64;
                    for id in ids {
                        let (purged, purged_bytes) = self.purge_namespace(id);
                        records += purged;
                        bytes = bytes.saturating_add(purged_bytes);
                    }
                    Ok((records, bytes))
                })();
                let _ = reply.send(result);
            }
            AdminCommand::SetQuota {
                namespace,
                dram,
                nvme,
                reply,
            } => {
                let result = (|| -> Result<u64> {
                    let id: NamespaceId = namespace
                        .as_slice()
                        .try_into()
                        .map_err(|_| invalid("namespace_id must be 32 bytes"))?;
                    if !self.namespaces.contains_key(&id) {
                        return Err(invalid("unknown namespace"));
                    }
                    self.quotas.insert(id, Quota { dram, nvme });
                    self.enforce_quota(id);
                    Ok(self.quota_excess(id))
                })();
                let _ = reply.send(result);
            }
        }
    }

    pub fn take_forward_events(&mut self) -> Vec<ForwardEvent> {
        std::mem::take(&mut self.forward_events)
    }

    /// The records of one requested chain, as the local catalog sees them.
    pub fn chain_records(&self, id: NamespaceId, chain: &[Key]) -> Vec<ChainRecord> {
        let Some(namespace) = self.namespaces.get(&id) else {
            return Vec::new();
        };
        let endpoint = chain.last().copied();
        chain
            .iter()
            .filter_map(|key| namespace.tree.node(key))
            .map(|node| ChainRecord {
                key: node.key,
                bytes: node.location.bytes,
                tail_pages: node.tail_pages as u32,
                row: node.row,
                remote: node.location.remote.is_some(),
                local: node.location.memory.is_some() || node.location.disk.is_some(),
                source: node
                    .location
                    .remote
                    .as_ref()
                    .map(|source| source.node.clone()),
                endpoint: Some(node.key) == endpoint,
                length: node.location.header.length_with_row(),
            })
            .collect()
    }

    /// Pin a record and its ancestors, so an eviction pass cannot take them while the caller
    /// installs its own bytes. Returns false when the record is gone.
    fn pin_chain(&mut self, id: NamespaceId, key: Key) -> bool {
        let mut chain = Vec::new();
        let mut current = Some(key);
        while let Some(key) = current {
            chain.push(key);
            current = self
                .namespaces
                .get(&id)
                .and_then(|namespace| namespace.tree.node(&key))
                .and_then(|node| node.parent);
        }
        if chain.first() != Some(&key) {
            return false;
        }
        let mut pinned = 0usize;
        for key in &chain {
            match self
                .namespaces
                .get_mut(&id)
                .map(|namespace| namespace.tree.pin(key))
            {
                Some(Ok(())) => pinned += 1,
                _ => {
                    for key in chain.iter().take(pinned) {
                        let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
                    }
                    return false;
                }
            }
        }
        true
    }

    fn unpin_chain(&mut self, id: NamespaceId, key: Key) {
        let mut current = Some(key);
        while let Some(key) = current {
            let Some(namespace) = self.namespaces.get_mut(&id) else {
                return;
            };
            if namespace.tree.unpin(&key).is_err() {
                return;
            }
            current = namespace.tree.node(&key).and_then(|node| node.parent);
        }
    }

    /// Install a fetched record as a local DRAM copy. The catalog may have moved on; a record
    /// that vanished or changed shape is refused and the caller drops the extent.
    fn adopt_remote(
        &mut self,
        id: NamespaceId,
        key: Key,
        extent: Arc<Extent>,
        row: bool,
    ) -> Result<()> {
        let bytes = extent.bytes();
        if bytes.len() < record::HEADER_BYTES {
            return Err(invalid("adopted record is shorter than a header"));
        }
        let header = Header::decode(&bytes[..record::HEADER_BYTES])?;
        let (previous, page_only_disk, descriptor) = {
            let namespace = self
                .namespaces
                .get(&id)
                .ok_or_else(|| invalid("adopted namespace is not registered"))?;
            let node = namespace
                .tree
                .node(&key)
                .ok_or_else(|| invalid("adopted record is not in the catalog"))?;
            if node.location.memory.is_some() && (node.row || !row) {
                // A complete copy, or a page-only copy when that is all this fetch carries.
                return Ok(());
            }
            if node.location.writing || node.location.job.is_some() {
                // The bytes on disk are owned by an in-flight write-back or compaction; a
                // fetched copy cannot be installed without racing its completion.
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "the record has a disk job in flight",
                ));
            }
            if header.key != key
                || header.parent != node.parent
                || header.tail_pages as usize != node.tail_pages
            {
                return Err(invalid("adopted record changed the chain shape"));
            }
            let previous = node
                .location
                .memory
                .is_some()
                .then_some(node.location.bytes);
            // A page-only disk copy cannot satisfy the delivered row; queue its rewrite.
            let page_only_disk = row && node.location.disk.is_some() && !node.row;
            (previous, page_only_disk, namespace.descriptor.clone())
        };
        if header.identity != descriptor.identity
            || header.fingerprint != descriptor.fingerprint
            || !header.matches(&descriptor)
            || header.flags != 0
            || header.length(row) as usize > bytes.len()
        {
            return Err(invalid("adopted record does not match the namespace"));
        }
        let charge = record_charge(&header, true);
        if !self.quota_allows(id, charge, charge) {
            return Err(invalid("namespace quota blocks the fetched record"));
        }
        let previous = previous.unwrap_or(0);
        // Adoption spends the same global budget as a local publication. Pin the record and
        // its ancestors first: make_room may evict, but never what is about to be replaced.
        if !self.pin_chain(id, key) {
            return Err(invalid("adopted record is not in the catalog"));
        }
        let made = self.make_room_with(Some(id), charge.saturating_sub(previous), &[]);
        self.unpin_chain(id, key);
        if !made {
            return Err(invalid("no room for the fetched record"));
        }
        {
            let namespace = self.namespaces.get_mut(&id).unwrap();
            let node = namespace.tree.node_mut(&key).unwrap();
            node.location.origin = Origin::Peer;
            node.location.header = header.clone();
            node.location.memory = Some(extent);
            node.location.bytes = charge;
            node.location.retry = Instant::now();
            if page_only_disk {
                node.location.rewrite = true;
            }
            node.row = row;
        }
        self.charged = self.charged.saturating_sub(previous).saturating_add(charge);
        self.metrics.remote_fetched += 1;
        self.clock += 1;
        let clock = self.clock;
        self.namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .access_at(&key, clock);
        self.refresh_durability(id);
        self.delta(id, 1, key);
        self.drain_remote_deferred(id);
        Ok(())
    }

    /// Allocate the DRAM destinations for a remote fetch through the same room making an
    /// admission uses, so eviction can free space and the allocation stays visible to
    /// admission while the transfer runs. The chain stays pinned until the serving Get plans
    /// its own pins or the reservation is released. `None` classifies a chain that cannot fit
    /// the arena as a miss: there is no windowed fetch path.
    fn reserve_fetch(
        &mut self,
        id: NamespaceId,
        chain: &[Key],
        lengths: &[u64],
        connection: u64,
    ) -> Result<Option<FetchReservation>> {
        // A fetch whose connection has already closed has no Get left to serve: do not pin.
        if !self.namespaces.contains_key(&id) || !self.sessions.contains_key(&connection) {
            return Ok(None);
        }
        let total = lengths
            .iter()
            .fold(0u64, |total, length| total.saturating_add(*length));
        if total > self.memory.capacity() as u64 {
            return Ok(None);
        }
        // Keep the chain the fetch is served from and adopts: admission may evict otherwise.
        let mut pinned = Vec::new();
        for key in chain {
            if let Some(namespace) = self.namespaces.get_mut(&id)
                && namespace.tree.pin(key).is_ok()
            {
                pinned.push(*key);
            }
        }
        let durable = self.config.root.is_some();
        let mut extents = Vec::with_capacity(lengths.len());
        let mut fitted = true;
        for length in lengths {
            let Ok(bytes) = usize::try_from(*length) else {
                fitted = false;
                break;
            };
            match self.memory_room_excluding(bytes, durable, &[]) {
                Some(extent) => extents.push(Arc::new(extent)),
                None => {
                    fitted = false;
                    break;
                }
            }
        }
        if !fitted {
            if let Some(namespace) = self.namespaces.get_mut(&id) {
                for key in pinned {
                    let _ = namespace.tree.unpin(&key);
                }
            }
            return Ok(None);
        }
        self.fetch_counter += 1;
        let token = self.fetch_counter;
        self.fetch_pins.insert(
            token,
            FetchPin {
                connection,
                namespace: id,
                keys: pinned,
            },
        );
        Ok(Some((token, extents)))
    }

    /// A record with no bytes of its own, no children, no remote source and no pin cannot
    /// serve anything and holds no charge; it is garbage.
    fn dead_anchor(node: &crate::tree::Node<Location>) -> bool {
        !node.pinned()
            && node.location.memory.is_none()
            && node.location.disk.is_none()
            && node.location.remote.is_none()
            && node.children() == 0
    }

    /// Removes a dead remote node and then any ancestor that is now equally dead.
    /// `remove_node` never cascades, so every node is checked before it goes, and a node with
    /// local bytes is left alone: eviction owns its charge accounting and disk retirement.
    fn remove_dead_remote(&mut self, id: NamespaceId, key: Key) {
        let mut current = Some(key);
        while let Some(key) = current {
            let dead = self
                .namespaces
                .get(&id)
                .and_then(|namespace| namespace.tree.node(&key))
                .is_some_and(Self::dead_anchor);
            if !dead {
                break;
            }
            let removed = self
                .namespaces
                .get_mut(&id)
                .and_then(|namespace| namespace.tree.remove_node(&key).ok());
            let Some(node) = removed else {
                break;
            };
            self.delta(id, 2, node.key);
            current = node.parent;
        }
    }

    /// Releases the pins a fetch reservation holds. Idempotent.
    fn release_fetch(&mut self, token: u64) {
        let Some(pin) = self.fetch_pins.remove(&token) else {
            return;
        };
        if let Some(namespace) = self.namespaces.get_mut(&pin.namespace) {
            for key in pin.keys {
                let _ = namespace.tree.unpin(&key);
            }
        }
    }

    fn apply_catalog(&mut self, node: &str, id: NamespaceId, event: proto::CatalogEvent) {
        use proto::catalog_event::Event;
        let Some(body) = event.event else {
            return;
        };
        let view_key = (id, node.to_string());
        match body {
            Event::SnapshotBegin(_) => {
                self.remote_snapshots
                    .entry(view_key.clone())
                    .or_default()
                    .clear();
                self.remote_deferred.remove(&view_key);
            }
            Event::Entry(entry) => {
                let Some((key, remote)) = remote_entry(&entry) else {
                    return;
                };
                let limit = self.config.remote_catalog_entries.max(1);
                if let Some(snapshot) = self.remote_snapshots.get_mut(&view_key)
                    && (snapshot.len() < limit || snapshot.contains_key(&key))
                {
                    snapshot.insert(key, remote);
                }
            }
            Event::SnapshotEnd(_) => {
                let Some(snapshot) = self.remote_snapshots.remove(&view_key) else {
                    return;
                };
                let previous = self
                    .remote_views
                    .insert(view_key, snapshot.clone())
                    .unwrap_or_default();
                let removed: Vec<Key> = previous
                    .keys()
                    .filter(|key| !snapshot.contains_key(key))
                    .copied()
                    .collect();
                for key in removed {
                    self.remove_remote_source(id, node, &key);
                }
                for (key, remote) in snapshot {
                    self.upsert_remote(id, node, key, remote);
                }
                self.drain_remote_deferred(id);
            }
            Event::Added(entry) | Event::Changed(entry) => {
                if self.remote_snapshots.contains_key(&view_key) {
                    return;
                }
                let Some((key, remote)) = remote_entry(&entry) else {
                    return;
                };
                let limit = self.config.remote_catalog_entries.max(1);
                let view = self.remote_views.entry(view_key).or_default();
                if view.len() < limit || view.contains_key(&key) {
                    view.insert(key, remote.clone());
                } else {
                    self.metrics.remote_refused += 1;
                    return;
                }
                self.upsert_remote(id, node, key, remote);
                self.drain_remote_deferred(id);
            }
            Event::Removed(key) => {
                if self.remote_snapshots.contains_key(&view_key) {
                    return;
                }
                let Some(key) = remote_key(Some(key)) else {
                    return;
                };
                if let Some(view) = self.remote_views.get_mut(&view_key) {
                    view.remove(&key);
                }
                if let Some(deferred) = self.remote_deferred.get_mut(&view_key) {
                    deferred.remove(&key);
                }
                self.remove_remote_source(id, node, &key);
            }
            Event::Room(_) => {}
        }
    }

    /// Retry deferred remote entries whose parent is now indexed.
    fn drain_remote_deferred(&mut self, id: NamespaceId) {
        loop {
            let ready: Vec<(String, Key)> = self
                .remote_deferred
                .iter()
                .filter(|((namespace, _), _)| *namespace == id)
                .flat_map(|((_, node), deferred)| {
                    deferred
                        .iter()
                        .filter(|(_, entry)| {
                            entry.parent.is_none_or(|parent| {
                                self.namespaces
                                    .get(&id)
                                    .is_some_and(|namespace| namespace.tree.node(&parent).is_some())
                            })
                        })
                        .map(move |(key, _)| (node.clone(), *key))
                })
                .collect();
            if ready.is_empty() {
                return;
            }
            for (node, key) in ready {
                let entry = self
                    .remote_deferred
                    .get_mut(&(id, node.clone()))
                    .and_then(|deferred| deferred.remove(&key));
                if let Some(entry) = entry {
                    self.upsert_remote(id, &node, key, entry);
                }
            }
        }
    }

    fn upsert_remote(&mut self, id: NamespaceId, node: &str, key: Key, remote: RemoteEntry) {
        let source = (!remote.unreadable).then(|| RemoteSource {
            node: node.to_string(),
            bytes: remote.bytes,
            row: remote.row,
        });
        let existed = self
            .namespaces
            .get(&id)
            .is_some_and(|namespace| namespace.tree.node(&key).is_some());
        if !existed {
            let parent_ok = remote.parent.is_none_or(|parent| {
                self.namespaces
                    .get(&id)
                    .is_some_and(|namespace| namespace.tree.node(&parent).is_some())
            });
            if !parent_ok {
                // A local publication or an earlier snapshot entry may still supply the
                // parent; keep the child until it does.
                let deferred = self
                    .remote_deferred
                    .entry((id, node.to_string()))
                    .or_default();
                if deferred.len() < self.config.remote_catalog_entries.max(1) {
                    deferred.insert(key, remote);
                } else {
                    self.metrics.remote_refused += 1;
                }
                return;
            }
            if source.is_some()
                && self.remote_only_count(id) >= self.config.remote_catalog_entries.max(1)
            {
                // Never evict the ancestor the incoming record needs.
                let mut protected = std::collections::HashSet::new();
                let mut parent = remote.parent;
                while let Some(current) = parent {
                    protected.insert(current);
                    parent = self
                        .namespaces
                        .get(&id)
                        .and_then(|namespace| namespace.tree.node(&current))
                        .and_then(|node| node.parent);
                }
                if !self.evict_remote(id, &protected) {
                    self.metrics.remote_refused += 1;
                    return;
                }
            }
        }
        {
            let Some(namespace) = self.namespaces.get_mut(&id) else {
                return;
            };
            if let Some(existing) = namespace.tree.node(&key) {
                if existing.parent != remote.parent
                    || existing.bucket != remote.bucket
                    || existing.tail_pages != remote.tail_pages as usize
                {
                    self.metrics.remote_conflicts += 1;
                    return;
                }
                let local = existing.location.memory.is_some() || existing.location.disk.is_some();
                // A local copy knows whether its own bytes carry the row; the upstream's flag
                // must not claim a row this manager does not have.
                let row = if local { existing.row } else { remote.row };
                let node_ref = namespace.tree.node_mut(&key).unwrap();
                node_ref.row = row;
                node_ref.location.remote = source;
                if !local {
                    node_ref.location.bytes = remote.bytes;
                }
            } else {
                let Ok(header) = Header::new(
                    &namespace.descriptor,
                    key,
                    remote.parent,
                    remote.bucket,
                    remote.tail_pages,
                ) else {
                    return;
                };
                let location = Location {
                    origin: Origin::Peer,
                    header,
                    memory: None,
                    disk: None,
                    remote: source,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: remote.bytes,
                };
                if namespace
                    .tree
                    .insert(
                        key,
                        remote.parent,
                        remote.bucket,
                        remote.tail_pages as usize,
                        remote.row,
                        location,
                    )
                    .is_err()
                {
                    return;
                }
            }
        }
        self.delta(id, if existed { 1 } else { 0 }, key);
    }

    fn remove_remote_source(&mut self, id: NamespaceId, node: &str, key: &Key) {
        let remove = {
            let Some(namespace) = self.namespaces.get_mut(&id) else {
                return;
            };
            let Some(existing) = namespace.tree.node(key) else {
                return;
            };
            if existing
                .location
                .remote
                .as_ref()
                .is_none_or(|source| source.node != node)
            {
                return;
            }
            let local = existing.location.memory.is_some() || existing.location.disk.is_some();
            let children = existing.children();
            let pinned = existing.pinned();
            if !local && children == 0 && !pinned {
                true
            } else {
                let node_ref = namespace.tree.node_mut(key).unwrap();
                node_ref.location.remote = None;
                // A bare anchor held only by a lease is dead once the lease ends; a serving
                // Get has it pinned right now. Retry from the cleanup turn.
                if !local && children == 0 && !self.remote_orphans.contains(&(id, *key)) {
                    self.remote_orphans.push((id, *key));
                }
                false
            }
        };
        if remove {
            // The source is what is being forgotten; the node is dead once it is cleared.
            if let Some(namespace) = self.namespaces.get_mut(&id) {
                if let Some(node) = namespace.tree.node_mut(key) {
                    node.location.remote = None;
                }
            }
            self.remove_dead_remote(id, *key);
        } else {
            self.delta(id, 1, *key);
        }
    }

    fn remote_only_count(&self, id: NamespaceId) -> usize {
        self.namespaces
            .get(&id)
            .map(|namespace| {
                namespace
                    .tree
                    .nodes()
                    .filter(|node| {
                        node.location.remote.is_some()
                            && node.location.memory.is_none()
                            && node.location.disk.is_none()
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn evict_remote(
        &mut self,
        id: NamespaceId,
        protected: &std::collections::HashSet<Key>,
    ) -> bool {
        let candidate = self.namespaces.get(&id).and_then(|namespace| {
            namespace
                .tree
                .nodes()
                .filter(|node| {
                    node.location.remote.is_some()
                        && node.location.memory.is_none()
                        && node.location.disk.is_none()
                        && node.children() == 0
                        && !node.pinned()
                        && !protected.contains(&node.key)
                })
                .min_by_key(|node| (node.accessed(), node.key.consumed_tokens))
                .map(|node| node.key)
        });
        let Some(key) = candidate else {
            return false;
        };
        // Dropping the catalogue entry drops its source; the node is dead from here.
        if let Some(namespace) = self.namespaces.get_mut(&id) {
            if let Some(node) = namespace.tree.node_mut(&key) {
                node.location.remote = None;
            }
        }
        self.remove_dead_remote(id, key);
        self.namespaces
            .get(&id)
            .and_then(|namespace| namespace.tree.node(&key))
            .is_none()
    }

    fn peer_gone(&mut self, node: &str) {
        let ids: Vec<NamespaceId> = self
            .remote_views
            .keys()
            .filter(|(_, name)| name == node)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.remote_views.remove(&(id, node.to_string()));
            self.remote_snapshots.remove(&(id, node.to_string()));
            self.remote_deferred.remove(&(id, node.to_string()));
            let live: Vec<Key> = self
                .namespaces
                .get(&id)
                .map(|namespace| {
                    namespace
                        .tree
                        .nodes()
                        .filter(|record| {
                            record
                                .location
                                .remote
                                .as_ref()
                                .is_some_and(|source| source.node == node)
                        })
                        .map(|record| record.key)
                        .collect()
                })
                .unwrap_or_default();
            for key in live {
                self.remove_remote_source(id, node, &key);
            }
        }
    }

    /// Leases a stored record's bytes for write-through forwarding.
    pub fn forward_source(&self, namespace: NamespaceId, key: Key) -> Result<ForwardRecord> {
        let node = self
            .namespaces
            .get(&namespace)
            .and_then(|namespace| namespace.tree.node(&key))
            .ok_or_else(|| invalid("forward source is not stored"))?;
        let memory = node.location.memory.clone();
        let disk = node.location.disk.clone();
        if memory.is_none() && disk.is_none() {
            return Err(invalid("forward source has no bytes"));
        }
        if let Some(extent) = &memory {
            extent.acquire();
        }
        if let Some(extent) = &disk {
            extent.acquire();
        }
        Ok(ForwardRecord {
            header: node.location.header.clone(),
            memory,
            disk,
        })
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
        // Advertising may claim a row the upstream holds; refetch decisions use the local flag.
        let row = node.row
            || node
                .location
                .remote
                .as_ref()
                .is_some_and(|source| source.row);
        EntryData {
            key,
            parent: node.parent,
            bucket: node.bucket,
            tail_pages: node.tail_pages as u32,
            row,
            tier: if node.location.memory.is_some() {
                0
            } else if node.location.durable {
                1
            } else if node.location.remote.is_some() {
                2
            } else {
                1
            },
            durable: node.location.durable,
            bytes: node.location.bytes,
            unreadable: node.location.memory.is_none()
                && node.location.disk.is_none()
                && node.location.remote.is_none(),
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
            let durable = node.location.disk.is_some() && parent_ok && !node.location.rewrite;
            if node.location.durable != durable {
                node.location.durable = durable;
                flipped.push((key, durable));
            }
        }
        for (key, durable) in flipped {
            self.delta(id, 1, key);
            if durable
                && self.namespaces[&id]
                    .tree
                    .node(&key)
                    .is_some_and(|node| node.location.origin == Origin::Local)
            {
                self.forward_events
                    .push(ForwardEvent::Durable { namespace: id, key });
            }
        }
    }

    fn charge_total(&self) -> u64 {
        self.charged + self.local.staging_used() as u64
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
        if self.namespaces[&id].purging {
            // The previous namespace contents are still being released; a new record could
            // race their disk completions.
            self.metrics.retry_queue += 1;
            self.reject_put(&events, proto::Reason::Queue as u8);
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
            // A catalog entry that only names an upstream copy is not a local record.
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
            // A peer names the exact ancestor it forwarded under; the near side restores a
            // missing ancestor and retries the child. A local Put only hints at its parent and
            // falls back to a root below when the hint is unusable.
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
        let child_header = match Header::new(
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
        let root_header = match Header::new(
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
        // what else must stay resident is a Room question, not OVERSIZE.
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
        // resident is a Budget, and without Room the record is simply not
        // stored.
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
        // A streaming refusal still has the arena shape as a retry candidate
        // whenever the record fits an arena extent.
        let alternative = (!selected.streaming).then_some(selected);
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
                            &child_header,
                            &selected_pins,
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
                            RootChoice::Wait(requirement, protected) => {
                                self.metrics.retry_budget += 1;
                                self.arm(connection, requirement, protected, None);
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
        // has no arena shape either, no Room can ever arrive, so refuse it as
        // OVERSIZE instead of arming a wait. A record that still fits an arena
        // extent waits for arena space.
        if requirement.streaming && requirement.allocation > self.staging.capacity() as u64 {
            if selected.streaming {
                self.metrics.oversize += 1;
                self.reject_put(&events, proto::Reason::Oversize as u8);
                return;
            }
            self.metrics.retry_budget += 1;
            self.arm(connection, selected, pins.clone(), alternative);
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
            self.arm(connection, requirement, pins.clone(), alternative);
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
                    self.arm(connection, requirement, pins.clone(), alternative);
                    self.reject_put(&events, proto::Reason::Budget as u8);
                    return;
                }
            }
        } else {
            None
        };
        if !self.quota_allows(
            id,
            requirement.reservation,
            if requirement.streaming {
                requirement.allocation
            } else {
                requirement.reservation
            },
        ) {
            drop(memory);
            drop(staging_hold);
            for key in &pins {
                let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
            }
            self.reject_put(&events, proto::Reason::Quota as u8);
            return;
        }
        if !self.admit(connection, requirement.charge) {
            drop(memory);
            drop(staging_hold);
            for key in &pins {
                let _ = self.namespaces.get_mut(&id).unwrap().tree.unpin(key);
            }
            self.metrics.retry_budget += 1;
            self.arm(connection, requirement, pins.clone(), alternative);
            self.reject_put(&events, proto::Reason::Budget as u8);
            return;
        }
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
        let first =
            parent.map_or(0, |parent| parent.consumed_tokens / descriptor.page_tokens) as u32;
        let grants = plan_put_grants(&header, first, self.config.max_window_bytes);
        let pages = vec![None; header.tail_pages as usize];
        let epoch = self.namespaces[&id].epoch;
        let remote = self
            .sessions
            .get(&connection)
            .is_some_and(|session| session.remote);
        let mut put = Put {
            namespace: id,
            epoch,
            remote,
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

    fn admit(&mut self, connection: u64, bytes: u64) -> bool {
        let now = Instant::now();
        self.sessions
            .get_mut(&connection)
            .unwrap()
            .budget
            .admit(bytes, now)
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

    /// Rootless fallback when the arena cannot hold the child beside its
    /// parent: try to store the whole chain as one record, otherwise report
    /// what the retry has to wait for. The caller has already refused a child
    /// whose own extent exceeds the arena, so the child's shape is
    /// structurally possible; a wait can succeed once unrelated records leave,
    /// and only the child's own requirement and its protected parent are
    /// waited on.
    fn rootless_fallback(
        &mut self,
        child: &Header,
        child_pins: &[Key],
        root: &Header,
        root_pins: Vec<Key>,
        arena_bytes: u64,
    ) -> RootChoice {
        let requirement = self.put_requirement(root, arena_bytes);
        if requirement.streaming {
            let waiting = self.put_requirement(child, arena_bytes);
            return RootChoice::Wait(waiting, child_pins.to_vec());
        }
        match self.memory_room_excluding(root.length_with_row() as usize, false, &root_pins) {
            Some(extent) => RootChoice::Store(extent),
            None => RootChoice::Wait(requirement, root_pins),
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

    /// Arm Room with the requirement of the shape that was just refused, the
    /// arena shape that may become possible later, and the keys the shape
    /// needs to survive. The latest refusal replaces the previous one, so an
    /// older, unserviceable requirement cannot keep Room tied to a charge this
    /// operation no longer needs.
    fn arm(
        &mut self,
        connection: u64,
        requirement: PutRequirement,
        protected: Vec<Key>,
        alternative: Option<PutRequirement>,
    ) {
        self.local.arm_budget(connection, requirement.charge);
        self.armed.insert(
            connection,
            ArmedRoom {
                requirement,
                alternative,
                protected,
            },
        );
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
            let requirement = put.requirement;
            let protected: Vec<Key> = put.pins.iter().copied().collect();
            (
                index,
                plan,
                header,
                memory,
                hold,
                hold_unit,
                slot,
                requirement,
                protected,
            )
        };
        let (index, plan, header, memory, hold, hold_unit, slot, requirement, protected) = prepared;
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
                self.arm(connection, requirement, protected, None);
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
        PublishTarget::Replace(node.location.remote.clone())
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
            PublishTarget::Insert | PublishTarget::Replace(_) => {}
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
            let remote = put.remote;
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
                remote,
                key,
                already_terminal,
                record_bytes,
            )
        };
        let (header, memory, bytes, reservation, pins, remote, key, already_terminal, record_bytes) =
            prepared;
        let (replace, source) = match target {
            PublishTarget::Replace(source) => (true, source),
            _ => (false, None),
        };
        let location = Location {
            origin: if remote { Origin::Peer } else { Origin::Local },
            header: header.clone(),
            remote: source,
            memory,
            disk,
            durable: false,
            writing: false,
            job: None,
            rewrite: false,
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
        if !remote {
            self.forward_events
                .push(ForwardEvent::Stored { namespace: id, key });
        }
        self.delta(id, if replace { 1 } else { 0 }, key);
        self.drain_remote_deferred(id);
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
            let (namespace, pins, keys) = {
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
                (
                    get.namespace,
                    std::mem::take(&mut get.pins),
                    get.records
                        .iter()
                        .map(|record| record.header.key)
                        .collect::<Vec<_>>(),
                )
            };
            self.metrics.gets_ok += 1;
            self.release_pins(namespace, pins);
            if !self.config.retain_remote {
                for key in keys {
                    self.release_fetched(namespace, key);
                }
            }
        }
    }

    /// Drop a fetched copy after its Get; the record falls back to its upstream source.
    fn release_fetched(&mut self, id: NamespaceId, key: Key) {
        let Some(node) = self
            .namespaces
            .get_mut(&id)
            .and_then(|namespace| namespace.tree.node_mut(&key))
        else {
            return;
        };
        if node.location.origin != Origin::Peer || node.location.remote.is_none() {
            return;
        }
        if node.location.memory.is_none() {
            return;
        }
        let old = node.location.bytes;
        node.location.memory = None;
        node.location.bytes = node
            .location
            .remote
            .as_ref()
            .map_or(0, |source| source.bytes);
        self.charged = self.charged.saturating_sub(old);
        self.delta(id, 1, key);
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
        self.armed.remove(&connection);
        self.local.close(connection);
        self.subscriptions.retain(|(id, _), _| *id != connection);
        // A fetch whose Get never began with this connection leaves no pins behind.
        let released: Vec<u64> = self
            .fetch_pins
            .iter()
            .filter(|(_, pin)| pin.connection == connection)
            .map(|(token, _)| *token)
            .collect();
        for token in released {
            self.release_fetch(token);
        }
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
        let remote = put.remote;
        let target = self.publish_target(id, &header);
        let (replace, source) = match target {
            PublishTarget::Insert => (false, None),
            PublishTarget::Replace(source) => (true, source),
            PublishTarget::Present | PublishTarget::Conflict => {
                self.finish_detached_put(Some(id), put);
                return;
            }
        };
        let location = Location {
            origin: if remote { Origin::Peer } else { Origin::Local },
            header: header.clone(),
            remote: source,
            memory,
            disk: Some(disk),
            durable: true,
            writing: false,
            job: None,
            rewrite: false,
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
            if !remote {
                self.forward_events
                    .push(ForwardEvent::Stored { namespace: id, key });
            }
            self.delta(id, if replace { 1 } else { 0 }, key);
            self.drain_remote_deferred(id);
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
        let mut orphans = std::mem::take(&mut self.remote_orphans);
        orphans.retain(|(id, key)| {
            let anchor = *key;
            if self
                .namespaces
                .get(id)
                .and_then(|namespace| namespace.tree.node(&anchor))
                .is_none()
            {
                // The record is gone; its entry is stale.
                return false;
            }
            // Only nodes with no local bytes are touched here: a node with bytes is left to
            // normal eviction, which owns the charge accounting and disk retirement.
            self.remove_dead_remote(*id, anchor);
            self.namespaces
                .get(id)
                .and_then(|namespace| namespace.tree.node(&anchor))
                .is_some_and(|node| !Self::dead_anchor(node))
        });
        self.remote_orphans = orphans;
        let draining: Vec<NamespaceId> = self
            .namespaces
            .iter()
            .filter(|(_, namespace)| namespace.purging)
            .map(|(id, _)| *id)
            .collect();
        for id in draining {
            self.namespaces
                .get_mut(&id)
                .unwrap()
                .purge_drain
                .retain(|extent| extent.strong_count() > 0);
            if !self.purge_busy(id) {
                self.namespaces.get_mut(&id).unwrap().purging = false;
            }
        }
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

    /// The namespace a disk job still belongs to, when completing it could touch that
    /// namespace's files out of order.
    fn job_namespace(&self, job: &Job) -> Option<NamespaceId> {
        match job {
            Job::Write { namespace, .. } | Job::Compact { namespace, .. } => Some(*namespace),
            Job::Chunk {
                connection,
                transfer,
            }
            | Job::Finish {
                connection,
                transfer,
                ..
            } => self
                .sessions
                .get(connection)
                .and_then(|session| session.ops.get(transfer))
                .and_then(|open| match &open.operation {
                    Operation::Put(put) => Some(put.namespace),
                    Operation::Get(_) => None,
                })
                .or_else(|| {
                    self.detached_puts
                        .get(&(*connection, *transfer))
                        .map(|put| put.namespace)
                }),
            Job::GetRead { .. } => None,
        }
    }

    /// True while a purged namespace still has disk work that could resolve later: retired
    /// extents, transfer drains or queued write-back/compaction/publication jobs.
    fn purge_busy(&self, id: NamespaceId) -> bool {
        self.namespaces.get(&id).is_some_and(|namespace| {
            namespace
                .purge_drain
                .iter()
                .any(|extent| extent.strong_count() > 0)
        }) || self.retired_puts.iter().any(|drain| drain.namespace == id)
            || self
                .jobs
                .values()
                .any(|job| self.job_namespace(job) == Some(id))
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
                        node.location.rewrite = false;
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
        let now = Instant::now();
        for session in self.sessions.values_mut() {
            session.budget.refill(now);
        }
        self.timeouts();
        self.room();
    }

    fn timeouts(&mut self) {
        let mut expired = Vec::new();
        for (connection, session) in &self.sessions {
            for (transfer, open) in &session.ops {
                if open.progress.elapsed() >= NO_PROGRESS {
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

    fn room(&mut self) {
        let connections: Vec<u64> = self.sessions.keys().copied().collect();
        for connection in connections {
            let Some(armed) = self.armed.get(&connection).cloned() else {
                continue;
            };
            let namespace = self
                .sessions
                .get(&connection)
                .and_then(|session| session.namespace);
            let _ = self.make_room_with(namespace, armed.requirement.charge, &armed.protected);
            let budget_ok = self
                .sessions
                .get(&connection)
                .is_some_and(|session| session.budget.balance() >= 0);
            let mut bytes = armed.requirement.charge;
            let mut capacity = self.room_capacity(armed.requirement, &armed.protected);
            if !capacity {
                // The refused streaming shape may still be unserviceable
                // while the arena shape becomes possible again.
                if let Some(alternative) = armed.alternative.filter(|alt| *alt != armed.requirement)
                {
                    let _ = self.make_room_with(namespace, alternative.charge, &armed.protected);
                    if self.room_capacity(alternative, &armed.protected) {
                        capacity = true;
                        bytes = alternative.charge;
                    }
                }
            }
            if budget_ok && capacity && self.local.take_armed(connection).is_some() {
                self.armed.remove(&connection);
                self.events.push(StoreEvent::Room { connection, bytes });
            }
        }
    }

    /// A retry can be admitted now when the same allocator path admission
    /// uses succeeds: the fixed staging hold fits, or the arena allocation
    /// succeeds with the shape's protected keys left in place.
    fn room_capacity(&mut self, requirement: PutRequirement, protected: &[Key]) -> bool {
        if requirement.streaming {
            self.charge_total().saturating_add(requirement.charge) <= self.config.nvme
                && requirement.allocation <= self.staging.capacity() as u64
                && self
                    .staging
                    .allocate(requirement.allocation as usize)
                    .is_some()
        } else {
            let arena_ok = requirement.allocation <= self.memory.capacity() as u64
                && self
                    .memory_room_excluding(
                        requirement.allocation as usize,
                        self.config.root.is_some(),
                        protected,
                    )
                    .is_some();
            arena_ok
                && (self.config.root.is_none()
                    || self.charge_total().saturating_add(requirement.charge) <= self.config.nvme)
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
                StoreEvent::Room { connection, bytes } => {
                    let keys: Vec<(u64, NamespaceId)> = self
                        .subscriptions
                        .keys()
                        .filter(|(id, _)| *id == connection)
                        .copied()
                        .collect();
                    for key in keys {
                        if self.buffer_or_send(key, LiveEvent::Room { bytes }) {
                            self.restart_snapshot(key);
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
            LiveEvent::Room { .. } => None,
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
                                event: Some(proto::catalog_event::Event::Entry(
                                    crate::control::entry_to_proto(&entry),
                                )),
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
            ("forwarded_total", self.metrics.forwarded),
            ("forward_failed_total", self.metrics.forward_failed),
            ("forward_retries_total", self.metrics.forward_retries),
            ("remote_fetched_total", self.metrics.remote_fetched),
        ] {
            text.push_str(&format!("cm_{name} {value}\n"));
        }
        text.push_str(&format!(
            "cm_nvme_seconds_total {}\n",
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
            text.push_str(&format!("cm_puts_total{{outcome=\"{outcome}\"}} {count}\n"));
        }
        for (outcome, count) in [
            ("ok", self.metrics.gets_ok),
            ("error", self.metrics.gets_error),
        ] {
            text.push_str(&format!("cm_gets_total{{outcome=\"{outcome}\"}} {count}\n"));
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
                text.push_str(&format!("cm_records{{namespace=\"{name}\",tier=\"{tier}\"}} {count}\ncm_bytes{{namespace=\"{name}\",tier=\"{tier}\"}} {bytes}\n"));
                let hits = self
                    .metrics
                    .hits
                    .get(&(name.clone(), tier))
                    .copied()
                    .unwrap_or(0);
                text.push_str(&format!(
                    "cm_hits_total{{namespace=\"{name}\",tier=\"{tier}\"}} {hits}\n"
                ));
            }
        }
        text
    }
}

pub type Reply<T> = tokio::sync::oneshot::Sender<T>;

pub enum Transport {
    Stream,
    Rdma,
}

pub struct PeerRegions {
    pub arena_bytes: u64,
    pub staging_bytes: u64,
    pub rdma: Vec<u8>,
}

pub struct PeerSession {
    pub generation: u64,
    pub transport: Transport,
    pub stream_addr: String,
    pub token: Vec<u8>,
    pub regions: PeerRegions,
}

pub enum PeerEvent {
    Connecting,
    Connected {
        node: String,
        hello: PeerSession,
    },
    Catalog {
        node: String,
        namespace: NamespaceId,
        event: proto::CatalogEvent,
    },
    Disconnected {
        node: String,
    },
    Forwarded {
        namespace: NamespaceId,
        key: Key,
        result: Result<()>,
    },
    /// One forward attempt failed on the transport; the forwarder will retry.
    ForwardFailed {
        namespace: NamespaceId,
        key: Key,
    },
    Data {
        node: String,
        transfer: u64,
        result: Result<()>,
    },
}

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

/// A fetch reservation: its token and one arena destination per fetched record.
pub type FetchReservation = (u64, Vec<Arc<Extent>>);

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
    PeerHello {
        connection: u64,
        node: String,
        reply: Reply<Result<u64>>,
    },
    /// Which records of a chain only exist upstream, including every destination shape.
    RemotePlan {
        namespace: NamespaceId,
        chain: Vec<Key>,
        reply: Reply<Result<Option<RemotePlan>>>,
    },
    /// Lease a record so the peer runtime can forward it (used to repair a missing ancestor).
    PrepareForward {
        namespace: NamespaceId,
        key: Key,
        reply: Reply<Result<ForwardJob>>,
    },
    /// Allocate the DRAM destinations for a remote fetch, making room by eviction, and pin the
    /// chain until the serving Get plans its own pins. `None` means the chain cannot fit the
    /// arena, so the fetch is a miss.
    ReserveFetch {
        namespace: NamespaceId,
        chain: Vec<Key>,
        lengths: Vec<u64>,
        connection: u64,
        reply: Reply<Result<Option<FetchReservation>>>,
    },
    /// Release a fetch reservation whose Get will not run (a failed transfer or adoption).
    ReleaseFetch {
        token: u64,
    },
    /// Release the windows of one transfer once the party that held them has drained.
    ReleaseTransfer {
        connection: u64,
        generation: u64,
        transfer: u64,
    },
    /// Install bytes fetched from an upstream as a local DRAM record. `row` records whether the
    /// fetch delivered the endpoint row or only the pages.
    AdoptRemote {
        namespace: NamespaceId,
        key: Key,
        extent: Arc<Extent>,
        row: bool,
        reply: Reply<Result<()>>,
    },
    Peer(PeerEvent),
    Admin(AdminCommand),
    Tick,
    Shutdown,
}

/// Administrative operations, executed on the store thread.
pub enum AdminCommand {
    Stats(Reply<Result<String>>),
    Pin {
        namespace: Vec<u8>,
        keys: Vec<Key>,
        pin: bool,
        reply: Reply<Result<u32>>,
    },
    Purge {
        namespace: Vec<u8>,
        all_namespaces: bool,
        reply: Reply<Result<(u64, u64)>>,
    },
    SetQuota {
        namespace: Vec<u8>,
        dram: u64,
        nvme: u64,
        reply: Reply<Result<u64>>,
    },
}

fn live_event(live: &LiveEvent, sequence: u64) -> Option<proto::CatalogEvent> {
    match live {
        LiveEvent::Catalog {
            kind, key, entry, ..
        } => {
            let event = if *kind == 2 {
                proto::catalog_event::Event::Removed(crate::control::key_to_proto(key))
            } else {
                let entry = entry.as_ref()?;
                let proto = crate::control::entry_to_proto(entry);
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
        LiveEvent::Room { bytes } => Some(proto::CatalogEvent {
            sequence,
            event: Some(proto::catalog_event::Event::Room(proto::Room {
                bytes: *bytes,
            })),
        }),
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
            StoreCommand::PeerHello {
                connection,
                node,
                reply,
            } => {
                let _ = reply.send(self.peer_hello(connection, &node));
            }
            StoreCommand::Peer(event) => match event {
                PeerEvent::Forwarded {
                    namespace,
                    key,
                    result,
                } => {
                    if result.is_ok() {
                        self.metrics.forwarded += 1;
                    } else {
                        self.metrics.forward_failed += 1;
                    }
                    self.forward_finished(namespace, key);
                }
                PeerEvent::ForwardFailed { namespace, key } => {
                    let _ = (namespace, key);
                    self.metrics.forward_retries += 1;
                }
                PeerEvent::Catalog {
                    node,
                    namespace,
                    event,
                } => self.apply_catalog(&node, namespace, event),
                PeerEvent::Disconnected { node } => self.peer_gone(&node),
                PeerEvent::Connecting | PeerEvent::Connected { .. } | PeerEvent::Data { .. } => {}
            },
            StoreCommand::RemotePlan {
                namespace,
                chain,
                reply,
            } => {
                let all = self.chain_records(namespace, &chain);
                let root = all.first().map(|record| record.key);
                let complete = all.len() == chain.len();
                // A record is needed when it has no local bytes, or when it is the chain
                // endpoint and its local copy lacks the row. The row flag of a record with
                // local bytes is its own; a merged upstream flag never hides a local row.
                let needed =
                    |record: &ChainRecord| !record.local || (record.endpoint && !record.row);
                // One Get from the first missing record to the chain endpoint. Local records in
                // between ride along; that costs a few bytes and keeps every request a valid
                // chain ending at a row-bearing endpoint.
                let first = all.iter().position(needed);
                let records = first.map(|start| all[start..].to_vec());
                let result = if complete && let Some(records) = records {
                    match self.namespaces.get(&namespace) {
                        Some(state) => Ok(Some(RemotePlan {
                            descriptor: state.descriptor.clone(),
                            root: root.unwrap(),
                            records,
                        })),
                        None => Err(invalid("Get namespace is not registered")),
                    }
                } else {
                    Ok(None)
                };
                let _ = reply.send(result);
            }
            StoreCommand::PrepareForward {
                namespace,
                key,
                reply,
            } => {
                let result = self.prepare_forward(namespace, key);
                let _ = reply.send(result);
            }
            StoreCommand::ReserveFetch {
                namespace,
                chain,
                lengths,
                connection,
                reply,
            } => {
                let result = self.reserve_fetch(namespace, &chain, &lengths, connection);
                let _ = reply.send(result);
            }
            StoreCommand::ReleaseFetch { token } => self.release_fetch(token),
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
            StoreCommand::AdoptRemote {
                namespace,
                key,
                extent,
                row,
                reply,
            } => {
                let result = self.adopt_remote(namespace, key, extent, row);
                let _ = reply.send(result);
            }
            StoreCommand::Admin(command) => self.admin(command),
            StoreCommand::Tick => {}
            StoreCommand::Shutdown => {}
        }
    }
}

fn validate_commit(put: &mut Put, descriptor: &Descriptor) -> Result<()> {
    let header = Header::decode(&put.commit_bytes[..4096])?;
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
        || !header.matches(descriptor)
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
    fn streaming_room_only_needs_a_window() {
        let (mut store, base, directory) = store_with_root("room-window-stream", 4096);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        // A record larger than staging still streams: the requirement
        // charges the disk record plus a fixed hold of two of its largest
        // windows, and Room only needs that hold to fit the staging region.
        let key = |tokens| Key {
            consumed_tokens: tokens,
            digest: [0; 16],
        };
        let header = Header::new(&value, key(4096 * 64), None, [0; 16], 4096).unwrap();
        let requirement = store.put_requirement(&header, 0);
        assert!(requirement.streaming);
        assert!(requirement.reservation > 8 * 1024 * 1024);
        assert!(requirement.unit <= 4 * 1024 * 1024);
        store.arm(1, requirement, Vec::new(), None);
        store.room();
        assert!(
            store
                .events
                .iter()
                .any(|event| matches!(event, StoreEvent::Room { connection: 1, .. }))
        );
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn put_requirement_matches_the_shape_the_put_gets() {
        let (store, directory) = store("requirement");
        let value = descriptor();
        let key = |tokens| Key {
            consumed_tokens: tokens,
            digest: [0; 16],
        };
        let one_page = Header::new(&value, key(64), None, [0; 16], 1).unwrap();
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
        let many = Header::new(&value, key(128 * 64), None, [0; 16], 128).unwrap();
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
        let header = Header::new(value, key, parent, [0; 16], tail).unwrap();
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
                    origin: Origin::Local,
                    remote: None,
                    header,
                    memory: Some(Arc::new(extent)),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes,
                },
            )
            .unwrap();
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
    fn room_does_not_offer_an_oversized_unit_it_cannot_fit() {
        let (mut store, base, directory) = store_with_root("room-unit", 4096);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        store.arm(
            1,
            PutRequirement {
                streaming: true,
                charge: crate::disk::ENVELOPE_BYTES + 16 * 1024 * 1024,
                reservation: crate::disk::ENVELOPE_BYTES,
                allocation: 16 * 1024 * 1024,
                unit: 16 * 1024 * 1024,
            },
            Vec::new(),
            None,
        );
        store.room();
        assert!(
            !store
                .events
                .iter()
                .any(|event| matches!(event, StoreEvent::Room { .. })),
            "a 16 MiB unit cannot fit the 8 MiB staging region"
        );
        store.local.take_armed(1);
        store.armed.remove(&1);
        store.arm(
            1,
            PutRequirement {
                streaming: true,
                charge: crate::disk::ENVELOPE_BYTES + 4 * 1024 * 1024,
                reservation: crate::disk::ENVELOPE_BYTES,
                allocation: 4 * 1024 * 1024,
                unit: 4 * 1024 * 1024,
            },
            Vec::new(),
            None,
        );
        store.room();
        assert!(
            store
                .events
                .iter()
                .any(|event| matches!(event, StoreEvent::Room { .. }))
        );
        drop(store);
        let _ = fs::remove_dir_all(base);
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
    fn arm_replaces_the_requirement_with_the_latest_refusal() {
        let (mut store, directory) = store("arm-latest");
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let large = PutRequirement::streaming(1024, 4096);
        let small = PutRequirement::arena(8192);
        store.arm(1, large, Vec::new(), None);
        store.arm(1, small, Vec::new(), None);
        assert_eq!(store.armed[&1].requirement, small);
        assert_eq!(store.local.armed_budget(1), Some(small.charge));
        assert_eq!(store.local.take_armed(1), Some(small.charge));
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn room_admits_an_arena_sized_reservation() {
        let (mut store, directory) = store("room-size");
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        let capacity = store.memory.capacity() as u64;
        store.arm(
            1,
            PutRequirement {
                streaming: false,
                charge: crate::disk::ENVELOPE_BYTES + capacity,
                reservation: crate::disk::ENVELOPE_BYTES + capacity,
                allocation: capacity,
                unit: capacity,
            },
            Vec::new(),
            None,
        );
        store.room();
        assert!(
            store
                .events
                .iter()
                .any(|event| matches!(event, StoreEvent::Room { connection: 1, .. }))
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn window_budget_arms_room() {
        let (mut store, base, directory) = store_with_root("room-window", 4096);
        let value = descriptor();
        store
            .hello(1, "l0".into(), std::process::id(), value)
            .unwrap();
        // Hold all of staging so the fixed hold cannot be reserved.
        let hold = store.staging.allocate(8 * 1024 * 1024).unwrap();
        let (events, mut receiver) = tokio::sync::mpsc::channel(4);
        store.submit_put(
            1,
            1,
            PutRequest {
                namespace: store.namespace_id().unwrap(),
                key: Key {
                    consumed_tokens: 128,
                    digest: [6; 16],
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
        assert!(!store.sessions[&1].ops.contains_key(&1));
        assert!(matches!(
            receiver.try_recv(),
            Ok(Ok(PutAnswer::Refused(refused)))
                if refused.reason == proto::Reason::Budget as u8
        ));
        let requirement = store.armed.get(&1).unwrap().requirement;
        assert!(requirement.streaming);
        assert_eq!(store.local.armed_budget(1), Some(requirement.charge));
        drop(hold);
        store.events.clear();
        store.room();
        assert!(
            store
                .events
                .iter()
                .any(|event| matches!(event, StoreEvent::Room { connection: 1, .. }))
        );
        assert!(!store.armed.contains_key(&1));
        drop(store);
        let _ = fs::remove_dir_all(base);
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
        config.write_budget = 1024 * 1024;
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
        assert!(store.budget_balance(1).unwrap() >= 0);
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
        assert!(store.budget_balance(1).unwrap() >= 0);
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(PathBuf::from("/dev/shm/selene").join(name));
    }

    fn insert_node(
        store: &mut Store,
        id: NamespaceId,
        value: &Descriptor,
        key: Key,
        parent: Option<Key>,
        tail: u32,
    ) {
        let header = Header::new(value, key, parent, [0; 16], tail).unwrap();
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
                    origin: Origin::Local,
                    remote: None,
                    header,
                    memory: None,
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
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
        let root_header = Header::new(&value, root_key, None, [0; 16], 1).unwrap();
        let child_header = Header::new(&value, child_key, Some(root_key), [0; 16], 1).unwrap();
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
                        origin: Origin::Local,
                        remote: None,
                        header: header.clone(),
                        memory: Some(extent),
                        disk: None,
                        durable: false,
                        writing: false,
                        job: None,
                        rewrite: false,
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
            let header = Header::new(&value, key, parent, [0; 16], 1).unwrap();
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
                        origin: Origin::Local,
                        remote: None,
                        header,
                        memory: None,
                        disk: None,
                        durable: false,
                        writing: false,
                        job: None,
                        rewrite: false,
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
    fn forward_source_leases_the_stored_bytes() {
        let (mut store, directory) = store("forward");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [9; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        let extent = Arc::new(
            store
                .memory
                .allocate(header.length_with_row() as usize)
                .unwrap(),
        );
        assert_eq!(extent.references(), 1);
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
                    origin: Origin::Local,
                    remote: None,
                    header: header.clone(),
                    memory: Some(extent.clone()),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: record_charge(&header, true),
                },
            )
            .unwrap();
        let lease = store.forward_source(id, key).unwrap();
        assert_eq!(lease.header, header);
        assert_eq!(extent.references(), 2);
        drop(lease);
        assert_eq!(extent.references(), 1);
        assert!(
            store
                .forward_source(
                    id,
                    Key {
                        consumed_tokens: 1,
                        digest: [1; 16],
                    },
                )
                .is_err()
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn admin_pins_protect_a_record_from_purge() {
        let (mut store, directory) = store("admin");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [9; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        let extent = Arc::new(
            store
                .memory
                .allocate(header.length_with_row() as usize)
                .unwrap(),
        );
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
                    origin: Origin::Local,
                    remote: None,
                    header: header.clone(),
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: record_charge(&header, true),
                },
            )
            .unwrap();
        let pin = |store: &mut Store, pin: bool| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::Admin(AdminCommand::Pin {
                namespace: id.to_vec(),
                keys: vec![key],
                pin,
                reply,
            }));
            answer.blocking_recv().unwrap().unwrap()
        };
        let purge = |store: &mut Store| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::Admin(AdminCommand::Purge {
                namespace: id.to_vec(),
                all_namespaces: false,
                reply,
            }));
            answer.blocking_recv().unwrap().unwrap()
        };
        assert_eq!(pin(&mut store, true), 1);
        // Repeated admin pins are idempotent.
        assert_eq!(pin(&mut store, true), 0);
        // A purge removes every record, pinned or not.
        assert_eq!(purge(&mut store), (1, record_charge(&header, true)));
        assert!(store.namespaces[&id].tree.node(&key).is_none());
        assert_eq!(pin(&mut store, false), 0);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_remote_child_waits_for_its_parent() {
        let (mut store, directory) = store("deferred");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let parent = Key {
            consumed_tokens: 128,
            digest: [1; 16],
        };
        let child = Key {
            consumed_tokens: 192,
            digest: [2; 16],
        };
        let entry = |key, parent: Option<Key>, tail: u64| proto::CatalogEntry {
            key: Some(crate::control::key_to_proto(&key)),
            parent: parent.map(|parent| crate::control::key_to_proto(&parent)),
            has_parent: parent.is_some(),
            bucket: vec![0; 16],
            tail_pages: tail,
            row: true,
            tier: proto::Tier::Nvme as i32,
            durable: false,
            bytes: 8192,
            unreadable: false,
        };
        let send = |store: &mut Store, sequence, body| {
            store.handle(StoreCommand::Peer(PeerEvent::Catalog {
                node: "upstream".into(),
                namespace: id,
                event: proto::CatalogEvent {
                    sequence,
                    event: Some(body),
                },
            }));
        };
        use proto::catalog_event::Event;
        // The child arrives before the parent and is held back.
        send(&mut store, 1, Event::Added(entry(child, Some(parent), 1)));
        assert!(store.namespaces[&id].tree.node(&child).is_none());
        send(&mut store, 2, Event::Added(entry(parent, None, 2)));
        assert!(store.namespaces[&id].tree.node(&parent).is_some());
        let node = store.namespaces[&id].tree.node(&child).unwrap();
        assert_eq!(node.location.remote.as_ref().unwrap().node, "upstream");
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_pending_reservation_counts_against_a_quota() {
        let (mut store, directory) = store("quota-pending");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [7; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        let used = 4096;
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&key)
            .unwrap()
            .location
            .bytes = used;
        store.retired_puts.push(PutDrain {
            connection: 1,
            transfer: 1,
            namespace: id,
            key,
            pins: VecDeque::new(),
            pending: 0,
            reservation: used,
            _temporary: None,
            _staging_hold: None,
        });
        store.quotas.insert(
            id,
            Quota {
                dram: 0,
                nvme: used,
            },
        );
        assert!(!store.quota_allows(id, 1, 1));
        store.quotas.insert(id, Quota { dram: 0, nvme: 1 });
        store.enforce_quota(id);
        assert_eq!(store.namespace_charge(id, false), 0);
        assert!(store.quota_excess(id) > 0);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_detached_publication_from_a_purged_epoch_is_dropped() {
        let (mut store, directory) = store("stale-publish");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [2; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let epoch = store.namespaces[&id].epoch;
        let put = Put {
            namespace: id,
            epoch,
            remote: false,
            header,
            first_page: 0,
            grants: Vec::new(),
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages: vec![None; 1],
                row: None,
                pages_filled: 0,
                row_filled: true,
            },
            memory: None,
            temporary: None,
            streaming: false,
            commit_bytes: Vec::new(),
            metadata: None,
            committed: true,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: true,
            pending: 0,
            blocked: VecDeque::new(),
            pins: VecDeque::new(),
            reservation: 4096,
            requirement: PutRequirement::arena(0),
            staging_hold: None,
            hold_unit: 0,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        store.charged = 4096;
        store.detached_puts.insert((1, 1), put);
        // A purge fenced the namespace after the put was detached.
        store.namespaces.get_mut(&id).unwrap().epoch += 1;
        let disk = Arc::new(DiskExtent::new(directory.join("stale.record")));
        store.publish_detached_put(1, 1, disk);
        assert!(store.namespaces[&id].tree.node(&key).is_none());
        assert_eq!(store.charged, 0);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_put_naming_an_absent_parent_asks_for_ancestry_repair() {
        let (mut store, directory) = store("parent");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store.peer_hello(1, "near").unwrap();
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
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
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
                    origin: Origin::Local,
                    remote: None,
                    header: header.clone(),
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
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
            matches!(store.publish_target(id, &header), PublishTarget::Replace(_)),
            "a page-only record must be replaced, not treated as present"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    fn store_with_delayed_disk(label: &str) -> (Store, PathBuf, PathBuf) {
        let name = format!(
            "selene-unit-{}-{label}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let base = std::env::temp_dir().join(&name);
        let mut config = Config::new(
            base.join("records"),
            base.join("manager.sock"),
            1 << 20,
            64 << 20,
        );
        config.name = name.clone();
        config.staging = 8 * 1024 * 1024;
        config.faults.delay_ms = 300;
        let store = Store::open(config).unwrap();
        let directory = PathBuf::from("/dev/shm/selene").join(name);
        (store, base, directory)
    }

    /// Encode a record header the way a client does, for building a fetch result.
    fn encoded_header(header: &Header) -> Vec<u8> {
        use crate::codec::Encoder;
        let mut encoder = Encoder::default();
        encoder.bytes(b"FLARECKP");
        encoder.u32(1);
        encoder.u32(header.flags);
        encoder.bytes(&header.identity);
        encoder.bytes(&header.fingerprint);
        encoder.key(header.key);
        encoder.u8(header.parent.is_some() as u8);
        encoder.key(header.parent.unwrap_or_default());
        encoder.bytes(&header.bucket);
        encoder.u64(header.page_tokens);
        encoder.u32(header.tail_pages);
        encoder.u64(header.page_segment);
        encoder.u64(header.row_segment);
        encoder.u64(header.row_checksum);
        encoder.u64(header.created);
        record::encode_ranks(&mut encoder, &header.ranks);
        let mut bytes = vec![0; record::HEADER_BYTES];
        bytes[..encoder.0.len()].copy_from_slice(&encoder.0);
        bytes
    }

    /// A fetched extent holding a valid, zero-payload record.
    fn fetched_extent(
        store: &mut Store,
        value: &Descriptor,
        key: Key,
        parent: Option<Key>,
        tail: u32,
    ) -> Arc<Extent> {
        let header = Header::new(value, key, parent, [0; 16], tail).unwrap();
        let mut extent = store
            .memory
            .allocate(header.length_with_row() as usize)
            .unwrap();
        extent.bytes_mut()[..record::HEADER_BYTES].copy_from_slice(&encoded_header(&header));
        Arc::new(extent)
    }

    /// Turn an indexed node into a catalog-only remote entry.
    fn make_remote(store: &mut Store, id: NamespaceId, key: Key, row: bool) {
        let node = store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&key)
            .unwrap();
        node.location.origin = Origin::Peer;
        node.location.remote = Some(RemoteSource {
            node: "upstream".into(),
            bytes: 8192,
            row,
        });
        node.row = row;
    }

    #[test]
    fn a_purge_waits_for_an_outstanding_publication_and_retires_it() {
        let (mut store, base, directory) = store_with_delayed_disk("purge-inflight");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [32; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        let metadata = vec![0; header.page_segment as usize];
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let epoch = store.namespaces[&id].epoch;
        let temporary = store.namespaces[&id].temporary(1).unwrap();
        let put = Put {
            namespace: id,
            epoch,
            remote: false,
            header,
            first_page: 0,
            grants: Vec::new(),
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages: vec![None; 1],
                row: None,
                pages_filled: 0,
                row_filled: true,
            },
            memory: None,
            temporary: Some(temporary),
            streaming: true,
            commit_bytes: Vec::new(),
            metadata: Some(Arc::new(metadata)),
            committed: true,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: false,
            pending: 0,
            blocked: VecDeque::new(),
            pins: VecDeque::new(),
            reservation: 0,
            requirement: PutRequirement::arena(0),
            staging_hold: None,
            hold_unit: 0,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        store.detached_puts.insert((2, 2), put);
        store.pump();
        assert!(
            store
                .detached_puts
                .get(&(2, 2))
                .is_some_and(|put| put.pending > 0),
            "the Finish job is outstanding"
        );
        store.purge_namespace(id);
        assert!(
            store.namespaces[&id].purging,
            "a purge keeps the namespace closed while its publication drains"
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.namespaces[&id].purging {
            assert!(Instant::now() < deadline, "the publication never drained");
            store.pump();
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            store.detached_puts.is_empty(),
            "the drained publication is released"
        );
        let path = store.namespaces[&id].path(key).unwrap();
        assert!(
            !path.exists(),
            "the file renamed before the epoch check was retired"
        );
        let root = base.join("records");
        drop(store);
        let recovered = crate::disk::recover(&root).unwrap();
        assert!(
            !recovered
                .iter()
                .flat_map(|namespace| namespace.records.iter())
                .any(|(header, _, _)| header.key == key),
            "a restart must not recover a purged publication"
        );
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_remote_plan_runs_to_the_endpoint_through_local_records() {
        let (mut store, directory) = store("plan-runs");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let first = Key {
            consumed_tokens: 64,
            digest: [20; 16],
        };
        let middle = Key {
            consumed_tokens: 128,
            digest: [21; 16],
        };
        let last = Key {
            consumed_tokens: 192,
            digest: [22; 16],
        };
        insert_node(&mut store, id, &value, first, None, 1);
        insert_node(&mut store, id, &value, middle, Some(first), 1);
        insert_node(&mut store, id, &value, last, Some(middle), 1);
        // A is missing and page-only upstream, B is local, C is the remote endpoint.
        make_remote(&mut store, id, first, false);
        make_remote(&mut store, id, last, true);
        let header = store.namespaces[&id].tree[&middle].location.header.clone();
        let extent = Arc::new(store.memory.allocate(header.length(true) as usize).unwrap());
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&middle)
            .unwrap()
            .location
            .memory = Some(extent);
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::RemotePlan {
            namespace: id,
            chain: vec![first, middle, last],
            reply,
        });
        store.pump();
        let plan = answer
            .try_recv()
            .expect("the store answered the plan")
            .expect("a plan result")
            .expect("a plan");
        let keys: Vec<Key> = plan.records.iter().map(|record| record.key).collect();
        assert_eq!(
            keys,
            vec![first, middle, last],
            "one Get runs from the first miss to the endpoint, through local B"
        );
        assert!(
            plan.records[2].endpoint,
            "the Get ends at the remote endpoint"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_remote_fetch_reserves_room_by_evicting_a_durable_leaf() {
        let (mut store, base, directory) = store_with_root("fetch-reserve", 1 << 20);
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let old = Key {
            consumed_tokens: 64,
            digest: [40; 16],
        };
        insert_node(&mut store, id, &value, old, None, 1);
        let extent = Arc::new(store.memory.allocate(1 << 20).unwrap());
        {
            let node = store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&old)
                .unwrap();
            node.location.memory = Some(extent);
            node.location.durable = true;
            node.location.bytes = 1 << 20;
        }
        let fresh = Key {
            consumed_tokens: 64,
            digest: [41; 16],
        };
        insert_node(&mut store, id, &value, fresh, None, 1);
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::ReserveFetch {
            namespace: id,
            chain: vec![fresh],
            lengths: vec![64 * 1024],
            connection: 1,
            reply,
        });
        let (token, extents) = answer
            .try_recv()
            .expect("the store answered the reservation")
            .expect("a reservation result")
            .expect("a fetch fits once the durable leaf is evicted");
        assert_eq!(extents.len(), 1);
        assert!(store.namespaces[&id].tree.node(&old).is_none());
        assert!(extents[0].bytes().len() >= 64 * 1024);
        assert!(store.namespaces[&id].tree[&fresh].pinned());
        store.handle(StoreCommand::ReleaseFetch { token });
        assert!(!store.namespaces[&id].tree[&fresh].pinned());
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_remote_fetch_larger_than_the_arena_is_a_miss() {
        let (mut store, directory) = store("fetch-miss");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [42; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::ReserveFetch {
            namespace: id,
            chain: vec![key],
            lengths: vec![2 << 20],
            connection: 1,
            reply,
        });
        let reserved = answer
            .try_recv()
            .expect("the store answered the reservation")
            .expect("a reservation result");
        assert!(reserved.is_none(), "a chain beyond the arena is a miss");
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_fetch_reservation_releases_only_its_own_pins() {
        let (mut store, directory) = store("fetch-two-pins");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let first = Key {
            consumed_tokens: 64,
            digest: [44; 16],
        };
        let second = Key {
            consumed_tokens: 64,
            digest: [45; 16],
        };
        insert_node(&mut store, id, &value, first, None, 1);
        insert_node(&mut store, id, &value, second, None, 1);
        let reserve = |store: &mut Store, key: Key| {
            let (reply, mut answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::ReserveFetch {
                namespace: id,
                chain: vec![key],
                lengths: vec![64 * 1024],
                connection: 1,
                reply,
            });
            answer
                .try_recv()
                .unwrap()
                .unwrap()
                .expect("the reservation fits")
                .0
        };
        let token_a = reserve(&mut store, first);
        let token_b = reserve(&mut store, second);
        assert!(store.namespaces[&id].tree[&first].pinned());
        assert!(store.namespaces[&id].tree[&second].pinned());
        store.handle(StoreCommand::ReleaseFetch { token: token_a });
        assert!(
            !store.namespaces[&id].tree[&first].pinned(),
            "the Get that owned A released exactly A"
        );
        assert!(
            store.namespaces[&id].tree[&second].pinned(),
            "a concurrent fetch keeps its protection"
        );
        assert!(store.fetch_pins.contains_key(&token_b));
        store.handle(StoreCommand::ReleaseFetch { token: token_b });
        assert!(!store.namespaces[&id].tree[&second].pinned());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_serving_get_does_not_release_a_fetch_reservation() {
        let (mut store, directory) = store("fetch-get-pins");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [46; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        let header = store.namespaces[&id].tree[&key].location.header.clone();
        let length = header.length(true);
        let extent = Arc::new(store.memory.allocate(length as usize).unwrap());
        {
            let node = store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&key)
                .unwrap();
            node.location.memory = Some(extent);
            node.location.bytes = length;
        }
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::ReserveFetch {
            namespace: id,
            chain: vec![key],
            lengths: vec![length],
            connection: 1,
            reply,
        });
        let (token, _) = answer
            .try_recv()
            .unwrap()
            .unwrap()
            .expect("the reservation fits");
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        store.submit_get(
            1,
            1,
            GetRequest {
                namespace: id,
                keys: vec![key],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
            },
            events,
        );
        store.pump();
        assert!(
            store.fetch_pins.contains_key(&token),
            "the Get leaves the fetch's own reservation alone"
        );
        assert!(store.namespaces[&id].tree[&key].pinned());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn an_anchor_kept_by_a_lease_is_pruned_when_the_lease_ends() {
        let (mut store, directory) = store("remote-orphan");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [48; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        make_remote(&mut store, id, key, true);
        store.remote_views.insert(
            (id, "upstream".to_string()),
            std::collections::BTreeMap::new(),
        );
        // A serving Get pins the anchor while its upstream disappears.
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .pin(&key)
            .unwrap();
        store.peer_gone("upstream");
        assert!(
            store.namespaces[&id].tree.node(&key).is_some(),
            "a leased anchor is not removed under the lease"
        );
        assert_eq!(store.remote_orphans.len(), 1, "the anchor is queued");
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .unpin(&key)
            .unwrap();
        store.cleanup_turn();
        assert!(
            store.namespaces[&id].tree.node(&key).is_none(),
            "the anchor is gone once its lease ends"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn pruning_a_dead_anchor_leaves_a_parent_with_bytes_to_eviction() {
        let (mut store, directory) = store("orphan-parent-bytes");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let parent = Key {
            consumed_tokens: 64,
            digest: [49; 16],
        };
        let child = Key {
            consumed_tokens: 128,
            digest: [50; 16],
        };
        insert_node(&mut store, id, &value, parent, None, 1);
        insert_node(&mut store, id, &value, child, Some(parent), 1);
        // The parent carries pages but no row: the tree would cascade it with the child, but
        // its local bytes belong to normal eviction and its charge must survive this prune.
        let header = store.namespaces[&id].tree[&parent].location.header.clone();
        let bytes = header.length(false);
        let extent = Arc::new(store.memory.allocate(bytes as usize).unwrap());
        {
            let node = store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&parent)
                .unwrap();
            node.row = false;
            node.location.memory = Some(extent);
            node.location.bytes = bytes;
        }
        store.charged = bytes;
        make_remote(&mut store, id, child, true);
        store.remote_views.insert(
            (id, "upstream".to_string()),
            std::collections::BTreeMap::new(),
        );
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .pin(&child)
            .unwrap();
        store.peer_gone("upstream");
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .unpin(&child)
            .unwrap();
        store.cleanup_turn();
        assert!(store.namespaces[&id].tree.node(&child).is_none());
        assert!(
            store.namespaces[&id].tree.node(&parent).is_some(),
            "a parent with local bytes is left to eviction"
        );
        assert_eq!(store.charged, bytes, "its charge is untouched");
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn pruning_a_dead_anchor_removes_a_dead_ancestor() {
        let (mut store, directory) = store("orphan-parent-empty");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let parent = Key {
            consumed_tokens: 64,
            digest: [52; 16],
        };
        let child = Key {
            consumed_tokens: 128,
            digest: [53; 16],
        };
        insert_node(&mut store, id, &value, parent, None, 1);
        insert_node(&mut store, id, &value, child, Some(parent), 1);
        // The parent is remote-only too: at disconnect it still has a child, so it is not
        // removable, and it becomes a dead ancestor only once the anchor goes.
        make_remote(&mut store, id, parent, true);
        make_remote(&mut store, id, child, true);
        store.remote_views.insert(
            (id, "upstream".to_string()),
            std::collections::BTreeMap::new(),
        );
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .pin(&child)
            .unwrap();
        store.peer_gone("upstream");
        assert!(
            store.namespaces[&id].tree.node(&parent).is_some(),
            "the parent still has its child"
        );
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .unpin(&child)
            .unwrap();
        store.cleanup_turn();
        assert!(store.namespaces[&id].tree.node(&child).is_none());
        assert!(
            store.namespaces[&id].tree.node(&parent).is_none(),
            "the now-childless dead ancestor goes with it"
        );
        assert!(store.remote_orphans.is_empty());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn orphan_entries_are_deduplicated_and_dropped_when_the_node_is_gone() {
        let (mut store, directory) = store("orphan-dedupe");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [51; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        make_remote(&mut store, id, key, true);
        store.remote_views.insert(
            (id, "upstream".to_string()),
            std::collections::BTreeMap::new(),
        );
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .pin(&key)
            .unwrap();
        store.peer_gone("upstream");
        store.peer_gone("upstream");
        assert_eq!(store.remote_orphans.len(), 1, "one entry per record");
        // The node goes away by another path: the entry must not outlive it.
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .unpin(&key)
            .unwrap();
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .remove_node(&key)
            .unwrap();
        store.cleanup_turn();
        assert!(store.remote_orphans.is_empty());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_fetch_reservation_needs_an_open_connection() {
        let (mut store, directory) = store("fetch-closed");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [47; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::ReserveFetch {
            namespace: id,
            chain: vec![key],
            lengths: vec![64 * 1024],
            connection: 9,
            reply,
        });
        let reserved = answer.try_recv().unwrap().unwrap();
        assert!(
            reserved.is_none(),
            "a fetch processed after its connection closed reserves nothing"
        );
        assert!(!store.namespaces[&id].tree[&key].pinned());
        assert!(store.fetch_pins.is_empty());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_local_page_only_row_is_advertised_from_the_upstream() {
        let (mut store, directory) = store("advertised-row");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [28; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        let path = store.namespaces[&id].path(key);
        // A compacted local record: pages on disk, no row, with an upstream row.
        let extent = path.map(|path| Arc::new(DiskExtent::new(path)));
        {
            let node = store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&key)
                .unwrap();
            node.row = false;
            node.location.origin = Origin::Peer;
            node.location.disk = extent;
            node.location.remote = Some(RemoteSource {
                node: "upstream".into(),
                bytes: 8192,
                row: true,
            });
        }
        let advertised = Store::entry(&store.namespaces[&id], key);
        assert!(
            advertised.row,
            "subscribers must see the upstream row on a local page-only record"
        );
        let record = store.chain_records(id, &[key]).remove(0);
        assert!(
            !record.row,
            "the refetch decision still uses the local row fact"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn adoption_is_fenced_while_a_disk_job_runs() {
        let (mut store, base, directory) = store_with_root("adopt-fence", 1 << 20);
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 128,
            digest: [29; 16],
        };
        insert_node(&mut store, id, &value, key, None, 2);
        make_remote(&mut store, id, key, true);
        let path = store.namespaces[&id].path(key).unwrap();
        {
            let node = store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&key)
                .unwrap();
            node.row = false;
            node.location.disk = Some(Arc::new(DiskExtent::new(path)));
            node.location.writing = true;
        }
        let extent = fetched_extent(&mut store, &value, key, None, 2);
        let error = store.adopt_remote(id, key, extent, true).unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::WouldBlock,
            "an in-flight disk job fences the adoption"
        );
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&key)
            .unwrap()
            .location
            .writing = false;
        let extent = fetched_extent(&mut store, &value, key, None, 2);
        store.adopt_remote(id, key, extent, true).unwrap();
        assert!(
            store.namespaces[&id].tree[&key].location.memory.is_some(),
            "a quiescent record is adopted"
        );
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_staging_hold_counts_against_the_dram_quota() {
        let (mut store, directory) = store("quota-staging");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [33; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        store.quotas.insert(
            id,
            Quota {
                dram: 4096,
                nvme: 0,
            },
        );
        // A streamed record's disk reservation is not a DRAM charge.
        assert!(
            store.quota_allows(id, store.memory.capacity() as u64 + 1, 0),
            "an oversize record's disk reservation does not consume the DRAM quota"
        );
        let hold = Arc::new(store.staging.allocate(8192).unwrap());
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let epoch = store.namespaces[&id].epoch;
        let put = Put {
            namespace: id,
            epoch,
            remote: false,
            header,
            first_page: 0,
            grants: Vec::new(),
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages: vec![None; 1],
                row: None,
                pages_filled: 0,
                row_filled: true,
            },
            memory: None,
            temporary: None,
            streaming: true,
            commit_bytes: Vec::new(),
            metadata: None,
            committed: false,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: false,
            pending: 0,
            blocked: VecDeque::new(),
            pins: VecDeque::new(),
            reservation: 4096,
            requirement: PutRequirement::streaming(4096, 4096),
            staging_hold: Some(hold),
            hold_unit: 4096,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        store.detached_puts.insert((4, 4), put);
        store.quotas.insert(
            id,
            Quota {
                dram: 4096,
                nvme: 0,
            },
        );
        assert_eq!(
            store.pending_memory(id),
            8192,
            "the held staging region is counted"
        );
        assert!(
            !store.quota_allows(id, 0, 0),
            "a held staging region consumes the DRAM quota"
        );
        assert!(store.quota_excess(id) > 0);
        assert!(
            !store.quota_allows(id, 0, 8192),
            "its staging hold does consume the DRAM quota"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_purge_drops_publications_waiting_for_their_parent() {
        // The delayed disk keeps the parent's write-back in flight, so the child publication
        // stays queued behind parent durability for the duration of the test.
        let (mut store, base, directory) = store_with_delayed_disk("purge-waiting");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        // A parent that is resident but not durable.
        let parent = Key {
            consumed_tokens: 64,
            digest: [30; 16],
        };
        let parent_header = Header::new(&value, parent, None, [0; 16], 1).unwrap();
        let parent_extent = Arc::new(
            store
                .memory
                .allocate(parent_header.length_with_row() as usize)
                .unwrap(),
        );
        let parent_bytes = record_charge(&parent_header, true);
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                parent,
                None,
                [0; 16],
                1,
                true,
                Location {
                    origin: Origin::Local,
                    remote: None,
                    header: parent_header,
                    memory: Some(parent_extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: parent_bytes,
                },
            )
            .unwrap();
        let child = Key {
            consumed_tokens: 128,
            digest: [31; 16],
        };
        let header = Header::new(&value, child, Some(parent), [0; 16], 1).unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let epoch = store.namespaces[&id].epoch;
        let put = Put {
            namespace: id,
            epoch,
            remote: false,
            header,
            first_page: 0,
            grants: Vec::new(),
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages: vec![None; 1],
                row: None,
                pages_filled: 0,
                row_filled: true,
            },
            memory: None,
            temporary: Some(store.namespaces[&id].temporary(1).unwrap()),
            streaming: true,
            commit_bytes: Vec::new(),
            metadata: Some(Arc::new(vec![0; 4096])),
            committed: true,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: false,
            pending: 0,
            blocked: VecDeque::new(),
            pins: VecDeque::new(),
            reservation: 0,
            requirement: PutRequirement::arena(0),
            staging_hold: None,
            hold_unit: 0,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        store.detached_puts.insert((2, 2), put);
        // The publication waits for the parent's durability instead of being renamed.
        store.pump();
        assert!(
            !store
                .jobs
                .values()
                .any(|job| matches!(job, Job::Finish { .. })),
            "the publication waits for its parent instead of being renamed"
        );
        store.purge_namespace(id);
        assert!(
            store.detached_puts.is_empty(),
            "a purge drops the publication waiting for its parent"
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.namespaces[&id].purging {
            assert!(Instant::now() < deadline, "the purge never drained");
            store.pump();
            std::thread::sleep(Duration::from_millis(10));
        }
        // A publication from an older epoch is dropped before any rename as well.
        let header = Header::new(&value, child, None, [0; 16], 2).unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let stale = Put {
            namespace: id,
            epoch: epoch.wrapping_sub(1),
            remote: false,
            header,
            first_page: 0,
            grants: Vec::new(),
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages: vec![None; 2],
                row: None,
                pages_filled: 0,
                row_filled: true,
            },
            memory: None,
            temporary: Some(store.namespaces[&id].temporary(2).unwrap()),
            streaming: true,
            commit_bytes: Vec::new(),
            metadata: Some(Arc::new(vec![0; 4096])),
            committed: true,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: false,
            pending: 0,
            blocked: VecDeque::new(),
            pins: VecDeque::new(),
            reservation: 0,
            requirement: PutRequirement::arena(0),
            staging_hold: None,
            hold_unit: 0,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        store.detached_puts.insert((3, 3), stale);
        store.pump();
        assert!(
            store.detached_puts.is_empty(),
            "a stale-epoch publication is dropped before the rename"
        );
        assert!(
            !store
                .jobs
                .values()
                .any(|job| matches!(job, Job::Finish { .. })),
            "nothing was renamed or queued"
        );
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn adoption_makes_room_without_evicting_its_target() {
        let (mut store, base, directory) = store_with_root("adopt-room", 1 << 20);
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let budget = store.config.nvme;
        // A durable leaf that the adoption must evict to fit.
        let leaf = Key {
            consumed_tokens: 64,
            digest: [23; 16],
        };
        let leaf_header = Header::new(&value, leaf, None, [0; 16], 1).unwrap();
        let leaf_path = store.namespaces[&id].path(leaf).unwrap();
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                leaf,
                None,
                [0; 16],
                1,
                true,
                Location {
                    origin: Origin::Local,
                    remote: None,
                    header: leaf_header,
                    memory: None,
                    disk: Some(Arc::new(DiskExtent::new(leaf_path))),
                    durable: true,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: 8 << 20,
                },
            )
            .unwrap();
        store.charged = budget - 1024;
        let target = Key {
            consumed_tokens: 128,
            digest: [24; 16],
        };
        insert_node(&mut store, id, &value, target, None, 2);
        make_remote(&mut store, id, target, true);
        let extent = fetched_extent(&mut store, &value, target, None, 2);
        store.adopt_remote(id, target, extent, true).unwrap();
        assert!(
            store.namespaces[&id].tree.node(&leaf).is_none(),
            "make_room evicted the durable leaf"
        );
        assert!(
            store.namespaces[&id]
                .tree
                .node(&target)
                .unwrap()
                .location
                .memory
                .is_some(),
            "the adopted record is resident"
        );

        // With the only candidate pinned, adoption reports no room instead of panicking.
        let pinned = Key {
            consumed_tokens: 192,
            digest: [25; 16],
        };
        let pinned_header = Header::new(&value, pinned, None, [0; 16], 3).unwrap();
        let pinned_path = store.namespaces[&id].path(pinned).unwrap();
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .insert(
                pinned,
                None,
                [0; 16],
                3,
                true,
                Location {
                    origin: Origin::Local,
                    remote: None,
                    header: pinned_header,
                    memory: None,
                    disk: Some(Arc::new(DiskExtent::new(pinned_path))),
                    durable: true,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: 8 << 20,
                },
            )
            .unwrap();
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .pin(&pinned)
            .unwrap();
        store.charged = budget - 1024;
        let blocked = Key {
            consumed_tokens: 256,
            digest: [26; 16],
        };
        insert_node(&mut store, id, &value, blocked, None, 4);
        make_remote(&mut store, id, blocked, true);
        let extent = fetched_extent(&mut store, &value, blocked, None, 4);
        assert!(
            store.adopt_remote(id, blocked, extent, true).is_err(),
            "a pinned candidate leaves no room for the adoption"
        );
        assert!(
            store.namespaces[&id]
                .tree
                .node(&blocked)
                .unwrap()
                .location
                .memory
                .is_none(),
            "a failed adoption installs nothing"
        );
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_purge_waits_for_the_write_back_before_a_replacement_put() {
        let (mut store, base, directory) = store_with_delayed_disk("purge-writeback");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [27; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
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
                    origin: Origin::Local,
                    remote: None,
                    header,
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes,
                },
            )
            .unwrap();
        store.pump();
        assert!(!store.jobs.is_empty(), "the write-back is still in flight");
        store.purge_namespace(id);
        assert!(
            store.namespaces[&id].purging,
            "a purge waits for the in-flight write-back"
        );
        let mut events = put_begin(&mut store, 1, id, key, None);
        match events.try_recv().unwrap().unwrap() {
            PutAnswer::Refused(refused) => assert_eq!(
                refused.reason,
                proto::Reason::Queue as u8,
                "a replacement Put is queued until the drain ends"
            ),
            _ => panic!("expected a queued refusal"),
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while store.namespaces[&id].purging {
            assert!(Instant::now() < deadline, "the purge never drained");
            store.pump();
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut events = put_begin(&mut store, 1, id, key, None);
        assert!(
            matches!(events.try_recv().unwrap().unwrap(), PutAnswer::Accept(_)),
            "the replacement Put is accepted after the drain"
        );
        drop(store);
        let _ = fs::remove_dir_all(base);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_remote_plan_fetches_only_missing_records() {
        let (mut store, directory) = store("plan-missing");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let first = Key {
            consumed_tokens: 64,
            digest: [1; 16],
        };
        let middle = Key {
            consumed_tokens: 128,
            digest: [2; 16],
        };
        let last = Key {
            consumed_tokens: 192,
            digest: [3; 16],
        };
        insert_node(&mut store, id, &value, first, None, 1);
        insert_node(&mut store, id, &value, middle, Some(first), 1);
        insert_node(&mut store, id, &value, last, Some(middle), 1);
        // The first record is local; the others exist only in the merged catalog.
        let header = store.namespaces[&id].tree[&first].location.header.clone();
        let extent = Arc::new(store.memory.allocate(header.length(true) as usize).unwrap());
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&first)
            .unwrap()
            .location
            .memory = Some(extent);
        for key in [middle, last] {
            let node = store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&key)
                .unwrap();
            node.location.origin = Origin::Peer;
            node.location.remote = Some(RemoteSource {
                node: "upstream".into(),
                bytes: 8192,
                row: true,
            });
        }
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::RemotePlan {
            namespace: id,
            chain: vec![first, middle, last],
            reply,
        });
        store.pump();
        let plan = answer
            .try_recv()
            .expect("the store answered the plan")
            .expect("a plan result")
            .expect("a plan");
        assert_eq!(plan.root, first, "placement uses the chain root");
        let keys: Vec<Key> = plan.records.iter().map(|record| record.key).collect();
        assert_eq!(
            keys,
            vec![middle, last],
            "the suffix starts at the first miss"
        );
        assert!(!plan.records[0].endpoint);
        assert!(plan.records[1].endpoint);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_page_only_endpoint_is_fetched_for_its_row() {
        let (mut store, directory) = store("plan-row");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let first = Key {
            consumed_tokens: 64,
            digest: [4; 16],
        };
        let last = Key {
            consumed_tokens: 128,
            digest: [5; 16],
        };
        insert_node(&mut store, id, &value, first, None, 1);
        insert_node(&mut store, id, &value, last, Some(first), 1);
        for key in [first, last] {
            let header = store.namespaces[&id].tree[&key].location.header.clone();
            let extent = Arc::new(store.memory.allocate(header.length(true) as usize).unwrap());
            store
                .namespaces
                .get_mut(&id)
                .unwrap()
                .tree
                .node_mut(&key)
                .unwrap()
                .location
                .memory = Some(extent);
        }
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&last)
            .unwrap()
            .row = false;
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&last)
            .unwrap()
            .location
            .remote = Some(RemoteSource {
            node: "upstream".into(),
            bytes: 8192,
            row: true,
        });
        let (reply, mut answer) = tokio::sync::oneshot::channel();
        store.handle(StoreCommand::RemotePlan {
            namespace: id,
            chain: vec![first, last],
            reply,
        });
        store.pump();
        let plan = answer
            .try_recv()
            .expect("the store answered the plan")
            .expect("a plan result")
            .expect("a plan");
        let keys: Vec<Key> = plan.records.iter().map(|record| record.key).collect();
        assert_eq!(
            keys,
            vec![last],
            "a page-only endpoint is refetched although its pages are local"
        );
        assert!(plan.records[0].endpoint);
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

    fn put_begin(
        store: &mut Store,
        connection: u64,
        id: NamespaceId,
        key: Key,
        parent: Option<Key>,
    ) -> tokio::sync::mpsc::Receiver<Result<PutAnswer>> {
        let value = store.namespaces[&id].descriptor.clone();
        let (events, receiver) = tokio::sync::mpsc::channel(4);
        let (reply, _answer) = tokio::sync::oneshot::channel();
        let chain_pages = u32::try_from(
            key.consumed_tokens / value.page_tokens
                - parent.map_or(0, |parent| parent.consumed_tokens / value.page_tokens),
        )
        .unwrap();
        store.handle(StoreCommand::PutBegin {
            connection,
            transfer: 1,
            generation: store.sessions[&connection].generation,
            namespace: id,
            descriptor: value,
            key,
            published_parent: parent,
            bucket: [0; 16],
            chain_pages: u32::try_from(key.consumed_tokens / 64).unwrap(),
            published_tail_pages: chain_pages,
            row: true,
            events,
            reply,
        });
        store.pump();
        receiver
    }

    #[test]
    fn a_purge_closes_the_namespace_until_its_disk_drains() {
        let (mut store, directory, _root) = store_with_root("purge-drain", 1 << 20);
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [10; 16],
        };
        insert_node(&mut store, id, &value, key, None, 1);
        let path = store.namespaces[&id].path(key).unwrap();
        store
            .namespaces
            .get_mut(&id)
            .unwrap()
            .tree
            .node_mut(&key)
            .unwrap()
            .location
            .disk = Some(Arc::new(DiskExtent::new(path)));
        store.purge_namespace(id);
        assert!(
            store.namespaces[&id].purging,
            "a purge with disk extents must close the namespace"
        );
        let next = Key {
            consumed_tokens: 128,
            digest: [11; 16],
        };
        let mut events = put_begin(&mut store, 1, id, next, None);
        match events.try_recv().unwrap().unwrap() {
            PutAnswer::Refused(refused) => assert_eq!(
                refused.reason,
                proto::Reason::Queue as u8,
                "a draining purge must queue new Puts"
            ),
            _ => panic!("expected a queued refusal"),
        }
        // The retired extent is released on the next cleanup turn.
        store.pump();
        assert!(!store.namespaces[&id].purging, "the drain completed");
        let mut events = put_begin(&mut store, 1, id, next, None);
        assert!(
            matches!(events.try_recv().unwrap().unwrap(), PutAnswer::Accept(_)),
            "the namespace accepts Puts again after the drain"
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
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
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
                    origin: Origin::Local,
                    remote: None,
                    header,
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
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
    }

    #[test]
    fn a_committed_put_and_a_detached_put_hold_their_reservation() {
        let (mut store, directory) = store("quota-unfinished");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store
            .hello(1, "l0".into(), std::process::id(), value.clone())
            .unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [13; 16],
        };
        let _events = put_begin(&mut store, 1, id, key, None);
        assert!(
            store.pending_reserved(id) > 0,
            "an accepted Put is reserved"
        );
        // A committed streaming Put is still unpublished while its disk write drains.
        {
            let open = store.sessions.get_mut(&1).unwrap().ops.get_mut(&1).unwrap();
            let Operation::Put(put) = &mut open.operation else {
                panic!("expected a Put");
            };
            put.committed = true;
        }
        assert!(
            store.pending_reserved(id) > 0,
            "a committed-but-unpublished Put still holds its reservation"
        );
        // Detached Puts keep their reservation until they settle too.
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        let (events, _receiver) = tokio::sync::mpsc::channel(4);
        let epoch = store.namespaces[&id].epoch;
        let detached = Put {
            namespace: id,
            epoch,
            remote: false,
            header,
            first_page: 0,
            grants: Vec::new(),
            windows: FilledWindows {
                granted: HashMap::new(),
                next: 0,
                pages: vec![None; 1],
                row: None,
                pages_filled: 0,
                row_filled: true,
            },
            memory: None,
            temporary: None,
            streaming: false,
            commit_bytes: Vec::new(),
            metadata: None,
            committed: true,
            finalizing: false,
            cancelled: false,
            terminal: false,
            finished: false,
            pending: 1,
            blocked: VecDeque::new(),
            pins: VecDeque::new(),
            reservation: 4096,
            requirement: PutRequirement::arena(0),
            staging_hold: None,
            hold_unit: 0,
            hold_slots: [None, None],
            events,
            outbox: VecDeque::new(),
        };
        store.detached_puts.insert((2, 2), detached);
        let with_detached = store.pending_reserved(id);
        store.detached_puts.remove(&(2, 2));
        assert_eq!(
            with_detached - store.pending_reserved(id),
            4096,
            "a detached Put holds its reservation"
        );
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_removed_remote_child_is_not_resurrected() {
        let (mut store, directory) = store("deferred-removed");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let parent = Key {
            consumed_tokens: 64,
            digest: [14; 16],
        };
        let child = Key {
            consumed_tokens: 128,
            digest: [15; 16],
        };
        let entry = |key, parent: Option<Key>, tail: u64| proto::CatalogEntry {
            key: Some(crate::control::key_to_proto(&key)),
            parent: parent.map(|parent| crate::control::key_to_proto(&parent)),
            has_parent: parent.is_some(),
            bucket: vec![0; 16],
            tail_pages: tail,
            row: true,
            tier: proto::Tier::Nvme as i32,
            durable: false,
            bytes: 8192,
            unreadable: false,
        };
        let send = |store: &mut Store, sequence, event| {
            store.handle(StoreCommand::Peer(PeerEvent::Catalog {
                node: "upstream".into(),
                namespace: id,
                event: proto::CatalogEvent {
                    sequence,
                    event: Some(event),
                },
            }));
            store.pump();
        };
        use proto::catalog_event::Event;
        send(&mut store, 1, Event::Added(entry(child, Some(parent), 1)));
        assert!(
            store.namespaces[&id].tree.node(&child).is_none(),
            "the child waits for its parent"
        );
        send(
            &mut store,
            2,
            Event::Removed(crate::control::key_to_proto(&child)),
        );
        send(&mut store, 3, Event::Added(entry(parent, None, 1)));
        assert!(
            store.namespaces[&id].tree.node(&child).is_none(),
            "a removed child must not be resurrected by a later parent"
        );
        assert!(store.namespaces[&id].tree.node(&parent).is_some());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn releasing_a_fetched_copy_returns_its_charge() {
        let (mut store, directory) = store("release-fetch");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [8; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
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
                    origin: Origin::Peer,
                    remote: Some(RemoteSource {
                        node: "upstream".into(),
                        bytes: 4096,
                        row: true,
                    }),
                    header,
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes,
                },
            )
            .unwrap();
        store.charged = bytes;
        store.release_fetched(id, key);
        assert_eq!(store.charged, 0);
        let node = store.namespaces[&id].tree.node(&key).unwrap();
        assert!(node.location.memory.is_none());
        assert_eq!(node.location.bytes, 4096);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_draining_or_opposite_transfer_id_is_fenced() {
        let (mut store, directory) = store("fence");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        store.peer_hello(1, "near").unwrap();
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
        store.peer_hello(1, "near").unwrap();
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
        store.peer_hello(1, "near").unwrap();
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
    fn a_dropped_forward_releases_its_pins() {
        let (mut store, directory) = store("forward-drop");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [3; 16],
        };
        let header = Header::new(&value, key, None, [0; 16], 1).unwrap();
        let extent = Arc::new(
            store
                .memory
                .allocate(header.length_with_row() as usize)
                .unwrap(),
        );
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
                    origin: Origin::Local,
                    remote: None,
                    header,
                    memory: Some(extent),
                    disk: None,
                    durable: false,
                    writing: false,
                    job: None,
                    rewrite: false,
                    retry: Instant::now(),
                    bytes: 4096,
                },
            )
            .unwrap();
        let job = store.prepare_forward(id, key).unwrap();
        assert!(store.namespaces[&id].tree.node(&key).unwrap().pinned());
        store.forward_dropped(id, key);
        assert!(!store.namespaces[&id].tree.node(&key).unwrap().pinned());
        drop(job);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn a_remote_catalog_snapshot_merges_and_removes_sources() {
        let (mut store, directory) = store("catalog");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let key = Key {
            consumed_tokens: 64,
            digest: [7; 16],
        };
        let entry = proto::CatalogEntry {
            key: Some(crate::control::key_to_proto(&key)),
            parent: None,
            has_parent: false,
            bucket: vec![0; 16],
            tail_pages: 1,
            row: true,
            tier: proto::Tier::Dram as i32,
            durable: false,
            bytes: 8192,
            unreadable: false,
        };
        let event = |sequence, body| proto::CatalogEvent {
            sequence,
            event: Some(body),
        };
        use proto::catalog_event::Event;
        for event in [
            event(1, Event::SnapshotBegin(proto::SnapshotBegin { seq0: 0 })),
            event(2, Event::Entry(entry)),
            event(3, Event::SnapshotEnd(proto::SnapshotEnd {})),
        ] {
            store.handle(StoreCommand::Peer(PeerEvent::Catalog {
                node: "upstream".into(),
                namespace: id,
                event,
            }));
        }
        let node = store.namespaces[&id].tree.node(&key).unwrap();
        assert_eq!(node.location.remote.as_ref().unwrap().node, "upstream");
        let records = store.chain_records(id, &[key]);
        assert!(records[0].remote && !records[0].local);
        store.handle(StoreCommand::Peer(PeerEvent::Catalog {
            node: "upstream".into(),
            namespace: id,
            event: event(4, Event::Removed(crate::control::key_to_proto(&key))),
        }));
        assert!(store.namespaces[&id].tree.node(&key).is_none());
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }

    #[test]
    fn admin_pins_protect_ancestors_and_quota_evicts_leaves() {
        let (mut store, directory) = store("quota");
        let value = descriptor();
        let id = store.register_namespace(value.clone()).unwrap();
        let root = Key {
            consumed_tokens: 64,
            digest: [1; 16],
        };
        let child = Key {
            consumed_tokens: 128,
            digest: [2; 16],
        };
        let insert = |store: &mut Store, key: Key, parent: Option<Key>, tail: u32| -> u64 {
            let header = Header::new(&value, key, parent, [0; 16], tail).unwrap();
            let bytes = record_charge(&header, true);
            let extent = Arc::new(
                store
                    .memory
                    .allocate(header.length_with_row() as usize)
                    .unwrap(),
            );
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
                        origin: Origin::Local,
                        remote: None,
                        header,
                        memory: Some(extent),
                        disk: None,
                        durable: false,
                        writing: false,
                        job: None,
                        rewrite: false,
                        retry: Instant::now(),
                        bytes,
                    },
                )
                .unwrap();
            store.charged += bytes;
            bytes
        };
        let root_bytes = insert(&mut store, root, None, 1);
        insert(&mut store, child, Some(root), 1);
        let pin = |store: &mut Store, key: Key, pin: bool| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::Admin(AdminCommand::Pin {
                namespace: id.to_vec(),
                keys: vec![key],
                pin,
                reply,
            }));
            answer.blocking_recv().unwrap().unwrap()
        };
        let quota = |store: &mut Store, nvme: u64| {
            let (reply, answer) = tokio::sync::oneshot::channel();
            store.handle(StoreCommand::Admin(AdminCommand::SetQuota {
                namespace: id.to_vec(),
                dram: 0,
                nvme,
                reply,
            }));
            answer.blocking_recv().unwrap().unwrap()
        };
        // The child and its ancestor both change protection.
        assert_eq!(pin(&mut store, child, true), 2);
        assert!(store.namespaces[&id].tree.node(&root).unwrap().pinned());
        assert_eq!(pin(&mut store, child, false), 2);
        assert!(!store.namespaces[&id].tree.node(&root).unwrap().pinned());
        // A quota at one record's size evicts the only eligible leaf.
        assert_eq!(quota(&mut store, root_bytes), 0);
        assert!(store.namespaces[&id].tree.node(&child).is_none());
        assert!(store.namespace_charge(id, false) <= root_bytes);
        // A pinned record cannot be evicted, so the excess is reported.
        assert_eq!(pin(&mut store, root, true), 1);
        assert_eq!(quota(&mut store, 1), root_bytes - 1);
        drop(store);
        let _ = fs::remove_dir_all(directory);
    }
}
