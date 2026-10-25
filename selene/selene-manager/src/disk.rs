use crate::{
    Result,
    arena::{Arena, Extent, ExtentSlice},
    invalid, proto,
    record::{self, Descriptor, Header},
};
use prost::Message;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

pub const ENVELOPE_BYTES: u64 = 4096;
const MAGIC: &[u8; 8] = b"FLCMREC1";
const DESCRIPTOR_MAGIC: &[u8; 8] = b"FLCDSC2\0";

pub struct RootLock {
    _file: File,
}
impl RootLock {
    pub fn acquire(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root.join(".lock"))?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { _file: file })
    }
}

pub struct DiskExtent {
    pub path: PathBuf,
    retired: AtomicBool,
    file: std::sync::Mutex<Option<Arc<File>>>,
    // The extent itself owns one reference; readers and jobs take additional ones.
    references: AtomicUsize,
}
impl DiskExtent {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            retired: AtomicBool::new(false),
            file: std::sync::Mutex::new(None),
            references: AtomicUsize::new(1),
        }
    }
    pub fn reader(&self) -> Result<Arc<File>> {
        let mut file = self.file.lock().unwrap();
        if file.is_none() {
            *file = Some(Arc::new(open_read(&self.path)?));
        }
        Ok(file.as_ref().unwrap().clone())
    }
    pub fn keep(&self) {
        self.retired.store(false, Ordering::Release);
    }
    pub fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
    pub fn acquire(&self) {
        self.references.fetch_add(1, Ordering::AcqRel);
    }
    pub fn release(&self) {
        let mut current = self.references.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return;
            }
            match self.references.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
    pub fn references(&self) -> usize {
        self.references.load(Ordering::Acquire)
    }
}
impl Drop for DiskExtent {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
pub struct IoGate {
    armed: AtomicBool,
    entered: SyncSender<()>,
    release: std::sync::Mutex<Receiver<()>>,
}
#[cfg(any(test, feature = "test-support"))]
impl IoGate {
    pub fn new() -> (Arc<Self>, Receiver<()>, SyncSender<()>) {
        let (entered, ready) = mpsc::sync_channel(1);
        let (release, proceed) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                armed: AtomicBool::new(true),
                entered,
                release: std::sync::Mutex::new(proceed),
            }),
            ready,
            release,
        )
    }
    pub fn arm(&self) {
        assert!(
            !self.armed.swap(true, Ordering::AcqRel),
            "disk gate is already armed"
        );
    }
    fn wait(&self) -> Result<()> {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.entered
                .send(())
                .map_err(|_| invalid("disk gate observer dropped"))?;
            self.release
                .lock()
                .unwrap()
                .recv()
                .map_err(|_| invalid("disk gate released without a signal"))?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug, Default)]
pub struct Faults {
    pub io_gate: Option<Arc<IoGate>>,
    pub read_gate: Option<Arc<IoGate>>,
    pub pause_at: Option<String>,
    pub signal: Option<PathBuf>,
    pub disk_full: Option<PathBuf>,
    pub delay_ms: u64,
    /// Ordered record of serviced tasks: `R` for a read, `W` for a task from
    /// the write channel. Used to observe the worker's own scheduling order
    /// without racing completion receivers.
    pub trace: Option<Arc<std::sync::Mutex<Vec<u8>>>>,
}
#[cfg(any(test, feature = "test-support"))]
impl Faults {
    pub(crate) fn checkpoint(&self, point: &str) {
        if self.pause_at.as_deref() == Some(point) {
            if let Some(path) = &self.signal {
                fs::write(path, point).unwrap();
                while path.exists() {
                    thread::sleep(Duration::from_millis(2));
                }
            }
        }
    }
    fn trace(&self, kind: u8) {
        if let Some(trace) = &self.trace {
            trace.lock().unwrap().push(kind);
        }
    }
    fn before_io(&self) -> Result<()> {
        if let Some(gate) = &self.io_gate {
            gate.wait()?;
        }
        if self.delay_ms > 0 {
            thread::sleep(Duration::from_millis(self.delay_ms));
        }
        if self.disk_full.as_ref().is_some_and(|path| path.exists()) {
            return Err(io::Error::from_raw_os_error(libc::ENOSPC));
        }
        Ok(())
    }
}

pub enum RecordData {
    Memory(Arc<Extent>),
    Metadata(Arc<Vec<u8>>),
}
impl RecordData {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Memory(extent) => extent.bytes(),
            Self::Metadata(bytes) => bytes,
        }
    }
}
pub enum Task {
    Publish {
        id: u64,
        temporary: Arc<DiskExtent>,
        destination: Arc<DiskExtent>,
        data: RecordData,
        header: Header,
        row: bool,
    },
    Chunk {
        id: u64,
        temporary: Arc<DiskExtent>,
        offset: u64,
        data: ExtentSlice,
    },
    Compact {
        id: u64,
        source: Arc<DiskExtent>,
        temporary: Arc<DiskExtent>,
        header: Header,
    },
    Read {
        id: u64,
        file: Arc<File>,
        offset: u64,
        buffer: Extent,
    },
}

impl Task {
    fn id(&self) -> u64 {
        match self {
            Self::Publish { id, .. }
            | Self::Chunk { id, .. }
            | Self::Compact { id, .. }
            | Self::Read { id, .. } => *id,
        }
    }
}

pub struct Completion {
    pub id: u64,
    pub result: Result<()>,
    pub bytes: u64,
    pub elapsed: Duration,
    pub data: Option<Extent>,
}

pub struct ReadTask {
    pub file: Arc<File>,
    pub offset: u64,
    pub buffer: Extent,
    pub ack: SyncSender<(Result<()>, Extent)>,
}

#[derive(Clone)]
pub struct Reader {
    sender: SyncSender<ReadTask>,
}
impl Reader {
    pub fn read(&self, file: &Arc<File>, offset: u64, buffer: Extent) -> Result<Extent> {
        let (ack, done) = mpsc::sync_channel(1);
        self.sender
            .send(ReadTask {
                file: file.clone(),
                offset,
                buffer,
                ack,
            })
            .map_err(|_| invalid("disk reader stopped"))?;
        let (result, buffer) = done.recv().map_err(|_| invalid("disk reader stopped"))?;
        result.map(|()| buffer)
    }
}

pub struct Worker {
    pub sender: SyncSender<Task>,
    /// Async reads that report through `receiver` and are serviced before
    /// queued writes, subject to the read-streak guard.
    pub read_sender: SyncSender<Task>,
    pub receiver: Receiver<Completion>,
    reader: Reader,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Worker {
    pub fn start(
        #[cfg(any(test, feature = "test-support"))] faults: Faults,
        lock: Arc<RootLock>,
    ) -> Self {
        let (sender, writes) = mpsc::sync_channel::<Task>(8);
        let (read_sender, reads) = mpsc::sync_channel::<ReadTask>(8);
        let (async_read_sender, async_reads) = mpsc::sync_channel::<Task>(8);
        let (done, receiver) = mpsc::sync_channel(16);
        let state_thread = thread::current();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = thread::spawn(move || {
            let _lock = lock;
            let mut read_streak = 0;
            loop {
                if thread_stop.load(Ordering::Acquire) {
                    break;
                }
                let mut acted = false;
                if read_streak < 4 {
                    if let Ok(mut task) = reads.try_recv() {
                        let result = read_into(
                            &task.file,
                            task.offset,
                            &mut task.buffer,
                            #[cfg(any(test, feature = "test-support"))]
                            &faults,
                        );
                        read_streak += 1;
                        #[cfg(any(test, feature = "test-support"))]
                        faults.trace(b'R');
                        let _ = task.ack.send((result, task.buffer));
                        acted = true;
                    } else if let Ok(task) = async_reads.try_recv() {
                        if let Task::Read {
                            id,
                            file,
                            offset,
                            mut buffer,
                            ..
                        } = task
                        {
                            let started = Instant::now();
                            let result = read_into(
                                &file,
                                offset,
                                &mut buffer,
                                #[cfg(any(test, feature = "test-support"))]
                                &faults,
                            );
                            let completion = Completion {
                                id,
                                result,
                                bytes: 0,
                                elapsed: started.elapsed(),
                                data: Some(buffer),
                            };
                            #[cfg(any(test, feature = "test-support"))]
                            faults.trace(b'R');
                            send_completion(&done, completion, &thread_stop);
                            state_thread.unpark();
                        }
                        read_streak += 1;
                        acted = true;
                    }
                }
                if !acted {
                    if let Ok(task) = writes.try_recv() {
                        read_streak = 0;
                        let started = Instant::now();
                        let id = task.id();
                        let (result, data) = match task {
                            Task::Read {
                                file,
                                offset,
                                buffer,
                                ..
                            } => {
                                let mut buffer = buffer;
                                let result = read_into(
                                    &file,
                                    offset,
                                    &mut buffer,
                                    #[cfg(any(test, feature = "test-support"))]
                                    &faults,
                                );
                                (result.map(|()| 0), Some(buffer))
                            }
                            task => (
                                execute(
                                    task,
                                    #[cfg(any(test, feature = "test-support"))]
                                    &faults,
                                ),
                                None,
                            ),
                        };
                        let bytes = result.as_ref().copied().unwrap_or(0);
                        let mut completion = Completion {
                            id,
                            result: result.map(|_| ()),
                            bytes,
                            elapsed: started.elapsed(),
                            data,
                        };
                        #[cfg(any(test, feature = "test-support"))]
                        faults.trace(b'W');
                        loop {
                            match done.try_send(completion) {
                                Ok(()) => break,
                                Err(mpsc::TrySendError::Full(returned)) => {
                                    completion = returned;
                                    if thread_stop.load(Ordering::Acquire) {
                                        break;
                                    }
                                    thread::park_timeout(Duration::from_micros(200));
                                }
                                Err(mpsc::TrySendError::Disconnected(_)) => break,
                            }
                        }
                        state_thread.unpark();
                        acted = true;
                    }
                }
                if !acted {
                    if let Ok(task) = async_reads.try_recv() {
                        if let Task::Read {
                            id,
                            file,
                            offset,
                            mut buffer,
                            ..
                        } = task
                        {
                            let started = Instant::now();
                            let result = read_into(
                                &file,
                                offset,
                                &mut buffer,
                                #[cfg(any(test, feature = "test-support"))]
                                &faults,
                            );
                            let completion = Completion {
                                id,
                                result,
                                bytes: 0,
                                elapsed: started.elapsed(),
                                data: Some(buffer),
                            };
                            #[cfg(any(test, feature = "test-support"))]
                            faults.trace(b'R');
                            send_completion(&done, completion, &thread_stop);
                            state_thread.unpark();
                        }
                        read_streak += 1;
                        acted = true;
                    }
                }
                if !acted {
                    if let Ok(mut task) = reads.try_recv() {
                        // No pending write: keep reading rather than idle.
                        let result = read_into(
                            &task.file,
                            task.offset,
                            &mut task.buffer,
                            #[cfg(any(test, feature = "test-support"))]
                            &faults,
                        );
                        read_streak += 1;
                        #[cfg(any(test, feature = "test-support"))]
                        faults.trace(b'R');
                        let _ = task.ack.send((result, task.buffer));
                        acted = true;
                    }
                }
                if !acted {
                    thread::park_timeout(Duration::from_micros(100));
                }
            }
        });
        Self {
            sender,
            read_sender: async_read_sender,
            receiver,
            reader: Reader {
                sender: read_sender,
            },
            stop,
            thread: Some(thread),
        }
    }
    pub fn reader(&self) -> Reader {
        self.reader.clone()
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn send_completion(done: &SyncSender<Completion>, mut completion: Completion, stop: &AtomicBool) {
    loop {
        match done.try_send(completion) {
            Ok(()) => break,
            Err(mpsc::TrySendError::Full(returned)) => {
                completion = returned;
                if stop.load(Ordering::Acquire) {
                    break;
                }
                thread::park_timeout(Duration::from_micros(200));
            }
            Err(mpsc::TrySendError::Disconnected(_)) => break,
        }
    }
}

fn read_into(
    file: &File,
    offset: u64,
    buffer: &mut Extent,
    #[cfg(any(test, feature = "test-support"))] faults: &Faults,
) -> Result<()> {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(gate) = &faults.read_gate {
        gate.wait()?;
    }
    read_at(file, buffer.bytes_mut(), offset)
}

fn open(path: &Path, write: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_DIRECT);
    if write {
        options.create(true).truncate(false);
    }
    options.open(path)
}

pub fn read_at(file: &File, bytes: &mut [u8], offset: u64) -> Result<()> {
    file.read_exact_at(bytes, offset)
}
fn write_at(file: &File, bytes: &[u8], offset: u64) -> Result<()> {
    file.write_all_at(bytes, offset)
}

fn execute(task: Task, #[cfg(any(test, feature = "test-support"))] faults: &Faults) -> Result<u64> {
    #[cfg(any(test, feature = "test-support"))]
    faults.before_io()?;
    match task {
        Task::Chunk {
            temporary,
            offset,
            data,
            ..
        } => {
            let file = open(&temporary.path, true)?;
            write_at(&file, data.bytes(), ENVELOPE_BYTES + offset)?;
            Ok(data.bytes().len() as u64)
        }
        Task::Publish {
            temporary,
            destination,
            data,
            header,
            row,
            ..
        } => {
            let file = open(&temporary.path, true)?;
            match &data {
                RecordData::Memory(extent) => {
                    #[cfg(any(test, feature = "test-support"))]
                    faults.checkpoint("parent_write");
                    write_at(&file, extent.bytes(), ENVELOPE_BYTES)?;
                }
                RecordData::Metadata(bytes) => {
                    let arena = Arena::new(4 * 1024 * 1024)?;
                    for (index, chunk) in bytes.chunks(4 * 1024 * 1024).enumerate() {
                        let mut buffer = arena.allocate(chunk.len()).unwrap();
                        buffer.bytes_mut().copy_from_slice(chunk);
                        write_at(
                            &file,
                            buffer.bytes(),
                            ENVELOPE_BYTES + (index * 4 * 1024 * 1024) as u64,
                        )?;
                    }
                }
            }
            finish(
                &file,
                &temporary.path,
                &destination.path,
                data.bytes(),
                &header,
                row,
                #[cfg(any(test, feature = "test-support"))]
                faults,
            )?;
            if matches!(data, RecordData::Metadata(_)) {
                destination.retire();
            }
            #[cfg(any(test, feature = "test-support"))]
            faults.checkpoint("after_rename");
            Ok(data.bytes().len() as u64 + ENVELOPE_BYTES)
        }
        Task::Compact {
            source,
            temporary,
            header,
            ..
        } => {
            let input = open(&source.path, false)?;
            let output = open(&temporary.path, true)?;
            let arena = Arena::new(4 * 1024 * 1024)?;
            let mut offset = ENVELOPE_BYTES;
            let mut metadata = Vec::new();
            while offset < ENVELOPE_BYTES + header.row_segment {
                let length =
                    (ENVELOPE_BYTES + header.row_segment - offset).min(4 * 1024 * 1024) as usize;
                let mut buffer = arena.allocate(length).unwrap();
                read_at(&input, buffer.bytes_mut(), offset)?;
                if metadata.len() < header.page_segment as usize {
                    let count = (header.page_segment as usize - metadata.len()).min(length);
                    metadata.extend_from_slice(&buffer.bytes()[..count]);
                }
                write_at(&output, buffer.bytes(), offset)?;
                offset += length as u64;
            }
            finish(
                &output,
                &temporary.path,
                &source.path,
                &metadata,
                &header,
                false,
                #[cfg(any(test, feature = "test-support"))]
                faults,
            )?;
            Ok(header.row_segment + ENVELOPE_BYTES)
        }
        Task::Read { .. } => Err(invalid("read task routed to the write executor")),
    }
}

fn finish(
    file: &File,
    temporary: &Path,
    final_path: &Path,
    metadata: &[u8],
    header: &Header,
    row: bool,
    #[cfg(any(test, feature = "test-support"))] faults: &Faults,
) -> Result<()> {
    let length = header.length(row);
    let arena = Arena::new(4096)?;
    let mut envelope = arena.allocate(4096).unwrap();
    envelope.bytes_mut().fill(0);
    envelope.bytes_mut()[..8].copy_from_slice(MAGIC);
    envelope.bytes_mut()[8..12].copy_from_slice(&1u32.to_le_bytes());
    envelope.bytes_mut()[12..16].copy_from_slice(&(row as u32).to_le_bytes());
    envelope.bytes_mut()[16..24].copy_from_slice(&length.to_le_bytes());
    let crc = crc32(&metadata[..header.page_segment as usize]);
    envelope.bytes_mut()[24..28].copy_from_slice(&crc.to_le_bytes());
    let crc = crc32(&envelope.bytes()[..28]);
    envelope.bytes_mut()[28..32].copy_from_slice(&crc.to_le_bytes());
    write_at(file, envelope.bytes(), 0)?;
    file.set_len(ENVELOPE_BYTES + length)?;
    file.sync_data()?;
    #[cfg(any(test, feature = "test-support"))]
    faults.checkpoint("before_rename");
    fs::rename(temporary, final_path)?;
    File::open(final_path.parent().unwrap())?.sync_all()?;
    Ok(())
}

pub fn open_read(path: &Path) -> Result<File> {
    open(path, false)
}

pub struct Recovered {
    pub descriptor: Descriptor,
    pub records: Vec<(Header, bool, Arc<DiskExtent>)>,
}

pub fn descriptor(root: &Path, value: &Descriptor) -> Result<()> {
    value.validate()?;
    let directory = root.join(value.name());
    fs::create_dir_all(&directory)?;
    let path = directory.join("descriptor");
    if path.exists() {
        if load_descriptor(&path)? != *value {
            return Err(invalid("namespace descriptor mismatch"));
        }
        return Ok(());
    }
    let encoded = proto_descriptor(value).encode_to_vec();
    let length = u32::try_from(encoded.len()).map_err(|_| invalid("descriptor length"))?;
    let mut bytes = Vec::with_capacity(16 + encoded.len());
    bytes.extend_from_slice(DESCRIPTOR_MAGIC);
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&encoded);
    let crc = crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    let temp = directory.join(".descriptor.tmp");
    let mut file = File::create(&temp)?;
    file.write_all(&bytes)?;
    file.sync_data()?;
    fs::rename(temp, path)?;
    File::open(directory)?.sync_all()
}

fn proto_descriptor(value: &Descriptor) -> proto::Descriptor {
    proto::Descriptor {
        identity: value.identity.to_vec(),
        fingerprint: value.fingerprint.to_vec(),
        page_tokens: value.page_tokens,
        ranks: value
            .ranks
            .iter()
            .map(|rank| proto::RankPlanes {
                plane_bytes: rank.planes.clone(),
                row_bytes: rank.row_bytes,
            })
            .collect(),
        page_record_bytes: value.page_bytes(),
        row_record_bytes: value.row_bytes(),
        pool_name: value.pool_name.clone(),
    }
}

fn record_descriptor(value: proto::Descriptor) -> Result<Descriptor> {
    let identity: [u8; 32] = value
        .identity
        .as_slice()
        .try_into()
        .map_err(|_| invalid("descriptor identity length"))?;
    let fingerprint: [u8; 16] = value
        .fingerprint
        .as_slice()
        .try_into()
        .map_err(|_| invalid("descriptor fingerprint length"))?;
    let descriptor = Descriptor {
        identity,
        fingerprint,
        page_tokens: value.page_tokens,
        ranks: value
            .ranks
            .into_iter()
            .map(|rank| record::RankSizes {
                planes: rank.plane_bytes,
                row_bytes: rank.row_bytes,
            })
            .collect(),
        pool_name: value.pool_name,
    };
    descriptor.validate()?;
    if value.page_record_bytes != descriptor.page_bytes()
        || value.row_record_bytes != descriptor.row_bytes()
    {
        return Err(invalid("record sizes disagree with ranks"));
    }
    Ok(descriptor)
}

fn load_descriptor(path: &Path) -> Result<Descriptor> {
    let mut bytes = Vec::new();
    File::open(path)?.take(8193).read_to_end(&mut bytes)?;
    if bytes.len() < 16 || bytes.len() > 8192 {
        return Err(invalid("descriptor length"));
    }
    if &bytes[..8] != DESCRIPTOR_MAGIC {
        return Err(invalid("descriptor magic"));
    }
    let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if 16 + length != bytes.len() {
        return Err(invalid("descriptor length"));
    }
    let tail = 12 + length;
    if crc32(&bytes[..tail]) != u32::from_le_bytes(bytes[tail..].try_into().unwrap()) {
        return Err(invalid("descriptor CRC"));
    }
    record_descriptor(proto::Descriptor::decode(&bytes[12..tail])?)
}

pub fn recover(root: &Path) -> Result<Vec<Recovered>> {
    let mut namespaces = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let directory = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(not_migratable(&directory));
        };
        if name.len() != 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(not_migratable(&directory));
        }
        let path = directory.join("descriptor");
        let descriptor = load_descriptor(&path).map_err(|_| not_migratable(&path))?;
        if name != descriptor.name() {
            return Err(not_migratable(&path));
        }
        let mut records = Vec::new();
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let path = entry.path();
            if name.starts_with('.') && name.ends_with(".tmp") {
                fs::remove_file(path)?;
                continue;
            }
            if !name.ends_with(".rec") {
                continue;
            }
            match inspect(&path, name, &descriptor) {
                Ok((header, row)) => records.push((header, row, Arc::new(DiskExtent::new(path)))),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    fs::remove_file(path)?
                }
                Err(error) => return Err(error),
            }
        }
        records.sort_by_key(|(header, _, _)| header.key);
        let mut kept = std::collections::HashSet::new();
        records.retain(|(header, _, extent)| {
            if header.parent.is_none_or(|parent| kept.contains(&parent)) {
                kept.insert(header.key);
                true
            } else {
                extent.retire();
                false
            }
        });
        namespaces.push(Recovered {
            descriptor,
            records,
        });
    }
    Ok(namespaces)
}

fn not_migratable(path: &Path) -> io::Error {
    invalid(format!(
        "{}: v3 roots are not migratable to the v2 descriptor format",
        path.display()
    ))
}

fn inspect(path: &Path, name: &str, descriptor: &Descriptor) -> Result<(Header, bool)> {
    let mut file = File::open(path)?;
    let mut prefix = [0; 4096];
    file.read_exact(&mut prefix)?;
    if &prefix[..8] != MAGIC
        || u32::from_le_bytes(prefix[8..12].try_into().unwrap()) != 1
        || crc32(&prefix[..28]) != u32::from_le_bytes(prefix[28..32].try_into().unwrap())
    {
        return Err(invalid("envelope magic, version or CRC"));
    }
    let row = match u32::from_le_bytes(prefix[12..16].try_into().unwrap()) {
        0 => false,
        1 => true,
        _ => return Err(invalid("envelope flags")),
    };
    let length = u64::from_le_bytes(prefix[16..24].try_into().unwrap());
    let mut block = [0; 4096];
    file.read_exact(&mut block)?;
    let header = Header::decode(&block).map_err(|e| invalid(e.to_string()))?;
    if record::parse_file_name(name) != Some(header.key)
        || header.identity != descriptor.identity
        || header.fingerprint != descriptor.fingerprint
        || !descriptor.matches(&header)
        || length != header.length(row)
        || file.metadata()?.len() != ENVELOPE_BYTES + length
    {
        return Err(invalid("envelope record mismatch"));
    }
    // Hash the variable checksum segment without allocating in proportion to an untrusted length.
    let mut crc = !0u32;
    crc_update(&mut crc, &block);
    let mut remaining = header.checksum_bytes();
    while remaining > 0 {
        file.read_exact(&mut block)?;
        crc_update(&mut crc, &block);
        remaining -= 4096;
    }
    if !crc != u32::from_le_bytes(prefix[24..28].try_into().unwrap()) {
        return Err(invalid("metadata CRC"));
    }
    Ok((header, row))
}

fn crc_update(crc: &mut u32, bytes: &[u8]) {
    for byte in bytes {
        *crc ^= u32::from(*byte);
        for _ in 0..8 {
            *crc = (*crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(*crc & 1));
        }
    }
}
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    crc_update(&mut crc, bytes);
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    fn base(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("selene-disk-{}-{label}", std::process::id()))
    }
    #[test]
    fn older_descriptors_keep_their_explicit_or_absent_salt() {
        let value = Descriptor {
            identity: [7; 32],
            fingerprint: [3; 16],
            page_tokens: 64,
            ranks: vec![record::RankSizes {
                planes: vec![4000],
                row_bytes: 1000,
            }],
            pool_name: String::new(),
        };
        let mut legacy = proto_descriptor(&value).encode_to_vec();
        let absent =
            record_descriptor(proto::Descriptor::decode(legacy.as_slice()).unwrap()).unwrap();
        assert_eq!(absent, value);
        // Former tenant string at field 7, unchanged in persisted descriptors.
        legacy.extend_from_slice(b"\x3a\x06legacy");
        let restored =
            record_descriptor(proto::Descriptor::decode(legacy.as_slice()).unwrap()).unwrap();
        assert_eq!(restored.pool_name, "legacy");
        assert_eq!(
            restored.namespace_id(),
            record::namespace_id(&value.identity, &value.fingerprint, "legacy")
        );
    }

    #[test]
    fn disk_queue_read_write_interleave() {
        let base = base("disk");
        fs::create_dir_all(&base).unwrap();
        let lock = Arc::new(RootLock::acquire(&base.join("root")).unwrap());
        let trace = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (gate, entered, release) = IoGate::new();
        let faults = Faults {
            io_gate: Some(gate),
            trace: Some(trace.clone()),
            ..Faults::default()
        };
        let worker = Worker::start(faults, lock);
        let source = base.join("source");
        let mut bytes = vec![0u8; 4096];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = index as u8;
        }
        fs::write(&source, &bytes).unwrap();
        let file = Arc::new(open_read(&source).unwrap());
        let arena = Arena::new(8 * 4096).unwrap();
        let reader = worker.reader();
        let write_arena = Arena::new(4 * 4096).unwrap();
        for index in 0..4 {
            let mut extent = write_arena.allocate(4096).unwrap();
            extent.bytes_mut().fill(index as u8);
            let temporary = Arc::new(DiskExtent::new(base.join(format!("w-{index}.tmp"))));
            temporary.retire();
            worker
                .sender
                .send(Task::Chunk {
                    id: index,
                    temporary,
                    offset: 0,
                    data: ExtentSlice::new(Arc::new(extent), 0, 4096),
                })
                .unwrap();
            if index == 0 {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        let mut readers = Vec::new();
        for _ in 0..8 {
            let (ack, done) = mpsc::sync_channel(1);
            reader
                .sender
                .send(ReadTask {
                    file: file.clone(),
                    offset: 0,
                    buffer: arena.allocate(4096).unwrap(),
                    ack,
                })
                .unwrap();
            readers.push(done);
        }
        release.send(()).unwrap();
        for _ in 0..4 {
            worker.receiver.recv().unwrap();
        }
        for done in readers {
            let (result, buffer) = done.recv_timeout(Duration::from_secs(5)).unwrap();
            result.unwrap();
            assert_eq!(buffer.bytes(), bytes.as_slice());
        }
        // The worker records its own service order, so the observation cannot be
        // reordered by reader or completion-receiver scheduling.
        let trace = trace.lock().unwrap();
        assert_eq!(trace.iter().filter(|kind| **kind == b'R').count(), 8);
        assert_eq!(trace.iter().filter(|kind| **kind == b'W').count(), 4);
        let mut run = 0;
        let mut saw_write = false;
        for &kind in trace.iter() {
            if kind == b'R' {
                run += 1;
                assert!(
                    !saw_write || run <= 4,
                    "more than four reads between writes"
                );
            } else {
                saw_write = true;
                run = 0;
            }
        }
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn async_reads_overtake_queued_writes() {
        let base = base("read-priority");
        fs::create_dir_all(&base).unwrap();
        let lock = Arc::new(RootLock::acquire(&base.join("root")).unwrap());
        let (gate, entered, release) = IoGate::new();
        let worker = Worker::start(
            Faults {
                io_gate: Some(gate),
                ..Faults::default()
            },
            lock,
        );
        let source = base.join("source");
        fs::write(&source, vec![7u8; 4096]).unwrap();
        let file = Arc::new(open_read(&source).unwrap());
        let arena = Arena::new(4096).unwrap();
        let write_arena = Arena::new(8 * 4096).unwrap();
        for index in 0..8u64 {
            let mut extent = write_arena.allocate(4096).unwrap();
            extent.bytes_mut().fill(index as u8);
            let temporary = Arc::new(DiskExtent::new(base.join(format!("w-{index}.tmp"))));
            temporary.retire();
            worker
                .sender
                .send(Task::Chunk {
                    id: index,
                    temporary,
                    offset: 0,
                    data: ExtentSlice::new(Arc::new(extent), 0, 4096),
                })
                .unwrap();
            if index == 0 {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        worker
            .read_sender
            .send(Task::Read {
                id: 999,
                file,
                offset: 0,
                buffer: arena.allocate(4096).unwrap(),
            })
            .unwrap();
        release.send(()).unwrap();
        // The write channel holds all eight writes, so the read is enqueued while at most the
        // first write is in flight; it must be the next completion.
        let mut writes_ahead = 0;
        loop {
            let completion = worker.receiver.recv().unwrap();
            if completion.id == 999 {
                assert_eq!(completion.data.unwrap().bytes(), &[7u8; 4096]);
                break;
            }
            writes_ahead += 1;
        }
        assert!(
            writes_ahead <= 1,
            "the read waited behind {writes_ahead} queued writes"
        );
        for _ in writes_ahead..8 {
            worker.receiver.recv().unwrap();
        }
        fs::remove_dir_all(base).unwrap();
    }
}
