//! Stream (TCP) data channel.
//!
//! Frames are little-endian. Every frame starts with `kind`, `transfer`, `index`, `piece` and
//! `pieces`; the kind then selects the remaining fields. A transfer is one granted window.

use super::{
    DataChannel, GrantKey, GrantTable, PIECE_BYTES, Region, RegionAccess, checked_range, invalid,
    piece_len, pieces,
};
use std::collections::{HashMap, HashSet};
use std::io::{self, IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const TOKEN_BYTES: usize = 32;
pub const KIND_WRITE: u8 = 1;
pub const KIND_READ: u8 = 2;
pub const KIND_READ_PAYLOAD: u8 = 3;
pub const KIND_ACK: u8 = 4;
pub const KIND_ERROR: u8 = 5;
const PREFIX: usize = 1 + 8 + 4 + 4 + 4;
const MAX_ERROR_TEXT: usize = 1024;
const SOCKET_BUFFER: u64 = 16 << 20;

const CODE_BAD_RANGE: u32 = 2;
const CODE_BAD_FRAME: u32 = 3;
const CODE_IO: u32 = 4;

const MISSING: u8 = 0;
const BUSY: u8 = 1;
const DONE: u8 = 2;

/// Receiver counters, exposed for metrics and tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub dropped_frames: u64,
    pub protocol_violations: u64,
    pub bad_tokens: u64,
    pub duplicates: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

fn prefix(kind: u8, key: GrantKey, piece: u32, pieces: u32) -> [u8; PREFIX] {
    let mut bytes = [0u8; PREFIX];
    bytes[0] = kind;
    bytes[1..9].copy_from_slice(&key.transfer.to_le_bytes());
    bytes[9..13].copy_from_slice(&key.index.to_le_bytes());
    bytes[13..17].copy_from_slice(&piece.to_le_bytes());
    bytes[17..21].copy_from_slice(&pieces.to_le_bytes());
    bytes
}

struct Parsed {
    kind: u8,
    key: GrantKey,
    piece: u32,
    pieces: u32,
}

fn parse_prefix(bytes: &[u8; PREFIX]) -> Parsed {
    Parsed {
        kind: bytes[0],
        key: GrantKey {
            transfer: u64::from_le_bytes(bytes[1..9].try_into().unwrap()),
            index: u32::from_le_bytes(bytes[9..13].try_into().unwrap()),
        },
        piece: u32::from_le_bytes(bytes[13..17].try_into().unwrap()),
        pieces: u32::from_le_bytes(bytes[17..21].try_into().unwrap()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Write,
    Read,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Active,
    Failed,
}

struct Pending {
    kind: Kind,
    destination: Option<(Region, u64)>,
    len: u64,
    pieces: u32,
    piece_state: Vec<u8>,
    received: u32,
    inflight: u32,
    phase: Phase,
    used: u64,
}

impl Pending {
    fn new(kind: Kind, destination: Option<(Region, u64)>, len: u64, count: u32) -> Self {
        Self {
            kind,
            destination,
            len,
            pieces: count,
            piece_state: vec![MISSING; count as usize],
            received: 0,
            inflight: 0,
            phase: Phase::Active,
            used: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum PieceStart {
    Accepted,
    Duplicate,
    Reject,
}

struct Shared {
    pending: Mutex<HashMap<GrantKey, Pending>>,
    changed: Condvar,
    done: Mutex<Vec<(GrantKey, io::Result<()>)>>,
    members: Vec<Mutex<Option<TcpStream>>>,
    closed: Vec<AtomicBool>,
    used: Vec<Mutex<HashSet<GrantKey>>>,
}

impl Shared {
    fn new(streams: &[TcpStream]) -> io::Result<Self> {
        let mut members = Vec::with_capacity(streams.len());
        for stream in streams {
            members.push(Mutex::new(Some(stream.try_clone()?)));
        }
        Ok(Self {
            pending: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
            done: Mutex::new(Vec::new()),
            members,
            closed: (0..streams.len()).map(|_| AtomicBool::new(false)).collect(),
            used: (0..streams.len())
                .map(|_| Mutex::new(HashSet::new()))
                .collect(),
        })
    }

    fn register(
        &self,
        key: GrantKey,
        kind: Kind,
        destination: Option<(Region, u64)>,
        len: u64,
    ) -> io::Result<u32> {
        let count = pieces(len);
        if count == 0 {
            return Err(invalid("zero-length transfer"));
        }
        let mut pending = self.pending.lock().unwrap();
        if pending.contains_key(&key) {
            return Err(invalid("duplicate transfer window"));
        }
        pending.insert(key, Pending::new(kind, destination, len, count));
        Ok(count)
    }

    fn use_connection(&self, key: GrantKey, index: usize) -> bool {
        if self.closed[index].load(Ordering::Acquire) {
            return false;
        }
        let mut map = self.pending.lock().unwrap();
        let Some(pending) = map.get_mut(&key) else {
            return false;
        };
        pending.used |= 1u64 << index;
        drop(map);
        self.used[index].lock().unwrap().insert(key);
        true
    }

    fn finish(&self, key: GrantKey, result: io::Result<()>) {
        let mut map = self.pending.lock().unwrap();
        if map.remove(&key).is_none() {
            return;
        }
        drop(map);
        for set in &self.used {
            set.lock().unwrap().remove(&key);
        }
        self.done.lock().unwrap().push((key, result));
    }

    /// Fail one window, first stopping every socket that carried it and waiting for any
    /// in-progress region access to finish.
    fn fail(&self, key: GrantKey, error: io::Error) {
        let used = {
            let mut map = self.pending.lock().unwrap();
            let Some(pending) = map.get_mut(&key) else {
                return;
            };
            if pending.phase == Phase::Failed {
                return;
            }
            pending.phase = Phase::Failed;
            pending.used
        };
        // Stop the sockets first: a reader blocked on a partial payload must not delay cleanup.
        for index in 0..self.members.len() {
            if used & (1u64 << index) != 0 {
                self.closed[index].store(true, Ordering::Release);
                if let Some(stream) = self.members[index].lock().unwrap().as_ref() {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
            }
        }
        {
            let mut map = self.pending.lock().unwrap();
            while map.get(&key).is_some_and(|pending| pending.inflight > 0) {
                map = self.changed.wait(map).unwrap();
            }
        }
        self.finish(key, Err(error));
    }

    fn fail_connection(&self, index: usize, error: io::Error) {
        self.closed[index].store(true, Ordering::Release);
        let keys: Vec<GrantKey> = self.used[index].lock().unwrap().drain().collect();
        for key in keys {
            self.fail(key, io::Error::new(error.kind(), error.to_string()));
        }
    }

    fn ack_piece(&self, key: GrantKey, piece: u32, frame_pieces: u32) {
        let mut map = self.pending.lock().unwrap();
        let Some(pending) = map.get_mut(&key) else {
            return;
        };
        if pending.phase != Phase::Active
            || pending.kind != Kind::Write
            || pending.pieces != frame_pieces
            || piece >= pending.pieces
        {
            return;
        }
        if pending.piece_state[piece as usize] != MISSING {
            return;
        }
        pending.piece_state[piece as usize] = DONE;
        pending.received += 1;
        if pending.received != pending.pieces {
            return;
        }
        map.remove(&key);
        drop(map);
        for set in &self.used {
            set.lock().unwrap().remove(&key);
        }
        self.done.lock().unwrap().push((key, Ok(())));
    }

    fn begin_piece(&self, key: GrantKey, piece: u32, frame_pieces: u32) -> PieceStart {
        let mut map = self.pending.lock().unwrap();
        let Some(pending) = map.get_mut(&key) else {
            return PieceStart::Reject;
        };
        if pending.phase != Phase::Active
            || pending.kind != Kind::Read
            || pending.pieces != frame_pieces
            || piece >= pending.pieces
        {
            return PieceStart::Reject;
        }
        match pending.piece_state[piece as usize] {
            MISSING => {
                pending.piece_state[piece as usize] = BUSY;
                pending.inflight += 1;
                PieceStart::Accepted
            }
            _ => PieceStart::Duplicate,
        }
    }

    fn end_piece(&self, key: GrantKey, piece: u32, ok: bool) {
        let mut map = self.pending.lock().unwrap();
        let Some(pending) = map.get_mut(&key) else {
            return;
        };
        if pending.piece_state.get(piece as usize) != Some(&BUSY) {
            return;
        }
        pending.piece_state[piece as usize] = if ok { DONE } else { MISSING };
        pending.inflight -= 1;
        if ok {
            pending.received += 1;
            if pending.phase == Phase::Active
                && pending.received == pending.pieces
                && pending.inflight == 0
            {
                map.remove(&key);
                drop(map);
                for set in &self.used {
                    set.lock().unwrap().remove(&key);
                }
                self.done.lock().unwrap().push((key, Ok(())));
                return;
            }
        }
        drop(map);
        self.changed.notify_all();
    }
}

fn send_frame(
    shared: &Shared,
    streams: &[TcpStream],
    index: usize,
    key: GrantKey,
    frame: &[u8],
    payload: &[u8],
) -> io::Result<()> {
    if !shared.use_connection(key, index) {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "data connection is closed",
        ));
    }
    let mut stream = &streams[index];
    if payload.is_empty() {
        return stream.write_all(frame);
    }
    let written = stream.write_vectored(&[IoSlice::new(frame), IoSlice::new(payload)])?;
    let total = frame.len() + payload.len();
    if written == total {
        return Ok(());
    }
    stream.write_all(&frame[written.min(frame.len())..])?;
    let consumed_payload = written.saturating_sub(frame.len());
    if consumed_payload < payload.len() {
        stream.write_all(&payload[consumed_payload..])?;
    }
    Ok(())
}

/// The sending side of the stream data channel.
pub struct StreamChannel {
    streams: Vec<TcpStream>,
    regions: Arc<dyn RegionAccess>,
    peer_sizes: (u64, u64),
    shared: Arc<Shared>,
    readers: Vec<JoinHandle<()>>,
    closed: bool,
}

impl StreamChannel {
    /// Open one token-authenticated connection to each address, under the caller's remaining
    /// connect budget: the connects are async, so a stalled peer is bounded by the deadline
    /// and the whole future can be cancelled. `peer_sizes` are the peer's arena and staging
    /// sizes, used to validate remote ranges.
    pub async fn connect_async(
        addresses: &[SocketAddr],
        token: &[u8; TOKEN_BYTES],
        regions: Arc<dyn RegionAccess>,
        peer_sizes: (u64, u64),
        deadline: tokio::time::Instant,
    ) -> io::Result<Self> {
        if addresses.is_empty() {
            return Err(invalid("a stream channel needs at least one connection"));
        }
        let mut streams = Vec::with_capacity(addresses.len());
        for address in addresses {
            let stream = tokio::time::timeout_at(deadline, tokio::net::TcpStream::connect(address))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "data connect made no progress")
                })??;
            stream.set_nodelay(true)?;
            let mut stream = stream.into_std()?;
            stream.set_nonblocking(false)?;
            set_buffers(&stream);
            stream.write_all(token)?;
            streams.push(stream);
        }
        let shared = Arc::new(Shared::new(&streams)?);
        let mut readers = Vec::with_capacity(streams.len());
        for (index, stream) in streams.iter().enumerate() {
            let stream = stream.try_clone()?;
            let shared = shared.clone();
            let regions = regions.clone();
            readers.push(
                thread::Builder::new()
                    .name("selene-data-read".into())
                    .spawn(move || reader_loop(stream, shared, regions, index))?,
            );
        }
        Ok(Self {
            streams,
            regions,
            peer_sizes,
            shared,
            readers,
            closed: false,
        })
    }

    fn peer_size(&self, region: Region) -> u64 {
        match region {
            Region::Arena => self.peer_sizes.0,
            Region::Staging => self.peer_sizes.1,
        }
    }

    fn local_range(&self, region: Region, offset: u64, len: u64) -> io::Result<()> {
        let length = usize::try_from(len).map_err(|_| invalid("transfer length"))?;
        checked_range(offset, length, self.regions.size(region))
            .map(|_| ())
            .ok_or_else(|| invalid("local range is misaligned or outside the region"))
    }

    fn remote_range(&self, region: Region, offset: u64, len: u64) -> io::Result<()> {
        let length = usize::try_from(len).map_err(|_| invalid("transfer length"))?;
        checked_range(offset, length, self.peer_size(region))
            .map(|_| ())
            .ok_or_else(|| invalid("remote range is misaligned or outside the peer region"))
    }
}

#[allow(clippy::too_many_arguments)]
impl DataChannel for StreamChannel {
    fn write(
        &mut self,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "data channel closed",
            ));
        }
        let key = GrantKey { transfer, index };
        if len == 0 {
            return Err(invalid("zero-length transfer"));
        }
        self.local_range(region, offset, len)?;
        self.remote_range(remote_region, remote_offset, len)?;
        let count = self.shared.register(key, Kind::Write, None, len)?;
        let connections = self.streams.len();
        for piece in 0..count {
            let length = piece_len(len, piece);
            let delta = u64::from(piece) * PIECE_BYTES as u64;
            let Some(source) = offset.checked_add(delta) else {
                self.shared.fail(key, invalid("source offset overflow"));
                return Ok(());
            };
            let Some(target) = remote_offset.checked_add(delta) else {
                self.shared.fail(key, invalid("remote offset overflow"));
                return Ok(());
            };
            let mut frame = Vec::with_capacity(PREFIX + 13);
            frame.extend_from_slice(&prefix(KIND_WRITE, key, piece, count));
            frame.push(remote_region.as_u8());
            frame.extend_from_slice(&target.to_le_bytes());
            frame.extend_from_slice(&(length as u32).to_le_bytes());
            let connection = piece as usize % connections;
            let mut send = |slice: &[u8]| {
                send_frame(&self.shared, &self.streams, connection, key, &frame, slice)
            };
            if let Err(error) = self.regions.with_read(region, source, length, &mut send) {
                self.shared.fail(key, error);
                return Ok(());
            }
        }
        Ok(())
    }

    fn read(
        &mut self,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "data channel closed",
            ));
        }
        let key = GrantKey { transfer, index };
        if len == 0 {
            return Err(invalid("zero-length transfer"));
        }
        self.local_range(region, offset, len)?;
        self.remote_range(remote_region, remote_offset, len)?;
        let count = self
            .shared
            .register(key, Kind::Read, Some((region, offset)), len)?;
        let connections = self.streams.len();
        for piece in 0..count {
            let length = piece_len(len, piece);
            let delta = u64::from(piece) * PIECE_BYTES as u64;
            let Some(destination) = offset.checked_add(delta) else {
                self.shared
                    .fail(key, invalid("destination offset overflow"));
                return Ok(());
            };
            let Some(source) = remote_offset.checked_add(delta) else {
                self.shared.fail(key, invalid("remote offset overflow"));
                return Ok(());
            };
            let mut frame = Vec::with_capacity(PREFIX + 22);
            frame.extend_from_slice(&prefix(KIND_READ, key, piece, count));
            frame.push(remote_region.as_u8());
            frame.extend_from_slice(&source.to_le_bytes());
            frame.push(region.as_u8());
            frame.extend_from_slice(&destination.to_le_bytes());
            frame.extend_from_slice(&(length as u32).to_le_bytes());
            let connection = piece as usize % connections;
            if let Err(error) =
                send_frame(&self.shared, &self.streams, connection, key, &frame, &[])
            {
                self.shared.fail(key, error);
                return Ok(());
            }
        }
        Ok(())
    }

    fn poll(&mut self, done: &mut Vec<(u64, u32, io::Result<()>)>) -> io::Result<()> {
        let mut queue = self.shared.done.lock().unwrap();
        for (key, result) in queue.drain(..) {
            done.push((key.transfer, key.index, result));
        }
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        for stream in &self.streams {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
        let keys: Vec<GrantKey> = self
            .shared
            .pending
            .lock()
            .unwrap()
            .keys()
            .copied()
            .collect();
        for key in keys {
            self.shared.finish(
                key,
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "data channel closed",
                )),
            );
        }
        Ok(())
    }
}

impl Drop for StreamChannel {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn discard(stream: &mut TcpStream, length: usize) -> io::Result<()> {
    let mut buffer = vec![0u8; length.min(PIECE_BYTES)];
    let mut remaining = length;
    while remaining > 0 {
        let take = remaining.min(buffer.len());
        stream.read_exact(&mut buffer[..take])?;
        remaining -= take;
    }
    Ok(())
}

fn reader_loop(
    mut stream: TcpStream,
    shared: Arc<Shared>,
    regions: Arc<dyn RegionAccess>,
    index: usize,
) {
    let mut prefix_bytes = [0u8; PREFIX];
    loop {
        if let Err(error) = stream.read_exact(&mut prefix_bytes) {
            shared.fail_connection(index, error);
            return;
        }
        let parsed = parse_prefix(&prefix_bytes);
        if parsed.pieces == 0 || parsed.piece >= parsed.pieces {
            shared.fail_connection(index, invalid("invalid piece numbering"));
            return;
        }
        match parsed.kind {
            KIND_ACK => shared.ack_piece(parsed.key, parsed.piece, parsed.pieces),
            KIND_ERROR => {
                let mut fields = [0u8; 8];
                if stream.read_exact(&mut fields).is_err() {
                    shared.fail_connection(index, invalid("short error frame"));
                    return;
                }
                let _code = u32::from_le_bytes(fields[..4].try_into().unwrap());
                let text_len = u32::from_le_bytes(fields[4..].try_into().unwrap()) as usize;
                if text_len > MAX_ERROR_TEXT {
                    shared.fail_connection(index, invalid("oversized error frame"));
                    return;
                }
                let mut text = vec![0u8; text_len];
                if text_len > 0 && stream.read_exact(&mut text).is_err() {
                    shared.fail_connection(index, invalid("short error text"));
                    return;
                }
                shared.fail(
                    parsed.key,
                    io::Error::other(format!(
                        "peer data error: {}",
                        String::from_utf8_lossy(&text)
                    )),
                );
            }
            KIND_READ_PAYLOAD => {
                let mut fields = [0u8; 13];
                if stream.read_exact(&mut fields).is_err() {
                    shared.fail_connection(index, invalid("short read payload frame"));
                    return;
                }
                let Some(region) = Region::from_u8(fields[0]) else {
                    shared.fail_connection(index, invalid("unknown read payload region"));
                    return;
                };
                let offset = u64::from_le_bytes(fields[1..9].try_into().unwrap());
                let length = u32::from_le_bytes(fields[9..13].try_into().unwrap()) as usize;
                if length == 0 || length > PIECE_BYTES || !length.is_multiple_of(4096) {
                    shared.fail_connection(index, invalid("invalid read payload length"));
                    return;
                }
                let expected = {
                    let map = shared.pending.lock().unwrap();
                    map.get(&parsed.key)
                        .map(|pending| (pending.destination, pending.len, pending.pieces))
                };
                let Some((Some((dest_region, dest_offset)), len, expected_pieces)) = expected
                else {
                    if discard(&mut stream, length).is_err() {
                        shared.fail_connection(index, invalid("short read payload"));
                        return;
                    }
                    continue;
                };
                let delta = u64::from(parsed.piece) * PIECE_BYTES as u64;
                let Some(want_offset) = dest_offset.checked_add(delta) else {
                    shared.fail_connection(index, invalid("payload offset overflow"));
                    return;
                };
                let want_length = piece_len(len, parsed.piece);
                if expected_pieces != parsed.pieces
                    || region != dest_region
                    || offset != want_offset
                    || length != want_length
                {
                    shared.fail(parsed.key, invalid("read payload outside its destination"));
                    if discard(&mut stream, length).is_err() {
                        shared.fail_connection(index, invalid("short read payload"));
                    }
                    return;
                }
                match shared.begin_piece(parsed.key, parsed.piece, parsed.pieces) {
                    PieceStart::Accepted => {
                        let mut write = |bytes: &mut [u8]| stream.read_exact(bytes);
                        match regions.with_write(region, offset, length, &mut write) {
                            Ok(()) => shared.end_piece(parsed.key, parsed.piece, true),
                            Err(error) => {
                                shared.end_piece(parsed.key, parsed.piece, false);
                                shared.fail(parsed.key, error);
                                return;
                            }
                        }
                    }
                    PieceStart::Duplicate | PieceStart::Reject => {
                        if discard(&mut stream, length).is_err() {
                            shared.fail_connection(index, invalid("short read payload"));
                            return;
                        }
                    }
                }
            }
            _ => {
                shared.fail_connection(index, invalid("unexpected data frame kind"));
                return;
            }
        }
    }
}

fn bump(counters: &Mutex<Counters>, update: impl FnOnce(&mut Counters)) {
    update(&mut counters.lock().unwrap());
}

fn send_error(
    stream: &mut TcpStream,
    key: GrantKey,
    piece: u32,
    pieces: u32,
    code: u32,
    text: &str,
) {
    let mut frame = Vec::with_capacity(PREFIX + 8 + text.len());
    frame.extend_from_slice(&prefix(KIND_ERROR, key, piece, pieces));
    frame.extend_from_slice(&code.to_le_bytes());
    frame.extend_from_slice(&(text.len() as u32).to_le_bytes());
    frame.extend_from_slice(text.as_bytes());
    let _ = stream.write_all(&frame);
}

fn close(stream: &mut TcpStream, _reason: &str, counters: &Mutex<Counters>) {
    bump(counters, |counters| counters.protocol_violations += 1);
    let _ = stream.shutdown(std::net::Shutdown::Both);
}

/// Duplicate tracking for received pieces. A piece is busy while its payload is being written
/// and done only afterwards; duplicates wait for the original write. Windows that stop making
/// progress are retired after [`PARTIAL_TTL`] so an abandoned transfer cannot retain bookkeeping
/// forever.
#[derive(Default)]
struct ReceivedState {
    pieces: Mutex<HashMap<GrantKey, HashMap<u32, u8>>>,
    retired: Mutex<HashSet<GrantKey>>,
    seen: Mutex<HashMap<GrantKey, Instant>>,
    last_prune: Mutex<Option<Instant>>,
    changed: Condvar,
}

/// How long a partly received window may sit without another piece before it is retired.
const PARTIAL_TTL: Duration = Duration::from_secs(30);
const PRUNE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Claim {
    First,
    Duplicate,
    Wait,
}

impl ReceivedState {
    fn claim(&self, key: GrantKey, piece: u32) -> Claim {
        self.maybe_prune();
        if self.retired.lock().unwrap().contains(&key) {
            return Claim::Duplicate;
        }
        let mut map = self.pieces.lock().unwrap();
        let entry = map.entry(key).or_default();
        self.seen.lock().unwrap().insert(key, Instant::now());
        match entry.get(&piece) {
            None => {
                entry.insert(piece, BUSY);
                Claim::First
            }
            Some(&DONE) => Claim::Duplicate,
            Some(_) => Claim::Wait,
        }
    }

    /// Retire windows older than [`PARTIAL_TTL`]. Called from the data path, at most once per
    /// [`PRUNE_INTERVAL`], so no store-side caller is needed to release abandoned windows.
    fn maybe_prune(&self) {
        let now = Instant::now();
        let mut last = self.last_prune.lock().unwrap();
        if last.is_some_and(|at| now.duration_since(at) < PRUNE_INTERVAL) {
            return;
        }
        *last = Some(now);
        drop(last);
        self.prune_expired(now);
    }

    fn prune_expired(&self, now: Instant) {
        let stale: Vec<GrantKey> = {
            let seen = self.seen.lock().unwrap();
            seen.iter()
                .filter(|(_, at)| now.duration_since(**at) >= PARTIAL_TTL)
                .map(|(key, _)| *key)
                .collect()
        };
        for key in stale {
            self.retire(key);
        }
    }

    fn complete(&self, key: GrantKey, piece: u32, ok: bool) {
        let mut map = self.pieces.lock().unwrap();
        if let Some(entry) = map.get_mut(&key) {
            if ok {
                entry.insert(piece, DONE);
            } else {
                entry.remove(&piece);
            }
        }
        drop(map);
        self.changed.notify_all();
    }

    fn retire(&self, key: GrantKey) {
        self.pieces.lock().unwrap().remove(&key);
        self.seen.lock().unwrap().remove(&key);
        let mut retired = self.retired.lock().unwrap();
        if retired.len() >= 8192 {
            // Bound the marker set: a straggler this old belongs to a retired transfer.
            retired.clear();
        }
        retired.insert(key);
        drop(retired);
        self.changed.notify_all();
    }

    fn is_complete(&self, key: GrantKey, pieces: u32) -> bool {
        self.pieces.lock().unwrap().get(&key).is_some_and(|states| {
            states.len() == pieces as usize && states.values().all(|state| *state == DONE)
        })
    }

    fn wait_done(&self, key: GrantKey, piece: u32) -> bool {
        let mut map = self.pieces.lock().unwrap();
        loop {
            match map.get(&key).and_then(|entry| entry.get(&piece)) {
                Some(&DONE) => return true,
                Some(_) => {}
                None => return false,
            }
            let (next, timeout) = self
                .changed
                .wait_timeout(map, Duration::from_secs(5))
                .unwrap();
            map = next;
            if timeout.timed_out() {
                return false;
            }
        }
    }
}

/// The near manager's announced region sizes. Several peers share one listener, so the receiver
/// keeps the largest announcement; the bound protects the peer's own mapping, not ours.
#[derive(Clone)]
pub struct PeerSizes {
    sizes: Arc<Mutex<(u64, u64)>>,
}

impl PeerSizes {
    pub fn new(arena: u64, staging: u64) -> Self {
        Self {
            sizes: Arc::new(Mutex::new((arena, staging))),
        }
    }

    /// Raise the stored sizes; a session may only widen the bound.
    #[allow(dead_code)] // called by the peer service, not by the channel tests
    pub fn widen(&self, arena: u64, staging: u64) {
        let mut sizes = self.sizes.lock().unwrap();
        sizes.0 = sizes.0.max(arena);
        sizes.1 = sizes.1.max(staging);
    }

    fn get(&self, region: Region) -> u64 {
        let sizes = self.sizes.lock().unwrap();
        match region {
            Region::Arena => sizes.0,
            Region::Staging => sizes.1,
        }
    }
}

/// Deadline for a client to present its whole token after connecting: one total budget from
/// accept, not an inactivity timeout, defaulting to the peer connect budget.
pub const TOKEN_READ_TIMEOUT: Duration = crate::peer_client::CONNECT_TIMEOUT;

/// The receiving side of the stream data channel.
pub struct StreamReceiver {
    regions: Arc<dyn RegionAccess>,
    grants: Arc<dyn GrantTable>,
    sessions: Arc<crate::grants::Sessions>,
    peer_sizes: PeerSizes,
    counters: Arc<Mutex<Counters>>,
    received: Arc<ReceivedState>,
    token_timeout: Duration,
}

impl StreamReceiver {
    pub fn new(
        regions: Arc<dyn RegionAccess>,
        grants: Arc<dyn GrantTable>,
        sessions: Arc<crate::grants::Sessions>,
        peer_sizes: PeerSizes,
    ) -> Self {
        Self {
            regions,
            grants,
            sessions,
            peer_sizes,
            counters: Arc::new(Mutex::new(Counters::default())),
            received: Arc::new(ReceivedState::default()),
            token_timeout: TOKEN_READ_TIMEOUT,
        }
    }

    /// Override how long a new connection may take to present its token.
    pub fn with_token_timeout(mut self, timeout: Duration) -> Self {
        self.token_timeout = timeout;
        self
    }

    pub fn counters_handle(&self) -> Arc<Mutex<Counters>> {
        self.counters.clone()
    }

    /// Release duplicate-tracking state once a grant is revoked. Frames for a retired window are
    /// dropped by the receiver without touching the region.
    #[allow(dead_code)] // called by the store when it releases a grant
    pub fn retire(&self, key: GrantKey) {
        self.received.retire(key);
    }

    /// Accept data connections until `stop` is set, handling each on its own thread.
    pub fn serve(
        &self,
        listener: TcpListener,
        stop: Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>> {
        listener.set_nonblocking(true)?;
        let sessions = self.sessions.clone();
        let regions = self.regions.clone();
        let grants = self.grants.clone();
        let peer_sizes = self.peer_sizes.clone();
        let counters = self.counters.clone();
        let received = self.received.clone();
        let token_timeout = self.token_timeout;
        thread::Builder::new()
            .name("selene-data-accept".into())
            .spawn(move || {
                // Every accepted connection is tracked from the accept, so stopping closes it
                // and its worker can be joined instead of lingering.
                let live: Arc<Mutex<std::collections::BTreeMap<u64, TcpStream>>> =
                    Arc::new(Mutex::new(std::collections::BTreeMap::new()));
                let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let mut workers = Vec::new();
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let id = next.fetch_add(1, Ordering::Relaxed);
                            let deadline = std::time::Instant::now() + token_timeout;
                            if let Ok(clone) = stream.try_clone() {
                                live.lock().unwrap().insert(id, clone);
                            }
                            let sessions = sessions.clone();
                            let regions = regions.clone();
                            let grants = grants.clone();
                            let counters = counters.clone();
                            let received = received.clone();
                            let peer_sizes = peer_sizes.clone();
                            let tracked = live.clone();
                            let spawned = thread::Builder::new()
                                .name("selene-data-serve".into())
                                .spawn(move || {
                                    handle(
                                        stream, regions, grants, sessions, peer_sizes, counters,
                                        received, deadline,
                                    );
                                    tracked.lock().unwrap().remove(&id);
                                });
                            if let Ok(worker) = spawned {
                                workers.push(worker);
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                for stream in live.lock().unwrap().values() {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
                for worker in workers {
                    let _ = worker.join();
                }
            })
    }
}

#[allow(clippy::too_many_arguments)]
fn handle(
    mut stream: TcpStream,
    regions: Arc<dyn RegionAccess>,
    grants: Arc<dyn GrantTable>,
    sessions: Arc<crate::grants::Sessions>,
    peer_sizes: PeerSizes,
    counters: Arc<Mutex<Counters>>,
    received: Arc<ReceivedState>,
    deadline: std::time::Instant,
) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_nonblocking(false);
    set_buffers(&stream);
    // One total budget from accept: a fragmented token is fine, a slow or silent one is not.
    let mut received_token = [0u8; TOKEN_BYTES];
    let mut filled = 0;
    while filled < TOKEN_BYTES {
        let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
            return;
        };
        if remaining.is_zero() {
            return;
        }
        let _ = stream.set_read_timeout(Some(remaining));
        match stream.read(&mut received_token[filled..]) {
            Ok(0) => return,
            Ok(count) => filled += count,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.kind() == io::ErrorKind::TimedOut =>
            {
                return;
            }
            Err(_) => return,
        }
    }
    let _ = stream.set_read_timeout(None);
    let Some(session) = sessions.connection(&received_token) else {
        bump(&counters, |counters| counters.bad_tokens += 1);
        return;
    };
    let Some(attachment) = sessions.attach(session, &stream) else {
        return;
    };
    struct SessionGuard {
        sessions: Arc<crate::grants::Sessions>,
        connection: u64,
        attachment: u64,
    }
    impl Drop for SessionGuard {
        fn drop(&mut self) {
            self.sessions.detach(self.connection, self.attachment);
        }
    }
    let _guard = SessionGuard {
        sessions: sessions.clone(),
        connection: session,
        attachment,
    };
    let peer_size = |region: Region| peer_sizes.get(region);
    let mut prefix_bytes = [0u8; PREFIX];
    loop {
        if stream.read_exact(&mut prefix_bytes).is_err() {
            return;
        }
        let parsed = parse_prefix(&prefix_bytes);
        if parsed.pieces == 0 || parsed.piece >= parsed.pieces {
            bump(&counters, |counters| counters.protocol_violations += 1);
            send_error(
                &mut stream,
                parsed.key,
                parsed.piece,
                parsed.pieces,
                CODE_BAD_FRAME,
                "invalid piece numbering",
            );
            let _ = stream.shutdown(std::net::Shutdown::Both);
            return;
        }
        match parsed.kind {
            KIND_WRITE => {
                let mut fields = [0u8; 13];
                if stream.read_exact(&mut fields).is_err() {
                    return;
                }
                let Some(region) = Region::from_u8(fields[0]) else {
                    close(&mut stream, "unknown region", &counters);
                    return;
                };
                let offset = u64::from_le_bytes(fields[1..9].try_into().unwrap());
                let length = u32::from_le_bytes(fields[9..13].try_into().unwrap()) as usize;
                if length == 0 || length > PIECE_BYTES {
                    close(&mut stream, "invalid write length", &counters);
                    return;
                }
                let grant = grants.window(session, parsed.key, true);
                let Some((grant_region, grant_offset, grant_len)) = grant else {
                    // The window ended; drop the frame but keep the stream framed.
                    if discard(&mut stream, length).is_err() {
                        return;
                    }
                    received.retire(parsed.key);
                    bump(&counters, |counters| counters.dropped_frames += 1);
                    continue;
                };
                let delta = u64::from(parsed.piece) * PIECE_BYTES as u64;
                let Some(expected_offset) = grant_offset.checked_add(delta) else {
                    close(&mut stream, "write offset overflow", &counters);
                    return;
                };
                let expected_length = piece_len(grant_len, parsed.piece);
                if region != grant_region
                    || parsed.pieces != pieces(grant_len)
                    || offset != expected_offset
                    || length != expected_length
                    || checked_range(offset, length, regions.size(region)).is_none()
                {
                    send_error(
                        &mut stream,
                        parsed.key,
                        parsed.piece,
                        parsed.pieces,
                        CODE_BAD_RANGE,
                        "write outside the granted window",
                    );
                    close(&mut stream, "write outside the grant", &counters);
                    return;
                }
                match received.claim(parsed.key, parsed.piece) {
                    Claim::First => {
                        let mut write = |bytes: &mut [u8]| stream.read_exact(bytes);
                        match regions.with_write(region, offset, length, &mut write) {
                            Ok(()) => received.complete(parsed.key, parsed.piece, true),
                            Err(error) => {
                                received.complete(parsed.key, parsed.piece, false);
                                send_error(
                                    &mut stream,
                                    parsed.key,
                                    parsed.piece,
                                    parsed.pieces,
                                    CODE_IO,
                                    &error.to_string(),
                                );
                                close(&mut stream, "region write", &counters);
                                return;
                            }
                        }
                        bump(&counters, |counters| {
                            counters.bytes_written += length as u64
                        });
                        let ack = prefix(KIND_ACK, parsed.key, parsed.piece, parsed.pieces);
                        if stream.write_all(&ack).is_err() {
                            return;
                        }
                        if received.is_complete(parsed.key, parsed.pieces) {
                            received.retire(parsed.key);
                        }
                    }
                    Claim::Duplicate => {
                        if discard(&mut stream, length).is_err() {
                            return;
                        }
                        bump(&counters, |counters| counters.duplicates += 1);
                        let ack = prefix(KIND_ACK, parsed.key, parsed.piece, parsed.pieces);
                        if stream.write_all(&ack).is_err() {
                            return;
                        }
                    }
                    Claim::Wait => {
                        if discard(&mut stream, length).is_err() {
                            return;
                        }
                        bump(&counters, |counters| counters.duplicates += 1);
                        if received.wait_done(parsed.key, parsed.piece) {
                            let ack = prefix(KIND_ACK, parsed.key, parsed.piece, parsed.pieces);
                            if stream.write_all(&ack).is_err() {
                                return;
                            }
                        }
                    }
                }
            }
            KIND_READ => {
                let mut fields = [0u8; 22];
                if stream.read_exact(&mut fields).is_err() {
                    return;
                }
                let Some(source_region) = Region::from_u8(fields[0]) else {
                    close(&mut stream, "unknown source region", &counters);
                    return;
                };
                let source_offset = u64::from_le_bytes(fields[1..9].try_into().unwrap());
                let Some(dest_region) = Region::from_u8(fields[9]) else {
                    close(&mut stream, "unknown destination region", &counters);
                    return;
                };
                let dest_offset = u64::from_le_bytes(fields[10..18].try_into().unwrap());
                let length = u32::from_le_bytes(fields[18..22].try_into().unwrap()) as usize;
                if length == 0 || length > PIECE_BYTES {
                    close(&mut stream, "invalid read length", &counters);
                    return;
                }
                let grant = grants.window(session, parsed.key, false);
                let Some((grant_region, grant_offset, grant_len)) = grant else {
                    bump(&counters, |counters| counters.dropped_frames += 1);
                    continue;
                };
                let delta = u64::from(parsed.piece) * PIECE_BYTES as u64;
                let Some(expected_offset) = grant_offset.checked_add(delta) else {
                    close(&mut stream, "read offset overflow", &counters);
                    return;
                };
                let expected_length = piece_len(grant_len, parsed.piece);
                if source_region != grant_region
                    || parsed.pieces != pieces(grant_len)
                    || source_offset != expected_offset
                    || length != expected_length
                    || checked_range(source_offset, length, regions.size(source_region)).is_none()
                    || checked_range(dest_offset, length, peer_size(dest_region)).is_none()
                {
                    send_error(
                        &mut stream,
                        parsed.key,
                        parsed.piece,
                        parsed.pieces,
                        CODE_BAD_RANGE,
                        "read outside the granted window",
                    );
                    close(&mut stream, "read outside the grant", &counters);
                    return;
                }
                let mut frame = Vec::with_capacity(PREFIX + 13);
                frame.extend_from_slice(&prefix(
                    KIND_READ_PAYLOAD,
                    parsed.key,
                    parsed.piece,
                    parsed.pieces,
                ));
                frame.push(dest_region.as_u8());
                frame.extend_from_slice(&dest_offset.to_le_bytes());
                frame.extend_from_slice(&(length as u32).to_le_bytes());
                let mut write = |bytes: &[u8]| {
                    stream.write_all(&frame)?;
                    stream.write_all(bytes)
                };
                match regions.with_read(source_region, source_offset, length, &mut write) {
                    Ok(()) => bump(&counters, |counters| counters.bytes_read += length as u64),
                    Err(error) => {
                        send_error(
                            &mut stream,
                            parsed.key,
                            parsed.piece,
                            parsed.pieces,
                            CODE_IO,
                            &error.to_string(),
                        );
                        close(&mut stream, "region read", &counters);
                        return;
                    }
                }
            }
            _ => {
                close(&mut stream, "unexpected frame kind", &counters);
                return;
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn set_buffers(stream: &TcpStream) {
    use std::os::fd::AsRawFd;
    let value: libc::c_int = SOCKET_BUFFER.min(i32::MAX as u64) as libc::c_int;
    // SAFETY: the descriptor is valid for the stream's lifetime, the pointer refers to a live
    // `c_int`, and the length is the size of that integer.
    unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &value as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &value as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn set_buffers(_stream: &TcpStream) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    struct TestRegions {
        arena: Mutex<Vec<u8>>,
        staging: Mutex<Vec<u8>>,
    }

    impl TestRegions {
        fn new(arena: usize, staging: usize) -> Arc<Self> {
            Arc::new(Self {
                arena: Mutex::new(vec![0u8; arena]),
                staging: Mutex::new(vec![0u8; staging]),
            })
        }

        fn bytes(&self, region: Region) -> Vec<u8> {
            match region {
                Region::Arena => self.arena.lock().unwrap().clone(),
                Region::Staging => self.staging.lock().unwrap().clone(),
            }
        }
    }

    impl RegionAccess for TestRegions {
        fn size(&self, region: Region) -> u64 {
            match region {
                Region::Arena => self.arena.lock().unwrap().len() as u64,
                Region::Staging => self.staging.lock().unwrap().len() as u64,
            }
        }

        fn with_read(
            &self,
            region: Region,
            offset: u64,
            length: usize,
            f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            let target = match region {
                Region::Arena => &self.arena,
                Region::Staging => &self.staging,
            };
            let start = usize::try_from(offset).map_err(|_| invalid("offset"))?;
            let end = start.checked_add(length).ok_or_else(|| invalid("range"))?;
            let guard = target.lock().unwrap();
            if end > guard.len() {
                return Err(invalid("read outside the region"));
            }
            f(&guard[start..end])
        }

        fn with_write(
            &self,
            region: Region,
            offset: u64,
            length: usize,
            f: &mut dyn FnMut(&mut [u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            let target = match region {
                Region::Arena => &self.arena,
                Region::Staging => &self.staging,
            };
            let start = usize::try_from(offset).map_err(|_| invalid("offset"))?;
            let end = start.checked_add(length).ok_or_else(|| invalid("range"))?;
            let mut guard = target.lock().unwrap();
            if end > guard.len() {
                return Err(invalid("write outside the region"));
            }
            f(&mut guard[start..end])
        }
    }

    struct TestGrants {
        windows: Mutex<HashMap<GrantKey, (Region, u64, u64)>>,
    }

    impl TestGrants {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                windows: Mutex::new(HashMap::new()),
            })
        }

        fn grant(&self, key: GrantKey, region: Region, offset: u64, len: u64) {
            self.windows
                .lock()
                .unwrap()
                .insert(key, (region, offset, len));
        }
    }

    impl GrantTable for TestGrants {
        fn window(&self, _session: u64, key: GrantKey, _put: bool) -> Option<(Region, u64, u64)> {
            self.windows.lock().unwrap().get(&key).copied()
        }
    }

    struct Fixture {
        address: SocketAddr,
        server_regions: Arc<TestRegions>,
        grants: Arc<TestGrants>,
        counters: Arc<Mutex<Counters>>,
        stop: Arc<AtomicBool>,
        accept: JoinHandle<()>,
    }

    fn fixture(token: [u8; TOKEN_BYTES]) -> Fixture {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_regions = TestRegions::new(8 << 20, 4 << 20);
        let grants = TestGrants::new();
        let sessions = Arc::new(crate::grants::Sessions::default());
        sessions.register(token, 1);
        let receiver = StreamReceiver::new(
            server_regions.clone(),
            grants.clone(),
            sessions,
            PeerSizes::new(8 << 20, 4 << 20),
        );
        let counters = receiver.counters_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let accept = receiver.serve(listener, stop.clone()).unwrap();
        Fixture {
            address,
            server_regions,
            grants,
            counters,
            stop,
            accept,
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            let _ = TcpStream::connect(self.address);
            let _ = std::mem::replace(&mut self.accept, thread::spawn(|| {})).join();
        }
    }

    impl Fixture {
        fn counters(&self) -> Counters {
            *self.counters.lock().unwrap()
        }

        fn wait(&self, check: impl Fn(&Counters) -> bool) -> Counters {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                let counters = self.counters();
                if check(&counters) || Instant::now() > deadline {
                    return counters;
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    fn wait(channel: &mut StreamChannel, key: GrantKey) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut done = Vec::new();
            channel.poll(&mut done)?;
            for (transfer, index, result) in done {
                if transfer == key.transfer && index == key.index {
                    return result;
                }
            }
            if Instant::now() > deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "data transfer"));
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    const TOKEN: [u8; TOKEN_BYTES] = [7u8; TOKEN_BYTES];

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_round_trip_over_three_connections() {
        let fixture = fixture(TOKEN);
        let regions = TestRegions::new(8 << 20, 4 << 20);
        let source: Vec<u8> = (0..(2 * PIECE_BYTES + 4096))
            .map(|index| (index % 251) as u8)
            .collect();
        regions.arena.lock().unwrap()[..source.len()].copy_from_slice(&source);
        let key = GrantKey {
            transfer: 11,
            index: 3,
        };
        fixture
            .grants
            .grant(key, Region::Arena, 0, source.len() as u64);
        let mut channel = StreamChannel::connect_async(
            &[fixture.address; 3],
            &TOKEN,
            regions,
            (8 << 20, 4 << 20),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        channel
            .write(
                11,
                3,
                Region::Arena,
                0,
                source.len() as u64,
                Region::Arena,
                0,
            )
            .unwrap();
        wait(&mut channel, key).unwrap();
        let got = fixture.server_regions.bytes(Region::Arena);
        if let Some(index) = got[..source.len()]
            .iter()
            .zip(source.iter())
            .position(|(got, want)| got != want)
        {
            let start = index.saturating_sub(8);
            let end = (index + 8).min(got.len());
            panic!(
                "mismatch at {index}: got {} want {}; region {:?} source {:?}",
                got[index],
                source[index],
                &got[start..end],
                &source[start..end]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_round_trip_writes_the_destination() {
        let fixture = fixture(TOKEN);
        let key = GrantKey {
            transfer: 12,
            index: 0,
        };
        let len = 2 * PIECE_BYTES + 8192;
        let expected = {
            let mut source = fixture.server_regions.arena.lock().unwrap();
            for (index, byte) in source[..len].iter_mut().enumerate() {
                *byte = (index % 239) as u8;
            }
            source[..len].to_vec()
        };
        fixture.grants.grant(key, Region::Arena, 0, len as u64);
        let regions = TestRegions::new(8 << 20, 4 << 20);
        let mut channel = StreamChannel::connect_async(
            &[fixture.address; 2],
            &TOKEN,
            regions.clone(),
            (8 << 20, 4 << 20),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        channel
            .read(12, 0, Region::Arena, 0, len as u64, Region::Arena, 0)
            .unwrap();
        wait(&mut channel, key).unwrap();
        assert!(
            regions.bytes(Region::Arena)[..len] == expected[..],
            "read bytes differ"
        );
    }

    fn raw(token: [u8; TOKEN_BYTES], address: SocketAddr) -> TcpStream {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(&token).unwrap();
        stream
    }

    fn write_frame(
        key: GrantKey,
        piece: u32,
        pieces: u32,
        region: Region,
        offset: u64,
        len: u32,
    ) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&prefix(KIND_WRITE, key, piece, pieces));
        frame.push(region.as_u8());
        frame.extend_from_slice(&offset.to_le_bytes());
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&vec![9u8; len as usize]);
        frame
    }

    #[test]
    fn an_unknown_transfer_is_dropped_and_the_connection_stays_up() {
        let fixture = fixture(TOKEN);
        let mut stream = raw(TOKEN, fixture.address);
        let unknown = GrantKey {
            transfer: 99,
            index: 0,
        };
        stream
            .write_all(&write_frame(unknown, 0, 1, Region::Arena, 0, 4096))
            .unwrap();
        let key = GrantKey {
            transfer: 13,
            index: 0,
        };
        fixture.grants.grant(key, Region::Arena, 0, 4096);
        stream
            .write_all(&write_frame(key, 0, 1, Region::Arena, 0, 4096))
            .unwrap();
        let mut ack = [0u8; PREFIX];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack[0], KIND_ACK);
        assert_eq!(fixture.counters().dropped_frames, 1);
    }

    #[test]
    fn a_write_outside_the_grant_closes_the_connection() {
        let fixture = fixture(TOKEN);
        let mut stream = raw(TOKEN, fixture.address);
        let key = GrantKey {
            transfer: 14,
            index: 0,
        };
        fixture.grants.grant(key, Region::Arena, 0, 4096);
        stream
            .write_all(&write_frame(key, 0, 1, Region::Arena, 4096, 4096))
            .unwrap();
        let mut byte = [0u8; 1];
        let _ = stream.read_exact(&mut byte);
        assert_eq!(
            fixture
                .wait(|c| c.protocol_violations > 0)
                .protocol_violations,
            1
        );
    }

    #[test]
    fn a_mismatched_piece_count_is_rejected() {
        let fixture = fixture(TOKEN);
        let mut stream = raw(TOKEN, fixture.address);
        let key = GrantKey {
            transfer: 15,
            index: 0,
        };
        fixture.grants.grant(key, Region::Arena, 0, 4096);
        stream
            .write_all(&write_frame(key, 0, 2, Region::Arena, 0, 4096))
            .unwrap();
        let mut byte = [0u8; 1];
        let _ = stream.read_exact(&mut byte);
        assert_eq!(
            fixture
                .wait(|c| c.protocol_violations > 0)
                .protocol_violations,
            1
        );
    }

    #[test]
    fn a_duplicate_piece_is_dropped_and_counted() {
        let fixture = fixture(TOKEN);
        let mut stream = raw(TOKEN, fixture.address);
        let key = GrantKey {
            transfer: 16,
            index: 0,
        };
        fixture.grants.grant(key, Region::Arena, 0, 4096);
        let frame = write_frame(key, 0, 1, Region::Arena, 0, 4096);
        stream.write_all(&frame).unwrap();
        let mut ack = [0u8; PREFIX];
        stream.read_exact(&mut ack).unwrap();
        stream.write_all(&frame).unwrap();
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(fixture.wait(|c| c.duplicates > 0).duplicates, 1);
        assert_eq!(
            &fixture.server_regions.bytes(Region::Arena)[..4096],
            &[9u8; 4096][..]
        );
    }

    #[test]
    fn a_read_destination_beyond_the_announced_size_is_rejected() {
        let fixture = fixture(TOKEN);
        let mut stream = raw(TOKEN, fixture.address);
        let key = GrantKey {
            transfer: 17,
            index: 0,
        };
        fixture.grants.grant(key, Region::Arena, 0, 4096);
        let mut frame = Vec::new();
        frame.extend_from_slice(&prefix(KIND_READ, key, 0, 1));
        frame.push(Region::Arena.as_u8());
        frame.extend_from_slice(&0u64.to_le_bytes());
        frame.push(Region::Arena.as_u8());
        frame.extend_from_slice(&(8u64 << 20).to_le_bytes());
        frame.extend_from_slice(&4096u32.to_le_bytes());
        stream.write_all(&frame).unwrap();
        let mut byte = [0u8; 1];
        let _ = stream.read_exact(&mut byte);
        assert_eq!(
            fixture
                .wait(|c| c.protocol_violations > 0)
                .protocol_violations,
            1
        );
    }

    #[test]
    fn a_wrong_token_is_rejected() {
        let fixture = fixture(TOKEN);
        let mut stream = raw([0u8; TOKEN_BYTES], fixture.address);
        let _ = stream.write_all(&write_frame(
            GrantKey {
                transfer: 1,
                index: 0,
            },
            0,
            1,
            Region::Arena,
            0,
            4096,
        ));
        let mut byte = [0u8; 1];
        assert!(stream.read_exact(&mut byte).is_err());
        assert_eq!(fixture.counters().bad_tokens, 1);
    }

    #[test]
    fn a_failed_write_shuts_down_every_socket_of_the_transfer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let first = TcpStream::connect(address).unwrap();
        let (server, _) = listener.accept().unwrap();
        let second = TcpStream::connect(address).unwrap();
        let (server_two, _) = listener.accept().unwrap();
        let shared = Shared::new(&[first, second]).unwrap();
        let key = GrantKey {
            transfer: 21,
            index: 0,
        };
        let pieces = shared
            .register(key, Kind::Write, None, 2 * PIECE_BYTES as u64)
            .unwrap();
        assert!(shared.use_connection(key, 0));
        assert!(shared.use_connection(key, 1));
        assert_eq!(pieces, 2);
        shared.fail(key, invalid("test failure"));
        let done = shared.done.lock().unwrap();
        assert_eq!(done.len(), 1);
        assert!(done[0].1.is_err());
        drop(done);
        assert!(shared.closed[0].load(Ordering::Acquire));
        assert!(shared.closed[1].load(Ordering::Acquire));
        let _ = (server, server_two);
    }

    #[test]
    fn an_ack_never_completes_a_read() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let first = TcpStream::connect(address).unwrap();
        let (_server, _) = listener.accept().unwrap();
        let shared = Shared::new(&[first]).unwrap();
        let key = GrantKey {
            transfer: 22,
            index: 0,
        };
        shared
            .register(key, Kind::Read, Some((Region::Arena, 0)), 4096)
            .unwrap();
        shared.ack_piece(key, 0, 1);
        assert!(shared.done.lock().unwrap().is_empty());
        assert!(shared.pending.lock().unwrap().contains_key(&key));
    }

    #[test]
    fn a_duplicate_piece_waits_for_the_original_write() {
        let state = Arc::new(ReceivedState::default());
        let key = GrantKey {
            transfer: 31,
            index: 0,
        };
        assert_eq!(state.claim(key, 0), Claim::First);
        assert_eq!(state.claim(key, 0), Claim::Wait);
        let waiter = {
            let state = state.clone();
            thread::spawn(move || state.wait_done(key, 0))
        };
        thread::sleep(Duration::from_millis(10));
        state.complete(key, 0, true);
        assert!(waiter.join().unwrap());
        assert_eq!(state.claim(key, 0), Claim::Duplicate);
        state.retire(key);
        assert_eq!(state.claim(key, 0), Claim::Duplicate);
        assert_eq!(
            state.claim(
                GrantKey {
                    transfer: 32,
                    index: 0
                },
                0
            ),
            Claim::First
        );
    }

    #[test]
    fn a_completed_window_is_retired_and_late_frames_are_dropped() {
        let state = ReceivedState::default();
        let key = GrantKey {
            transfer: 33,
            index: 0,
        };
        assert_eq!(state.claim(key, 0), Claim::First);
        state.complete(key, 0, true);
        assert!(state.is_complete(key, 1));
        state.retire(key);
        assert_eq!(state.claim(key, 0), Claim::Duplicate);
        assert!(!state.retired.lock().unwrap().is_empty());
    }

    #[test]
    fn abandoned_partial_windows_are_retired_by_age() {
        let state = ReceivedState::default();
        let abandoned = GrantKey {
            transfer: 34,
            index: 0,
        };
        assert_eq!(state.claim(abandoned, 0), Claim::First);
        assert!(state.pieces.lock().unwrap().contains_key(&abandoned));
        state.prune_expired(Instant::now() + PARTIAL_TTL + Duration::from_secs(1));
        assert!(!state.pieces.lock().unwrap().contains_key(&abandoned));
        assert!(!state.seen.lock().unwrap().contains_key(&abandoned));
        // A straggler for the abandoned window is dropped rather than restarting it.
        assert_eq!(state.claim(abandoned, 0), Claim::Duplicate);
        let fresh = GrantKey {
            transfer: 35,
            index: 0,
        };
        assert_eq!(state.claim(fresh, 0), Claim::First);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closing_the_channel_fails_outstanding_windows() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_millis(100));
            drop(stream);
        });
        let mut channel = StreamChannel::connect_async(
            &[address],
            &TOKEN,
            TestRegions::new(8 << 20, 4 << 20),
            (8 << 20, 4 << 20),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        channel
            .write(32, 0, Region::Arena, 0, 4096, Region::Arena, 0)
            .unwrap();
        channel.close().unwrap();
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert!(
            done.iter()
                .any(|(transfer, index, result)| *transfer == 32 && *index == 0 && result.is_err())
        );
        server.join().unwrap();
    }

    #[test]
    fn duplicate_read_payloads_are_not_written_twice() {
        let shared = Shared::new(&[]).unwrap();
        let key = GrantKey {
            transfer: 23,
            index: 0,
        };
        shared
            .register(
                key,
                Kind::Read,
                Some((Region::Arena, 0)),
                2 * PIECE_BYTES as u64,
            )
            .unwrap();
        assert_eq!(shared.begin_piece(key, 0, 2), PieceStart::Accepted);
        shared.end_piece(key, 0, true);
        assert_eq!(shared.begin_piece(key, 0, 2), PieceStart::Duplicate);
        assert_eq!(shared.begin_piece(key, 2, 2), PieceStart::Reject);
        assert!(shared.done.lock().unwrap().is_empty());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_data_connect_is_bounded_by_its_deadline() {
        // TEST-NET-1 does not answer; the deadline must end the attempt.
        let started = Instant::now();
        let error = StreamChannel::connect_async(
            &["192.0.2.1:9".parse().unwrap()],
            &[0u8; TOKEN_BYTES],
            TestRegions::new(4096, 4096),
            (4096, 4096),
            tokio::time::Instant::now() + Duration::from_millis(200),
        )
        .await
        .err()
        .expect("an unreachable data address must not connect");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the connect was bounded: {error}"
        );
    }

    #[test]
    fn a_fragmented_token_within_the_deadline_is_accepted() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let receiver = StreamReceiver::new(
            TestRegions::new(8 << 20, 4 << 20),
            TestGrants::new(),
            Arc::new(crate::grants::Sessions::default()),
            PeerSizes::new(8 << 20, 4 << 20),
        )
        .with_token_timeout(Duration::from_millis(600));
        let counters = receiver.counters_handle();
        let stop = Arc::new(AtomicBool::new(false));
        let accept = receiver.serve(listener, stop.clone()).unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        client.write_all(&[3u8; 8]).unwrap();
        thread::sleep(Duration::from_millis(200));
        client.write_all(&[3u8; TOKEN_BYTES - 8]).unwrap();
        // The whole token arrives inside the total budget, so it is read and rejected as an
        // unknown token instead of timing out mid-token.
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if counters.lock().unwrap().bad_tokens == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the fragmented token was not read"
            );
            thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(address);
        let _ = accept.join();
    }

    #[test]
    fn a_client_that_never_sends_its_token_is_dropped() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let sessions = Arc::new(crate::grants::Sessions::default());
        sessions.register([7u8; TOKEN_BYTES], 1);
        let receiver = StreamReceiver::new(
            TestRegions::new(8 << 20, 4 << 20),
            TestGrants::new(),
            sessions,
            PeerSizes::new(8 << 20, 4 << 20),
        )
        .with_token_timeout(Duration::from_millis(200));
        let stop = Arc::new(AtomicBool::new(false));
        let accept = receiver.serve(listener, stop.clone()).unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let started = Instant::now();
        let mut byte = [0u8; 1];
        // The server drops the connection once the token deadline passes.
        let closed = matches!(client.read(&mut byte), Ok(0) | Err(_));
        assert!(closed, "the stalled client is dropped");
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert!(started.elapsed() < Duration::from_secs(5));
        stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(address);
        let _ = accept.join();
    }
}
