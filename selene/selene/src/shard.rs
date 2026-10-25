//! Framed byte links shared by the rank record relay and pool protocol.
use crate::{
    Error, PrefixKey, Result,
    geometry::invalid,
    record::{self, HEADER_BYTES, Header},
};
use std::{
    io::{ErrorKind, Read, Write},
    net::{Shutdown, TcpStream},
    sync::{
        Arc, Condvar, Mutex,
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
pub const IDLE: Duration = Duration::from_millis(1);
pub const BUSY: Duration = Duration::from_micros(100);
pub const BACKLOG: usize = 64;
const FRAME_LIMIT: usize = 64 << 20;
const REPLY_TIMEOUT: Duration = Duration::from_secs(120);
// ---------------------------------------------------------------------------------------------
// Framing

pub struct Writer(Vec<u8>);

impl Writer {
    pub fn new(tag: u8) -> Self {
        Self(vec![tag, 0, 0, 0, 0])
    }

    pub fn u8(&mut self, value: u8) -> &mut Self {
        self.0.push(value);
        self
    }

    pub fn u32(&mut self, value: u32) -> &mut Self {
        self.0.extend(value.to_le_bytes());
        self
    }

    pub fn u64(&mut self, value: u64) -> &mut Self {
        self.0.extend(value.to_le_bytes());
        self
    }

    pub fn bytes(&mut self, value: &[u8]) -> &mut Self {
        self.0.extend(value);
        self
    }

    pub fn text(&mut self, value: &str) -> &mut Self {
        self.u32(value.len() as u32).bytes(value.as_bytes())
    }

    pub fn key(&mut self, key: &PrefixKey) -> &mut Self {
        self.u64(key.consumed_tokens).bytes(&key.digest)
    }

    pub fn header(&mut self, header: &Header) -> Result<&mut Self> {
        Ok(self.bytes(&header.encode()?))
    }

    pub fn finish(mut self) -> Vec<u8> {
        let length = (self.0.len() - 5) as u32;
        self.0[1..5].copy_from_slice(&length.to_le_bytes());
        self.0
    }
}

pub struct Reader<'a>(pub &'a [u8]);

impl<'a> Reader<'a> {
    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        if self.0.len() < count {
            return Err(invalid("truncated prefix record message"));
        }
        let (head, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(head)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn text(&mut self) -> Result<&'a str> {
        let length = self.u32()? as usize;
        std::str::from_utf8(self.take(length)?)
            .map_err(|_| invalid("prefix record message text is not UTF-8"))
    }

    pub fn key(&mut self) -> Result<PrefixKey> {
        Ok(PrefixKey {
            consumed_tokens: self.u64()?,
            digest: self.take(16)?.try_into().unwrap(),
        })
    }

    pub fn header(&mut self) -> Result<Header> {
        Header::decode(self.take(HEADER_BYTES)?)
    }

    /// A count of items that each occupy at least `item` bytes of what remains.
    pub fn count(&mut self, item: usize) -> Result<usize> {
        let count = self.u32()? as usize;
        if count.saturating_mul(item) > self.0.len() {
            return Err(invalid("prefix record message count exceeds its payload"));
        }
        Ok(count)
    }

    pub fn end(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid("prefix record message has trailing bytes"))
        }
    }
}

/// Writes queued frames on one connection and records how many are still queued.
#[derive(Default)]
struct Outgoing {
    state: Mutex<OutgoingState>,
    changed: Condvar,
}

#[derive(Default)]
struct OutgoingState {
    queued: usize,
    failure: Option<String>,
}

impl Outgoing {
    fn lock(&self) -> std::sync::MutexGuard<'_, OutgoingState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One end of a record connection.
///
/// Reads never block. Writes go through a writer thread, so the model worker
/// thread only enqueues a frame; a peer that stops reading fills the queue and
/// fails the job instead of stalling the worker.
pub struct Link {
    pub(crate) stream: TcpStream,
    outgoing: Option<SyncSender<Vec<u8>>>,
    writer: Option<JoinHandle<()>>,
    queued: Arc<Outgoing>,
    pub(crate) received: Vec<u8>,
    /// Set once the connection failed or the other end closed it.
    broken: Option<Error>,
}

impl Link {
    pub fn new(stream: TcpStream) -> Result<Self> {
        stream
            .set_nodelay(true)
            .and_then(|()| stream.set_nonblocking(true))
            .map_err(|error| record::io_failure("configure a prefix record connection", error))?;
        let writer_stream = stream
            .try_clone()
            .map_err(|error| record::io_failure("duplicate a prefix record connection", error))?;
        let (sender, jobs) = sync_channel(BACKLOG);
        let queued = Arc::new(Outgoing::default());
        let progress = queued.clone();
        let writer = std::thread::Builder::new()
            .name("flare-offload-link".into())
            .spawn(move || write_frames(writer_stream, jobs, progress))
            .map_err(|error| invalid(format!("cannot start a prefix record writer: {error}")))?;
        Ok(Self {
            stream,
            outgoing: Some(sender),
            writer: Some(writer),
            queued,
            received: Vec::new(),
            broken: None,
        })
    }

    pub fn fail(&mut self, error: Error) -> Error {
        let error = self.broken.get_or_insert(error).clone();
        // The peer must learn that this rank stopped driving it, so its own
        // transfers drain instead of running on with no one to report to.
        let _ = self.stream.shutdown(Shutdown::Both);
        error
    }

    /// Queue a frame without waiting. Fails when the peer is not keeping up.
    pub fn enqueue(&mut self, frame: &[u8]) -> Result<()> {
        if let Some(error) = &self.broken {
            return Err(error.clone());
        }
        if frame.len() > FRAME_LIMIT {
            return Err(self.fail(invalid("prefix record frame exceeds its limit")));
        }
        let mut state = self.queued.lock();
        if let Some(failure) = &state.failure {
            let error = invalid(failure.clone());
            drop(state);
            return Err(self.fail(error));
        }
        state.queued += 1;
        drop(state);
        let sender = self
            .outgoing
            .as_ref()
            .ok_or_else(|| invalid("prefix record connection is closed"))?;
        match sender.try_send(frame.to_vec()) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.queued.lock().queued -= 1;
                Err(self.fail(invalid(
                    "prefix record connection backlog is full; the peer is not keeping up",
                )))
            }
            Err(TrySendError::Disconnected(_)) => {
                self.queued.lock().queued -= 1;
                Err(self.fail(invalid("prefix record connection closed")))
            }
        }
    }

    /// Queue a frame and wait until the writer has sent every queued frame.
    pub fn send(&mut self, frame: &[u8]) -> Result<()> {
        self.enqueue(frame)?;
        self.drain_outgoing(REPLY_TIMEOUT)
    }

    /// Wait until nothing is queued, the writer failed, or `timeout` passes.
    pub fn drain_outgoing(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut state = self.queued.lock();
        loop {
            if let Some(failure) = &state.failure {
                let error = invalid(failure.clone());
                drop(state);
                return Err(self.fail(error));
            }
            if state.queued == 0 {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                drop(state);
                return Err(self.fail(invalid("prefix record connection stalled")));
            }
            state = self
                .queued
                .changed
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
        self.outgoing.take();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

/// Write queued frames in order. A failure is recorded for the owner and the
/// connection is shut down so the peer stops.
fn write_frames(mut stream: TcpStream, jobs: Receiver<Vec<u8>>, progress: Arc<Outgoing>) {
    for frame in jobs {
        let deadline = Instant::now() + REPLY_TIMEOUT;
        let mut sent = 0;
        let mut failure = None;
        while sent < frame.len() {
            match stream.write(&frame[sent..]) {
                Ok(0) => {
                    failure = Some("prefix record connection closed".to_owned());
                    break;
                }
                Ok(count) => sent += count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        failure = Some("prefix record connection stalled".to_owned());
                        break;
                    }
                    std::thread::sleep(BUSY);
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => {
                    failure = Some(format!("send on a prefix record connection: {error}"));
                    break;
                }
            }
        }
        let mut state = progress.lock();
        state.queued -= 1;
        if failure.is_some() {
            state.failure = failure;
        }
        let failed = state.failure.is_some();
        drop(state);
        progress.changed.notify_all();
        if failed {
            let _ = stream.shutdown(Shutdown::Both);
            return;
        }
    }
}

impl Link {
    /// The next complete frame, if one has arrived.
    pub fn receive(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        if let Some(error) = &self.broken {
            return Err(error.clone());
        }
        receive_frame(&mut self.stream, &mut self.received).map_err(|error| self.fail(error))
    }

    /// Wait up to `timeout` for a frame tagged `tag`.
    pub fn expect(&mut self, tag: u8, timeout: Duration) -> Result<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some((received, payload)) = self.receive()? {
                if received != tag {
                    return Err(self.fail(invalid("unexpected prefix record message")));
                }
                return Ok(payload);
            }
            if Instant::now() >= deadline {
                return Err(self.fail(invalid("prefix record peer did not answer in time")));
            }
            std::thread::sleep(IDLE);
        }
    }
}

pub(crate) fn receive_frame(
    stream: &mut TcpStream,
    received: &mut Vec<u8>,
) -> Result<Option<(u8, Vec<u8>)>> {
    let mut buffer = [0u8; 64 << 10];
    loop {
        if received.len() >= 5 {
            let length = u32::from_le_bytes(received[1..5].try_into().unwrap()) as usize;
            if length > FRAME_LIMIT {
                return Err(invalid("prefix record frame exceeds its limit"));
            }
            if received.len() >= 5 + length {
                let tag = received[0];
                let payload = received[5..5 + length].to_vec();
                received.drain(..5 + length);
                return Ok(Some((tag, payload)));
            }
        }
        match stream.read(&mut buffer) {
            Ok(0) => return Err(invalid("prefix record connection closed")),
            Ok(count) => received.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(record::io_failure(
                    "receive on a prefix record connection",
                    error,
                ));
            }
        }
    }
}
