//! The data channel instances on other hosts copy through.
//!
//! An instance without shared memory cannot map the manager's windows, so the
//! manager moves their bytes itself, over one connection per transfer. A claim's
//! windows (filled by the store, in the manager's own regions) are written to the
//! instance in order; a put's windows (granted by the store) are filled from what
//! the instance writes. The framing is `flare_selene::pool::frame`.
//!
//! The pool actor owns every decision. It opens a transfer and gets its token,
//! queues windows or grants as the store produces them, and stops the transfer
//! when the claim or put ends. A handler task serves the transfer's connection
//! and reports what it did through [`Event`]. Physical fencing is the handler's
//! exit: `Closed` is sent only after the connection has been dropped, so no byte
//! of a window is read or written afterwards. The actor must not release a
//! window to the store before `Closed` (or before `stop` answered that no handler
//! ever ran).

use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use flare_selene::pool::{
    SliceLayout,
    frame::{Carrier, NO_SLOT, Shape, TOKEN_BYTES, ToInstance, ToManager, Window},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::{Notify, mpsc, watch},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

use super::{DataChannel, Region, RegionAccess, invalid};
use crate::arena::{Arena, Extent};

/// A connection that has not presented its token by then is dropped.
const TOKEN_TIMEOUT: Duration = Duration::from_secs(5);
/// A write or a read that makes no progress for this long ends the transfer.
const IO_TIMEOUT: Duration = Duration::from_secs(20);
/// The most a closing connection may take to say goodbye.
const GOODBYE: Duration = Duration::from_millis(200);
const SOCKET_BUFFER: u64 = 16 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// The manager sends windows of a claim.
    Get,
    /// The manager receives windows of a put.
    Put,
}

/// A claim's window, filled by the store, with the checksums stored with its
/// record. The window lies in one of the manager's regions.
#[derive(Clone, Debug)]
pub struct WindowOut {
    pub shape: Shape,
    pub page_xxh3: Vec<u64>,
    pub row_xxh3: u64,
    pub region: Region,
    pub offset: u64,
}

/// A put's window, granted by the store for the instance to fill.
#[derive(Clone, Copy, Debug)]
pub struct GrantOut {
    pub shape: Shape,
    pub region: Region,
    pub offset: u64,
}

/// What a handler reports. `Closed` is always last.
#[derive(Debug)]
pub enum Event {
    /// A claim's window is in the socket; the window may be released.
    Sent { transfer: u64, index: u32 },
    /// A put's window is filled, with the checksums the instance stated.
    Filled {
        transfer: u64,
        index: u32,
        row_xxh3: Option<u64>,
        page_xxh3: Vec<u64>,
    },
    /// The handler ended the transfer on an error.
    Failed { transfer: u64, reason: String },
    /// The handler has exited and its connection is closed.
    Closed { transfer: u64 },
}

struct Queues {
    windows: VecDeque<WindowOut>,
    grants: VecDeque<GrantOut>,
    finished: bool,
    attached: bool,
}

struct Transfer {
    kind: Kind,
    /// A leg of a multi-process instance's rank: the transfer carries only that rank's
    /// slice of each window (`Shape::length` is the slice's length), gathered out of the
    /// window before it is sent and scattered into it after it is received.
    slicing: Option<(Arc<SliceLayout>, usize)>,
    token: [u8; TOKEN_BYTES],
    queues: Mutex<Queues>,
    wake: Notify,
    stopped: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    tokens: HashMap<[u8; TOKEN_BYTES], u64>,
    transfers: HashMap<u64, Arc<Transfer>>,
    open: bool,
}

/// The manager's RDMA device. A transfer whose instance asks for RDMA gets a queue pair
/// from it; the pair's description goes to the instance and the instance's comes back.
pub trait Rdma: Send + Sync {
    fn pair(&self) -> io::Result<Box<dyn Pair>>;
}

/// A queue pair made for one transfer, not yet connected.
pub trait Pair: Send {
    /// This end's description for the peer.
    fn handshake(&self) -> Vec<u8>;
    /// Bring the pair up against the peer's description. The channel it returns moves bytes
    /// between the manager's regions and the peer's staging; dropping it moves the pair to
    /// the error state and destroys it.
    fn connect(self: Box<Self>, peer: &[u8]) -> io::Result<Box<dyn DataChannel>>;
}

/// Bytes and windows moved, by carrier, for the metrics.
#[derive(Default)]
struct Counters {
    windows: [AtomicU64; 2],
    bytes: [AtomicU64; 2],
}

impl Counters {
    fn add(&self, carrier: usize, bytes: u64) {
        self.windows[carrier].fetch_add(1, Ordering::Relaxed);
        self.bytes[carrier].fetch_add(bytes, Ordering::Relaxed);
    }
}

/// What the hub has moved: `[stream, rdma]`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Moved {
    pub windows: [u64; 2],
    pub bytes: [u64; 2],
}

struct Inner {
    rdma: Option<Arc<dyn Rdma>>,
    counters: Counters,
    address: String,
    regions: Arc<dyn RegionAccess>,
    acceptor: Option<TlsAcceptor>,
    max_pages: u32,
    events: mpsc::UnboundedSender<Event>,
    state: Mutex<State>,
    next: AtomicU64,
    handle: Handle,
    /// The registered region a rank's leg gathers its packed slices in before they cross the
    /// network, and scatters them from after.
    packed: Mutex<Option<Arena>>,
    /// Tests hold back `Closed` to play a handler whose exit cannot be observed.
    #[cfg(test)]
    closed_delay: Mutex<Duration>,
}

/// The listener's state and the transfers it serves. Cheap to clone.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
    closing: watch::Sender<bool>,
}

fn random_token() -> io::Result<[u8; TOKEN_BYTES]> {
    use std::io::Read;
    let mut token = [0u8; TOKEN_BYTES];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut token)?;
    Ok(token)
}

impl Hub {
    /// Serve the data channel on `listener`. `max_pages` bounds the checksum
    /// table a window may announce. Without `tls` the connection is plaintext.
    pub fn start(
        handle: &Handle,
        listener: std::net::TcpListener,
        regions: Arc<dyn RegionAccess>,
        tls: Option<Arc<rustls::ServerConfig>>,
        max_pages: u32,
        rdma: Option<Arc<dyn Rdma>>,
    ) -> io::Result<(Hub, mpsc::UnboundedReceiver<Event>)> {
        if rdma.is_some() && tls.is_some() {
            return Err(invalid(
                "RDMA transfers are not encrypted, so they cannot be offered with TLS",
            ));
        }
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let (events, receiver) = mpsc::unbounded_channel();
        let (closing, closed) = watch::channel(false);
        let inner = Arc::new(Inner {
            rdma,
            counters: Counters::default(),
            address,
            regions,
            acceptor: tls.map(TlsAcceptor::from),
            max_pages,
            events,
            state: Mutex::new(State {
                open: true,
                ..State::default()
            }),
            next: AtomicU64::new(1),
            handle: handle.clone(),
            packed: Mutex::new(None),
            #[cfg(test)]
            closed_delay: Mutex::new(Duration::ZERO),
        });
        let accepting = inner.clone();
        let listener = {
            let _guard = handle.enter();
            TcpListener::from_std(listener)?
        };
        handle.spawn(accept_loop(accepting, listener, closed));
        Ok((Hub { inner, closing }, receiver))
    }

    /// Give rank legs on RDMA the registered arena to pack their slices in. Without one, a
    /// rank's leg that asks for RDMA is refused and uses the stream.
    pub fn set_packed(&self, arena: Arena) {
        *self.inner.packed.lock().unwrap() = Some(arena);
    }

    /// The `host:port` clients are told to connect to. A host that is the
    /// unspecified address means "the host you reached me by".
    pub fn address(&self) -> &str {
        &self.inner.address
    }

    /// Whether instances may ask for the RDMA carrier.
    pub fn offers_rdma(&self) -> bool {
        self.inner.rdma.is_some()
    }

    /// What has been moved so far, by carrier.
    pub fn moved(&self) -> Moved {
        let load = |counters: &[AtomicU64; 2]| {
            [
                counters[0].load(Ordering::Relaxed),
                counters[1].load(Ordering::Relaxed),
            ]
        };
        Moved {
            windows: load(&self.inner.counters.windows),
            bytes: load(&self.inner.counters.bytes),
        }
    }

    /// Whether connections are encrypted.
    pub fn encrypted(&self) -> bool {
        self.inner.acceptor.is_some()
    }

    /// Begin a transfer. Its token authenticates the one connection that serves it.
    pub fn open(&self, kind: Kind) -> io::Result<(u64, [u8; TOKEN_BYTES])> {
        self.open_with(kind, None)
    }

    /// Begin the leg of rank `rank` of a multi-process instance: only that rank's slices of
    /// the windows travel on it.
    pub fn open_rank(
        &self,
        kind: Kind,
        layout: Arc<SliceLayout>,
        rank: usize,
    ) -> io::Result<(u64, [u8; TOKEN_BYTES])> {
        if rank >= layout.world() {
            return Err(invalid("no such rank"));
        }
        self.open_with(kind, Some((layout, rank)))
    }

    fn open_with(
        &self,
        kind: Kind,
        slicing: Option<(Arc<SliceLayout>, usize)>,
    ) -> io::Result<(u64, [u8; TOKEN_BYTES])> {
        let token = random_token()?;
        let transfer = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let (stopped, _) = watch::channel(false);
        let entry = Arc::new(Transfer {
            kind,
            slicing,
            token,
            queues: Mutex::new(Queues {
                windows: VecDeque::new(),
                grants: VecDeque::new(),
                finished: false,
                attached: false,
            }),
            wake: Notify::new(),
            stopped,
        });
        let mut state = self.inner.state.lock().unwrap();
        if !state.open {
            return Err(io::Error::other("the data channel is shut down"));
        }
        state.tokens.insert(token, transfer);
        state.transfers.insert(transfer, entry);
        Ok((transfer, token))
    }

    fn transfer(&self, transfer: u64) -> Option<Arc<Transfer>> {
        self.inner
            .state
            .lock()
            .unwrap()
            .transfers
            .get(&transfer)
            .cloned()
    }

    /// Queue a claim's filled window for the instance.
    pub fn push_window(&self, transfer: u64, window: WindowOut) {
        if let Some(entry) = self.transfer(transfer) {
            entry.queues.lock().unwrap().windows.push_back(window);
            entry.wake.notify_one();
        }
    }

    /// Queue a put's granted window for the instance to fill.
    pub fn push_grant(&self, transfer: u64, grant: GrantOut) {
        if let Some(entry) = self.transfer(transfer) {
            entry.queues.lock().unwrap().grants.push_back(grant);
            entry.wake.notify_one();
        }
    }

    /// No window follows the ones queued: the handler sends END after them.
    pub fn finish(&self, transfer: u64) {
        if let Some(entry) = self.transfer(transfer) {
            entry.queues.lock().unwrap().finished = true;
            entry.wake.notify_one();
        }
    }

    /// End the transfer now. Returns `true` when no handler ever ran or it has
    /// already exited, so nothing can touch the transfer's windows any more;
    /// otherwise `Closed` follows once the handler has dropped its connection.
    pub fn stop(&self, transfer: u64) -> bool {
        let mut state = self.inner.state.lock().unwrap();
        let Some(entry) = state.transfers.get(&transfer).cloned() else {
            return true;
        };
        // Taking the token and the stop flag in one critical section with the
        // handler's attach means a connection can never attach to a stopped transfer.
        state.tokens.remove(&entry.token);
        let _ = entry.stopped.send(true);
        let attached = entry.queues.lock().unwrap().attached;
        if !attached {
            state.transfers.remove(&transfer);
        }
        !attached
    }

    /// Forget a transfer whose handler has exited.
    pub fn forget(&self, transfer: u64) {
        self.inner.state.lock().unwrap().transfers.remove(&transfer);
    }

    /// Stop accepting and stop every transfer.
    pub fn shutdown(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.open = false;
        state.tokens.clear();
        for entry in state.transfers.values() {
            let _ = entry.stopped.send(true);
        }
        let _ = self.closing.send(true);
    }

    /// Hold back every later `Closed` by `delay`, as if the handler could not be seen to exit.
    #[cfg(test)]
    pub fn delay_closed(&self, delay: Duration) {
        *self.inner.closed_delay.lock().unwrap() = delay;
    }

    /// Transfers with a live handler, for tests and metrics.
    pub fn handlers(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap()
            .transfers
            .values()
            .filter(|entry| entry.queues.lock().unwrap().attached)
            .count()
    }
}

async fn accept_loop(inner: Arc<Inner>, listener: TcpListener, mut closing: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let inner = inner.clone();
                    let handle = inner.handle.clone();
                    handle.spawn(async move { serve(inner, stream).await });
                }
                Err(error) => {
                    // A transient accept failure (descriptor exhaustion) must not end the listener.
                    eprintln!("selene: warning: the data listener could not accept: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
            _ = closing.changed() => return,
        }
    }
}

trait Conn: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Conn for T {}

#[cfg(target_os = "linux")]
fn set_buffers(stream: &TcpStream) {
    use std::os::fd::AsRawFd;
    let value: libc::c_int = SOCKET_BUFFER.min(i32::MAX as u64) as libc::c_int;
    for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        // SAFETY: the descriptor is valid for the stream's lifetime, the pointer refers to a
        // live `c_int`, and the length is that integer's size.
        unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                &value as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_buffers(_stream: &TcpStream) {}

/// Why a handler stopped.
enum End {
    /// The actor stopped the transfer.
    Stopped,
    /// The transfer is complete.
    Done,
    Failed(String),
}

fn failure(error: impl std::fmt::Display) -> End {
    End::Failed(error.to_string())
}

async fn serve(inner: Arc<Inner>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    set_buffers(&stream);
    // The handshake and the token share one budget.
    let accepted = timeout(TOKEN_TIMEOUT, async {
        let mut conn: Box<dyn Conn> = match &inner.acceptor {
            Some(acceptor) => Box::new(acceptor.accept(stream).await?),
            None => Box::new(stream),
        };
        let mut token = [0u8; TOKEN_BYTES];
        conn.read_exact(&mut token).await?;
        Ok::<_, io::Error>((conn, token))
    })
    .await;
    let Ok(Ok((mut conn, token))) = accepted else {
        return;
    };
    // Look the token up and attach in one critical section with `stop`, so a
    // stopped transfer (its token already removed) can never gain a handler.
    // A token is good for one connection, whatever becomes of it.
    let (transfer, entry) = {
        let mut state = inner.state.lock().unwrap();
        let Some(transfer) = state.tokens.remove(&token) else {
            return;
        };
        let Some(entry) = state.transfers.get(&transfer).cloned() else {
            return;
        };
        if *entry.stopped.borrow() {
            return;
        }
        entry.queues.lock().unwrap().attached = true;
        (transfer, entry)
    };
    let mut stopped = entry.stopped.subscribe();
    // An RDMA transfer's channel lives here, outside the handler, so it can be fenced
    // properly whichever way the handler ends.
    let mut rdma: Option<Box<dyn DataChannel>> = None;
    // Packed staging a work request may still touch: freed only after the queue pair is fenced.
    let held: Mutex<Vec<Extent>> = Mutex::new(Vec::new());
    let result = tokio::select! {
        result = run(&inner, transfer, &entry, &mut conn, &mut rdma, &held) => result,
        _ = async { let _ = stopped.wait_for(|stop| *stop).await; } => End::Stopped,
    };
    // Fence the queue pair: it moves to the error state, its requests are flushed and it is
    // destroyed, on a thread that may block for the device.
    if let Some(channel) = rdma.take() {
        let _ = tokio::task::spawn_blocking(move || {
            let mut channel = channel;
            let _ = channel.close();
            drop(channel);
        })
        .await;
    }
    // Nothing is in flight any more, so the packed staging goes back.
    held.lock().unwrap().clear();
    // Say goodbye if the peer will listen (a TLS peer needs the close notice to tell an
    // end from a cut), then drop the connection: only now is the transfer's memory quiet.
    let _ = timeout(GOODBYE, conn.shutdown()).await;
    drop(conn);
    #[cfg(test)]
    {
        let delay = *inner.closed_delay.lock().unwrap();
        tokio::time::sleep(delay).await;
    }
    entry.queues.lock().unwrap().attached = false;
    if let End::Failed(reason) = result {
        let _ = inner.events.send(Event::Failed { transfer, reason });
    }
    let _ = inner.events.send(Event::Closed { transfer });
}

async fn run(
    inner: &Inner,
    transfer: u64,
    entry: &Transfer,
    conn: &mut Box<dyn Conn>,
    rdma: &mut Option<Box<dyn DataChannel>>,
    held: &Mutex<Vec<Extent>>,
) -> End {
    let hello = match timeout(IO_TIMEOUT, ToManager::read(&mut *conn, inner.max_pages)).await {
        Ok(Ok(frame)) => frame,
        Ok(Err(error)) => return failure(error),
        Err(_) => return End::Failed("the instance sent no hello".into()),
    };
    match hello {
        ToManager::Hello(Carrier::Stream) => {}
        ToManager::Hello(Carrier::Rdma(peer)) => match open_rdma(inner, conn, &peer).await {
            Ok(channel) => *rdma = Some(channel),
            Err(error) => {
                // Say why before the connection closes, so the instance falls back.
                let _ = ToInstance::Abort(error.to_string()).write(conn).await;
                return failure(error);
            }
        },
        _ => return End::Failed("the instance did not open with a hello".into()),
    }
    if rdma.is_some() && entry.slicing.is_some() && inner.packed.lock().unwrap().is_none() {
        let _ = ToInstance::Abort("this manager has no packed staging for a rank's leg".into())
            .write(conn)
            .await;
        return End::Failed("a rank's leg asked for RDMA without packed staging".into());
    }
    let result = match (entry.kind, rdma.as_mut()) {
        (Kind::Get, None) => send_windows(inner, transfer, entry, conn).await,
        (Kind::Put, None) => receive_windows(inner, transfer, entry, conn).await,
        (Kind::Get, Some(channel)) => {
            send_windows_rdma(inner, transfer, entry, conn, channel, held).await
        }
        (Kind::Put, Some(channel)) => {
            receive_windows_rdma(inner, transfer, entry, conn, channel, held).await
        }
    };
    match result {
        Ok(()) => End::Done,
        Err(error) => failure(error),
    }
}

/// Make a queue pair for the instance's, answer with ours and bring it up.
async fn open_rdma(
    inner: &Inner,
    conn: &mut Box<dyn Conn>,
    peer: &[u8],
) -> io::Result<Box<dyn DataChannel>> {
    let rdma = inner
        .rdma
        .as_ref()
        .ok_or_else(|| invalid("this manager does not offer RDMA"))?;
    let pair = rdma.pair()?;
    let handshake = pair.handshake();
    timeout(IO_TIMEOUT, async {
        ToInstance::Accept(handshake).write(conn).await?;
        conn.flush().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading"))??;
    pair.connect(peer)
}

/// A window's bytes in one of the manager's regions.
///
/// SAFETY: the caller owns `[offset, offset + length)` of the region for as long as the
/// returned slice lives: the store leased the window to this transfer and does not
/// release it until the handler reports `Closed`. The mapping outlives `regions`.
unsafe fn window_bytes<'a>(
    regions: &dyn RegionAccess,
    region: Region,
    offset: u64,
    length: u64,
) -> io::Result<&'a mut [u8]> {
    let (base, size) = regions
        .mapping(region)
        .ok_or_else(|| invalid("the region is not mapped"))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("window range overflow"))?;
    if end > size as u64 {
        return Err(invalid("a window lies outside its region"));
    }
    // SAFETY: the range was checked against the mapping; exclusivity is the caller's contract.
    Ok(unsafe { std::slice::from_raw_parts_mut(base.add(offset as usize), length as usize) })
}

async fn send_windows(
    inner: &Inner,
    transfer: u64,
    entry: &Transfer,
    conn: &mut Box<dyn Conn>,
) -> io::Result<()> {
    loop {
        let next = {
            let mut queues = entry.queues.lock().unwrap();
            match queues.windows.pop_front() {
                Some(window) => Some(Some(window)),
                None if queues.finished => Some(None),
                None => None,
            }
        };
        let Some(next) = next else {
            entry.wake.notified().await;
            continue;
        };
        let Some(window) = next else {
            timeout(IO_TIMEOUT, async {
                ToInstance::End.write(conn).await?;
                conn.flush().await
            })
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading")
            })??;
            let _ = timeout(GOODBYE, conn.shutdown()).await;
            return Ok(());
        };
        // SAFETY: see `window_bytes`; the slice is only read.
        let packed;
        let payload: &[u8] = match &entry.slicing {
            None => unsafe {
                window_bytes(
                    &*inner.regions,
                    window.region,
                    window.offset,
                    window.shape.length,
                )?
            },
            Some((layout, rank)) => {
                let (pages, row) = (window.shape.page_count, window.shape.row);
                if window.shape.length != layout.slice_bytes(*rank, pages, row) {
                    return Err(invalid("a rank window's length is not its slice's"));
                }
                // SAFETY: as above, over the whole window the slice lies in.
                let whole = unsafe {
                    window_bytes(
                        &*inner.regions,
                        window.region,
                        window.offset,
                        layout.window_bytes(pages, row),
                    )?
                };
                let mut bytes = vec![0u8; window.shape.length as usize];
                layout
                    .gather(*rank, whole, pages, row, &mut bytes)
                    .map_err(|error| invalid(error.to_string()))?;
                packed = bytes;
                &packed
            }
        };
        let frame = ToInstance::Window(Window {
            shape: window.shape,
            page_xxh3: window.page_xxh3,
            row_xxh3: window.row_xxh3,
            slot: NO_SLOT,
        });
        timeout(IO_TIMEOUT, async {
            frame.write(conn).await?;
            conn.write_all(payload).await?;
            conn.flush().await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading"))??;
        inner.counters.add(0, window.shape.length);
        let _ = inner.events.send(Event::Sent {
            transfer,
            index: window.shape.index,
        });
    }
}

/// How long one RDMA window may take to complete.
const RDMA_WINDOW: Duration = Duration::from_secs(10);

/// Wait for window `(transfer, index)` of the channel to complete. Completions are polled:
/// a few spins on the fast path, then short sleeps so an idle transfer costs little.
async fn rdma_done(
    channel: &mut Box<dyn DataChannel>,
    transfer: u64,
    index: u32,
) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + RDMA_WINDOW;
    let mut spins = 0u32;
    loop {
        let mut done = Vec::new();
        channel.poll(&mut done)?;
        for (id, window, result) in done {
            if id == transfer && window == index {
                return result;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "an RDMA window did not complete",
            ));
        }
        spins += 1;
        if spins < 200 {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(Duration::from_micros(200)).await;
        }
    }
}

/// A claim over RDMA: each window is written into a slot the instance lent, and named in
/// a WINDOW frame once the write has completed.
async fn send_windows_rdma(
    inner: &Inner,
    transfer: u64,
    entry: &Transfer,
    conn: &mut Box<dyn Conn>,
    channel: &mut Box<dyn DataChannel>,
    held: &Mutex<Vec<Extent>>,
) -> io::Result<()> {
    let (mut reader, mut writer) = tokio::io::split(conn);
    let (lend, mut lent) = mpsc::unbounded_channel::<(u64, u64)>();
    let reading = async {
        loop {
            match ToManager::read_or_end(&mut reader, inner.max_pages).await? {
                Some(ToManager::Credit { offset, length }) => {
                    let _ = lend.send((offset, length));
                }
                Some(_) => return Err(invalid("a claim's instance sends only credits")),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the instance closed the connection",
                    ));
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    };
    let writing = async {
        loop {
            let next = {
                let mut queues = entry.queues.lock().unwrap();
                match queues.windows.pop_front() {
                    Some(window) => Some(Some(window)),
                    None if queues.finished => Some(None),
                    None => None,
                }
            };
            let Some(next) = next else {
                entry.wake.notified().await;
                continue;
            };
            let Some(window) = next else {
                timeout(IO_TIMEOUT, async {
                    ToInstance::End.write(&mut writer).await?;
                    writer.flush().await
                })
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading")
                })??;
                return Ok(());
            };
            let length = window.shape.length;
            let (offset, lent_bytes) = timeout(IO_TIMEOUT, lent.recv())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the instance lent no slot"))?
                .ok_or_else(|| invalid("the instance closed the connection"))?;
            if length > lent_bytes || !offset.is_multiple_of(4096) {
                return Err(invalid("a window does not fit the slot the instance lent"));
            }
            // A rank's slices are strided in the window: gathered into packed staging first,
            // and that buffer is what crosses the network.
            let (from_region, from_offset) = match &entry.slicing {
                None => (window.region, window.offset),
                Some((layout, rank)) => {
                    let (pages, row) = (window.shape.page_count, window.shape.row);
                    if length != layout.slice_bytes(*rank, pages, row) {
                        return Err(invalid("a rank window's length is not its slice's"));
                    }
                    let arena = inner
                        .packed
                        .lock()
                        .unwrap()
                        .clone()
                        .ok_or_else(|| invalid("no packed staging for a rank's leg"))?;
                    let mut packed = arena
                        .allocate(length as usize)
                        .ok_or_else(|| io::Error::other("packed staging is full"))?;
                    // SAFETY: see `window_bytes`; the window is only read.
                    let whole = unsafe {
                        window_bytes(
                            &*inner.regions,
                            window.region,
                            window.offset,
                            layout.window_bytes(pages, row),
                        )?
                    };
                    layout
                        .gather(*rank, whole, pages, row, packed.bytes_mut())
                        .map_err(|error| invalid(error.to_string()))?;
                    let at = packed.offset() as u64;
                    held.lock().unwrap().push(packed);
                    (Region::Staging, at)
                }
            };
            channel.write(
                transfer,
                window.shape.index,
                from_region,
                from_offset,
                length,
                Region::Staging,
                offset,
            )?;
            rdma_done(channel, transfer, window.shape.index).await?;
            if entry.slicing.is_some() {
                // The write completed: the packed buffer is no longer read.
                held.lock().unwrap().pop();
            }
            let frame = ToInstance::Window(Window {
                shape: window.shape,
                page_xxh3: window.page_xxh3,
                row_xxh3: window.row_xxh3,
                slot: offset,
            });
            timeout(IO_TIMEOUT, async {
                frame.write(&mut writer).await?;
                writer.flush().await
            })
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading")
            })??;
            inner.counters.add(1, length);
            let _ = inner.events.send(Event::Sent {
                transfer,
                index: window.shape.index,
            });
        }
    };
    tokio::select! {
        result = writing => result,
        result = reading => result,
    }
}

/// A put over RDMA: the instance names the slot it filled and the manager reads it into the
/// granted window, then releases the slot with TAKEN.
async fn receive_windows_rdma(
    inner: &Inner,
    transfer: u64,
    entry: &Transfer,
    conn: &mut Box<dyn Conn>,
    channel: &mut Box<dyn DataChannel>,
    held: &Mutex<Vec<Extent>>,
) -> io::Result<()> {
    let (mut reader, mut writer) = tokio::io::split(conn);
    let (expect, mut expected) = mpsc::unbounded_channel::<GrantOut>();
    let (took, mut taken) = mpsc::unbounded_channel::<u32>();
    let writing = async {
        loop {
            let grants: Vec<GrantOut> = entry.queues.lock().unwrap().grants.drain(..).collect();
            for grant in grants {
                // Tell the reader first: the instance may answer before this write returns.
                let _ = expect.send(grant);
                timeout(IO_TIMEOUT, async {
                    ToInstance::Grant(grant.shape).write(&mut writer).await?;
                    writer.flush().await
                })
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading")
                })??;
            }
            tokio::select! {
                _ = entry.wake.notified() => {}
                index = taken.recv() => {
                    let Some(index) = index else { return Ok::<(), io::Error>(()) };
                    timeout(IO_TIMEOUT, async {
                        ToInstance::Taken(index).write(&mut writer).await?;
                        writer.flush().await
                    })
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading")
                    })??;
                }
            }
        }
    };
    let reading = async {
        while let Some(grant) = expected.recv().await {
            let frame = timeout(IO_TIMEOUT, ToManager::read(&mut reader, inner.max_pages))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "the instance sent nothing")
                })??;
            let ToManager::Filled {
                index,
                row_xxh3,
                page_xxh3,
                slot,
            } = frame
            else {
                return Err(invalid("the instance sent a frame a put does not take"));
            };
            if slot == NO_SLOT || !slot.is_multiple_of(4096) {
                return Err(invalid("a filled window names no usable slot"));
            }
            if index != grant.shape.index
                || page_xxh3.len() != grant.shape.page_count as usize
                || row_xxh3.is_some() != grant.shape.row
            {
                return Err(invalid("a filled window does not match its grant"));
            }
            match &entry.slicing {
                None => {
                    channel.read(
                        transfer,
                        index,
                        grant.region,
                        grant.offset,
                        grant.shape.length,
                        Region::Staging,
                        slot,
                    )?;
                    rdma_done(channel, transfer, index).await?;
                }
                Some((layout, rank)) => {
                    // The rank's slice is read into packed staging and then scattered into the
                    // strided places it belongs in the granted window.
                    let (pages, row) = (grant.shape.page_count, grant.shape.row);
                    if grant.shape.length != layout.slice_bytes(*rank, pages, row) {
                        return Err(invalid("a rank window's length is not its slice's"));
                    }
                    let arena = inner
                        .packed
                        .lock()
                        .unwrap()
                        .clone()
                        .ok_or_else(|| invalid("no packed staging for a rank's leg"))?;
                    let packed = arena
                        .allocate(grant.shape.length as usize)
                        .ok_or_else(|| io::Error::other("packed staging is full"))?;
                    let at = packed.offset() as u64;
                    held.lock().unwrap().push(packed);
                    channel.read(
                        transfer,
                        index,
                        Region::Staging,
                        at,
                        grant.shape.length,
                        Region::Staging,
                        slot,
                    )?;
                    rdma_done(channel, transfer, index).await?;
                    let packed = held.lock().unwrap().pop().expect("pushed above");
                    // SAFETY: see `window_bytes`; the store leased this window to the put.
                    let whole = unsafe {
                        window_bytes(
                            &*inner.regions,
                            grant.region,
                            grant.offset,
                            layout.window_bytes(pages, row),
                        )?
                    };
                    layout
                        .scatter(*rank, whole, pages, row, packed.bytes())
                        .map_err(|error| invalid(error.to_string()))?;
                }
            }
            inner.counters.add(1, grant.shape.length);
            let _ = inner.events.send(Event::Filled {
                transfer,
                index,
                row_xxh3,
                page_xxh3,
            });
            let _ = took.send(index);
        }
        Ok(())
    };
    tokio::select! {
        result = writing => result,
        result = reading => result,
    }
}

async fn receive_windows(
    inner: &Inner,
    transfer: u64,
    entry: &Transfer,
    conn: &mut Box<dyn Conn>,
) -> io::Result<()> {
    let (mut reader, mut writer) = tokio::io::split(conn);
    let (expect, mut expected) = mpsc::unbounded_channel::<GrantOut>();
    let writing = async {
        loop {
            let grants: Vec<GrantOut> = entry.queues.lock().unwrap().grants.drain(..).collect();
            if grants.is_empty() {
                entry.wake.notified().await;
                continue;
            }
            for grant in grants {
                // Tell the reader first: the instance may answer before this write returns.
                let _ = expect.send(grant);
                timeout(IO_TIMEOUT, async {
                    ToInstance::Grant(grant.shape).write(&mut writer).await?;
                    writer.flush().await
                })
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading")
                })??;
            }
        }
    };
    let reading = async {
        while let Some(grant) = expected.recv().await {
            let frame = timeout(IO_TIMEOUT, ToManager::read(&mut reader, inner.max_pages))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "the instance sent nothing")
                })??;
            let ToManager::Filled {
                index,
                row_xxh3,
                page_xxh3,
                slot,
            } = frame
            else {
                return Err(invalid("the instance sent a frame a put does not take"));
            };
            if slot != NO_SLOT {
                return Err(invalid(
                    "a filled window names a slot on the stream carrier",
                ));
            }
            if index != grant.shape.index
                || page_xxh3.len() != grant.shape.page_count as usize
                || row_xxh3.is_some() != grant.shape.row
            {
                return Err(invalid("a filled window does not match its grant"));
            }
            // SAFETY: see `window_bytes`; the store leased this window to the put.
            match &entry.slicing {
                None => {
                    let payload = unsafe {
                        window_bytes(
                            &*inner.regions,
                            grant.region,
                            grant.offset,
                            grant.shape.length,
                        )?
                    };
                    timeout(IO_TIMEOUT, reader.read_exact(payload))
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "the instance stopped writing")
                        })??;
                }
                Some((layout, rank)) => {
                    let (pages, row) = (grant.shape.page_count, grant.shape.row);
                    if grant.shape.length != layout.slice_bytes(*rank, pages, row) {
                        return Err(invalid("a rank window's length is not its slice's"));
                    }
                    let mut packed = vec![0u8; grant.shape.length as usize];
                    timeout(IO_TIMEOUT, reader.read_exact(&mut packed))
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "the instance stopped writing")
                        })??;
                    // SAFETY: as above, over the whole window the slice lies in.
                    let whole = unsafe {
                        window_bytes(
                            &*inner.regions,
                            grant.region,
                            grant.offset,
                            layout.window_bytes(pages, row),
                        )?
                    };
                    layout
                        .scatter(*rank, whole, pages, row, &packed)
                        .map_err(|error| invalid(error.to_string()))?;
                }
            }
            inner.counters.add(0, grant.shape.length);
            let _ = inner.events.send(Event::Filled {
                transfer,
                index,
                row_xxh3,
                page_xxh3,
            });
        }
        Ok(())
    };
    // Either direction ending, normally or not, ends the handler.
    tokio::select! {
        result = writing => result,
        result = reading => result,
    }
}

/// Parse the manager's certificate chain and key into a TLS server configuration.
pub fn server_config(
    certificate_pem: &[u8],
    key_pem: &[u8],
) -> io::Result<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(certificate_pem)
        .collect::<Result<_, _>>()
        .map_err(|error| invalid(format!("the TLS certificate: {error}")))?;
    if chain.is_empty() {
        return Err(invalid("the TLS certificate file holds no certificate"));
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem)
        .map_err(|error| invalid(format!("the TLS key: {error}")))?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(format!("TLS: {error}")))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|error| invalid(format!("TLS: {error}")))?;
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arena::Arena, regions::LocalRegions};
    use flare_selene::pool::frame::TOKEN_BYTES;
    use std::net::TcpListener as StdListener;

    const PAGE: usize = 4096;

    /// Runs a test's future on its own runtime and fails with a message, instead of hanging,
    /// if it has not finished in [`TEST_LIMIT`]: every wait in these tests is for a peer that
    /// is in the same test, so a stuck one is a bug to report, never a reason to wait.
    const TEST_LIMIT: Duration = Duration::from_secs(60);

    fn bounded(test: impl std::future::Future<Output = ()>) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let name = std::thread::current()
            .name()
            .unwrap_or("a test")
            .to_string();
        runtime.block_on(async {
            if timeout(TEST_LIMIT, test).await.is_err() {
                panic!("{name} did not finish within {TEST_LIMIT:?}: a connection or event it waits for never came");
            }
        });
        // A stuck background task must not keep the process either.
        runtime.shutdown_timeout(Duration::from_secs(5));
    }

    fn regions() -> Arc<LocalRegions> {
        Arc::new(LocalRegions::new(
            Arena::new(1 << 20).unwrap(),
            Arena::new(1 << 20).unwrap(),
        ))
    }

    fn start(
        regions: Arc<LocalRegions>,
        tls: Option<Arc<rustls::ServerConfig>>,
    ) -> (Hub, mpsc::UnboundedReceiver<Event>, String) {
        let listener = StdListener::bind("127.0.0.1:0").unwrap();
        let (hub, events) =
            Hub::start(&Handle::current(), listener, regions, tls, 64, None).unwrap();
        let address = hub.address().to_string();
        (hub, events, address)
    }

    fn fill(regions: &LocalRegions, region: Region, offset: u64, bytes: &[u8]) {
        regions
            .with_write(region, offset, bytes.len(), &mut |dst| {
                dst.copy_from_slice(bytes);
                Ok(())
            })
            .unwrap();
    }

    fn shape(index: u32, pages: u32, row: bool) -> Shape {
        Shape {
            index,
            record: 0,
            first_page: 0,
            page_count: pages,
            row,
            length: u64::from(pages) * PAGE as u64 + if row { PAGE as u64 } else { 0 },
        }
    }

    async fn connect(address: &str, token: &[u8; TOKEN_BYTES]) -> Box<dyn Conn> {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(token).await.unwrap();
        ToManager::Hello(Carrier::Stream)
            .write(&mut stream)
            .await
            .unwrap();
        Box::new(stream)
    }

    async fn next(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
        timeout(Duration::from_secs(5), events.recv())
            .await
            .expect("an event in time")
            .expect("the hub is alive")
    }

    #[test]
    fn a_claims_windows_arrive_in_order_with_their_checksums_and_end() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let (transfer, token) = hub.open(Kind::Get).unwrap();
            let first: Vec<u8> = (0..2 * PAGE + PAGE).map(|i| (i % 251) as u8).collect();
            let second = vec![7u8; PAGE];
            fill(&regions, Region::Arena, 0, &first);
            fill(&regions, Region::Staging, 8192, &second);
            let mut conn = connect(&address, &token).await;
            hub.push_window(
                transfer,
                WindowOut {
                    shape: shape(0, 2, true),
                    page_xxh3: vec![11, 12],
                    row_xxh3: 13,
                    region: Region::Arena,
                    offset: 0,
                },
            );
            hub.push_window(
                transfer,
                WindowOut {
                    shape: shape(1, 1, false),
                    page_xxh3: vec![21],
                    row_xxh3: 0,
                    region: Region::Staging,
                    offset: 8192,
                },
            );
            hub.finish(transfer);
            let mut payload = vec![0u8; first.len()];
            let ToInstance::Window(window) = ToInstance::read(&mut conn, 64).await.unwrap() else {
                panic!("expected a window");
            };
            assert_eq!(window.shape, shape(0, 2, true));
            assert_eq!(
                (window.page_xxh3.clone(), window.row_xxh3),
                (vec![11, 12], 13)
            );
            conn.read_exact(&mut payload).await.unwrap();
            assert_eq!(payload, first);
            let ToInstance::Window(window) = ToInstance::read(&mut conn, 64).await.unwrap() else {
                panic!("expected a second window");
            };
            assert_eq!(window.shape.index, 1);
            let mut payload = vec![0u8; PAGE];
            conn.read_exact(&mut payload).await.unwrap();
            assert_eq!(payload, second);
            assert_eq!(
                ToInstance::read(&mut conn, 64).await.unwrap(),
                ToInstance::End
            );
            // Sent twice, then the handler leaves on its own.
            assert!(matches!(
                next(&mut events).await,
                Event::Sent { index: 0, .. }
            ));
            assert!(matches!(
                next(&mut events).await,
                Event::Sent { index: 1, .. }
            ));
            assert!(matches!(next(&mut events).await, Event::Closed { .. }));
            assert!(
                hub.stop(transfer),
                "an exited handler leaves nothing to wait for"
            );
        })
    }

    #[test]
    fn a_token_serves_one_connection_and_a_wrong_token_none() {
        bounded(async {
            let (hub, mut events, address) = start(regions(), None);
            let (transfer, token) = hub.open(Kind::Get).unwrap();
            let _first = connect(&address, &token).await;
            // Wait until the first connection has been attached, or the second could win the race.
            while hub.handlers() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // The token is spent: a second connection is closed without a byte.
            let mut second = connect(&address, &token).await;
            assert!(ToInstance::read(&mut second, 64).await.is_err());
            let mut wrong = connect(&address, &[9; TOKEN_BYTES]).await;
            assert!(ToInstance::read(&mut wrong, 64).await.is_err());
            assert!(!hub.stop(transfer), "a handler is attached");
            assert!(matches!(next(&mut events).await, Event::Closed { .. }));
        })
    }

    #[test]
    fn stopping_before_any_connection_needs_no_wait_and_blocks_a_late_one() {
        bounded(async {
            let (hub, _events, address) = start(regions(), None);
            let (transfer, token) = hub.open(Kind::Get).unwrap();
            assert!(hub.stop(transfer), "no handler ever ran");
            let mut late = connect(&address, &token).await;
            assert!(ToInstance::read(&mut late, 64).await.is_err());
            assert_eq!(hub.handlers(), 0);
        })
    }

    #[test]
    fn stopping_a_stalled_transfer_ends_its_handler_and_reports_closed() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let (transfer, token) = hub.open(Kind::Get).unwrap();
            // The instance connects and never reads: the socket buffers fill.
            let _conn = connect(&address, &token).await;
            fill(&regions, Region::Arena, 0, &vec![1u8; 1 << 20]);
            for index in 0..64 {
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: Shape {
                            length: 1 << 20,
                            page_count: 256,
                            ..shape(index, 0, false)
                        },
                        page_xxh3: vec![0; 256],
                        row_xxh3: 0,
                        region: Region::Arena,
                        offset: 0,
                    },
                );
            }
            // Wait until the handler is stuck in a write (some windows went, not all).
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(hub.handlers(), 1);
            assert!(!hub.stop(transfer));
            loop {
                match next(&mut events).await {
                    Event::Closed { transfer: closed } => {
                        assert_eq!(closed, transfer);
                        break;
                    }
                    Event::Sent { .. } => {}
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert_eq!(hub.handlers(), 0);
        })
    }

    #[test]
    fn a_peer_that_drops_mid_transfer_fails_it_and_closes_the_handler() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let (transfer, token) = hub.open(Kind::Get).unwrap();
            let conn = connect(&address, &token).await;
            drop(conn);
            fill(&regions, Region::Arena, 0, &vec![1u8; 1 << 20]);
            for index in 0..32 {
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: Shape {
                            length: 1 << 20,
                            page_count: 256,
                            ..shape(index, 0, false)
                        },
                        page_xxh3: vec![0; 256],
                        row_xxh3: 0,
                        region: Region::Arena,
                        offset: 0,
                    },
                );
            }
            let mut failed = false;
            loop {
                match next(&mut events).await {
                    Event::Failed { .. } => failed = true,
                    Event::Closed { .. } => break,
                    Event::Sent { .. } => {}
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert!(failed, "the broken connection is reported");
        })
    }

    #[test]
    fn a_puts_windows_are_filled_from_what_the_instance_writes() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let (transfer, token) = hub.open(Kind::Put).unwrap();
            let mut conn = connect(&address, &token).await;
            hub.push_grant(
                transfer,
                GrantOut {
                    shape: shape(0, 2, true),
                    region: Region::Staging,
                    offset: 4096,
                },
            );
            let ToInstance::Grant(granted) = ToInstance::read(&mut conn, 64).await.unwrap() else {
                panic!("expected a grant");
            };
            assert_eq!(granted, shape(0, 2, true));
            let bytes: Vec<u8> = (0..3 * PAGE).map(|i| (i % 13) as u8).collect();
            ToManager::Filled {
                index: 0,
                row_xxh3: Some(3),
                page_xxh3: vec![1, 2],
                slot: NO_SLOT,
            }
            .write(&mut conn)
            .await
            .unwrap();
            conn.write_all(&bytes).await.unwrap();
            let Event::Filled {
                index,
                row_xxh3,
                page_xxh3,
                ..
            } = next(&mut events).await
            else {
                panic!("expected Filled");
            };
            assert_eq!((index, row_xxh3, page_xxh3), (0, Some(3), vec![1, 2]));
            regions
                .with_read(Region::Staging, 4096, bytes.len(), &mut |src| {
                    assert_eq!(src, &bytes[..]);
                    Ok(())
                })
                .unwrap();
            // A second grant is served the same way, after the first.
            hub.push_grant(
                transfer,
                GrantOut {
                    shape: shape(1, 1, false),
                    region: Region::Staging,
                    offset: 65536,
                },
            );
            assert!(matches!(
                ToInstance::read(&mut conn, 64).await.unwrap(),
                ToInstance::Grant(_)
            ));
            hub.stop(transfer);
            assert!(matches!(next(&mut events).await, Event::Closed { .. }));
        })
    }

    fn rank_layout(world: usize) -> Arc<SliceLayout> {
        use flare_selene::pool::{Geometry, RankPlanes};
        let rank = RankPlanes {
            plane_bytes: vec![4096, 4096],
            row_bytes: 4096,
        };
        Arc::new(
            SliceLayout::new(&Geometry {
                page_tokens: 64,
                ranks: vec![rank; world],
                page_record_bytes: world as u64 * 8192,
                row_record_bytes: world as u64 * 4096,
            })
            .unwrap(),
        )
    }

    fn rank_shape(layout: &SliceLayout, rank: usize, index: u32, pages: u32, row: bool) -> Shape {
        Shape {
            length: layout.slice_bytes(rank, pages, row),
            ..shape(index, pages, row)
        }
    }

    #[test]
    fn a_ranks_leg_carries_only_its_slices_of_each_window_and_other_ranks_see_theirs() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let layout = rank_layout(3);
            let window: Vec<u8> = (0..layout.window_bytes(2, true) as usize)
                .map(|i| (i % 241) as u8)
                .collect();
            fill(&regions, Region::Staging, 8192, &window);
            let mut transfers = Vec::new();
            for rank in 0..3 {
                let (transfer, token) = hub.open_rank(Kind::Get, layout.clone(), rank).unwrap();
                let conn = connect(&address, &token).await;
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: rank_shape(&layout, rank, 0, 2, true),
                        page_xxh3: vec![1, 2],
                        row_xxh3: 3,
                        region: Region::Staging,
                        offset: 8192,
                    },
                );
                hub.finish(transfer);
                transfers.push((transfer, conn));
            }
            for (rank, (_, conn)) in transfers.iter_mut().enumerate() {
                let ToInstance::Window(got) = ToInstance::read(conn, 64).await.unwrap() else {
                    panic!("expected a window");
                };
                assert_eq!(got.shape, rank_shape(&layout, rank, 0, 2, true));
                let mut payload = vec![0u8; got.shape.length as usize];
                conn.read_exact(&mut payload).await.unwrap();
                let mut expect = vec![0u8; payload.len()];
                layout.gather(rank, &window, 2, true, &mut expect).unwrap();
                assert_eq!(payload, expect, "rank {rank}");
                assert_eq!(ToInstance::read(conn, 64).await.unwrap(), ToInstance::End);
            }
            let mut sent = 0;
            let mut closed = 0;
            while sent < 3 || closed < 3 {
                match next(&mut events).await {
                    Event::Sent { .. } => sent += 1,
                    Event::Closed { .. } => closed += 1,
                    other => panic!("unexpected {other:?}"),
                }
            }
        })
    }

    #[test]
    fn the_ranks_legs_of_a_put_assemble_one_window_and_a_wrong_length_ends_the_leg() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let layout = rank_layout(2);
            let truth: Vec<u8> = (0..layout.window_bytes(2, true) as usize)
                .map(|i| (i % 233) as u8)
                .collect();
            for rank in [1usize, 0] {
                let (transfer, token) = hub.open_rank(Kind::Put, layout.clone(), rank).unwrap();
                let mut conn = connect(&address, &token).await;
                hub.push_grant(
                    transfer,
                    GrantOut {
                        shape: rank_shape(&layout, rank, 0, 2, true),
                        region: Region::Staging,
                        offset: 16384,
                    },
                );
                let ToInstance::Grant(granted) = ToInstance::read(&mut conn, 64).await.unwrap()
                else {
                    panic!("expected a grant");
                };
                assert_eq!(granted, rank_shape(&layout, rank, 0, 2, true));
                let mut packed = vec![0u8; granted.length as usize];
                layout.gather(rank, &truth, 2, true, &mut packed).unwrap();
                ToManager::Filled {
                    index: 0,
                    row_xxh3: Some(3),
                    page_xxh3: vec![1, 2],
                    slot: NO_SLOT,
                }
                .write(&mut conn)
                .await
                .unwrap();
                conn.write_all(&packed).await.unwrap();
                let Event::Filled { index, .. } = next(&mut events).await else {
                    panic!("expected Filled");
                };
                assert_eq!(index, 0);
                hub.stop(transfer);
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
            }
            regions
                .with_read(Region::Staging, 16384, truth.len(), &mut |window| {
                    assert_eq!(window, &truth[..], "both ranks' slices are in place");
                    Ok(())
                })
                .unwrap();
            // A grant whose shape is not the rank's slice ends the leg.
            let (transfer, token) = hub.open_rank(Kind::Put, layout.clone(), 0).unwrap();
            let mut conn = connect(&address, &token).await;
            hub.push_grant(
                transfer,
                GrantOut {
                    shape: shape(0, 2, true),
                    region: Region::Staging,
                    offset: 16384,
                },
            );
            let ToInstance::Grant(_) = ToInstance::read(&mut conn, 64).await.unwrap() else {
                panic!("expected a grant");
            };
            ToManager::Filled {
                index: 0,
                row_xxh3: Some(3),
                page_xxh3: vec![1, 2],
                slot: NO_SLOT,
            }
            .write(&mut conn)
            .await
            .unwrap();
            conn.write_all(&vec![0u8; 3 * PAGE]).await.unwrap();
            let mut failed = false;
            loop {
                match next(&mut events).await {
                    Event::Failed { .. } => failed = true,
                    Event::Closed { .. } => break,
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert!(failed);
        })
    }

    #[test]
    fn a_filled_window_that_disagrees_with_its_grant_fails_the_put_without_writing() {
        bounded(async {
            let regions = regions();
            let (hub, mut events, address) = start(regions.clone(), None);
            let (transfer, token) = hub.open(Kind::Put).unwrap();
            let mut conn = connect(&address, &token).await;
            hub.push_grant(
                transfer,
                GrantOut {
                    shape: shape(0, 2, false),
                    region: Region::Staging,
                    offset: 0,
                },
            );
            let _ = ToInstance::read(&mut conn, 64).await.unwrap();
            // One checksum for a two-page window.
            ToManager::Filled {
                index: 0,
                row_xxh3: None,
                page_xxh3: vec![1],
                slot: NO_SLOT,
            }
            .write(&mut conn)
            .await
            .unwrap();
            conn.write_all(&vec![0xee; 2 * PAGE]).await.unwrap();
            loop {
                match next(&mut events).await {
                    Event::Failed { .. } => {}
                    Event::Closed { .. } => break,
                    other => panic!("unexpected {other:?}"),
                }
            }
            regions
                .with_read(Region::Staging, 0, 2 * PAGE, &mut |src| {
                    assert!(src.iter().all(|byte| *byte == 0), "nothing was written");
                    Ok(())
                })
                .unwrap();
        })
    }

    fn certificate() -> (Arc<rustls::ServerConfig>, rustls::RootCertStore) {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let pem = generated.cert.pem();
        let config = server_config(
            pem.as_bytes(),
            generated.signing_key.serialize_pem().as_bytes(),
        )
        .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        for der in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            roots.add(der.unwrap()).unwrap();
        }
        (config, roots)
    }

    #[test]
    fn a_tls_hub_serves_a_client_that_trusts_it_and_refuses_one_that_does_not() {
        bounded(async {
            use rustls::pki_types::ServerName;
            let regions = regions();
            let (config, roots) = certificate();
            let (hub, mut events, address) = start(regions.clone(), Some(config));
            assert!(hub.encrypted());
            let (transfer, token) = hub.open(Kind::Get).unwrap();
            fill(&regions, Region::Arena, 0, &vec![5u8; PAGE]);
            hub.push_window(
                transfer,
                WindowOut {
                    shape: shape(0, 1, false),
                    page_xxh3: vec![1],
                    row_xxh3: 0,
                    region: Region::Arena,
                    offset: 0,
                },
            );
            hub.finish(transfer);
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let client = |roots: rustls::RootCertStore| {
                let config = rustls::ClientConfig::builder_with_provider(provider.clone())
                    .with_safe_default_protocol_versions()
                    .unwrap()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                tokio_rustls::TlsConnector::from(Arc::new(config))
            };
            // A client with some other root fails the handshake; the token stays good.
            let tcp = TcpStream::connect(&address).await.unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            let (_, other_roots) = certificate();
            assert!(
                client(other_roots)
                    .connect(name.clone(), tcp)
                    .await
                    .is_err()
            );
            let tcp = TcpStream::connect(&address).await.unwrap();
            let mut conn = client(roots).connect(name, tcp).await.unwrap();
            conn.write_all(&token).await.unwrap();
            ToManager::Hello(Carrier::Stream)
                .write(&mut conn)
                .await
                .unwrap();
            let ToInstance::Window(window) = ToInstance::read(&mut conn, 64).await.unwrap() else {
                panic!("expected a window");
            };
            assert_eq!(window.page_xxh3, vec![1]);
            let mut payload = vec![0u8; PAGE];
            conn.read_exact(&mut payload).await.unwrap();
            assert!(payload.iter().all(|byte| *byte == 5));
            assert_eq!(
                ToInstance::read(&mut conn, 64).await.unwrap(),
                ToInstance::End
            );
            assert!(matches!(next(&mut events).await, Event::Sent { .. }));
        })
    }

    #[test]
    fn a_certificate_and_key_that_do_not_belong_together_are_refused() {
        let one = rcgen::generate_simple_self_signed(vec!["a".to_string()]).unwrap();
        let two = rcgen::generate_simple_self_signed(vec!["b".to_string()]).unwrap();
        assert!(
            server_config(
                one.cert.pem().as_bytes(),
                two.signing_key.serialize_pem().as_bytes()
            )
            .is_err()
        );
        assert!(server_config(b"", one.signing_key.serialize_pem().as_bytes()).is_err());
    }

    /// An RDMA device that moves bytes between the manager's regions and a buffer standing in
    /// for the instance's lent staging, so the gather and scatter around a rank's leg can be
    /// tested without hardware.
    mod fake_rdma {
        use super::*;
        use crate::data::channel::{Pair, Rdma};

        pub struct Device {
            pub remote: Arc<Mutex<Vec<u8>>>,
            pub regions: Arc<LocalRegions>,
        }

        struct Queue {
            remote: Arc<Mutex<Vec<u8>>>,
            regions: Arc<LocalRegions>,
            done: Vec<(u64, u32, io::Result<()>)>,
        }

        impl Rdma for Device {
            fn pair(&self) -> io::Result<Box<dyn Pair>> {
                Ok(Box::new(Queue {
                    remote: self.remote.clone(),
                    regions: self.regions.clone(),
                    done: Vec::new(),
                }))
            }
        }

        impl Pair for Queue {
            fn handshake(&self) -> Vec<u8> {
                vec![7]
            }
            fn connect(self: Box<Self>, _: &[u8]) -> io::Result<Box<dyn DataChannel>> {
                Ok(self)
            }
        }

        impl DataChannel for Queue {
            fn write(
                &mut self,
                transfer: u64,
                index: u32,
                region: Region,
                offset: u64,
                len: u64,
                _: Region,
                remote_offset: u64,
            ) -> io::Result<()> {
                let remote = self.remote.clone();
                self.regions
                    .with_read(region, offset, len as usize, &mut |bytes| {
                        let at = remote_offset as usize;
                        remote.lock().unwrap()[at..at + bytes.len()].copy_from_slice(bytes);
                        Ok(())
                    })?;
                self.done.push((transfer, index, Ok(())));
                Ok(())
            }
            fn read(
                &mut self,
                transfer: u64,
                index: u32,
                region: Region,
                offset: u64,
                len: u64,
                _: Region,
                remote_offset: u64,
            ) -> io::Result<()> {
                let remote = self.remote.clone();
                self.regions
                    .with_write(region, offset, len as usize, &mut |bytes| {
                        let at = remote_offset as usize;
                        bytes.copy_from_slice(&remote.lock().unwrap()[at..at + bytes.len()]);
                        Ok(())
                    })?;
                self.done.push((transfer, index, Ok(())));
                Ok(())
            }
            fn poll(&mut self, done: &mut Vec<(u64, u32, io::Result<()>)>) -> io::Result<()> {
                done.append(&mut self.done);
                Ok(())
            }
            fn close(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
    }

    /// A hub with the fake device, and the staging arena it packs in.
    fn start_fake_rdma() -> (
        Hub,
        mpsc::UnboundedReceiver<Event>,
        String,
        Arc<LocalRegions>,
        Arena,
        Arc<Mutex<Vec<u8>>>,
    ) {
        let staging = Arena::new(1 << 20).unwrap();
        let regions = Arc::new(LocalRegions::new(
            Arena::new(1 << 20).unwrap(),
            staging.clone(),
        ));
        let remote = Arc::new(Mutex::new(vec![0u8; 1 << 20]));
        let device = fake_rdma::Device {
            remote: remote.clone(),
            regions: regions.clone(),
        };
        let listener = StdListener::bind("127.0.0.1:0").unwrap();
        let (hub, events) = Hub::start(
            &Handle::current(),
            listener,
            regions.clone(),
            None,
            64,
            Some(Arc::new(device)),
        )
        .unwrap();
        hub.set_packed(staging.clone());
        let address = hub.address().to_string();
        (hub, events, address, regions, staging, remote)
    }

    async fn frame(conn: &mut (impl AsyncRead + Unpin)) -> ToInstance {
        timeout(Duration::from_secs(5), ToInstance::read(conn, 64))
            .await
            .expect("a frame in time")
            .unwrap()
    }

    async fn dial_rdma(address: &str, token: &[u8; TOKEN_BYTES]) -> Box<dyn Conn> {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(token).await.unwrap();
        ToManager::Hello(Carrier::Rdma(vec![1]))
            .write(&mut stream)
            .await
            .unwrap();
        let ToInstance::Accept(_) = frame(&mut stream).await else {
            panic!("expected the manager's end of the pair");
        };
        Box::new(stream)
    }

    #[test]
    fn a_ranks_slices_are_gathered_into_packed_staging_and_written_into_its_slot() {
        bounded(async {
            let (hub, mut events, address, regions, staging, remote) = start_fake_rdma();
            let layout = rank_layout(3);
            let window: Vec<u8> = (0..layout.window_bytes(3, true) as usize)
                .map(|i| (i % 239) as u8)
                .collect();
            fill(&regions, Region::Arena, 0, &window);
            for rank in [0usize, 2] {
                let (transfer, token) = hub.open_rank(Kind::Get, layout.clone(), rank).unwrap();
                let mut conn = dial_rdma(&address, &token).await;
                let slot = 32768u64;
                ToManager::Credit {
                    offset: slot,
                    length: layout.slice_bytes(rank, 3, true),
                }
                .write(&mut conn)
                .await
                .unwrap();
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: rank_shape(&layout, rank, 0, 3, true),
                        page_xxh3: vec![1, 2, 3],
                        row_xxh3: 4,
                        region: Region::Arena,
                        offset: 0,
                    },
                );
                hub.finish(transfer);
                let ToInstance::Window(got) = frame(&mut conn).await else {
                    panic!("expected a window");
                };
                assert_eq!(got.slot, slot, "named by the slot it was written into");
                assert_eq!(got.shape, rank_shape(&layout, rank, 0, 3, true));
                let mut expect = vec![0u8; got.shape.length as usize];
                layout.gather(rank, &window, 3, true, &mut expect).unwrap();
                assert_eq!(
                    &remote.lock().unwrap()[slot as usize..slot as usize + expect.len()],
                    &expect[..],
                    "rank {rank}'s slice landed in its slot"
                );
                assert_eq!(frame(&mut conn).await, ToInstance::End);
                assert!(matches!(
                    next(&mut events).await,
                    Event::Sent { index: 0, .. }
                ));
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
            }
            assert_eq!(staging.used(), 0, "the packed staging came back");
        })
    }

    #[test]
    fn a_ranks_slice_is_read_into_packed_staging_and_scattered_into_the_window() {
        bounded(async {
            let (hub, mut events, address, regions, staging, remote) = start_fake_rdma();
            let layout = rank_layout(2);
            let truth: Vec<u8> = (0..layout.window_bytes(2, true) as usize)
                .map(|i| (i % 227) as u8)
                .collect();
            for rank in [1usize, 0] {
                let (transfer, token) = hub.open_rank(Kind::Put, layout.clone(), rank).unwrap();
                let mut conn = dial_rdma(&address, &token).await;
                hub.push_grant(
                    transfer,
                    GrantOut {
                        shape: rank_shape(&layout, rank, 0, 2, true),
                        region: Region::Arena,
                        offset: 8192,
                    },
                );
                let ToInstance::Grant(granted) = frame(&mut conn).await else {
                    panic!("expected a grant");
                };
                let mut packed = vec![0u8; granted.length as usize];
                layout.gather(rank, &truth, 2, true, &mut packed).unwrap();
                let slot = 65536u64;
                remote.lock().unwrap()[slot as usize..slot as usize + packed.len()]
                    .copy_from_slice(&packed);
                ToManager::Filled {
                    index: 0,
                    row_xxh3: Some(3),
                    page_xxh3: vec![1, 2],
                    slot,
                }
                .write(&mut conn)
                .await
                .unwrap();
                let Event::Filled { index, .. } = next(&mut events).await else {
                    panic!("expected Filled");
                };
                assert_eq!(index, 0);
                assert_eq!(frame(&mut conn).await, ToInstance::Taken(0));
                hub.stop(transfer);
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
            }
            regions
                .with_read(Region::Arena, 8192, truth.len(), &mut |window| {
                    assert_eq!(window, &truth[..], "both slices are in their places");
                    Ok(())
                })
                .unwrap();
            assert_eq!(staging.used(), 0, "the packed staging came back");
        })
    }

    #[cfg(feature = "rdma")]
    mod over_rdma {
        use super::*;
        use flare_selene::pool::rdma;

        /// The instance's end, over the real device: staging, a registration and a pair.
        struct Instance {
            staging: Arena,
            device: Arc<rdma::Device>,
        }

        impl Instance {
            fn new() -> Self {
                let staging = Arena::new(1 << 20).unwrap();
                let (base, len) = staging.mapping();
                // SAFETY: the arena stays mapped and in place for the life of the instance.
                let device = unsafe { rdma::Device::open(base, len) }.unwrap();
                Self { staging, device }
            }

            #[allow(clippy::mut_from_ref)]
            fn bytes(&self, offset: usize, length: usize) -> &mut [u8] {
                let (base, len) = self.staging.mapping();
                assert!(offset + length <= len);
                // SAFETY: inside the mapping; the test drives it from one task.
                unsafe { std::slice::from_raw_parts_mut(base.add(offset), length) }
            }

            /// Connect, say hello for RDMA and bring the pair up.
            async fn dial(
                &self,
                address: &str,
                token: &[u8; TOKEN_BYTES],
            ) -> (TcpStream, rdma::Pair) {
                let mut stream = TcpStream::connect(address).await.unwrap();
                stream.write_all(token).await.unwrap();
                let pair = rdma::Pair::new(&self.device).unwrap();
                ToManager::Hello(Carrier::Rdma(pair.handshake().encode().to_vec()))
                    .write(&mut stream)
                    .await
                    .unwrap();
                let ToInstance::Accept(theirs) = ToInstance::read(&mut stream, 64).await.unwrap()
                else {
                    panic!("expected the manager's end of the pair");
                };
                pair.connect(&rdma::Handshake::decode(&theirs).unwrap())
                    .unwrap();
                (stream, pair)
            }
        }

        fn start_rdma(regions: Arc<LocalRegions>) -> (Hub, mpsc::UnboundedReceiver<Event>, String) {
            let device = crate::data::ibv::Device::open(regions.clone()).unwrap();
            let listener = StdListener::bind("127.0.0.1:0").unwrap();
            let (hub, events) = Hub::start(
                &Handle::current(),
                listener,
                regions,
                None,
                64,
                Some(Arc::new(device)),
            )
            .unwrap();
            let address = hub.address().to_string();
            (hub, events, address)
        }

        #[test]
        fn a_claims_windows_are_written_into_lent_slots_and_named() {
            bounded(async {
                let regions = regions();
                let (hub, mut events, address) = start_rdma(regions.clone());
                assert!(hub.offers_rdma());
                let instance = Instance::new();
                let (transfer, token) = hub.open(Kind::Get).unwrap();
                let (mut stream, pair) = instance.dial(&address, &token).await;
                // Two slots lent, and two windows: each lands in the slot lent for it.
                for offset in [0u64, 4 * PAGE as u64] {
                    ToManager::Credit {
                        offset,
                        length: 4 * PAGE as u64,
                    }
                    .write(&mut stream)
                    .await
                    .unwrap();
                }
                let first: Vec<u8> = (0..2 * PAGE).map(|i| (i % 253) as u8).collect();
                let second: Vec<u8> = (0..PAGE).map(|i| (i % 7 + 1) as u8).collect();
                fill(&regions, Region::Arena, 0, &first);
                fill(&regions, Region::Staging, 8192, &second);
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: shape(0, 2, false),
                        page_xxh3: vec![1, 2],
                        row_xxh3: 0,
                        region: Region::Arena,
                        offset: 0,
                    },
                );
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: shape(1, 1, false),
                        page_xxh3: vec![3],
                        row_xxh3: 0,
                        region: Region::Staging,
                        offset: 8192,
                    },
                );
                hub.finish(transfer);
                let ToInstance::Window(one) = ToInstance::read(&mut stream, 64).await.unwrap()
                else {
                    panic!("expected a window");
                };
                assert_eq!(
                    (one.shape.index, one.slot, one.page_xxh3.clone()),
                    (0, 0, vec![1, 2])
                );
                assert_eq!(instance.bytes(0, 2 * PAGE), &first[..]);
                let ToInstance::Window(two) = ToInstance::read(&mut stream, 64).await.unwrap()
                else {
                    panic!("expected a second window");
                };
                assert_eq!((two.shape.index, two.slot), (1, 4 * PAGE as u64));
                assert_eq!(instance.bytes(4 * PAGE, PAGE), &second[..]);
                assert_eq!(
                    ToInstance::read(&mut stream, 64).await.unwrap(),
                    ToInstance::End
                );
                assert!(matches!(
                    next(&mut events).await,
                    Event::Sent { index: 0, .. }
                ));
                assert!(matches!(
                    next(&mut events).await,
                    Event::Sent { index: 1, .. }
                ));
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
                let moved = hub.moved();
                assert_eq!(moved.windows, [0, 2]);
                assert_eq!(moved.bytes, [0, 3 * PAGE as u64]);
                drop(pair);
            })
        }

        #[test]
        fn a_window_that_does_not_fit_the_lent_slot_fails_the_transfer() {
            bounded(async {
                let regions = regions();
                let (hub, mut events, address) = start_rdma(regions.clone());
                let instance = Instance::new();
                let (transfer, token) = hub.open(Kind::Get).unwrap();
                let (mut stream, _pair) = instance.dial(&address, &token).await;
                ToManager::Credit {
                    offset: 0,
                    length: PAGE as u64,
                }
                .write(&mut stream)
                .await
                .unwrap();
                hub.push_window(
                    transfer,
                    WindowOut {
                        shape: shape(0, 2, false),
                        page_xxh3: vec![1, 2],
                        row_xxh3: 0,
                        region: Region::Arena,
                        offset: 0,
                    },
                );
                assert!(matches!(next(&mut events).await, Event::Failed { .. }));
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
                assert!(instance.bytes(0, PAGE).iter().all(|byte| *byte == 0));
            })
        }

        #[test]
        fn a_puts_slot_is_read_into_the_window_and_released() {
            bounded(async {
                let regions = regions();
                let (hub, mut events, address) = start_rdma(regions.clone());
                let instance = Instance::new();
                let (transfer, token) = hub.open(Kind::Put).unwrap();
                let (mut stream, _pair) = instance.dial(&address, &token).await;
                hub.push_grant(
                    transfer,
                    GrantOut {
                        shape: shape(0, 2, true),
                        region: Region::Staging,
                        offset: 4096,
                    },
                );
                let ToInstance::Grant(granted) = ToInstance::read(&mut stream, 64).await.unwrap()
                else {
                    panic!("expected a grant");
                };
                assert_eq!(granted, shape(0, 2, true));
                let bytes: Vec<u8> = (0..3 * PAGE).map(|i| (i % 13) as u8).collect();
                instance
                    .bytes(2 * PAGE, bytes.len())
                    .copy_from_slice(&bytes);
                ToManager::Filled {
                    index: 0,
                    row_xxh3: Some(3),
                    page_xxh3: vec![1, 2],
                    slot: 2 * PAGE as u64,
                }
                .write(&mut stream)
                .await
                .unwrap();
                let Event::Filled {
                    index, page_xxh3, ..
                } = next(&mut events).await
                else {
                    panic!("expected Filled");
                };
                assert_eq!((index, page_xxh3), (0, vec![1, 2]));
                regions
                    .with_read(Region::Staging, 4096, bytes.len(), &mut |src| {
                        assert_eq!(src, &bytes[..]);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(
                    ToInstance::read(&mut stream, 64).await.unwrap(),
                    ToInstance::Taken(0)
                );
                hub.stop(transfer);
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
            })
        }

        #[test]
        fn stopping_an_rdma_transfer_fences_its_pair_before_it_reports_closed() {
            bounded(async {
                let regions = regions();
                let (hub, mut events, address) = start_rdma(regions.clone());
                let instance = Instance::new();
                let (transfer, token) = hub.open(Kind::Get).unwrap();
                let (mut stream, pair) = instance.dial(&address, &token).await;
                ToManager::Credit {
                    offset: 0,
                    length: 4 * PAGE as u64,
                }
                .write(&mut stream)
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(hub.handlers(), 1);
                assert!(!hub.stop(transfer));
                assert!(matches!(next(&mut events).await, Event::Closed { .. }));
                assert_eq!(hub.handlers(), 0);
                drop(pair);
            })
        }

        #[test]
        fn a_manager_without_rdma_or_with_tls_refuses_an_rdma_hello() {
            bounded(async {
                let regions = regions();
                let (hub, mut events, address) = start(regions.clone(), None);
                let instance = Instance::new();
                let (_transfer, token) = hub.open(Kind::Get).unwrap();
                let mut stream = TcpStream::connect(&address).await.unwrap();
                stream.write_all(&token).await.unwrap();
                let pair = rdma::Pair::new(&instance.device).unwrap();
                ToManager::Hello(Carrier::Rdma(pair.handshake().encode().to_vec()))
                    .write(&mut stream)
                    .await
                    .unwrap();
                let ToInstance::Abort(why) = ToInstance::read(&mut stream, 64).await.unwrap()
                else {
                    panic!("expected an abort");
                };
                assert!(why.contains("does not offer RDMA"), "{why}");
                assert!(matches!(next(&mut events).await, Event::Failed { .. }));
                // TLS and RDMA do not mix at the hub either.
                let generated =
                    rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
                let tls = server_config(
                    generated.cert.pem().as_bytes(),
                    generated.signing_key.serialize_pem().as_bytes(),
                )
                .unwrap();
                let device = crate::data::ibv::Device::open(regions.clone()).unwrap();
                let listener = StdListener::bind("127.0.0.1:0").unwrap();
                assert!(
                    Hub::start(
                        &Handle::current(),
                        listener,
                        regions,
                        Some(tls),
                        64,
                        Some(Arc::new(device)),
                    )
                    .is_err()
                );
            })
        }
    }
}
