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

use flare_selene::pool::frame::{Carrier, Shape, TOKEN_BYTES, ToInstance, ToManager, Window};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::{Notify, mpsc, watch},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;

use super::{Region, RegionAccess, invalid};

/// A connection that has not presented its token by then is dropped.
const TOKEN_TIMEOUT: Duration = Duration::from_secs(5);
/// A write or a read that makes no progress for this long ends the transfer.
const IO_TIMEOUT: Duration = Duration::from_secs(20);
/// The most a closing connection may take to say goodbye.
const GOODBYE: Duration = Duration::from_millis(200);
const SOCKET_BUFFER: u64 = 16 << 20;
/// Pages are multiples of this, so a window holds at most `max_window / PAGE_FLOOR` of them.
const PAGE_FLOOR: u64 = 4096;

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

struct Inner {
    address: String,
    regions: Arc<dyn RegionAccess>,
    acceptor: Option<TlsAcceptor>,
    max_window: u64,
    max_pages: u32,
    events: mpsc::UnboundedSender<Event>,
    state: Mutex<State>,
    next: AtomicU64,
    handle: Handle,
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
    /// Serve the data channel on `listener`. `max_window` bounds a window's
    /// payload, and with it the pages a window may name. Without `tls` the
    /// connection is plaintext.
    pub fn start(
        handle: &Handle,
        listener: std::net::TcpListener,
        regions: Arc<dyn RegionAccess>,
        tls: Option<Arc<rustls::ServerConfig>>,
        max_window: u64,
    ) -> io::Result<(Hub, mpsc::UnboundedReceiver<Event>)> {
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?.to_string();
        let (events, receiver) = mpsc::unbounded_channel();
        let (closing, closed) = watch::channel(false);
        let inner = Arc::new(Inner {
            address,
            regions,
            acceptor: tls.map(TlsAcceptor::from),
            max_window,
            max_pages: u32::try_from(max_window / PAGE_FLOOR)
                .unwrap_or(u32::MAX)
                .max(1),
            events,
            state: Mutex::new(State {
                open: true,
                ..State::default()
            }),
            next: AtomicU64::new(1),
            handle: handle.clone(),
        });
        let accepting = inner.clone();
        let listener = {
            let _guard = handle.enter();
            TcpListener::from_std(listener)?
        };
        handle.spawn(accept_loop(accepting, listener, closed));
        Ok((Hub { inner, closing }, receiver))
    }

    /// The `host:port` clients are told to connect to. A host that is the
    /// unspecified address means "the host you reached me by".
    pub fn address(&self) -> &str {
        &self.inner.address
    }

    /// Whether connections are encrypted.
    pub fn encrypted(&self) -> bool {
        self.inner.acceptor.is_some()
    }

    /// Begin a transfer. Its token authenticates the one connection that serves it.
    pub fn open(&self, kind: Kind) -> io::Result<(u64, [u8; TOKEN_BYTES])> {
        let token = random_token()?;
        let transfer = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let (stopped, _) = watch::channel(false);
        let entry = Arc::new(Transfer {
            kind,
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
    let Ok(Ok((conn, token))) = accepted else {
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
    let result = tokio::select! {
        result = run(&inner, transfer, &entry, conn) => result,
        _ = async { let _ = stopped.wait_for(|stop| *stop).await; } => End::Stopped,
    };
    // `run` owned the connection and dropped it with its future, whichever branch
    // won; only now is the transfer's memory quiet.
    entry.queues.lock().unwrap().attached = false;
    if let End::Failed(reason) = result {
        let _ = inner.events.send(Event::Failed { transfer, reason });
    }
    let _ = inner.events.send(Event::Closed { transfer });
}

async fn run(inner: &Inner, transfer: u64, entry: &Transfer, mut conn: Box<dyn Conn>) -> End {
    let hello = match timeout(IO_TIMEOUT, ToManager::read(&mut conn, inner.max_pages)).await {
        Ok(Ok(frame)) => frame,
        Ok(Err(error)) => return failure(error),
        Err(_) => return End::Failed("the instance sent no hello".into()),
    };
    if hello != ToManager::Hello(Carrier::Tcp) {
        return End::Failed("the instance did not ask for the stream carrier".into());
    }
    let result = match entry.kind {
        Kind::Get => send_windows(inner, transfer, entry, &mut conn).await,
        Kind::Put => receive_windows(inner, transfer, entry, conn).await,
    };
    match result {
        Ok(()) => End::Done,
        Err(error) => failure(error),
    }
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
        if window.shape.length > inner.max_window {
            return Err(invalid("a window is larger than the window limit"));
        }
        // SAFETY: see `window_bytes`; the slice is only read.
        let payload = unsafe {
            window_bytes(
                &*inner.regions,
                window.region,
                window.offset,
                window.shape.length,
            )?
        };
        let frame = ToInstance::Window(Window {
            shape: window.shape,
            page_xxh3: window.page_xxh3,
            row_xxh3: window.row_xxh3,
        });
        timeout(IO_TIMEOUT, async {
            frame.write(conn).await?;
            conn.write_all(payload).await?;
            conn.flush().await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the instance stopped reading"))??;
        let _ = inner.events.send(Event::Sent {
            transfer,
            index: window.shape.index,
        });
    }
}

async fn receive_windows(
    inner: &Inner,
    transfer: u64,
    entry: &Transfer,
    conn: Box<dyn Conn>,
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
                if grant.shape.length > inner.max_window {
                    return Err(invalid("a window is larger than the window limit"));
                }
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
            } = frame
            else {
                return Err(invalid("the instance sent a frame a put does not take"));
            };
            if index != grant.shape.index
                || page_xxh3.len() != grant.shape.page_count as usize
                || row_xxh3.is_some() != grant.shape.row
            {
                return Err(invalid("a filled window does not match its grant"));
            }
            // SAFETY: see `window_bytes`; the store leased this window to the put.
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
            Hub::start(&Handle::current(), listener, regions, tls, 1 << 20).unwrap();
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
        ToManager::Hello(Carrier::Tcp)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claims_windows_arrive_in_order_with_their_checksums_and_end() {
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_token_serves_one_connection_and_a_wrong_token_none() {
        let (hub, mut events, address) = start(regions(), None);
        let (transfer, token) = hub.open(Kind::Get).unwrap();
        let _first = connect(&address, &token).await;
        // The token is spent: a second connection is closed without a byte.
        let mut second = connect(&address, &token).await;
        assert!(ToInstance::read(&mut second, 64).await.is_err());
        let mut wrong = connect(&address, &[9; TOKEN_BYTES]).await;
        assert!(ToInstance::read(&mut wrong, 64).await.is_err());
        assert!(!hub.stop(transfer), "a handler is attached");
        assert!(matches!(next(&mut events).await, Event::Closed { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopping_before_any_connection_needs_no_wait_and_blocks_a_late_one() {
        let (hub, _events, address) = start(regions(), None);
        let (transfer, token) = hub.open(Kind::Get).unwrap();
        assert!(hub.stop(transfer), "no handler ever ran");
        let mut late = connect(&address, &token).await;
        assert!(ToInstance::read(&mut late, 64).await.is_err());
        assert_eq!(hub.handlers(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopping_a_stalled_transfer_ends_its_handler_and_reports_closed() {
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_peer_that_drops_mid_transfer_fails_it_and_closes_the_handler() {
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_puts_windows_are_filled_from_what_the_instance_writes() {
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_filled_window_that_disagrees_with_its_grant_fails_the_put_without_writing() {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tls_hub_serves_a_client_that_trusts_it_and_refuses_one_that_does_not() {
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
        ToManager::Hello(Carrier::Tcp)
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
}
