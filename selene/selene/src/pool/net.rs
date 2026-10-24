//! The data channel's instance side: how a session without shared memory moves
//! a claim's or a put's windows.
//!
//! The manager names a connection in its grant. Each transfer gets one task that
//! connects, presents the token and moves the windows between the socket and
//! slots of the instance's own registered staging memory. Tasks decide nothing:
//! they report what arrived or went out as [`Event`]s, and the engine, which
//! owns the protocol, turns those into the same steps shared windows take.
//!
//! A slot is owned by the task from the moment it takes it until it reports the
//! slot in an event, and by the engine from then until it releases it. A task that
//! is stopped gives its slot back when it is dropped, which happens before it
//! reports `Closed`: the engine frees nothing a task could still touch.

use std::{
    collections::VecDeque,
    io,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{Notify, Semaphore, mpsc},
    task::JoinHandle,
    time::timeout,
};
use tokio_rustls::TlsConnector;

use crate::{Error, HostRegistrar, Result};

use super::{
    frame::{Carrier, NO_SLOT, Shape, TOKEN_BYTES, ToInstance, ToManager, Window},
    shm::Region,
    wire,
};

/// Connecting, including the TLS handshake, may take this long.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// A claim's frames and a window's payload may stall this long.
const IDLE_TIMEOUT: Duration = Duration::from_secs(20);
/// The manager never plans a window of more pages than this.
const MAX_PAGES: u32 = 64;
const ALIGN: u64 = 4096;

// ---------------------------------------------------------------- staging

/// Fixed-size slots in one registered region, for windows in transit.
pub(crate) struct Staging {
    region: Region,
    slot_bytes: u64,
    free: Mutex<Vec<u32>>,
    permits: Semaphore,
    slots: u32,
}

impl Staging {
    /// `slots` windows of up to `slot_bytes` each, mapped and registered with
    /// the devices that copy to and from them.
    pub(crate) fn new(
        registrar: &dyn HostRegistrar,
        context: usize,
        slots: u32,
        slot_bytes: u64,
    ) -> Result<Self> {
        let slot_bytes = slot_bytes.div_ceil(ALIGN) * ALIGN;
        let total = usize::try_from(slot_bytes * u64::from(slots))
            .map_err(|_| Error::new("the data channel's staging is too large"))?;
        let mut region = Region::anonymous(total)?;
        region.register(registrar, context)?;
        Ok(Self {
            region,
            slot_bytes,
            free: Mutex::new((0..slots).rev().collect()),
            permits: Semaphore::new(slots as usize),
            slots,
        })
    }

    pub(crate) fn region(&self) -> &Region {
        &self.region
    }

    pub(crate) fn slot_bytes(&self) -> u64 {
        self.slot_bytes
    }

    pub(crate) fn offset(&self, slot: u32) -> u64 {
        u64::from(slot) * self.slot_bytes
    }

    /// Wait for a free slot.
    async fn take(self: &Arc<Self>) -> Held {
        let permit = self
            .permits
            .acquire()
            .await
            .expect("the semaphore is never closed");
        permit.forget();
        let slot = self
            .free
            .lock()
            .unwrap()
            .pop()
            .expect("a permit stands for a free slot");
        Held {
            staging: self.clone(),
            slot: Some(slot),
        }
    }

    /// Give a slot back. The caller guarantees nothing reads or writes it.
    pub(crate) fn release(&self, slot: u32) {
        debug_assert!(slot < self.slots);
        self.free.lock().unwrap().push(slot);
        self.permits.add_permits(1);
    }

    #[cfg(test)]
    pub(crate) fn free_slots(&self) -> usize {
        self.free.lock().unwrap().len()
    }

    /// Take a slot as a task would, without waiting.
    #[cfg(test)]
    pub(crate) fn take_now(&self) -> Option<u32> {
        let permit = self.permits.try_acquire().ok()?;
        permit.forget();
        self.free.lock().unwrap().pop()
    }

    /// The bytes of a slot's first `length`.
    ///
    /// SAFETY: the caller owns the slot (see the module comment) and no other
    /// reference to its bytes exists while the slice lives.
    #[allow(clippy::mut_from_ref)]
    unsafe fn bytes(&self, slot: u32, length: u64) -> io::Result<&mut [u8]> {
        let offset = self.offset(slot);
        if length > self.slot_bytes {
            return Err(invalid("a window is larger than a slot"));
        }
        let address = self
            .region
            .address(offset, length)
            .ok_or_else(|| invalid("a slot lies outside the staging region"))?;
        // SAFETY: inside the region (checked above); exclusivity is the caller's.
        Ok(unsafe { std::slice::from_raw_parts_mut(address as *mut u8, length as usize) })
    }
}

/// A slot held by a task, returned if the task is dropped before it reports it.
struct Held {
    staging: Arc<Staging>,
    slot: Option<u32>,
}

impl Held {
    fn slot(&self) -> u32 {
        self.slot.expect("held")
    }

    /// Hand the slot to the engine.
    fn give(mut self) -> u32 {
        self.slot.take().expect("held")
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.staging.release(slot);
        }
    }
}

// ---------------------------------------------------------------- events

/// Which transfer an event is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Id {
    Claim(u64),
    Put(u64),
}

#[derive(Debug)]
pub(crate) enum Event {
    /// A claim's window is in `slot`, whole.
    Window {
        claim: u64,
        window: Window,
        slot: u32,
    },
    /// The manager ended a claim's transfer cleanly.
    End { claim: u64 },
    /// The manager granted a put's window; its bytes go in `slot`.
    Granted { put: u64, shape: Shape, slot: u32 },
    /// A put's filled window has left `slot`.
    Sent { put: u64, index: u32, slot: u32 },
    /// The transfer failed. `Closed` follows.
    Failed { id: Id, error: String },
    /// The task is gone and its connection closed.
    Closed { id: Id },
}

/// What the tasks tell the engine, in order.
pub(crate) struct Events {
    queue: Mutex<VecDeque<Event>>,
    wake: Arc<Notify>,
}

impl Events {
    pub(crate) fn new(wake: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            wake,
        })
    }

    pub(crate) fn push(&self, event: Event) {
        self.queue.lock().unwrap().push_back(event);
        self.wake.notify_one();
    }

    pub(crate) fn take(&self) -> Vec<Event> {
        self.queue.lock().unwrap().drain(..).collect()
    }
}

/// Reports `Closed` when its task is dropped, after everything the task held.
struct ClosedOnDrop {
    events: Arc<Events>,
    id: Id,
}

impl Drop for ClosedOnDrop {
    fn drop(&mut self) {
        self.events.push(Event::Closed { id: self.id });
    }
}

// ---------------------------------------------------------------- channels

/// A put's filled window, for the task to write.
pub(crate) struct Filled {
    pub index: u32,
    pub row_xxh3: Option<u64>,
    pub page_xxh3: Vec<u64>,
    pub slot: u32,
    pub length: u64,
}

/// What the engine holds of a running transfer.
pub(crate) struct Channel {
    task: Option<JoinHandle<()>>,
    filled: Option<mpsc::UnboundedSender<Filled>>,
}

impl Channel {
    /// A channel with no task, for tests that play the task themselves.
    #[cfg(test)]
    pub(crate) fn detached(put: bool) -> (Self, Option<mpsc::UnboundedReceiver<Filled>>) {
        if put {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                Self {
                    task: None,
                    filled: Some(tx),
                },
                Some(rx),
            )
        } else {
            (
                Self {
                    task: None,
                    filled: None,
                },
                None,
            )
        }
    }

    /// Stop the transfer. `Closed` is reported once the task has been dropped.
    pub(crate) fn stop(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }

    /// Queue a filled window of a put. `false` if the transfer is already gone.
    pub(crate) fn send(&self, filled: Filled) -> bool {
        self.filled
            .as_ref()
            .is_some_and(|channel| channel.send(filled).is_ok())
    }
}

/// What opening a transfer needs.
pub(crate) struct Open {
    pub id: Id,
    pub reference: wire::DataChannelRef,
}

/// Opens transfers. The engine owns one per session; tests substitute their own.
pub(crate) trait Dial: Send + Sync {
    fn open(&self, request: Open) -> io::Result<Channel>;
}

/// Opens transfers over TCP, with TLS when the manager declared it.
pub(crate) struct Tcp {
    /// The host the session was opened to, for a data address that names none.
    pub host: String,
    pub tls: Option<(TlsConnector, rustls::pki_types::ServerName<'static>)>,
    pub staging: Arc<Staging>,
    pub events: Arc<Events>,
}

impl Dial for Tcp {
    fn open(&self, request: Open) -> io::Result<Channel> {
        let token: [u8; TOKEN_BYTES] = request
            .reference
            .token
            .as_slice()
            .try_into()
            .map_err(|_| invalid("the data channel's token is not 32 bytes"))?;
        if request.reference.transport != wire::DataTransport::Tcp as i32 {
            return Err(invalid("the data channel is not offered over TCP"));
        }
        let target = Target {
            host: self.host.clone(),
            address: request.reference.address.clone(),
            tls: self.tls.clone(),
            token,
            staging: self.staging.clone(),
            events: self.events.clone(),
        };
        let (events, id) = (self.events.clone(), request.id);
        match request.id {
            Id::Claim(_) => {
                let task = tokio::spawn(async move {
                    let _closed = ClosedOnDrop {
                        events: events.clone(),
                        id,
                    };
                    if let Err(error) = receive(&target, id).await {
                        events.push(Event::Failed {
                            id,
                            error: error.to_string(),
                        });
                    }
                });
                Ok(Channel {
                    task: Some(task),
                    filled: None,
                })
            }
            Id::Put(_) => {
                let (filled, commands) = mpsc::unbounded_channel();
                let task = tokio::spawn(async move {
                    let _closed = ClosedOnDrop {
                        events: events.clone(),
                        id,
                    };
                    if let Err(error) = fill(&target, id, commands).await {
                        events.push(Event::Failed {
                            id,
                            error: error.to_string(),
                        });
                    }
                });
                Ok(Channel {
                    task: Some(task),
                    filled: Some(filled),
                })
            }
        }
    }
}

/// A TLS connector trusting the CA file's certificates, and the name to expect.
pub(crate) fn tls_for(
    ca: &Path,
    host: &str,
) -> Result<(TlsConnector, rustls::pki_types::ServerName<'static>)> {
    use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
    let pem = std::fs::read(ca).map_err(|e| Error::caused_by("cannot read the CA file", e))?;
    let mut roots = rustls::RootCertStore::empty();
    let mut count = 0;
    for certificate in CertificateDer::pem_slice_iter(&pem) {
        let certificate =
            certificate.map_err(|e| Error::new(format!("the CA file is not PEM: {e}")))?;
        roots
            .add(certificate)
            .map_err(|e| Error::new(format!("the CA file holds a bad certificate: {e}")))?;
        count += 1;
    }
    if count == 0 {
        return Err(Error::new("the CA file holds no certificate"));
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(format!("TLS: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from(host.trim_matches(['[', ']']).to_string())
        .map_err(|e| Error::new(format!("the pool host is not a valid TLS name: {e}")))?;
    Ok((TlsConnector::from(Arc::new(config)), name))
}

// ---------------------------------------------------------------- tasks

struct Target {
    host: String,
    address: String,
    tls: Option<(TlsConnector, rustls::pki_types::ServerName<'static>)>,
    token: [u8; TOKEN_BYTES],
    staging: Arc<Staging>,
    events: Arc<Events>,
}

trait Conn: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Conn for T {}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn timed_out(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, what.to_string())
}

async fn connect(target: &Target) -> io::Result<Box<dyn Conn>> {
    let (host, port) = target
        .address
        .rsplit_once(':')
        .ok_or_else(|| invalid("the data channel's address has no port"))?;
    let port: u16 = port
        .parse()
        .map_err(|_| invalid("the data channel's port is not a number"))?;
    // A manager bound to every interface cannot name the one clients reach it by.
    let named = host.trim_matches(['[', ']']);
    let host = match named.parse::<std::net::IpAddr>() {
        Ok(ip) if ip.is_unspecified() => target.host.as_str(),
        _ => named,
    };
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| timed_out("connecting to the data channel timed out"))??;
    stream.set_nodelay(true)?;
    let mut conn: Box<dyn Conn> = match &target.tls {
        Some((connector, name)) => Box::new(
            timeout(CONNECT_TIMEOUT, connector.connect(name.clone(), stream))
                .await
                .map_err(|_| timed_out("the data channel's TLS handshake timed out"))??,
        ),
        None => Box::new(stream),
    };
    conn.write_all(&target.token).await?;
    ToManager::Hello(Carrier::Stream).write(&mut conn).await?;
    conn.flush().await?;
    Ok(conn)
}

/// A claim: windows arrive until the manager ends the transfer.
async fn receive(target: &Target, id: Id) -> io::Result<()> {
    let Id::Claim(claim) = id else {
        unreachable!("receive serves claims");
    };
    let mut conn = connect(target).await?;
    loop {
        let frame = timeout(IDLE_TIMEOUT, ToInstance::read(&mut conn, MAX_PAGES))
            .await
            .map_err(|_| timed_out("the manager sent nothing"))??;
        match frame {
            ToInstance::Window(window) => {
                let length = window.shape.length;
                if window.slot != NO_SLOT {
                    return Err(invalid("a window names a slot on the stream carrier"));
                }
                if length == 0 || length % ALIGN != 0 || length > target.staging.slot_bytes() {
                    return Err(invalid(format!(
                        "window {} has an unusable length ({length})",
                        window.shape.index
                    )));
                }
                // The slot is ours until the event names it.
                let held = target.staging.take().await;
                // SAFETY: `held` is a slot this task owns.
                let bytes = unsafe { target.staging.bytes(held.slot(), length)? };
                timeout(IDLE_TIMEOUT, conn.read_exact(bytes))
                    .await
                    .map_err(|_| timed_out("the manager stopped sending a window"))??;
                target.events.push(Event::Window {
                    claim,
                    window,
                    slot: held.give(),
                });
            }
            ToInstance::End => {
                target.events.push(Event::End { claim });
                return Ok(());
            }
            ToInstance::Abort(text) => {
                return Err(io::Error::other(format!(
                    "the manager ended the transfer: {text}"
                )));
            }
            _ => return Err(invalid("a claim's channel carries only windows")),
        }
    }
}

/// A put: the manager grants windows and the engine fills and returns them.
async fn fill(
    target: &Target,
    id: Id,
    mut filled: mpsc::UnboundedReceiver<Filled>,
) -> io::Result<()> {
    let Id::Put(put) = id else {
        unreachable!("fill serves puts");
    };
    let conn = connect(target).await?;
    let (mut reader, mut writer) = tokio::io::split(conn);
    let reading = async {
        loop {
            // Grants come as the manager's store makes them: no deadline. A manager
            // that closes the connection between frames is ending the put, and says
            // how on the session; the engine fails a put it did not finish.
            let Some(frame) = ToInstance::read_or_end(&mut reader, MAX_PAGES).await? else {
                return Ok(());
            };
            match frame {
                ToInstance::Grant(shape) => {
                    let length = shape.length;
                    if length == 0 || length % ALIGN != 0 || length > target.staging.slot_bytes() {
                        return Err(invalid(format!(
                            "window {} has an unusable length ({length})",
                            shape.index
                        )));
                    }
                    let held = target.staging.take().await;
                    target.events.push(Event::Granted {
                        put,
                        shape,
                        slot: held.give(),
                    });
                }
                ToInstance::Abort(text) => {
                    return Err(io::Error::other(format!(
                        "the manager ended the transfer: {text}"
                    )));
                }
                _ => return Err(invalid("a put's channel carries only grants")),
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), io::Error>(())
    };
    let writing = async {
        while let Some(window) = filled.recv().await {
            // SAFETY: the engine handed the slot over with the window and does not touch
            // it until `Sent`.
            let bytes = unsafe { target.staging.bytes(window.slot, window.length)? };
            let frame = ToManager::Filled {
                index: window.index,
                row_xxh3: window.row_xxh3,
                page_xxh3: window.page_xxh3,
                slot: NO_SLOT,
            };
            timeout(IDLE_TIMEOUT, async {
                frame.write(&mut writer).await?;
                writer.write_all(bytes).await?;
                writer.flush().await
            })
            .await
            .map_err(|_| timed_out("the manager stopped reading a window"))??;
            target.events.push(Event::Sent {
                put,
                index: window.index,
                slot: window.slot,
            });
        }
        Ok::<(), io::Error>(())
    };
    // Either side ending, cleanly or not, ends the transfer.
    tokio::select! {
        result = reading => result,
        result = writing => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Registration, record::checksum};
    use tokio::net::TcpListener;

    struct Reg;
    impl Registration for Reg {
        fn device_address(&self) -> u64 {
            0
        }
    }
    struct OkRegistrar;
    impl HostRegistrar for OkRegistrar {
        fn register(&self, _: usize, _: usize, _: usize) -> Result<Box<dyn Registration>> {
            Ok(Box::new(Reg))
        }
        fn map_split(&self, _: &[(usize, i32, usize)]) -> Result<Box<dyn crate::SplitMapping>> {
            Err("not used".into())
        }
    }

    const PAGE: usize = 4096;

    fn staging(slots: u32) -> Arc<Staging> {
        Arc::new(Staging::new(&OkRegistrar, 0, slots, 4 * PAGE as u64).unwrap())
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

    struct Rig {
        listener: TcpListener,
        staging: Arc<Staging>,
        events: Arc<Events>,
        wake: Arc<Notify>,
        dial: Tcp,
    }

    fn rig(slots: u32, tls: Option<(TlsConnector, rustls::pki_types::ServerName<'static>)>) -> Rig {
        let listener = {
            let std = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            std.set_nonblocking(true).unwrap();
            TcpListener::from_std(std).unwrap()
        };
        let staging = staging(slots);
        let wake = Arc::new(Notify::new());
        let events = Events::new(wake.clone());
        let dial = Tcp {
            host: "127.0.0.1".into(),
            tls,
            staging: staging.clone(),
            events: events.clone(),
        };
        Rig {
            listener,
            staging,
            events,
            wake,
            dial,
        }
    }

    impl Rig {
        fn reference(&self, token: u8) -> wire::DataChannelRef {
            wire::DataChannelRef {
                transport: wire::DataTransport::Tcp as i32,
                address: self.listener.local_addr().unwrap().to_string(),
                token: vec![token; TOKEN_BYTES],
                rdma: vec![],
            }
        }

        /// Events until `want` accepts one; the others are returned too.
        async fn until(&self, mut want: impl FnMut(&Event) -> bool) -> Vec<Event> {
            let mut seen = Vec::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                for event in self.events.take() {
                    let done = want(&event);
                    seen.push(event);
                    if done {
                        return seen;
                    }
                }
                tokio::time::timeout_at(deadline, self.wake.notified())
                    .await
                    .expect("an event in time");
            }
        }

        /// Accept the task's connection and read its token and hello.
        async fn accept(&self) -> (TcpStream, [u8; TOKEN_BYTES]) {
            let (mut stream, _) = self.listener.accept().await.unwrap();
            let mut token = [0u8; TOKEN_BYTES];
            stream.read_exact(&mut token).await.unwrap();
            let hello = ToManager::read(&mut stream, MAX_PAGES).await.unwrap();
            assert_eq!(hello, ToManager::Hello(Carrier::Stream));
            (stream, token)
        }
    }

    fn window_frame(shape: Shape, bytes: &[u8]) -> Window {
        Window {
            shape,
            page_xxh3: (0..shape.page_count as usize)
                .map(|i| checksum(&bytes[i * PAGE..(i + 1) * PAGE]))
                .collect(),
            row_xxh3: if shape.row {
                checksum(&bytes[shape.page_count as usize * PAGE..])
            } else {
                0
            },
            slot: NO_SLOT,
        }
    }

    #[tokio::test]
    async fn a_claims_windows_land_whole_in_slots_and_the_task_reports_the_end() {
        let rig = rig(4, None);
        let channel = rig
            .dial
            .open(Open {
                id: Id::Claim(3),
                reference: rig.reference(5),
            })
            .unwrap();
        let (mut stream, token) = rig.accept().await;
        assert_eq!(token, [5; TOKEN_BYTES]);
        let first: Vec<u8> = (0..2 * PAGE).map(|i| (i % 253) as u8).collect();
        let second: Vec<u8> = (0..2 * PAGE).map(|i| (i % 7) as u8).collect();
        for (index, bytes, row) in [(0u32, &first, false), (1, &second, true)] {
            let pages = if row { 1 } else { 2 };
            let shape = shape(index, pages, row);
            ToInstance::Window(window_frame(shape, bytes))
                .write(&mut stream)
                .await
                .unwrap();
            stream
                .write_all(&bytes[..shape.length as usize])
                .await
                .unwrap();
        }
        ToInstance::End.write(&mut stream).await.unwrap();
        let events = rig
            .until(|e| matches!(e, Event::Closed { id: Id::Claim(3) }))
            .await;
        let mut windows = Vec::new();
        let mut ended = false;
        for event in events {
            match event {
                Event::Window {
                    claim: 3,
                    window,
                    slot,
                } => {
                    let bytes = rig
                        .staging
                        .region()
                        .bytes(rig.staging.offset(slot), window.shape.length)
                        .unwrap()
                        .to_vec();
                    windows.push((window, bytes, slot));
                }
                Event::End { claim: 3 } => ended = true,
                Event::Closed { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(ended);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].1, first);
        assert_eq!(windows[1].1, second[..PAGE + PAGE]);
        assert_eq!(windows[1].0.shape.index, 1);
        // The engine owns the slots until it gives them back.
        assert_eq!(rig.staging.free_slots(), 2);
        for (_, _, slot) in windows {
            rig.staging.release(slot);
        }
        assert_eq!(rig.staging.free_slots(), 4);
        drop(channel);
    }

    #[tokio::test]
    async fn a_task_stopped_while_waiting_for_a_slot_gives_nothing_away_and_reports_closed() {
        let rig = rig(1, None);
        // The only slot is taken, so the first window waits for it.
        let held = rig.staging.take_now().unwrap();
        let channel = rig
            .dial
            .open(Open {
                id: Id::Claim(1),
                reference: rig.reference(1),
            })
            .unwrap();
        let (mut stream, _) = rig.accept().await;
        ToInstance::Window(window_frame(shape(0, 1, false), &[1; PAGE]))
            .write(&mut stream)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        channel.stop();
        rig.until(|e| matches!(e, Event::Closed { .. })).await;
        assert_eq!(
            rig.staging.free_slots(),
            0,
            "the engine's slot is not touched"
        );
        rig.staging.release(held);
        assert_eq!(rig.staging.free_slots(), 1);
    }

    #[tokio::test]
    async fn a_connection_that_breaks_mid_window_fails_the_claim_and_returns_the_slot() {
        let rig = rig(2, None);
        let _channel = rig
            .dial
            .open(Open {
                id: Id::Claim(2),
                reference: rig.reference(1),
            })
            .unwrap();
        let (mut stream, _) = rig.accept().await;
        ToInstance::Window(window_frame(shape(0, 2, false), &[1; 2 * PAGE]))
            .write(&mut stream)
            .await
            .unwrap();
        stream.write_all(&[1; PAGE]).await.unwrap();
        drop(stream);
        let events = rig.until(|e| matches!(e, Event::Closed { .. })).await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                Event::Failed {
                    id: Id::Claim(2),
                    ..
                }
            )),
            "{events:?}"
        );
        assert!(!events.iter().any(|e| matches!(e, Event::Window { .. })));
        assert_eq!(
            rig.staging.free_slots(),
            2,
            "the slot being filled came back"
        );
    }

    #[tokio::test]
    async fn a_window_longer_than_a_slot_or_off_the_page_grid_ends_the_transfer() {
        let rig = rig(2, None);
        for length in [5 * PAGE as u64, 100] {
            let _channel = rig
                .dial
                .open(Open {
                    id: Id::Claim(4),
                    reference: rig.reference(1),
                })
                .unwrap();
            let (mut stream, _) = rig.accept().await;
            let mut odd = shape(0, 1, false);
            odd.length = length;
            ToInstance::Window(Window {
                shape: odd,
                page_xxh3: vec![0],
                row_xxh3: 0,
                slot: NO_SLOT,
            })
            .write(&mut stream)
            .await
            .unwrap();
            let events = rig.until(|e| matches!(e, Event::Closed { .. })).await;
            assert!(
                events.iter().any(|e| matches!(e, Event::Failed { .. })),
                "{events:?}"
            );
        }
        assert_eq!(rig.staging.free_slots(), 2);
    }

    #[tokio::test]
    async fn an_abort_from_the_manager_fails_the_claim_with_its_words() {
        let rig = rig(2, None);
        let _channel = rig
            .dial
            .open(Open {
                id: Id::Claim(5),
                reference: rig.reference(1),
            })
            .unwrap();
        let (mut stream, _) = rig.accept().await;
        ToInstance::Abort("the claim ended".into())
            .write(&mut stream)
            .await
            .unwrap();
        let events = rig.until(|e| matches!(e, Event::Closed { .. })).await;
        assert!(events.iter().any(
            |e| matches!(e, Event::Failed { error, .. } if error.contains("the claim ended"))
        ));
    }

    #[tokio::test]
    async fn a_put_is_granted_slots_and_writes_what_the_engine_hands_back() {
        let rig = rig(2, None);
        let channel = rig
            .dial
            .open(Open {
                id: Id::Put(6),
                reference: rig.reference(9),
            })
            .unwrap();
        let (mut stream, token) = rig.accept().await;
        assert_eq!(token, [9; TOKEN_BYTES]);
        ToInstance::Grant(shape(0, 2, true))
            .write(&mut stream)
            .await
            .unwrap();
        let events = rig.until(|e| matches!(e, Event::Granted { .. })).await;
        let Some(Event::Granted {
            put: 6,
            shape: granted,
            slot,
        }) = events.into_iter().last()
        else {
            panic!("expected a grant");
        };
        assert_eq!(granted, shape(0, 2, true));
        // The engine fills the slot and hands it to the task.
        let bytes: Vec<u8> = (0..3 * PAGE).map(|i| (i % 91) as u8).collect();
        rig.staging
            .region()
            .bytes_mut(rig.staging.offset(slot), bytes.len() as u64)
            .unwrap()
            .copy_from_slice(&bytes);
        assert!(channel.send(Filled {
            index: 0,
            row_xxh3: Some(11),
            page_xxh3: vec![1, 2],
            slot,
            length: bytes.len() as u64,
        }));
        let frame = ToManager::read(&mut stream, MAX_PAGES).await.unwrap();
        assert_eq!(
            frame,
            ToManager::Filled {
                index: 0,
                row_xxh3: Some(11),
                page_xxh3: vec![1, 2],
                slot: NO_SLOT
            }
        );
        let mut got = vec![0u8; bytes.len()];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(got, bytes);
        let events = rig.until(|e| matches!(e, Event::Sent { .. })).await;
        assert!(matches!(
            events.last(),
            Some(Event::Sent { put: 6, index: 0, slot: sent }) if *sent == slot
        ));
        // Stopping the put ends the task; it reports Closed and the slot stays the engine's.
        channel.stop();
        rig.until(|e| matches!(e, Event::Closed { id: Id::Put(6) }))
            .await;
        assert_eq!(rig.staging.free_slots(), 1);
        rig.staging.release(slot);
    }

    #[tokio::test]
    async fn a_data_address_naming_no_host_is_reached_at_the_host_of_the_session() {
        let rig = rig(1, None);
        let port = rig.listener.local_addr().unwrap().port();
        let mut reference = rig.reference(3);
        reference.address = format!("0.0.0.0:{port}");
        let _channel = rig
            .dial
            .open(Open {
                id: Id::Claim(7),
                reference,
            })
            .unwrap();
        let (_stream, token) = rig.accept().await;
        assert_eq!(token, [3; TOKEN_BYTES]);
    }

    #[tokio::test]
    async fn a_reference_this_client_cannot_use_is_refused_before_connecting() {
        let rig = rig(1, None);
        let mut reference = rig.reference(1);
        reference.token = vec![1; 31];
        assert!(
            rig.dial
                .open(Open {
                    id: Id::Claim(8),
                    reference
                })
                .is_err()
        );
        let mut reference = rig.reference(1);
        reference.transport = wire::DataTransport::Rdma as i32;
        assert!(
            rig.dial
                .open(Open {
                    id: Id::Claim(8),
                    reference
                })
                .is_err()
        );
    }

    fn tls_pair() -> (
        Arc<rustls::ServerConfig>,
        (TlsConnector, rustls::pki_types::ServerName<'static>),
        std::path::PathBuf,
    ) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "selene-net-tls-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = dir.join("ca.pem");
        std::fs::write(&ca, generated.cert.pem()).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let certificate = rustls::pki_types::CertificateDer::from(generated.cert.der().to_vec());
        let key = rustls::pki_types::PrivateKeyDer::try_from(generated.signing_key.serialize_der())
            .unwrap();
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .unwrap();
        let client = tls_for(&ca, "localhost").unwrap();
        (Arc::new(server), client, dir)
    }

    #[tokio::test]
    async fn a_claim_over_tls_trusts_only_its_ca() {
        let (server, client, dir) = tls_pair();
        let rig = rig(2, Some(client));
        let acceptor = tokio_rustls::TlsAcceptor::from(server);
        let channel = rig
            .dial
            .open(Open {
                id: Id::Claim(1),
                reference: rig.reference(4),
            })
            .unwrap();
        let (tcp, _) = rig.listener.accept().await.unwrap();
        let mut stream = acceptor.accept(tcp).await.unwrap();
        let mut token = [0u8; TOKEN_BYTES];
        stream.read_exact(&mut token).await.unwrap();
        assert_eq!(token, [4; TOKEN_BYTES]);
        assert_eq!(
            ToManager::read(&mut stream, MAX_PAGES).await.unwrap(),
            ToManager::Hello(Carrier::Stream)
        );
        let bytes = vec![6u8; PAGE];
        ToInstance::Window(window_frame(shape(0, 1, false), &bytes))
            .write(&mut stream)
            .await
            .unwrap();
        stream.write_all(&bytes).await.unwrap();
        ToInstance::End.write(&mut stream).await.unwrap();
        stream.flush().await.unwrap();
        let events = rig.until(|e| matches!(e, Event::Closed { .. })).await;
        assert!(events.iter().any(|e| matches!(e, Event::Window { .. })));
        drop(channel);
        // A manager whose certificate is not the CA's fails the handshake.
        let (other_server, _, other_dir) = tls_pair();
        let rig = rig_with_same_client(&rig);
        let acceptor = tokio_rustls::TlsAcceptor::from(other_server);
        let _channel = rig
            .dial
            .open(Open {
                id: Id::Claim(2),
                reference: rig.reference(4),
            })
            .unwrap();
        let (tcp, _) = rig.listener.accept().await.unwrap();
        let _ = acceptor.accept(tcp).await;
        let events = rig.until(|e| matches!(e, Event::Closed { .. })).await;
        assert!(
            events.iter().any(|e| matches!(e, Event::Failed { .. })),
            "{events:?}"
        );
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(other_dir);
    }

    /// A fresh listener and event queue with the same TLS client as `rig`.
    fn rig_with_same_client(rig: &Rig) -> Rig {
        self::rig(2, rig.dial.tls.clone())
    }

    #[test]
    fn slots_are_taken_in_turn_and_returned_whole() {
        let staging = staging(3);
        let taken: Vec<u32> = (0..3).map(|_| staging.take_now().unwrap()).collect();
        assert!(staging.take_now().is_none());
        let mut sorted = taken.clone();
        sorted.sort();
        assert_eq!(sorted, [0, 1, 2]);
        staging.release(taken[1]);
        assert_eq!(staging.take_now(), Some(taken[1]));
        assert_eq!(staging.slot_bytes(), 4 * PAGE as u64);
        assert_eq!(staging.offset(2), 2 * 4 * PAGE as u64);
    }

    #[test]
    fn a_ca_file_with_no_certificate_or_an_unreadable_one_is_refused() {
        let dir = std::env::temp_dir().join(format!("selene-net-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, "nothing here").unwrap();
        assert!(tls_for(&empty, "localhost").is_err());
        assert!(tls_for(&dir.join("missing.pem"), "localhost").is_err());
        let _ = std::fs::remove_dir_all(dir);
    }
}
