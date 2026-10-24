use crate::{
    Key, Result,
    data::{DataChannel, GrantKey, Region, RegionAccess},
    grants::SharedGrants,
    invalid, proto,
    proto::{
        get_message, get_reply, local_cache_server::LocalCache,
        local_cache_server::LocalCacheServer, peer_cache_server::PeerCache,
        peer_cache_server::PeerCacheServer, put_message, put_reply,
    },
    record::{Descriptor, RankSizes},
    regions::LocalRegions,
    store::{self, AdminCommand, Config, GetAnswer, NO_PROGRESS, PutAnswer, Store, StoreCommand},
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Read, Write},
    net::TcpListener,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot},
};
use tokio_stream::{
    StreamExt, wrappers::ReceiverStream, wrappers::TcpListenerStream, wrappers::UnixListenerStream,
};
use tonic::{
    Request, Response, Status, Streaming, transport::Server, transport::server::Connected,
};

const MAX_MESSAGE: usize = 16 * 1024 * 1024;
const GENERATION: &str = "x-selene-generation";

type Epochs = HashMap<(u64, [u8; 32]), Arc<AtomicU64>>;

type SubscribeStream = Pin<
    Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::CatalogEvent, Status>> + Send>,
>;
type PutStream =
    Pin<Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::PutReply, Status>> + Send>>;
type GetStream =
    Pin<Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::GetReply, Status>> + Send>>;

#[derive(Clone)]
pub struct PeerInfo {
    pub connection: u64,
    /// A Unix-domain connection, whose credentials are the kernel's.
    pub unix: bool,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}

/// Everything a peer connection needs from the storage manager: the data listener identity,
/// the shared mappings and the live-window table.
pub struct PeerData {
    pub stream_addr: String,
    pub regions: Arc<LocalRegions>,
    pub peer_sizes: crate::data::stream::PeerSizes,
    pub grants: Arc<SharedGrants>,
    pub sessions: Arc<crate::grants::Sessions>,
}

/// Clears every window of one transfer when its task ends, whatever the exit path.
struct GrantGuard {
    grants: Arc<SharedGrants>,
    session: u64,
    transfer: u64,
}

impl Drop for GrantGuard {
    fn drop(&mut self) {
        self.grants.remove_transfer(self.session, self.transfer);
    }
}

fn region_from_wire(value: i32) -> Option<Region> {
    match proto::Region::try_from(value) {
        Ok(proto::Region::Arena) => Some(Region::Arena),
        Ok(proto::Region::Staging) => Some(Region::Staging),
        _ => None,
    }
}

impl Connected for PeerStream {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> PeerInfo {
        self.info.clone()
    }
}

pub struct PeerStream {
    inner: tokio::net::UnixStream,
    info: PeerInfo,
    commands: mpsc::Sender<StoreCommand>,
    runtime: tokio::runtime::Handle,
    generations: Arc<Mutex<HashMap<u64, Generation>>>,
    over_limit: bool,
    _slot: ConnectionSlot,
}

struct ConnectionSlot(Arc<AtomicUsize>);

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl AsyncRead for PeerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.over_limit {
            // A clean EOF closes the excess transport without failing the
            // whole incoming stream.
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.over_limit {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection limit",
            )));
        }
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl Drop for PeerStream {
    fn drop(&mut self) {
        self.generations
            .lock()
            .unwrap()
            .remove(&self.info.connection);
        let command = StoreCommand::ConnectionClosed {
            connection: self.info.connection,
        };
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let commands = self.commands.clone();
                self.runtime.spawn(async move {
                    let _ = commands.send(command).await;
                });
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// One accepted peer connection over TCP (optionally inside TLS).
pub struct TcpPeerStream {
    inner: tokio::net::TcpStream,
    info: PeerInfo,
    commands: mpsc::Sender<StoreCommand>,
    runtime: tokio::runtime::Handle,
    generations: Arc<Mutex<HashMap<u64, Generation>>>,
    rdma_sessions: Arc<Mutex<HashMap<u64, Box<dyn DataChannel>>>>,
    sessions: Arc<crate::grants::Sessions>,
}

impl Connected for TcpPeerStream {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> PeerInfo {
        self.info.clone()
    }
}

impl AsyncRead for TcpPeerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl AsyncWrite for TcpPeerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

impl Drop for TcpPeerStream {
    fn drop(&mut self) {
        self.generations
            .lock()
            .unwrap()
            .remove(&self.info.connection);
        self.rdma_sessions
            .lock()
            .unwrap()
            .remove(&self.info.connection);
        // Stop the session's data sockets and let their copies finish before the store frees
        // the windows those copies may still be writing.
        let sessions = self.sessions.clone();
        let connection = self.info.connection;
        let commands = self.commands.clone();
        sessions.stop(connection);
        self.runtime.spawn(async move {
            let _ = tokio::task::spawn_blocking(move || {
                sessions.drain(connection);
                sessions.forget_session(connection);
            })
            .await;
            let _ = commands
                .send(StoreCommand::ConnectionClosed { connection })
                .await;
        });
    }
}

#[derive(Default)]
struct Generation {
    current: u64,
    issued: HashSet<u64>,
}

#[derive(Clone)]
pub struct Service {
    commands: mpsc::Sender<StoreCommand>,
    generations: Arc<Mutex<HashMap<u64, Generation>>>,
    transfers: Arc<AtomicU64>,
    grants: Arc<SharedGrants>,
    /// Set only when this manager serves peers (`--listen-peer`).
    peer: Option<Arc<PeerData>>,
    /// One connected server-side QP per peer connection, kept alive for the session. Populated
    /// only by the RDMA build; the socket wrapper clears it on drop.
    #[allow(dead_code)]
    rdma_sessions: Arc<Mutex<HashMap<u64, Box<dyn DataChannel>>>>,
    /// Near-tier upstream requests; absent when this manager has no upstreams.
    peer_fetch: Option<mpsc::Sender<crate::peer_runtime::PeerCommand>>,
    /// One subscription epoch per connection; a newer Subscribe supersedes the old stream.
    epochs: Arc<Mutex<Epochs>>,
}

impl Service {
    fn epoch(&self, connection: u64, namespace: [u8; 32]) -> Arc<AtomicU64> {
        let mut epochs = self.epochs.lock().unwrap();
        epochs
            .entry((connection, namespace))
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }
}

pub fn key_to_proto(key: &Key) -> proto::Key {
    proto::Key {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.to_vec(),
    }
}

fn key_from_proto(key: Option<proto::Key>) -> std::result::Result<Key, Status> {
    let key = key.ok_or_else(|| Status::invalid_argument("missing key"))?;
    let digest: [u8; 16] = key
        .digest
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("key digest must be 16 bytes"))?;
    if key.consumed_tokens == 0 {
        return Err(Status::invalid_argument("zero consumed_tokens"));
    }
    Ok(Key {
        consumed_tokens: key.consumed_tokens,
        digest,
    })
}

fn descriptor_from_proto(
    value: Option<proto::Descriptor>,
) -> std::result::Result<Descriptor, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("missing descriptor"))?;
    let identity: [u8; 32] = value
        .identity
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("descriptor identity must be 32 bytes"))?;
    let fingerprint: [u8; 16] = value
        .fingerprint
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("descriptor fingerprint must be 16 bytes"))?;
    let ranks = value
        .ranks
        .into_iter()
        .map(|rank| RankSizes {
            planes: rank.plane_bytes,
            row_bytes: rank.row_bytes,
        })
        .collect();
    let descriptor = Descriptor {
        identity,
        fingerprint,
        page_tokens: value.page_tokens,
        ranks,
        tenant: value.tenant,
    };
    descriptor
        .validate()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if value.page_record_bytes != 0 && value.page_record_bytes != descriptor.page_bytes()
        || value.row_record_bytes != 0 && value.row_record_bytes != descriptor.row_bytes()
    {
        return Err(Status::invalid_argument("descriptor record sizes"));
    }
    Ok(descriptor)
}

pub fn entry_to_proto(entry: &store::EntryData) -> proto::CatalogEntry {
    proto::CatalogEntry {
        key: Some(key_to_proto(&entry.key)),
        parent: entry.parent.map(|parent| key_to_proto(&parent)),
        has_parent: entry.parent.is_some(),
        bucket: entry.bucket.to_vec(),
        tail_pages: u64::from(entry.tail_pages),
        row: entry.row,
        tier: match entry.tier {
            0 => proto::Tier::Dram as i32,
            2 => proto::Tier::Remote as i32,
            _ => proto::Tier::Nvme as i32,
        },
        durable: entry.durable,
        bytes: entry.bytes,
        unreadable: entry.unreadable,
    }
}

fn map_hello_error(error: io::Error) -> Status {
    match error.kind() {
        io::ErrorKind::AlreadyExists => Status::failed_precondition(error.to_string()),
        io::ErrorKind::WouldBlock => Status::resource_exhausted(error.to_string()),
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::internal(error.to_string()),
    }
}

fn put_reply(answer: PutAnswer) -> proto::PutReply {
    use put_reply::R;
    let r = match answer {
        PutAnswer::Accept(accept) => R::Accept(proto::PutAccept {
            transfer: accept.transfer,
            first_page: u64::from(accept.first_page),
            parent: accept.parent.map(|parent| key_to_proto(&parent)),
            has_parent: accept.parent.is_some(),
            record_bytes: accept.record_bytes,
        }),
        PutAnswer::Grant(grant) => R::Grant(window_grant(grant)),
        PutAnswer::Stored { bytes } => R::Stored(proto::Stored { bytes }),
        PutAnswer::Refused(refused) => R::Refused(proto::Refused {
            reason: i32::from(refused.reason),
            detail: refused.detail,
        }),
    };
    proto::PutReply { r: Some(r) }
}

fn get_reply(answer: GetAnswer) -> proto::GetReply {
    use get_reply::R;
    let r = match answer {
        GetAnswer::Leased {
            transfer,
            record_bytes,
        } => R::Leased(proto::GetLeased {
            transfer,
            record_bytes,
        }),
        GetAnswer::Meta {
            record,
            metadata_offset,
            metadata,
            source,
        } => R::Meta(proto::RecordMeta {
            record,
            metadata_offset,
            metadata,
            source,
        }),
        GetAnswer::Ready(grant) => R::Ready(window_grant(grant)),
        GetAnswer::Done => R::Done(proto::GetDone {}),
        GetAnswer::Refused(refused) => R::Refused(proto::Refused {
            reason: i32::from(refused.reason),
            detail: refused.detail,
        }),
    };
    proto::GetReply { r: Some(r) }
}

fn window_grant(grant: store::WindowGrant) -> proto::WindowGrant {
    proto::WindowGrant {
        transfer: grant.transfer,
        index: grant.index,
        record: grant.record,
        record_offset: grant.record_offset,
        length: grant.length,
        memory: Some(proto::WindowRef {
            region: grant.region,
            offset: grant.offset,
            length: grant.length,
        }),
        page_first: grant.page_first,
        page_count: grant.page_count,
        row: grant.row,
    }
}

fn refused(reason: proto::Reason) -> proto::PutReply {
    proto::PutReply {
        r: Some(put_reply::R::Refused(proto::Refused {
            reason: reason as i32,
            detail: String::new(),
        })),
    }
}

fn refused_get(reason: proto::Reason) -> proto::GetReply {
    proto::GetReply {
        r: Some(get_reply::R::Refused(proto::Refused {
            reason: reason as i32,
            detail: String::new(),
        })),
    }
}

impl Service {
    fn peer<T>(request: &Request<T>) -> std::result::Result<PeerInfo, Status> {
        request
            .extensions()
            .get::<PeerInfo>()
            .cloned()
            .ok_or_else(|| Status::internal("missing peer credentials"))
    }

    fn generation<T>(
        &self,
        request: &Request<T>,
        peer: &PeerInfo,
    ) -> std::result::Result<u64, Status> {
        let value = request
            .metadata()
            .get(GENERATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| Status::unauthenticated("missing generation"))?;
        let generations = self.generations.lock().unwrap();
        let Some(generation) = generations.get(&peer.connection) else {
            return Err(Status::unauthenticated("no Hello on this connection"));
        };
        if value == generation.current {
            Ok(value)
        } else if generation.issued.contains(&value) {
            Err(Status::aborted("generation superseded"))
        } else {
            Err(Status::unauthenticated("unknown generation"))
        }
    }

    async fn send(&self, command: StoreCommand) -> std::result::Result<(), Status> {
        self.commands
            .send(command)
            .await
            .map_err(|_| Status::unavailable("store stopped"))
    }

    fn record_generation(&self, connection: u64, generation: u64) {
        let mut generations = self.generations.lock().unwrap();
        let state = generations.entry(connection).or_default();
        if state.current != 0 {
            state.issued.insert(state.current);
        }
        state.current = generation;
        state.issued.insert(generation);
    }
}

impl Service {
    async fn hello_impl(
        &self,
        request: Request<proto::HelloRequest>,
    ) -> std::result::Result<Response<proto::HelloReply>, Status> {
        let peer = Self::peer(&request)?;
        let message = request.into_inner();
        if message.protocol != 1 {
            return Err(Status::failed_precondition("protocol mismatch"));
        }
        let descriptor = descriptor_from_proto(message.descriptor)?;
        let (reply, answer) = oneshot::channel();
        self.send(StoreCommand::Hello {
            connection: peer.connection,
            replica: message.replica,
            pid: peer.pid,
            descriptor,
            reply,
        })
        .await?;
        let answer = answer
            .await
            .map_err(|_| Status::internal("store dropped Hello"))?
            .map_err(map_hello_error)?;
        self.record_generation(peer.connection, answer.generation);
        Ok(Response::new(proto::HelloReply {
            namespace_id: answer.namespace.to_vec(),
            generation: answer.generation,
            arena_path: answer.arena_path,
            arena_bytes: answer.arena_bytes,
            staging_path: answer.staging_path,
            staging_bytes: answer.staging_bytes,
            max_open_puts: store::OPEN_PUTS as u32,
            max_open_gets: store::OPEN_GETS as u32,
            max_window_bytes: answer.max_window_bytes,
            window_credits: store::WINDOW_CREDITS,
        }))
    }

    async fn subscribe_impl(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> std::result::Result<Response<SubscribeStream>, Status> {
        let peer = Self::peer(&request)?;
        let generation = self.generation(&request, &peer)?;
        let message = request.into_inner();
        let namespace: [u8; 32] = message
            .namespace_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("namespace id must be 32 bytes"))?;
        if message.generation != generation {
            return Err(Status::unauthenticated("generation mismatch"));
        }
        let descriptor = descriptor_from_proto(message.descriptor)?;
        let epoch = self.epoch(peer.connection, namespace);
        let this_epoch = epoch.fetch_add(1, Ordering::AcqRel) + 1;
        let (events, receiver) = mpsc::channel(65_536);
        let (reply, answer) = oneshot::channel();
        self.send(StoreCommand::Subscribe {
            connection: peer.connection,
            namespace,
            generation,
            descriptor,
            events,
            reply,
        })
        .await?;
        answer
            .await
            .map_err(|_| Status::internal("store dropped Subscribe"))?
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotConnected | io::ErrorKind::PermissionDenied => {
                    Status::unauthenticated(error.to_string())
                }
                _ => Status::failed_precondition(error.to_string()),
            })?;
        let current = epoch.clone();
        Ok(Response::new(Box::pin(
            ReceiverStream::new(receiver)
                .take_while(move |_| current.load(Ordering::Acquire) == this_epoch)
                .map(Ok::<_, Status>),
        )))
    }

    async fn locate_impl(
        &self,
        _request: Request<proto::LocateRequest>,
    ) -> std::result::Result<Response<proto::LocateReply>, Status> {
        Err(Status::unimplemented("Locate is not served"))
    }

    async fn put_impl(
        &self,
        request: Request<Streaming<proto::PutMessage>>,
    ) -> std::result::Result<Response<PutStream>, Status> {
        let peer = Self::peer(&request)?;
        let generation = self.generation(&request, &peer)?;
        let stream = request.into_inner();
        let transfer = self.transfers.fetch_add(1, Ordering::Relaxed);
        let commands = self.commands.clone();
        let grants = self.grants.clone();
        let (output, receiver) = mpsc::channel(8);
        let sessions = self.peer.as_ref().map(|data| data.sessions.clone());
        tokio::spawn(put_task(
            peer, stream, commands, grants, sessions, transfer, generation, output,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    async fn get_impl(
        &self,
        request: Request<Streaming<proto::GetMessage>>,
        peer_request: bool,
    ) -> std::result::Result<Response<GetStream>, Status> {
        let peer = Self::peer(&request)?;
        let generation = self.generation(&request, &peer)?;
        let stream = request.into_inner();
        let transfer = self.transfers.fetch_add(1, Ordering::Relaxed);
        let commands = self.commands.clone();
        let grants = self.grants.clone();
        let (output, receiver) = mpsc::channel(8);
        tokio::spawn(get_task(
            peer,
            stream,
            commands,
            grants,
            self.peer.as_ref().map(|data| data.sessions.clone()),
            transfer,
            generation,
            peer_request,
            self.peer_fetch.clone(),
            output,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

#[tonic::async_trait]
impl LocalCache for Service {
    type SubscribeStream = SubscribeStream;
    type PutStream = PutStream;
    type GetStream = GetStream;

    async fn hello(
        &self,
        request: Request<proto::HelloRequest>,
    ) -> std::result::Result<Response<proto::HelloReply>, Status> {
        self.hello_impl(request).await
    }

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        self.subscribe_impl(request).await
    }

    async fn locate(
        &self,
        request: Request<proto::LocateRequest>,
    ) -> std::result::Result<Response<proto::LocateReply>, Status> {
        self.locate_impl(request).await
    }

    async fn put(
        &self,
        request: Request<Streaming<proto::PutMessage>>,
    ) -> std::result::Result<Response<Self::PutStream>, Status> {
        self.put_impl(request).await
    }

    async fn get(
        &self,
        request: Request<Streaming<proto::GetMessage>>,
    ) -> std::result::Result<Response<Self::GetStream>, Status> {
        self.get_impl(request, false).await
    }
}

#[tonic::async_trait]
impl PeerCache for Service {
    type SubscribeStream = SubscribeStream;
    type PutStream = PutStream;
    type GetStream = GetStream;

    async fn peer_hello(
        &self,
        request: Request<proto::PeerHelloRequest>,
    ) -> std::result::Result<Response<proto::PeerHelloReply>, Status> {
        let peer = Self::peer(&request)?;
        let Some(data) = &self.peer else {
            return Err(Status::unimplemented("this manager does not serve peers"));
        };
        let message = request.into_inner();
        if message.protocol != 1 {
            return Err(Status::failed_precondition("protocol mismatch"));
        }
        if message.node.is_empty() || message.node.len() > 256 {
            return Err(Status::invalid_argument("peer node name"));
        }
        #[allow(unused_mut)]
        let mut transport = "stream".to_string();
        #[allow(unused_mut)]
        let mut server_rdma = Vec::new();
        #[cfg(feature = "rdma")]
        if message.transports.iter().any(|name| name == "rdma")
            && !message.rdma.is_empty()
            && crate::data::rdma::Handshake::decode(&message.rdma).is_ok()
        {
            let handshake = crate::data::rdma::Handshake::decode(&message.rdma)
                .expect("the handshake was decoded above");
            let regions: Arc<dyn crate::data::RegionAccess> = data.regions.clone();
            match crate::data::ibv::LocalRdma::open(regions) {
                Ok(local) => {
                    let ours = local.handshake();
                    match local.connect(handshake, (message.arena_bytes, message.staging_bytes)) {
                        Ok(channel) => {
                            self.rdma_sessions
                                .lock()
                                .unwrap()
                                .insert(peer.connection, channel);
                            server_rdma = ours.encode().to_vec();
                            transport = "rdma".to_string();
                        }
                        Err(error) => eprintln!(
                            "selene: experimental RDMA accept failed ({error}); using stream"
                        ),
                    }
                }
                Err(error) => {
                    eprintln!("selene: experimental RDMA setup failed ({error}); using stream")
                }
            }
        }
        let (reply, answer) = oneshot::channel();
        self.send(StoreCommand::PeerHello {
            connection: peer.connection,
            node: message.node,
            reply,
        })
        .await?;
        let generation = answer
            .await
            .map_err(|_| Status::internal("store dropped PeerHello"))?
            .map_err(map_hello_error)?;
        self.record_generation(peer.connection, generation);
        data.peer_sizes
            .widen(message.arena_bytes, message.staging_bytes);
        let token = random_token()?;
        data.sessions.register(token, peer.connection);
        Ok(Response::new(proto::PeerHelloReply {
            generation,
            transport,
            stream_token: token.to_vec(),
            stream_addr: data.stream_addr.clone(),
            rdma: server_rdma,
            arena_bytes: data.regions.size(Region::Arena),
            staging_bytes: data.regions.size(Region::Staging),
        }))
    }

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        self.subscribe_impl(request).await
    }

    async fn put(
        &self,
        request: Request<Streaming<proto::PutMessage>>,
    ) -> std::result::Result<Response<Self::PutStream>, Status> {
        self.put_impl(request).await
    }

    async fn get(
        &self,
        request: Request<Streaming<proto::GetMessage>>,
    ) -> std::result::Result<Response<Self::GetStream>, Status> {
        self.get_impl(request, true).await
    }
}

async fn pending_put_terminal(
    events: &mut mpsc::Receiver<std::result::Result<PutAnswer, io::Error>>,
) -> Option<PutAnswer> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(Ok(answer)))
                if matches!(answer, PutAnswer::Stored { .. } | PutAnswer::Refused(_)) =>
            {
                return Some(answer);
            }
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

async fn pending_get_terminal(
    events: &mut mpsc::Receiver<std::result::Result<GetAnswer, io::Error>>,
) -> Option<GetAnswer> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
    loop {
        match tokio::time::timeout_at(deadline, events.recv()).await {
            Ok(Some(Ok(answer))) if matches!(answer, GetAnswer::Done | GetAnswer::Refused(_)) => {
                return Some(answer);
            }
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

/// Stop a peer's data sockets and let their copies drain before the store releases windows.
async fn cancel_task(
    commands: &mpsc::Sender<StoreCommand>,
    sessions: &Option<Arc<crate::grants::Sessions>>,
    generation: u64,
    connection: u64,
    transfer: u64,
) {
    if let Some(sessions) = sessions {
        let sessions = sessions.clone();
        // The windows are released only after every copy into them has left the data path.
        let _ = tokio::task::spawn_blocking(move || {
            sessions.stop(connection);
            sessions.drain(connection);
        })
        .await;
    }
    let _ = commands
        .send(StoreCommand::Cancel {
            connection,
            generation,
            transfer,
        })
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn put_task(
    peer: PeerInfo,
    mut stream: Streaming<proto::PutMessage>,
    commands: mpsc::Sender<StoreCommand>,
    grants: Arc<SharedGrants>,
    data_sessions: Option<Arc<crate::grants::Sessions>>,
    transfer: u64,
    generation: u64,
    output: mpsc::Sender<std::result::Result<proto::PutReply, Status>>,
) {
    let first = match stream.message().await {
        Ok(Some(message)) => message,
        Ok(None) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
        Err(status) => {
            let _ = output.send(Err(status)).await;
            return;
        }
    };
    let begin = match first.m {
        Some(put_message::M::Begin(begin)) => begin,
        _ => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let namespace: [u8; 32] = match begin.namespace_id.as_slice().try_into() {
        Ok(namespace) => namespace,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let descriptor = match descriptor_from_proto(begin.descriptor) {
        Ok(descriptor) => descriptor,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let key = match key_from_proto(begin.key) {
        Ok(key) => key,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let bucket: [u8; 16] = match begin.bucket.as_slice().try_into() {
        Ok(bucket) => bucket,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let chain_pages = match u32::try_from(begin.chain_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let tail_pages = match u32::try_from(begin.published_tail_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
            return;
        }
    };
    let published_parent =
        if begin.has_published_parent {
            if begin.published_parent.as_ref().is_some_and(|key| {
                key.consumed_tokens == 0 && key.digest.iter().all(|byte| *byte == 0)
            }) {
                let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                return;
            }
            match key_from_proto(begin.published_parent) {
                Ok(key) => Some(key),
                Err(_) => {
                    let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                    return;
                }
            }
        } else {
            None
        };
    let (events, event_stream) = mpsc::channel(4);
    let (reply, answer) = oneshot::channel();
    if commands
        .send(StoreCommand::PutBegin {
            connection: peer.connection,
            transfer,
            generation,
            namespace,
            descriptor,
            key,
            published_parent,
            bucket,
            chain_pages,
            published_tail_pages: tail_pages,
            row: begin.row,
            events,
            reply,
        })
        .await
        .is_err()
    {
        let _ = output.send(Err(Status::unavailable("store stopped"))).await;
        return;
    }
    match answer.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let status = match error.kind() {
                io::ErrorKind::PermissionDenied => Status::aborted(error.to_string()),
                io::ErrorKind::AlreadyExists | io::ErrorKind::NotConnected => {
                    Status::failed_precondition(error.to_string())
                }
                _ => Status::invalid_argument(error.to_string()),
            };
            let _ = output.send(Err(status)).await;
            return;
        }
        Err(_) => {
            let _ = output
                .send(Err(Status::internal("store dropped PutBegin")))
                .await;
            return;
        }
    }
    let mut event_stream = event_stream;
    let _guard = GrantGuard {
        grants: grants.clone(),
        session: peer.connection,
        transfer,
    };
    loop {
        tokio::select! {
            event = event_stream.recv() => {
                match event {
                    Some(Ok(answer)) => {
                        if let PutAnswer::Grant(grant) = &answer
                            && let Some(region) = region_from_wire(grant.region)
                        {
                            grants.insert(
                                GrantKey {
                                    transfer,
                                    index: grant.index,
                                },
                                crate::grants::GrantWindow {
                                    session: peer.connection,
                                    put: true,
                                    region,
                                    offset: grant.offset,
                                    length: grant.length,
                                },
                            );
                        }
                        let terminal = matches!(answer, PutAnswer::Stored { .. } | PutAnswer::Refused(_));
                        if output.send(Ok(put_reply(answer))).await.is_err() {
                            return;
                        }
                        if terminal {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        let _ = output.send(Err(Status::internal(error.to_string()))).await;
                        break;
                    }
                    None => break,
                }
            }
            message = stream.message() => {
                match message {
                    Ok(Some(message)) => match message.m {
                        Some(put_message::M::Filled(filled)) => {
                            if filled.transfer != transfer {
                                let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                break;
                            }
                            let (reply, ack) = oneshot::channel();
                            if commands.send(StoreCommand::WindowFilled {
                                connection: peer.connection,
                                generation,
                                transfer,
                                index: filled.index,
                                page_xxh3: filled.page_xxh3,
                                row_xxh3: filled.has_row.then_some(filled.row_xxh3),
                                reply,
                            }).await.is_err() {
                                break;
                            }
                            let ack = ack.await;
                            if matches!(&ack, Ok(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied) {
                                let _ = output.send(Err(Status::aborted("generation superseded"))).await;
                                break;
                            }
                            if !matches!(ack, Ok(Ok(()))) {
                                match pending_put_terminal(&mut event_stream).await {
                                    Some(answer) => {
                                        let _ = output.send(Ok(put_reply(answer))).await;
                                    }
                                    None => {
                                        let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                    }
                                }
                                break;
                            }
                            // The store accepted the filled window; no more pieces may arrive
                            // for it.
                            grants.remove(
                                peer.connection,
                                true,
                                GrantKey {
                                    transfer,
                                    index: filled.index,
                                },
                            );
                        }
                        Some(put_message::M::Commit(commit)) => {
                            if commit.transfer != transfer {
                                let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                break;
                            }
                            let (reply, ack) = oneshot::channel();
                            if commands.send(StoreCommand::PutCommit {
                                connection: peer.connection,
                                generation,
                                transfer,
                                metadata_offset: commit.metadata_offset,
                                metadata: commit.metadata,
                                reply,
                            }).await.is_err() {
                                break;
                            }
                            let ack = ack.await;
                            if matches!(&ack, Ok(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied) {
                                let _ = output.send(Err(Status::aborted("generation superseded"))).await;
                                break;
                            }
                            if !matches!(ack, Ok(Ok(()))) {
                                match pending_put_terminal(&mut event_stream).await {
                                    Some(answer) => {
                                        let _ = output.send(Ok(put_reply(answer))).await;
                                    }
                                    None => {
                                        let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                                    }
                                }
                                break;
                            }
                        }
                        Some(put_message::M::Cancel(_)) => {
                            cancel_task(
                                &commands,
                                &data_sessions,
                                generation,
                                peer.connection,
                                transfer,
                            )
                            .await;
                            if let Ok(Some(Ok(answer))) = tokio::time::timeout(
                                Duration::from_millis(500),
                                event_stream.recv(),
                            )
                            .await
                            {
                                let _ = output.send(Ok(put_reply(answer))).await;
                            }
                            break;
                        }
                        Some(put_message::M::Begin(_)) | None => {
                            let _ = output.send(Ok(refused(proto::Reason::Invalid))).await;
                            cancel_task(
                                &commands,
                                &data_sessions,
                                generation,
                                peer.connection,
                                transfer,
                            )
                            .await;
                            break;
                        }
                    },
                    Ok(None) => {
                        cancel_task(
                            &commands,
                            &data_sessions,
                            generation,
                            peer.connection,
                            transfer,
                        )
                        .await;
                        break;
                    }
                    Err(_) => {
                        cancel_task(
                            &commands,
                            &data_sessions,
                            generation,
                            peer.connection,
                            transfer,
                        )
                        .await;
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(NO_PROGRESS) => {
                let _ = output.send(Err(Status::deadline_exceeded("no progress"))).await;
                cancel_task(
                    &commands,
                    &data_sessions,
                    generation,
                    peer.connection,
                    transfer,
                )
                .await;
                break;
            }
        }
    }
}

/// Releases a fetch reservation when the Get that owns it ends, however it ends. The pins
/// must outlive adoption and the serving lease; a connection close also sweeps every token
/// that connection created.
struct Reservation {
    token: u64,
    commands: mpsc::Sender<StoreCommand>,
}

impl Reservation {
    fn new(token: u64, commands: &mpsc::Sender<StoreCommand>) -> Self {
        Self {
            token,
            commands: commands.clone(),
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let token = self.token;
        let commands = self.commands.clone();
        // Drop cannot await: hand the release to the runtime, and use the non-blocking send
        // outside one (shutdown). The store's connection sweep is the final backstop.
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let _ = commands.send(StoreCommand::ReleaseFetch { token }).await;
                });
            }
            Err(_) => {
                let _ = commands.try_send(StoreCommand::ReleaseFetch { token });
            }
        }
    }
}

/// What a remote Get needed before the store can plan its windows.
enum RemoteFetch {
    /// Nothing to fetch; the store serves the chain from local bytes.
    Local,
    /// The chain was fetched and adopted; this token releases its pins when the Get ends.
    Fetched(u64),
    /// The chain cannot be held in DRAM; the Get is a miss.
    Miss,
}

/// A Get whose chain only exists upstream fetches it into local DRAM before the store plans
/// the windows, so the serving path stays unchanged.
async fn prepare_remote(
    commands: &mpsc::Sender<StoreCommand>,
    peer: Option<&mpsc::Sender<crate::peer_runtime::PeerCommand>>,
    namespace: [u8; 32],
    chain: &[Key],
    connection: u64,
) -> std::result::Result<RemoteFetch, Status> {
    let (reply, answer) = oneshot::channel();
    commands
        .send(StoreCommand::RemotePlan {
            namespace,
            chain: chain.to_vec(),
            reply,
        })
        .await
        .map_err(|_| Status::unavailable("store stopped"))?;
    let plan = answer
        .await
        .map_err(|_| Status::internal("store dropped RemotePlan"))?
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let Some(plan) = plan else {
        return Ok(RemoteFetch::Local);
    };
    let Some(peer) = peer else {
        return Err(Status::unavailable(
            "the chain is only on an upstream and no peer is configured",
        ));
    };
    let (reply, answer) = oneshot::channel();
    peer.send(crate::peer_runtime::PeerCommand::Fetch {
        namespace,
        plan,
        connection,
        reply,
    })
    .await
    .map_err(|_| Status::unavailable("peer runtime stopped"))?;
    let fetched = answer
        .await
        .map_err(|_| Status::internal("peer runtime dropped the fetch"))?;
    let fetched = match fetched {
        Ok(fetched) => fetched,
        // A chain that cannot fit the arena is a miss: the replica recomputes it.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RemoteFetch::Miss);
        }
        Err(error) => return Err(Status::unavailable(error.to_string())),
    };
    // The store pins the fetched suffix until the Get ends; a failed adoption must let it go.
    let reservation = fetched.reservation;
    for (key, extent, row) in fetched.records {
        let (reply, answer) = oneshot::channel();
        commands
            .send(StoreCommand::AdoptRemote {
                namespace,
                key,
                extent,
                row,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("store stopped"))?;
        let adopted = answer
            .await
            .map_err(|_| Status::internal("store dropped AdoptRemote"))?
            .map_err(|error| {
                // A record with a disk job in flight is transiently unavailable.
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    Status::unavailable(error.to_string())
                } else {
                    Status::internal(error.to_string())
                }
            });
        if let Err(status) = adopted {
            let _ = commands
                .send(StoreCommand::ReleaseFetch { token: reservation })
                .await;
            return Err(status);
        }
    }
    Ok(RemoteFetch::Fetched(reservation))
}

#[allow(clippy::too_many_arguments)]
async fn get_task(
    peer: PeerInfo,
    mut stream: Streaming<proto::GetMessage>,
    commands: mpsc::Sender<StoreCommand>,
    grants: Arc<SharedGrants>,
    data_sessions: Option<Arc<crate::grants::Sessions>>,
    mut transfer: u64,
    generation: u64,
    peer_request: bool,
    peer_fetch: Option<mpsc::Sender<crate::peer_runtime::PeerCommand>>,
    output: mpsc::Sender<std::result::Result<proto::GetReply, Status>>,
) {
    let first = match stream.message().await {
        Ok(Some(message)) => message,
        Ok(None) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
        Err(status) => {
            let _ = output.send(Err(status)).await;
            return;
        }
    };
    let begin = match first.m {
        Some(get_message::M::Begin(begin)) => begin,
        _ => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    let namespace: [u8; 32] = match begin.namespace_id.as_slice().try_into() {
        Ok(namespace) => namespace,
        Err(_) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    if peer_request {
        if begin.destinations.len() != begin.chain.len() || begin.transfer == 0 {
            let _ = output
                .send(Err(Status::invalid_argument(
                    "PeerCache.Get destinations/transfer",
                )))
                .await;
            return;
        }
        // The peer identifies grants by the transfer it chose; operations are per connection,
        // so that id is unique where the store needs it.
        transfer = begin.transfer;
    } else if !begin.destinations.is_empty() || begin.transfer != 0 {
        let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
        return;
    }
    let mut chain = Vec::with_capacity(begin.chain.len());
    for key in begin.chain {
        match key_from_proto(Some(key)) {
            Ok(key) => chain.push(key),
            Err(_) => {
                let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                return;
            }
        }
    }
    if chain.is_empty() || chain.len() > store::MAX_CHAIN {
        let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
        return;
    }
    let skip_pages = match u32::try_from(begin.skip_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    let expected_pages = match u32::try_from(begin.expected_pages) {
        Ok(pages) => pages,
        Err(_) => {
            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
            return;
        }
    };
    // Holds the fetch reservation until this task ends, so the fetched chain stays pinned
    // through adoption and the serving lease.
    let _reservation = if peer_request {
        None
    } else {
        match prepare_remote(
            &commands,
            peer_fetch.as_ref(),
            namespace,
            &chain,
            peer.connection,
        )
        .await
        {
            Ok(RemoteFetch::Local) => None,
            Ok(RemoteFetch::Fetched(token)) => Some(Reservation::new(token, &commands)),
            // The remote chain cannot be fetched into DRAM: report a miss, not an error.
            Ok(RemoteFetch::Miss) => {
                let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                return;
            }
            Err(status) => {
                let _ = output.send(Err(status)).await;
                return;
            }
        }
    };
    let (events, event_stream) = mpsc::channel(4);
    let (reply, answer) = oneshot::channel();
    if commands
        .send(StoreCommand::GetBegin {
            connection: peer.connection,
            transfer,
            generation,
            namespace,
            chain,
            skip_pages,
            expected_pages,
            row: begin.row,
            events,
            reply,
        })
        .await
        .is_err()
    {
        let _ = output.send(Err(Status::unavailable("store stopped"))).await;
        return;
    }
    match answer.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let status = match error.kind() {
                io::ErrorKind::PermissionDenied => Status::aborted(error.to_string()),
                io::ErrorKind::NotConnected => Status::unauthenticated(error.to_string()),
                _ => Status::invalid_argument(error.to_string()),
            };
            let _ = output.send(Err(status)).await;
            return;
        }
        Err(_) => {
            let _ = output
                .send(Err(Status::internal("store dropped GetBegin")))
                .await;
            return;
        }
    }
    let mut event_stream = event_stream;
    let _guard = GrantGuard {
        grants: grants.clone(),
        session: peer.connection,
        transfer,
    };
    loop {
        tokio::select! {
            event = event_stream.recv() => {
                match event {
                    Some(Ok(answer)) => {
                        if let GetAnswer::Ready(grant) = &answer
                            && let Some(region) = region_from_wire(grant.region)
                        {
                            grants.insert(
                                GrantKey {
                                    transfer,
                                    index: grant.index,
                                },
                                crate::grants::GrantWindow {
                                    session: peer.connection,
                                    put: false,
                                    region,
                                    offset: grant.offset,
                                    length: grant.length,
                                },
                            );
                        }
                        let terminal = matches!(answer, GetAnswer::Done | GetAnswer::Refused(_));
                        if output.send(Ok(get_reply(answer))).await.is_err() {
                            return;
                        }
                        if terminal {
                            break;
                        }
                    }
                    Some(Err(error)) => {
                        let _ = output.send(Err(Status::internal(error.to_string()))).await;
                        break;
                    }
                    None => break,
                }
            }
            message = stream.message() => {
                match message {
                    Ok(Some(message)) => match message.m {
                        Some(get_message::M::Released(released)) => {
                            if released.transfer != transfer {
                                let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                                break;
                            }
                            let (reply, ack) = oneshot::channel();
                            if commands.send(StoreCommand::WindowReleased {
                                connection: peer.connection,
                                generation,
                                transfer,
                                index: released.index,
                                reply,
                            }).await.is_err() {
                                break;
                            }
                            let ack = ack.await;
                            if matches!(&ack, Ok(Err(error)) if error.kind() == io::ErrorKind::PermissionDenied) {
                                let _ = output.send(Err(Status::aborted("generation superseded"))).await;
                                break;
                            }
                            if !matches!(ack, Ok(Ok(()))) {
                                match pending_get_terminal(&mut event_stream).await {
                                    Some(answer) => {
                                        let _ = output.send(Ok(get_reply(answer))).await;
                                    }
                                    None => {
                                        let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                                    }
                                }
                                break;
                            }
                            grants.remove(
                                peer.connection,
                                false,
                                GrantKey {
                                    transfer,
                                    index: released.index,
                                },
                            );
                        }
                        Some(get_message::M::Cancel(_)) => {
                            cancel_task(
                                &commands,
                                &data_sessions,
                                generation,
                                peer.connection,
                                transfer,
                            )
                            .await;
                            break;
                        }
                        Some(get_message::M::Begin(_)) | None => {
                            let _ = output.send(Ok(refused_get(proto::Reason::Invalid))).await;
                            cancel_task(
                                &commands,
                                &data_sessions,
                                generation,
                                peer.connection,
                                transfer,
                            )
                            .await;
                            break;
                        }
                    },
                    Ok(None) => {
                        cancel_task(
                            &commands,
                            &data_sessions,
                            generation,
                            peer.connection,
                            transfer,
                        )
                        .await;
                        break;
                    }
                    Err(_) => {
                        cancel_task(
                            &commands,
                            &data_sessions,
                            generation,
                            peer.connection,
                            transfer,
                        )
                        .await;
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(NO_PROGRESS) => {
                let _ = output.send(Err(Status::deadline_exceeded("no progress"))).await;
                cancel_task(
                    &commands,
                    &data_sessions,
                    generation,
                    peer.connection,
                    transfer,
                )
                .await;
                break;
            }
        }
    }
}

struct StoreAdmin {
    commands: mpsc::Sender<StoreCommand>,
}

impl StoreAdmin {
    async fn ask<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T>>) -> StoreCommand,
    ) -> io::Result<T> {
        let (reply, answer) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| io::Error::other("store stopped"))?;
        answer
            .await
            .map_err(|_| io::Error::other("store dropped the admin request"))?
    }
}

#[tonic::async_trait]
impl crate::admin::AdminBackend for StoreAdmin {
    async fn stats(&self) -> io::Result<String> {
        self.ask(|reply| StoreCommand::Admin(AdminCommand::Stats(reply)))
            .await
    }

    async fn pin(&self, namespace: Vec<u8>, keys: Vec<Key>, pin: bool) -> io::Result<u32> {
        self.ask(|reply| {
            StoreCommand::Admin(AdminCommand::Pin {
                namespace,
                keys,
                pin,
                reply,
            })
        })
        .await
    }

    async fn purge(&self, namespace: Vec<u8>, all_namespaces: bool) -> io::Result<(u64, u64)> {
        self.ask(|reply| {
            StoreCommand::Admin(AdminCommand::Purge {
                namespace,
                all_namespaces,
                reply,
            })
        })
        .await
    }

    async fn set_quota(&self, namespace: Vec<u8>, dram: u64, nvme: u64) -> io::Result<u64> {
        self.ask(|reply| {
            StoreCommand::Admin(AdminCommand::SetQuota {
                namespace,
                dram,
                nvme,
                reply,
            })
        })
        .await
    }
}

/// The local-only entry point used by the existing tests.
pub fn run(config: Config, stop: Arc<AtomicBool>) -> Result<()> {
    run_peer(local_config(&config), config, stop)
}

/// Build the storage configuration for a manager config. The CLI never sets faults; tests and
/// disk-fault harnesses pass their own `store::Config` straight to `run_peer`.
pub fn store_config(config: &crate::config::Config) -> Result<Config> {
    Ok(Config {
        name: config.name.clone(),
        root: config.root.clone(),
        socket: config.socket.clone(),
        memory: usize::try_from(config.memory)
            .map_err(|_| invalid("--memory is too large for this host"))?,
        nvme: config.nvme,
        staging: usize::try_from(config.staging)
            .map_err(|_| invalid("--staging is too large for this host"))?,
        metrics: config.metrics.map(|address| address.to_string()),
        faults: Default::default(),
        write_budget: config.write_budget,
        max_window_bytes: store::DEFAULT_MAX_WINDOW,
        remote_catalog_entries: usize::try_from(config.remote_catalog_entries)
            .unwrap_or(usize::MAX),
        retain_remote: config.retain_remote,
        pool: Default::default(),
    })
}

fn local_config(config: &Config) -> crate::config::Config {
    let mut local = crate::config::Config::defaults();
    local.name = config.name.clone();
    local.socket = config.socket.clone();
    local.socket_explicit = true;
    local.root = config.root.clone();
    local.memory = config.memory as u64;
    local.nvme = config.nvme;
    local.staging = config.staging as u64;
    local.metrics = config.metrics.as_deref().and_then(|text| text.parse().ok());
    local.write_budget = config.write_budget;
    local
}

async fn watcher(stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Stops the manager's threads and removes its socket, whether startup failed or serving
/// ended. Dropping it releases the store's instance and root locks with the owner thread.
struct Cleanup {
    stop: Arc<AtomicBool>,
    commands: mpsc::Sender<StoreCommand>,
    owner: Option<thread::JoinHandle<()>>,
    tick: Option<thread::JoinHandle<()>>,
    metrics: Option<thread::JoinHandle<()>>,
    peer: Option<thread::JoinHandle<()>>,
    data: Option<thread::JoinHandle<()>>,
    socket: PathBuf,
}

impl Cleanup {
    fn finish(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Unlink the socket while this process still holds the store's locks: no replacement
        // manager can have bound a new socket under the same path yet.
        let _ = fs::remove_file(&self.socket);
        // Delivery is guaranteed even when another sender is alive (the tick thread, the peer
        // runtime or a handler task) and even with a full queue; the owner always exits, so
        // the Store and its locks are released. `run` calls this from its own thread.
        let _ = self.commands.blocking_send(StoreCommand::Shutdown);
        // The tick, peer, data and metrics threads exit on the flag.
        if let Some(thread) = self.tick.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.owner.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.metrics.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.data.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.peer.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

pub fn run_peer(
    config: crate::config::Config,
    store_config: Config,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    config.validate()?;
    for warning in config.startup_warnings() {
        eprintln!("{warning}");
    }
    // Validate TLS before anything is started or accepted: a certificate error must not
    // leave an accepting data listener behind.
    validate_tls(&config)?;
    let pool_settings = store_config.pool.clone();
    let pool_root = store_config.root.clone();
    let store = Store::open(store_config)?;
    let regions = Arc::new(store.regions());
    let grants = Arc::new(SharedGrants::default());
    let directory = config
        .socket
        .parent()
        .ok_or_else(|| invalid("socket directory"))?;
    prepare_socket_directory(directory)?;
    if config.socket.exists() {
        if !fs::symlink_metadata(&config.socket)?
            .file_type()
            .is_socket()
        {
            return Err(invalid("socket path is not a socket"));
        }
        if std::os::unix::net::UnixStream::connect(&config.socket).is_ok() {
            return Err(invalid("socket is already in use"));
        }
        fs::remove_file(&config.socket)?;
    }
    // The runtime exists, and its signal handling is registered, before the socket does: a
    // termination signal during startup then waits for the server instead of killing the
    // process by default disposition.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let (mut terminate, mut interrupt) = runtime.block_on(async {
        Ok::<_, io::Error>((
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
        ))
    })?;
    // The pool actor runs on the runtime; its sessions arrive through the gRPC service.
    let (shm, staging_arena, recovered_records) = store.pool_view();
    let pool_config = crate::pool::PoolConfig {
        limits: pool_settings.limits.clone(),
        reply_retention: pool_settings.reply_retention,
        source_wait: pool_settings.source_wait,
        instance_retention: pool_settings.instance_retention,
        store_directory: pool_root,
        encryption: if config.tls_cert.is_some() {
            crate::proto::v2::Encryption::Tls
        } else {
            crate::proto::v2::Encryption::Plaintext
        },
        transports: pool_transports(&config),
        data_address: None,
    };
    let (pool_core, pool_handle, pool_events) =
        crate::pool::Core::new(pool_config, shm, staging_arena, true, recovered_records);
    let pool_service = crate::pool::PoolService::new(pool_handle.clone());
    let pool_task = runtime.spawn(pool_core.run(pool_events));
    // Bind every listener before any thread starts, so a startup failure leaves no threads
    // and no locks behind; the guard below also removes the socket it creates.
    let metrics_text = Arc::new(Mutex::new(String::new()));
    let metrics_address = config.metrics.map(|address| address.to_string());
    let metrics_listener = bind_metrics(metrics_address.as_deref())?;
    let peer_data_listener = match (config.listen_peer, config.listen_data) {
        (Some(_), Some(address)) => Some(std::net::TcpListener::bind(address)?),
        _ => None,
    };
    let peer_listener = match config.listen_peer {
        Some(address) => {
            let listener = std::net::TcpListener::bind(address)?;
            listener.set_nonblocking(true)?;
            Some(listener)
        }
        None => None,
    };
    let admin_listener = match config.listen_admin {
        Some(address) => {
            let listener = std::net::TcpListener::bind(address)?;
            listener.set_nonblocking(true)?;
            Some(listener)
        }
        None => None,
    };
    let listener = std::os::unix::net::UnixListener::bind(&config.socket).map_err(|error| {
        invalid(format!(
            "cannot bind socket {} in {}: {error}",
            config.socket.display(),
            directory.display()
        ))
    })?;
    let (commands, mut receiver) = mpsc::channel::<StoreCommand>(64);
    let mut cleanup = Cleanup {
        stop: stop.clone(),
        commands: commands.clone(),
        owner: None,
        tick: None,
        metrics: None,
        peer: None,
        data: None,
        socket: config.socket.clone(),
    };
    fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o660))?;
    listener.set_nonblocking(true)?;
    if let Some(listener) = metrics_listener {
        cleanup.metrics = Some(serve_metrics(listener, metrics_text.clone(), stop.clone()));
    }
    let (peer_commands, peer_fetch) = if config.upstreams.is_empty() {
        (None, None)
    } else {
        let (sender, thread) =
            crate::peer_runtime::spawn(&config, regions.clone(), commands.clone(), stop.clone())?;
        cleanup.peer = Some(thread);
        (Some(sender.clone()), Some(sender))
    };
    let peer_data = match peer_data_listener {
        Some(listener) => {
            let stream_addr = listener.local_addr()?.to_string();
            let sessions = Arc::new(crate::grants::Sessions::default());
            let peer_sizes = crate::data::stream::PeerSizes::new(0, 0);
            let shared: Arc<dyn RegionAccess> = regions.clone();
            let receiver = crate::data::stream::StreamReceiver::new(
                shared,
                grants.clone(),
                sessions.clone(),
                peer_sizes.clone(),
            );
            cleanup.data = Some(receiver.serve(listener, stop.clone())?);
            Some(Arc::new(PeerData {
                stream_addr,
                regions: regions.clone(),
                peer_sizes,
                grants: grants.clone(),
                sessions,
            }))
        }
        None => None,
    };
    let owner_metrics = metrics_text.clone();
    cleanup.owner = Some(thread::spawn(move || {
        let mut store = store;
        let mut subscribed = std::collections::HashSet::new();
        let mut forwarded = std::collections::HashSet::new();
        'outer: while let Some(mut command) = receiver.blocking_recv() {
            // Drain the batch before dispatching so queued Gets and Puts are
            // ordered by the scheduler rather than by arrival.
            let mut batch = 64;
            loop {
                let shutdown = matches!(command, StoreCommand::Shutdown);
                store.handle(command);
                if shutdown {
                    break 'outer;
                }
                batch -= 1;
                if batch == 0 {
                    break;
                }
                match receiver.try_recv() {
                    Ok(next) => command = next,
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break 'outer,
                }
            }
            store.pump();
            *owner_metrics.lock().unwrap() = store.metrics_text();
            if let Some(sender) = &peer_commands {
                let events = store.take_forward_events();
                for event in events {
                    let (namespace, key) = match event {
                        crate::store::ForwardEvent::Stored { namespace, key }
                        | crate::store::ForwardEvent::Durable { namespace, key } => {
                            (namespace, key)
                        }
                    };
                    // Stored and the later Durable event describe the same record.
                    // Parent first: a child whose ancestor is missing upstream would be
                    // refused and retried; prepare the whole chain in order instead.
                    for ancestor in store.ancestors_of(namespace, key) {
                        if !forwarded.insert((namespace, ancestor)) {
                            continue;
                        }
                        match store.prepare_forward(namespace, ancestor) {
                            Ok(job) => {
                                if sender
                                    .try_send(crate::peer_runtime::PeerCommand::Forward(Box::new(
                                        job,
                                    )))
                                    .is_err()
                                {
                                    store.forward_dropped(namespace, ancestor);
                                }
                            }
                            Err(_) => {
                                forwarded.remove(&(namespace, ancestor));
                            }
                        }
                    }
                }
                for (namespace, descriptor) in store.descriptors() {
                    if subscribed.insert(namespace) {
                        let _ = sender.try_send(crate::peer_runtime::PeerCommand::Subscribe {
                            namespace,
                            descriptor: crate::peer_client::descriptor_to_proto(
                                &descriptor,
                                &descriptor.tenant,
                            ),
                        });
                    }
                }
            }
        }
    }));
    let tick_commands = commands.clone();
    let tick_stop = stop.clone();
    cleanup.tick = Some(thread::spawn(move || {
        while !tick_stop.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(10));
            let _ = tick_commands.try_send(StoreCommand::Tick);
        }
    }));
    let rdma_sessions: Arc<Mutex<HashMap<u64, Box<dyn DataChannel>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let generations = Arc::new(Mutex::new(HashMap::new()));
    let service = Service {
        commands: commands.clone(),
        generations: generations.clone(),
        transfers: Arc::new(AtomicU64::new(1)),
        grants: grants.clone(),
        peer: peer_data.clone(),
        rdma_sessions: rdma_sessions.clone(),
        peer_fetch,
        epochs: Arc::new(Mutex::new(Epochs::new())),
    };
    let uid = unsafe { libc::geteuid() };
    let gid = unsafe { libc::getegid() };
    let incoming_commands = commands.clone();
    let incoming_generations = generations.clone();
    let counter = Arc::new(AtomicU64::new(1));
    let connections = Arc::new(AtomicUsize::new(0));
    let async_stop = stop.clone();
    let peer_stream_commands = commands.clone();
    let peer_server_tls = server_tls(&config)?;
    let admin_server_tls = server_tls(&config)?;
    let peer_generations = generations.clone();
    let peer_rdma = rdma_sessions.clone();
    let peer_sessions = peer_data
        .as_ref()
        .map(|data| data.sessions.clone())
        .unwrap_or_default();
    // Local replicas and peers share one id space: both index the same store sessions.
    let peer_counter = counter.clone();
    let admin_backend: Arc<dyn crate::admin::AdminBackend> = Arc::new(StoreAdmin {
        commands: commands.clone(),
    });
    // The network listeners outlive the stop request until the pool has drained its claims.
    let admin_stop = Arc::new(AtomicBool::new(false));
    let serve_stop = admin_stop.clone();
    let final_stop = serve_stop.clone();
    let drain_wait =
        Duration::from_millis(u64::from(pool_settings.limits.max_claim_deadline_ms) + 2000);
    runtime.block_on(async move {
        let listener = tokio::net::UnixListener::from_std(listener)?;
        let incoming = UnixListenerStream::new(listener).map(move |stream| {
            let stream = stream?;
            let credentials = stream
                .peer_cred()
                .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
            if credentials.uid() != uid || credentials.gid() != gid {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "peer uid/gid differs from the manager",
                ));
            }
            let connection = counter.fetch_add(1, Ordering::Relaxed);
            let open = connections.fetch_add(1, Ordering::AcqRel) + 1;
            Ok(PeerStream {
                inner: stream,
                over_limit: open > store::MAX_CONNECTIONS,
                _slot: ConnectionSlot(connections.clone()),
                info: PeerInfo {
                    connection,
                    unix: true,
                    pid: credentials.pid().unwrap_or(0) as u32,
                    uid,
                    gid,
                },
                commands: incoming_commands.clone(),
                runtime: tokio::runtime::Handle::current(),
                generations: incoming_generations.clone(),
            })
        });
        let server = Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(Duration::from_secs(20)))
            .add_service(
                LocalCacheServer::new(service.clone())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
            )
            .add_service(crate::proto::admin_server::AdminServer::new(
                crate::admin::AdminService::new(admin_backend.clone()),
            ))
            .add_service(
                crate::proto::v2::pool_server::PoolServer::new(pool_service.clone())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
            );
        let peer_task = match peer_listener {
            Some(listener) => {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let incoming = TcpListenerStream::new(listener).map(move |stream| {
                    let stream = stream?;
                    let connection = peer_counter.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, io::Error>(TcpPeerStream {
                        inner: stream,
                        info: PeerInfo {
                            connection,
                            unix: false,
                            pid: 0,
                            uid: 0,
                            gid: 0,
                        },
                        commands: peer_stream_commands.clone(),
                        runtime: tokio::runtime::Handle::current(),
                        generations: peer_generations.clone(),
                        rdma_sessions: peer_rdma.clone(),
                        sessions: peer_sessions.clone(),
                    })
                });
                let service = service.clone();
                let peer_pool_service = pool_service.clone();
                let mut builder = Server::builder()
                    .http2_keepalive_interval(Some(Duration::from_secs(10)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(20)));
                if let Some(tls) = peer_server_tls {
                    builder = builder
                        .tls_config(tls)
                        .map_err(|error| invalid(format!("peer TLS: {error}")))?;
                }
                let stop = final_stop.clone();
                Some(tokio::spawn(async move {
                    let server = builder
                        .add_service(
                            PeerCacheServer::new(service)
                                .max_decoding_message_size(MAX_MESSAGE)
                                .max_encoding_message_size(MAX_MESSAGE),
                        )
                        .add_service(
                            crate::proto::v2::pool_server::PoolServer::new(peer_pool_service)
                                .max_decoding_message_size(MAX_MESSAGE)
                                .max_encoding_message_size(MAX_MESSAGE),
                        )
                        .serve_with_incoming(incoming);
                    tokio::select! {
                        result = server => result
                            .map_err(|error| invalid(format!("peer server: {error}"))),
                        _ = watcher(stop) => Ok(()),
                    }
                }))
            }
            None => None,
        };
        let admin_task = match admin_listener {
            Some(listener) => {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let incoming = TcpListenerStream::new(listener).map(|stream| stream);
                let mut builder = Server::builder()
                    .http2_keepalive_interval(Some(Duration::from_secs(10)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(20)));
                if let Some(tls) = admin_server_tls {
                    builder = builder
                        .tls_config(tls)
                        .map_err(|error| invalid(format!("admin TLS: {error}")))?;
                }
                Some(tokio::spawn(async move {
                    let server = builder
                        .add_service(crate::proto::admin_server::AdminServer::new(
                            crate::admin::AdminService::new(admin_backend),
                        ))
                        .serve_with_incoming(incoming);
                    tokio::select! {
                        result = server => result
                            .map_err(|error| invalid(format!("admin server: {error}"))),
                        _ = watcher(admin_stop) => Ok(()),
                    }
                }))
            }
            None => None,
        };
        let signal_stop = async_stop.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = terminate.recv() => {}
                _ = interrupt.recv() => {}
            }
            signal_stop.store(true, Ordering::Release);
        });
        let server = server.serve_with_incoming(incoming);
        // A stop request first lets the pool finish the claims in flight, then closes
        // its sessions; only then do the listeners stop.
        let drain = {
            let events = pool_handle.events.clone();
            async move {
                watcher(async_stop).await;
                let (done, finished) = oneshot::channel();
                if events
                    .send(crate::pool::core::Event::Shutdown { done })
                    .await
                    .is_ok()
                {
                    let _ = tokio::time::timeout(drain_wait, finished).await;
                }
            }
        };
        let local = tokio::select! {
            result = server => result
                .map_err(|error| invalid(format!("server: {error}"))),
            // A long-lived subscription would make a graceful shutdown wait for the peer to
            // close it; the manager owns the session and stops it.
            _ = drain => Ok(()),
        };
        // A local-server error must not leave the TCP listeners waiting for a stop that no
        // signal will set.
        serve_stop.store(true, Ordering::Release);
        pool_task.abort();
        if let Some(task) = peer_task {
            let _ = task.await;
        }
        if let Some(task) = admin_task {
            let _ = task.await;
        }
        local
    })
    // `cleanup` stops and joins every thread and removes the socket on every path, before the
    // runtime drops.
}

/// Builds the complete server TLS configuration once, so the PEMs are parsed and the key is
/// checked against the certificate before the manager opens anything.
fn validate_tls(config: &crate::config::Config) -> Result<()> {
    if let Some(tls) = server_tls(config)? {
        Server::builder()
            .tls_config(tls)
            .map_err(|error| invalid(format!("TLS: {error}")))?;
    }
    Ok(())
}

fn server_tls(config: &crate::config::Config) -> Result<Option<tonic::transport::ServerTlsConfig>> {
    let (Some(cert), Some(key), Some(ca)) = (&config.tls_cert, &config.tls_key, &config.tls_ca)
    else {
        return Ok(None);
    };
    Ok(Some(
        tonic::transport::ServerTlsConfig::new()
            .identity(tonic::transport::Identity::from_pem(
                fs::read(cert)?,
                fs::read(key)?,
            ))
            .client_ca_root(tonic::transport::Certificate::from_pem(fs::read(ca)?)),
    ))
}

fn pool_transports(config: &crate::config::Config) -> Vec<crate::proto::v2::DataTransport> {
    use crate::proto::v2::DataTransport;
    let mut transports = vec![DataTransport::Tcp];
    if config.rdma_enabled() {
        transports.push(DataTransport::Rdma);
    }
    transports
}

fn random_token() -> Result<[u8; 32]> {
    use std::io::Read as _;
    let mut token = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut token)?;
    Ok(token)
}

/// Creates the socket's directory when it is missing. A directory that already
/// exists keeps its mode and ownership: the manager only sets the mode of a
/// directory it created, so pointing `--socket` into a host directory such as
/// `/tmp` or `/run` cannot change that directory's mode.
fn prepare_socket_directory(directory: &Path) -> Result<()> {
    if !directory.exists() {
        fs::create_dir_all(directory)?;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o770))?;
    }
    Ok(())
}

/// Bind the metrics listener before any thread exists, so a taken port fails startup while
/// the manager still owns nothing.
fn bind_metrics(address: Option<&str>) -> Result<Option<TcpListener>> {
    let Some(address) = address else {
        return Ok(None);
    };
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    Ok(Some(listener))
}

fn serve_metrics(
    listener: TcpListener,
    text: Arc<Mutex<String>>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(100)));
                    let mut request = [0; 1024];
                    let count = stream.read(&mut request).unwrap_or(0);
                    let (status, body) = if request[..count].starts_with(b"GET /metrics ") {
                        ("200 OK", text.lock().unwrap().clone())
                    } else {
                        ("404 Not Found", String::new())
                    };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(_) => break,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_shuts_down_a_stalled_owner_with_a_full_queue() {
        use std::sync::atomic::AtomicBool as Flag;
        use std::sync::mpsc as std_mpsc;

        let base = std::env::temp_dir().join(format!("selene-cleanup-{}", std::process::id()));
        let config = crate::store::Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            64 * 1024,
            64 << 20,
        );
        let store = crate::store::Store::open(config.clone()).unwrap();
        fs::create_dir_all(config.socket.parent().unwrap()).unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&config.socket).unwrap();
        let socket = config.socket.clone();
        let worker_socket = socket.clone();
        let root = config.root.clone().unwrap();
        let (commands, mut receiver) = mpsc::channel::<StoreCommand>(1);
        let (gate_tx, gate_rx) = std_mpsc::channel::<()>();
        let (gated_tx, gated_rx) = std_mpsc::channel::<()>();
        let (removed_tx, removed_rx) = std_mpsc::channel::<bool>();
        let mut cleanup = Cleanup {
            stop: Arc::new(Flag::new(false)),
            commands: commands.clone(),
            owner: None,
            tick: None,
            metrics: None,
            peer: None,
            data: None,
            socket: socket.clone(),
        };
        cleanup.owner = Some(thread::spawn(move || {
            let mut store = Some(store);
            let mut gated = false;
            while let Some(command) = receiver.blocking_recv() {
                if matches!(command, StoreCommand::Shutdown) {
                    let _ = removed_tx.send(!worker_socket.exists());
                    break;
                }
                if !gated {
                    // Hold the queue full until the test releases this thread.
                    gated = true;
                    let _ = gated_tx.send(());
                    let _ = gate_rx.recv_timeout(Duration::from_secs(10));
                }
            }
            drop(store.take());
        }));
        // The owner holds the first command, so the second one fills the queue and keeps it
        // full for longer than a bounded retry would wait.
        commands.try_send(StoreCommand::Tick).unwrap();
        gated_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the owner took the first command");
        commands.try_send(StoreCommand::Tick).unwrap();
        drop(commands);
        let finished = Arc::new(Flag::new(false));
        let flag = finished.clone();
        let worker = thread::spawn(move || {
            drop(cleanup);
            flag.store(true, Ordering::Release);
        });
        thread::sleep(Duration::from_millis(150));
        gate_tx.send(()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !finished.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "cleanup did not finish: the owner never saw Shutdown"
            );
            thread::sleep(Duration::from_millis(10));
        }
        worker.join().unwrap();
        assert_eq!(
            removed_rx.recv_timeout(Duration::from_secs(5)),
            Ok(true),
            "the socket is removed before the store drops"
        );
        assert!(!socket.exists());
        assert!(
            crate::disk::RootLock::acquire(&root).is_ok(),
            "the owner released the root lock"
        );
        drop(listener);
        let _ = std::fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn dropping_a_reservation_releases_its_token() {
        let (commands, mut receiver) = mpsc::channel(4);
        drop(Reservation::new(7, &commands));
        let released = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .expect("the release is sent");
        assert!(matches!(
            released,
            Some(StoreCommand::ReleaseFetch { token: 7 })
        ));
    }
}
