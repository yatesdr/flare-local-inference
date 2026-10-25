//! Starting, serving and stopping a manager: the store's owner thread, the pool actor, the
//! Unix socket every instance on this host connects to, the optional listener for instances on
//! other hosts, and the metrics endpoint.

use crate::{
    Result, invalid,
    proto::v2::pool_server::PoolServer,
    store::{Config, Store, StoreCommand},
};
use std::{
    fs,
    io::{self, Read, Write},
    net::TcpListener,
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot},
};
use tokio_stream::{StreamExt, wrappers::TcpListenerStream, wrappers::UnixListenerStream};
use tonic::{transport::Server, transport::server::Connected};

const MAX_MESSAGE: usize = 16 * 1024 * 1024;

/// What a connection tells the service about its peer.
#[derive(Clone)]
pub struct PeerInfo {
    pub connection: u64,
    /// A Unix-domain connection, whose credentials are the kernel's.
    pub unix: bool,
    pub pid: u32,
    pub uid: u32,
    pub gid: u32,
}

/// One accepted connection, with the credentials the service needs.
pub struct PeerStream<S> {
    inner: S,
    info: PeerInfo,
}

impl<S> Connected for PeerStream<S> {
    type ConnectInfo = PeerInfo;

    fn connect_info(&self) -> PeerInfo {
        self.info.clone()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PeerStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PeerStream<S> {
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

/// The entry point for a manager with no network listener (tests and embedding).
pub fn run(config: Config, stop: Arc<AtomicBool>) -> Result<()> {
    serve(local_config(&config), config, stop)
}

/// Build the storage configuration for a manager config. The CLI never sets faults; tests and
/// disk-fault harnesses pass their own `store::Config` straight to `serve`.
pub fn store_config(config: &crate::config::Config) -> Result<Config> {
    let mut store = Config::new(
        config.root.clone().unwrap_or_default(),
        config.socket.clone(),
        usize::try_from(config.memory)
            .map_err(|_| invalid("--memory is too large for this host"))?,
        config.nvme,
    );
    store.name = config.name.clone();
    store.root = config.root.clone();
    store.staging = usize::try_from(config.staging)
        .map_err(|_| invalid("--staging is too large for this host"))?;
    store.metrics = config.metrics.map(|address| address.to_string());
    store.write_budget = config.write_budget;
    store.pool.source_wait = config.source_wait;
    store.pool.instance_retention = config.instance_retention;
    Ok(store)
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
    socket: PathBuf,
}

impl Cleanup {
    fn finish(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Unlink the socket while this process still holds the store's locks: no replacement
        // manager can have bound a new socket under the same path yet.
        let _ = fs::remove_file(&self.socket);
        // Delivery is guaranteed even when another sender is alive (the tick thread, the pool
        // or a handler task) and even with a full queue; the owner always exits, so the Store
        // and its locks are released. `serve` calls this from its own thread.
        let _ = self.commands.blocking_send(StoreCommand::Shutdown);
        // The tick and metrics threads exit on the flag.
        if let Some(thread) = self.tick.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.owner.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.metrics.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Opens the store and serves the pool protocol until `stop` is set, then drains the claims in
/// flight, closes the sessions and releases everything.
pub fn serve(
    config: crate::config::Config,
    store_config: Config,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    config.validate()?;
    for warning in config.startup_warnings() {
        eprintln!("{warning}");
    }
    // Validate TLS before anything is started or accepted: a certificate error must not
    // leave an accepting listener behind.
    validate_tls(&config)?;
    let data_tls = data_tls(&config)?;
    let pool_settings = store_config.pool.clone();
    let pool_root = store_config.root.clone();
    let store = Store::open(store_config)?;
    let regions = Arc::new(store.regions());
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
    let (commands, mut receiver) = mpsc::channel::<StoreCommand>(64);
    // Instances on other hosts copy through the data channel, which binds before the
    // pool exists so a taken port fails startup.
    let (channel, channel_events) = match config.data_address()? {
        Some(address) => {
            let listener = std::net::TcpListener::bind(address)?;
            let (hub, events) = crate::data::channel::Hub::start(
                runtime.handle(),
                listener,
                regions.clone(),
                data_tls,
                crate::pool::windows::MAX_WINDOW_PAGES as u32,
                rdma_device(&config, &regions)?,
            )?;
            (Some(hub), Some(events))
        }
        None => (None, None),
    };
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
        channel,
        regions: Some(regions.clone()),
        held: store.held_flag(),
    };
    if let Some(hub) = &pool_config.channel {
        hub.set_packed(staging_arena.clone());
    }
    let (pool_core, pool_handle, pool_events) = crate::pool::Core::new(
        pool_config,
        shm,
        staging_arena,
        true,
        recovered_records,
        Some(commands.clone()),
    );
    let pool_service = crate::pool::PoolService::new(pool_handle.clone());
    let pool_task = runtime.spawn(pool_core.run(pool_events));
    if let Some(mut events) = channel_events {
        let pool_events = pool_handle.events.clone();
        runtime.spawn(async move {
            while let Some(event) = events.recv().await {
                if pool_events
                    .send(crate::pool::core::Event::Channel(event))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
    }
    // Bind every listener before any thread starts, so a startup failure leaves no threads
    // and no locks behind; the guard below also removes the socket it creates.
    let metrics_text = Arc::new(Mutex::new(String::new()));
    let metrics_address = config.metrics.map(|address| address.to_string());
    let metrics_listener = bind_metrics(metrics_address.as_deref())?;
    let network_listener = match config.listen {
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
    let mut cleanup = Cleanup {
        stop: stop.clone(),
        commands: commands.clone(),
        owner: None,
        tick: None,
        metrics: None,
        socket: config.socket.clone(),
    };
    fs::set_permissions(&config.socket, fs::Permissions::from_mode(0o660))?;
    listener.set_nonblocking(true)?;
    if let Some(listener) = metrics_listener {
        cleanup.metrics = Some(serve_metrics(
            listener,
            metrics_text.clone(),
            pool_handle.shared.clone(),
            stop.clone(),
        ));
    }
    let owner_metrics = metrics_text.clone();
    cleanup.owner = Some(thread::spawn(move || {
        let mut store = store;
        'outer: while let Some(mut command) = receiver.blocking_recv() {
            // Drain the batch before dispatching so queued operations are ordered by the
            // scheduler rather than by arrival.
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
    let network_tls = server_tls(&config)?;
    let counter = Arc::new(AtomicU64::new(1));
    let unix_counter = counter.clone();
    let async_stop = stop.clone();
    // The network listener outlives the stop request until the pool has drained its claims.
    let serve_stop = Arc::new(AtomicBool::new(false));
    let network_stop = serve_stop.clone();
    let drain_wait =
        Duration::from_millis(u64::from(pool_settings.limits.max_claim_deadline_ms) + 2000);
    runtime.block_on(async move {
        let listener = tokio::net::UnixListener::from_std(listener)?;
        let incoming = UnixListenerStream::new(listener).map(move |stream| {
            let stream = stream?;
            let credentials = stream
                .peer_cred()
                .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
            Ok::<_, io::Error>(PeerStream {
                inner: stream,
                info: PeerInfo {
                    connection: unix_counter.fetch_add(1, Ordering::Relaxed),
                    unix: true,
                    pid: credentials.pid().unwrap_or(0) as u32,
                    uid: credentials.uid(),
                    gid: credentials.gid(),
                },
            })
        });
        let server = Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(Duration::from_secs(20)))
            .add_service(
                PoolServer::new(pool_service.clone())
                    .max_decoding_message_size(MAX_MESSAGE)
                    .max_encoding_message_size(MAX_MESSAGE),
            );
        let network_task = match network_listener {
            Some(listener) => {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let incoming = TcpListenerStream::new(listener).map(move |stream| {
                    let stream = stream?;
                    // The session carries small messages in both directions; Nagle's
                    // algorithm with the peer's delayed acknowledgments holds them for tens
                    // of milliseconds.
                    stream.set_nodelay(true)?;
                    Ok::<_, io::Error>(PeerStream {
                        inner: stream,
                        info: PeerInfo {
                            connection: counter.fetch_add(1, Ordering::Relaxed),
                            unix: false,
                            pid: 0,
                            uid: 0,
                            gid: 0,
                        },
                    })
                });
                let mut builder = Server::builder()
                    .http2_keepalive_interval(Some(Duration::from_secs(10)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(20)));
                if let Some(tls) = network_tls {
                    builder = builder
                        .tls_config(tls)
                        .map_err(|error| invalid(format!("TLS: {error}")))?;
                }
                let service = pool_service.clone();
                Some(tokio::spawn(async move {
                    let server = builder
                        .add_service(
                            PoolServer::new(service)
                                .max_decoding_message_size(MAX_MESSAGE)
                                .max_encoding_message_size(MAX_MESSAGE),
                        )
                        .serve_with_incoming(incoming);
                    tokio::select! {
                        result = server => result
                            .map_err(|error| invalid(format!("network server: {error}"))),
                        _ = watcher(network_stop) => Ok(()),
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
            _ = drain => Ok(()),
        };
        // A local-server error must not leave the network listener waiting for a stop that no
        // signal will set.
        serve_stop.store(true, Ordering::Release);
        pool_task.abort();
        if let Some(task) = network_task {
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

/// The RDMA device the data channel offers, when `--rdma` asks for it. A manager that was asked
/// for RDMA and cannot have it does not start.
#[cfg(feature = "rdma")]
fn rdma_device(
    config: &crate::config::Config,
    regions: &Arc<crate::regions::LocalRegions>,
) -> Result<Option<Arc<dyn crate::data::channel::Rdma>>> {
    if !config.rdma_enabled() {
        return Ok(None);
    }
    let device = crate::data::ibv::Device::open(regions.clone())
        .map_err(|error| invalid(format!("--rdma: {error}")))?;
    Ok(Some(Arc::new(device)))
}

#[cfg(not(feature = "rdma"))]
fn rdma_device(
    _: &crate::config::Config,
    _: &Arc<crate::regions::LocalRegions>,
) -> Result<Option<Arc<dyn crate::data::channel::Rdma>>> {
    Ok(None)
}

/// The data channel's TLS configuration: the same certificate and key as the session listener.
fn data_tls(config: &crate::config::Config) -> Result<Option<Arc<rustls::ServerConfig>>> {
    let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) else {
        return Ok(None);
    };
    Ok(Some(crate::data::channel::server_config(
        &fs::read(cert)?,
        &fs::read(key)?,
    )?))
}

/// The manager's identity for clients that verify it against their own CA. There are no
/// client certificates: an instance is told apart by its session, not by a credential.
fn server_tls(config: &crate::config::Config) -> Result<Option<tonic::transport::ServerTlsConfig>> {
    let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) else {
        return Ok(None);
    };
    Ok(Some(tonic::transport::ServerTlsConfig::new().identity(
        tonic::transport::Identity::from_pem(fs::read(cert)?, fs::read(key)?),
    )))
}

/// The data transports Hello offers. Only a manager that listens for other hosts offers any.
fn pool_transports(config: &crate::config::Config) -> Vec<crate::proto::v2::DataTransport> {
    use crate::proto::v2::DataTransport;
    if config.listen.is_none() {
        return Vec::new();
    }
    let mut transports = vec![DataTransport::Tcp];
    if config.rdma_enabled() {
        transports.push(DataTransport::Rdma);
    }
    transports
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
    pool: Arc<crate::pool::Shared>,
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
                        ("200 OK", {
                            let mut body = text.lock().unwrap().clone();
                            body.push_str(&pool.metrics.lock().unwrap());
                            body
                        })
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
        let mut config = crate::store::Config::new(
            base.join("records"),
            base.join("run/m.sock"),
            64 * 1024,
            64 << 20,
        );
        config.name = format!("selene-cleanup-{}", std::process::id());
        config.staging = 8 << 20;
        let shm = std::path::Path::new(crate::local::SHM_ROOT).join(&config.name);
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
        let _ = std::fs::remove_dir_all(shm);
    }
}
