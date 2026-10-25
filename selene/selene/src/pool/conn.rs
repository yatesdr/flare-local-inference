//! The I/O thread: connect, check Hello, open the session, and run the engine
//! until the session ends; then drain and reconnect.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use hyper_util::rt::TokioIo;
use tokio::sync::{Notify, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Uri};
use tower::service_fn;

use crate::{CLOSE_TIMEOUT, Error, Result};

use super::{
    client::PoolAddress,
    engine::{Ended, Engine, PROTOCOL},
    net::{self, Dial},
    shared::PoolStatus,
    shm::{Attachment, Regions},
    startup::{Approve, PlannedRegion, RegisteredRegion, Startup},
    wire::{self, client_message::M as Out, server_message::M as In},
};

pub(crate) struct Link {
    pub address: PoolAddress,
    pub ca: Option<std::path::PathBuf>,
    /// Where the trusted certificates come from when there is no CA file.
    pub roots: net::Roots,
    pub wake: Arc<Notify>,
    pub stop: Arc<AtomicBool>,
    /// The registration window `PoolClient::start` waits on.
    pub startup: Arc<Startup>,
    /// The startup approval of the registration plan, if any.
    pub approve: Option<Approve>,
}

const HELLO_TIMEOUT: Duration = Duration::from_secs(3);
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const FIRST_BACKOFF: Duration = Duration::from_millis(100);
/// Reconnect promptly so a restarted manager can release the previous run's region hold.
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(1);
pub(crate) const BUSY_TICK: Duration = Duration::from_millis(1);
pub(crate) const IDLE_TICK: Duration = Duration::from_millis(20);
const OUTBOUND: usize = 4096;
const GOODBYE_WAIT: Duration = Duration::from_millis(500);

pub(crate) fn checked_limits(stated: Option<wire::Limits>) -> wire::Limits {
    stated.expect("Hello limits were validated")
}

/// Whether a Hello reply lets this client proceed. `connected_with` is what
/// the client connected with: TLS exactly when it has a CA. A reply that
/// declares anything else, or another protocol, disables the pool for the
/// process.
pub(crate) fn check_hello(
    reply: &wire::HelloReply,
    connected_with: wire::Encryption,
) -> std::result::Result<(), String> {
    if reply.status == wire::HelloStatus::VersionMismatch as i32 || reply.protocol != PROTOCOL {
        return Err(format!(
            "the pool manager speaks protocol {}, this client speaks {PROTOCOL}; using local tiers only",
            reply.protocol
        ));
    }
    if reply.status != wire::HelloStatus::HelloOk as i32 {
        return Err("the pool manager refused Hello; using local tiers only".into());
    }
    if reply.encryption != connected_with as i32 {
        return Err(format!(
            "the pool manager declares {:?} but this client connected with {connected_with:?}; using local tiers only",
            wire::Encryption::try_from(reply.encryption)
        ));
    }
    if reply.pool_name.is_empty()
        || reply.pool_name.len() > 255
        || reply.pool_name.chars().any(char::is_control)
    {
        return Err("the manager returned an invalid pool name".into());
    }
    super::limits::validate(reply.limits)?;
    Ok(())
}

/// What ended an attempt to reach the manager.
enum Attempt {
    /// Try again after a backoff.
    Retry(String),
    /// The pool is off for this process.
    Disabled(String),
    /// The session ended after it was open.
    Ended,
}

pub(crate) fn run(mut engine: Engine, link: Link) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::caused_by("cannot start the pool runtime", e))?;
    let result = runtime.block_on(serve(&mut engine, &link));
    // Whatever ended the thread, `start` must not keep waiting for it.
    link.startup.close();
    result
}

async fn serve(engine: &mut Engine, link: &Link) -> Result<()> {
    let mut backoff = FIRST_BACKOFF;
    let mut warned = false;
    loop {
        if link.stop.load(Ordering::SeqCst) {
            break;
        }
        if never_attaching(engine, link) {
            // `PoolClient::start` gave up and said so: stay on the local tiers.
            engine.shared.fail_lookups();
            idle_until_stop(engine, link).await;
            break;
        }
        let started = Instant::now();
        match attempt(engine, link).await {
            Attempt::Disabled(reason) => {
                link.startup.close();
                eprintln!("warning: {reason}");
                engine.shared.set_status(PoolStatus::Disabled(reason));
                engine.shared.fail_lookups();
                engine.close_ranks();
                idle_until_stop(engine, link).await;
                break;
            }
            Attempt::Retry(why) => {
                if !warned {
                    warned = true;
                    eprintln!(
                        "warning: the pool manager is not reachable ({why}); using local tiers, will keep trying"
                    );
                }
            }
            Attempt::Ended => {
                warned = false;
                if started.elapsed() > MAX_BACKOFF {
                    backoff = FIRST_BACKOFF;
                }
            }
        }
        if link.stop.load(Ordering::SeqCst) {
            break;
        }
        // The lost session's copies drain before anything is released.
        if !drain(engine, link, Some(backoff)).await {
            break;
        }
        if engine.quarantined() {
            idle_until_stop(engine, link).await;
            break;
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
    shutdown(engine, link).await
}

/// Keep serving commands and ticking until `wait` passes (or forever when
/// `None`) and the engine is quiet. `false` when stop was requested.
async fn drain(engine: &mut Engine, link: &Link, wait: Option<Duration>) -> bool {
    let until = wait.map(|w| Instant::now() + w);
    loop {
        let now = Instant::now();
        for command in engine.shared.take_commands() {
            engine.command(now, command);
        }
        engine.tick(now);
        if link.stop.load(Ordering::SeqCst) {
            return false;
        }
        if engine.quarantined() {
            return true;
        }
        if engine.quiet() && until.is_none_or(|u| now >= u) {
            return true;
        }
        let tick = if engine.busy() { BUSY_TICK } else { IDLE_TICK };
        let _ = tokio::time::timeout(tick, link.wake.notified()).await;
    }
}

async fn idle_until_stop(engine: &mut Engine, link: &Link) {
    while !link.stop.load(Ordering::SeqCst) {
        let now = Instant::now();
        for command in engine.shared.take_commands() {
            engine.command(now, command);
        }
        engine.tick(now);
        let _ = tokio::time::timeout(IDLE_TICK, link.wake.notified()).await;
    }
}

/// Close: stop submitting, drain, release, and say Goodbye if a session is open.
async fn shutdown(engine: &mut Engine, link: &Link) -> Result<()> {
    let _ = link;
    let started = Instant::now();
    engine.begin_close(Instant::now());
    while !engine.quiet() && started.elapsed() < CLOSE_TIMEOUT && !engine.quarantined() {
        engine.tick(Instant::now());
        tokio::time::sleep(BUSY_TICK).await;
    }
    if engine.quiet() && !engine.quarantined() {
        Ok(())
    } else {
        Err(Error::new(
            "the pool could not drain its transfers at close",
        ))
    }
}

/// Why the manager could not be reached.
pub(crate) enum Unreachable {
    /// Possibly for now.
    Retry(String),
    /// For as long as this process runs: its certificate cannot be verified.
    Untrusted(String),
}

impl From<String> for Unreachable {
    fn from(why: String) -> Self {
        Unreachable::Retry(why)
    }
}

/// The failure of a TLS handshake in a connection error's causes, if that is what failed.
fn tls_failure(error: &(dyn std::error::Error + 'static)) -> Option<String> {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = cause {
        let found = current.downcast_ref::<rustls::Error>().or_else(|| {
            current
                .downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        });
        if let Some(found) = found {
            return Some(match found {
                rustls::Error::InvalidCertificate(why) => {
                    use rustls::CertificateError as Cert;
                    match why {
                        Cert::UnknownIssuer => {
                            "the manager's certificate was issued by a CA that is not trusted"
                                .to_string()
                        }
                        Cert::BadSignature => {
                            "the manager's certificate was not signed by a trusted CA".to_string()
                        }
                        Cert::Expired | Cert::ExpiredContext { .. } => {
                            "the manager's certificate has expired".to_string()
                        }
                        Cert::NotValidYet | Cert::NotValidYetContext { .. } => {
                            "the manager's certificate is not valid yet".to_string()
                        }
                        Cert::NotValidForName | Cert::NotValidForNameContext { .. } => {
                            "the manager's certificate is not valid for the name it was reached by"
                                .to_string()
                        }
                        other => format!("the manager's certificate is not valid: {other:?}"),
                    }
                }
                other => format!("the TLS handshake failed: {other}"),
            });
        }
        cause = current.source();
    }
    None
}

/// PEM text for a DER certificate, which tonic takes.
fn pem_of(der: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut text = String::from("-----BEGIN CERTIFICATE-----\n");
    let mut line = 0;
    for chunk in der.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        for i in 0..4 {
            text.push(if i <= chunk.len() {
                ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char
            } else {
                '='
            });
            line += 1;
            if line == 64 {
                text.push('\n');
                line = 0;
            }
        }
    }
    if line != 0 {
        text.push('\n');
    }
    text.push_str("-----END CERTIFICATE-----\n");
    text
}

pub(crate) async fn connect(
    link: &Link,
) -> std::result::Result<(Channel, wire::Encryption), Unreachable> {
    match &link.address {
        PoolAddress::Unix(path) => {
            let path = path.clone();
            let channel = Endpoint::try_from("http://selene.local")
                .map_err(|e| e.to_string())?
                .connect_timeout(Duration::from_secs(2))
                .connect_with_connector(service_fn(move |_: Uri| {
                    let path = path.clone();
                    async move {
                        Ok::<_, std::io::Error>(TokioIo::new(
                            tokio::net::UnixStream::connect(path).await?,
                        ))
                    }
                }))
                .await
                .map_err(|e| e.to_string())?;
            Ok((channel, wire::Encryption::Plaintext))
        }
        PoolAddress::Tcp(address) | PoolAddress::TcpPlaintext(address) => {
            let tls = matches!(link.address, PoolAddress::Tcp(_));
            let scheme = if tls { "https" } else { "http" };
            let mut endpoint = Endpoint::from_shared(format!("{scheme}://{address}"))
                .map_err(|e| e.to_string())?
                .connect_timeout(Duration::from_secs(2))
                .tcp_nodelay(true);
            if tls {
                let certificates = net::trusted(link.ca.as_deref(), &link.roots)
                    .map_err(|e| Unreachable::Untrusted(e.to_string()))?;
                let host = address
                    .rsplit_once(':')
                    .map_or(address.as_str(), |(h, _)| h);
                let mut config = ClientTlsConfig::new().domain_name(host.trim_matches(['[', ']']));
                for certificate in &certificates {
                    config = config.ca_certificate(Certificate::from_pem(pem_of(certificate)));
                }
                endpoint = endpoint.tls_config(config).map_err(|e| e.to_string())?;
            }
            let channel = endpoint
                .connect()
                .await
                .map_err(|e| match tls_failure(&e) {
                    Some(why) => Unreachable::Untrusted(why),
                    None => Unreachable::Retry(e.to_string()),
                })?;
            Ok((
                channel,
                if tls {
                    wire::Encryption::Tls
                } else {
                    wire::Encryption::Plaintext
                },
            ))
        }
    }
}

/// Resolves once the owner asked to stop.
async fn stop_requested(link: &Link) {
    while !link.stop.load(Ordering::SeqCst) {
        let _ = tokio::time::timeout(Duration::from_millis(50), link.wake.notified()).await;
    }
}

/// An open session, ready to hand to the engine.
struct Established {
    hello: wire::HelloReply,
    opened: wire::SessionOpened,
    regions: Option<Arc<Regions>>,
    /// How this session opens data-channel transfers when it has no shared memory.
    dial: Option<Arc<dyn Dial>>,
    stream: tonic::Streaming<wire::ServerMessage>,
    tx: mpsc::Sender<wire::ClientMessage>,
}

/// A value that changes when the manager, its host or the way copies reach it do.
fn topology_of(hello: &wire::HelloReply, shared_memory: bool) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hello.manager_incarnation.hash(&mut hasher);
    hello.host_id.hash(&mut hasher);
    hello.data_transports.hash(&mut hasher);
    shared_memory.hash(&mut hasher);
    // Zero is reserved for "no session yet".
    hasher.finish() | 1
}

/// The startup window closed with the client unattached, so it never will be.
fn never_attaching(engine: &Engine, link: &Link) -> bool {
    engine.attachment.is_none() && link.startup.is_closed()
}

async fn attempt(engine: &mut Engine, link: &Link) -> Attempt {
    if never_attaching(engine, link) {
        return Attempt::Ended;
    }
    engine.shared.set_status(PoolStatus::Connecting);
    // Stopping interrupts connecting; once a session is open the engine owns it.
    let established = tokio::select! {
        result = establish(engine, link) => result,
        _ = stop_requested(link) => return Attempt::Ended,
    };
    let Established {
        hello,
        opened,
        regions,
        dial,
        mut stream,
        tx,
    } = match established {
        Ok(established) => established,
        Err(attempt) => return attempt,
    };
    engine
        .shared
        .set_topology(topology_of(&hello, regions.is_some()));
    if let Some(dial) = dial {
        engine.set_dial(dial);
    }
    engine.session_opened(
        hello.manager_incarnation,
        &opened,
        checked_limits(hello.limits),
        regions,
    );
    run_session(engine, link, &mut stream, tx).await;
    Attempt::Ended
}

async fn establish(engine: &mut Engine, link: &Link) -> std::result::Result<Established, Attempt> {
    let (channel, connected_with) = match connect(link).await {
        Ok(connected) => connected,
        Err(Unreachable::Retry(why)) => return Err(Attempt::Retry(why)),
        Err(Unreachable::Untrusted(why)) => {
            return Err(Attempt::Disabled(format!(
                "cannot verify the pool manager over TLS ({why}); trust its CA on this host or give the CA file; using local tiers only"
            )));
        }
    };
    let mut client = wire::pool_client::PoolClient::new(channel);
    let hello = tokio::time::timeout(
        HELLO_TIMEOUT,
        client.hello(wire::HelloRequest {
            protocol: PROTOCOL,
            client_version: env!("CARGO_PKG_VERSION").into(),
            connected_with: connected_with as i32,
        }),
    )
    .await;
    let hello = match hello {
        Ok(Ok(reply)) => reply.into_inner(),
        Ok(Err(status)) => return Err(Attempt::Retry(format!("Hello failed: {status}"))),
        Err(_) => return Err(Attempt::Retry("Hello timed out".into())),
    };
    if let Err(reason) = check_hello(&hello, connected_with) {
        return Err(Attempt::Disabled(reason));
    }
    Arc::make_mut(&mut engine.st)
        .bind_pool(&hello.pool_name)
        .map_err(|error| Attempt::Disabled(error.to_string()))?;
    if let Some(ranks) = &mut engine.ranks {
        ranks.bind(engine.st.instance_id);
    }
    let (regions, dial) = match attach(engine, link, &hello) {
        Ok(memory) => memory,
        Err(attempt) => return Err(attempt),
    };
    let st = engine.st.clone();
    let (tx, rx) = mpsc::channel::<wire::ClientMessage>(OUTBOUND);
    let open = wire::OpenSession {
        manager_incarnation: hello.manager_incarnation,
        instance_id: st.instance_id.to_vec(),
        scope: Some(super::codec::scope_to(&st.scope)),
        process_nonce: st.nonce.to_vec(),
        rank_secret: st.rank_secret.map(|s| s.to_vec()).unwrap_or_default(),
        export_slots: if regions.is_some() || dial.is_some() {
            st.export_slots
        } else {
            0
        },
        claim_slots: if regions.is_some() || dial.is_some() {
            st.claim_slots
        } else {
            0
        },
    };
    let _ = tx
        .send(wire::ClientMessage {
            header: None,
            m: Some(Out::Open(open)),
        })
        .await;
    let response =
        tokio::time::timeout(OPEN_TIMEOUT, client.session(ReceiverStream::new(rx))).await;
    let mut stream = match response {
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(status)) => return Err(Attempt::Retry(format!("session failed: {status}"))),
        Err(_) => return Err(Attempt::Retry("opening the session timed out".into())),
    };
    let opened = match tokio::time::timeout(OPEN_TIMEOUT, stream.message()).await {
        Ok(Ok(Some(wire::ServerMessage {
            m: Some(In::Opened(opened)),
            ..
        }))) => opened,
        Ok(Ok(_)) => {
            return Err(Attempt::Retry(
                "the manager did not open the session".into(),
            ));
        }
        Ok(Err(status)) => return Err(Attempt::Retry(format!("session failed: {status}"))),
        Err(_) => return Err(Attempt::Retry("opening the session timed out".into())),
    };
    match wire::OpenStatus::try_from(opened.status) {
        Ok(wire::OpenStatus::OpenOk) => {}
        Ok(wire::OpenStatus::InvalidScope) => {
            return Err(Attempt::Disabled(
                "the pool manager refused this instance's scope; using local tiers only".into(),
            ));
        }
        other => return Err(Attempt::Retry(format!("session not opened ({other:?})"))),
    }
    if opened.scope_id != st.scope_id {
        return Err(Attempt::Disabled(
            "the pool manager and this instance disagree on the scope id; using local tiers only"
                .into(),
        ));
    }
    Ok(Established {
        hello,
        opened,
        regions,
        dial,
        stream,
        tx,
    })
}

/// The data channel over TCP to the manager at `address`: staging made once for the
/// process, and TLS exactly when the client was given a CA.
fn net_dial(
    engine: &mut Engine,
    link: &Link,
    address: &str,
    limits: wire::Limits,
    rdma_offered: bool,
) -> Result<Arc<dyn Dial>> {
    let host = address
        .rsplit_once(':')
        .map_or(address, |(host, _)| host)
        .to_string();
    let tls = if matches!(link.address, PoolAddress::Tcp(_)) {
        Some(net::tls_trusting(
            net::trusted(link.ca.as_deref(), &link.roots)?,
            &host,
        )?)
    } else {
        None
    };
    let staging = engine.net_staging(&limits)?;
    Ok(Arc::new(net::Tcp {
        host,
        tls,
        staging,
        events: engine.net_events.clone(),
        rdma_offered,
        rdma_broken: engine.rdma_broken.clone(),
    }))
}

/// What this connection copies through: the manager's shared regions, this process's own
/// data-channel staging, or nothing.
///
/// The first connect decides, and registers what it needs while registration is open. Every
/// later connect must find what the first attached to: the same shared files continue with the
/// existing registrations; different ones turn the pool off, since they cannot be registered now.
fn attach(
    engine: &mut Engine,
    link: &Link,
    hello: &wire::HelloReply,
) -> std::result::Result<(Option<Arc<Regions>>, Option<Arc<dyn Dial>>), Attempt> {
    let limits = checked_limits(hello.limits);
    let rdma_offered = hello
        .data_transports
        .contains(&(wire::DataTransport::Rdma as i32));
    let network = match &link.address {
        PoolAddress::Tcp(address) | PoolAddress::TcpPlaintext(address)
            if !hello.data_transports.is_empty() =>
        {
            Some(address)
        }
        _ => None,
    };
    let dial = |engine: &mut Engine, address: &String| match net_dial(
        engine,
        link,
        address,
        limits.clone(),
        rdma_offered,
    ) {
        Ok(dial) => Some(dial),
        Err(error) => {
            eprintln!(
                "warning: cannot use the pool's data channel ({error}); restores and puts are off"
            );
            None
        }
    };
    match &engine.attachment {
        Some(Attachment::Shm { shared, regions }) => {
            let regions = regions.clone();
            match hello.shared_memory.as_ref() {
                Some(now) if now == shared => Ok((Some(regions), None)),
                other => Err(Attempt::Disabled(regions_changed(shared, other))),
            }
        }
        Some(Attachment::Net) => Ok((None, network.and_then(|address| dial(engine, address)))),
        Some(Attachment::Nothing) => Ok((None, None)),
        None => {
            // The first connect: the only time anything is registered.
            if !link.startup.begin() {
                return Err(Attempt::Ended);
            }
            let plan = match hello.shared_memory.as_ref() {
                Some(shared) => vec![
                    PlannedRegion {
                        name: "arena",
                        bytes: shared.arena_bytes,
                    },
                    PlannedRegion {
                        name: "staging",
                        bytes: shared.staging_bytes,
                    },
                ],
                None if network.is_some() => vec![PlannedRegion {
                    name: "data-staging",
                    bytes: engine.net_staging_bytes(&limits),
                }],
                None => Vec::new(),
            };
            if let Some(approve) = &link.approve {
                if let Err(reason) = approve(&plan) {
                    let sizes = plan
                        .iter()
                        .map(|region| format!("{} {} bytes", region.name, region.bytes))
                        .collect::<Vec<_>>()
                        .join(", ");
                    engine.close_registration();
                    link.startup.done(Vec::new());
                    return Err(Attempt::Disabled(format!(
                        "the pool's host memory ({sizes}) was not approved ({reason}); using local tiers only"
                    )));
                }
            }
            let mut registered = Vec::new();
            let (attachment, regions, net) = match hello.shared_memory.as_ref() {
                Some(shared) => match Regions::map(shared)
                    .and_then(|regions| register_regions(engine, regions))
                {
                    Ok(regions) => {
                        let regions = Arc::new(regions);
                        registered.push(registered_region("arena", &regions.arena));
                        registered.push(registered_region("staging", &regions.staging));
                        (
                            Attachment::Shm {
                                shared: shared.clone(),
                                regions: regions.clone(),
                            },
                            Some(regions),
                            None,
                        )
                    }
                    Err(error) => {
                        eprintln!(
                            "warning: cannot use the pool's shared memory ({error}); restores and exports are off"
                        );
                        (Attachment::Nothing, None, None)
                    }
                },
                // Without shared memory, windows pass through this process's own staging and the
                // manager's data channel, if it offers one.
                None => match network.and_then(|address| dial(engine, address)) {
                    Some(dial) => {
                        if let Some(staging) = &engine.net {
                            registered.push(registered_region("data-staging", staging.region()));
                        }
                        (Attachment::Net, None, Some(dial))
                    }
                    None => (Attachment::Nothing, None, None),
                },
            };
            engine.attachment = Some(attachment);
            engine.close_registration();
            link.startup.done(registered);
            Ok((regions, net))
        }
    }
}

pub(crate) fn registered_region(
    name: &'static str,
    region: &super::shm::Region,
) -> RegisteredRegion {
    RegisteredRegion {
        name,
        address: region.base(),
        bytes: region.len(),
    }
}

/// Why the manager's shared files no longer are the ones this process registered.
pub(crate) fn regions_changed(
    registered: &wire::SharedMemory,
    now: Option<&wire::SharedMemory>,
) -> String {
    let what = match now {
        None => "no longer offers shared memory".to_string(),
        Some(now) if now.regions_generation != registered.regions_generation => format!(
            "recreated its shared memory (generation {:016x}, was {:016x})",
            now.regions_generation, registered.regions_generation
        ),
        Some(_) => "moved its shared memory".to_string(),
    };
    format!(
        "the pool manager {what}; this instance registered the old regions at startup and cannot register others, so it uses its local tiers only until it is restarted"
    )
}

fn register_regions(engine: &Engine, mut regions: Regions) -> Result<Regions> {
    let registrar = engine.registrar();
    regions.arena.register(registrar, engine.st.context)?;
    regions.staging.register(registrar, engine.st.context)?;
    Ok(regions)
}

async fn run_session(
    engine: &mut Engine,
    link: &Link,
    stream: &mut tonic::Streaming<wire::ServerMessage>,
    tx: mpsc::Sender<wire::ClientMessage>,
) {
    let mut closing = false;
    let mut close_started = None;
    let mut said_goodbye = false;
    loop {
        let now = Instant::now();
        if !flush(engine, &tx) {
            break;
        }
        if link.stop.load(Ordering::SeqCst) && !closing {
            closing = true;
            close_started = Some(now);
            engine.begin_close(now);
            continue;
        }
        if close_started.is_some_and(|started| now.duration_since(started) > CLOSE_TIMEOUT) {
            break;
        }
        if closing && engine.quiet() {
            engine.goodbye();
            flush(engine, &tx);
            said_goodbye = true;
            break;
        }
        if engine.pump(now) {
            tokio::task::yield_now().await;
            continue;
        }
        let tick = if engine.busy() { BUSY_TICK } else { IDLE_TICK };
        tokio::select! {
            message = stream.message() => match message {
                Ok(Some(message)) => {
                    if let Err(ended) = engine.handle(Instant::now(), message) {
                        match ended {
                            Ended::Closing(why) | Ended::Violation(why) => {
                                eprintln!("warning: pool session ended: {why}");
                            }
                        }
                        break;
                    }
                }
                Ok(None) | Err(_) => break,
            },
            _ = link.wake.notified() => {
                let now = Instant::now();
                for command in engine.shared.take_commands() {
                    engine.command(now, command);
                }
            }
            _ = tokio::time::sleep(tick) => engine.tick(Instant::now()),
        }
    }
    drop(tx);
    if said_goodbye {
        // Let the transport deliver Goodbye: the manager ends its side of the
        // stream when ours ends.
        let _ = tokio::time::timeout(GOODBYE_WAIT, async {
            while let Ok(Some(_)) = stream.message().await {}
        })
        .await;
    }
    engine.session_lost(Instant::now());
}

/// Send what the engine queued. `false` if the manager is not reading.
fn flush(engine: &mut Engine, tx: &mpsc::Sender<wire::ClientMessage>) -> bool {
    for message in engine.take_out() {
        if tx.try_send(message).is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(
        status: wire::HelloStatus,
        protocol: u32,
        encryption: wire::Encryption,
    ) -> wire::HelloReply {
        wire::HelloReply {
            pool_name: "test".into(),
            limits: Some(super::super::limits::wire_defaults()),
            status: status as i32,
            protocol,
            encryption: encryption as i32,
            ..Default::default()
        }
    }

    #[test]
    fn hello_requires_a_valid_pool_name() {
        let mut hello = reply(
            wire::HelloStatus::HelloOk,
            PROTOCOL,
            wire::Encryption::Plaintext,
        );
        for name in [String::new(), "x".repeat(256), "bad\nname".into()] {
            hello.pool_name = name;
            assert!(check_hello(&hello, wire::Encryption::Plaintext).is_err());
        }
    }

    #[test]
    fn a_version_mismatch_disables_the_pool_with_one_reason() {
        let mismatch = reply(
            wire::HelloStatus::VersionMismatch,
            1,
            wire::Encryption::Plaintext,
        );
        let reason = check_hello(&mismatch, wire::Encryption::Plaintext).unwrap_err();
        assert!(reason.contains("protocol 1") && reason.contains("local tiers"));
        let other = reply(wire::HelloStatus::HelloOk, 3, wire::Encryption::Plaintext);
        assert!(check_hello(&other, wire::Encryption::Plaintext).is_err());
    }

    #[test]
    fn a_client_with_a_ca_never_accepts_plaintext_and_the_reverse_is_refused_too() {
        let plain = reply(
            wire::HelloStatus::HelloOk,
            PROTOCOL,
            wire::Encryption::Plaintext,
        );
        assert!(check_hello(&plain, wire::Encryption::Tls).is_err());
        let tls = reply(wire::HelloStatus::HelloOk, PROTOCOL, wire::Encryption::Tls);
        assert!(check_hello(&tls, wire::Encryption::Plaintext).is_err());
        assert!(check_hello(&tls, wire::Encryption::Tls).is_ok());
        assert!(check_hello(&plain, wire::Encryption::Plaintext).is_ok());
    }

    #[test]
    fn limits_must_be_stated_and_nonzero() {
        assert!(super::super::limits::validate(None).is_err());
        let mut limits = super::super::limits::wire_defaults();
        limits.max_candidates = 3;
        assert!(super::super::limits::validate(Some(limits)).is_ok());
        assert_eq!(checked_limits(Some(limits)).max_candidates, 3);
        limits.max_window_bytes = 0;
        assert!(super::super::limits::validate(Some(limits)).is_err());
    }
}
