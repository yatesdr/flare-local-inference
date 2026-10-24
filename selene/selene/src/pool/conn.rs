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
    shared::PoolStatus,
    shm::Regions,
    wire::{self, client_message::M as Out, server_message::M as In},
};

pub(crate) struct Link {
    pub address: PoolAddress,
    pub ca: Option<std::path::PathBuf>,
    pub wake: Arc<Notify>,
    pub stop: Arc<AtomicBool>,
}

const HELLO_TIMEOUT: Duration = Duration::from_secs(3);
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const BUSY_TICK: Duration = Duration::from_millis(1);
const IDLE_TICK: Duration = Duration::from_millis(20);
const OUTBOUND: usize = 4096;

/// Limits the manager left unstated take the protocol's defaults.
pub(crate) fn limits_or_default(stated: Option<wire::Limits>) -> wire::Limits {
    let mut l = stated.unwrap_or_default();
    let defaults = [
        (&mut l.max_concurrent_lookups, 8),
        (&mut l.max_candidates, 8),
        (&mut l.max_chain_nodes, 4096),
        (&mut l.max_boundaries, 8192),
        (&mut l.max_notices_per_batch, 256),
        (&mut l.max_unacked_notices, 16384),
        (&mut l.max_resync_chunk_entries, 1024),
        (&mut l.max_resync_entries, 4_000_000),
        (&mut l.max_resync_buffered_notices, 65536),
        (&mut l.max_claims_in_flight, 2),
        (&mut l.max_export_slots, 2),
        (&mut l.window_credits, 2),
        (&mut l.max_claim_deadline_ms, 30000),
        (&mut l.pin_deadline_ms, 250),
        (&mut l.progress_timeout_ms, 3000),
        (&mut l.max_open_puts, 4),
        (&mut l.resync_timeout_ms, 5000),
    ];
    for (field, default) in defaults {
        if *field == 0 {
            *field = default;
        }
    }
    if l.max_window_bytes == 0 {
        l.max_window_bytes = 4 << 20;
    }
    l
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
    runtime.block_on(serve(&mut engine, &link))
}

async fn serve(engine: &mut Engine, link: &Link) -> Result<()> {
    let mut backoff = FIRST_BACKOFF;
    let mut warned = false;
    loop {
        if link.stop.load(Ordering::SeqCst) {
            break;
        }
        let started = Instant::now();
        match attempt(engine, link).await {
            Attempt::Disabled(reason) => {
                eprintln!("warning: {reason}");
                engine.shared.set_status(PoolStatus::Disabled(reason));
                engine.shared.fail_lookups();
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

async fn connect(link: &Link) -> std::result::Result<(Channel, wire::Encryption), String> {
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
        PoolAddress::Tcp(address) => {
            let tls = link.ca.is_some();
            let scheme = if tls { "https" } else { "http" };
            let mut endpoint = Endpoint::from_shared(format!("{scheme}://{address}"))
                .map_err(|e| e.to_string())?
                .connect_timeout(Duration::from_secs(2))
                .tcp_nodelay(true);
            if let Some(ca) = &link.ca {
                let pem = std::fs::read(ca).map_err(|e| format!("cannot read the CA file: {e}"))?;
                let host = address
                    .rsplit_once(':')
                    .map_or(address.as_str(), |(h, _)| h);
                endpoint = endpoint
                    .tls_config(
                        ClientTlsConfig::new()
                            .ca_certificate(Certificate::from_pem(pem))
                            .domain_name(host.trim_matches(['[', ']'])),
                    )
                    .map_err(|e| e.to_string())?;
            }
            let channel = endpoint.connect().await.map_err(|e| e.to_string())?;
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

async fn attempt(engine: &mut Engine, link: &Link) -> Attempt {
    engine.shared.set_status(PoolStatus::Connecting);
    let (channel, connected_with) = match connect(link).await {
        Ok(connected) => connected,
        Err(why) => return Attempt::Retry(why),
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
        Ok(Err(status)) => return Attempt::Retry(format!("Hello failed: {status}")),
        Err(_) => return Attempt::Retry("Hello timed out".into()),
    };
    if let Err(reason) = check_hello(&hello, connected_with) {
        return Attempt::Disabled(reason);
    }
    let regions = match hello.shared_memory.as_ref() {
        Some(shared) => match map_regions(engine, shared) {
            Ok(regions) => Some(Arc::new(regions)),
            Err(error) => {
                eprintln!(
                    "warning: cannot use the pool's shared memory ({error}); restores and exports are off"
                );
                None
            }
        },
        None => None,
    };
    let st = engine.st.clone();
    let (tx, rx) = mpsc::channel::<wire::ClientMessage>(OUTBOUND);
    let open = wire::OpenSession {
        manager_incarnation: hello.manager_incarnation,
        instance_id: st.instance_id.to_vec(),
        instance_name: st.name.clone(),
        scope: Some(super::codec::scope_to(&st.scope)),
        process_nonce: st.nonce.to_vec(),
        export_slots: if regions.is_some() {
            st.export_slots
        } else {
            0
        },
        claim_slots: if regions.is_some() { st.claim_slots } else { 0 },
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
        Ok(Err(status)) => return Attempt::Retry(format!("session failed: {status}")),
        Err(_) => return Attempt::Retry("opening the session timed out".into()),
    };
    let opened = match tokio::time::timeout(OPEN_TIMEOUT, stream.message()).await {
        Ok(Ok(Some(wire::ServerMessage {
            m: Some(In::Opened(opened)),
            ..
        }))) => opened,
        Ok(Ok(_)) => return Attempt::Retry("the manager did not open the session".into()),
        Ok(Err(status)) => return Attempt::Retry(format!("session failed: {status}")),
        Err(_) => return Attempt::Retry("opening the session timed out".into()),
    };
    match wire::OpenStatus::try_from(opened.status) {
        Ok(wire::OpenStatus::OpenOk) => {}
        Ok(wire::OpenStatus::InvalidScope) => {
            return Attempt::Disabled(
                "the pool manager refused this instance's scope; using local tiers only".into(),
            );
        }
        other => return Attempt::Retry(format!("session not opened ({other:?})")),
    }
    if opened.scope_id != st.scope_id {
        return Attempt::Disabled(
            "the pool manager and this instance disagree on the scope id; using local tiers only"
                .into(),
        );
    }
    engine.session_opened(
        hello.manager_incarnation,
        &opened,
        limits_or_default(hello.limits),
        regions,
    );
    run_session(engine, link, &mut stream, tx).await;
    Attempt::Ended
}

fn map_regions(engine: &Engine, shared: &wire::SharedMemory) -> Result<Regions> {
    let mut regions = Regions::map(shared)?;
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
    loop {
        let now = Instant::now();
        if !flush(engine, &tx) {
            break;
        }
        if link.stop.load(Ordering::SeqCst) && !closing {
            closing = true;
            engine.begin_close(now);
            continue;
        }
        if closing && engine.quiet() {
            engine.goodbye();
            flush(engine, &tx);
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
            status: status as i32,
            protocol,
            encryption: encryption as i32,
            ..Default::default()
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
    fn unstated_limits_take_the_protocol_defaults_and_stated_ones_are_kept() {
        let limits = limits_or_default(Some(wire::Limits {
            max_candidates: 3,
            ..Default::default()
        }));
        assert_eq!(limits.max_candidates, 3);
        assert_eq!(limits.max_notices_per_batch, 256);
        assert_eq!(limits.pin_deadline_ms, 250);
        assert_eq!(limits_or_default(None).max_window_bytes, 4 << 20);
    }
}
