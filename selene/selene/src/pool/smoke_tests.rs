//! The connection task against a minimal in-process server speaking the wire
//! protocol over a Unix socket. It checks the transport wiring only (connect,
//! Hello, open, snapshot, notices, Goodbye); the protocol's behaviour is tested
//! on the engine, and against the real manager once it is in this tree.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio_stream::{
    Stream, StreamExt,
    wrappers::{ReceiverStream, UnixListenerStream},
};

use crate::{
    PrefixKey, Registration, Result,
    pool::{
        IndexEntry, PoolAddress, PoolClient, PoolConfig, PoolStatus, Tier,
        identity::{Geometry, RankPlanes, Scope, scope_id},
        net,
        wire::{self, client_message::M as Out, server_message::M as In},
    },
    testing::{StoppedCopier, geometry},
};

struct Server {
    /// A scope id to answer with instead of the right one.
    wrong_scope: bool,
    hello: wire::HelloReply,
    seen: Arc<Mutex<Vec<wire::ClientMessage>>>,
    opened: Arc<Mutex<u32>>,
}

type Replies =
    Pin<Box<dyn Stream<Item = std::result::Result<wire::ServerMessage, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl wire::pool_server::Pool for Server {
    async fn hello(
        &self,
        _: tonic::Request<wire::HelloRequest>,
    ) -> std::result::Result<tonic::Response<wire::HelloReply>, tonic::Status> {
        Ok(tonic::Response::new(self.hello.clone()))
    }

    type SessionStream = Replies;

    async fn session(
        &self,
        request: tonic::Request<tonic::Streaming<wire::ClientMessage>>,
    ) -> std::result::Result<tonic::Response<Self::SessionStream>, tonic::Status> {
        *self.opened.lock().unwrap() += 1;
        let mut incoming = request.into_inner();
        let seen = self.seen.clone();
        let incarnation = self.hello.manager_incarnation;
        let wrong_scope = self.wrong_scope;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let header = Some(wire::Header {
                manager_incarnation: incarnation,
                session_incarnation: 1,
            });
            while let Some(Ok(message)) = incoming.next().await {
                let reply = match &message.m {
                    Some(Out::Open(open)) => {
                        let scope = open.scope.clone().unwrap();
                        let geometry = scope.geometry.clone().unwrap();
                        let id = scope_id(
                            &scope.tenant,
                            scope.model_identity.as_slice().try_into().unwrap(),
                            scope.layout_fingerprint.as_slice().try_into().unwrap(),
                            &Geometry {
                                page_tokens: geometry.page_tokens,
                                ranks: geometry
                                    .ranks
                                    .iter()
                                    .map(|r| RankPlanes {
                                        plane_bytes: r.plane_bytes.clone(),
                                        row_bytes: r.row_bytes,
                                    })
                                    .collect(),
                                page_record_bytes: geometry.page_record_bytes,
                                row_record_bytes: geometry.row_record_bytes,
                            },
                        )
                        .unwrap();
                        Some(wire::ServerMessage {
                            header: None,
                            m: Some(In::Opened(wire::SessionOpened {
                                status: wire::OpenStatus::OpenOk as i32,
                                session_incarnation: 1,
                                scope_id: if wrong_scope {
                                    vec![0; 32]
                                } else {
                                    id.to_vec()
                                },
                                export_slots: 0,
                                claim_slots: 0,
                                readiness: Some(wire::ReadinessInfo {
                                    state: wire::ReadinessState::Ready as i32,
                                    ..Default::default()
                                }),
                                resync_required: true,
                            })),
                        })
                    }
                    Some(Out::ResyncEnd(end)) => Some(wire::ServerMessage {
                        header,
                        m: Some(In::ResyncInstalled(wire::ResyncInstalled {
                            resync_id: end.resync_id,
                            watermark: 0,
                            entries: end.entries,
                            applied_through: 0,
                        })),
                    }),
                    _ => None,
                };
                seen.lock().unwrap().push(message);
                if let Some(reply) = reply
                    && tx.send(Ok(reply)).await.is_err()
                {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

struct NoRegistrar;
impl crate::HostRegistrar for NoRegistrar {
    fn register(&self, _: usize, _: usize, _: usize) -> Result<Box<dyn Registration>> {
        Err("not used".into())
    }
    fn map_split(&self, _: &[(usize, i32, usize)]) -> Result<Box<dyn crate::SplitMapping>> {
        Err("not used".into())
    }
}

struct Fixture {
    seen: Arc<Mutex<Vec<wire::ClientMessage>>>,
    opened: Arc<Mutex<u32>>,
    path: std::path::PathBuf,
    _runtime: tokio::runtime::Runtime,
}

fn serve(name: &str, hello: wire::HelloReply) -> Fixture {
    serve_with(name, hello, false)
}

fn serve_with(name: &str, hello: wire::HelloReply, wrong_scope: bool) -> Fixture {
    let path =
        std::env::temp_dir().join(format!("selene-smoke-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let opened = Arc::new(Mutex::new(0));
    let server = Server {
        wrong_scope,
        hello,
        seen: seen.clone(),
        opened: opened.clone(),
    };
    let listener = {
        let _guard = runtime.enter();
        tokio::net::UnixListener::bind(&path).unwrap()
    };
    runtime.spawn(
        tonic::transport::Server::builder()
            .add_service(wire::pool_server::PoolServer::new(server))
            .serve_with_incoming(UnixListenerStream::new(listener)),
    );
    Fixture {
        seen,
        opened,
        path,
        _runtime: runtime,
    }
}

fn hello(protocol: u32, status: wire::HelloStatus) -> wire::HelloReply {
    wire::HelloReply {
        status: status as i32,
        protocol,
        manager_incarnation: 99,
        encryption: wire::Encryption::Plaintext as i32,
        readiness: Some(wire::ReadinessInfo {
            state: wire::ReadinessState::Ready as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn config(path: &std::path::Path) -> (PoolConfig, crate::SharedCopier) {
    let g = geometry(64, &[("k", 4096), ("v", 4096)], 4096);
    let copier = StoppedCopier::shared(std::slice::from_ref(&g)).unwrap();
    let config = PoolConfig {
        address: PoolAddress::Unix(path.to_path_buf()),
        tenant: "t".into(),
        ca: None,
        scope: Scope {
            tenant: "t".into(),
            model_identity: [1; 32],
            layout_fingerprint: [2; 16],
            geometry: Geometry {
                page_tokens: 64,
                ranks: vec![RankPlanes {
                    plane_bytes: vec![4096, 4096],
                    row_bytes: 4096,
                }],
                page_record_bytes: 8192,
                row_record_bytes: 4096,
            },
        },
        instance_name: "smoke".into(),
        gpu_uuids: vec![[7; 16]],
        export_slots: 2,
        claim_slots: 2,
        level_order: None,
        registration_context: 0,
    };
    (config, copier)
}

fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_client_connects_opens_a_session_syncs_announces_and_says_goodbye() {
    let server = serve("ok", hello(2, wire::HelloStatus::HelloOk));
    let (config, copier) = config(&server.path);
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar)).unwrap();
    wait_for("Ready", || client.status() == PoolStatus::Ready);
    client.saved(IndexEntry {
        key: PrefixKey {
            consumed_tokens: 64,
            digest: [4; 16],
        },
        parent: None,
        bucket: [0; 16],
        tail_pages: 1,
        row: true,
        tier: Tier::Gpu,
        generation: 1,
        bytes: 8192,
    });
    let notices = || {
        server
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|m| matches!(m.m, Some(Out::Notices(_))))
            .count()
    };
    wait_for("the notice", || notices() == 1);
    client.close().unwrap();
    wait_for("Goodbye", || {
        server
            .seen
            .lock()
            .unwrap()
            .iter()
            .any(|m| matches!(m.m, Some(Out::Goodbye(_))))
    });
    let seen = server.seen.lock().unwrap();
    let open = seen
        .iter()
        .find_map(|m| match &m.m {
            Some(Out::Open(open)) => Some(open),
            _ => None,
        })
        .unwrap();
    assert_eq!(open.manager_incarnation, 99);
    assert_eq!(open.instance_id.len(), 16);
    assert_eq!(open.process_nonce.len(), 16);
    assert_eq!(open.export_slots, 0, "no shared memory, so no exports");
    assert!(matches!(
        seen.last().and_then(|m| m.m.as_ref()),
        Some(Out::Goodbye(g)) if g.quiesced
    ));
    let _ = std::fs::remove_file(&server.path);
}

#[test]
fn a_protocol_mismatch_disables_the_pool_without_opening_a_session() {
    let server = serve("mismatch", hello(1, wire::HelloStatus::VersionMismatch));
    let (config, copier) = config(&server.path);
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar)).unwrap();
    wait_for("Disabled", || {
        matches!(client.status(), PoolStatus::Disabled(_))
    });
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(*server.opened.lock().unwrap(), 0);
    client.close().unwrap();
    let _ = std::fs::remove_file(&server.path);
}

#[test]
fn an_unreachable_manager_leaves_the_client_local_and_close_returns() {
    let path = std::env::temp_dir().join(format!("selene-smoke-none-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let (config, copier) = config(&path);
    let started = Instant::now();
    let mut client = PoolClient::start_waiting(
        config,
        copier,
        Arc::new(NoRegistrar),
        net::Roots::System,
        Duration::from_millis(300),
    )
    .unwrap();
    // `start` returns when the wait is up, not before and not much after.
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(5));
    let PoolStatus::Disabled(reason) = client.status() else {
        panic!("{:?}", client.status());
    };
    assert!(
        reason.contains("did not answer") && reason.contains("will not attach"),
        "{reason}"
    );
    assert!(client.registered_regions().is_empty());
    let mut events = Vec::new();
    client.poll(&mut events).unwrap();
    client.close().unwrap();
}

#[test]
fn a_scope_id_the_manager_computes_differently_disables_the_pool() {
    let server = serve_with("scope", hello(2, wire::HelloStatus::HelloOk), true);
    let (config, copier) = config(&server.path);
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar)).unwrap();
    wait_for("Disabled", || {
        matches!(client.status(), PoolStatus::Disabled(_))
    });
    client.close().unwrap();
    let _ = std::fs::remove_file(&server.path);
}

// ---- TLS ----------------------------------------------------------------------

struct Pki {
    ca_der: Vec<u8>,
    ca_pem: String,
    cert_pem: String,
    key_pem: String,
}

fn pki() -> Pki {
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(Vec::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "selene test ca");
    let ca = params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(params, ca_key);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .signed_by(&leaf_key, &issuer)
        .unwrap();
    Pki {
        ca_der: ca.der().to_vec(),
        ca_pem: ca.pem(),
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
    }
}

/// A server on a TCP port, with TLS when `pki` is given.
fn serve_tcp(hello: wire::HelloReply, pki: Option<&Pki>) -> (Fixture, u16) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let opened = Arc::new(Mutex::new(0));
    let server = Server {
        wrong_scope: false,
        hello,
        seen: seen.clone(),
        opened: opened.clone(),
    };
    let listener = {
        let _guard = runtime.enter();
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        tokio::net::TcpListener::from_std(std_listener).unwrap()
    };
    let port = listener.local_addr().unwrap().port();
    let mut builder = tonic::transport::Server::builder();
    if let Some(pki) = pki {
        builder = builder
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(&pki.cert_pem, &pki.key_pem),
            ))
            .unwrap();
    }
    runtime.spawn(
        builder
            .add_service(wire::pool_server::PoolServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (
        Fixture {
            seen,
            opened,
            path: std::path::PathBuf::new(),
            _runtime: runtime,
        },
        port,
    )
}

fn tcp_config(
    port: u16,
    ca: Option<&Pki>,
    dir: &std::path::Path,
) -> (PoolConfig, crate::SharedCopier) {
    let (mut config, copier) = config(&dir.join("unused"));
    config.address = PoolAddress::Tcp(format!("localhost:{port}"));
    config.ca = ca.map(|pki| {
        let path = dir.join(format!("ca-{port}.pem"));
        std::fs::write(&path, &pki.ca_pem).unwrap();
        path
    });
    (config, copier)
}

fn tls_hello() -> wire::HelloReply {
    let mut reply = hello(2, wire::HelloStatus::HelloOk);
    reply.encryption = wire::Encryption::Tls as i32;
    reply
}

#[test]
fn a_client_with_a_ca_requires_tls_and_connects_to_a_tls_manager() {
    let pki = pki();
    let (server, port) = serve_tcp(tls_hello(), Some(&pki));
    let (config, copier) = tcp_config(port, Some(&pki), &std::env::temp_dir());
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar)).unwrap();
    wait_for("Ready over TLS", || client.status() == PoolStatus::Ready);
    assert_eq!(*server.opened.lock().unwrap(), 1);
    client.close().unwrap();
}

/// The reason a client gave for turning the pool off, once it has.
fn disabled_reason(client: &PoolClient) -> String {
    let mut reason = None;
    wait_for("the pool to be turned off", || {
        if let PoolStatus::Disabled(why) = client.status() {
            reason = Some(why);
            true
        } else {
            false
        }
    });
    reason.unwrap()
}

#[test]
fn a_client_without_a_ca_file_trusts_the_hosts_store_and_connects() {
    let pki = pki();
    let (server, port) = serve_tcp(tls_hello(), Some(&pki));
    let (config, copier) = tcp_config(port, None, &std::env::temp_dir());
    // The host's store, here a test's, holds the CA: no flag names it.
    let roots = net::Roots::Fixed(vec![pki.ca_der.clone().into()]);
    let mut client =
        PoolClient::start_trusting(config, copier, Arc::new(NoRegistrar), roots).unwrap();
    wait_for("Ready over TLS", || client.status() == PoolStatus::Ready);
    assert_eq!(*server.opened.lock().unwrap(), 1);
    client.close().unwrap();
}

#[test]
fn a_client_whose_store_does_not_trust_the_manager_says_why_once_and_stays_local() {
    let pki = pki();
    let other = self::pki();
    let (server, port) = serve_tcp(tls_hello(), Some(&pki));
    let (config, copier) = tcp_config(port, None, &std::env::temp_dir());
    // A store that holds a different CA: the manager's certificate has no path to it.
    let roots = net::Roots::Fixed(vec![other.ca_der.clone().into()]);
    let mut client =
        PoolClient::start_trusting(config, copier, Arc::new(NoRegistrar), roots).unwrap();
    let reason = disabled_reason(&client);
    assert!(
        reason.contains("cannot verify the pool manager over TLS")
            && reason.contains("not signed by a trusted CA"),
        "{reason}"
    );
    // It does not keep trying: the manager sees no session now or later.
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(*server.opened.lock().unwrap(), 0);
    assert_eq!(client.status(), PoolStatus::Disabled(reason));
    client.close().unwrap();
}

#[test]
fn a_ca_file_overrides_the_hosts_store() {
    let pki = pki();
    let other = self::pki();
    let (server, port) = serve_tcp(tls_hello(), Some(&pki));
    let (config, copier) = tcp_config(port, Some(&pki), &std::env::temp_dir());
    // The store trusts something else; the file names the right CA.
    let roots = net::Roots::Fixed(vec![other.ca_der.clone().into()]);
    let mut client =
        PoolClient::start_trusting(config, copier, Arc::new(NoRegistrar), roots).unwrap();
    wait_for("Ready with the CA file", || {
        client.status() == PoolStatus::Ready
    });
    assert_eq!(*server.opened.lock().unwrap(), 1);
    client.close().unwrap();
}

#[test]
fn a_plaintext_network_address_is_explicit() {
    let (server, port) = serve_tcp(hello(2, wire::HelloStatus::HelloOk), None);
    let (mut config, copier) = tcp_config(port, None, &std::env::temp_dir());
    config.address = PoolAddress::TcpPlaintext(format!("localhost:{port}"));
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar)).unwrap();
    wait_for("Ready without TLS", || client.status() == PoolStatus::Ready);
    assert_eq!(*server.opened.lock().unwrap(), 1);
    client.close().unwrap();
}

#[test]
fn a_client_with_a_ca_never_downgrades_to_a_plaintext_manager() {
    let pki = pki();
    let (server, port) = serve_tcp(hello(2, wire::HelloStatus::HelloOk), None);
    let (config, copier) = tcp_config(port, Some(&pki), &std::env::temp_dir());
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar)).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_ne!(client.status(), PoolStatus::Ready);
    assert_eq!(*server.opened.lock().unwrap(), 0);
    client.close().unwrap();
}
