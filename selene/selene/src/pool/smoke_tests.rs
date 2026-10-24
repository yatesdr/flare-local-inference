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
        wire::{self, client_message::M as Out, server_message::M as In},
    },
    testing::{StoppedCopier, geometry},
};

struct Server {
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
                                scope_id: id.to_vec(),
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
    let path =
        std::env::temp_dir().join(format!("selene-smoke-{name}-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let opened = Arc::new(Mutex::new(0));
    let server = Server {
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
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar), None).unwrap();
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
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar), None).unwrap();
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
    let mut client = PoolClient::start(config, copier, Arc::new(NoRegistrar), None).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_ne!(client.status(), PoolStatus::Ready);
    let mut events = Vec::new();
    client.poll(&mut events).unwrap();
    client.close().unwrap();
}
