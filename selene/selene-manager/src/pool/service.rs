//! The gRPC `Pool` service: `Hello`, and one `Session` stream per instance process.

use super::core::{Event, OpenPeer, PoolHandle};
use crate::control::PeerInfo;
use crate::proto::v2::{
    self, ClientMessage, HelloReply, HelloRequest, ServerMessage, pool_server::Pool,
};
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

/// Outbound messages queued per session before it is considered stuck.
const OUTBOUND: usize = 4096;

type SessionStream = Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send>>;
type RankStream = Pin<Box<dyn Stream<Item = Result<v2::RankServerMessage, Status>> + Send>>;

#[derive(Clone)]
pub struct PoolService {
    handle: PoolHandle,
}

impl PoolService {
    pub fn new(handle: PoolHandle) -> Self {
        Self { handle }
    }
}

#[tonic::async_trait]
impl Pool for PoolService {
    type SessionStream = SessionStream;
    type RankStream = RankStream;

    async fn rank(
        &self,
        request: Request<Streaming<v2::RankClientMessage>>,
    ) -> Result<Response<Self::RankStream>, Status> {
        let info = request.extensions().get::<PeerInfo>().cloned();
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("the stream ended before `attach`"))?;
        let Some(v2::rank_client_message::M::Attach(attach)) = first.m else {
            return Err(Status::invalid_argument(
                "the first message must be `attach`",
            ));
        };
        let (tx, rx) = mpsc::channel(OUTBOUND);
        let (reply, answer) = oneshot::channel();
        let peer = info.map_or(
            OpenPeer {
                unix: false,
                pid: 0,
                uid: 0,
                gid: 0,
            },
            |info| OpenPeer {
                unix: info.unix,
                pid: info.pid,
                uid: info.uid,
                gid: info.gid,
            },
        );
        let events = self.handle.events.clone();
        events
            .send(Event::RankOpen {
                peer,
                attach: Box::new(attach),
                tx,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("the pool is stopped"))?;
        let aid = answer
            .await
            .map_err(|_| Status::unavailable("the pool is stopped"))?;
        if let Some(aid) = aid {
            tokio::spawn(async move {
                while let Ok(Some(message)) = inbound.message().await {
                    let event = Event::RankMessage {
                        aid,
                        message: Box::new(message),
                    };
                    if events.send(event).await.is_err() {
                        return;
                    }
                }
                let _ = events.send(Event::RankClosed { aid }).await;
            });
        }
        // A refused attach still delivers its `RankAttached` and then ends.
        Ok(Response::new(Box::pin(
            ReceiverStream::new(rx).map(Ok::<_, Status>),
        )))
    }

    async fn hello(&self, request: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        let shared = &self.handle.shared;
        let info = request.extensions().get::<PeerInfo>().cloned();
        let message = request.into_inner();
        if message.protocol != 2 {
            return Ok(Response::new(HelloReply {
                status: v2::HelloStatus::VersionMismatch as i32,
                protocol: 2,
                ..Default::default()
            }));
        }
        let same_host =
            info.is_some_and(|info| info.unix && info.uid == shared.uid && info.gid == shared.gid);
        let readiness = *shared.readiness.lock().unwrap();
        Ok(Response::new(HelloReply {
            status: v2::HelloStatus::HelloOk as i32,
            protocol: 2,
            manager_incarnation: shared.incarnation,
            encryption: shared.encryption as i32,
            readiness: Some(readiness),
            data_transports: shared.transports.iter().map(|t| *t as i32).collect(),
            limits: Some(super::limits::to_proto(&shared.limits)),
            shared_memory: same_host.then(|| shared.shm.clone()),
            host_id: shared.host_id.to_vec(),
        }))
    }

    async fn session(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let info = request.extensions().get::<PeerInfo>().cloned();
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("the stream ended before `open`"))?;
        let Some(v2::client_message::M::Open(open)) = first.m else {
            return Err(Status::invalid_argument("the first message must be `open`"));
        };
        let (tx, rx) = mpsc::channel(OUTBOUND);
        let (reply, answer) = oneshot::channel();
        let peer = info.map_or(
            OpenPeer {
                unix: false,
                pid: 0,
                uid: 0,
                gid: 0,
            },
            |info| OpenPeer {
                unix: info.unix,
                pid: info.pid,
                uid: info.uid,
                gid: info.gid,
            },
        );
        let events = self.handle.events.clone();
        events
            .send(Event::Open {
                peer,
                open: Box::new(open),
                tx,
                reply,
            })
            .await
            .map_err(|_| Status::unavailable("the pool is stopped"))?;
        let sid = answer
            .await
            .map_err(|_| Status::unavailable("the pool is stopped"))?;
        if let Some(sid) = sid {
            tokio::spawn(async move {
                while let Ok(Some(message)) = inbound.message().await {
                    let event = Event::Message {
                        sid,
                        message: Box::new(message),
                    };
                    if events.send(event).await.is_err() {
                        return;
                    }
                }
                let _ = events.send(Event::Closed { sid }).await;
            });
        }
        // A refused open still delivers its `SessionOpened` and then ends.
        Ok(Response::new(Box::pin(
            ReceiverStream::new(rx).map(Ok::<_, Status>),
        )))
    }
}
