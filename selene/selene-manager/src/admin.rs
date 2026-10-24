//! Admin service: stats, pins, purge and quotas.
//!
//! The service is transport-agnostic. `control.rs` mounts it on the Unix socket or the mTLS
//! admin listener and supplies a backend that sends commands to the store thread.

use crate::Key;
use crate::peer_client::key_from_proto;
use crate::proto;
use std::io;
use std::sync::Arc;
use tonic::Status;

/// The operations the store thread implements for the admin service.
#[tonic::async_trait]
pub trait AdminBackend: Send + Sync {
    async fn stats(&self) -> io::Result<String>;
    async fn pin(&self, namespace: Vec<u8>, keys: Vec<Key>, pin: bool) -> io::Result<u32>;
    async fn purge(&self, namespace: Vec<u8>, all_namespaces: bool) -> io::Result<(u64, u64)>;
    async fn set_quota(&self, namespace: Vec<u8>, dram: u64, nvme: u64) -> io::Result<u64>;
}

pub struct AdminService {
    backend: Arc<dyn AdminBackend>,
}

impl AdminService {
    pub fn new(backend: Arc<dyn AdminBackend>) -> Self {
        Self { backend }
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn status(error: io::Error) -> Status {
    match error.kind() {
        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => {
            Status::invalid_argument(error.to_string())
        }
        _ => Status::internal(error.to_string()),
    }
}

fn namespace(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.len() != 32 {
        return Err(invalid("namespace_id must be 32 bytes"));
    }
    Ok(bytes.to_vec())
}

#[tonic::async_trait]
impl proto::admin_server::Admin for AdminService {
    async fn stats(
        &self,
        _request: tonic::Request<proto::StatsRequest>,
    ) -> Result<tonic::Response<proto::StatsReply>, Status> {
        let prometheus = self.backend.stats().await.map_err(status)?;
        Ok(tonic::Response::new(proto::StatsReply { prometheus }))
    }

    async fn pin(
        &self,
        request: tonic::Request<proto::PinRequest>,
    ) -> Result<tonic::Response<proto::PinReply>, Status> {
        let request = request.into_inner();
        let namespace = namespace(&request.namespace_id).map_err(status)?;
        let mut keys = Vec::with_capacity(request.keys.len());
        for key in request.keys {
            keys.push(key_from_proto(key).map_err(status)?);
        }
        let changed = self
            .backend
            .pin(namespace, keys, request.pin)
            .await
            .map_err(status)?;
        Ok(tonic::Response::new(proto::PinReply { changed }))
    }

    async fn purge(
        &self,
        request: tonic::Request<proto::PurgeRequest>,
    ) -> Result<tonic::Response<proto::PurgeReply>, Status> {
        let request = request.into_inner();
        let namespace = if request.all_namespaces {
            Vec::new()
        } else {
            namespace(&request.namespace_id).map_err(status)?
        };
        let (records, bytes) = self
            .backend
            .purge(namespace, request.all_namespaces)
            .await
            .map_err(status)?;
        Ok(tonic::Response::new(proto::PurgeReply { records, bytes }))
    }

    async fn set_quota(
        &self,
        request: tonic::Request<proto::QuotaRequest>,
    ) -> Result<tonic::Response<proto::QuotaReply>, Status> {
        let request = request.into_inner();
        let namespace = namespace(&request.namespace_id).map_err(status)?;
        let blocked = self
            .backend
            .set_quota(namespace, request.dram_bytes, request.nvme_bytes)
            .await
            .map_err(status)?;
        Ok(tonic::Response::new(proto::QuotaReply {
            blocked_bytes: blocked,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::admin_server::Admin as _;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Calls {
        stats: u32,
        pins: Vec<(Vec<u8>, Vec<Key>, bool)>,
        purges: Vec<(Vec<u8>, bool)>,
        quotas: Vec<(Vec<u8>, u64, u64)>,
    }

    struct Fake {
        calls: Mutex<Calls>,
        fail: bool,
    }

    impl Fake {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Calls::default()),
                fail: false,
            })
        }
    }

    #[tonic::async_trait]
    impl AdminBackend for Fake {
        async fn stats(&self) -> io::Result<String> {
            self.calls.lock().unwrap().stats += 1;
            if self.fail {
                return Err(io::Error::other("backend down"));
            }
            Ok("selene_up 1\n".to_string())
        }

        async fn pin(&self, namespace: Vec<u8>, keys: Vec<Key>, pin: bool) -> io::Result<u32> {
            self.calls
                .lock()
                .unwrap()
                .pins
                .push((namespace, keys.clone(), pin));
            Ok(keys.len() as u32)
        }

        async fn purge(&self, namespace: Vec<u8>, all: bool) -> io::Result<(u64, u64)> {
            self.calls.lock().unwrap().purges.push((namespace, all));
            Ok((3, 9000))
        }

        async fn set_quota(&self, namespace: Vec<u8>, dram: u64, nvme: u64) -> io::Result<u64> {
            self.calls
                .lock()
                .unwrap()
                .quotas
                .push((namespace, dram, nvme));
            Ok(128)
        }
    }

    fn key(value: u8) -> proto::Key {
        proto::Key {
            consumed_tokens: u64::from(value) * 64,
            digest: vec![value; 16],
        }
    }

    #[tokio::test]
    async fn stats_returns_the_backend_body() {
        let fake = Fake::new();
        let service = AdminService::new(fake.clone());
        let reply = service
            .stats(tonic::Request::new(proto::StatsRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.prometheus, "selene_up 1\n");
        assert_eq!(fake.calls.lock().unwrap().stats, 1);
    }

    #[tokio::test]
    async fn pin_passes_keys_and_returns_the_changed_count() {
        let fake = Fake::new();
        let service = AdminService::new(fake.clone());
        let reply = service
            .pin(tonic::Request::new(proto::PinRequest {
                namespace_id: vec![1; 32],
                keys: vec![key(1), key(2)],
                pin: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.changed, 2);
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls.pins.len(), 1);
        assert_eq!(calls.pins[0].1[0].digest, [1; 16]);
        assert!(calls.pins[0].2);
    }

    #[tokio::test]
    async fn malformed_requests_are_invalid_arguments() {
        let fake = Fake::new();
        let service = AdminService::new(fake);
        assert_eq!(
            service
                .pin(tonic::Request::new(proto::PinRequest {
                    namespace_id: vec![1; 3],
                    keys: Vec::new(),
                    pin: true,
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            service
                .pin(tonic::Request::new(proto::PinRequest {
                    namespace_id: vec![1; 32],
                    keys: vec![proto::Key {
                        consumed_tokens: 1,
                        digest: vec![1; 3],
                    }],
                    pin: true,
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            service
                .set_quota(tonic::Request::new(proto::QuotaRequest {
                    namespace_id: vec![1; 8],
                    dram_bytes: 0,
                    nvme_bytes: 0,
                }))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn purge_all_namespaces_needs_no_namespace_id() {
        let fake = Fake::new();
        let service = AdminService::new(fake.clone());
        let reply = service
            .purge(tonic::Request::new(proto::PurgeRequest {
                namespace_id: Vec::new(),
                all_namespaces: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.records, 3);
        assert_eq!(reply.bytes, 9000);
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls.purges, vec![(Vec::new(), true)]);
    }

    #[tokio::test]
    async fn quota_reports_blocked_bytes() {
        let fake = Fake::new();
        let service = AdminService::new(fake.clone());
        let reply = service
            .set_quota(tonic::Request::new(proto::QuotaRequest {
                namespace_id: vec![2; 32],
                dram_bytes: 1 << 20,
                nvme_bytes: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.blocked_bytes, 128);
        assert_eq!(
            fake.calls.lock().unwrap().quotas,
            vec![(vec![2; 32], 1 << 20, 0)]
        );
    }

    #[tokio::test]
    async fn backend_failures_are_internal_errors() {
        let fake = Arc::new(Fake {
            calls: Mutex::new(Calls::default()),
            fail: true,
        });
        let service = AdminService::new(fake);
        assert_eq!(
            service
                .stats(tonic::Request::new(proto::StatsRequest {}))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Internal
        );
    }
}
