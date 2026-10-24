//! PeerCache client. The merged catalog lives in the store.
//!
//! The client owns one TLS/plaintext control channel, one token-authenticated stream data
//! channel, and the merged remote catalog. It is driven by the store's peer thread, so blocking
//! data-channel calls run on a runtime with no other work.

use crate::config::Upstream;
use crate::data::rdma::{self as transport_select, Transport};
use crate::data::stream::{StreamChannel, TOKEN_BYTES};
use crate::data::{DataChannel, GrantKey, Region, RegionAccess};
use crate::placement::{self, Node};
use crate::record;
use crate::{Key, proto};
use std::io;
use std::net::SocketAddr;
use std::os::unix::fs::FileExt as _;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

const PROTOCOL: u32 = 1;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound for a whole peer connection, including `PeerHello`.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const METADATA_CHUNK: usize = 16 << 20;

#[derive(Clone, Debug)]
pub struct TlsSettings {
    pub ca: Vec<u8>,
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    pub domain: String,
}

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

pub fn key_to_proto(key: Key) -> proto::Key {
    proto::Key {
        consumed_tokens: key.consumed_tokens,
        digest: key.digest.to_vec(),
    }
}

pub fn key_from_proto(key: proto::Key) -> io::Result<Key> {
    let digest: [u8; 16] = key
        .digest
        .as_slice()
        .try_into()
        .map_err(|_| invalid("key digest is not 16 bytes"))?;
    Ok(Key {
        consumed_tokens: key.consumed_tokens,
        digest,
    })
}

pub fn descriptor_to_proto(descriptor: &record::Descriptor, tenant: &str) -> proto::Descriptor {
    proto::Descriptor {
        identity: descriptor.identity.to_vec(),
        fingerprint: descriptor.fingerprint.to_vec(),
        page_tokens: descriptor.page_tokens,
        ranks: descriptor
            .ranks
            .iter()
            .map(|rank| proto::RankPlanes {
                plane_bytes: rank.planes.clone(),
                row_bytes: rank.row_bytes,
            })
            .collect(),
        page_record_bytes: descriptor.page_bytes(),
        row_record_bytes: descriptor.row_bytes(),
        tenant: tenant.to_string(),
    }
}

fn region_from_proto(value: i32) -> io::Result<Region> {
    match proto::Region::try_from(value) {
        Ok(proto::Region::Arena) => Ok(Region::Arena),
        Ok(proto::Region::Staging) => Ok(Region::Staging),
        _ => Err(invalid("unknown region")),
    }
}

/// One remote manager.
pub struct PeerClient {
    generation: u64,
    client: proto::peer_cache_client::PeerCacheClient<Channel>,
    data: Box<dyn DataChannel>,
    regions: Arc<dyn RegionAccess>,
    next_transfer: u64,
    peer_sizes: (u64, u64),
    rdma: bool,
    rdma_failed: bool,
}

impl PeerClient {
    /// Connect the control channel, run `PeerHello`, and open the data channel.
    pub async fn connect(
        address: &str,
        node: &str,
        regions: Arc<dyn RegionAccess>,
        connections: usize,
        tls: Option<TlsSettings>,
        transport: &str,
    ) -> io::Result<Self> {
        Self::connect_within(
            address,
            node,
            regions,
            connections,
            tls,
            transport,
            CONNECT_TIMEOUT,
        )
        .await
    }

    /// [`PeerClient::connect`] with an explicit deadline for the whole exchange, so a stalled
    /// upstream cannot hold a startup or a reconnect forever.
    pub async fn connect_within(
        address: &str,
        node: &str,
        regions: Arc<dyn RegionAccess>,
        connections: usize,
        tls: Option<TlsSettings>,
        transport: &str,
        timeout: Duration,
    ) -> io::Result<Self> {
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::time::timeout_at(
            deadline,
            Self::connect_inner(
                address,
                node,
                regions,
                connections,
                tls,
                transport,
                deadline,
            ),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer connect made no progress"))?
    }

    async fn connect_inner(
        address: &str,
        node: &str,
        regions: Arc<dyn RegionAccess>,
        connections: usize,
        tls: Option<TlsSettings>,
        transport: &str,
        deadline: tokio::time::Instant,
    ) -> io::Result<Self> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        let mut endpoint = Endpoint::from_shared(format!("{scheme}://{address}")).map_err(other)?;
        if let Some(tls) = &tls {
            let config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(tls.ca.clone()))
                .identity(Identity::from_pem(tls.cert.clone(), tls.key.clone()))
                .domain_name(tls.domain.clone());
            endpoint = endpoint.tls_config(config).map_err(other)?;
        }
        let channel = endpoint.connect().await.map_err(other)?;
        let mut client = proto::peer_cache_client::PeerCacheClient::new(channel);
        // An explicit `stream` configuration never opens an RDMA device.
        let local_rdma = if transport == "stream" {
            None
        } else {
            local_rdma(&regions)
        };
        #[cfg(feature = "rdma")]
        let (transports, rdma) = match local_rdma.as_ref() {
            Some(local) => (
                vec!["rdma".to_string(), "stream".to_string()],
                local.handshake().encode().to_vec(),
            ),
            None => (vec!["stream".to_string()], Vec::new()),
        };
        #[cfg(not(feature = "rdma"))]
        let (transports, rdma) = (vec!["stream".to_string()], Vec::new());
        let reply = client
            .peer_hello(proto::PeerHelloRequest {
                protocol: PROTOCOL,
                node: node.to_string(),
                transports,
                rdma,
                arena_bytes: regions.size(Region::Arena),
                staging_bytes: regions.size(Region::Staging),
            })
            .await
            .map_err(other)?
            .into_inner();
        let handshake_ok = local_rdma.is_some() && reply.transport == "rdma";
        let selected = transport_select::select(transport, cfg!(feature = "rdma"), handshake_ok)?;
        let token: [u8; TOKEN_BYTES] = reply
            .stream_token
            .as_slice()
            .try_into()
            .map_err(|_| invalid("peer sent a short stream token"))?;
        let data_address: SocketAddr = reply
            .stream_addr
            .parse()
            .map_err(|_| invalid("invalid stream address"))?;
        let peer_sizes = (reply.arena_bytes, reply.staging_bytes);
        let count = connections.max(1);
        let stream = || async {
            StreamChannel::connect_async(
                &vec![data_address; count],
                &token,
                regions.clone(),
                peer_sizes,
                deadline,
            )
            .await
            .map(|channel| Box::new(channel) as Box<dyn DataChannel>)
        };
        let data: Box<dyn DataChannel> = if selected == Transport::Rdma {
            #[cfg(feature = "rdma")]
            {
                let local = local_rdma.expect("RDMA selected without a local handshake");
                match crate::data::rdma::Handshake::decode(&reply.rdma) {
                    Ok(peer) => match local.connect(peer, peer_sizes) {
                        Ok(channel) => channel,
                        Err(error) => {
                            eprintln!(
                                "selene: experimental RDMA channel setup failed ({error}); using stream"
                            );
                            stream().await?
                        }
                    },
                    Err(error) => {
                        eprintln!(
                            "selene: experimental RDMA handshake was invalid ({error}); using stream"
                        );
                        stream().await?
                    }
                }
            }
            #[cfg(not(feature = "rdma"))]
            {
                return Err(invalid("RDMA selected in a build without the rdma feature"));
            }
        } else {
            stream().await?
        };
        let rdma = data.is_rdma();
        Ok(Self {
            generation: reply.generation,
            client,
            data,
            regions,
            rdma,
            rdma_failed: false,
            next_transfer: 1,
            peer_sizes,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn peer_sizes(&self) -> (u64, u64) {
        self.peer_sizes
    }

    /// Whether this session moves record bytes over RDMA.
    pub fn uses_rdma(&self) -> bool {
        self.rdma
    }

    /// Whether an RDMA data-path error occurred. The set owning this client records that and
    /// reconnects the peer with the stream transport.
    pub fn rdma_failed(&self) -> bool {
        self.rdma_failed
    }

    fn note_data_error(&mut self, error: io::Error) -> io::Error {
        if self.rdma {
            self.rdma_failed = true;
        }
        error
    }

    fn request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        let value = MetadataValue::try_from(self.generation.to_string().as_str())
            .expect("a decimal generation is valid metadata");
        request.metadata_mut().insert("x-selene-generation", value);
        request
    }

    pub async fn subscribe(
        &mut self,
        namespace: [u8; 32],
        descriptor: proto::Descriptor,
    ) -> io::Result<tonic::Streaming<proto::CatalogEvent>> {
        let request = self.request(proto::SubscribeRequest {
            namespace_id: namespace.to_vec(),
            generation: self.generation,
            descriptor: Some(descriptor),
        });
        Ok(self
            .client
            .subscribe(request)
            .await
            .map_err(|status| match status.code() {
                // A refused namespace is a permanent per-namespace failure, not a broken
                // connection: the caller retries that subscription alone.
                tonic::Code::InvalidArgument
                | tonic::Code::FailedPrecondition
                | tonic::Code::NotFound
                | tonic::Code::Unimplemented => invalid(status.to_string()),
                _ => other(status),
            })?
            .into_inner())
    }

    async fn wait_data(&mut self, key: GrantKey) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + IDLE_TIMEOUT;
        loop {
            let mut done = Vec::new();
            if let Err(error) = self.data.poll(&mut done) {
                return Err(self.note_data_error(error));
            }
            for (transfer, index, result) in done {
                if transfer == key.transfer && index == key.index {
                    return result.map_err(|error| self.note_data_error(error));
                }
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = self.data.close();
                let error =
                    io::Error::new(io::ErrorKind::TimedOut, "data transfer made no progress");
                return Err(self.note_data_error(error));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Forward one stored record to this peer, keeping its exact parent and tail.
    pub async fn forward(&mut self, mut record: ForwardRecord) -> io::Result<Forwarded> {
        let header = record::Header::decode(&record.header)?;
        let tail_pages = u32::try_from(record.published_tail_pages)
            .map_err(|_| invalid("published tail does not fit u32"))?;
        if header.key != record.key
            || header.parent != record.published_parent
            || header.tail_pages != tail_pages
        {
            return Err(invalid(
                "forwarded record does not match its requested shape",
            ));
        }
        let page_segment = usize::try_from(header.page_segment)
            .map_err(|_| invalid("page segment does not fit usize"))?;
        if record.header.len() != record::HEADER_BYTES
            || record.checksum_segment.len() + record::HEADER_BYTES != page_segment
        {
            return Err(invalid("forwarded metadata has the wrong length"));
        }
        let record_len = header.length(record.row);
        let (sender, receiver) = mpsc::channel(8);
        let request = self.request(ReceiverStream::new(receiver));
        let mut responses = self.client.put(request).await.map_err(other)?.into_inner();
        let begin = proto::PutMessage {
            m: Some(proto::put_message::M::Begin(proto::PutBegin {
                namespace_id: record.namespace.to_vec(),
                key: Some(key_to_proto(record.key)),
                chain_pages: record.chain_pages,
                row: record.row,
                published_parent: record.published_parent.map(key_to_proto),
                has_published_parent: record.published_parent.is_some(),
                bucket: record.bucket.to_vec(),
                published_tail_pages: record.published_tail_pages,
                descriptor: Some(record.descriptor.clone()),
            })),
        };
        sender.send(begin).await.map_err(other)?;
        let mut transfer = 0u64;
        let mut first_page = 0u64;
        let mut pages = 0u32;
        let mut row = false;
        let mut committed = false;
        loop {
            let Some(reply) = tokio::time::timeout(IDLE_TIMEOUT, responses.message())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer Put made no progress"))?
                .map_err(other)?
            else {
                return Err(invalid("peer Put ended before storing the record"));
            };
            match reply.r {
                Some(proto::put_reply::R::Accept(accept)) => {
                    if accept.has_parent != record.published_parent.is_some()
                        || (accept.has_parent
                            && key_from_proto(
                                accept
                                    .parent
                                    .clone()
                                    .ok_or_else(|| invalid("missing parent"))?,
                            )? != record.published_parent.unwrap())
                        || Some(accept.first_page)
                            != record.chain_pages.checked_sub(record.published_tail_pages)
                    {
                        return Err(invalid("peer selected a different record shape"));
                    }
                    transfer = accept.transfer;
                    first_page = accept.first_page;
                }
                Some(proto::put_reply::R::Grant(grant)) => {
                    if grant.transfer != transfer || grant.record != 0 {
                        return Err(invalid("peer grant names a different transfer"));
                    }
                    forward_grant(&header, record.row, &grant, record_len)?;
                    if grant.page_first < first_page {
                        return Err(invalid("peer granted a page before the record"));
                    }
                    let memory = grant
                        .memory
                        .as_ref()
                        .ok_or_else(|| invalid("peer grant has no memory window"))?;
                    let memory_region = region_from_proto(memory.region)?;
                    let (source_region, source) = record.source.window(&grant)?;
                    let written = self.data.write(
                        transfer,
                        grant.index,
                        source_region,
                        source,
                        grant.length,
                        memory_region,
                        memory.offset,
                    );
                    if let Err(error) = written {
                        return Err(self.note_data_error(error));
                    }
                    self.wait_data(GrantKey {
                        transfer,
                        index: grant.index,
                    })
                    .await?;
                    let mut hashes = Vec::with_capacity(grant.page_count as usize);
                    for page in 0..grant.page_count {
                        let index = table_index(
                            record.chain_pages,
                            u64::from(header.tail_pages),
                            grant.page_first,
                            u64::from(page),
                        )?;
                        let start = index
                            .checked_mul(8)
                            .ok_or_else(|| invalid("checksum index overflow"))?;
                        let end = start + 8;
                        hashes.push(u64::from_le_bytes(
                            record.checksum_segment[start..end]
                                .try_into()
                                .expect("8-byte checksum"),
                        ));
                    }
                    pages += grant.page_count;
                    if grant.row {
                        row = true;
                    }
                    // The window is reported filled only now: `wait_data` returned after every
                    // piece had a completion, so on RDMA the receiver's memory already holds the
                    // bytes. This message is also the receiver's acknowledgment: the store
                    // validates it and the grant is withdrawn only after that acceptance.
                    let filled = proto::PutMessage {
                        m: Some(proto::put_message::M::Filled(proto::WindowFilled {
                            transfer,
                            index: grant.index,
                            page_xxh3: hashes,
                            row_xxh3: if grant.row { header.row_checksum } else { 0 },
                            has_row: grant.row,
                        })),
                    };
                    sender.send(filled).await.map_err(other)?;
                    if !committed && pages >= header.tail_pages && (!record.row || row) {
                        let mut metadata = record.header.clone();
                        metadata.extend_from_slice(&record.checksum_segment);
                        let mut offset = 0usize;
                        while offset < metadata.len() {
                            let end = (offset + METADATA_CHUNK).min(metadata.len());
                            let commit = proto::PutMessage {
                                m: Some(proto::put_message::M::Commit(proto::PutCommit {
                                    transfer,
                                    metadata_offset: offset as u64,
                                    metadata: metadata[offset..end].to_vec(),
                                })),
                            };
                            sender.send(commit).await.map_err(other)?;
                            offset = end;
                        }
                        committed = true;
                    }
                }
                Some(proto::put_reply::R::Stored(stored)) => {
                    return Ok(Forwarded {
                        bytes: stored.bytes,
                    });
                }
                Some(proto::put_reply::R::Refused(refused)) => {
                    if refused.reason == proto::Reason::Present as i32 {
                        return Ok(Forwarded { bytes: 0 });
                    }
                    if refused.reason == proto::Reason::Invalid as i32
                        && refused.detail.contains("published parent")
                    {
                        // The caller repairs the ancestor instead of retrying the child alone.
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "peer is missing the published parent",
                        ));
                    }
                    return Err(invalid(format!(
                        "peer refused the record: {:?} {}",
                        refused.reason, refused.detail
                    )));
                }
                None => return Err(invalid("peer Put reply has no body")),
            }
        }
    }

    /// Fetch a chain from this peer into the caller's reserved destinations.
    pub async fn fetch(&mut self, request: FetchRequest) -> io::Result<Fetched> {
        if request.destinations.len() != request.chain.len() {
            return Err(invalid("one destination is required per chain record"));
        }
        descriptor_from_proto(&request.descriptor)?;
        let transfer = self.next_transfer;
        self.next_transfer = self
            .next_transfer
            .checked_add(1)
            .ok_or_else(|| invalid("transfer ids exhausted"))?;
        let (sender, receiver) = mpsc::channel(8);
        let request_message = self.request(ReceiverStream::new(receiver));
        let mut responses = self
            .client
            .get(request_message)
            .await
            .map_err(other)?
            .into_inner();
        let begin = proto::GetMessage {
            m: Some(proto::get_message::M::Begin(proto::GetBegin {
                namespace_id: request.namespace.to_vec(),
                chain: request.chain.iter().map(|key| key_to_proto(*key)).collect(),
                skip_pages: request.skip_pages,
                expected_pages: request.expected_pages,
                row: request.row,
                destinations: request.destinations.clone(),
                transfer,
            })),
        };
        sender.send(begin).await.map_err(other)?;
        let mut record_bytes = Vec::new();
        let mut metadata: Vec<MetaBuffer> = (0..request.chain.len())
            .map(|_| MetaBuffer::new())
            .collect();
        let mut coverage = FetchCoverage::new();
        loop {
            let Some(reply) = tokio::time::timeout(IDLE_TIMEOUT, responses.message())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "peer Get made no progress"))?
                .map_err(other)?
            else {
                return Err(invalid("peer Get ended before completing"));
            };
            match reply.r {
                Some(proto::get_reply::R::Leased(leased)) => {
                    if leased.transfer != transfer {
                        return Err(invalid("peer leased a different transfer"));
                    }
                    record_bytes = leased.record_bytes;
                    if record_bytes.len() != request.chain.len() {
                        return Err(invalid("peer leased the wrong record count"));
                    }
                }
                Some(proto::get_reply::R::Meta(meta)) => {
                    let index = meta.record as usize;
                    let buffer = metadata
                        .get_mut(index)
                        .ok_or_else(|| invalid("metadata for an unknown record"))?;
                    buffer.push(meta.metadata_offset, &meta.metadata)?;
                    if !buffer.checked {
                        if let Some(header) = buffer.header.clone() {
                            validate_meta(index, &request, &header)?;
                            buffer.checked = true;
                        }
                    }
                }
                Some(proto::get_reply::R::Ready(grant)) => {
                    if grant.transfer != transfer {
                        return Err(invalid("peer granted a different transfer"));
                    }
                    let index = grant.record as usize;
                    let destination = request
                        .destinations
                        .get(index)
                        .ok_or_else(|| invalid("grant for an unknown record"))?;
                    let header = metadata
                        .get(index)
                        .and_then(|buffer| buffer.header.clone())
                        .ok_or_else(|| invalid("grant before its metadata"))?;
                    validate_grant_shape(
                        &header,
                        &grant,
                        header.length_with_row(),
                        Some(destination.bytes),
                    )?;
                    let dest_region = region_from_proto(destination.region)?;
                    let memory = grant
                        .memory
                        .as_ref()
                        .ok_or_else(|| invalid("peer grant has no memory window"))?;
                    let source_region = region_from_proto(memory.region)?;
                    let dest_offset = destination
                        .offset
                        .checked_add(grant.record_offset)
                        .ok_or_else(|| invalid("destination offset overflow"))?;
                    let read = self.data.read(
                        transfer,
                        grant.index,
                        dest_region,
                        dest_offset,
                        grant.length,
                        source_region,
                        memory.offset,
                    );
                    if let Err(error) = read {
                        return Err(self.note_data_error(error));
                    }
                    self.wait_data(GrantKey {
                        transfer,
                        index: grant.index,
                    })
                    .await?;
                    self.validate_window(&metadata[index], destination, &grant)?;
                    let released = proto::GetMessage {
                        m: Some(proto::get_message::M::Released(proto::WindowReleased {
                            transfer,
                            index: grant.index,
                        })),
                    };
                    sender.send(released).await.map_err(other)?;
                    coverage.grant(index, &header, &grant, request.row, request.chain.len())?;
                }
                Some(proto::get_reply::R::Done(_)) => {
                    let headers: Vec<record::Header> = metadata
                        .iter()
                        .map(|buffer| {
                            buffer
                                .header
                                .clone()
                                .ok_or_else(|| invalid("record metadata is missing"))
                        })
                        .collect::<io::Result<_>>()?;
                    coverage.verify(
                        &headers,
                        request.skip_pages,
                        request.expected_pages,
                        request.row,
                    )?;
                    let mut total = 0u64;
                    for (index, buffer) in metadata.iter_mut().enumerate() {
                        let header = buffer
                            .header
                            .clone()
                            .ok_or_else(|| invalid("record metadata is missing"))?;
                        if !buffer.complete() {
                            return Err(invalid("record metadata is incomplete"));
                        }
                        let destination = &request.destinations[index];
                        if header.page_segment > destination.bytes {
                            return Err(invalid("destination is smaller than its metadata"));
                        }
                        let expected = header.length(index + 1 == request.chain.len());
                        if record_bytes
                            .get(index)
                            .is_some_and(|bytes| *bytes != expected)
                        {
                            return Err(invalid("peer leased the wrong record length"));
                        }
                        let region = region_from_proto(destination.region)?;
                        let bytes = &buffer.bytes;
                        self.regions.with_write(
                            region,
                            destination.offset,
                            bytes.len(),
                            &mut |target| {
                                target.copy_from_slice(bytes);
                                Ok(())
                            },
                        )?;
                        total += expected;
                    }
                    let metadata = metadata.into_iter().map(|buffer| buffer.bytes).collect();
                    return Ok(Fetched {
                        record_bytes,
                        metadata,
                        bytes: total,
                    });
                }
                Some(proto::get_reply::R::Refused(refused)) => {
                    return Err(invalid(format!(
                        "peer refused the fetch: {:?} {}",
                        refused.reason, refused.detail
                    )));
                }
                None => return Err(invalid("peer Get reply has no body")),
            }
        }
    }

    fn validate_window(
        &self,
        meta: &MetaBuffer,
        destination: &proto::GetDestination,
        grant: &proto::WindowGrant,
    ) -> io::Result<()> {
        let header = meta
            .header
            .as_ref()
            .ok_or_else(|| invalid("record metadata is missing"))?;
        let region = region_from_proto(destination.region)?;
        if grant.page_count > 0 {
            let page_bytes = header.page_record_bytes() as usize;
            let chain = header.key.consumed_tokens / header.page_tokens;
            for page in 0..grant.page_count {
                let index = table_index(
                    chain,
                    u64::from(header.tail_pages),
                    grant.page_first,
                    u64::from(page),
                )?;
                let start = index
                    .checked_mul(8)
                    .and_then(|start| start.checked_add(record::HEADER_BYTES))
                    .ok_or_else(|| invalid("checksum index overflow"))?;
                let end = start + 8;
                let expected = u64::from_le_bytes(
                    meta.bytes
                        .get(start..end)
                        .ok_or_else(|| invalid("missing checksum entry"))?
                        .try_into()
                        .expect("8-byte checksum"),
                );
                let offset = destination
                    .offset
                    .checked_add(grant.record_offset)
                    .and_then(|base| {
                        u64::from(page)
                            .checked_mul(page_bytes as u64)
                            .and_then(|delta| base.checked_add(delta))
                    })
                    .ok_or_else(|| invalid("page offset overflow"))?;
                let actual = self.hash(region, offset, page_bytes)?;
                if actual != expected {
                    return Err(invalid("page checksum mismatch"));
                }
            }
        }
        if grant.row {
            let offset = destination
                .offset
                .checked_add(header.row_segment)
                .ok_or_else(|| invalid("row offset overflow"))?;
            let actual = self.hash(region, offset, header.row_record_bytes() as usize)?;
            if actual != header.row_checksum {
                return Err(invalid("row checksum mismatch"));
            }
        }
        Ok(())
    }

    fn hash(&self, region: Region, offset: u64, length: usize) -> io::Result<u64> {
        let mut hash = 0u64;
        self.regions
            .with_read(region, offset, length, &mut |bytes| {
                hash = twox_hash::XxHash3_64::oneshot(bytes);
                Ok(())
            })?;
        Ok(hash)
    }
}

/// All configured upstreams, their connected clients, and placement.
pub struct PeerSet {
    upstreams: Vec<Upstream>,
    clients: std::collections::BTreeMap<String, PeerClient>,
    failed: Vec<String>,
    regions: Arc<dyn RegionAccess>,
    connections: usize,
    tls: Option<TlsSettings>,
    transport: String,
    /// Peers whose current connection failed on the RDMA data path. They reconnect as stream.
    stream_only: std::collections::BTreeSet<String>,
    retry_at: std::collections::BTreeMap<String, std::time::Instant>,
    attempts: std::collections::BTreeMap<String, u32>,
}

/// TLS settings for one upstream: the certificate domain must match the host it dials.
fn tls_for(tls: &Option<TlsSettings>, address: &str) -> Option<TlsSettings> {
    tls.clone().map(|mut settings| {
        settings.domain = address
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']).to_string())
            .unwrap_or_else(|| address.to_string());
        settings
    })
}

impl PeerSet {
    /// Connect every upstream. At least one must succeed; unreachable upstreams are recorded
    /// and excluded from placement until the manager restarts.
    pub async fn connect(
        upstreams: &[Upstream],
        regions: Arc<dyn RegionAccess>,
        connections: usize,
        tls: Option<TlsSettings>,
        transport: &str,
    ) -> io::Result<Self> {
        Self::connect_within(
            upstreams,
            regions,
            connections,
            tls,
            transport,
            CONNECT_TIMEOUT,
        )
        .await
    }

    /// [`PeerSet::connect`] with an explicit deadline per upstream. Upstreams are connected
    /// with bounded concurrency, so one that never answers delays only itself: the ones that
    /// connected are kept and the rest are recorded as failed.
    pub async fn connect_within(
        upstreams: &[Upstream],
        regions: Arc<dyn RegionAccess>,
        connections: usize,
        tls: Option<TlsSettings>,
        transport: &str,
        per_peer: Duration,
    ) -> io::Result<Self> {
        const CONNECT_BATCH: usize = 8;
        let mut tasks = tokio::task::JoinSet::new();
        let mut results = Vec::with_capacity(upstreams.len());
        let mut next = 0;
        while next < upstreams.len() || !tasks.is_empty() {
            while next < upstreams.len() && tasks.len() < CONNECT_BATCH {
                let upstream = upstreams[next].clone();
                next += 1;
                let regions = regions.clone();
                let tls = tls_for(&tls, &upstream.address);
                let transport = transport.to_string();
                tasks.spawn(async move {
                    let result = PeerClient::connect_within(
                        &upstream.address,
                        &upstream.name,
                        regions,
                        connections,
                        tls,
                        &transport,
                        per_peer,
                    )
                    .await;
                    (upstream.name, result)
                });
            }
            let Some(joined) = tasks.join_next().await else {
                break;
            };
            match joined {
                Ok((name, result)) => results.push((name, result)),
                // A panicked connect task is a failed upstream, not a failed set.
                Err(_) => results.push((String::new(), Err(invalid("peer connect task failed")))),
            }
        }
        let mut clients = std::collections::BTreeMap::new();
        let mut failed = Vec::new();
        for (name, result) in results {
            match result {
                Ok(client) => {
                    clients.insert(name, client);
                }
                Err(_) => failed.push(name),
            }
        }
        if clients.is_empty() {
            return Err(invalid("no upstream peers connected"));
        }
        let now = std::time::Instant::now();
        let retry_at = failed
            .iter()
            .map(|name| (name.clone(), now + std::time::Duration::from_millis(100)))
            .collect();
        Ok(Self {
            upstreams: upstreams.to_vec(),
            clients,
            failed,
            regions,
            connections,
            tls,
            transport: transport.to_string(),
            stream_only: std::collections::BTreeSet::new(),
            retry_at,
            attempts: std::collections::BTreeMap::new(),
        })
    }

    /// The upstream that owns a chain, by rendezvous over the connected nodes.
    pub fn owner(&self, namespace: &[u8; 32], root: &Key) -> Option<&str> {
        let root = placement::root_key_bytes(root.consumed_tokens, &root.digest);
        let nodes: Vec<Node<'_>> = self
            .upstreams
            .iter()
            .map(|upstream| Node {
                name: &upstream.name,
                weight: upstream.weight,
            })
            .collect();
        placement::choose(namespace, &root, &nodes)
    }

    pub fn client(&self, name: &str) -> Option<&PeerClient> {
        self.clients.get(name)
    }

    pub fn client_mut(&mut self, name: &str) -> Option<&mut PeerClient> {
        self.clients.get_mut(name)
    }

    pub fn failed(&self) -> &[String] {
        &self.failed
    }

    pub fn connected(&self) -> impl Iterator<Item = &str> {
        self.clients.keys().map(String::as_str)
    }

    /// Remove a peer after a failed call. It is excluded from placement and retried later. A peer
    /// that failed on the RDMA data path reconnects as stream and stays stream-only until restart.
    /// Returns whether a connected client was removed.
    pub fn drop_client(&mut self, name: &str) -> bool {
        let Some(client) = self.clients.remove(name) else {
            return false;
        };
        if client.rdma_failed() {
            self.stream_only.insert(name.to_string());
        }
        if !self.failed.iter().any(|failed| failed == name) {
            self.failed.push(name.to_string());
            self.retry_at.insert(
                name.to_string(),
                std::time::Instant::now() + std::time::Duration::from_millis(100),
            );
        }
        true
    }

    /// Reconnect failed upstreams whose backoff has elapsed. Returns the names reconnected.
    pub async fn reconnect(&mut self, now: std::time::Instant) -> Vec<String> {
        let due: Vec<String> = self
            .failed
            .iter()
            .filter(|name| self.retry_at.get(*name).is_none_or(|at| *at <= now))
            .cloned()
            .collect();
        let mut reconnected = Vec::new();
        for name in due {
            let Some(upstream) = self
                .upstreams
                .iter()
                .find(|upstream| upstream.name == name)
                .cloned()
            else {
                continue;
            };
            let transport = if self.stream_only.contains(&name) {
                "stream".to_string()
            } else {
                self.transport.clone()
            };
            match PeerClient::connect(
                &upstream.address,
                &upstream.name,
                self.regions.clone(),
                self.connections,
                tls_for(&self.tls, &upstream.address),
                &transport,
            )
            .await
            {
                Ok(client) => {
                    self.failed.retain(|failed| failed != &name);
                    self.retry_at.remove(&name);
                    self.attempts.remove(&name);
                    self.clients.insert(name.clone(), client);
                    reconnected.push(name);
                }
                Err(_) => {
                    let attempts = self.attempts.entry(name.clone()).or_insert(0);
                    *attempts = attempts.saturating_add(1);
                    let delay = backoff(*attempts);
                    self.retry_at.insert(name.clone(), now + delay);
                }
            }
        }
        reconnected
    }

    /// Subscribe one namespace on every connected upstream.
    pub async fn subscribe(
        &mut self,
        namespace: [u8; 32],
        descriptor: proto::Descriptor,
    ) -> Vec<(String, tonic::Streaming<proto::CatalogEvent>)> {
        let names: Vec<String> = self.clients.keys().cloned().collect();
        let mut streams = Vec::new();
        for name in names {
            if let Ok(stream) = self
                .subscribe_client(&name, namespace, descriptor.clone())
                .await
            {
                streams.push((name, stream));
            }
        }
        streams
    }

    /// Subscribe one namespace on one upstream.
    pub async fn subscribe_client(
        &mut self,
        name: &str,
        namespace: [u8; 32],
        descriptor: proto::Descriptor,
    ) -> io::Result<tonic::Streaming<proto::CatalogEvent>> {
        let client = self
            .clients
            .get_mut(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "upstream is not connected"))?;
        client.subscribe(namespace, descriptor).await
    }
}

/// Peer reconnect backoff: 0.1 s doubling to a 5 s cap.
pub fn backoff(attempts: u32) -> std::time::Duration {
    let millis = 100u64.saturating_mul(1u64 << attempts.min(6));
    std::time::Duration::from_millis(millis).min(std::time::Duration::from_secs(5))
}

/// Forwarding grants must stay within the bytes the record actually holds.
fn forward_grant(
    header: &record::Header,
    row: bool,
    grant: &proto::WindowGrant,
    reservation: u64,
) -> io::Result<()> {
    if grant.row && !row {
        return Err(invalid("peer granted a row the record does not carry"));
    }
    validate_grant_shape(header, grant, reservation, None)
}

/// Distinct pages and the endpoint row actually returned by a fetch.
struct FetchCoverage {
    pages: std::collections::BTreeSet<(usize, u64)>,
    row: Option<usize>,
}

impl FetchCoverage {
    fn new() -> Self {
        Self {
            pages: std::collections::BTreeSet::new(),
            row: None,
        }
    }

    fn grant(
        &mut self,
        index: usize,
        header: &record::Header,
        grant: &proto::WindowGrant,
        requests_row: bool,
        records: usize,
    ) -> io::Result<()> {
        if grant.row {
            if !requests_row || index + 1 != records {
                return Err(invalid("peer granted a row outside the requested endpoint"));
            }
            self.row = Some(index);
        }
        if grant.page_count > 0 {
            let tail = u64::from(header.tail_pages);
            let chain = header.key.consumed_tokens / header.page_tokens;
            for page in 0..grant.page_count {
                let local = table_index(chain, tail, grant.page_first, u64::from(page))?;
                self.pages.insert((index, local as u64));
            }
        }
        Ok(())
    }

    fn verify(
        &self,
        headers: &[record::Header],
        skip_pages: u64,
        expected_pages: u64,
        row: bool,
    ) -> io::Result<()> {
        let mut wanted = std::collections::BTreeSet::new();
        for (index, header) in headers.iter().enumerate() {
            let tail = u64::from(header.tail_pages);
            let start = if index == 0 { skip_pages } else { 0 };
            if start > tail {
                return Err(invalid("skip_pages exceeds the first record"));
            }
            for page in start..tail {
                wanted.insert((index, page));
            }
        }
        if wanted.len() as u64 != expected_pages {
            return Err(invalid("expected_pages does not match the chain"));
        }
        if self.pages != wanted {
            return Err(invalid("fetch did not cover the requested pages"));
        }
        if row && self.row.is_none() {
            return Err(invalid("fetch returned no endpoint row"));
        }
        Ok(())
    }
}

/// Try to open the local RDMA device; failures fall back to stream with one log line.
#[cfg(feature = "rdma")]
fn local_rdma(regions: &Arc<dyn RegionAccess>) -> Option<crate::data::ibv::LocalRdma> {
    if regions.mapping(Region::Arena).is_none() || regions.mapping(Region::Staging).is_none() {
        eprintln!("selene: experimental RDMA regions are not mapped; using stream");
        return None;
    }
    match crate::data::ibv::LocalRdma::open(regions.clone()) {
        Ok(local) => Some(local),
        Err(error) => {
            eprintln!("selene: experimental RDMA setup failed ({error}); using stream");
            None
        }
    }
}

#[cfg(not(feature = "rdma"))]
fn local_rdma(_regions: &Arc<dyn RegionAccess>) -> Option<std::convert::Infallible> {
    None
}

const MAX_METADATA: u64 = 16 << 20;

/// Assembles one record's header and checksum segment from contiguous fragments.
struct MetaBuffer {
    bytes: Vec<u8>,
    header: Option<record::Header>,
    checked: bool,
}

impl MetaBuffer {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            header: None,
            checked: false,
        }
    }

    fn complete(&self) -> bool {
        self.header
            .as_ref()
            .is_some_and(|header| self.bytes.len() as u64 == header.page_segment)
    }

    fn push(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if offset != self.bytes.len() as u64 {
            return Err(invalid("metadata fragments must be contiguous"));
        }
        let limit = self
            .header
            .as_ref()
            .map_or(MAX_METADATA, |header| header.page_segment);
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| invalid("metadata length overflow"))?;
        if end > limit {
            return Err(invalid("metadata fragment exceeds the record"));
        }
        self.bytes.extend_from_slice(data);
        if self.header.is_none() && self.bytes.len() >= record::HEADER_BYTES {
            let header = record::Header::decode(&self.bytes[..record::HEADER_BYTES])?;
            if self.bytes.len() as u64 > header.page_segment {
                return Err(invalid("metadata exceeds the record"));
            }
            self.header = Some(header);
        }
        Ok(())
    }
}

/// Checksum-table index for a page. The table covers the record's tail, not the whole chain.
fn table_index(chain_pages: u64, tail_pages: u64, page_first: u64, page: u64) -> io::Result<usize> {
    if page >= tail_pages {
        return Err(invalid("page outside the record tail"));
    }
    let first = chain_pages
        .checked_sub(tail_pages)
        .ok_or_else(|| invalid("record tail exceeds the chain"))?;
    let relative = page_first
        .checked_sub(first)
        .ok_or_else(|| invalid("page before the record"))?;
    let index = relative
        .checked_add(page)
        .ok_or_else(|| invalid("page index overflow"))?;
    if index >= tail_pages {
        return Err(invalid("page outside the record tail"));
    }
    usize::try_from(index).map_err(|_| invalid("page index"))
}

/// Whole-unit grant shape and reservation bounds.
fn validate_grant_shape(
    header: &record::Header,
    grant: &proto::WindowGrant,
    reservation: u64,
    destination_bytes: Option<u64>,
) -> io::Result<()> {
    let memory = grant
        .memory
        .as_ref()
        .ok_or_else(|| invalid("grant has no memory window"))?;
    if memory.length != grant.length {
        return Err(invalid("each grant needs one memory window length"));
    }
    if grant.length == 0
        || !grant.length.is_multiple_of(4096)
        || !grant.record_offset.is_multiple_of(4096)
        || !memory.offset.is_multiple_of(4096)
    {
        return Err(invalid("grant is not 4096-aligned"));
    }
    if grant.page_count > 64 {
        return Err(invalid("grant exceeds 64 pages"));
    }
    let end = grant
        .record_offset
        .checked_add(grant.length)
        .ok_or_else(|| invalid("grant overflow"))?;
    if end > reservation {
        return Err(invalid("grant exceeds the record reservation"));
    }
    if let Some(bytes) = destination_bytes
        && end > bytes
    {
        return Err(invalid("grant exceeds the destination reservation"));
    }
    let page_bytes = header.page_record_bytes();
    let row_bytes = header.row_record_bytes();
    let chain = header.key.consumed_tokens / header.page_tokens;
    let tail = u64::from(header.tail_pages);
    let first = chain
        .checked_sub(tail)
        .ok_or_else(|| invalid("record tail exceeds the chain"))?;
    if grant.page_count == 0 {
        if !grant.row {
            return Err(invalid("empty grant"));
        }
        if grant.record_offset != header.row_segment || grant.length != row_bytes {
            return Err(invalid("row grant has the wrong shape"));
        }
        return Ok(());
    }
    let relative = grant
        .page_first
        .checked_sub(first)
        .ok_or_else(|| invalid("page before the record"))?;
    if relative
        .checked_add(u64::from(grant.page_count))
        .is_none_or(|end| end > tail)
    {
        return Err(invalid("pages outside the record"));
    }
    let pages_len = u64::from(grant.page_count)
        .checked_mul(page_bytes)
        .ok_or_else(|| invalid("page length overflow"))?;
    let expected_offset = header
        .page_segment
        .checked_add(
            relative
                .checked_mul(page_bytes)
                .ok_or_else(|| invalid("page offset overflow"))?,
        )
        .ok_or_else(|| invalid("page offset overflow"))?;
    if grant.record_offset != expected_offset {
        return Err(invalid("grant does not start at its first page"));
    }
    if grant.row {
        if grant.length != pages_len + row_bytes
            || grant.record_offset.checked_add(pages_len) != Some(header.row_segment)
        {
            return Err(invalid("combined grant has the wrong shape"));
        }
    } else if grant.length != pages_len {
        return Err(invalid("page grant has the wrong length"));
    }
    Ok(())
}

fn descriptor_from_proto(descriptor: &proto::Descriptor) -> io::Result<record::Descriptor> {
    let identity: [u8; 32] = descriptor
        .identity
        .as_slice()
        .try_into()
        .map_err(|_| invalid("descriptor identity is not 32 bytes"))?;
    let fingerprint: [u8; 16] = descriptor
        .fingerprint
        .as_slice()
        .try_into()
        .map_err(|_| invalid("descriptor fingerprint is not 16 bytes"))?;
    let ranks = descriptor
        .ranks
        .iter()
        .map(|rank| record::RankSizes {
            planes: rank.plane_bytes.clone(),
            row_bytes: rank.row_bytes,
        })
        .collect();
    let value = record::Descriptor {
        identity,
        fingerprint,
        page_tokens: descriptor.page_tokens,
        ranks,
        tenant: descriptor.tenant.clone(),
    };
    value.validate()?;
    if value.page_bytes() != descriptor.page_record_bytes
        || value.row_bytes() != descriptor.row_record_bytes
    {
        return Err(invalid("descriptor sizes disagree with its ranks"));
    }
    Ok(value)
}

fn validate_meta(index: usize, request: &FetchRequest, header: &record::Header) -> io::Result<()> {
    let expected = descriptor_from_proto(&request.descriptor)?;
    if header.key != request.chain[index] {
        return Err(invalid("record key does not match the chain"));
    }
    if index > 0 && header.parent != Some(request.chain[index - 1]) {
        return Err(invalid("record ancestry does not match the chain"));
    }
    if header.identity != expected.identity
        || header.fingerprint != expected.fingerprint
        || !header.matches(&expected)
    {
        return Err(invalid("record geometry does not match the descriptor"));
    }
    Ok(())
}

/// Where a forward's record bytes come from. A disk-backed record is paged into a bounded
/// staging window one grant at a time, so a record larger than the staging region still
/// forwards.
pub enum ForwardBytes {
    Shared {
        region: Region,
        offset: u64,
    },
    Disk {
        file: Arc<std::fs::File>,
        /// The record's start within the file, past the manager's envelope.
        offset: u64,
        staging: crate::arena::Extent,
    },
}

impl ForwardBytes {
    /// Point one grant at its bytes, paging a disk record into the staging window first.
    fn window(&mut self, grant: &proto::WindowGrant) -> io::Result<(Region, u64)> {
        match self {
            Self::Shared { region, offset } => Ok((
                *region,
                offset
                    .checked_add(grant.record_offset)
                    .ok_or_else(|| invalid("grant offset overflow"))?,
            )),
            Self::Disk {
                file,
                offset,
                staging,
            } => {
                let length = usize::try_from(grant.length)
                    .map_err(|_| invalid("grant length does not fit"))?;
                let base = staging.offset() as u64;
                let bytes = staging.bytes_mut();
                if length > bytes.len() {
                    return Err(invalid(
                        "peer granted a window larger than the forward staging buffer",
                    ));
                }
                let at = offset
                    .checked_add(grant.record_offset)
                    .ok_or_else(|| invalid("grant offset overflow"))?;
                file.read_exact_at(&mut bytes[..length], at)?;
                Ok((Region::Staging, base))
            }
        }
    }
}

/// One record the caller wants forwarded.
pub struct ForwardRecord {
    pub namespace: [u8; 32],
    pub key: Key,
    pub chain_pages: u64,
    pub published_parent: Option<Key>,
    pub bucket: [u8; 16],
    pub published_tail_pages: u64,
    pub row: bool,
    pub descriptor: proto::Descriptor,
    /// Where the record's bytes are: a local region or a disk file paged into staging.
    pub source: ForwardBytes,
    pub header: Vec<u8>,
    pub checksum_segment: Vec<u8>,
}

pub struct Forwarded {
    pub bytes: u64,
}

pub struct FetchRequest {
    pub descriptor: proto::Descriptor,
    pub namespace: [u8; 32],
    pub chain: Vec<Key>,
    pub skip_pages: u64,
    pub expected_pages: u64,
    pub row: bool,
    pub destinations: Vec<proto::GetDestination>,
}

pub struct Fetched {
    pub record_bytes: Vec<u64>,
    pub metadata: Vec<Vec<u8>>,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::GrantTable;
    use crate::data::stream::StreamReceiver;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    struct TestRegions {
        arena: Mutex<Vec<u8>>,
        staging: Mutex<Vec<u8>>,
    }

    impl TestRegions {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                arena: Mutex::new(vec![0u8; 8 << 20]),
                staging: Mutex::new(vec![0u8; 4 << 20]),
            })
        }
    }

    impl RegionAccess for TestRegions {
        fn size(&self, region: Region) -> u64 {
            match region {
                Region::Arena => self.arena.lock().unwrap().len() as u64,
                Region::Staging => self.staging.lock().unwrap().len() as u64,
            }
        }

        fn with_read(
            &self,
            region: Region,
            offset: u64,
            length: usize,
            f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            let target = match region {
                Region::Arena => &self.arena,
                Region::Staging => &self.staging,
            };
            let start = usize::try_from(offset).map_err(|_| invalid("offset"))?;
            let end = start.checked_add(length).ok_or_else(|| invalid("range"))?;
            let guard = target.lock().unwrap();
            if end > guard.len() {
                return Err(invalid("read outside the region"));
            }
            f(&guard[start..end])
        }

        fn with_write(
            &self,
            region: Region,
            offset: u64,
            length: usize,
            f: &mut dyn FnMut(&mut [u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            let target = match region {
                Region::Arena => &self.arena,
                Region::Staging => &self.staging,
            };
            let start = usize::try_from(offset).map_err(|_| invalid("offset"))?;
            let end = start.checked_add(length).ok_or_else(|| invalid("range"))?;
            let mut guard = target.lock().unwrap();
            if end > guard.len() {
                return Err(invalid("write outside the region"));
            }
            f(&mut guard[start..end])
        }
    }

    struct TestGrants {
        windows: Mutex<HashMap<GrantKey, (Region, u64, u64)>>,
    }

    impl TestGrants {
        fn grant(&self, key: GrantKey, region: Region, offset: u64, len: u64) {
            self.windows
                .lock()
                .unwrap()
                .insert(key, (region, offset, len));
        }
    }

    impl GrantTable for TestGrants {
        fn window(&self, _session: u64, key: GrantKey, _put: bool) -> Option<(Region, u64, u64)> {
            self.windows.lock().unwrap().get(&key).copied()
        }
    }

    const TOKEN: [u8; TOKEN_BYTES] = [9u8; TOKEN_BYTES];

    fn hash(bytes: &[u8]) -> u64 {
        twox_hash::XxHash3_64::oneshot(bytes)
    }

    fn put_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn put_u64(out: &mut Vec<u8>, value: u64) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn encode_header(key: Key, row_checksum: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(record::HEADER_BYTES);
        out.extend_from_slice(b"FLARECKP");
        put_u32(&mut out, 1);
        put_u32(&mut out, 0);
        out.extend_from_slice(&[1u8; 32]);
        out.extend_from_slice(&[2u8; 16]);
        put_u64(&mut out, key.consumed_tokens);
        out.extend_from_slice(&key.digest);
        out.push(0);
        put_u64(&mut out, 0);
        out.extend_from_slice(&[0u8; 16]);
        out.extend_from_slice(&[3u8; 16]);
        put_u64(&mut out, key.consumed_tokens);
        put_u32(&mut out, 1);
        put_u64(&mut out, 8192);
        put_u64(&mut out, 12288);
        put_u64(&mut out, row_checksum);
        put_u64(&mut out, 0);
        put_u32(&mut out, 1);
        put_u32(&mut out, 1);
        put_u64(&mut out, 4096);
        put_u64(&mut out, 4096);
        out.resize(record::HEADER_BYTES, 0);
        out
    }

    #[derive(Clone)]
    struct FixtureRecord {
        key: Key,
        proto_key: proto::Key,
        descriptor: proto::Descriptor,
        header: Vec<u8>,
        checksums: Vec<u8>,
        page: Vec<u8>,
        row: Vec<u8>,
        row_checksum: u64,
    }

    impl FixtureRecord {
        fn new(consumed_tokens: u64, digest: [u8; 16]) -> Self {
            let key = Key {
                consumed_tokens,
                digest,
            };
            let page: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
            let row: Vec<u8> = (0..4096).map(|index| (index % 239) as u8).collect();
            let row_checksum = hash(&row);
            let mut checksums = vec![0u8; 4096];
            checksums[..8].copy_from_slice(&hash(&page).to_le_bytes());
            Self {
                key,
                proto_key: key_to_proto(key),
                descriptor: proto::Descriptor {
                    identity: vec![1; 32],
                    fingerprint: vec![2; 16],
                    page_tokens: consumed_tokens,
                    ranks: vec![proto::RankPlanes {
                        plane_bytes: vec![4096],
                        row_bytes: 4096,
                    }],
                    page_record_bytes: 4096,
                    row_record_bytes: 4096,
                    tenant: String::new(),
                },
                header: encode_header(key, row_checksum),
                checksums,
                page,
                row,
                row_checksum,
            }
        }

        fn len(&self) -> u64 {
            (self.header.len() + self.checksums.len() + self.page.len() + self.row.len()) as u64
        }

        fn metadata(&self) -> Vec<u8> {
            let mut metadata = self.header.clone();
            metadata.extend_from_slice(&self.checksums);
            metadata
        }

        fn bytes(&self) -> Vec<u8> {
            let mut bytes = self.metadata();
            bytes.extend_from_slice(&self.page);
            bytes.extend_from_slice(&self.row);
            bytes
        }
    }

    #[derive(Clone)]
    struct Fake {
        token: [u8; TOKEN_BYTES],
        data: SocketAddr,
        record: FixtureRecord,
        regions: Arc<TestRegions>,
        grants: Arc<TestGrants>,
        present: bool,
        refuse_subscribe: Option<tonic::Code>,
    }

    fn check_generation<T>(request: &tonic::Request<T>) -> Result<(), tonic::Status> {
        match request.metadata().get("x-selene-generation") {
            Some(value) if value.to_str().ok() == Some("7") => Ok(()),
            _ => Err(tonic::Status::unauthenticated("missing generation")),
        }
    }

    fn window(
        transfer: u64,
        index: u32,
        record_offset: u64,
        length: u64,
        offset: u64,
    ) -> proto::WindowGrant {
        proto::WindowGrant {
            transfer,
            index,
            record: 0,
            record_offset,
            length,
            memory: Some(proto::WindowRef {
                region: proto::Region::Arena as i32,
                offset,
                length,
            }),
            page_first: 0,
            page_count: if index == 0 { 1 } else { 0 },
            row: index != 0,
        }
    }

    #[tonic::async_trait]
    impl proto::peer_cache_server::PeerCache for Fake {
        type SubscribeStream = ReceiverStream<Result<proto::CatalogEvent, tonic::Status>>;
        type PutStream = ReceiverStream<Result<proto::PutReply, tonic::Status>>;
        type GetStream = ReceiverStream<Result<proto::GetReply, tonic::Status>>;

        async fn peer_hello(
            &self,
            _request: tonic::Request<proto::PeerHelloRequest>,
        ) -> Result<tonic::Response<proto::PeerHelloReply>, tonic::Status> {
            Ok(tonic::Response::new(proto::PeerHelloReply {
                generation: 7,
                transport: "stream".to_string(),
                stream_token: self.token.to_vec(),
                stream_addr: self.data.to_string(),
                rdma: Vec::new(),
                arena_bytes: 8 << 20,
                staging_bytes: 4 << 20,
            }))
        }

        async fn subscribe(
            &self,
            request: tonic::Request<proto::SubscribeRequest>,
        ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
            check_generation(&request)?;
            if let Some(code) = self.refuse_subscribe {
                return Err(tonic::Status::new(code, "namespace refused"));
            }
            let (sender, receiver) = mpsc::channel(8);
            let key = self.record.proto_key.clone();
            tokio::spawn(async move {
                let events = [
                    proto::CatalogEvent {
                        sequence: 1,
                        event: Some(proto::catalog_event::Event::SnapshotBegin(
                            proto::SnapshotBegin { seq0: 0 },
                        )),
                    },
                    proto::CatalogEvent {
                        sequence: 2,
                        event: Some(proto::catalog_event::Event::Entry(proto::CatalogEntry {
                            key: Some(key),
                            parent: None,
                            has_parent: false,
                            bucket: vec![3; 16],
                            tail_pages: 1,
                            row: true,
                            tier: proto::Tier::Dram as i32,
                            durable: true,
                            bytes: 16384,
                            unreadable: false,
                        })),
                    },
                    proto::CatalogEvent {
                        sequence: 3,
                        event: Some(proto::catalog_event::Event::SnapshotEnd(
                            proto::SnapshotEnd {},
                        )),
                    },
                ];
                for event in events {
                    if sender.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
            });
            Ok(tonic::Response::new(ReceiverStream::new(receiver)))
        }

        async fn put(
            &self,
            request: tonic::Request<tonic::Streaming<proto::PutMessage>>,
        ) -> Result<tonic::Response<Self::PutStream>, tonic::Status> {
            check_generation(&request)?;
            let mut input = request.into_inner();
            let (sender, receiver) = mpsc::channel(8);
            let record = self.record.clone();
            let grants = self.grants.clone();
            let regions = self.regions.clone();
            let present = self.present;
            tokio::spawn(async move {
                let Ok(Some(message)) = input.message().await else {
                    return;
                };
                let Some(proto::put_message::M::Begin(_begin)) = message.m else {
                    return;
                };
                if present {
                    let _ = sender
                        .send(Ok(proto::PutReply {
                            r: Some(proto::put_reply::R::Refused(proto::Refused {
                                reason: proto::Reason::Present as i32,
                                detail: "already present".to_string(),
                            })),
                        }))
                        .await;
                    return;
                }
                let transfer = 42;
                let page_bytes = record.page.len() as u64;
                let row_bytes = record.row.len() as u64;
                grants.grant(
                    GrantKey { transfer, index: 0 },
                    Region::Arena,
                    0,
                    page_bytes,
                );
                grants.grant(
                    GrantKey { transfer, index: 1 },
                    Region::Arena,
                    page_bytes,
                    row_bytes,
                );
                let accept = proto::PutReply {
                    r: Some(proto::put_reply::R::Accept(proto::PutAccept {
                        transfer,
                        first_page: 0,
                        parent: None,
                        has_parent: false,
                        record_bytes: record.len(),
                    })),
                };
                if sender.send(Ok(accept)).await.is_err() {
                    return;
                }
                let grant0 = window(transfer, 0, 8192, page_bytes, 0);
                if sender
                    .send(Ok(proto::PutReply {
                        r: Some(proto::put_reply::R::Grant(grant0)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                let Ok(Some(message)) = input.message().await else {
                    return;
                };
                let Some(proto::put_message::M::Filled(filled)) = message.m else {
                    return;
                };
                assert_eq!(filled.index, 0);
                assert_eq!(filled.page_xxh3, vec![hash(&record.page)]);
                assert_eq!(
                    &regions.arena.lock().unwrap()[..page_bytes as usize],
                    &record.page[..]
                );
                let grant1 = window(transfer, 1, 12288, row_bytes, page_bytes);
                if sender
                    .send(Ok(proto::PutReply {
                        r: Some(proto::put_reply::R::Grant(grant1)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                let Ok(Some(message)) = input.message().await else {
                    return;
                };
                let Some(proto::put_message::M::Filled(filled)) = message.m else {
                    return;
                };
                assert_eq!(filled.index, 1);
                assert!(filled.has_row);
                assert_eq!(filled.row_xxh3, record.row_checksum);
                assert_eq!(
                    &regions.arena.lock().unwrap()
                        [page_bytes as usize..(page_bytes + row_bytes) as usize],
                    &record.row[..]
                );
                let expected = record.metadata();
                let mut metadata = vec![0u8; expected.len()];
                let mut seen = 0usize;
                while seen < expected.len() {
                    let Ok(Some(message)) = input.message().await else {
                        return;
                    };
                    let Some(proto::put_message::M::Commit(commit)) = message.m else {
                        return;
                    };
                    let start = commit.metadata_offset as usize;
                    let end = start + commit.metadata.len();
                    metadata[start..end].copy_from_slice(&commit.metadata);
                    seen = seen.max(end);
                }
                assert_eq!(metadata, expected);
                let _ = sender
                    .send(Ok(proto::PutReply {
                        r: Some(proto::put_reply::R::Stored(proto::Stored {
                            bytes: record.len(),
                        })),
                    }))
                    .await;
            });
            Ok(tonic::Response::new(ReceiverStream::new(receiver)))
        }

        async fn get(
            &self,
            request: tonic::Request<tonic::Streaming<proto::GetMessage>>,
        ) -> Result<tonic::Response<Self::GetStream>, tonic::Status> {
            check_generation(&request)?;
            let mut input = request.into_inner();
            let (sender, receiver) = mpsc::channel(8);
            let record = self.record.clone();
            let grants = self.grants.clone();
            tokio::spawn(async move {
                let Ok(Some(message)) = input.message().await else {
                    return;
                };
                let Some(proto::get_message::M::Begin(begin)) = message.m else {
                    return;
                };
                let transfer = begin.transfer;
                let page_bytes = record.page.len() as u64;
                let row_bytes = record.row.len() as u64;
                let replies = [
                    proto::GetReply {
                        r: Some(proto::get_reply::R::Leased(proto::GetLeased {
                            transfer,
                            record_bytes: vec![record.len()],
                        })),
                    },
                    proto::GetReply {
                        r: Some(proto::get_reply::R::Meta(proto::RecordMeta {
                            record: 0,
                            metadata_offset: 0,
                            metadata: record.metadata(),
                            source: proto::Tier::Dram as i32,
                        })),
                    },
                ];
                for reply in replies {
                    if sender.send(Ok(reply)).await.is_err() {
                        return;
                    }
                }
                grants.grant(
                    GrantKey { transfer, index: 0 },
                    Region::Arena,
                    8192,
                    page_bytes,
                );
                if sender
                    .send(Ok(proto::GetReply {
                        r: Some(proto::get_reply::R::Ready(window(
                            transfer, 0, 8192, page_bytes, 8192,
                        ))),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                let Ok(Some(message)) = input.message().await else {
                    return;
                };
                let Some(proto::get_message::M::Released(released)) = message.m else {
                    return;
                };
                assert_eq!(released.index, 0);
                grants.grant(
                    GrantKey { transfer, index: 1 },
                    Region::Arena,
                    12288,
                    row_bytes,
                );
                if sender
                    .send(Ok(proto::GetReply {
                        r: Some(proto::get_reply::R::Ready(window(
                            transfer, 1, 12288, row_bytes, 12288,
                        ))),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
                let Ok(Some(message)) = input.message().await else {
                    return;
                };
                let Some(proto::get_message::M::Released(released)) = message.m else {
                    return;
                };
                assert_eq!(released.index, 1);
                let _ = sender
                    .send(Ok(proto::GetReply {
                        r: Some(proto::get_reply::R::Done(proto::GetDone {})),
                    }))
                    .await;
            });
            Ok(tonic::Response::new(ReceiverStream::new(receiver)))
        }
    }

    struct Fixture {
        address: String,
        regions: Arc<TestRegions>,
        record: FixtureRecord,
        _accept: std::thread::JoinHandle<()>,
        _server: tokio::task::JoinHandle<()>,
    }

    async fn fixture_with(key: Key) -> Fixture {
        fixture_mode(key, false).await
    }

    async fn fixture_refusing_subscribe(code: tonic::Code) -> Fixture {
        fixture_mode_with(
            Key {
                consumed_tokens: 64,
                digest: [4; 16],
            },
            false,
            Some(code),
        )
        .await
    }

    async fn fixture_mode(key: Key, present: bool) -> Fixture {
        fixture_mode_with(key, present, None).await
    }

    async fn fixture_mode_with(
        key: Key,
        present: bool,
        refuse_subscribe: Option<tonic::Code>,
    ) -> Fixture {
        let data_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let data_address = data_listener.local_addr().unwrap();
        let regions = TestRegions::new();
        let record = FixtureRecord::new(key.consumed_tokens, key.digest);
        {
            let mut arena = regions.arena.lock().unwrap();
            let bytes = record.bytes();
            arena[..bytes.len()].copy_from_slice(&bytes);
        }
        let grants = Arc::new(TestGrants {
            windows: Mutex::new(HashMap::new()),
        });
        let sessions = Arc::new(crate::grants::Sessions::default());
        sessions.register(TOKEN, 1);
        let receiver = StreamReceiver::new(
            regions.clone(),
            grants.clone(),
            sessions,
            crate::data::stream::PeerSizes::new(8 << 20, 4 << 20),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let accept = receiver.serve(data_listener, stop.clone()).unwrap();
        let fake = Fake {
            token: TOKEN,
            data: data_address,
            record: record.clone(),
            regions: regions.clone(),
            grants,
            present,
            refuse_subscribe,
        };
        let control = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_address = control.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(proto::peer_cache_server::PeerCacheServer::new(fake))
                .serve_with_incoming(TcpListenerStream::new(control))
                .await;
        });
        Fixture {
            address: control_address.to_string(),
            regions,
            record,
            _accept: accept,
            _server: server,
        }
    }

    fn fixture() -> impl std::future::Future<Output = Fixture> {
        fixture_with(Key {
            consumed_tokens: 64,
            digest: [4; 16],
        })
    }

    async fn client(fixture: &Fixture, local: Arc<TestRegions>) -> PeerClient {
        PeerClient::connect(&fixture.address, "node-a", local, 2, None, "stream")
            .await
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_transfers_the_exact_record() {
        let fixture = fixture().await;
        let local = TestRegions::new();
        let record = fixture.record.clone();
        {
            let mut arena = local.arena.lock().unwrap();
            let bytes = record.bytes();
            arena[..bytes.len()].copy_from_slice(&bytes);
        }
        let mut client = client(&fixture, local).await;
        let forwarded = client
            .forward(ForwardRecord {
                namespace: [0u8; 32],
                key: record.key,
                chain_pages: 1,
                published_parent: None,
                bucket: [3; 16],
                published_tail_pages: 1,
                row: true,
                descriptor: record.descriptor.clone(),
                source: ForwardBytes::Shared {
                    region: Region::Arena,
                    offset: 0,
                },
                header: record.header.clone(),
                checksum_segment: record.checksums.clone(),
            })
            .await
            .unwrap();
        assert_eq!(forwarded.bytes, record.len());
        let arena = fixture.regions.arena.lock().unwrap();
        assert_eq!(&arena[..record.page.len()], &record.page[..]);
        assert_eq!(
            &arena[record.page.len()..record.page.len() + record.row.len()],
            &record.row[..]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_disk_record_forwards_through_a_bounded_staging_window() {
        let fixture = fixture().await;
        let record = fixture.record.clone();
        let regions = Arc::new(crate::regions::LocalRegions::new(
            crate::arena::Arena::new(8 << 20).unwrap(),
            crate::arena::Arena::new(4 << 20).unwrap(),
        ));
        let path = std::env::temp_dir().join(format!(
            "selene-disk-forward-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        // The manager's 4096-byte record envelope precedes the record in its file.
        const ENVELOPE: u64 = 4096;
        {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&path).unwrap();
            file.write_all(&vec![0u8; ENVELOPE as usize]).unwrap();
            file.write_all(&record.bytes()).unwrap();
        }
        let file = Arc::new(std::fs::File::open(&path).unwrap());
        // One window at a time, smaller than the whole record (metadata plus pages plus row).
        let window = record.page.len().max(record.row.len());
        let staging = regions.staging().allocate(window).unwrap();
        let mut client =
            PeerClient::connect(&fixture.address, "node-a", regions, 2, None, "stream")
                .await
                .unwrap();
        let forwarded = client
            .forward(ForwardRecord {
                namespace: [0u8; 32],
                key: record.key,
                chain_pages: 1,
                published_parent: None,
                bucket: [3; 16],
                published_tail_pages: 1,
                row: true,
                descriptor: record.descriptor.clone(),
                source: ForwardBytes::Disk {
                    file,
                    offset: ENVELOPE,
                    staging,
                },
                header: record.header.clone(),
                checksum_segment: record.checksums.clone(),
            })
            .await
            .unwrap();
        assert_eq!(forwarded.bytes, record.len());
        let arena = fixture.regions.arena.lock().unwrap();
        assert_eq!(&arena[..record.page.len()], &record.page[..]);
        assert_eq!(
            &arena[record.page.len()..record.page.len() + record.row.len()],
            &record.row[..]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_restores_and_validates_the_record() {
        let fixture = fixture().await;
        let local = TestRegions::new();
        let record = fixture.record.clone();
        let mut client = client(&fixture, local.clone()).await;
        let fetched = client
            .fetch(FetchRequest {
                descriptor: record.descriptor.clone(),
                namespace: [0u8; 32],
                chain: vec![record.key],
                skip_pages: 0,
                expected_pages: 1,
                row: true,
                destinations: vec![proto::GetDestination {
                    record: 0,
                    region: proto::Region::Arena as i32,
                    offset: 0,
                    bytes: record.len(),
                }],
            })
            .await
            .unwrap();
        assert_eq!(fetched.record_bytes, vec![record.len()]);
        assert_eq!(fetched.metadata, vec![record.metadata()]);
        let arena = local.arena.lock().unwrap();
        assert_eq!(
            &arena[record.header.len() + record.checksums.len()
                ..record.header.len() + record.checksums.len() + record.page.len()],
            &record.page[..]
        );
        assert_eq!(
            &arena[record.header.len() + record.checksums.len() + record.page.len()
                ..record.header.len()
                    + record.checksums.len()
                    + record.page.len()
                    + record.row.len()],
            &record.row[..]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn placement_selects_the_owning_peer_and_forwards_to_it() {
        let a = Key {
            consumed_tokens: 1024,
            digest: [0xaa; 16],
        };
        let b = Key {
            consumed_tokens: 256,
            digest: [0xbb; 16],
        };
        let c = Key {
            consumed_tokens: 64,
            digest: [0xcc; 16],
        };
        let alpha = fixture_with(b).await;
        let beta = fixture_with(a).await;
        let gamma = fixture_with(c).await;
        let upstreams = vec![
            Upstream {
                name: "alpha".to_string(),
                address: alpha.address.clone(),
                weight: 1,
            },
            Upstream {
                name: "beta".to_string(),
                address: beta.address.clone(),
                weight: 1,
            },
            Upstream {
                name: "gamma".to_string(),
                address: gamma.address.clone(),
                weight: 1,
            },
        ];
        let local = TestRegions::new();
        let mut set = PeerSet::connect(&upstreams, local.clone(), 1, None, "stream")
            .await
            .unwrap();
        let namespace: [u8; 32] = std::array::from_fn(|index| index as u8);
        assert_eq!(set.owner(&namespace, &a), Some("beta"));
        assert_eq!(set.owner(&namespace, &b), Some("alpha"));
        let record = beta.record.clone();
        {
            let mut arena = local.arena.lock().unwrap();
            let bytes = record.bytes();
            arena[..bytes.len()].copy_from_slice(&bytes);
        }
        let forwarded = set
            .client_mut("beta")
            .unwrap()
            .forward(ForwardRecord {
                namespace,
                key: record.key,
                chain_pages: 1,
                published_parent: None,
                bucket: [3; 16],
                published_tail_pages: 1,
                row: true,
                descriptor: record.descriptor.clone(),
                source: ForwardBytes::Shared {
                    region: Region::Arena,
                    offset: 0,
                },
                header: record.header.clone(),
                checksum_segment: record.checksums.clone(),
            })
            .await
            .unwrap();
        assert_eq!(forwarded.bytes, record.len());
        {
            let arena = beta.regions.arena.lock().unwrap();
            assert_eq!(&arena[..record.page.len()], &record.page[..]);
            assert_eq!(
                &arena[record.page.len()..record.page.len() + record.row.len()],
                &record.row[..]
            );
        }
        let mut weighted = upstreams;
        weighted[2].weight = 9;
        let weighted_set = PeerSet::connect(&weighted, local, 1, None, "stream")
            .await
            .unwrap();
        assert_eq!(weighted_set.owner(&namespace, &a), Some("gamma"));
    }

    fn three_upstreams(alpha: &Fixture, beta: &Fixture, gamma: &Fixture) -> Vec<Upstream> {
        vec![
            Upstream {
                name: "alpha".to_string(),
                address: alpha.address.clone(),
                weight: 1,
            },
            Upstream {
                name: "beta".to_string(),
                address: beta.address.clone(),
                weight: 1,
            },
            Upstream {
                name: "gamma".to_string(),
                address: gamma.address.clone(),
                weight: 1,
            },
        ]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dropping_the_owner_moves_placement_and_marks_it_failed() {
        let a = Key {
            consumed_tokens: 1024,
            digest: [0xaa; 16],
        };
        let alpha = fixture_with(Key {
            consumed_tokens: 256,
            digest: [0xbb; 16],
        })
        .await;
        let beta = fixture_with(a).await;
        let gamma = fixture_with(Key {
            consumed_tokens: 64,
            digest: [0xcc; 16],
        })
        .await;
        let upstreams = three_upstreams(&alpha, &beta, &gamma);
        let mut set = PeerSet::connect(&upstreams, TestRegions::new(), 1, None, "stream")
            .await
            .unwrap();
        let namespace: [u8; 32] = std::array::from_fn(|index| index as u8);
        assert_eq!(set.owner(&namespace, &a), Some("beta"));
        set.drop_client("beta");
        assert!(set.failed().contains(&"beta".to_string()));
        assert_eq!(set.owner(&namespace, &a), Some("beta"));
        assert!(set.client_mut("beta").is_none());
        // Dropping the client is the point of abandoning a forward: the stalled connection goes
        // away, and the backoff retry opens a fresh one with fresh window credits.
        let reconnected = set
            .reconnect(std::time::Instant::now() + std::time::Duration::from_secs(1))
            .await;
        assert_eq!(reconnected, vec!["beta".to_string()]);
        assert!(set.client_mut("beta").is_some());
        assert!(set.failed().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn present_counts_as_a_successful_forward() {
        let a = Key {
            consumed_tokens: 64,
            digest: [0xaa; 16],
        };
        let peer = fixture_mode(a, true).await;
        let local = TestRegions::new();
        let record = peer.record.clone();
        {
            let mut arena = local.arena.lock().unwrap();
            let bytes = record.bytes();
            arena[..bytes.len()].copy_from_slice(&bytes);
        }
        let mut client = client(&peer, local).await;
        let forwarded = client
            .forward(ForwardRecord {
                namespace: [0u8; 32],
                key: record.key,
                chain_pages: 1,
                published_parent: None,
                bucket: [3; 16],
                published_tail_pages: 1,
                row: true,
                descriptor: record.descriptor.clone(),
                source: ForwardBytes::Shared {
                    region: Region::Arena,
                    offset: 0,
                },
                header: record.header.clone(),
                checksum_segment: record.checksums.clone(),
            })
            .await
            .unwrap();
        assert_eq!(forwarded.bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribe_fans_in_to_every_connected_peer() {
        let alpha = fixture_with(Key {
            consumed_tokens: 64,
            digest: [0xaa; 16],
        })
        .await;
        let beta = fixture_with(Key {
            consumed_tokens: 64,
            digest: [0xbb; 16],
        })
        .await;
        let gamma = fixture_with(Key {
            consumed_tokens: 64,
            digest: [0xcc; 16],
        })
        .await;
        let upstreams = three_upstreams(&alpha, &beta, &gamma);
        let mut set = PeerSet::connect(&upstreams, TestRegions::new(), 1, None, "stream")
            .await
            .unwrap();
        let mut streams = set.subscribe([0u8; 32], proto::Descriptor::default()).await;
        assert_eq!(streams.len(), 3);
        for (_name, stream) in &mut streams {
            loop {
                let Some(event) = stream.message().await.unwrap() else {
                    panic!("peer stream ended before the snapshot");
                };
                if matches!(
                    event.event,
                    Some(proto::catalog_event::Event::SnapshotEnd(_))
                ) {
                    break;
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_silent_upstream_does_not_discard_a_healthy_one() {
        let good = fixture().await;
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_address = silent.local_addr().unwrap().to_string();
        let hold = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = silent.accept().await {
                held.push(stream);
            }
        });
        // The silent upstream comes first: the connect must survive it.
        let upstreams = vec![
            Upstream {
                name: "silent".to_string(),
                address: silent_address,
                weight: 1,
            },
            Upstream {
                name: "good".to_string(),
                address: good.address.clone(),
                weight: 1,
            },
        ];
        let started = std::time::Instant::now();
        let set = PeerSet::connect_within(
            &upstreams,
            TestRegions::new(),
            1,
            None,
            "stream",
            std::time::Duration::from_millis(200),
        )
        .await
        .expect("the healthy upstream connects");
        assert!(set.client("good").is_some());
        assert_eq!(set.failed(), vec!["silent".to_string()]);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        hold.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_silent_upstream_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let serve = tokio::spawn(async move {
            // Accept and never speak: the control handshake cannot complete.
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                held.push(stream);
            }
        });
        let started = std::time::Instant::now();
        let error = PeerClient::connect_within(
            &address,
            "node-a",
            TestRegions::new(),
            1,
            None,
            "stream",
            std::time::Duration::from_millis(200),
        )
        .await
        .err()
        .expect("a silent upstream must not connect");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        serve.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_namespace_is_not_a_broken_connection() {
        let fixture = fixture_refusing_subscribe(tonic::Code::FailedPrecondition).await;
        let mut client = client(&fixture, TestRegions::new()).await;
        let error = client
            .subscribe([0u8; 32], proto::Descriptor::default())
            .await
            .unwrap_err();
        // The caller retries this namespace alone; the connection stays usable.
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn reconnect_backoff_is_exponential_and_capped() {
        assert_eq!(backoff(0), std::time::Duration::from_millis(100));
        assert_eq!(backoff(1), std::time::Duration::from_millis(200));
        assert_eq!(backoff(2), std::time::Duration::from_millis(400));
        assert_eq!(backoff(6), std::time::Duration::from_secs(5));
        assert_eq!(backoff(100), std::time::Duration::from_secs(5));
    }

    #[test]
    fn checksum_indexes_are_relative_to_the_record_tail() {
        assert_eq!(table_index(16, 4, 12, 0).unwrap(), 0);
        assert_eq!(table_index(16, 4, 13, 1).unwrap(), 2);
        assert!(table_index(16, 4, 11, 0).is_err());
        assert!(table_index(16, 4, 16, 0).is_err());
        assert!(table_index(16, 4, 12, 4).is_err());
        assert!(table_index(2, 4, 0, 0).is_err());
    }

    #[test]
    fn grant_shapes_are_bounded_by_the_record_and_destination() {
        let record = FixtureRecord::new(64, [4; 16]);
        let header = record::Header::decode(&record.header).unwrap();
        let page = window(1, 0, header.page_segment, 4096, 0);
        validate_grant_shape(&header, &page, record.len(), Some(record.len())).unwrap();
        assert!(validate_grant_shape(&header, &page, record.len(), Some(4096)).is_err());
        assert!(validate_grant_shape(&header, &page, 4096, None).is_err());
        let mut bad_memory = page;
        bad_memory.memory.as_mut().unwrap().length = 1;
        assert!(validate_grant_shape(&header, &bad_memory, record.len(), None).is_err());
        let mut combined = page;
        combined.row = true;
        combined.length = 8192;
        combined.memory.as_mut().unwrap().length = 8192;
        validate_grant_shape(&header, &combined, record.len(), None).unwrap();
        combined.length = 4096;
        combined.memory.as_mut().unwrap().length = 4096;
        assert!(validate_grant_shape(&header, &combined, record.len(), None).is_err());
        let row = window(1, 1, header.row_segment, 4096, 8192);
        validate_grant_shape(&header, &row, record.len(), None).unwrap();
        let mut bad_row = row;
        bad_row.record_offset = 4096;
        assert!(validate_grant_shape(&header, &bad_row, record.len(), None).is_err());
    }

    #[test]
    fn metadata_fragments_are_contiguous_and_bounded() {
        let record = FixtureRecord::new(64, [4; 16]);
        let metadata = record.metadata();
        let mut buffer = MetaBuffer::new();
        buffer.push(0, &metadata[..4096]).unwrap();
        assert!(buffer.header.is_some());
        assert!(!buffer.complete());
        buffer.push(4096, &metadata[4096..]).unwrap();
        assert!(buffer.complete());
        let mut gap = MetaBuffer::new();
        gap.push(0, &metadata[..2048]).unwrap();
        assert!(gap.push(4096, &metadata[4096..]).is_err());
        let mut oversized = MetaBuffer::new();
        assert!(oversized.push(0, &vec![0u8; 17 << 20]).is_err());
        let mut beyond = MetaBuffer::new();
        beyond.push(0, &metadata).unwrap();
        assert!(beyond.push(metadata.len() as u64, &[0u8; 4096]).is_err());
    }

    #[test]
    fn descriptor_conversion_checks_sizes_and_lengths() {
        let record = FixtureRecord::new(64, [4; 16]);
        let descriptor = descriptor_from_proto(&record.descriptor).unwrap();
        assert_eq!(descriptor.page_tokens, 64);
        let mut wrong_sizes = record.descriptor.clone();
        wrong_sizes.page_record_bytes = 8192;
        assert!(descriptor_from_proto(&wrong_sizes).is_err());
        let mut short = record.descriptor.clone();
        short.identity = vec![1; 3];
        assert!(descriptor_from_proto(&short).is_err());
    }

    #[test]
    fn metadata_must_match_the_requested_chain_and_descriptor() {
        let record = FixtureRecord::new(64, [4; 16]);
        let header = record::Header::decode(&record.header).unwrap();
        let request = FetchRequest {
            descriptor: record.descriptor.clone(),
            namespace: [0u8; 32],
            chain: vec![record.key],
            skip_pages: 0,
            expected_pages: 1,
            row: true,
            destinations: Vec::new(),
        };
        validate_meta(0, &request, &header).unwrap();
        let mut wrong_key = FetchRequest {
            descriptor: record.descriptor.clone(),
            namespace: [0u8; 32],
            chain: vec![Key {
                consumed_tokens: 128,
                digest: [4; 16],
            }],
            skip_pages: 0,
            expected_pages: 1,
            row: true,
            destinations: Vec::new(),
        };
        assert!(validate_meta(0, &wrong_key, &header).is_err());
        wrong_key.chain[0] = record.key;
        let mut wrong_descriptor = record.descriptor.clone();
        wrong_descriptor.page_tokens = 128;
        wrong_key.descriptor = wrong_descriptor;
        assert!(validate_meta(0, &wrong_key, &header).is_err());
        let mut child = header.clone();
        child.key = Key {
            consumed_tokens: 128,
            digest: [5; 16],
        };
        child.parent = Some(record.key);
        let child_request = FetchRequest {
            descriptor: record.descriptor.clone(),
            namespace: [0u8; 32],
            chain: vec![record.key, child.key],
            skip_pages: 0,
            expected_pages: 1,
            row: true,
            destinations: Vec::new(),
        };
        validate_meta(1, &child_request, &child).unwrap();
        child.parent = None;
        assert!(validate_meta(1, &child_request, &child).is_err());
    }

    #[test]
    fn rowless_forwarding_rejects_row_grants_and_row_bytes() {
        let record = FixtureRecord::new(64, [4; 16]);
        let header = record::Header::decode(&record.header).unwrap();
        let reservation = header.length(false);
        let page = window(1, 0, header.page_segment, 4096, 0);
        forward_grant(&header, false, &page, reservation).unwrap();
        let row = window(1, 1, header.row_segment, 4096, 8192);
        assert!(forward_grant(&header, false, &row, reservation).is_err());
        let combined = proto::WindowGrant {
            transfer: 1,
            index: 2,
            record: 0,
            record_offset: header.page_segment,
            length: 8192,
            memory: Some(proto::WindowRef {
                region: proto::Region::Arena as i32,
                offset: 0,
                length: 8192,
            }),
            page_first: 0,
            page_count: 1,
            row: true,
        };
        assert!(forward_grant(&header, false, &combined, reservation).is_err());
        forward_grant(&header, true, &row, header.length(true)).unwrap();
    }

    #[test]
    fn fetch_coverage_counts_distinct_pages_and_the_final_row() {
        let record = FixtureRecord::new(64, [4; 16]);
        let header = record::Header::decode(&record.header).unwrap();
        let mut child = header.clone();
        child.key = Key {
            consumed_tokens: 128,
            digest: [5; 16],
        };
        child.parent = Some(record.key);
        let page = |index: u32, record_offset: u64| proto::WindowGrant {
            transfer: 1,
            index,
            record: 0,
            record_offset,
            length: 4096,
            memory: Some(proto::WindowRef {
                region: proto::Region::Arena as i32,
                offset: 0,
                length: 4096,
            }),
            page_first: 0,
            page_count: 1,
            row: false,
        };
        let mut coverage = FetchCoverage::new();
        coverage
            .grant(0, &header, &page(0, header.page_segment), true, 2)
            .unwrap();
        coverage
            .grant(0, &header, &page(9, header.page_segment), true, 2)
            .unwrap();
        assert!(
            coverage
                .verify(&[header.clone(), child.clone()], 0, 2, true)
                .is_err()
        );
        let mut child_page = page(1, child.page_segment);
        child_page.page_first = 1;
        coverage.grant(1, &child, &child_page, true, 2).unwrap();
        assert!(
            coverage
                .verify(&[header.clone(), child.clone()], 0, 2, true)
                .is_err()
        );
        let row = proto::WindowGrant {
            transfer: 1,
            index: 2,
            record: 0,
            record_offset: child.row_segment,
            length: 4096,
            memory: Some(proto::WindowRef {
                region: proto::Region::Arena as i32,
                offset: 0,
                length: 4096,
            }),
            page_first: 0,
            page_count: 0,
            row: true,
        };
        coverage.grant(1, &child, &row, true, 2).unwrap();
        coverage
            .verify(&[header.clone(), child], 0, 2, true)
            .unwrap();
        let mut wrong = FetchCoverage::new();
        assert!(wrong.grant(0, &header, &row, true, 2).is_err());
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn a_region_without_a_mapping_falls_back_to_stream() {
        let regions: Arc<dyn RegionAccess> = TestRegions::new();
        assert!(local_rdma(&regions).is_none());
    }
}
