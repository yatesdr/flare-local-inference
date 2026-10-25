//! A scripted instance on the RDMA carrier: it registers staging, makes a queue pair for
//! each transfer, lends slots to a claim and offers its slots to a put, as the client does.

use super::{
    content_for, descriptor,
    pool::{Damage, PutResult, Session},
};
use flare_selene::pool::{
    frame::{Carrier, NO_SLOT, ToInstance, ToManager, Window},
    rdma,
};
use selene_manager::{
    Key,
    arena::Arena,
    proto::v2::{self, client_message as c, server_message::M},
};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, net::TcpStream, time::timeout};

const MAX_PAGES: u32 = 64;
const SLOT: usize = 1 << 20;

pub struct Instance {
    staging: Arena,
    device: Arc<rdma::Device>,
}

pub struct Dialed {
    pub stream: TcpStream,
    pub pair: rdma::Pair,
}

pub struct Received {
    pub window: Window,
    pub payload: Vec<u8>,
}

impl Instance {
    pub fn new() -> Self {
        let staging = Arena::new(4 * SLOT).unwrap();
        let (base, len) = staging.mapping();
        // SAFETY: the arena stays mapped and in place for the life of the instance.
        let device = unsafe { rdma::Device::open(base, len) }.expect("an RDMA device");
        Self { staging, device }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn bytes(&self, offset: usize, length: usize) -> &mut [u8] {
        let (base, len) = self.staging.mapping();
        assert!(offset + length <= len);
        // SAFETY: inside the mapping; the test drives one transfer at a time.
        unsafe { std::slice::from_raw_parts_mut(base.add(offset), length) }
    }

    /// Connect, say hello for RDMA and bring the queue pair up.
    pub async fn dial(&self, reference: &v2::DataChannelRef) -> Dialed {
        use tokio::io::AsyncWriteExt;
        let mut stream = TcpStream::connect(&reference.address).await.unwrap();
        stream.write_all(&reference.token).await.unwrap();
        let pair = rdma::Pair::new(&self.device).unwrap();
        ToManager::Hello(Carrier::Rdma(pair.handshake().encode().to_vec()))
            .write(&mut stream)
            .await
            .unwrap();
        let ToInstance::Accept(theirs) = ToInstance::read(&mut stream, MAX_PAGES).await.unwrap()
        else {
            panic!("expected the manager's end of the pair");
        };
        pair.connect(&rdma::Handshake::decode(&theirs).unwrap())
            .unwrap();
        Dialed { stream, pair }
    }

    /// Lend two slots, read windows as they are named, and lend a slot back for each.
    pub async fn read_windows(&self, dialed: &mut Dialed) -> (Vec<Received>, bool) {
        let mut received = Vec::new();
        let mut next = 0usize;
        for _ in 0..2 {
            lend(&mut dialed.stream, next).await;
            next += 1;
        }
        loop {
            let frame = match timeout(
                Duration::from_secs(10),
                ToInstance::read(&mut dialed.stream, MAX_PAGES),
            )
            .await
            {
                Ok(Ok(frame)) => frame,
                _ => return (received, false),
            };
            match frame {
                ToInstance::Window(window) => {
                    assert_ne!(window.slot, NO_SLOT);
                    let payload = self
                        .bytes(window.slot as usize, window.shape.length as usize)
                        .to_vec();
                    received.push(Received { window, payload });
                    lend(&mut dialed.stream, next).await;
                    next += 1;
                }
                ToInstance::End => return (received, true),
                _ => return (received, false),
            }
        }
    }
}

async fn lend(stream: &mut TcpStream, n: usize) {
    ToManager::Credit {
        offset: ((n % 4) * SLOT) as u64,
        length: SLOT as u64,
    }
    .write(stream)
    .await
    .unwrap();
}

/// A put through RDMA: the manager's grants are filled in slot 0 and read from there.
pub async fn put_record(
    instance: &Instance,
    session: &mut Session,
    put_id: u64,
    key: Key,
    parent: Option<Key>,
    damage: Damage,
) -> PutResult {
    use super::pool::PAGE_TOKENS;
    session
        .send(c::M::PutBegin(v2::PutBegin {
            put_id,
            key: Some(v2::PrefixKey {
                consumed_tokens: key.consumed_tokens,
                digest: key.digest.to_vec(),
            }),
            chain_pages: key.consumed_tokens / PAGE_TOKENS,
            row: true,
            has_published_parent: parent.is_some(),
            published_parent: parent.map(|parent| v2::PrefixKey {
                consumed_tokens: parent.consumed_tokens,
                digest: parent.digest.to_vec(),
            }),
            bucket: vec![4; 16],
            published_tail_pages: key.consumed_tokens / PAGE_TOKENS
                - parent.map_or(0, |parent| parent.consumed_tokens / PAGE_TOKENS),
            mode: v2::CopyMode::DataChannel as i32,
        }))
        .await;
    let accept = session
        .take(|m| match m {
            M::PutAccept(accept) if accept.put_id == put_id => Some(Ok(accept.clone())),
            M::PutRefused(refused) if refused.put_id == put_id => Some(Err(refused.clone())),
            _ => None,
        })
        .await;
    let accept = match accept {
        Ok(accept) => accept,
        Err(refused) => return PutResult::Refused(refused),
    };
    let reference = accept.channel.clone().expect("the accept names a channel");
    assert_eq!(reference.transport, v2::DataTransport::Rdma as i32);
    let mut dialed = instance.dial(&reference).await;
    let first_page = accept.first_page as u32;
    let accepted_parent = accept.parent.as_ref().map(|parent| Key {
        consumed_tokens: parent.consumed_tokens,
        digest: parent.digest.as_slice().try_into().unwrap(),
    });
    let (header, mut bytes, _) = content_for(&descriptor(), key, accepted_parent, first_page);
    let payload = header.length_with_row() - header.page_segment;
    let page_bytes = header.page_record_bytes() as usize;
    let mut covered = 0u64;
    while covered < payload {
        let Ok(Ok(ToInstance::Grant(shape))) = timeout(
            Duration::from_secs(10),
            ToInstance::read(&mut dialed.stream, MAX_PAGES),
        )
        .await
        else {
            break;
        };
        let start = if shape.page_count > 0 {
            header.page_segment as usize + (shape.first_page - first_page) as usize * page_bytes
        } else {
            header.row_segment as usize
        };
        let mut hashes = Vec::new();
        for position in 0..shape.page_count as usize {
            let relative = (shape.first_page - first_page) as usize + position;
            let page = header.page_segment as usize + relative * page_bytes;
            if damage.corrupt_page == Some(relative) {
                bytes[page] ^= 1;
            }
            hashes.push(selene_manager::record::checksum(
                &bytes[page..page + page_bytes],
            ));
        }
        let row_hash = shape
            .row
            .then(|| selene_manager::record::checksum(&bytes[header.row_segment as usize..]));
        let length = shape.length as usize;
        instance
            .bytes(0, length)
            .copy_from_slice(&bytes[start..start + length]);
        ToManager::Filled {
            index: shape.index,
            row_xxh3: row_hash,
            page_xxh3: hashes,
            slot: 0,
        }
        .write(&mut dialed.stream)
        .await
        .unwrap();
        // The manager reads the slot and says so; the slot is free again.
        match timeout(
            Duration::from_secs(10),
            ToInstance::read(&mut dialed.stream, MAX_PAGES),
        )
        .await
        {
            Ok(Ok(ToInstance::Taken(index))) => assert_eq!(index, shape.index),
            _ => break,
        }
        covered += shape.length;
        if covered >= payload {
            let mut metadata = bytes[..header.page_segment as usize].to_vec();
            if damage.wrong_key {
                metadata[40] ^= 0xff;
            }
            session
                .send(c::M::PutCommit(v2::PutCommit {
                    put_id,
                    metadata_offset: 0,
                    metadata,
                }))
                .await;
        }
    }
    let next = session
        .take(|m| match m {
            M::Stored(stored) if stored.put_id == put_id => Some(Ok(stored.bytes)),
            M::PutRefused(refused) if refused.put_id == put_id => Some(Err(refused.clone())),
            _ => None,
        })
        .await;
    drop(dialed.stream.read(&mut [0u8; 0]));
    match next {
        Ok(bytes) => PutResult::Stored(bytes),
        Err(refused) => PutResult::Refused(refused),
    }
}
