//! A scripted instance on the data channel: it dials the grant's address, presents the
//! token, and reads a claim's windows or fills a put's, byte for byte as a real client
//! would. It shares the framing with the client crate and nothing else.

use super::{
    content_for, descriptor,
    pool::{Damage, PutResult, Session},
};
use flare_selene::pool::frame::{Carrier, ToInstance, ToManager, Window};
use selene_manager::{
    Key,
    proto::v2::{self, client_message as c, server_message::M},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

pub type Conn = Box<dyn Stream>;

/// What the instance requires of the connection.
#[derive(Clone)]
pub enum Security {
    Plain,
    /// TLS, trusting these roots, with the manager's certificate named `localhost`.
    Tls(rustls::RootCertStore),
}

/// Pages in a window never exceed this.
pub const MAX_PAGES: u32 = 64;

pub fn connector(roots: rustls::RootCertStore) -> tokio_rustls::TlsConnector {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tokio_rustls::TlsConnector::from(Arc::new(config))
}

/// Connect to the grant's address and present its token and hello.
pub async fn dial(reference: &v2::DataChannelRef, security: &Security) -> Conn {
    let tcp = TcpStream::connect(&reference.address)
        .await
        .expect("connect to the data listener");
    let mut conn: Conn = match security {
        Security::Plain => Box::new(tcp),
        Security::Tls(roots) => Box::new(
            connector(roots.clone())
                .connect(
                    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                    tcp,
                )
                .await
                .expect("the TLS handshake"),
        ),
    };
    conn.write_all(&reference.token).await.unwrap();
    ToManager::Hello(Carrier::Tcp)
        .write(&mut conn)
        .await
        .unwrap();
    conn
}

pub struct Received {
    pub window: Window,
    pub payload: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    /// The manager said the claim is complete.
    End,
    /// The manager said why it stopped.
    Abort(String),
    /// The connection ended or broke first.
    Broken,
}

/// Read windows and their payloads until the manager ends the transfer.
pub async fn read_windows(conn: &mut Conn) -> (Vec<Received>, Ended) {
    let mut received = Vec::new();
    loop {
        let frame = match timeout(Duration::from_secs(10), ToInstance::read(conn, MAX_PAGES)).await
        {
            Ok(Ok(frame)) => frame,
            _ => return (received, Ended::Broken),
        };
        match frame {
            ToInstance::Window(window) => {
                let mut payload = vec![0u8; window.shape.length as usize];
                match timeout(Duration::from_secs(10), conn.read_exact(&mut payload)).await {
                    Ok(Ok(_)) => received.push(Received { window, payload }),
                    _ => return (received, Ended::Broken),
                }
            }
            ToInstance::End => return (received, Ended::End),
            ToInstance::Abort(text) => return (received, Ended::Abort(text)),
            ToInstance::Grant(_) => return (received, Ended::Broken),
        }
    }
}

/// A put through the data channel: the same record `Session::put_record` writes through
/// shared windows, filled by the manager's grants over the connection.
pub async fn put_record(
    session: &mut Session,
    security: &Security,
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
            generation: 1,
        }))
        .await;
    enum First {
        Accept(v2::PutAccept),
        Refused(v2::PutRefused),
    }
    let first = session
        .take(|m| match m {
            M::PutAccept(accept) if accept.put_id == put_id => Some(First::Accept(accept.clone())),
            M::PutRefused(refused) if refused.put_id == put_id => {
                Some(First::Refused(refused.clone()))
            }
            _ => None,
        })
        .await;
    let accept = match first {
        First::Accept(accept) => accept,
        First::Refused(refused) => return PutResult::Refused(refused),
    };
    let channel = accept.channel.clone().expect("the accept names a channel");
    if damage.cancel_after_accept {
        session
            .send(c::M::PutCancel(v2::PutCancel { put_id }))
            .await;
        return PutResult::Refused(v2::PutRefused {
            put_id,
            reason: 0,
            detail: "cancelled by the test".into(),
        });
    }
    let mut conn = dial(&channel, security).await;
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
        let frame = match timeout(
            Duration::from_secs(10),
            ToInstance::read(&mut conn, MAX_PAGES),
        )
        .await
        {
            Ok(Ok(frame)) => frame,
            // The manager ended the transfer: it says why on the session.
            _ => break,
        };
        let ToInstance::Grant(shape) = frame else {
            panic!("a put's channel carries grants, not {frame:?}");
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
        ToManager::Filled {
            index: shape.index,
            row_xxh3: row_hash,
            page_xxh3: hashes,
        }
        .write(&mut conn)
        .await
        .unwrap();
        conn.write_all(&bytes[start..start + length]).await.unwrap();
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
    enum Next {
        Stored(v2::Stored),
        Refused(v2::PutRefused),
    }
    let next = session
        .take(|m| match m {
            M::Stored(stored) if stored.put_id == put_id => Some(Next::Stored(*stored)),
            M::PutRefused(refused) if refused.put_id == put_id => {
                Some(Next::Refused(refused.clone()))
            }
            _ => None,
        })
        .await;
    match next {
        Next::Stored(stored) => PutResult::Stored(stored.bytes),
        Next::Refused(refused) => PutResult::Refused(refused),
    }
}
