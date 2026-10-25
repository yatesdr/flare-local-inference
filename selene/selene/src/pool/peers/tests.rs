use super::*;
use std::io::Write;

fn offer(rank: usize, enabled: bool) -> PoolOffer {
    PoolOffer {
        tenant: enabled.then(|| "tenant".into()),
        identity: [3; 32],
        layout: [4; 16],
        rank,
        world: 2,
        gpu_uuid: [rank as u8 + 1; 16],
    }
}

#[test]
fn pool_agreement_checks_identity_layout_tenant_world_and_rank_bindings() {
    let zero = offer(0, true);
    let peer = offer(1, true);
    let good = vec![zero.encode(), peer.encode()];
    assert_eq!(
        zero.agreement(&good).unwrap().unwrap(),
        vec![[1; 16], [2; 16]]
    );
    for bad in [
        PoolOffer {
            tenant: None,
            ..peer.clone()
        },
        PoolOffer {
            tenant: Some("other".into()),
            ..peer.clone()
        },
        PoolOffer {
            identity: [9; 32],
            ..peer.clone()
        },
        PoolOffer {
            layout: [9; 16],
            ..peer.clone()
        },
        PoolOffer {
            rank: 0,
            ..peer.clone()
        },
        PoolOffer {
            world: 3,
            ..peer.clone()
        },
    ] {
        assert!(zero.agreement(&[zero.encode(), bad.encode()]).is_err());
        assert!(bad.agreement(&good).is_err());
    }
}

fn connected() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    (client, listener.accept().unwrap().0)
}

fn ranks(enabled: bool) -> (Option<PoolPeers>, Option<PoolPeers>) {
    let (zero, peer) = connected();
    let addresses = vec![(zero.peer_addr().unwrap(), zero.local_addr().unwrap())];
    let records = vec![offer(0, enabled).encode(), offer(1, enabled).encode()];
    let peer_records = records.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        pool_peer(offer(1, enabled), &peer_records, || Ok(rx.recv().unwrap())).unwrap()
    });
    let zero = pool_rank_zero(offer(0, enabled), &records, &addresses, |offers| {
        // The peer sees its own per-rank offer in position zero of the exchange result.
        tx.send(vec![offers[1].clone()]).unwrap();
        Ok(())
    })
    .unwrap();
    drop(peer);
    (zero, thread.join().unwrap())
}

#[test]
fn pool_only_ranks_exchange_whole_instance_identity_and_opaque_frames() {
    let (zero, peer) = ranks(true);
    let (mut zero, mut peer) = (zero.unwrap(), peer.unwrap());
    assert_eq!(zero.gpu_uuids, vec![[1; 16], [2; 16]]);
    assert_eq!(peer.gpu_uuids, zero.gpu_uuids);
    let mut zero = zero.links.remove(0).into_rank_link().unwrap();
    let mut peer = peer.links.remove(0).into_rank_link().unwrap();
    assert!(
        zero.receiver
            .recv(Duration::from_millis(1))
            .unwrap()
            .is_none()
    );
    // A blocked receiver does not lock out the sending half.
    let receiver =
        std::thread::spawn(move || zero.receiver.recv(Duration::from_secs(5)).unwrap().unwrap());
    zero.sender.send(vec![0, 1, 255]).unwrap();
    assert_eq!(
        peer.receiver.recv(Duration::from_secs(5)).unwrap().unwrap(),
        [0, 1, 255]
    );
    peer.sender.send(vec![3; 65537]).unwrap();
    assert_eq!(receiver.join().unwrap(), vec![3; 65537]);
}

#[test]
fn splitting_a_link_preserves_frames_received_with_the_handshake() {
    let (zero, mut peer) = connected();
    let hello = Writer::new(HELLO).finish();
    let mut pool = Writer::new(POOL);
    pool.bytes(&[5, 7, 9]);
    peer.write_all(&[hello, pool.finish()].concat()).unwrap();
    let mut zero = Link::new(zero).unwrap();
    zero.expect(HELLO, Duration::from_secs(5)).unwrap();
    let mut link = zero.into_rank_link().unwrap();
    assert_eq!(
        link.receiver.recv(Duration::from_secs(5)).unwrap().unwrap(),
        [5, 7, 9]
    );
}

#[test]
fn no_pool_creates_no_pool_links() {
    let (zero, peer) = ranks(false);
    assert!(zero.is_none() && peer.is_none());
}
