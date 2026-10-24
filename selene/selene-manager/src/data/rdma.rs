//! RDMA data channel (experimental).
//!
//! This module implements the setup payload exchanged in `PeerHello` and the transport-selection
//! rules. The ibverbs queue-pair layer lands in M6a and needs a RoCE or soft-RoCE host; it must
//! register each region file and use one-sided `IBV_WR_RDMA_WRITE` / `IBV_WR_RDMA_READ` with the
//! keys below. No part of this module is used by Flare.

use super::{
    DataChannel, GrantKey, PIECE_BYTES, Region, checked_range, invalid, piece_len, pieces,
};
use std::collections::HashMap;
use std::io;

/// Fixed handshake length: version, qpn, psn, gid, lid, both rkeys and both base addresses.
pub const HANDSHAKE_BYTES: usize = 4 + 4 + 4 + 16 + 2 + 4 + 4 + 8 + 8;
pub const HANDSHAKE_VERSION: u32 = 1;

/// What a peer advertised during `PeerHello`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub qpn: u32,
    pub psn: u32,
    pub gid: [u8; 16],
    pub lid: u16,
    pub rkey_arena: u32,
    pub rkey_staging: u32,
    pub arena_base: u64,
    pub staging_base: u64,
}

impl Handshake {
    pub fn encode(&self) -> [u8; HANDSHAKE_BYTES] {
        let mut out = [0u8; HANDSHAKE_BYTES];
        let mut offset = 0;
        out[offset..offset + 4].copy_from_slice(&HANDSHAKE_VERSION.to_le_bytes());
        offset += 4;
        out[offset..offset + 4].copy_from_slice(&self.qpn.to_le_bytes());
        offset += 4;
        out[offset..offset + 4].copy_from_slice(&self.psn.to_le_bytes());
        offset += 4;
        out[offset..offset + 16].copy_from_slice(&self.gid);
        offset += 16;
        out[offset..offset + 2].copy_from_slice(&self.lid.to_le_bytes());
        offset += 2;
        out[offset..offset + 4].copy_from_slice(&self.rkey_arena.to_le_bytes());
        offset += 4;
        out[offset..offset + 4].copy_from_slice(&self.rkey_staging.to_le_bytes());
        offset += 4;
        out[offset..offset + 8].copy_from_slice(&self.arena_base.to_le_bytes());
        offset += 8;
        out[offset..offset + 8].copy_from_slice(&self.staging_base.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != HANDSHAKE_BYTES {
            return Err(invalid("RDMA handshake has the wrong length"));
        }
        let version = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if version != HANDSHAKE_VERSION {
            return Err(invalid("unsupported RDMA handshake version"));
        }
        let mut gid = [0u8; 16];
        gid.copy_from_slice(&bytes[12..28]);
        Ok(Self {
            qpn: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            psn: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            gid,
            lid: u16::from_le_bytes(bytes[28..30].try_into().unwrap()),
            rkey_arena: u32::from_le_bytes(bytes[30..34].try_into().unwrap()),
            rkey_staging: u32::from_le_bytes(bytes[34..38].try_into().unwrap()),
            arena_base: u64::from_le_bytes(bytes[38..46].try_into().unwrap()),
            staging_base: u64::from_le_bytes(bytes[46..54].try_into().unwrap()),
        })
    }

    /// The remote key for a region, or `None` when the peer did not register it.
    pub fn rkey(&self, region: Region) -> Option<u32> {
        match region {
            Region::Arena if self.rkey_arena != 0 => Some(self.rkey_arena),
            Region::Staging if self.rkey_staging != 0 => Some(self.rkey_staging),
            _ => None,
        }
    }

    pub fn base(&self, region: Region) -> u64 {
        match region {
            Region::Arena => self.arena_base,
            Region::Staging => self.staging_base,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    Stream,
    Rdma,
}

/// Pick the peer transport. `--peer-transport rdma` without the feature is an error; an RDMA
/// handshake that fails after selection falls back to stream with one log line.
pub fn select(configured: &str, built: bool, handshake_ok: bool) -> io::Result<Transport> {
    match configured {
        "stream" => Ok(Transport::Stream),
        "rdma" => {
            if !built {
                return Err(invalid(
                    "--peer-transport rdma requires a build with the `rdma` feature",
                ));
            }
            Ok(if handshake_ok {
                Transport::Rdma
            } else {
                Transport::Stream
            })
        }
        "auto" => Ok(if built && handshake_ok {
            Transport::Rdma
        } else {
            Transport::Stream
        }),
        other => Err(invalid(format!("invalid --peer-transport {other:?}"))),
    }
}

/// One posted operation, as the verbs layer sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Remote {
    pub address: u64,
    pub rkey: u32,
}

/// The mockable verbs layer.
///
/// Completion contract: a completion for a write means the sender's queue pair observed the
/// operation finish, and on RC that means the bytes were placed in the peer's registered memory.
/// `RdmaChannel` reports a window filled only once every one of its pieces has a completion, so
/// the peer client cannot send `WindowFilled` before the receiver's bytes are visible.
///
/// Plain `RDMA_WRITE` produces no completion on the responder's queue pair, so the receiver's
/// acknowledgment is the control-path one: the peer client sends `WindowFilled` only after these
/// local completions, and the window is not reusable (its grant is withdrawn) until the store
/// has accepted that message. A `WRITE_WITH_IMM` variant would add a responder CQ event but
/// would not replace that acknowledgment. Reads need no such distinction: their payload is
/// local.
pub trait Verbs: Send {
    fn memory_key(&self, region: Region) -> io::Result<u32>;
    /// The registered length of a region; bounds are always taken from registration.
    fn local_size(&self, region: Region) -> u64;
    fn post_write(
        &mut self,
        transfer: u64,
        id: u64,
        local_key: u32,
        local_offset: u64,
        len: usize,
        remote: Remote,
    ) -> io::Result<()>;
    fn post_read(
        &mut self,
        transfer: u64,
        id: u64,
        local_key: u32,
        local_offset: u64,
        len: usize,
        remote: Remote,
    ) -> io::Result<()>;
    fn poll(&mut self, done: &mut Vec<(u64, io::Result<()>)>) -> io::Result<()>;
    /// Drain every completion for a transfer before its window is reused. Completions for other
    /// transfers are appended to `drained`; completions for `transfer` are discarded.
    fn quiesce(
        &mut self,
        transfer: u64,
        drained: &mut Vec<(u64, io::Result<()>)>,
    ) -> io::Result<()>;
    /// Fail the queue pair and drain it. Called only when quiescing failed, so no DMA may still
    /// target a window that the caller is about to release.
    fn abort(&mut self);
}

const MAX_OUTSTANDING: usize = 16;

struct Window {
    pieces: u32,
    completed: u32,
    done: Vec<bool>,
}

/// The RDMA data channel. The verbs implementation supplies the queue pair; this type owns the
/// piece split, the outstanding cap, completion mapping and drain-before-reuse rules.
///
/// A window is reported done only after a local completion for every piece. That is placement,
/// not ownership: the peer client then sends `WindowFilled` and waits for the store's acceptance
/// before treating the window as reusable and withdrawing its grant.
pub struct RdmaChannel<V: Verbs> {
    verbs: V,
    peer: Handshake,
    local_keys: (u32, u32),
    peer_sizes: (u64, u64),
    windows: HashMap<GrantKey, Window>,
    ids: HashMap<u64, (GrantKey, u32)>,
    next_id: u64,
    done: Vec<(GrantKey, io::Result<()>)>,
    closed: bool,
    failed: bool,
}

impl<V: Verbs> RdmaChannel<V> {
    pub fn new(verbs: V, peer: Handshake, peer_sizes: (u64, u64)) -> io::Result<Self> {
        let local_keys = (
            verbs.memory_key(Region::Arena)?,
            verbs.memory_key(Region::Staging)?,
        );
        Ok(Self {
            verbs,
            peer,
            local_keys,
            peer_sizes,
            windows: HashMap::new(),
            ids: HashMap::new(),
            next_id: 1,
            done: Vec::new(),
            closed: false,
            failed: false,
        })
    }

    fn local_size(&self, region: Region) -> u64 {
        self.verbs.local_size(region)
    }

    fn peer_size(&self, region: Region) -> u64 {
        match region {
            Region::Arena => self.peer_sizes.0,
            Region::Staging => self.peer_sizes.1,
        }
    }

    fn local_key(&self, region: Region) -> u32 {
        match region {
            Region::Arena => self.local_keys.0,
            Region::Staging => self.local_keys.1,
        }
    }

    fn begin(&mut self, key: GrantKey, len: u64) -> io::Result<u32> {
        let count = pieces(len);
        if count == 0 {
            return Err(invalid("zero-length transfer"));
        }
        if self.windows.contains_key(&key) {
            return Err(invalid("duplicate transfer window"));
        }
        self.windows.insert(
            key,
            Window {
                pieces: count,
                completed: 0,
                done: vec![false; count as usize],
            },
        );
        Ok(count)
    }

    /// Wait until the send queue has room for one more work request. Large windows are posted in
    /// batches; a window larger than the queue is never rejected just for its size.
    fn make_room(&mut self) -> io::Result<()> {
        if self.ids.len() < MAX_OUTSTANDING {
            return Ok(());
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let mut drained = Vec::new();
            self.verbs.poll(&mut drained)?;
            for (id, result) in drained {
                self.complete(id, result);
            }
            if self.ids.len() < MAX_OUTSTANDING {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "RDMA send queue did not drain",
                ));
            }
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    }

    /// Report a failed window. A successful quiesce proves that no work remains; a failed one
    /// aborts the queue pair so nothing can write into a window the caller may now release.
    fn fail(&mut self, key: GrantKey, error: io::Error) {
        let mut drained = Vec::new();
        match self.verbs.quiesce(key.transfer, &mut drained) {
            Ok(()) => {
                self.done.push((key, Err(error)));
                for (id, result) in drained {
                    self.complete(id, result);
                }
            }
            Err(quiesce) => {
                self.verbs.abort();
                self.failed = true;
                self.ids.clear();
                self.windows.clear();
                self.done.push((
                    key,
                    Err(io::Error::other(format!(
                        "RDMA window failed ({error}) and quiescing failed ({quiesce}); channel aborted"
                    ))),
                ));
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn post(
        &mut self,
        key: GrantKey,
        piece: u32,
        read: bool,
        local_region: Region,
        local_offset: u64,
        len: usize,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()> {
        let Some(rkey) = self.peer.rkey(remote_region) else {
            return Err(invalid("peer did not register that region"));
        };
        let remote = Remote {
            address: self
                .peer
                .base(remote_region)
                .checked_add(remote_offset)
                .ok_or_else(|| invalid("remote address overflow"))?,
            rkey,
        };
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| invalid("RDMA operation ids exhausted"))?;
        let local_key = self.local_key(local_region);
        if read {
            self.verbs
                .post_read(key.transfer, id, local_key, local_offset, len, remote)?;
        } else {
            self.verbs
                .post_write(key.transfer, id, local_key, local_offset, len, remote)?;
        }
        self.ids.insert(id, (key, piece));
        Ok(())
    }

    fn complete(&mut self, id: u64, result: io::Result<()>) {
        let Some((key, piece)) = self.ids.remove(&id) else {
            return;
        };
        let Some(window) = self.windows.get_mut(&key) else {
            return;
        };
        match result {
            Ok(()) => {
                if !window.done[piece as usize] {
                    window.done[piece as usize] = true;
                    window.completed += 1;
                }
                if window.completed == window.pieces {
                    self.windows.remove(&key);
                    self.done.push((key, Ok(())));
                }
            }
            Err(error) => {
                self.windows.remove(&key);
                self.fail(key, error);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        &mut self,
        read: bool,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "data channel closed",
            ));
        }
        if self.failed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "RDMA channel aborted after a failed quiesce",
            ));
        }
        if len == 0
            || !len.is_multiple_of(4096)
            || !offset.is_multiple_of(4096)
            || !remote_offset.is_multiple_of(4096)
        {
            return Err(invalid("RDMA range is not 4096-aligned"));
        }
        if checked_range(offset, len as usize, self.local_size(region)).is_none() {
            return Err(invalid("local range is outside the region"));
        }
        if checked_range(remote_offset, len as usize, self.peer_size(remote_region)).is_none() {
            return Err(invalid("remote range is outside the peer region"));
        }
        let key = GrantKey { transfer, index };
        let count = self.begin(key, len)?;
        for piece in 0..count {
            if let Err(error) = self.make_room() {
                self.windows.remove(&key);
                self.fail(key, error);
                return Ok(());
            }
            if self.failed {
                self.windows.remove(&key);
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "RDMA channel aborted after a failed quiesce",
                ));
            }
            let length = piece_len(len, piece);
            let delta = u64::from(piece) * PIECE_BYTES as u64;
            let local = offset
                .checked_add(delta)
                .ok_or_else(|| invalid("local offset overflow"))?;
            let remote = remote_offset
                .checked_add(delta)
                .ok_or_else(|| invalid("remote offset overflow"))?;
            if let Err(error) = self.post(
                key,
                piece,
                read,
                region,
                local,
                length,
                remote_region,
                remote,
            ) {
                self.windows.remove(&key);
                self.fail(key, error);
                return Ok(());
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
impl<V: Verbs> DataChannel for RdmaChannel<V> {
    fn write(
        &mut self,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()> {
        self.start(
            false,
            transfer,
            index,
            region,
            offset,
            len,
            remote_region,
            remote_offset,
        )
    }

    fn read(
        &mut self,
        transfer: u64,
        index: u32,
        region: Region,
        offset: u64,
        len: u64,
        remote_region: Region,
        remote_offset: u64,
    ) -> io::Result<()> {
        self.start(
            true,
            transfer,
            index,
            region,
            offset,
            len,
            remote_region,
            remote_offset,
        )
    }

    fn poll(&mut self, done: &mut Vec<(u64, u32, io::Result<()>)>) -> io::Result<()> {
        let mut drained = Vec::new();
        self.verbs.poll(&mut drained)?;
        for (id, result) in drained {
            self.complete(id, result);
        }
        for (key, result) in self.done.drain(..) {
            done.push((key.transfer, key.index, result));
        }
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let keys: Vec<GrantKey> = self.windows.keys().copied().collect();
        for key in keys {
            let mut drained = Vec::new();
            match self.verbs.quiesce(key.transfer, &mut drained) {
                Ok(()) => {
                    self.windows.remove(&key);
                    self.done.push((
                        key,
                        Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "data channel closed",
                        )),
                    ));
                    for (id, result) in drained {
                        self.complete(id, result);
                    }
                }
                Err(_) => {
                    self.verbs.abort();
                    self.failed = true;
                    self.ids.clear();
                    self.windows.clear();
                    self.done.push((
                        key,
                        Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "data channel closed while a transfer could not be quiesced",
                        )),
                    ));
                    break;
                }
            }
        }
        self.ids.clear();
        Ok(())
    }

    fn is_rdma(&self) -> bool {
        true
    }
}

impl<V: Verbs> Drop for RdmaChannel<V> {
    fn drop(&mut self) {
        let transfers: Vec<u64> = self.windows.keys().map(|key| key.transfer).collect();
        for transfer in transfers {
            let mut drained = Vec::new();
            if self.verbs.quiesce(transfer, &mut drained).is_err() {
                self.verbs.abort();
                break;
            }
        }
        self.windows.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handshake() -> Handshake {
        Handshake {
            qpn: 17,
            psn: 0x1234_5678,
            gid: [0xab; 16],
            lid: 0x0102,
            rkey_arena: 0x1111_2222,
            rkey_staging: 0x3333_4444,
            arena_base: 0x7f00_0000_0000,
            staging_base: 0x7f00_1000_0000,
        }
    }

    #[test]
    fn handshake_round_trips() {
        let value = handshake();
        let bytes = value.encode();
        assert_eq!(bytes.len(), HANDSHAKE_BYTES);
        assert_eq!(Handshake::decode(&bytes).unwrap(), value);
    }

    #[test]
    fn handshake_rejects_short_and_unknown_versions() {
        assert!(Handshake::decode(&[0u8; HANDSHAKE_BYTES - 1]).is_err());
        let mut bytes = handshake().encode();
        bytes[..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(Handshake::decode(&bytes).is_err());
    }

    #[test]
    fn region_keys_and_bases_are_available() {
        let value = handshake();
        assert_eq!(value.rkey(Region::Arena), Some(value.rkey_arena));
        assert_eq!(value.rkey(Region::Staging), Some(value.rkey_staging));
        assert_eq!(value.base(Region::Arena), value.arena_base);
        assert_eq!(value.base(Region::Staging), value.staging_base);
        let mut missing = value;
        missing.rkey_arena = 0;
        assert_eq!(missing.rkey(Region::Arena), None);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Post {
        read: bool,
        id: u64,
        local_key: u32,
        local_offset: u64,
        len: usize,
        remote: Remote,
    }

    #[derive(Default)]
    struct MockState {
        posts: Vec<Post>,
        completions: std::collections::VecDeque<(u64, io::Result<()>)>,
        quiesced: Vec<u64>,
        aborted: u32,
        quiesce_fails: bool,
        auto_complete: bool,
    }

    struct MockVerbs {
        state: std::sync::Arc<std::sync::Mutex<MockState>>,
    }

    impl Verbs for MockVerbs {
        fn memory_key(&self, region: Region) -> io::Result<u32> {
            Ok(match region {
                Region::Arena => 1,
                Region::Staging => 2,
            })
        }

        fn local_size(&self, region: Region) -> u64 {
            match region {
                Region::Arena => 64 << 20,
                Region::Staging => 8 << 20,
            }
        }

        fn post_write(
            &mut self,
            _transfer: u64,
            id: u64,
            local_key: u32,
            local_offset: u64,
            len: usize,
            remote: Remote,
        ) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.posts.push(Post {
                read: false,
                id,
                local_key,
                local_offset,
                len,
                remote,
            });
            if state.auto_complete {
                state.completions.push_back((id, Ok(())));
            }
            Ok(())
        }

        fn post_read(
            &mut self,
            _transfer: u64,
            id: u64,
            local_key: u32,
            local_offset: u64,
            len: usize,
            remote: Remote,
        ) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.posts.push(Post {
                read: true,
                id,
                local_key,
                local_offset,
                len,
                remote,
            });
            if state.auto_complete {
                state.completions.push_back((id, Ok(())));
            }
            Ok(())
        }

        fn poll(&mut self, done: &mut Vec<(u64, io::Result<()>)>) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            while let Some(completion) = state.completions.pop_front() {
                done.push(completion);
            }
            Ok(())
        }

        fn quiesce(
            &mut self,
            transfer: u64,
            _drained: &mut Vec<(u64, io::Result<()>)>,
        ) -> io::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.quiesced.push(transfer);
            if state.quiesce_fails {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "mock quiesce timeout",
                ));
            }
            Ok(())
        }

        fn abort(&mut self) {
            self.state.lock().unwrap().aborted += 1;
        }
    }

    fn mock() -> (
        RdmaChannel<MockVerbs>,
        std::sync::Arc<std::sync::Mutex<MockState>>,
    ) {
        let state = std::sync::Arc::new(std::sync::Mutex::new(MockState::default()));
        let verbs = MockVerbs {
            state: state.clone(),
        };
        let peer = Handshake {
            qpn: 3,
            psn: 7,
            gid: [1; 16],
            lid: 1,
            rkey_arena: 11,
            rkey_staging: 12,
            arena_base: 0x1000_0000,
            staging_base: 0x2000_0000,
        };
        let channel = RdmaChannel::new(verbs, peer, (64 << 20, 8 << 20)).unwrap();
        (channel, state)
    }

    fn complete(
        state: &std::sync::Arc<std::sync::Mutex<MockState>>,
        id: u64,
        result: io::Result<()>,
    ) {
        state.lock().unwrap().completions.push_back((id, result));
    }

    #[test]
    fn write_splits_into_pieces_and_maps_remote_offsets() {
        let (mut channel, state) = mock();
        let len = 2 * PIECE_BYTES as u64 + 4096;
        channel
            .write(len, 0, Region::Arena, 0, len, Region::Arena, 4096)
            .unwrap();
        let ids: Vec<u64> = {
            let state = state.lock().unwrap();
            assert_eq!(state.posts.len(), 3);
            assert_eq!(
                state.posts.iter().map(|post| post.len).collect::<Vec<_>>(),
                vec![PIECE_BYTES, PIECE_BYTES, 4096]
            );
            assert_eq!(state.posts[0].local_key, 1);
            assert_eq!(state.posts[0].remote.rkey, 11);
            assert_eq!(state.posts[0].remote.address, 0x1000_0000 + 4096);
            assert_eq!(
                state.posts[1].remote.address,
                0x1000_0000 + 4096 + PIECE_BYTES as u64
            );
            assert!(state.posts.iter().all(|post| !post.read));
            state.posts.iter().map(|post| post.id).collect()
        };
        for id in ids {
            complete(&state, id, Ok(()));
        }
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert_eq!(done.len(), 1);
        assert!(done[0].2.is_ok());
        assert_eq!(done[0].0, len);
    }

    #[test]
    fn large_windows_are_posted_in_batches_instead_of_being_refused() {
        let (mut channel, state) = mock();
        state.lock().unwrap().auto_complete = true;
        let len = (MAX_OUTSTANDING as u64 + 1) * PIECE_BYTES as u64;
        channel
            .write(42, 0, Region::Arena, 0, len, Region::Arena, 0)
            .unwrap();
        assert_eq!(state.lock().unwrap().posts.len(), MAX_OUTSTANDING + 1);
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert_eq!(done.len(), 1);
        assert_eq!((done[0].0, done[0].1), (42, 0));
        assert!(done[0].2.is_ok());
    }

    #[test]
    fn a_window_is_not_filled_until_every_piece_completes() {
        let (mut channel, state) = mock();
        let len = 4 * PIECE_BYTES as u64;
        channel
            .write(7, 0, Region::Arena, 0, len, Region::Arena, 0)
            .unwrap();
        let ids: Vec<u64> = state
            .lock()
            .unwrap()
            .posts
            .iter()
            .map(|post| post.id)
            .collect();
        for id in &ids[..3] {
            complete(&state, *id, Ok(()));
        }
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert!(
            done.is_empty(),
            "a partially completed window must stay open"
        );
        complete(&state, ids[3], Ok(()));
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert_eq!(done.len(), 1);
        assert!(done[0].2.is_ok());
    }

    #[test]
    fn a_failed_quiesce_aborts_the_channel_and_reports_failure() {
        let (mut channel, state) = mock();
        channel
            .write(
                9,
                0,
                Region::Arena,
                0,
                2 * PIECE_BYTES as u64,
                Region::Arena,
                0,
            )
            .unwrap();
        let first = state.lock().unwrap().posts[0].id;
        state.lock().unwrap().quiesce_fails = true;
        complete(&state, first, Err(invalid("rxe")));
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert_eq!(done.len(), 1);
        assert!(done[0].2.is_err());
        assert_eq!(state.lock().unwrap().aborted, 1);
        let error = channel
            .write(10, 0, Region::Arena, 0, 4096, Region::Arena, 0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn a_failure_quiesces_the_transfer_before_reporting() {
        let (mut channel, state) = mock();
        channel
            .write(
                5,
                0,
                Region::Arena,
                0,
                2 * PIECE_BYTES as u64,
                Region::Arena,
                0,
            )
            .unwrap();
        let (first, second) = {
            let state = state.lock().unwrap();
            (state.posts[0].id, state.posts[1].id)
        };
        complete(&state, first, Err(invalid("rxe")));
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert_eq!(done.len(), 1);
        assert!(done[0].2.is_err());
        assert_eq!(state.lock().unwrap().quiesced, vec![5]);
        complete(&state, second, Ok(()));
        let mut done = Vec::new();
        channel.poll(&mut done).unwrap();
        assert!(done.is_empty());
    }

    #[test]
    fn read_posts_the_remote_source_and_local_destination() {
        let (mut channel, state) = mock();
        channel
            .read(6, 1, Region::Arena, 0, 4096, Region::Staging, 8192)
            .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.posts.len(), 1);
        assert!(state.posts[0].read);
        assert_eq!(state.posts[0].local_key, 1);
        assert_eq!(state.posts[0].local_offset, 0);
        assert_eq!(state.posts[0].remote.rkey, 12);
        assert_eq!(state.posts[0].remote.address, 0x2000_0000 + 8192);
    }

    #[test]
    fn misaligned_or_out_of_range_requests_are_rejected() {
        let (mut channel, _state) = mock();
        assert!(
            channel
                .write(1, 0, Region::Arena, 1, 4096, Region::Arena, 0)
                .is_err()
        );
        assert!(
            channel
                .write(1, 0, Region::Arena, 0, 1, Region::Arena, 0)
                .is_err()
        );
        assert!(
            channel
                .write(1, 0, Region::Arena, 64 << 20, 4096, Region::Arena, 0)
                .is_err()
        );
        assert!(
            channel
                .write(1, 0, Region::Arena, 0, 4096, Region::Arena, 64 << 20)
                .is_err()
        );
        assert!(
            channel
                .write(1, 0, Region::Arena, 0, 4096, Region::Staging, 8 << 20)
                .is_err()
        );
    }

    #[test]
    fn transport_selection_follows_the_spec() {
        assert_eq!(select("stream", true, true).unwrap(), Transport::Stream);
        assert_eq!(select("rdma", true, true).unwrap(), Transport::Rdma);
        assert_eq!(select("rdma", true, false).unwrap(), Transport::Stream);
        assert!(select("rdma", false, true).is_err());
        assert_eq!(select("auto", true, true).unwrap(), Transport::Rdma);
        assert_eq!(select("auto", true, false).unwrap(), Transport::Stream);
        assert_eq!(select("auto", false, true).unwrap(), Transport::Stream);
        assert_eq!(select("auto", false, false).unwrap(), Transport::Stream);
        assert!(select("carrier-pigeon", true, true).is_err());
    }
}
