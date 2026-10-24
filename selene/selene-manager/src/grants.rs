//! Live window table and data-socket registry shared by the peer service and the stream
//! receiver.
//!
//! A manager-to-manager Put or Get is granted through `StoreCommand`, but the bytes move on the
//! data channel, which must know the exact window the store handed out and which session and
//! direction it belongs to. The peer service records every grant as it forwards it and removes
//! it when the window ends. A disconnect stops that session's data sockets first, so no copy is
//! still running when the store reclaims its windows.

use crate::data::{GrantKey, GrantTable, Region};
use std::collections::{HashMap, HashSet};
use std::net::{Shutdown, TcpStream};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrantWindow {
    pub session: u64,
    pub put: bool,
    pub region: Region,
    pub offset: u64,
    pub length: u64,
}

/// Session and direction are part of the key: two peers may use the same transfer ids.
type WindowKey = (u64, bool, GrantKey);

#[derive(Default)]
pub struct SharedGrants {
    windows: Mutex<HashMap<WindowKey, GrantWindow>>,
}

impl SharedGrants {
    pub fn insert(&self, key: GrantKey, window: GrantWindow) {
        self.windows
            .lock()
            .unwrap()
            .insert((window.session, window.put, key), window);
    }

    pub fn remove(&self, session: u64, put: bool, key: GrantKey) {
        self.windows.lock().unwrap().remove(&(session, put, key));
    }

    pub fn remove_transfer(&self, session: u64, transfer: u64) {
        self.windows
            .lock()
            .unwrap()
            .retain(|(owned, _, key), _| !(*owned == session && key.transfer == transfer));
    }

    pub fn len(&self) -> usize {
        self.windows.lock().unwrap().len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl GrantTable for SharedGrants {
    fn window(&self, session: u64, key: GrantKey, put: bool) -> Option<(Region, u64, u64)> {
        self.windows
            .lock()
            .unwrap()
            .get(&(session, put, key))
            .map(|window| (window.region, window.offset, window.length))
    }
}

#[derive(Default)]
struct Active {
    next: u64,
    sockets: HashMap<u64, TcpStream>,
}

/// Per-peer data sessions. The data listener accepts many sockets; each carries a token the
/// peer service minted for one control session.
#[derive(Default)]
struct State {
    tokens: HashMap<[u8; 32], u64>,
    active: HashMap<u64, Active>,
    stopped: HashSet<u64>,
}

#[derive(Default)]
pub struct Sessions {
    state: Mutex<State>,
}

impl Sessions {
    pub fn register(&self, token: [u8; 32], connection: u64) {
        let mut state = self.state.lock().unwrap();
        state.tokens.insert(token, connection);
        state.stopped.remove(&connection);
    }

    /// Drop every token of a closed session.
    pub fn forget_session(&self, connection: u64) {
        let mut state = self.state.lock().unwrap();
        state.tokens.retain(|_, session| *session != connection);
        state.active.remove(&connection);
    }

    pub fn connection(&self, token: &[u8; 32]) -> Option<u64> {
        self.state.lock().unwrap().tokens.get(token).copied()
    }

    /// Record an accepted data socket. Stopping and attaching are one atomic check, so no
    /// socket can attach after its session has been stopped.
    pub fn attach(&self, connection: u64, stream: &TcpStream) -> Option<u64> {
        let clone = stream.try_clone().ok()?;
        let mut state = self.state.lock().unwrap();
        if state.stopped.contains(&connection) {
            return None;
        }
        let active = state.active.entry(connection).or_default();
        active.next = active.next.wrapping_add(1);
        let id = active.next;
        active.sockets.insert(id, clone);
        Some(id)
    }

    pub fn detach(&self, connection: u64, id: u64) {
        let mut state = self.state.lock().unwrap();
        if let Some(active) = state.active.get_mut(&connection) {
            active.sockets.remove(&id);
            if active.sockets.is_empty() {
                state.active.remove(&connection);
            }
        }
    }

    /// Stop accepting data for a session and shut down its sockets.
    pub fn stop(&self, connection: u64) {
        let mut state = self.state.lock().unwrap();
        state.stopped.insert(connection);
        if let Some(active) = state.active.get(&connection) {
            for socket in active.sockets.values() {
                let _ = socket.shutdown(Shutdown::Both);
            }
        }
    }

    /// Wait until every data handler for the session has left its copy path. The sockets were
    /// shut down first, so this terminates once the bounded copies finish; a caller that cannot
    /// observe completion must not release the session's windows.
    pub fn drain(&self, connection: u64) {
        loop {
            let active = self
                .state
                .lock()
                .unwrap()
                .active
                .get(&connection)
                .map_or(0, |state| state.sockets.len());
            if active == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_are_scoped_by_session_and_direction() {
        let grants = SharedGrants::default();
        let key = GrantKey {
            transfer: 4,
            index: 1,
        };
        grants.insert(
            key,
            GrantWindow {
                session: 1,
                put: true,
                region: Region::Staging,
                offset: 8192,
                length: 4096,
            },
        );
        assert_eq!(
            grants.window(1, key, true),
            Some((Region::Staging, 8192, 4096))
        );
        assert_eq!(grants.window(1, key, false), None);
        assert_eq!(grants.window(2, key, true), None);
        grants.remove(1, true, key);
        assert_eq!(grants.window(1, key, true), None);
    }

    #[test]
    fn two_sessions_may_hold_the_same_transfer_id() {
        let grants = SharedGrants::default();
        let key = GrantKey {
            transfer: 9,
            index: 0,
        };
        grants.insert(
            key,
            GrantWindow {
                session: 1,
                put: false,
                region: Region::Arena,
                offset: 4096,
                length: 4096,
            },
        );
        grants.insert(
            key,
            GrantWindow {
                session: 2,
                put: false,
                region: Region::Arena,
                offset: 8192,
                length: 4096,
            },
        );
        assert_eq!(
            grants.window(1, key, false),
            Some((Region::Arena, 4096, 4096))
        );
        assert_eq!(
            grants.window(2, key, false),
            Some((Region::Arena, 8192, 4096))
        );
        grants.remove_transfer(1, 9);
        assert_eq!(grants.window(1, key, false), None);
        assert_eq!(
            grants.window(2, key, false),
            Some((Region::Arena, 8192, 4096))
        );
    }

    #[test]
    fn stopping_a_session_shuts_down_attached_sockets() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).unwrap();
        let (server, _) = listener.accept().unwrap();
        let sessions = Sessions::default();
        sessions.register([1; 32], 9);
        assert_eq!(sessions.connection(&[1; 32]), Some(9));
        let attachment = sessions.attach(9, &server).unwrap();
        sessions.stop(9);
        assert!(sessions.attach(9, &server).is_none());
        sessions.detach(9, attachment);
        sessions.drain(9);
        sessions.forget_session(9);
        assert_eq!(sessions.connection(&[1; 32]), None);
        let _ = client;
    }

    #[test]
    fn a_transfer_removes_only_its_session_windows() {
        let grants = SharedGrants::default();
        for (session, index) in [(1, 0), (1, 1), (2, 0)] {
            grants.insert(
                GrantKey { transfer: 7, index },
                GrantWindow {
                    session,
                    put: false,
                    region: Region::Arena,
                    offset: u64::from(index) * 4096,
                    length: 4096,
                },
            );
        }
        grants.remove_transfer(1, 7);
        assert_eq!(grants.len(), 1);
        assert!(
            grants
                .window(
                    2,
                    GrantKey {
                        transfer: 7,
                        index: 0
                    },
                    false
                )
                .is_some()
        );
    }
}
