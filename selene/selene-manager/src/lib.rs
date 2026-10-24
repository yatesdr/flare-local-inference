pub mod admin;
pub mod arena;
pub mod codec;
pub mod config;
pub mod control;
pub mod data;
pub mod disk;
pub mod grants;
pub mod local;
pub mod peer;
pub mod peer_client;
pub mod peer_runtime;
pub mod placement;
pub mod pool;
pub mod proto;
pub mod record;
pub mod regions;
pub mod store;
pub mod tree;

use std::io;

pub type Result<T> = io::Result<T>;

pub fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key {
    pub consumed_tokens: u64,
    pub digest: [u8; 16],
}
