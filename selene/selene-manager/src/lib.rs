pub mod arena;
pub mod config;
pub mod control;
pub mod data;
pub mod disk;
pub mod local;
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

pub use flare_selene::PrefixKey as Key;
