#![allow(clippy::large_enum_variant)]

//! The store's internal vocabulary lives at the top of this module; the pool protocol, the
//! only protocol the manager speaks, is [`v2`].

tonic::include_proto!("selene.store");

/// The pool protocol, `selene.v2`.
pub use flare_selene::pool::wire as v2;
