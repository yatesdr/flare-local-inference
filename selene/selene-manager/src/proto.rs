#![allow(clippy::large_enum_variant)]

tonic::include_proto!("selene.v1");

/// The pool protocol, `selene.v2`.
pub mod v2 {
    tonic::include_proto!("selene.v2");
}
