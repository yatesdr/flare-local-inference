use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "7b445c02e52ef634cdbc3c50a2afaca8e57bed3d1f593f5f61539ef428d2c43d";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/selene.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
