use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "bdb8f8a260d5d2f08fbeabffd79d806e7f9b783196cbcf07033af3490871a2db";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/selene.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
