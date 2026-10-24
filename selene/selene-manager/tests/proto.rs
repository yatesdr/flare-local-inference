use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "92523f844df097bd87d00d344443e0ff5564fc21b372e98af9d6583f35fb621d";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/selene.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
