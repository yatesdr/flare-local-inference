use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "c3446cd6b6b0f91e8b4fd85710370ec496334750a59dba1f0bf79c90fde7aa5b";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/selene.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
