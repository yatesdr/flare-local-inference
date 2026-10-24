use sha2::{Digest, Sha256};

const PROTO_SHA256: &str = "80805fbcd2cc60f11f8f0538c67be47880d6da3f8a90af28166642420cd22fc9";

#[test]
fn manager_proto_matches_the_pinned_digest() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/selene.proto");
    let bytes = std::fs::read(path).unwrap();
    let digest = Sha256::digest(&bytes);
    assert_eq!(format!("{digest:x}"), PROTO_SHA256);
}
