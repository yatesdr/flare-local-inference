fn main() {
    println!("cargo:rerun-if-changed=proto/store.proto");
    // The store's own vocabulary: messages only.
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/store.proto"], &["proto"])
        .expect("compile the store protobuf schema");
}
