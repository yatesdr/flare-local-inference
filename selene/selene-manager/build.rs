fn main() {
    println!("cargo:rerun-if-changed=proto/selene.proto");
    println!("cargo:rerun-if-changed=proto/store.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/selene.proto"], &["proto"])
        .expect("compile the selene.v2 protobuf schema");
    // The store's own vocabulary: messages only.
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/store.proto"], &["proto"])
        .expect("compile the store protobuf schema");
}
