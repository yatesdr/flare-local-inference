fn main() {
    println!("cargo:rerun-if-changed=proto/selene.proto");
    println!("cargo:rerun-if-changed=proto/legacy.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/selene.proto", "proto/legacy.proto"], &["proto"])
        .expect("compile selene protobuf schemas");
}
