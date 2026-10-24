fn main() {
    println!("cargo:rerun-if-changed=proto/selene.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/selene.proto"], &["proto"])
        .expect("compile selene protobuf schema");
}
