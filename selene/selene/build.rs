fn main() {
    let proto = "../selene-manager/proto/selene.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto], &["../selene-manager/proto"])
        .expect("compile the selene pool protocol");
}
