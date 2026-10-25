fn main() {
    let proto = "../selene-manager/proto/selene.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed=proto/rank_link.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto], &["../selene-manager/proto"])
        .expect("compile the selene pool protocol");
    // The rank link's messages reuse the pool protocol's types.
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .extern_path(".selene.v2", "crate::pool::wire")
        .compile_protos(
            &["proto/rank_link.proto"],
            &["proto", "../selene-manager/proto"],
        )
        .expect("compile the rank link protocol");
}
