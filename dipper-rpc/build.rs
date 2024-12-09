fn main() {
    // Build the DIPs indexer RPC client
    tonic_build::configure()
        .include_file("indexer.mod.rs")
        .build_client(true)
        .build_server(false)
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src/indexer/gen")
        .compile_protos(&["proto/indexer.proto"], &["proto/"])
        .expect("Failed to compile DIPs indexer RPC proto(s)");

    // Build the DIPs gateway RPC server service
    tonic_build::configure()
        .include_file("gateway.mod.rs")
        .build_client(false)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src/indexer/gen")
        .compile_protos(&["proto/gateway.proto"], &["proto"])
        .expect("Failed to compile DIPs gateway RPC proto(s)");
}
