fn main() {
    tonic_build::configure()
        .include_file("mod.rs")
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src/indexer/gen")
        .compile_protos(&["proto/dips.proto"], &["proto"])
        .expect("Failed to compile dips proto(s)");

    // Instruct cargo to re-run the build script if the proto files change
    println!("cargo:rerun-if-changed=proto");
}
