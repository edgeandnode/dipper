# dipper-producer

This crate contains protobuf definitions for dipper indexer agreement event streaming.
The generated Rust bindings are committed to the repository and only need to be regenerated when the `.proto` file changes.

## Protobuf Generation

The build script uses a configuration flag `gen_event_proto` that enables protobuf code generation via `prost-build`. When enabled, the build script compiles `proto/indexing-agreement-events.proto` into Rust types under `src/proto/`.

To regenerate protobuf bindings, run:

```bash
just gen-indexing-agreement-events-proto
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_event_proto" cargo check -p dipper-producer
```

This will regenerate `src/proto/dipper.subgraph.indexing.agreement.events.v1.rs` from `proto/indexing-agreement-events.proto`.
