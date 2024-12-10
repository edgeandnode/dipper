//! DIPs gRPC API for the DIPs Gateway.
//!
//! This module contains the generated code to implement the DIPs Gateway's gRPC API:
//! - [`rpc::gateway_server`]: The tonic gRPC service implementation.
//! - [`rpc::indexer_client`]: The indexer's DIPs gRPC client.

pub mod rpc {
    /// The DIPs gRPC server for the gateway.
    ///
    /// This module contains the generated code to implement the gateway's DIPs gRPC server.
    pub mod gateway_server {
        include!("indexer/gen/gateway.mod.rs");
    }

    /// The RPC client for the indexer's DIPs gRPC API.
    ///
    /// This module contains the generated code to interact with the indexer's DIPs gRPC server.
    pub mod indexer_client {
        include!("indexer/gen/indexer.mod.rs");
    }
}
