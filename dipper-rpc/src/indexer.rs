//! DIPs gRPC API for the DIPs Gateway.
//!
//! This module contains the generated code to implement the DIPs Gateway's gRPC API:
//! - [`gateway_server`]: The tonic gRPC service implementation.
//! - [`indexer_client`]: The indexer's DIPs gRPC client.

pub mod gateway_server;
pub mod indexer_client;
