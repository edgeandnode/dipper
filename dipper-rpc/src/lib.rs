//! # dipper-rpc
//!
//! RPC client and server implementations for the Dipper protocol.
//!
//! ## Features
//!
//! - `admin`: Enables the admin RPC functionality
//! - `indexer`: Enables the indexer RPC functionality

#[cfg(feature = "admin-rpc")]
pub mod admin;

#[cfg(feature = "indexer-rpc")]
pub mod indexer;
