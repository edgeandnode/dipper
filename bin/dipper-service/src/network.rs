//! A service providing information about the indexers in the network.

pub mod api;
pub mod provider;
pub mod service;
pub mod subgraph;

#[allow(unused_imports)] // TODO: Remove this once the module is used
pub use api::{Indexer, NetworkProvider};
