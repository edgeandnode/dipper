//! A service providing information about the indexers in the network.

pub mod api;
pub mod provider;
pub mod service;
mod subgraph;

#[allow(unused_imports)] // TODO: Remove this once the module is used
pub use subgraph::{
    client::Client as SubgraphClient,
    snapshot::{Deployment, Indexer, Snapshot, Subgraph, SubgraphVersion},
};
