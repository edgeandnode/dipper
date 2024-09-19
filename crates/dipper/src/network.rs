//! A service providing information about the indexers in the network.

pub mod service;
mod subgraph;

pub use subgraph::{
    client::Client as SubgraphClient,
    snapshot::{Deployment, Indexer, Snapshot, Subgraph, SubgraphVersion},
};
