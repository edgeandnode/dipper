//! A service providing information about the indexers in the network.

pub mod service;
mod subgraph;

pub use subgraph::snapshot::{Deployment, Indexer, Snapshot, Subgraph, SubgraphVersion};
