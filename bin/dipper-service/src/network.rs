//! A service providing information about the indexers in the network.

mod api;
pub mod fetch;
pub mod provider;
pub mod service;

#[allow(unused_imports)]
pub use api::{Allocation, Indexer, NetworkProvider};

#[cfg(test)]
mod tests {
    mod it_fetch_subgraph_epoch_data;
    mod it_fetch_subgraph_topology_data;
}
