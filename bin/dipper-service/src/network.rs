//! A service providing information about the indexers in the network.

mod api;
pub mod fetch;
pub mod provider;
pub mod service;

#[allow(unused_imports)]
pub use api::{Allocation, Deployment, Indexer, NetworkProvider};
pub use fetch::snapshot::Snapshot;

#[cfg(test)]
mod tests {
    mod it_fetch_subgraph_data;
}
