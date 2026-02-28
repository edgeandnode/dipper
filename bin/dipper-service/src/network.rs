//! A service providing information about the indexers in the network.

mod api;
pub mod fetch;
pub mod provider;
pub mod service;

pub use api::NetworkProvider;

#[cfg(test)]
mod tests {
    mod it_fetch_subgraph_topology_data;
}
