use reqwest::Url;
use thegraph_core::{alloy::primitives::Address, DeploymentId, IndexerId};

/// An indexer.
pub struct Indexer {
    /// The indexer's ID (Eth address)
    pub id: IndexerId,
    /// The indexer's URL
    pub url: Url,
}

/// The network provider trait.
///
/// Provides a set of methods to interact with the network provider abstracting the
/// access to the Graph network snapshot.
pub trait NetworkProvider {
    /// Get indexer by ID.
    fn get_indexer_by_id(&self, indexer_id: &IndexerId) -> Option<Indexer>;

    /// Get a list of indexers not indexing the subgraph deployment.
    fn get_indexers_not_indexing_a_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Vec<Indexer>;

    /// Get indexer ID for operator address.
    fn get_indexer_id_for_operator_address(&self, operator_address: &Address) -> Option<IndexerId>;
}
