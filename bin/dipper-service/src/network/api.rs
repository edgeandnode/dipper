use reqwest::Url;
use thegraph_core::{
    alloy::primitives::Address, AllocationId, DeploymentId, IndexerId, ProofOfIndexing, SubgraphId,
};

/// An indexer.
pub struct Indexer {
    /// The indexer's ID (Eth address)
    pub id: IndexerId,
    /// The indexer's URL
    pub url: Url,
}

/// A Subgraph deployment.
pub struct Deployment {}

/// A network allocation.
pub struct Allocation {
    /// The allocation ID
    pub id: AllocationId,
    /// The epoch when the allocation was made
    pub opened_at: u32,
    /// The epoch when the allocation was closed
    pub closed_at: Option<u32>,
    /// The indexer ID
    pub indexer_id: IndexerId,
    /// The deployment ID
    pub deployment_id: DeploymentId,
    /// The subgraph ID
    pub subgraph_id: SubgraphId,
    /// The amount of tokens staked by the indexer for the allocation
    pub allocated_tokens: u128,
    /// The allocation proof of indexing
    pub proof_of_indexing: Option<ProofOfIndexing>,
}

/// The network provider trait.
///
/// Provides a set of methods to interact with the network provider abstracting the
/// access to the Graph network snapshot.
pub trait NetworkProvider {
    /// Get Deployment by ID.
    fn get_deployment_by_id(&self, deployment_id: &DeploymentId) -> Option<Deployment>;

    /// Get allocation by ID.
    fn get_allocation_by_id(&self, allocation_id: &AllocationId) -> Option<Allocation>;

    /// Get a list of indexers not indexing the subgraph deployment.
    fn get_indexers_not_indexing_a_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Vec<Indexer>;

    /// Get indexer ID for operator address.
    fn get_indexer_id_for_operator_address(&self, operator_address: &Address) -> Option<IndexerId>;

    /// Get the latest epoch.
    ///
    /// This is the latest known epoch in the network. If the network snapshot fails to
    /// update, this epoch may be outdated.
    fn get_current_epoch(&self) -> u32;
}
