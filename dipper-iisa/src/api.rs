use std::collections::HashMap;

use async_trait::async_trait;
use thegraph_core::{DeploymentId, IndexerId};
use url::Url;

/// An indexer
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer ID
    pub id: IndexerId,
    /// The indexer URL
    pub url: Url,
}

/// Context for load balancing during indexer selection.
///
/// This context provides the IISA with information about current system state,
/// enabling intelligent load balancing decisions rather than naive selection.
#[derive(Debug, Clone, Default)]
pub struct SelectionContext {
    /// Indexer IDs that already have active agreements for this deployment.
    ///
    /// Used to avoid selecting indexers that are already working on the same deployment.
    pub existing_indexers: Vec<IndexerId>,

    /// For each deployment, the indexers that have pending/active agreements for it.
    ///
    /// Used to exclude indexers with pending work from new assignments.
    /// Key: Deployment ID, Value: List of indexer IDs working on that deployment.
    pub pending_agreements: HashMap<DeploymentId, Vec<IndexerId>>,

    /// Indexer IDs that should be excluded from selection entirely.
    ///
    /// Used for indexers that have been flagged for poor performance, trust issues,
    /// or other reasons that make them unsuitable for any deployment.
    pub indexer_denylist: Vec<IndexerId>,

    /// For each deployment, indexers that have recently declined agreements.
    ///
    /// Used to avoid re-offering agreements to indexers that recently declined.
    /// Key: Deployment ID, Value: List of indexer IDs that declined.
    pub declined_indexers: HashMap<DeploymentId, Vec<IndexerId>>,
}

/// The `SelectionError` enum represents the errors that can occur during the candidate selection
/// process.
#[derive(Debug, thiserror::Error)]
pub enum SelectionError {
    /// Indexer Selection service is not available.
    ///
    /// An error occurred while sending a request to the IISA service.
    #[error("IISA service is not available")]
    IisaServiceUnavailable,

    /// An error occurred during the selection process.
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

/// The `CandidateSelection` trait defines the interface for the Indexer Selection Algorithm
/// service, which is responsible for selecting indexers from a provided list of candidates.
#[async_trait]
pub trait CandidateSelection {
    /// Select one indexer from the given list of candidates.
    ///
    /// # Arguments
    /// * `deployment_id` - The deployment to select an indexer for
    /// * `candidates` - List of candidate indexers to choose from
    /// * `context` - Load balancing context with existing assignments and pending work
    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        context: &SelectionContext,
    ) -> Result<Option<Indexer>, SelectionError>;

    /// Selects the best `num_candidates` indexers from the given list of candidates.
    ///
    /// # Arguments
    /// * `deployment_id` - The deployment to select indexers for
    /// * `candidates` - List of candidate indexers to choose from
    /// * `num_candidates` - Maximum number of indexers to select
    /// * `context` - Load balancing context with existing assignments and pending work
    async fn select(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        num_candidates: usize,
        context: &SelectionContext,
    ) -> Result<Vec<Indexer>, SelectionError>;
}
