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

/// The `CandidateSelection` trait defines the interface for the Indexer Selection Algorithm
/// service, which is responsible for selecting indexers from a provided list of candidates.
#[async_trait]
pub trait CandidateSelection {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Select one indexer from the given list of candidates.
    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
    ) -> Result<Option<Indexer>, Self::Error>;

    /// Selects the best `num_candidates` indexers from the given list of candidates.
    async fn select(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        num_candidates: usize,
    ) -> Result<Vec<Indexer>, Self::Error>;
}
