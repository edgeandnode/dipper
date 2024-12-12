use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thegraph_core::{alloy::primitives::ChainId, DeploymentId};

/// The queue worker message enum.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    /// Process a new indexing request.
    ///
    /// Given an indexing request, run the IISA and get a list of indexers that
    /// can index the subgraph deployment.
    ///
    /// See [`ProcessNewIndexingRequest`] for more details.
    ProcessNewIndexingRequest(ProcessNewIndexingRequest),

    /// Process indexing request cancellation.
    ///
    /// When a customer cancels an indexing request, this message is sent to the
    /// queue worker to notify it that the indexing request has been cancelled.
    ///
    /// This should trigger the queue worker to cancel any associated indexing
    /// agreements and notify the indexers.
    ///
    /// See [`ProcessIndexingRequestCancellation`] for more details.
    ProcessIndexingRequestCancellation(ProcessIndexingRequestCancellation),

    /// Find a new indexer to fulfill an indexing request.
    ///
    /// When an indexer cancels an indexing agreement, a new indexer must be selected
    /// to fulfill the indexing request.
    FindIndexerForIndexingRequest(FindIndexerForIndexingRequest),

    /// Send an indexing agreement proposal to the indexer.
    ///
    /// See [`SendIndexingAgreementProposal`] for more details.
    SendIndexingAgreementProposal(SendIndexingAgreementProposal),

    /// Send an indexing agreement cancellation to the indexer.
    ///
    /// See [`SendIndexingAgreementCancellation`] for more details.
    SendIndexingAgreementCancellation(SendIndexingAgreementCancellation),

    /// Process indexing agreement cancellation triggered by the indexer.
    ///
    /// When an indexer cancels an indexing agreement, a new indexer must be selected
    /// to fulfill the indexing request.
    ///
    /// See [`ProcessIndexingAgreementCancellation`] for more details.
    ProcessIndexingAgreementIndexerCancellation(ProcessIndexingAgreementCancellation),

    /// Process an indexing agreement cancellation triggered by the customer.
    ///
    /// When a customer cancels an indexing agreement, the queue worker must notify
    /// the indexer that the agreement has been cancelled. Additionally, a new indexer
    /// must be selected to fulfill the indexing request.
    ///
    /// See [`ProcessIndexingAgreementCancellation`] for more details.
    ProcessIndexingAgreementRequesterCancellation(ProcessIndexingAgreementCancellation),
}

/// Given a new indexing request, run the IISA and get a list of indexers that
/// can index the subgraph deployment.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessNewIndexingRequest {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the subgraph deployment
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph deployment
    pub deployment_chain_id: ChainId,
    /// The maximum number of indexers to select
    pub num_candidates: usize,
}

/// Process indexing request cancellation.
///
/// This message is sent to the queue worker to notify it that an indexing request
/// has been cancelled. This should trigger the queue worker to cancel any ongoing
/// indexing agreement proposals.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessIndexingRequestCancellation {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
}

/// Find a new indexer to fulfill an indexing request.
///
/// When an indexer cancels an indexing agreement, a new indexer must be selected
/// to fulfill the indexing request.
#[derive(Debug, Serialize, Deserialize)]
pub struct FindIndexerForIndexingRequest {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the subgraph deployment
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph deployment
    pub deployment_chain_id: ChainId,
}

/// Send an indexing agreement proposal to the indexer.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct SendIndexingAgreementProposal {
    #[serde_as(as = "DisplayFromStr")]
    pub indexer_url: Url,

    pub agreement_id: IndexingAgreementId,
    pub indexing_request_id: IndexingRequestId,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
}

/// Send an indexing agreement cancellation to the indexer.
///
/// This message is sent to the indexers to notify them that an indexing agreement
/// has been cancelled.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct SendIndexingAgreementCancellation {
    #[serde_as(as = "DisplayFromStr")]
    pub indexer_url: Url,
    pub agreement_id: IndexingAgreementId,
}

/// Process indexing agreement cancellation.
///
/// When a requester (or indexer) cancels an indexing agreement, a new indexer must be selected
/// to fulfill the indexing request.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessIndexingAgreementCancellation {
    /// The ID of the indexing agreement.
    pub agreement_id: IndexingAgreementId,
}
