//! # Indexer client
//!
//! The indexer client is responsible for communicating directly with the indexers.
//!
//! This module defines different traits to interact with the indexers:
//! - [`DipsClient`]: Send DIPs related requests to the indexers.

use std::time::Duration;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::DeploymentId;
use url::Url;

/// The indexer client error type for DIPs endpoint
#[derive(Debug, thiserror::Error)]
pub enum DipsError {}

#[derive(Debug)]
pub enum AgreementProposalResponse {
    Accepted,
    Rejected,
}

impl std::fmt::Display for AgreementProposalResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgreementProposalResponse::Accepted => write!(f, "ACCEPTED"),
            AgreementProposalResponse::Rejected => write!(f, "REJECTED"),
        }
    }
}

/// Indexer client's DIPs trait
#[async_trait]
pub trait DipsClient {
    /// Send an indexing agreement proposal request to the indexer
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        duration: Duration,
    ) -> Result<AgreementProposalResponse, DipsError>;

    /// Send an indexing agreement cancel request to the indexer
    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: Url,
        indexing_agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
    ) -> Result<(), DipsError>;
}
