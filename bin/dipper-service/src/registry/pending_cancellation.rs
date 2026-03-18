//! Pending cancellation registry trait.
//!
//! Pending cancellations track agreements that should be cancelled only after
//! their replacement is confirmed on-chain. This prevents under-allocation
//! during reassessment.

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::DeploymentId;

use super::result::Result as RegistryResult;

/// A pending cancellation record linking a new (replacement) agreement
/// to the old agreement it should replace once accepted on-chain.
#[derive(Debug, Clone)]
pub struct PendingCancellation {
    pub new_agreement_id: IndexingAgreementId,
    pub old_agreement_id: IndexingAgreementId,
    pub deployment_id: DeploymentId,
    pub indexing_request_id: IndexingRequestId,
}

#[async_trait]
pub trait PendingCancellationRegistry {
    /// Get all pending cancellations linked to a new agreement.
    async fn get_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<Vec<PendingCancellation>>;

    /// Delete all pending cancellation records for a new agreement.
    /// Called when the new agreement fails (old agreements stay active).
    async fn delete_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Delete a single pending cancellation record.
    /// Called after a pending cancellation has been successfully processed
    /// (old agreement cancelled or already in terminal state).
    async fn delete_pending_cancellation(
        &self,
        new_agreement_id: IndexingAgreementId,
        old_agreement_id: IndexingAgreementId,
    ) -> RegistryResult<()>;
}
