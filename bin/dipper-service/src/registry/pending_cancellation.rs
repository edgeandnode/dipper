//! Pending cancellation registry trait.
//!
//! Pending cancellations track agreements that should be cancelled only after
//! their replacement is confirmed on-chain. This prevents under-allocation
//! during reassessment.

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;

use super::result::Result as RegistryResult;

/// A pending cancellation record linking a new (replacement) agreement
/// to the old agreement it should replace once accepted on-chain.
#[derive(Debug, Clone)]
pub struct PendingCancellation {
    pub old_agreement_id: IndexingAgreementId,
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

    /// List `new_agreement_id`s of pending cancellation rows whose linked
    /// agreement is in `AcceptedOnChain` status. Used by the chain_listener
    /// sweep to recover from a partial-progress crash inside
    /// `execute_pending_cancellations`. Re-running the function on each
    /// returned ID is idempotent. Capped at `limit` rows so a backlog
    /// drains across polls instead of blocking one tick.
    async fn list_executable_pending_cancellations(
        &self,
        limit: i64,
    ) -> RegistryResult<Vec<IndexingAgreementId>>;
}
