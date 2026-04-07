//! Cancel a rejected agreement that was accepted on-chain
//!
//! When an indexer rejects an agreement off-chain but later accepts it on-chain,
//! the chain listener detects this and queues this job to cancel the agreement
//! via `cancelIndexingAgreementByPayer` on the SubgraphService contract.

use std::time::Duration;

use dipper_core::ids::IndexingAgreementId;

use crate::{
    chain_client::ChainClient,
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobMeta, JobResult},
};

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
}

/// Cancel a rejected agreement on-chain.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub agreement_id: IndexingAgreementId,
}

/// Cancel a rejected agreement on-chain.
///
/// This is called when an indexer rejected the proposal off-chain but then accepted
/// on-chain anyway. We cancel the agreement via `cancelIndexingAgreementByPayer` to
/// ensure the indexer doesn't receive payment for work we didn't want.
pub async fn handle<R, T>(
    ctx: Ctx<R, T>,
    Message { agreement_id }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: AgreementRegistry,
    T: ChainClient,
{
    // Look up the agreement
    let agreement = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    let agreement = match agreement {
        Some(a) => a,
        None => {
            tracing::error!(
                agreement_id = %agreement_id,
                "Agreement not found for on-chain cancellation"
            );
            return Ok(());
        }
    };

    // Verify the agreement is in Rejected status (off-chain rejection that got accepted on-chain)
    // The chain listener should only queue this job for Rejected agreements
    if agreement.status != IndexingAgreementStatus::Rejected {
        tracing::warn!(
            agreement_id = %agreement_id,
            status = %agreement.status,
            "Agreement not in Rejected status, skipping on-chain cancellation"
        );
        return Ok(());
    }

    tracing::info!(
        agreement_id = %agreement_id,
        indexer_id = %agreement.indexer.id,
        "Canceling rejected agreement on-chain"
    );

    // Send the cancellation transaction
    match ctx
        .chain_client
        .cancel_indexing_agreement_by_payer(agreement.id.as_bytes())
        .await
    {
        Ok(tx_hash) => {
            tracing::info!(
                agreement_id = %agreement_id,
                tx_hash = %tx_hash,
                "Successfully submitted on-chain cancellation"
            );

            // Update the agreement status to CanceledByRequester
            // (we're the payer/requester canceling it)
            if let Err(err) = ctx
                .registry
                .mark_indexing_agreement_as_canceled_by_requester(agreement_id)
                .await
            {
                tracing::error!(
                    agreement_id = %agreement_id,
                    error = %err,
                    "Failed to update agreement status after on-chain cancellation"
                );
                // Don't fail the job - the on-chain tx succeeded, status update can be retried
            } else {
                tracing::info!(
                    agreement_id = %agreement_id,
                    old_status = "REJECTED",
                    new_status = "CANCELED_BY_REQUESTER",
                    reason = "canceled_on_chain_after_rejection",
                    "agreement state transition"
                );
            }

            Ok(())
        }
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to cancel agreement on-chain, will retry"
            );
            // Retry with backoff - on-chain transactions can fail due to gas issues, nonce, etc.
            Err(JobError::Retryable(err.into(), Duration::from_secs(30)))
        }
    }
}
