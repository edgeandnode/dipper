use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};

use crate::{
    registry::{AgreementRegistry, IndexingRequestRegistry, PendingCancellationRegistry},
    worker::{
        result::{JobError, JobMeta, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, W> {
    pub registry: R,
    pub queue: W,
}

/// Process indexing agreement cancellation triggered by the requester.
///
/// When a requester cancels an indexing agreement, a new indexer must be selected
/// to fulfill the indexing request.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the indexing agreement
    pub agreement_id: IndexingAgreementId,
}

pub async fn handle_requester_cancellation<R, W>(
    ctx: Ctx<R, W>,
    Message {
        indexing_request_id,
        agreement_id,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry + PendingCancellationRegistry,
    W: WorkerQueue,
{
    tracing::trace!(
        %indexing_request_id,
        %agreement_id,
        "Processing indexing agreement cancellation (CANCELED_BY_REQUESTER)"
    );

    // Check the status of the agreement before processing the cancellation
    let Some(agreement) = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    else {
        tracing::error!(
            %indexing_request_id,
            %agreement_id,
            "Indexing agreement not found"
        );
        return Ok(());
    };

    ctx.registry
        .mark_indexing_agreement_as_canceled_by_requester(agreement_id)
        .await
        .map_err(|err| {
            tracing::error!(
                %indexing_request_id,
                %agreement_id,
                error=?err,
                "Failed to mark indexing agreement as CANCELED_BY_REQUESTER"
            );
            JobError::Fatal(err.into())
        })?;

    tracing::info!(
        agreement_id = %agreement_id,
        indexing_request_id = %indexing_request_id,
        old_status = %agreement.status,
        new_status = "CANCELED_BY_REQUESTER",
        reason = "canceled_by_requester",
        "agreement state transition"
    );

    // Clean up pending cancellations: if this cancelled agreement was a
    // replacement, the old agreement it was replacing should stay active.
    if let Err(err) = ctx
        .registry
        .delete_pending_cancellations_by_new_agreement(agreement.id)
        .await
    {
        tracing::warn!(
            %agreement_id,
            error=%err,
            "Failed to clean up pending cancellations for requester-cancelled agreement"
        );
    }

    // TODO(PR 2): trigger on-chain cancel_indexing_agreement_by_payer here.
    // PR 1b only removes the dead-letter gRPC notification; the on-chain state
    // is still untouched (same as today).

    // Get the indexing request associated with the agreement
    let Some(indexing_request) = ctx
        .registry
        .get_indexing_request_by_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    else {
        tracing::error!(
            %indexing_request_id,
            %agreement_id,
            "Indexing request not found"
        );
        return Ok(());
    };

    // Reassess the indexing request to find replacement indexers
    ctx.queue
        .reassess_indexing_request(
            indexing_request.id,
            indexing_request.deployment_id,
            indexing_request.deployment_chain_id,
            indexing_request.num_candidates,
        )
        .await
        .map_err(|err| {
            tracing::error!(
                %indexing_request_id,
                %agreement_id,
                error=?err,
                "Failed to queue task: 'reassess_indexing_request'"
            );
            JobError::Fatal(err)
        })?;

    Ok(())
}
