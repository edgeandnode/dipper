use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};

use crate::{
    registry::{AgreementRegistry, IndexingRequestRegistry},
    worker::{
        result::{JobError, JobMeta, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, W> {
    pub registry: R,
    pub queue: W,
}

/// Process indexing agreement cancellation.
///
/// When a requester (or indexer) cancels an indexing agreement, a new indexer must be selected
/// to fulfill the indexing request.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the indexing agreement
    pub agreement_id: IndexingAgreementId,
}

/// Process indexing agreement cancellation.
pub async fn handle_indexer_cancellation<R, W>(
    ctx: Ctx<R, W>,
    Message {
        indexing_request_id,
        agreement_id,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
{
    tracing::trace!(
        %indexing_request_id,
        %agreement_id,
        "Processing indexing agreement cancellation (CANCELED_BY_INDEXER)"
    );

    // Check the status of the agreement before processing the cancellation
    let Some(agreement) = ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    else {
        tracing::error!(%indexing_request_id, %agreement_id, "Indexing agreement not found");
        return Ok(());
    };

    // Mark the agreement as canceled by the indexer
    ctx.registry
        .mark_indexing_agreement_as_canceled_by_indexer(&agreement.id)
        .await
        .map_err(|err| {
            tracing::error!(
                %indexing_request_id,
                %agreement_id,
                error=?err,
                "Failed to mark indexing agreement as CANCELED_BY_INDEXER"
            );
            JobError::Fatal(err.into())
        })?;

    // Get the indexing request associated with the agreement
    let Some(indexing_request) = ctx
        .registry
        .get_indexing_request_by_id(&agreement.indexing_request_id)
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

pub async fn handle_requester_cancellation<R, W>(
    ctx: Ctx<R, W>,
    Message {
        indexing_request_id,
        agreement_id,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
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

    // Send the indexing agreement cancellation notification to the indexer
    ctx.queue
        .send_indexing_agreement_cancellation(
            agreement.indexer.url,
            *indexing_request_id,
            *agreement_id,
        )
        .await
        .map_err(|err| {
            tracing::error!(
                %indexing_request_id,
                %agreement_id,
                error=?err,
                "Failed to queue task: 'send_indexing_agreement_cancellation'"
            );
            JobError::Fatal(err)
        })?;

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
