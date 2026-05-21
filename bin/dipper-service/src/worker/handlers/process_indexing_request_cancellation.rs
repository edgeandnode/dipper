use dipper_core::ids::IndexingRequestId;

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

/// Process indexing request cancellation.
///
/// This message is sent to the queue worker to notify it that an indexing request
/// has been cancelled. This should trigger the queue worker to cancel any ongoing
/// indexing agreement proposals.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
}

pub async fn handle<R, W>(
    ctx: Ctx<R, W>,
    Message {
        indexing_request_id,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
{
    // Get the indexing agreements associated with the indexing request
    let agreements = ctx
        .registry
        .get_active_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        agreements=?agreements.iter().map(|agreement| agreement.id.to_string()).collect::<Vec<_>>(),
        "Processing indexing request cancellation"
    );

    for agreement in &agreements {
        // Mark the agreement as canceled by the requester
        ctx.registry
            .mark_indexing_agreement_as_canceled_by_requester(&agreement.id)
            .await
            .map_err(|err| {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement.id,
                    error=?err, "Failed to mark indexing agreement as CANCELED_BY_REQUESTER"
                );
                JobError::Fatal(err.into())
            })?;
    }

    // TODO(PR 2): trigger on-chain cancel_indexing_agreement_by_payer for each
    // agreement here. PR 1b only removes the dead-letter gRPC notification that
    // used to fire next, leaving the on-chain state untouched (same as today).

    Ok(())
}
