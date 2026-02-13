use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_rpc::indexer::indexer_client::rpc::ProposalResponse;
use serde_with::serde_as;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::{
    config::DEFAULT_MAX_CANDIDATES,
    indexer_rpc_client::IndexerClient,
    registry::{AgreementRegistry, IndexingAgreementStatus, IndexingRequestRegistry},
    worker::{
        result::{JobError, JobMeta, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, W, C> {
    pub registry: R,
    pub queue: W,
    pub indexer_client: C,
}

/// Send an indexing agreement proposal to the indexer.
#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub indexer_url: Url,

    pub agreement_id: IndexingAgreementId,
    pub indexing_request_id: IndexingRequestId,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
}

/// Send an indexing agreement proposal to the indexer.
///
/// This function sends a SignedRCA to the indexer and processes the response:
/// - `Accept`: The indexer received the proposal and may accept on-chain before the deadline.
///   Agreement stays in `Created` until an on-chain acceptance event is observed.
/// - `Reject`: The indexer explicitly rejected the proposal. Agreement is marked as
///   `DeliveryFailed` and the indexing request is reassessed to find replacement indexers.
/// - Network error: Same handling as `Reject` - mark failed and reassess.
pub async fn handle<R, W, C>(
    ctx: Ctx<R, W, C>,
    Message {
        indexer_url,
        agreement_id,
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
    C: IndexerClient,
{
    // TODO: THIS IS A HACK
    let indexer_url = {
        let mut url = indexer_url.clone();
        url.set_port(Some(7602)).unwrap();
        url
    };

    // Check the status of the agreement before sending the proposal
    let agreement = match ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    {
        None => {
            tracing::error!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                "Indexing agreement not found"
            );
            return Ok(());
        }
        Some(agreement) => match agreement.status {
            IndexingAgreementStatus::Created => agreement,
            status => {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    "Not sending agreement proposal. Invalid agreement status: {status}",
                );
                return Ok(());
            }
        },
    };

    tracing::debug!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        deployment_id=%deployment_id,
        indexer_url=%indexer_url,
        "Sending indexing agreement proposal"
    );

    let response = ctx
        .indexer_client
        .send_indexing_agreement_proposal(&indexer_url, *agreement_id, agreement.voucher)
        .await;

    match response {
        Ok(ProposalResponse::Accept) => {
            tracing::debug!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                deployment_id=%deployment_id,
                indexer_url=%indexer_url,
                "Agreement proposal accepted by indexer"
            );
            // Agreement stays in Created, waiting for on-chain acceptance
        }
        Ok(ProposalResponse::Reject) => {
            tracing::info!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                deployment_id=%deployment_id,
                indexer_url=%indexer_url,
                "Agreement proposal rejected by indexer"
            );
            // Treat rejection same as delivery failure - mark and reassess
            mark_failed_and_reassess(
                &ctx,
                agreement_id,
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
            )
            .await?;
        }
        Err(err) => {
            tracing::error!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                error=?err,
                "Failed to send indexing agreement proposal"
            );
            mark_failed_and_reassess(
                &ctx,
                agreement_id,
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
            )
            .await?;
        }
    }

    Ok(())
}

/// Mark an agreement as delivery failed and queue reassessment.
async fn mark_failed_and_reassess<R, W, C>(
    ctx: &Ctx<R, W, C>,
    agreement_id: &IndexingAgreementId,
    indexing_request_id: &IndexingRequestId,
    deployment_id: &DeploymentId,
    deployment_chain_id: &ChainId,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
    C: IndexerClient,
{
    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        "Marking indexing agreement as DELIVERY_FAILED"
    );
    ctx.registry
        .mark_indexing_agreement_as_delivery_failed(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Reassess the indexing request to find replacement indexers
    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        "Reassessing indexing request after failure"
    );
    let indexing_request = ctx
        .registry
        .get_indexing_request_by_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    let num_candidates = indexing_request
        .map(|r| r.num_candidates)
        .unwrap_or(DEFAULT_MAX_CANDIDATES);
    if let Err(err) = ctx
        .queue
        .reassess_indexing_request(
            *indexing_request_id,
            *deployment_id,
            *deployment_chain_id,
            num_candidates,
        )
        .await
    {
        tracing::error!(error=%err, "Failed to queue task: 'reassess_indexing_request'");
        return Err(JobError::Fatal(err));
    }

    Ok(())
}
