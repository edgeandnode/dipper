use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use serde_with::serde_as;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::{
    indexer_rpc_client::IndexerClient,
    network::NetworkProvider,
    registry::{
        AgreementRegistry, IndexingAgreementStatus, IndexingAgreementVoucher,
        IndexingAgreementVoucherMetadata, IndexingRequestRegistry,
    },
    worker::{
        result::{JobError, JobMeta, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, N, W, C> {
    pub registry: R,
    pub network: N,
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

/// Send an indexing agreement proposal to the indexer (fire-and-forget).
///
/// This function sends a SignedRCA to the indexer. If delivery fails, the agreement is marked
/// as delivery failed and the indexing request is reassessed. On successful delivery, the
/// agreement stays in `Created` until an on-chain acceptance event is observed.
pub async fn handle<R, N, W, C>(
    ctx: Ctx<R, N, W, C>,
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
    N: NetworkProvider,
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

    let voucher = IndexingAgreementVoucher {
        payer: agreement.voucher.payer,
        recipient: agreement.voucher.recipient,
        service: agreement.voucher.service,
        duration_epochs: agreement.voucher.duration_epochs,
        max_initial_amount: agreement.voucher.max_initial_amount,
        max_ongoing_amount_per_epoch: agreement.voucher.max_ongoing_amount_per_epoch,
        max_epochs_per_collection: agreement.voucher.max_epochs_per_collection,
        min_epochs_per_collection: agreement.voucher.min_epochs_per_collection,
        deadline: agreement.voucher.deadline,
        metadata: IndexingAgreementVoucherMetadata {
            base_price_per_epoch: agreement.voucher.metadata.base_price_per_epoch,
            price_per_entity: agreement.voucher.metadata.price_per_entity,
            subgraph_deployment_id: agreement.voucher.metadata.subgraph_deployment_id,
            protocol_network: agreement.voucher.metadata.protocol_network,
            chain_id: agreement.voucher.metadata.chain_id,
        },
    };

    tracing::debug!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        deployment_id=%deployment_id,
        indexer_url=%indexer_url,
        "Sending indexing agreement proposal"
    );
    match ctx
        .indexer_client
        .send_indexing_agreement_proposal(&indexer_url, *agreement_id, voucher)
        .await
    {
        Ok(()) => {
            tracing::debug!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                deployment_id=%deployment_id,
                indexer_url=%indexer_url,
                "Agreement proposal delivered"
            );
        }
        Err(err) => {
            tracing::error!(error=?err, "Failed to send indexing agreement proposal");
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
                "Reassessing indexing request after delivery failure"
            );
            let indexing_request = ctx
                .registry
                .get_indexing_request_by_id(indexing_request_id)
                .await
                .map_err(|err| JobError::Fatal(err.into()))?;
            let num_candidates = indexing_request.map(|r| r.num_candidates).unwrap_or(3);
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
            };
        }
    }

    Ok(())
}
