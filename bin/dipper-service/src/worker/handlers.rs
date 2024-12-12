use std::{collections::BTreeMap, sync::Arc, time::Duration};

use dipper_iisa::{CandidateSelection, Indexer as IndexerCandidate};
use dipper_pgmq::result::JobResult;
use dipper_registry::{
    IndexingAgreementStatus, IndexingAgreementVoucher, IndexingAgreementVoucherMetadata, Registry,
};
use thegraph_core::alloy::primitives::ChainId;

use super::messages::{
    FindIndexerForIndexingRequest, ProcessIndexingAgreementCancellation,
    ProcessIndexingRequestCancellation, ProcessNewIndexingRequest,
    SendIndexingAgreementCancellation, SendIndexingAgreementProposal,
};
use crate::{
    context::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    indexers::{AgreementProposalResponse, DipsClient},
    network::NetworkProvider,
    signer::PrivateKeyEip712Signer,
    worker::WorkerQueue,
};

pub struct ProcessNewIndexingRequestCtx<R, N, W, I> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub chain_price: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    pub registry: R,
    pub network: N,
    pub queue: W,
    pub iisa: I,
}

pub(super) async fn process_new_indexing_request<R, N, W, I>(
    ProcessNewIndexingRequestCtx {
        signer,
        agreement_conf,
        chain_price,
        registry,
        network,
        queue,
        iisa,
    }: ProcessNewIndexingRequestCtx<R, N, W, I>,
    ProcessNewIndexingRequest {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    }: ProcessNewIndexingRequest,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Get the indexers that are not indexing the deployment amd treat it as the raw candidate list
    // and pass it to the IISA to get the final list of candidates
    let indexers = network
        .get_indexers_not_indexing_a_deployment_id(&deployment_id)
        .into_iter()
        .map(|indexer| IndexerCandidate {
            id: indexer.id,
            url: indexer.url,
        })
        .collect::<Vec<_>>();
    if indexers.is_empty() {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            "No indexers available to fulfill the indexing request"
        );
        return Ok(JobResult::Ok(()));
    }

    let candidates = iisa.select(deployment_id, indexers, num_candidates).await?;
    if candidates.is_empty() {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            "No candidates selected to fulfill the indexing request"
        );
        return Ok(JobResult::Ok(()));
    }

    // Create indexing agreements for the selected indexers and register them in the registry
    for candidate in candidates {
        let voucher_metadata = {
            let prices = chain_price
                .get(&deployment_chain_id)
                .ok_or(anyhow::anyhow!(
                    "Chain prices not found for chain_id: {}",
                    deployment_chain_id
                ))?;
            IndexingAgreementVoucherMetadata {
                deployment_id,
                price_per_block: prices.price_per_block,
                price_per_entity_per_epoch: prices.price_per_entity_per_epoch,
            }
        };

        let voucher = IndexingAgreementVoucher {
            payer: signer.address(),
            recipient: candidate.id.into_inner(),
            service: agreement_conf.service(),
            duration_epochs: agreement_conf.duration_epochs(),
            max_initial_amount: agreement_conf.max_initial_amount(),
            max_ongoing_amount_per_epoch: agreement_conf.max_ongoing_amount_per_epoch(),
            max_epochs_per_collection: agreement_conf.max_epochs_per_collection(),
            min_epochs_per_collection: agreement_conf.min_epochs_per_collection(),
            metadata: voucher_metadata,
        };

        let agreement_id = registry
            .register_new_indexing_agreement(
                indexing_request_id,
                deployment_id,
                candidate.id,
                candidate.url.clone(),
                voucher,
            )
            .await?;

        if let Err(err) = queue
            .send_indexing_agreement_proposal(
                candidate.url,
                agreement_id,
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
            )
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_proposal'");
            return Err(err);
        }
    }

    Ok(JobResult::Ok(()))
}

pub struct ProcessIndexingRequestCancellationCtx<R, W> {
    pub registry: R,
    pub queue: W,
}

pub(super) async fn process_indexing_request_cancellation<R, W>(
    ProcessIndexingRequestCancellationCtx { registry, queue }: ProcessIndexingRequestCancellationCtx<R, W>,
    ProcessIndexingRequestCancellation {
        indexing_request_id,
    }: ProcessIndexingRequestCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    W: WorkerQueue,
{
    // Get the indexing agreements associated with the indexing request
    let agreements = registry
        .get_indexing_request_active_indexing_agreements(&indexing_request_id)
        .await?;

    // Mark all the agreements as canceled by the requester
    // TODO: Allow marking multiple agreements as CANCELED_BY_REQUESTER in a single query
    for agreement in agreements.iter() {
        registry
            .mark_indexing_agreement_as_canceled_by_requester(&agreement.id)
            .await?;
    }

    // Send the indexing agreement cancellation notification to the indexers
    for agreement in agreements {
        if let Err(err) = queue
            .send_indexing_agreement_cancellation(
                agreement.indexer.url,
                agreement.id,
                indexing_request_id,
            )
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_cancellation'");
            return Err(err);
        }
    }

    Ok(JobResult::Ok(()))
}

pub struct FindIndexerForIndexingRequestCtx<R, N, W, I> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub chain_price: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    pub registry: R,
    pub network: N,
    pub queue: W,
    pub iisa: I,
}

pub(super) async fn find_indexer_for_indexing_request<R, N, W, I>(
    FindIndexerForIndexingRequestCtx {
        signer,
        agreement_conf,
        chain_price,
        registry,
        network,
        queue,
        iisa,
    }: FindIndexerForIndexingRequestCtx<R, N, W, I>,
    FindIndexerForIndexingRequest {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: FindIndexerForIndexingRequest,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Get the indexers that are not indexing the deployment, not rejected or canceled this indexing
    // request, and not already indexing this indexing request
    let already_indexing = registry
        .get_indexing_request_active_indexing_agreements(&indexing_request_id)
        .await?
        .into_iter()
        .map(|agreement| agreement.indexer.id)
        .collect::<Vec<_>>();
    let rejected_or_canceled = registry
        .get_indexing_request_rejected_indexing_agreements(&indexing_request_id)
        .await?
        .into_iter()
        .map(|agreement| agreement.indexer.id)
        .collect::<Vec<_>>();

    let indexers = network
        .get_indexers_not_indexing_a_deployment_id(&deployment_id)
        .into_iter()
        .filter(|indexer| {
            !already_indexing.contains(&indexer.id) && !rejected_or_canceled.contains(&indexer.id)
        })
        .map(|indexer| IndexerCandidate {
            id: indexer.id,
            url: indexer.url,
        })
        .collect::<Vec<_>>();
    if indexers.is_empty() {
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            "No indexers available to fulfill the indexing request"
        );
        return Ok(JobResult::Ok(()));
    }

    let Some(candidate) = iisa.select_one(deployment_id, indexers).await? else {
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            "No candidates selected to fulfill the indexing request"
        );
        return Ok(JobResult::Ok(()));
    };

    let voucher_metadata = {
        let prices = chain_price
            .get(&deployment_chain_id)
            .ok_or(anyhow::anyhow!(
                "Chain prices not found for chain_id: {}",
                deployment_chain_id
            ))?;
        IndexingAgreementVoucherMetadata {
            deployment_id,
            price_per_block: prices.price_per_block,
            price_per_entity_per_epoch: prices.price_per_entity_per_epoch,
        }
    };

    let voucher = IndexingAgreementVoucher {
        payer: signer.address(),
        recipient: candidate.id.into_inner(),
        service: agreement_conf.service(),
        duration_epochs: agreement_conf.duration_epochs(),
        max_initial_amount: agreement_conf.max_initial_amount(),
        max_ongoing_amount_per_epoch: agreement_conf.max_ongoing_amount_per_epoch(),
        max_epochs_per_collection: agreement_conf.max_epochs_per_collection(),
        min_epochs_per_collection: agreement_conf.min_epochs_per_collection(),
        metadata: voucher_metadata,
    };

    // Create indexing agreements for the selected indexers and register them in the registry
    let agreement_id = registry
        .register_new_indexing_agreement(
            indexing_request_id,
            deployment_id,
            candidate.id,
            candidate.url.clone(),
            voucher,
        )
        .await?;

    // Send indexing agreement proposal to the selected indexer
    if let Err(err) = queue
        .send_indexing_agreement_proposal(
            candidate.url,
            agreement_id,
            indexing_request_id,
            deployment_id,
            deployment_chain_id,
        )
        .await
    {
        tracing::error!(error=%err, "Failed to queue task: 'send_indexing_agreement_proposal'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}

pub struct SendIndexingAgreementProposalCtx<R, W, C> {
    pub registry: R,
    pub queue: W,
    pub indexer_client: C,
}

/// Send an indexing agreement proposal to the indexer.
///
/// This function sends an indexing agreement proposal to the indexer. If the proposal is accepted,
/// the agreement is marked as accepted in the registry. If the proposal is rejected, the agreement
/// is marked as rejected in the registry.
///
/// In the case of an error, mark the agreement as delivery failed in the registry.
pub(super) async fn send_indexing_agreement_proposal<R, W, C>(
    SendIndexingAgreementProposalCtx {
        registry,
        queue,
        indexer_client,
    }: SendIndexingAgreementProposalCtx<R, W, C>,
    SendIndexingAgreementProposal {
        indexer_url,
        agreement_id,
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: SendIndexingAgreementProposal,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    W: WorkerQueue,
    C: DipsClient,
{
    // Check the status of the agreement before sending the proposal
    match registry.get_indexing_agreement_by_id(agreement_id).await? {
        None => {
            tracing::error!(agreement_id=%agreement_id, "Indexing agreement not found");
            return Ok(JobResult::Ok(()));
        }
        Some(agreement) => match agreement.status {
            IndexingAgreementStatus::Created => {}
            IndexingAgreementStatus::Accepted | IndexingAgreementStatus::Rejected => {
                tracing::error!(
                    agreement_id=%agreement_id,
                    "Not sending agreement proposal. Agreement already accepted/rejected"
                );
                return Ok(JobResult::Ok(()));
            }
            _ => {
                tracing::error!(
                    agreement_id=%agreement_id,
                    "Not sending agreement proposal. Invalid agreement status: {}",
                    agreement.status,
                );
                return Ok(JobResult::Ok(()));
            }
        },
    }

    match indexer_client
        .send_indexing_agreement_proposal(
            indexer_url,
            agreement_id,
            indexing_request_id,
            deployment_id,
        )
        .await
    {
        Ok(resp) => match resp {
            AgreementProposalResponse::Accepted => {
                registry
                    .mark_indexing_agreement_as_accepted(&agreement_id)
                    .await?;
            }
            AgreementProposalResponse::Rejected => {
                registry
                    .mark_indexing_agreement_as_rejected(&agreement_id)
                    .await?;

                // Request a new indexer to fulfill the indexing request
                if let Err(err) = queue
                    .find_indexer_for_indexing_request(
                        indexing_request_id,
                        deployment_id,
                        deployment_chain_id,
                    )
                    .await
                {
                    tracing::error!(error=%err, "Failed to queue task: 'find_indexer_for_indexing_request'");
                    return Err(err);
                };
            }
        },
        Err(err) => {
            tracing::error!(error=?err, "Failed to send indexing agreement proposal");
            registry
                .mark_indexing_agreement_as_delivery_failed(&agreement_id)
                .await?;
        }
    }

    Ok(JobResult::Ok(()))
}

pub struct SendIndexingAgreementCancellationCtx<R, C> {
    pub registry: R,
    pub indexer_client: C,
}

/// Send an indexing agreement cancellation to the indexer.
///
/// This function sends an indexing agreement cancellation to the indexer. If the notification
/// fails, retry after 10 seconds.
pub(super) async fn send_indexing_agreement_cancellation<R, C>(
    SendIndexingAgreementCancellationCtx {
        registry,
        indexer_client,
    }: SendIndexingAgreementCancellationCtx<R, C>,
    SendIndexingAgreementCancellation {
        indexer_url,
        agreement_id,
        indexing_request_id,
    }: SendIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    C: DipsClient,
{
    // Check the status of the agreement before sending the cancellation
    let agreement = registry.get_indexing_agreement_by_id(agreement_id).await?;
    match agreement {
        None => {
            tracing::error!(agreement_id=%agreement_id, "Indexing agreement not found");
            return Ok(JobResult::Ok(()));
        }
        Some(agreement) => match agreement.status {
            IndexingAgreementStatus::Accepted => {}
            IndexingAgreementStatus::CanceledByRequester => {
                tracing::error!(
                    agreement_id=%agreement_id,
                    "Not sending agreement cancellation notification. Agreement already canceled"
                );
                return Ok(JobResult::Ok(()));
            }
            _ => {
                tracing::error!(
                    agreement_id=%agreement_id,
                    "Not sending agreement cancellation notification. Invalid agreement status: {}",
                    agreement.status,
                );
                return Ok(JobResult::Ok(()));
            }
        },
    }

    if let Err(err) = indexer_client
        .send_indexing_agreement_cancellation_notification(
            indexer_url,
            agreement_id,
            indexing_request_id,
        )
        .await
    {
        tracing::error!(error=?err, "Failed to send indexing agreement cancellation");
        return Ok(JobResult::Retry(Duration::from_secs(20), err.into()));
    };

    Ok(JobResult::Ok(()))
}

pub struct ProcessIndexingAgreementCancellationCtx<R, W> {
    pub registry: R,
    pub queue: W,
}

/// Process indexing agreement cancellation.
pub(super) async fn process_indexing_agreement_indexer_cancellation<R, W>(
    ProcessIndexingAgreementCancellationCtx { queue, registry }: ProcessIndexingAgreementCancellationCtx<R, W>,
    ProcessIndexingAgreementCancellation { agreement_id }: ProcessIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    W: WorkerQueue,
{
    // Check the status of the agreement before processing the cancellation
    let Some(agreement) = registry.get_indexing_agreement_by_id(agreement_id).await? else {
        tracing::error!(agreement_id=%agreement_id, "Indexing agreement not found");
        return Ok(JobResult::Ok(()));
    };

    // Mark the agreement as canceled by the indexer
    registry
        .mark_indexing_agreement_as_canceled_by_indexer(&agreement.id)
        .await?;

    // Send an agreement cancellation notification to the indexer
    if let Err(err) = queue
        .send_indexing_agreement_cancellation(
            agreement.indexer.url.clone(),
            agreement.id,
            agreement.indexing_request_id,
        )
        .await
    {
        tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_cancellation'");
        return Err(err);
    }

    // Get the indexing request associated with the agreement
    let Some(indexing_request) = registry
        .get_indexing_request_by_id(&agreement.indexing_request_id)
        .await?
    else {
        tracing::error!(agreement_id=%agreement_id, "Indexing request not found");
        return Ok(JobResult::Ok(()));
    };

    // Request a new indexer to fulfill the indexing request
    if let Err(err) = queue
        .find_indexer_for_indexing_request(
            indexing_request.id,
            indexing_request.deployment_id,
            indexing_request.deployment_chain_id,
        )
        .await
    {
        tracing::error!(error=?err, "Failed to queue task: 'find_indexer_for_indexing_request'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}

pub(super) async fn process_indexing_agreement_requester_cancellation<R, W>(
    ProcessIndexingAgreementCancellationCtx { queue, registry }: ProcessIndexingAgreementCancellationCtx<R, W>,
    ProcessIndexingAgreementCancellation { agreement_id }: ProcessIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: Registry,
    W: WorkerQueue,
{
    // Check the status of the agreement before processing the cancellation
    let Some(agreement) = registry.get_indexing_agreement_by_id(agreement_id).await? else {
        tracing::error!(agreement_id=%agreement_id, "Indexing agreement not found");
        return Ok(JobResult::Ok(()));
    };

    // Mark the agreement as canceled by the requester
    registry
        .mark_indexing_agreement_as_canceled_by_requester(&agreement.id)
        .await?;

    // Get the indexing request associated with the agreement
    let Some(indexing_request) = registry
        .get_indexing_request_by_id(&agreement.indexing_request_id)
        .await?
    else {
        tracing::error!(agreement_id=%agreement_id, "Indexing request not found");
        return Ok(JobResult::Ok(()));
    };

    // Request a new indexer to fulfill the indexing request
    if let Err(err) = queue
        .find_indexer_for_indexing_request(
            indexing_request.id,
            indexing_request.deployment_id,
            indexing_request.deployment_chain_id,
        )
        .await
    {
        tracing::error!(error=?err, "Failed to queue task: 'find_indexer_for_indexing_request'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}
