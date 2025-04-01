use std::{collections::BTreeMap, sync::Arc, time::Duration};

use dipper_iisa::{CandidateSelection, Indexer as IndexerCandidate};
use thegraph_core::alloy::primitives::ChainId;

use super::messages::{
    FindIndexerForIndexingRequest, ProcessIndexingAgreementCancellation,
    ProcessIndexingRequestCancellation, ProcessNewIndexingRequest,
    SendIndexingAgreementCancellation, SendIndexingAgreementProposal,
};
use crate::{
    context::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    indexer_rpc_client::{AgreementProposalResponse, IndexerClient},
    network::NetworkProvider,
    registry::{
        AgreementRegistry, IndexingAgreementStatus, IndexingAgreementVoucher,
        IndexingAgreementVoucherMetadata, IndexingRequestRegistry,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::{WorkerQueue, result::JobResult},
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
    }: &ProcessNewIndexingRequest,
) -> anyhow::Result<JobResult<()>>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Get the indexers that are not indexing the deployment amd treat it as the raw candidate list
    // and pass it to the IISA to get the final list of candidates
    let indexers = network
        .get_indexers_not_indexing_a_deployment_id(deployment_id)
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

    let candidates = iisa
        .select(*deployment_id, indexers, *num_candidates)
        .await?;
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
            let prices = match chain_price.get(deployment_chain_id) {
                Some(prices) => prices,
                None => {
                    tracing::warn!(
                        indexing_request_id=%indexing_request_id,
                        deployment_id=%deployment_id,
                        chain_id=%deployment_chain_id,
                        "Chain prices not found"
                    );
                    return Err(anyhow::anyhow!("Chain prices not found for chain_id"));
                }
            };

            IndexingAgreementVoucherMetadata {
                base_price_per_epoch: prices.base_price_per_epoch,
                price_per_entity: prices.price_per_entity,
                subgraph_deployment_id: *deployment_id,
                protocol_network: signer.chain_id(),
                chain_id: *deployment_chain_id,
            }
        };

        let voucher = IndexingAgreementVoucher {
            payer: signer.address(),
            recipient: candidate.id.into_inner(),
            service: agreement_conf.service(),
            duration_epochs: agreement_conf.duration_epochs(),
            max_initial_amount: agreement_conf.max_initial_amount(),
            max_ongoing_amount_per_epoch: agreement_conf.max_ongoing_amount_per_epoch(),
            min_epochs_per_collection: agreement_conf.min_epochs_per_collection(),
            max_epochs_per_collection: agreement_conf.max_epochs_per_collection(),
            deadline: Default::default(), // TODO(v2): add the deadline
            metadata: voucher_metadata,
        };

        let agreement_id = registry
            .register_new_indexing_agreement(
                *indexing_request_id,
                *deployment_id,
                candidate.id,
                candidate.url.clone(),
                voucher,
            )
            .await?;

        if let Err(err) = queue
            .send_indexing_agreement_proposal(
                candidate.url,
                agreement_id,
                *indexing_request_id,
                *deployment_id,
                *deployment_chain_id,
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
    }: &ProcessIndexingRequestCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
{
    // Get the indexing agreements associated with the indexing request
    let agreements = registry
        .get_active_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await?;

    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        agreements=?agreements.iter().map(|agreement| agreement.id.to_string()).collect::<Vec<_>>(),
        "Processing indexing request cancellation"
    );

    // Send the indexing agreement cancellation notification to the indexers
    for agreement in agreements {
        if let Err(err) = queue
            .send_indexing_agreement_cancellation(
                agreement.indexer.url,
                *indexing_request_id,
                agreement.id,
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
    }: &FindIndexerForIndexingRequest,
) -> anyhow::Result<JobResult<()>>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Get the indexers that are not indexing the deployment, not rejected or canceled this indexing
    // request, and not already indexing this indexing request
    let already_indexing = registry
        .get_active_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await?
        .into_iter()
        .map(|agreement| agreement.indexer.id)
        .collect::<Vec<_>>();
    let rejected_or_canceled = registry
        .get_rejected_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await?
        .into_iter()
        .map(|agreement| agreement.indexer.id)
        .collect::<Vec<_>>();

    let indexers = network
        .get_indexers_not_indexing_a_deployment_id(deployment_id)
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

    let Some(candidate) = iisa.select_one(*deployment_id, indexers).await? else {
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            "No candidates selected to fulfill the indexing request"
        );
        return Ok(JobResult::Ok(()));
    };

    let voucher_metadata = {
        let prices = chain_price.get(deployment_chain_id).ok_or(anyhow::anyhow!(
            "Chain prices not found for chain_id: {}",
            deployment_chain_id
        ))?;
        IndexingAgreementVoucherMetadata {
            base_price_per_epoch: prices.base_price_per_epoch,
            price_per_entity: prices.price_per_entity,
            subgraph_deployment_id: *deployment_id,
            protocol_network: signer.chain_id(),
            chain_id: *deployment_chain_id,
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
        deadline: Default::default(), // TODO(v2): add the deadline
        metadata: voucher_metadata,
    };

    // Create indexing agreements for the selected indexers and register them in the registry
    let agreement_id = registry
        .register_new_indexing_agreement(
            *indexing_request_id,
            *deployment_id,
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
            *indexing_request_id,
            *deployment_id,
            *deployment_chain_id,
        )
        .await
    {
        tracing::error!(error=%err, "Failed to queue task: 'send_indexing_agreement_proposal'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}

pub struct SendIndexingAgreementProposalCtx<R, N, W, C> {
    pub registry: R,
    pub network: N,
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
pub(super) async fn send_indexing_agreement_proposal<R, N, W, C>(
    SendIndexingAgreementProposalCtx {
        registry,
        network,
        queue,
        indexer_client,
    }: SendIndexingAgreementProposalCtx<R, N, W, C>,
    SendIndexingAgreementProposal {
        indexer_url,
        agreement_id,
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: &SendIndexingAgreementProposal,
) -> anyhow::Result<JobResult<()>>
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

    let current_epoch = network.get_current_epoch();

    // Check the status of the agreement before sending the proposal
    let agreement = match registry.get_indexing_agreement_by_id(agreement_id).await? {
        None => {
            tracing::error!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                "Indexing agreement not found"
            );
            return Ok(JobResult::Ok(()));
        }
        Some(agreement) => match agreement.status {
            IndexingAgreementStatus::Created => agreement,
            IndexingAgreementStatus::Accepted { .. } | IndexingAgreementStatus::Rejected => {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    "Not sending agreement proposal. Agreement already accepted/rejected"
                );
                return Ok(JobResult::Ok(()));
            }
            status => {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    "Not sending agreement proposal. Invalid agreement status: {status}",
                );
                return Ok(JobResult::Ok(()));
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
    match indexer_client
        .send_indexing_agreement_proposal(&indexer_url, *agreement_id, voucher)
        .await
    {
        Ok(resp) => match resp {
            AgreementProposalResponse::Accepted => {
                tracing::debug!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    deployment_id=%deployment_id,
                    indexer_url=%indexer_url,
                    "Agreement proposal accepted"
                );
                registry
                    .mark_indexing_agreement_as_accepted(agreement_id, current_epoch)
                    .await?;
            }
            AgreementProposalResponse::Rejected => {
                tracing::debug!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    deployment_id=%deployment_id,
                    indexer_url=%indexer_url,
                    "Agreement proposal rejected"
                );
                registry
                    .mark_indexing_agreement_as_rejected(agreement_id)
                    .await?;

                // Request a new indexer to fulfill the indexing request
                tracing::trace!(
                    indexing_request_id=%indexing_request_id,
                    "Requesting a new indexer to fulfill the indexing request"
                );
                if let Err(err) = queue
                    .find_indexer_for_indexing_request(
                        *indexing_request_id,
                        *deployment_id,
                        *deployment_chain_id,
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
            tracing::trace!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                "Marking indexing agreement as DELIVERY_FAILED"
            );
            registry
                .mark_indexing_agreement_as_delivery_failed(agreement_id)
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
        indexing_request_id,
        agreement_id,
    }: &SendIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: AgreementRegistry,
    C: IndexerClient,
{
    // TODO: THIS IS A HACK
    let indexer_url = {
        let mut url = indexer_url.clone();
        url.set_port(Some(7602)).unwrap();
        url
    };

    // Check the status of the agreement before sending the cancellation
    let agreement = registry.get_indexing_agreement_by_id(agreement_id).await?;
    match agreement {
        None => {
            tracing::error!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                "Indexing agreement not found"
            );
            return Ok(JobResult::Ok(()));
        }
        Some(agreement) => match agreement.status {
            IndexingAgreementStatus::Accepted { .. } => {}
            IndexingAgreementStatus::CanceledByRequester => {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    "Not sending agreement cancellation notification. Agreement already canceled"
                );
                return Ok(JobResult::Ok(()));
            }
            _ => {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    "Not sending agreement cancellation notification. Invalid agreement status: {}",
                    agreement.status,
                );
                return Ok(JobResult::Ok(()));
            }
        },
    }

    tracing::debug!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        indexer_url=%indexer_url,
        "Sending indexing agreement cancellation notification"
    );

    if let Err(err) = indexer_client
        .send_indexing_agreement_cancellation_notification(&indexer_url, *agreement_id)
        .await
    {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            error=?err,
            "Failed to send indexing agreement cancellation. Re-trying in 20 seconds"
        );
        return Ok(JobResult::Retry(Duration::from_secs(20), err.into()));
    };

    tracing::debug!(
        %indexing_request_id,
        %agreement_id,
        %indexer_url,
        "Indexing agreement cancellation accepted by indexer"
    );

    registry
        .mark_indexing_agreement_as_canceled_by_requester(agreement_id)
        .await
        .map_err(|err| {
            tracing::error!(
                %indexing_request_id,
                %agreement_id,
                error=?err,
                "Failed to mark indexing agreement as CANCELED_BY_REQUESTER");
            err
        })?;

    Ok(JobResult::Ok(()))
}

pub struct ProcessIndexingAgreementCancellationCtx<R, W> {
    pub registry: R,
    pub queue: W,
}

/// Process indexing agreement cancellation.
pub(super) async fn process_indexing_agreement_indexer_cancellation<R, W>(
    ProcessIndexingAgreementCancellationCtx { queue, registry }: ProcessIndexingAgreementCancellationCtx<R, W>,
    ProcessIndexingAgreementCancellation {
        indexing_request_id,
        agreement_id,
    }: &ProcessIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
{
    // Check the status of the agreement before processing the cancellation
    let Some(agreement) = registry.get_indexing_agreement_by_id(agreement_id).await? else {
        tracing::error!(%indexing_request_id, %agreement_id, "Indexing agreement not found");
        return Ok(JobResult::Ok(()));
    };

    // Mark the agreement as canceled by the indexer
    registry
        .mark_indexing_agreement_as_canceled_by_indexer(&agreement.id)
        .await?;

    tracing::debug!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        indexer_url=%agreement.indexer.url,
        "Sending indexing agreement cancellation to the indexer"
    );

    // Send an agreement cancellation notification to the indexer
    if let Err(err) = queue
        .send_indexing_agreement_cancellation(
            agreement.indexer.url.clone(),
            *indexing_request_id,
            agreement.id,
        )
        .await
    {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            error=?err,
            "Failed to queue task: 'send_indexing_agreement_cancellation'"
        );
        return Err(err);
    }

    // Get the indexing request associated with the agreement
    let Some(indexing_request) = registry
        .get_indexing_request_by_id(&agreement.indexing_request_id)
        .await?
    else {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            "Indexing request not found"
        );
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
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            error=?err,
            "Failed to queue task: 'find_indexer_for_indexing_request'"
        );
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}

pub(super) async fn process_indexing_agreement_requester_cancellation<R, W>(
    ProcessIndexingAgreementCancellationCtx { queue, registry }: ProcessIndexingAgreementCancellationCtx<R, W>,
    ProcessIndexingAgreementCancellation {
        indexing_request_id,
        agreement_id,
    }: &ProcessIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
{
    // Check the status of the agreement before processing the cancellation
    let Some(agreement) = registry.get_indexing_agreement_by_id(agreement_id).await? else {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            "Indexing agreement not found"
        );
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
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            "Indexing request not found"
        );
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
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            error=?err,
            "Failed to queue task: 'find_indexer_for_indexing_request'"
        );
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}
