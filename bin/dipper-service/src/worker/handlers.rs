use std::{sync::Arc, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::{CandidateSelection, Indexer as IndexerCandidate};
use dipper_pgmq::{queue::Queue, result::JobResult};
use dipper_registry::{
    IndexingAgreementStatus, IndexingAgreementVoucher, IndexingAgreementVoucherMetadata, Registry,
};

use super::{
    context::{Context, IndexingAgreementConfig},
    messages::{
        FindIndexerForIndexingRequest, Message, ProcessIndexingAgreementCancellation,
        ProcessIndexingRequestCancellation, ProcessNewIndexingRequest,
        SendIndexingAgreementCancellation, SendIndexingAgreementProposal,
    },
};
use crate::{
    indexers::{AgreementProposalResponse, DipsClient},
    network::NetworkProvider,
    signer::PrivateKeyEip712Signer,
};

/// Default agreement duration (60 days).
const DEFAULT_AGREEMENT_DURATION: Duration = Duration::from_secs(60 * 24 * 60 * 60);

pub(super) struct ProcessNewIndexingRequestState<Q, N, R, I> {
    signer: Arc<PrivateKeyEip712Signer>,
    agreement_conf: Arc<IndexingAgreementConfig>,
    queue: Q,
    network: N,
    registry: R,
    iisa: I,
}

impl<Q, N, R, C, I> FromState<Context<Q, N, R, C, I>> for ProcessNewIndexingRequestState<Q, N, R, I>
where
    Q: Clone,
    N: Clone,
    R: Clone,
    I: Clone,
{
    fn from_state(state: &Context<Q, N, R, C, I>) -> Self {
        Self {
            signer: state.signer.clone(),
            agreement_conf: state.agreement_conf.clone(),
            queue: state.queue.clone(),
            network: state.network.clone(),
            registry: state.registry.clone(),
            iisa: state.iisa.clone(),
        }
    }
}

pub(super) async fn process_new_indexing_request<Q, N, R, I>(
    ProcessNewIndexingRequestState {
        signer,
        agreement_conf,
        queue,
        network,
        registry,
        iisa,
    }: ProcessNewIndexingRequestState<Q, N, R, I>,
    ProcessNewIndexingRequest {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    }: ProcessNewIndexingRequest,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message>,
    N: NetworkProvider,
    R: Registry,
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
            let prices = agreement_conf.chain_price(&deployment_chain_id)?;
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
            .push(Message::SendIndexingAgreementProposal(
                SendIndexingAgreementProposal {
                    indexer_url: candidate.url,
                    agreement_id,
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
                    duration: DEFAULT_AGREEMENT_DURATION,
                },
            ))
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_proposal'");
            return Err(err);
        }
    }

    Ok(JobResult::Ok(()))
}

pub(super) struct ProcessIndexingRequestCancellationState<Q, R> {
    queue: Q,
    registry: R,
}

impl<Q, N, R, C, I> FromState<Context<Q, N, R, C, I>>
    for ProcessIndexingRequestCancellationState<Q, R>
where
    Q: Clone,
    R: Clone,
{
    fn from_state(state: &Context<Q, N, R, C, I>) -> Self {
        Self {
            queue: state.queue.clone(),
            registry: state.registry.clone(),
        }
    }
}

pub(super) async fn process_indexing_request_cancellation<Q, R>(
    ProcessIndexingRequestCancellationState { queue, registry }: ProcessIndexingRequestCancellationState<Q, R>,
    ProcessIndexingRequestCancellation {
        indexing_request_id,
    }: ProcessIndexingRequestCancellation,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message>,
    R: Registry,
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
            .push(Message::SendIndexingAgreementCancellation(
                SendIndexingAgreementCancellation {
                    indexer_url: agreement.indexer.url,
                    agreement_id: agreement.id,
                    indexing_request_id,
                },
            ))
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_cancellation'");
            return Err(err);
        }
    }

    Ok(JobResult::Ok(()))
}

pub(super) struct FindIndexerForIndexingRequestState<Q, N, R, I> {
    signer: Arc<PrivateKeyEip712Signer>,
    agreement_conf: Arc<IndexingAgreementConfig>,
    queue: Q,
    network: N,
    registry: R,
    iisa: I,
}

impl<Q, N, R, C, I> FromState<Context<Q, N, R, C, I>>
    for FindIndexerForIndexingRequestState<Q, N, R, I>
where
    Q: Clone,
    N: Clone,
    R: Clone,
    I: Clone,
{
    fn from_state(state: &Context<Q, N, R, C, I>) -> Self {
        Self {
            signer: state.signer.clone(),
            agreement_conf: state.agreement_conf.clone(),
            queue: state.queue.clone(),
            network: state.network.clone(),
            registry: state.registry.clone(),
            iisa: state.iisa.clone(),
        }
    }
}

pub(super) async fn find_indexer_for_indexing_request<Q, N, R, I>(
    FindIndexerForIndexingRequestState {
        signer,
        agreement_conf,
        queue,
        network,
        registry,
        iisa,
    }: FindIndexerForIndexingRequestState<Q, N, R, I>,
    FindIndexerForIndexingRequest {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: FindIndexerForIndexingRequest,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message>,
    N: NetworkProvider,
    R: Registry,
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
        let prices = agreement_conf.chain_price(&deployment_chain_id)?;
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
        .push(Message::SendIndexingAgreementProposal(
            SendIndexingAgreementProposal {
                indexer_url: candidate.url.clone(),
                agreement_id,
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
                duration: DEFAULT_AGREEMENT_DURATION,
            },
        ))
        .await
    {
        tracing::error!(error=%err, "Failed to queue task: 'send_indexing_agreement_proposal'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}

pub(super) struct SendIndexingAgreementProposalState<Q, R, C> {
    queue: Q,
    registry: R,
    indexer_client: C,
}

impl<Q, N, R, C, I> FromState<Context<Q, N, R, C, I>>
    for SendIndexingAgreementProposalState<Q, R, C>
where
    Q: Clone,
    R: Clone,
    C: Clone,
{
    fn from_state(state: &Context<Q, N, R, C, I>) -> Self {
        Self {
            queue: state.queue.clone(),
            registry: state.registry.clone(),
            indexer_client: state.indexer_client.clone(),
        }
    }
}

/// Send an indexing agreement proposal to the indexer.
///
/// This function sends an indexing agreement proposal to the indexer. If the proposal is accepted,
/// the agreement is marked as accepted in the registry. If the proposal is rejected, the agreement
/// is marked as rejected in the registry.
///
/// In the case of an error, mark the agreement as delivery failed in the registry.
pub(super) async fn send_indexing_agreement_proposal<Q, R, C>(
    SendIndexingAgreementProposalState {
        queue,
        registry,
        indexer_client,
    }: SendIndexingAgreementProposalState<Q, R, C>,
    SendIndexingAgreementProposal {
        indexer_url,
        agreement_id,
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        duration,
    }: SendIndexingAgreementProposal,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message>,
    R: Registry,
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
            duration,
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
                    .push(Message::FindIndexerForIndexingRequest(
                        FindIndexerForIndexingRequest {
                            indexing_request_id,
                            deployment_id,
                            deployment_chain_id,
                        },
                    ))
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

pub(super) struct SendIndexingAgreementCancellationState<R, C> {
    registry: R,
    indexer_client: C,
}

impl<Q, N, R, C, I> FromState<Context<Q, N, R, C, I>>
    for SendIndexingAgreementCancellationState<R, C>
where
    R: Clone,
    C: Clone,
{
    fn from_state(state: &Context<Q, N, R, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            indexer_client: state.indexer_client.clone(),
        }
    }
}

/// Send an indexing agreement cancellation to the indexer.
///
/// This function sends an indexing agreement cancellation to the indexer. If the notification
/// fails, retry after 10 seconds.
pub(super) async fn send_indexing_agreement_cancellation<R, C>(
    SendIndexingAgreementCancellationState {
        registry,
        indexer_client,
    }: SendIndexingAgreementCancellationState<R, C>,
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

pub(super) struct ProcessIndexingAgreementCancellationState<Q, R> {
    queue: Q,
    registry: R,
}

impl<Q, N, R, C, I> FromState<Context<Q, N, R, C, I>>
    for ProcessIndexingAgreementCancellationState<Q, R>
where
    Q: Clone,
    R: Clone,
{
    fn from_state(state: &Context<Q, N, R, C, I>) -> Self {
        Self {
            queue: state.queue.clone(),
            registry: state.registry.clone(),
        }
    }
}

/// Process indexing agreement cancellation.
pub(super) async fn process_indexing_agreement_indexer_cancellation<Q, R>(
    ProcessIndexingAgreementCancellationState { queue, registry }: ProcessIndexingAgreementCancellationState<Q, R>,
    ProcessIndexingAgreementCancellation { agreement_id }: ProcessIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message>,
    R: Registry,
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
        .push(Message::SendIndexingAgreementCancellation(
            SendIndexingAgreementCancellation {
                indexer_url: agreement.indexer.url.clone(),
                agreement_id: agreement.id,
                indexing_request_id: agreement.indexing_request_id,
            },
        ))
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
        .push(Message::FindIndexerForIndexingRequest(
            FindIndexerForIndexingRequest {
                indexing_request_id: indexing_request.id,
                deployment_id: indexing_request.deployment_id,
                deployment_chain_id: indexing_request.deployment_chain_id,
            },
        ))
        .await
    {
        tracing::error!(error=?err, "Failed to queue task: 'find_indexer_for_indexing_request'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}

pub(super) async fn process_indexing_agreement_requester_cancellation<Q, R>(
    ProcessIndexingAgreementCancellationState { queue, registry }: ProcessIndexingAgreementCancellationState<Q, R>,
    ProcessIndexingAgreementCancellation { agreement_id }: ProcessIndexingAgreementCancellation,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message>,
    R: Registry,
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
        .push(Message::FindIndexerForIndexingRequest(
            FindIndexerForIndexingRequest {
                indexing_request_id: indexing_request.id,
                deployment_id: indexing_request.deployment_id,
                deployment_chain_id: indexing_request.deployment_chain_id,
            },
        ))
        .await
    {
        tracing::error!(error=?err, "Failed to queue task: 'find_indexer_for_indexing_request'");
        return Err(err);
    }

    Ok(JobResult::Ok(()))
}
