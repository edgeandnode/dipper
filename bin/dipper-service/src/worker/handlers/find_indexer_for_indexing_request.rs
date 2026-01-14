use std::{collections::BTreeMap, sync::Arc, time::Duration};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, Indexer as IndexerCandidate, SelectionError};
use jsonrpsee::core::Serialize;
use rand::seq::IndexedRandom;
use serde::Deserialize;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};

use super::selection_context::gather_selection_context;
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::NetworkProvider,
    registry::{
        AgreementRegistry, BlocklistRegistry, IndexingAgreementVoucher,
        IndexingAgreementVoucherMetadata, IndexingRequestRegistry,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::{
        result::{IISA_FALLBACK_THRESHOLD, JobError, JobMeta, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, N, W, I> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub chain_price: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    pub registry: R,
    pub network: N,
    pub queue: W,
    pub iisa: I,
}

/// Find a new indexer to fulfill an indexing request.
///
/// When an indexer cancels an indexing agreement, a new indexer must be selected
/// to fulfill the indexing request.
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the subgraph deployment
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph deployment
    pub deployment_chain_id: ChainId,
}

pub async fn handle<R, N, W, I>(
    ctx: Ctx<R, N, W, I>,
    Message {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: &Message,
    job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry + BlocklistRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Get the indexers that are not indexing the deployment, not rejected or canceled this indexing
    // request, and not already indexing this indexing request
    let already_indexing = ctx
        .registry
        .get_active_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
        .into_iter()
        .map(|agreement| agreement.indexer.id)
        .collect::<Vec<_>>();
    let rejected_or_canceled = ctx
        .registry
        .get_rejected_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
        .into_iter()
        .map(|agreement| agreement.indexer.id)
        .collect::<Vec<_>>();

    let indexers = ctx
        .network
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
        return Ok(());
    }

    // Gather load balancing context for IISA
    let context = gather_selection_context(&ctx.registry, deployment_id, &indexers).await?;

    // Check if we're in fallback-eligible state (job has been retrying for 6+ hours)
    // This allows us to avoid cloning indexers on the happy path
    let fallback_eligible = job_meta.age_exceeds(IISA_FALLBACK_THRESHOLD);

    // Try IISA selection, with random fallback if IISA has been unavailable for too long
    let candidate = if fallback_eligible {
        // Clone indexers only when fallback might be needed
        match ctx
            .iisa
            .select_one(*deployment_id, indexers.clone(), &context)
            .await
        {
            Ok(Some(candidate)) => candidate,
            Ok(None) => {
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    "No candidates selected to fulfill the indexing request"
                );
                return Ok(());
            }
            Err(SelectionError::IisaServiceUnavailable) => {
                // IISA unavailable for 6+ hours, fall back to random selection
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    age_hours=%((time::OffsetDateTime::now_utc() - job_meta.created_at).whole_hours()),
                    "IISA unavailable for 6+ hours, using random selection fallback"
                );
                match indexers.choose(&mut rand::rng()).cloned() {
                    Some(candidate) => candidate,
                    None => {
                        tracing::warn!(
                            indexing_request_id=%indexing_request_id,
                            "No candidates available for random fallback"
                        );
                        return Ok(());
                    }
                }
            }
            Err(SelectionError::Error(e)) => return Err(JobError::Fatal(e)),
        }
    } else {
        // Normal path: no clone needed since we won't fall back to random
        match ctx
            .iisa
            .select_one(*deployment_id, indexers, &context)
            .await
        {
            Ok(Some(candidate)) => candidate,
            Ok(None) => {
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    "No candidates selected to fulfill the indexing request"
                );
                return Ok(());
            }
            Err(SelectionError::IisaServiceUnavailable) => {
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    "IISA service unavailable, will retry"
                );
                return Err(JobError::Retryable(
                    SelectionError::IisaServiceUnavailable.into(),
                    Duration::from_secs(5),
                ));
            }
            Err(SelectionError::Error(e)) => return Err(JobError::Fatal(e)),
        }
    };

    let voucher_metadata = {
        let prices = ctx
            .chain_price
            .get(deployment_chain_id)
            .ok_or(JobError::Fatal(anyhow::anyhow!(
                "Chain prices not found for chain_id: {}",
                deployment_chain_id
            )))?;
        IndexingAgreementVoucherMetadata {
            base_price_per_epoch: prices.base_price_per_epoch,
            price_per_entity: prices.price_per_entity,
            subgraph_deployment_id: *deployment_id,
            protocol_network: ctx.signer.chain_id(),
            chain_id: *deployment_chain_id,
        }
    };

    let voucher = IndexingAgreementVoucher {
        payer: ctx.signer.address(),
        recipient: candidate.id.into_inner(),
        service: ctx.agreement_conf.service(),
        duration_epochs: ctx.agreement_conf.duration_epochs(),
        max_initial_amount: ctx.agreement_conf.max_initial_amount(),
        max_ongoing_amount_per_epoch: ctx.agreement_conf.max_ongoing_amount_per_epoch(),
        max_epochs_per_collection: ctx.agreement_conf.max_epochs_per_collection(),
        min_epochs_per_collection: ctx.agreement_conf.min_epochs_per_collection(),
        deadline: Default::default(), // TODO(v2): add the deadline
        metadata: voucher_metadata,
    };

    // Create indexing agreements for the selected indexers and register them in the registry
    let agreement_id = ctx
        .registry
        .register_new_indexing_agreement(
            *indexing_request_id,
            *deployment_id,
            candidate.id,
            candidate.url.clone(),
            voucher,
        )
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Send indexing agreement proposal to the selected indexer
    if let Err(err) = ctx
        .queue
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
        return Err(JobError::Fatal(err));
    }

    Ok(())
}
