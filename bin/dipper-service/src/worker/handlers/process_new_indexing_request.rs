use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, SelectionError};
use rand::seq::IndexedRandom;
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};

use super::selection_context::gather_selection_context;
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::NetworkProvider,
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementVoucher,
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

/// Given a new indexing request, run the IISA and get a list of indexers that
/// can index the subgraph deployment.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the subgraph deployment
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph deployment
    pub deployment_chain_id: ChainId,
    /// The maximum number of indexers to select
    pub num_candidates: usize,
}

pub async fn handle<R, N, W, I>(
    ctx: Ctx<R, N, W, I>,
    Message {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    }: &Message,
    job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry + IndexerDenylistRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Gather load balancing context for IISA
    let context = gather_selection_context(&ctx.registry, deployment_id).await?;

    // Check if we're in fallback-eligible state (job has been retrying for 6+ hours)
    let fallback_eligible = job_meta.age_exceeds(IISA_FALLBACK_THRESHOLD);

    // Try IISA selection, with random fallback if IISA has been unavailable for too long
    let selected_ids = if fallback_eligible {
        match ctx
            .iisa
            .select_indexers(*deployment_id, *num_candidates, &context)
            .await
        {
            Ok(ids) => ids,
            Err(SelectionError::IisaServiceUnavailable) => {
                // IISA unavailable for 6+ hours, fall back to random selection from network
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    age_hours=%((time::OffsetDateTime::now_utc() - job_meta.created_at).whole_hours()),
                    "IISA unavailable for 6+ hours, using random selection fallback"
                );
                let available_indexers: Vec<IndexerId> = ctx
                    .network
                    .get_indexers_not_indexing_a_deployment_id(deployment_id)
                    .into_iter()
                    .map(|i| i.id)
                    .collect();
                let mut rng = rand::rng();
                available_indexers
                    .choose_multiple(&mut rng, *num_candidates)
                    .copied()
                    .collect()
            }
            Err(SelectionError::Error(e)) => return Err(JobError::Fatal(e)),
        }
    } else {
        // Normal path
        match ctx
            .iisa
            .select_indexers(*deployment_id, *num_candidates, &context)
            .await
        {
            Ok(ids) => ids,
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

    if selected_ids.is_empty() {
        tracing::error!(
            indexing_request_id=%indexing_request_id,
            "No candidates selected to fulfill the indexing request"
        );
        return Ok(());
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs();

    // Resolve indexer IDs to full indexer objects (with URLs) via network topology
    for indexer_id in selected_ids {
        let indexer = match ctx.network.get_indexer_by_id(&indexer_id) {
            Some(indexer) => indexer,
            None => {
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    indexer_id=%indexer_id,
                    "IISA selected indexer not found in network topology, skipping"
                );
                continue;
            }
        };

        let voucher_metadata = {
            let prices = match ctx.chain_price.get(deployment_chain_id) {
                Some(prices) => prices,
                None => {
                    tracing::warn!(
                        indexing_request_id=%indexing_request_id,
                        deployment_id=%deployment_id,
                        chain_id=%deployment_chain_id,
                        "Chain prices not found"
                    );
                    return Err(JobError::Fatal(anyhow::anyhow!(
                        "Chain prices not found for chain_id"
                    )));
                }
            };

            IndexingAgreementVoucherMetadata {
                tokens_per_second: prices.tokens_per_second,
                tokens_per_entity_per_second: prices.tokens_per_entity_per_second,
                subgraph_deployment_id: *deployment_id,
                protocol_network: ctx.signer.chain_id(),
                chain_id: *deployment_chain_id,
            }
        };

        let voucher = IndexingAgreementVoucher {
            payer: ctx.signer.address(),
            service_provider: indexer.id.into_inner(),
            data_service: ctx.agreement_conf.data_service(),
            ends_at: now.saturating_add(ctx.agreement_conf.duration_seconds()),
            max_initial_tokens: ctx.agreement_conf.max_initial_tokens(),
            max_ongoing_tokens_per_second: ctx.agreement_conf.max_ongoing_tokens_per_second(),
            min_seconds_per_collection: ctx.agreement_conf.min_seconds_per_collection(),
            max_seconds_per_collection: ctx.agreement_conf.max_seconds_per_collection(),
            deadline: now.saturating_add(ctx.agreement_conf.deadline_seconds()),
            metadata: voucher_metadata,
        };

        let agreement_id = ctx
            .registry
            .register_new_indexing_agreement(
                *indexing_request_id,
                *deployment_id,
                indexer.id,
                indexer.url.clone(),
                voucher,
            )
            .await
            .map_err(|err| JobError::Fatal(err.into()))?;

        if let Err(err) = ctx
            .queue
            .send_indexing_agreement_proposal(
                indexer.url,
                agreement_id,
                *indexing_request_id,
                *deployment_id,
                *deployment_chain_id,
            )
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'send_indexing_agreement_proposal'");
            return Err(JobError::Fatal(err));
        }
    }

    Ok(())
}
