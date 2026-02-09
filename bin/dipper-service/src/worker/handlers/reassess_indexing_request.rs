use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, SelectionError};
use jsonrpsee::core::Serialize;
use serde::Deserialize;
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
        result::{JobError, JobMeta, JobResult},
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

/// Reassess an indexing request against the current IISA target state.
///
/// The IISA returns an idempotent target group of indexers that should be assigned
/// to a deployment. This handler diffs the target group against current active
/// agreements, canceling stale assignments and creating new ones as needed.
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    /// The ID of the indexing request
    pub indexing_request_id: IndexingRequestId,
    /// The ID of the subgraph deployment
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph deployment
    pub deployment_chain_id: ChainId,
    /// The desired group size (how many indexers should be assigned)
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
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry + IndexerDenylistRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Gather load balancing context for IISA
    let context = gather_selection_context(&ctx.registry, deployment_id).await?;

    // Select the target group of indexers via IISA — no random fallback for reassessment,
    // since canceling good indexers to assign random ones would be destructive
    let target_ids: HashSet<IndexerId> = match ctx
        .iisa
        .select_indexers(*deployment_id, *num_candidates, &context)
        .await
    {
        Ok(ids) => ids.into_iter().collect(),
        Err(SelectionError::IisaServiceUnavailable) => {
            tracing::warn!(
                indexing_request_id=%indexing_request_id,
                "IISA service unavailable for reassessment, will retry"
            );
            return Err(JobError::Retryable(
                SelectionError::IisaServiceUnavailable.into(),
                Duration::from_secs(60),
            ));
        }
        Err(SelectionError::Error(e)) => return Err(JobError::Fatal(e)),
    };

    // Get current active agreements for this indexing request
    let active_agreements = ctx
        .registry
        .get_active_indexing_agreements_by_indexing_request_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    let current_ids: HashSet<IndexerId> = active_agreements
        .iter()
        .map(|agreement| agreement.indexer.id)
        .collect();

    // Compute the diff
    let to_cancel: HashSet<&IndexerId> = current_ids.difference(&target_ids).collect();
    let to_add: HashSet<&IndexerId> = target_ids.difference(&current_ids).collect();

    if to_cancel.is_empty() && to_add.is_empty() {
        tracing::debug!(
            indexing_request_id=%indexing_request_id,
            deployment_id=%deployment_id,
            "reassessment: no changes needed"
        );
        return Ok(());
    }

    // Cancel agreements for indexers no longer in the target group
    for agreement in &active_agreements {
        if !to_cancel.contains(&agreement.indexer.id) {
            continue;
        }

        ctx.registry
            .mark_indexing_agreement_as_canceled_by_requester(&agreement.id)
            .await
            .map_err(|err| JobError::Fatal(err.into()))?;

        if let Err(err) = ctx
            .queue
            .send_indexing_agreement_cancellation(
                agreement.indexer.url.clone(),
                *indexing_request_id,
                agreement.id,
            )
            .await
        {
            tracing::error!(
                error=%err,
                agreement_id=%agreement.id,
                "Failed to queue task: 'send_indexing_agreement_cancellation'"
            );
        }
    }

    // Look up chain prices once (bail early if missing)
    let prices = ctx
        .chain_price
        .get(deployment_chain_id)
        .ok_or(JobError::Fatal(anyhow::anyhow!(
            "Chain prices not found for chain_id: {}",
            deployment_chain_id
        )))?;

    // Create agreements for indexers newly in the target group.
    // Continue on per-indexer failures so that a single error does not prevent
    // the remaining additions from being processed.
    let mut add_failures = 0u32;
    for indexer_id in &to_add {
        let candidate = match ctx.network.get_indexer_by_id(indexer_id) {
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

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_secs();

        let voucher_metadata = IndexingAgreementVoucherMetadata {
            tokens_per_second: prices.tokens_per_second,
            tokens_per_entity_per_second: prices.tokens_per_entity_per_second,
            subgraph_deployment_id: *deployment_id,
            protocol_network: ctx.signer.chain_id(),
            chain_id: *deployment_chain_id,
        };

        let voucher = IndexingAgreementVoucher {
            payer: ctx.signer.address(),
            service_provider: candidate.id.into_inner(),
            data_service: ctx.agreement_conf.data_service(),
            ends_at: now + ctx.agreement_conf.duration_seconds(),
            max_initial_tokens: ctx.agreement_conf.max_initial_tokens(),
            max_ongoing_tokens_per_second: ctx.agreement_conf.max_ongoing_tokens_per_second(),
            min_seconds_per_collection: ctx.agreement_conf.min_seconds_per_collection(),
            max_seconds_per_collection: ctx.agreement_conf.max_seconds_per_collection(),
            deadline: now + ctx.agreement_conf.deadline_seconds(),
            metadata: voucher_metadata,
        };

        let agreement_id = match ctx
            .registry
            .register_new_indexing_agreement(
                *indexing_request_id,
                *deployment_id,
                candidate.id,
                candidate.url.clone(),
                voucher,
            )
            .await
        {
            Ok(id) => id,
            Err(err) => {
                add_failures += 1;
                tracing::error!(
                    error=%err,
                    indexer_id=%indexer_id,
                    "Failed to register new indexing agreement, skipping indexer"
                );
                continue;
            }
        };

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
            add_failures += 1;
            tracing::error!(
                error=%err,
                "Failed to queue task: 'send_indexing_agreement_proposal'"
            );
        }
    }

    if add_failures > 0 {
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            failures=add_failures,
            "some agreement additions failed during reassessment"
        );
    }

    tracing::info!(
        indexing_request_id=%indexing_request_id,
        deployment_id=%deployment_id,
        added = to_add.len(),
        canceled = to_cancel.len(),
        "reassessment complete"
    );

    Ok(())
}
