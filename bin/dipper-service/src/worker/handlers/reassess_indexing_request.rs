use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, SelectedIndexer, SelectionError};
use graph_networks_registry::NetworksRegistry;
use jsonrpsee::core::Serialize;
use serde::Deserialize;
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};

use super::selection_context::gather_selection_context;
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::{NetworkProvider, service::entity_count_cache::EntityCountCache},
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementVoucher,
        IndexingAgreementVoucherMetadata, IndexingRequestRegistry, PendingCancellationRegistry,
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
    pub networks_registry: Arc<NetworksRegistry>,
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,
    pub entity_count_cache: EntityCountCache,
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
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    I: CandidateSelection,
{
    // Gather load balancing context for IISA, including chain/ceiling info
    let mut context = gather_selection_context(
        &ctx.registry,
        deployment_id,
        ctx.agreement_conf.declined_indexer_lookback_days(),
        ctx.agreement_conf.price_rejection_lookback_days(),
        ctx.agreement_conf.signer_rejection_lookback_minutes(),
        &ctx.entity_count_cache,
    )
    .await?;

    // Map numeric chain ID to chain name for IISA ceiling/filtering
    let chain_name = super::process_new_indexing_request::resolve_chain_name(
        *deployment_chain_id,
        &ctx.networks_registry,
        &ctx.additional_networks,
    );
    if let Some(name) = &chain_name {
        context.chain_id = Some(name.clone());
        context.max_grt_per_30_days = ctx.agreement_conf.max_grt_per_30_days().get(name).copied();
    }

    // Select the target group of indexers via IISA — no random fallback for reassessment,
    // since canceling good indexers to assign random ones would be destructive
    let target_selected: Vec<SelectedIndexer> = match ctx
        .iisa
        .select_indexers(*deployment_id, *num_candidates, &context)
        .await
    {
        Ok(indexers) => indexers,
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

    // Build lookup maps: ID set for diffing, and pricing for new agreements
    let target_ids: HashSet<IndexerId> = target_selected.iter().map(|s| s.id).collect();
    let target_pricing: HashMap<IndexerId, &SelectedIndexer> =
        target_selected.iter().map(|s| (s.id, s)).collect();

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

    let fallback_prices = ctx.chain_price.get(deployment_chain_id);

    // --- Add new agreements FIRST ---
    // We add before cancelling to prevent under-allocation. Old agreements
    // stay active until the chain_listener confirms the replacement is
    // accepted on-chain (see pending cancellations below).
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs();

    // Pre-compute old agreements to cancel so we can pair replacements
    // atomically during registration.
    let old_to_cancel: Vec<_> = active_agreements
        .iter()
        .filter(|a| to_cancel.contains(&a.indexer.id))
        .collect();
    let mut old_iter = old_to_cancel.into_iter();

    let mut successful_new_ids: Vec<dipper_core::ids::IndexingAgreementId> = vec![];
    let mut add_failures = 0u32;
    let mut _pending_recorded = 0u32;
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

        // Use per-indexer pricing from IISA when available, otherwise fall back
        // to the static pricing_table config
        let selected = target_pricing
            .get(indexer_id)
            .expect("ID from to_add must exist in target_pricing");
        let (tokens_per_second, tokens_per_entity_per_second) =
            match super::process_new_indexing_request::resolve_pricing(
                selected,
                fallback_prices,
                deployment_chain_id,
            ) {
                Some(prices) => prices,
                None => {
                    tracing::warn!(
                        indexing_request_id=%indexing_request_id,
                        indexer_id=%indexer_id,
                        "No pricing available, skipping indexer"
                    );
                    add_failures += 1;
                    continue;
                }
            };

        let voucher_metadata = IndexingAgreementVoucherMetadata {
            tokens_per_second,
            tokens_per_entity_per_second,
            subgraph_deployment_id: *deployment_id,
            protocol_network: ctx.signer.chain_id(),
            chain_id: *deployment_chain_id,
        };

        let voucher = IndexingAgreementVoucher {
            payer: ctx.signer.address(),
            service_provider: candidate.id.into_inner(),
            data_service: ctx.agreement_conf.data_service(),
            ends_at: now.saturating_add(ctx.agreement_conf.duration_seconds()),
            max_initial_tokens: ctx.agreement_conf.max_initial_tokens(),
            max_ongoing_tokens_per_second: ctx.agreement_conf.max_ongoing_tokens_per_second(),
            min_seconds_per_collection: ctx.agreement_conf.min_seconds_per_collection(),
            max_seconds_per_collection: ctx.agreement_conf.max_seconds_per_collection(),
            deadline: now.saturating_add(ctx.agreement_conf.deadline_seconds()),
            metadata: voucher_metadata,
        };

        // If this add replaces an old agreement, register both atomically
        // so a crash cannot leave an agreement without its pending cancellation.
        let agreement_id = if let Some(old_agreement) = old_iter.next() {
            match ctx
                .registry
                .register_agreement_with_pending_cancellation(
                    *indexing_request_id,
                    *deployment_id,
                    candidate.id,
                    candidate.url.clone(),
                    voucher,
                    old_agreement.id,
                )
                .await
            {
                Ok(id) => {
                    tracing::info!(
                        agreement_id = %id,
                        indexing_request_id = %indexing_request_id,
                        old_status = "none",
                        new_status = "CREATED",
                        reason = "reassessment_replacement",
                        "agreement state transition"
                    );
                    _pending_recorded += 1;
                    id
                }
                Err(err) => {
                    add_failures += 1;
                    tracing::error!(
                        error=%err,
                        indexer_id=%indexer_id,
                        old_agreement_id=%old_agreement.id,
                        "Failed to register replacement agreement, skipping indexer"
                    );
                    continue;
                }
            }
        } else {
            match ctx
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
                Ok(id) => {
                    tracing::info!(
                        agreement_id = %id,
                        indexing_request_id = %indexing_request_id,
                        old_status = "none",
                        new_status = "CREATED",
                        reason = "reassessment_addition",
                        "agreement state transition"
                    );
                    id
                }
                Err(err) => {
                    add_failures += 1;
                    tracing::error!(
                        error=%err,
                        indexer_id=%indexer_id,
                        "Failed to register new indexing agreement, skipping indexer"
                    );
                    continue;
                }
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
        } else {
            tracing::debug!(
                agreement_id = %agreement_id,
                indexer_id = %indexer_id,
                "proposal queued"
            );
            successful_new_ids.push(agreement_id);
        }
    }

    // Cancel old agreements that have no replacement to pair with.
    // These indexers are leaving the target group with nothing taking
    // their place, so there is no on-chain acceptance to wait for.
    let mut directly_cancelled = 0u32;
    for old_agreement in old_iter {
        if let Err(err) = ctx
            .registry
            .mark_indexing_agreement_as_canceled_by_requester(&old_agreement.id)
            .await
        {
            tracing::error!(
                error=%err,
                agreement_id=%old_agreement.id,
                "Failed to cancel unpaired old agreement"
            );
            continue;
        }

        tracing::info!(
            agreement_id = %old_agreement.id,
            indexing_request_id = %indexing_request_id,
            old_status = %old_agreement.status,
            new_status = "CANCELED_BY_REQUESTER",
            reason = "reassessment_not_in_target_group",
            "agreement state transition"
        );

        if let Err(err) = ctx
            .queue
            .send_indexing_agreement_cancellation(
                old_agreement.indexer.url.clone(),
                *indexing_request_id,
                old_agreement.id,
            )
            .await
        {
            tracing::error!(
                error=%err,
                agreement_id=%old_agreement.id,
                "Failed to queue cancellation notification for unpaired old agreement"
            );
        }

        directly_cancelled += 1;
    }

    if add_failures > 0 {
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            failures=add_failures,
            "some agreement additions failed during reassessment"
        );
    }

    tracing::info!(
        indexing_request_id = %indexing_request_id,
        deployment_id = %deployment_id,
        created = successful_new_ids.len(),
        canceled = directly_cancelled,
        failed = add_failures,
        "reassessment summary"
    );

    Ok(())
}
