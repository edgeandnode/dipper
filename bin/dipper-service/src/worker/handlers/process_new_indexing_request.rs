use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, SelectedIndexer, SelectionError};
use rand::seq::IndexedRandom;
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{ChainId, U256},
};

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
    // Gather load balancing context for IISA, including chain/ceiling info
    let mut context = gather_selection_context(
        &ctx.registry,
        deployment_id,
        ctx.agreement_conf.declined_indexer_lookback_days(),
        ctx.agreement_conf.price_rejection_lookback_days(),
    )
    .await?;

    // Map numeric chain ID to chain name for IISA ceiling/filtering
    let chain_name = chain_id_to_name(*deployment_chain_id);
    if let Some(name) = &chain_name {
        context.chain_id = Some(name.clone());
        context.max_grt_per_30_days = ctx.agreement_conf.max_grt_per_30_days().get(name).copied();
    }

    // Check if we're in fallback-eligible state (job has been retrying for 6+ hours)
    let fallback_eligible = job_meta.age_exceeds(IISA_FALLBACK_THRESHOLD);

    // Try IISA selection, with random fallback if IISA has been unavailable for too long
    let selected: Vec<SelectedIndexer> = if fallback_eligible {
        match ctx
            .iisa
            .select_indexers(*deployment_id, *num_candidates, &context)
            .await
        {
            Ok(indexers) => indexers,
            Err(SelectionError::IisaServiceUnavailable) => {
                // IISA unavailable for 6+ hours, fall back to random selection from network
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    age_hours=%((time::OffsetDateTime::now_utc() - job_meta.created_at).whole_hours()),
                    "IISA unavailable for 6+ hours, using random selection fallback"
                );

                // Build exclusion set from denylist, declined indexers, and pending agreements
                let mut excluded: std::collections::HashSet<IndexerId> =
                    context.indexer_denylist.iter().copied().collect();

                // Add indexers that recently declined this deployment
                if let Some(declined) = context.declined_indexers.get(deployment_id) {
                    excluded.extend(declined.iter().copied());
                }

                // Add indexers with pending agreements (to avoid overloading)
                for indexers in context.pending_agreements.values() {
                    excluded.extend(indexers.iter().copied());
                }

                let available_indexers: Vec<IndexerId> = ctx
                    .network
                    .get_indexers_not_indexing_a_deployment_id(deployment_id)
                    .into_iter()
                    .map(|i| i.id)
                    .filter(|id| !excluded.contains(id))
                    .collect();

                let mut rng = rand::rng();
                available_indexers
                    .choose_multiple(&mut rng, *num_candidates)
                    .copied()
                    .map(|id| SelectedIndexer {
                        id,
                        min_grt_per_30_days: None,
                        min_grt_per_billion_entities_per_30_days: None,
                    })
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
            Ok(indexers) => indexers,
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

    if selected.is_empty() {
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
    for selected_indexer in &selected {
        let indexer = match ctx.network.get_indexer_by_id(&selected_indexer.id) {
            Some(indexer) => indexer,
            None => {
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    indexer_id=%selected_indexer.id,
                    "IISA selected indexer not found in network topology, skipping"
                );
                continue;
            }
        };

        // Use per-indexer pricing from IISA if available, otherwise fall back to
        // the static pricing_table config
        let voucher_metadata = match resolve_pricing(
            selected_indexer,
            ctx.chain_price.get(deployment_chain_id),
            deployment_chain_id,
        ) {
            Some(meta) => IndexingAgreementVoucherMetadata {
                tokens_per_second: meta.0,
                tokens_per_entity_per_second: meta.1,
                subgraph_deployment_id: *deployment_id,
                protocol_network: ctx.signer.chain_id(),
                chain_id: *deployment_chain_id,
            },
            None => {
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    deployment_id=%deployment_id,
                    chain_id=%deployment_chain_id,
                    indexer_id=%selected_indexer.id,
                    "No pricing available (neither IISA nor config), skipping"
                );
                continue;
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

        tracing::info!(
            indexing_request_id=%indexing_request_id,
            indexer_id=%indexer.id,
            tokens_per_second=%voucher.metadata.tokens_per_second,
            tokens_per_entity_per_second=%voucher.metadata.tokens_per_entity_per_second,
            iisa_price=selected_indexer.min_grt_per_30_days.is_some(),
            "Creating agreement with pricing"
        );

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

/// Seconds in 30 days (30 * 24 * 60 * 60).
const SECONDS_PER_30_DAYS: u128 = 2_592_000;

/// 1 GRT = 10^18 wei.
const WEI_PER_GRT: u128 = 1_000_000_000_000_000_000;

/// Convert GRT per 30 days to wei per second (ceiling division to protect indexers).
fn grt_per_30_days_to_wei_per_second(grt: f64) -> U256 {
    // Convert to integer wei, then divide by seconds using ceiling division.
    // The ceiling protects indexers from rounding losses.
    let total_wei = (grt * WEI_PER_GRT as f64) as u128;
    let wei_per_second = total_wei.div_ceil(SECONDS_PER_30_DAYS);
    U256::from(wei_per_second)
}

/// Convert GRT per billion entities per 30 days to wei per entity per second.
fn grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second(grt: f64) -> U256 {
    // 1 billion entities = 1_000_000_000
    let total_wei = (grt * WEI_PER_GRT as f64 / 1_000_000_000.0) as u128;
    let wei_per_second = total_wei.div_ceil(SECONDS_PER_30_DAYS);
    U256::from(wei_per_second)
}

/// Resolve pricing for a selected indexer.
///
/// Uses per-indexer pricing from IISA when available, otherwise falls back to
/// the static pricing_table config. Returns `None` if neither source has pricing.
pub(crate) fn resolve_pricing(
    selected: &SelectedIndexer,
    fallback_prices: Option<&IndexingAgreementChainPrices>,
    chain_id: &ChainId,
) -> Option<(U256, U256)> {
    // Prefer IISA-reported per-indexer prices
    if let Some(grt_per_30d) = selected.min_grt_per_30_days {
        let tokens_per_second = grt_per_30_days_to_wei_per_second(grt_per_30d);
        let tokens_per_entity_per_second = selected
            .min_grt_per_billion_entities_per_30_days
            .map(grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second)
            .unwrap_or(U256::ZERO);
        return Some((tokens_per_second, tokens_per_entity_per_second));
    }

    // Fall back to static pricing_table
    if let Some(prices) = fallback_prices {
        return Some((
            prices.tokens_per_second,
            prices.tokens_per_entity_per_second,
        ));
    }

    tracing::warn!(
        indexer_id=%selected.id,
        chain_id=%chain_id,
        "No pricing from IISA and no fallback in pricing_table"
    );
    None
}

/// Map well-known EIP-155 chain IDs to human-readable chain names.
///
/// These names match the keys used in indexer-rs's `min_grt_per_30_days` config
/// and in the IISA `dips_supported_networks` field.
pub(crate) fn chain_id_to_name(chain_id: ChainId) -> Option<String> {
    match chain_id {
        1 => Some("mainnet".to_string()),
        42161 => Some("arbitrum-one".to_string()),
        8453 => Some("base".to_string()),
        10 => Some("optimism".to_string()),
        137 => Some("matic".to_string()),
        _ => {
            tracing::debug!(chain_id=%chain_id, "Unknown chain ID, no name mapping");
            None
        }
    }
}
