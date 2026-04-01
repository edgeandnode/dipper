use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, FallbackFilter, SelectedIndexer, SelectionError};
use graph_networks_registry::NetworksRegistry;
use rand::seq::IndexedRandom;
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{ChainId, U256},
};

use super::selection_context::gather_selection_context;
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    indexer_rpc_client::compute_on_chain_id,
    network::{NetworkProvider, service::entity_count_cache::EntityCountCache},
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
    pub fallback_filter: Arc<FallbackFilter>,
    pub networks_registry: Arc<NetworksRegistry>,
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,
    pub entity_count_cache: EntityCountCache,
    /// Wakes the chain_listener when a proposal is dispatched so it starts
    /// fast-polling for on-chain acceptance events immediately.
    pub chain_listener_notify: Arc<tokio::sync::Notify>,
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
        ctx.agreement_conf.signer_rejection_lookback_minutes(),
        &ctx.entity_count_cache,
    )
    .await?;

    // Map numeric chain ID to chain name for IISA ceiling/filtering
    let chain_name = resolve_chain_name(
        *deployment_chain_id,
        &ctx.networks_registry,
        &ctx.additional_networks,
    );
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
                // IISA unavailable for 6+ hours, fall back to filtered selection from network
                tracing::warn!(
                    indexing_request_id=%indexing_request_id,
                    age_hours=%((time::OffsetDateTime::now_utc() - job_meta.created_at).whole_hours()),
                    "IISA unavailable for 6+ hours, using fallback selection"
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

                // Get candidates with URLs for direct /dips/info fetching
                let candidates: Vec<_> = ctx
                    .network
                    .get_indexers_not_indexing_a_deployment_id(deployment_id)
                    .into_iter()
                    .filter(|i| !excluded.contains(&i.id))
                    .map(|i| (i.id, i.url))
                    .collect();

                let candidate_count = candidates.len();

                // Filter by chain support and pricing via direct /dips/info fetch
                let filtered = if let Some(ref name) = chain_name {
                    ctx.fallback_filter
                        .filter_indexers(candidates, name, context.max_grt_per_30_days)
                        .await
                } else {
                    // No chain mapping - can't filter, convert to SelectedIndexer without pricing
                    tracing::warn!(
                        chain_id=%deployment_chain_id,
                        "No chain name mapping, skipping fallback filter"
                    );
                    candidates
                        .into_iter()
                        .map(|(id, _)| SelectedIndexer {
                            id,
                            min_grt_per_30_days: None,
                            min_grt_per_billion_entities_per_30_days: None,
                        })
                        .collect()
                };

                tracing::info!(
                    indexing_request_id=%indexing_request_id,
                    candidates=%candidate_count,
                    filtered=%filtered.len(),
                    "Fallback selection filtered candidates"
                );

                // Random selection from filtered candidates
                let mut rng = rand::rng();
                filtered
                    .choose_multiple(&mut rng, *num_candidates)
                    .cloned()
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

        // Generate the agreement ID up front so we can derive the on-chain ID
        // (the nonce is derived from the UUID, so we need it before INSERT).
        let agreement_id_candidate = dipper_core::ids::IndexingAgreementId::new();
        let on_chain_id = compute_on_chain_id(agreement_id_candidate, &voucher);

        let agreement_id = match ctx
            .registry
            .register_new_indexing_agreement(
                agreement_id_candidate,
                *indexing_request_id,
                *deployment_id,
                indexer.id,
                indexer.url.clone(),
                voucher,
                &on_chain_id,
            )
            .await
        {
            Ok(id) => id,
            Err(err) => {
                // Unique constraint violation (23505) on the active-agreement-per-indexer-deployment
                // index means IISA selected an indexer that already has an active agreement for this
                // deployment. This is a benign race condition -- log and skip to the next candidate.
                if is_unique_constraint_violation(&err) {
                    tracing::warn!(
                        indexing_request_id=%indexing_request_id,
                        indexer_id=%indexer.id,
                        deployment_id=%deployment_id,
                        "skipping candidate: active agreement already exists for this indexer+deployment"
                    );
                    continue;
                }
                return Err(JobError::Fatal(err.into()));
            }
        };

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

        tracing::debug!(
            indexing_request_id=%indexing_request_id,
            agreement_id=%agreement_id,
            indexer_id=%selected_indexer.id,
            "proposal queued for dispatch"
        );
    }

    // Wake the chain_listener so it switches to fast-polling for on-chain events.
    ctx.chain_listener_notify.notify_one();

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
        tracing::warn!(
            indexer_id=%selected.id,
            chain_id=%chain_id,
            tokens_per_second=%prices.tokens_per_second,
            tokens_per_entity_per_second=%prices.tokens_per_entity_per_second,
            "IISA returned no per-indexer pricing, falling back to static pricing_table"
        );
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

/// Check whether a registry error is a Postgres unique constraint violation (error code 23505).
fn is_unique_constraint_violation(err: &crate::registry::Error) -> bool {
    if let crate::registry::Error::BackendError(pg_err) = err
        && let dipper_pgregistry::Error::DbError(sqlx_err) = pg_err
        && let Some(db_err) = sqlx_err.as_database_error()
    {
        return db_err.code().as_deref() == Some("23505");
    }
    false
}

/// Resolve a numeric chain ID to the canonical network name used by The Graph ecosystem.
///
/// Looks up the chain in the official graph-networks-registry first (using CAIP-2 format
/// `eip155:{chain_id}`), then falls back to the `additional_networks` config map for
/// dev/test chains not in the registry (e.g. `1337` -> `"hardhat"`).
pub(crate) fn resolve_chain_name(
    chain_id: ChainId,
    registry: &NetworksRegistry,
    additional_networks: &BTreeMap<ChainId, String>,
) -> Option<String> {
    let caip2 = format!("eip155:{chain_id}");
    if let Some(network) = registry.get_network_by_caip2_id(&caip2) {
        return Some(network.id.clone());
    }
    if let Some(name) = additional_networks.get(&chain_id) {
        return Some(name.clone());
    }
    tracing::debug!(chain_id=%chain_id, "No network name found in registry or additional_networks");
    None
}
