use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use dipper_core::{ids::IndexingRequestId, time::now_secs};
use dipper_iisa::{CandidateSelection, SelectedIndexer, SelectionError};
use graph_networks_registry::NetworksRegistry;
use jsonrpsee::core::Serialize;
use serde::Deserialize;
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};

use super::selection_context::gather_selection_context;
use crate::{
    chain_client::ChainClient,
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    indexer_rpc_client::compute_on_chain_id,
    network::{
        provider::NetworkProviderService,
        service::{
            chain_listener::ChainListenerStateRegistry, entity_count_cache::EntityCountCache,
        },
    },
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingAgreementTerms,
        IndexingAgreementTermsMetadata, IndexingRequestRegistry, NewAgreementParams,
        PendingCancellationRegistry,
    },
    signing::eip712::Eip712Signer,
    worker::{
        result::{JobError, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, W, I, T> {
    pub signer: Arc<Eip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub chain_price: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    pub registry: R,
    pub network: NetworkProviderService,
    pub queue: W,
    pub iisa: I,
    pub chain_client: T,
    pub networks_registry: Arc<NetworksRegistry>,
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,
    pub entity_count_cache: EntityCountCache,
    pub chain_listener_notify: Arc<tokio::sync::Notify>,
    /// When true, compute agreement deadlines from chain time
    /// (the chain_listener's persisted `last_processed_block_timestamp`)
    /// instead of wall clock. See `ChainListenerConfig::bypass_chain_clock_defenses`
    /// for the rationale and threat-model implications.
    pub bypass_chain_clock_defenses: bool,
    /// Chain ID used to read `last_processed_block_timestamp` from the
    /// chain_listener state registry when `bypass_chain_clock_defenses`
    /// is true. `None` disables the bypass path even if the flag is set
    /// (handler falls back to wall clock with a warning).
    pub chain_listener_chain_id: Option<u64>,
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

pub async fn handle<R, W, I, T>(
    ctx: Ctx<R, W, I, T>,
    Message {
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
        num_candidates,
    }: &Message,
) -> JobResult<()>
where
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + ChainListenerStateRegistry,
    W: WorkerQueue,
    I: CandidateSelection,
    T: ChainClient,
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
    let chain_name = super::selection_helpers::resolve_chain_name(
        *deployment_chain_id,
        &ctx.networks_registry,
        &ctx.additional_networks,
    );
    if let Some(name) = &chain_name {
        context.chain_id = Some(name.clone());
        context.max_grt_per_30_days = ctx.agreement_conf.max_grt_per_30_days().get(name).copied();
    }

    // Select the target group of indexers via IISA. If IISA is unreachable
    // we retry with exponential backoff rather than falling back to a
    // different selection mechanism: cancelling existing good indexers to
    // assign random ones would be destructive, and there is no per-indexer
    // /dips/info fallback for new requests either. Prolonged IISA outages
    // stall new requests until IISA recovers — see README "IISA dependency".
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
    //
    // `now` denominates `terms.deadline` and `terms.ends_at`. The
    // expiration service compares both against the chain_listener's
    // persisted chain timestamp. In production the two clocks track
    // each other so wall time is fine; in local-network where
    // `evm_increaseTime` advances chain time independently of wall,
    // `bypass_chain_clock_defenses` reroutes us to chain time so
    // freshly created agreements don't appear born-expired.
    let now = resolve_deadline_clock(
        ctx.bypass_chain_clock_defenses,
        ctx.chain_listener_chain_id,
        &ctx.registry,
    )
    .await?;

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
            match super::selection_helpers::resolve_pricing(
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

        let terms_metadata = IndexingAgreementTermsMetadata {
            tokens_per_second,
            tokens_per_entity_per_second,
            subgraph_deployment_id: *deployment_id,
            protocol_network: ctx.signer.chain_id(),
            chain_id: *deployment_chain_id,
        };

        let terms = IndexingAgreementTerms {
            payer: ctx.signer.address(),
            service_provider: candidate.id.into_inner(),
            data_service: ctx.agreement_conf.data_service(),
            ends_at: now.saturating_add(ctx.agreement_conf.duration_seconds()),
            max_initial_tokens: ctx.agreement_conf.max_initial_tokens(),
            max_ongoing_tokens_per_second: ctx.agreement_conf.max_ongoing_tokens_per_second(),
            min_seconds_per_collection: ctx.agreement_conf.min_seconds_per_collection(),
            max_seconds_per_collection: ctx.agreement_conf.max_seconds_per_collection(),
            deadline: now.saturating_add(ctx.agreement_conf.deadline_seconds()),
            conditions: 0,
            metadata: terms_metadata,
        };

        // Generate a UUID for nonce derivation, then compute the on-chain ID
        // which becomes the agreement's primary key.
        let nonce_uuid = uuid::Uuid::now_v7();
        let agreement_id_candidate = compute_on_chain_id(nonce_uuid, &terms);

        // If this add replaces an old agreement, register both atomically
        // so a crash cannot leave an agreement without its pending cancellation.
        let agreement_id = if let Some(old_agreement) = old_iter.next() {
            match ctx
                .registry
                .register_agreement_with_pending_cancellation(
                    NewAgreementParams {
                        agreement_id: agreement_id_candidate,
                        nonce_uuid,
                        request_id: *indexing_request_id,
                        deployment_id: *deployment_id,
                        indexer_id: candidate.id,
                        indexer_url: candidate.url.clone(),
                        terms,
                    },
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
                .register_new_indexing_agreement(NewAgreementParams {
                    agreement_id: agreement_id_candidate,
                    nonce_uuid,
                    request_id: *indexing_request_id,
                    deployment_id: *deployment_id,
                    indexer_id: candidate.id,
                    indexer_url: candidate.url.clone(),
                    terms,
                })
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
    // their place. Fire the on-chain cancel first; only mark the local row
    // CanceledByRequester after the chain tx is accepted, so retry on a
    // transient chain-client failure does the right thing.
    let mut directly_cancelled = 0u32;
    let mut cancel_failures = 0u32;
    for old_agreement in old_iter {
        // Skip agreements that haven't been accepted on-chain yet -- there is
        // nothing on the contract to cancel. The local row goes straight to
        // CanceledByRequester so the indexer never picks it up.
        let needs_on_chain_cancel = matches!(
            old_agreement.status,
            crate::registry::IndexingAgreementStatus::AcceptedOnChain
        );

        if needs_on_chain_cancel {
            match ctx
                .chain_client
                .cancel_indexing_agreement_by_payer(old_agreement.id.as_bytes())
                .await
            {
                Ok(Some(tx_hash)) => {
                    tracing::info!(
                        agreement_id = %old_agreement.id,
                        indexing_request_id = %indexing_request_id,
                        %tx_hash,
                        "Submitted on-chain cancellation for unpaired old agreement"
                    );
                }
                Ok(None) => {
                    tracing::info!(
                        agreement_id = %old_agreement.id,
                        indexing_request_id = %indexing_request_id,
                        "Unpaired old agreement already canceled on-chain; proceeding with local cleanup"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        agreement_id = %old_agreement.id,
                        "On-chain cancel failed; will retry on next reassessment"
                    );
                    cancel_failures += 1;
                    continue;
                }
            }
        }

        if let Err(err) = ctx
            .registry
            .mark_indexing_agreement_as_canceled_by_requester(&old_agreement.id)
            .await
        {
            tracing::error!(
                error=%err,
                agreement_id=%old_agreement.id,
                "Failed to mark unpaired old agreement as canceled in local DB"
            );
            cancel_failures += 1;
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

        directly_cancelled += 1;
    }

    if add_failures > 0 {
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            failures=add_failures,
            "some agreement additions failed during reassessment"
        );
    }

    if cancel_failures > 0 {
        // Two recovery paths cover any agreements left AcceptedOnChain here:
        //
        // - Shrink-to-zero (request now Canceled): the chain_listener's
        //   `sweep_orphan_canceled_agreements` retries on every sweep tick
        //   (default ~5 min at fast poll, ~5 h at slow poll).
        // - Shrink-not-zero (request still Open with too many agreements):
        //   the periodic reassignment service re-queues reassessment at its
        //   configured cadence (default 24 h).
        tracing::warn!(
            indexing_request_id=%indexing_request_id,
            failures=cancel_failures,
            "some agreement cancels failed during reassessment; retry will fire \
             via the orphan-cancel sweep (Canceled requests) or the periodic \
             reassignment service (Open requests over-target)"
        );
    }

    tracing::info!(
        indexing_request_id = %indexing_request_id,
        deployment_id = %deployment_id,
        created = successful_new_ids.len(),
        canceled = directly_cancelled,
        cancel_failures,
        add_failures,
        "reassessment summary"
    );

    if !successful_new_ids.is_empty() {
        ctx.chain_listener_notify.notify_one();
    }

    Ok(())
}

/// Pick the clock to denominate `terms.deadline` and `terms.ends_at`.
///
/// When `bypass` is false (production), return wall clock — matching
/// the original behavior and the simple, NTP-tracking case.
///
/// When `bypass` is true (local-network testing), fetch the
/// chain_listener's persisted chain timestamp instead so deadlines
/// stay denominated in chain time. Falls back to wall clock (with a
/// warning) when the chain listener hasn't bootstrapped yet or no
/// chain ID is configured; returns `JobError::Retryable` only if the
/// registry call itself errors, which the worker framework will
/// back-off and retry.
async fn resolve_deadline_clock<R>(
    bypass: bool,
    chain_listener_chain_id: Option<u64>,
    registry: &R,
) -> JobResult<u64>
where
    R: ChainListenerStateRegistry,
{
    if !bypass {
        return Ok(now_secs());
    }
    let Some(chain_id) = chain_listener_chain_id else {
        tracing::warn!(
            event = "deadline_clock_fallback",
            reason = "no chain_listener chain_id configured",
            "bypass_chain_clock_defenses=true but no chain id; falling back to wall clock"
        );
        return Ok(now_secs());
    };
    match registry.get_chain_listener_state(chain_id).await {
        Ok(Some(state)) => match state.last_processed_block_timestamp {
            Some(ts) => Ok(ts),
            None => {
                tracing::warn!(
                    event = "deadline_clock_fallback",
                    reason = "chain_listener state has no timestamp yet",
                    chain_id,
                    "falling back to wall clock for deadline computation"
                );
                Ok(now_secs())
            }
        },
        Ok(None) => {
            tracing::warn!(
                event = "deadline_clock_fallback",
                reason = "chain_listener has no persisted state yet",
                chain_id,
                "falling back to wall clock for deadline computation"
            );
            Ok(now_secs())
        }
        Err(err) => {
            tracing::warn!(
                event = "deadline_clock_lookup_failed",
                chain_id,
                error = %err,
                "failed to read chain_listener state for deadline; retrying job"
            );
            Err(JobError::Retryable(
                anyhow::anyhow!("chain_listener state lookup failed: {err}"),
                Duration::from_secs(5),
            ))
        }
    }
}
