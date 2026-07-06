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
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{ChainId, U256},
};

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
        DipsAcceptingCache, UnresponsiveBreaker,
        result::{JobError, JobResult},
        service::WorkerQueue,
    },
};

/// RCA `conditions` bit that flags the RecurringAgreementManager contract as the
/// agreement owner. Matches the on-chain `uint16` width.
const CONDITION_AGREEMENT_OWNER: u16 = 1u16 << 1;

pub struct Ctx<R, W, I, T> {
    pub signer: Arc<Eip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    /// EIP-712 domain the RCA terms hash is computed under, to persist
    /// `terms_version_hash` for the cancel path.
    pub rca_domain: Arc<std::sync::RwLock<thegraph_core::alloy::sol_types::Eip712Domain>>,
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
    /// Global reassess lock; only one reassessment runs at a time across all
    /// worker loops (see `crate::worker::context::ReassessLock`).
    pub reassess_lock: crate::worker::context::ReassessLock,
    /// Mass-unresponsive circuit breaker (see its module).
    pub unresponsive_breaker: Arc<UnresponsiveBreaker>,
    /// Cache of IISA's DIPs-accepting set (the breaker's denominator).
    pub dips_accepting_cache: DipsAcceptingCache,
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
    // Only one reassessment runs globally at a time; if another loop holds the
    // lock this pass would diff the same baseline, so defer ~1s rather than park
    // this loop. Deferral isn't a failure: no backoff, no attempt count.
    let _reassess_guard = match ctx.reassess_lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => return Err(JobError::Deferred(Duration::from_secs(1))),
    };

    // Gather load balancing context for IISA, including chain/ceiling info
    let (mut context, unresponsive) = gather_selection_context(
        &ctx.registry,
        deployment_id,
        ctx.agreement_conf.declined_indexer_lookback_days(),
        ctx.agreement_conf.price_rejection_lookback_days(),
        ctx.agreement_conf.transient_rejection_lookback_minutes(),
        ctx.agreement_conf.uncertain_rejection_lookback_days(),
        ctx.agreement_conf.unresponsive_indexer_lookback_days(),
        *deployment_chain_id,
        &ctx.entity_count_cache,
    )
    .await?;

    // The chain name is required for IISA filtering, pricing and the breaker; an
    // unresolved chain means a missing additional_networks entry, so fail loudly
    // rather than select without a chain filter or price ceiling.
    let chain_name = super::selection_helpers::resolve_chain_name(
        *deployment_chain_id,
        &ctx.networks_registry,
        &ctx.additional_networks,
    )
    .ok_or_else(|| {
        JobError::Fatal(anyhow::anyhow!(
            "no network name for chain id {}; add it to additional_networks",
            *deployment_chain_id
        ))
    })?;

    // Cap the breaker's pool to indexers dipper could actually pay on this chain, so
    // the denominator matches the indexers the unresponsive numerator is drawn from.
    let max_grt_per_30_days = ctx
        .agreement_conf
        .max_grt_per_30_days()
        .get(&chain_name)
        .copied();

    // Per-chain mass-unresponsive breaker: when a large fraction of this chain's
    // DIPs-accepting pool is unresponsive at once it's a dipper-side outage, so
    // suppress this chain's exclusion rather than benching everyone serving it.
    if !unresponsive.is_empty() {
        let snapshot = ctx
            .dips_accepting_cache
            .get_or_fetch(&ctx.iisa, &chain_name, max_grt_per_30_days)
            .await;
        let suppress = ctx.unresponsive_breaker.evaluate(
            &chain_name,
            &unresponsive,
            snapshot.as_deref(),
            ctx.agreement_conf.mass_unresponsive_trip_fraction(),
            ctx.agreement_conf.mass_unresponsive_reset_fraction(),
            ctx.agreement_conf.dips_accepting_snapshot_max_age_hours(),
        );
        if suppress {
            tracing::debug!(
                chain = chain_name.as_str(),
                would_bench = unresponsive.len(),
                "unresponsive breaker tripped; skipping this chain's unresponsive exclusion"
            );
        } else {
            context.indexer_denylist.extend(unresponsive);
        }
    }

    context.chain_id = Some(chain_name.clone());
    context.max_grt_per_30_days = max_grt_per_30_days;

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

    // Surface the IISA outcome alongside the diff so the operator can tell
    // "everyone is declined" apart from "we're already synced" without
    // cross-referencing IISA logs. `requested_num_candidates` vs
    // `iisa_returned_count` is the key diagnostic — a shortfall means
    // IISA filtered candidates out (typically the declined/denylist sets).
    tracing::info!(
        indexing_request_id=%indexing_request_id,
        deployment_id=%deployment_id,
        requested_num_candidates = num_candidates,
        iisa_returned_count = target_selected.len(),
        current_active_count = current_ids.len(),
        to_add_count = to_add.len(),
        to_cancel_count = to_cancel.len(),
        "reassessment diff computed"
    );

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
        &ctx.chain_client,
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

        // The RCA's contract caps don't need to be per-chain. At large
        // subgraph sizes the entity-driven contribution dominates the
        // per-second base rate, so per-chain variation is noise relative
        // to that. One flat cap across all chains keeps the on-chain
        // ceiling meaningful (it actually bites if metadata is bad)
        // without per-chain accounting that the entity component would
        // swamp anyway.
        //
        // `max_initial_tokens` is set to zero for v1 of the pricing
        // system. The contract adds `maxInitialTokens` to the first
        // `collect()`'s allowance on top of `maxOngoingTokensPerSecond *
        // elapsed`, so any non-zero value lets the first month claim
        // beyond the configured monthly ceiling. Initial-sync
        // compensation is left to ongoing rate accumulation while we
        // gather data on how indexers actually exercise the cap.
        let agreement_cap_grt = ctx.agreement_conf.max_agreement_grt_per_30_days();

        // The RecurringAgreementManager contract is the payer and is flagged as
        // the agreement owner via the conditions bit.
        let payer = ctx.agreement_conf.recurring_agreement_manager();

        let terms = IndexingAgreementTerms {
            payer,
            service_provider: candidate.id.into_inner(),
            data_service: ctx.agreement_conf.data_service(),
            ends_at: now.saturating_add(ctx.agreement_conf.duration_seconds()),
            max_initial_tokens: U256::ZERO,
            max_ongoing_tokens_per_second:
                super::selection_helpers::grt_per_30_days_to_wei_per_second(agreement_cap_grt),
            min_seconds_per_collection: ctx.agreement_conf.min_seconds_per_collection(),
            max_seconds_per_collection: ctx.agreement_conf.max_seconds_per_collection(),
            deadline: now.saturating_add(ctx.agreement_conf.deadline_seconds()),
            conditions: CONDITION_AGREEMENT_OWNER,
            metadata: terms_metadata,
        };

        // Generate a UUID for nonce derivation, then compute the on-chain ID
        // which becomes the agreement's primary key.
        let nonce_uuid = uuid::Uuid::now_v7();
        let agreement_id_candidate = compute_on_chain_id(nonce_uuid, &terms);

        // Persist the EIP-712 terms hash now so the cancel path can pass it to
        // the manager's cancelAgreement().
        let terms_version_hash = Some(compute_terms_version_hash(
            nonce_uuid,
            &terms,
            &ctx.rca_domain,
        ));

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
                        terms_version_hash,
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
                    terms_version_hash,
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
            match crate::cancel_dispatch::cancel_agreement_on_chain(
                &ctx.chain_client,
                old_agreement,
                &ctx.agreement_conf,
            )
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

/// Compute the EIP-712 terms hash persisted for the protocol-managed cancel
/// path, reusing the proposal signer's RCA-to-sol conversion and signing-hash so
/// the value matches the hash dipper signs over.
fn compute_terms_version_hash(
    nonce_uuid: uuid::Uuid,
    terms: &IndexingAgreementTerms,
    rca_domain: &std::sync::RwLock<thegraph_core::alloy::sol_types::Eip712Domain>,
) -> Vec<u8> {
    use thegraph_core::alloy::sol_types::SolStruct;

    // versionHash is the domain-separated EIP-712 signing hash, matching
    // RecurringCollector._hashRCA (the value it stores and checks on cancel);
    // frozen by terms_version_hash_matches_frozen_golden_value.
    let (rca, _) = crate::indexer_rpc_client::into_sol_rca(nonce_uuid, terms.clone());
    let domain = rca_domain.read().expect("RCA domain lock poisoned").clone();
    rca.eip712_signing_hash(&domain).to_vec()
}

/// Pick the clock denominating `terms.deadline`/`terms.ends_at`: wall clock when
/// `bypass` is false; otherwise chain time — live chain head, else the listener's
/// persisted timestamp, else wall clock, warning on each demotion; registry errors retry.
async fn resolve_deadline_clock<R, C>(
    bypass: bool,
    chain_listener_chain_id: Option<u64>,
    registry: &R,
    chain_client: &C,
) -> JobResult<u64>
where
    R: ChainListenerStateRegistry,
    C: ChainClient,
{
    if !bypass {
        return Ok(now_secs());
    }
    // Prefer a live chain-head read: the persisted chain_listener timestamp
    // idles behind a fast-moving local chain, so trusting it would stamp a
    // deadline in the chain's past and make the agreement born-expired.
    match chain_client.latest_block_timestamp().await {
        Ok(ts) => return Ok(ts),
        Err(err) => {
            tracing::warn!(
                event = "deadline_clock_live_chain_unavailable",
                error = %err,
                "live chain timestamp read failed; falling back to persisted listener state or wall clock"
            );
        }
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

#[cfg(test)]
mod terms_hash_tests {
    use std::sync::RwLock;

    use thegraph_core::alloy::{
        primitives::{Address, B256, U256, b256},
        sol_types::Eip712Domain,
    };

    use super::compute_terms_version_hash;
    use crate::registry::{IndexingAgreementTerms, IndexingAgreementTermsMetadata};

    fn fixed_nonce() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)
    }

    fn fixed_domain() -> Eip712Domain {
        Eip712Domain::new(
            Some("RecurringCollector".into()),
            Some("1".into()),
            Some(U256::from(1u64)),
            Some(Address::repeat_byte(0x42)),
            None,
        )
    }

    fn fixed_terms() -> IndexingAgreementTerms {
        IndexingAgreementTerms {
            payer: Address::repeat_byte(0x01),
            service_provider: Address::repeat_byte(0x02),
            data_service: Address::repeat_byte(0x03),
            deadline: 1_700_000_000,
            ends_at: 1_700_086_400,
            max_initial_tokens: U256::from(1_000u64),
            max_ongoing_tokens_per_second: U256::from(5u64),
            min_seconds_per_collection: 60,
            max_seconds_per_collection: 3_600,
            conditions: 2,
            metadata: IndexingAgreementTermsMetadata {
                tokens_per_second: U256::from(7u64),
                tokens_per_entity_per_second: U256::from(3u64),
                subgraph_deployment_id: "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
                    .parse()
                    .unwrap(),
                protocol_network: 1u64,
                chain_id: 1u64,
            },
        }
    }

    #[test]
    fn terms_version_hash_matches_frozen_golden_value() {
        // tc-2: freeze the exact EIP-712 terms hash. Every protocol-managed
        // cancel hands this 32-byte value to the collector as the versionHash it
        // checks against its own _hashRCA, so a silent encoding drift must fail.
        let hash =
            compute_terms_version_hash(fixed_nonce(), &fixed_terms(), &RwLock::new(fixed_domain()));
        assert_eq!(hash.len(), 32);
        assert_eq!(
            B256::from_slice(&hash),
            b256!("45b21c6b2757363fa9aa2219cb378a623fbf9d50204b6deab2368fb09512f31e"),
        );
    }

    #[test]
    fn terms_version_hash_is_input_sensitive() {
        let base =
            compute_terms_version_hash(fixed_nonce(), &fixed_terms(), &RwLock::new(fixed_domain()));

        let other_nonce = compute_terms_version_hash(
            uuid::Uuid::from_u128(0xdead_beef),
            &fixed_terms(),
            &RwLock::new(fixed_domain()),
        );
        assert_ne!(base, other_nonce, "nonce must change the hash");

        let other_domain = Eip712Domain::new(
            Some("RecurringCollector".into()),
            Some("2".into()),
            Some(U256::from(1u64)),
            Some(Address::repeat_byte(0x42)),
            None,
        );
        let other =
            compute_terms_version_hash(fixed_nonce(), &fixed_terms(), &RwLock::new(other_domain));
        assert_ne!(base, other, "domain must change the hash");
    }
}

#[cfg(test)]
mod deadline_clock_tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use dipper_core::time::now_secs;
    use thegraph_core::alloy::primitives::{Address, B256};

    use super::{JobError, resolve_deadline_clock};
    use crate::{
        chain_client::{ChainClient, ChainClientError},
        network::service::{
            chain_events::Cursor,
            chain_listener::{ChainListenerState, ChainListenerStateRegistry},
        },
    };

    /// `head: Some(ts)` mocks a live chain-head read; `None` mocks an RPC failure.
    struct MockChainClient {
        head: Option<u64>,
    }

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            self.head
                .ok_or_else(|| ChainClientError::RpcError(anyhow::anyhow!("head read failed")))
        }

        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            Ok(false)
        }
    }

    /// `fail: true` makes the state lookup itself error, exercising the
    /// retryable-job branch rather than any clock fallback.
    struct MockRegistry {
        state: Option<ChainListenerState>,
        fail: bool,
    }

    #[async_trait]
    impl ChainListenerStateRegistry for MockRegistry {
        async fn get_chain_listener_state(
            &self,
            _chain_id: u64,
        ) -> Result<Option<ChainListenerState>, crate::registry::Error> {
            if self.fail {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            Ok(self.state.clone())
        }

        async fn update_chain_listener_state(
            &self,
            _chain_id: u64,
            _cursor: &Cursor,
            _last_processed_block_timestamp: Option<u64>,
        ) -> Result<(), crate::registry::Error> {
            Ok(())
        }
    }

    fn listener_state(ts: Option<u64>) -> ChainListenerState {
        ChainListenerState {
            _chain_id: 1337,
            last_processed_block: 1,
            last_processed_id: None,
            last_processed_block_timestamp: ts,
        }
    }

    /// Asserts `got` was produced by `now_secs()` between `before` and now.
    fn assert_wall_clock(got: u64, before: u64) {
        assert!(
            got >= before && got <= now_secs(),
            "expected wall clock, got {got}"
        );
    }

    #[tokio::test]
    async fn bypass_off_uses_wall_clock() {
        let before = now_secs();
        let got = resolve_deadline_clock(
            false,
            Some(1337),
            &MockRegistry {
                state: Some(listener_state(Some(42))),
                fail: false,
            },
            &MockChainClient { head: Some(9) },
        )
        .await
        .unwrap();
        assert_wall_clock(got, before);
    }

    #[tokio::test]
    async fn bypass_prefers_live_chain_head() {
        let got = resolve_deadline_clock(
            true,
            Some(1337),
            &MockRegistry {
                state: Some(listener_state(Some(999))),
                fail: false,
            },
            &MockChainClient { head: Some(12_345) },
        )
        .await
        .unwrap();
        assert_eq!(got, 12_345, "live head must win over listener state");
    }

    #[tokio::test]
    async fn head_failure_falls_back_to_listener_state() {
        let got = resolve_deadline_clock(
            true,
            Some(1337),
            &MockRegistry {
                state: Some(listener_state(Some(4_242))),
                fail: false,
            },
            &MockChainClient { head: None },
        )
        .await
        .unwrap();
        assert_eq!(got, 4_242, "listener state is the second preference");
    }

    #[tokio::test]
    async fn head_failure_without_chain_id_falls_back_to_wall_clock() {
        let before = now_secs();
        let got = resolve_deadline_clock(
            true,
            None,
            &MockRegistry {
                state: None,
                fail: false,
            },
            &MockChainClient { head: None },
        )
        .await
        .unwrap();
        assert_wall_clock(got, before);
    }

    #[tokio::test]
    async fn head_failure_without_listener_state_falls_back_to_wall_clock() {
        let before = now_secs();
        let got = resolve_deadline_clock(
            true,
            Some(1337),
            &MockRegistry {
                state: None,
                fail: false,
            },
            &MockChainClient { head: None },
        )
        .await
        .unwrap();
        assert_wall_clock(got, before);
    }

    #[tokio::test]
    async fn head_failure_with_registry_error_retries_the_job() {
        let got = resolve_deadline_clock(
            true,
            Some(1337),
            &MockRegistry {
                state: None,
                fail: true,
            },
            &MockChainClient { head: None },
        )
        .await;
        match got {
            Err(JobError::Retryable(_, delay)) => assert_eq!(delay, Duration::from_secs(5)),
            other => panic!("expected a retryable job error, got {other:?}"),
        }
    }
}
