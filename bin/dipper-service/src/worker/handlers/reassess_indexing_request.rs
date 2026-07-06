use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use dipper_core::{ids::IndexingRequestId, time::now_secs};
use dipper_iisa::{CandidateSelection, SelectedIndexer, SelectionError};
use dipper_producer::{events::SubgraphIndexingAgreementEventsProducer, proto};
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
        result::{JobError, JobResult},
        service::WorkerQueue,
    },
};

/// RCA `conditions` bit that flags the RecurringAgreementManager contract as the
/// agreement owner. Matches the on-chain `uint16` width.
const CONDITION_AGREEMENT_OWNER: u16 = 1u16 << 1;

/// Evicts a request's entry from `reassess_locks` on drop when no other job
/// holds or waits on it, keeping the map bounded to in-flight requests.
struct EvictReassessLock {
    locks: crate::worker::context::ReassessLocks,
    id: IndexingRequestId,
}

impl Drop for EvictReassessLock {
    fn drop(&mut self) {
        // strong_count == 1: only the map's own Arc remains (the handler's clone
        // and guard already dropped, no other job cloned it). remove_if re-checks
        // under the shard lock so a concurrent insert can't be lost.
        self.locks
            .remove_if(&self.id, |_, lock| std::sync::Arc::strong_count(lock) == 1);
    }
}

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
    /// Per-request locks that serialise concurrent reassess jobs for the same
    /// request id (see `crate::worker::context::ReassessLocks`).
    pub reassess_locks: crate::worker::context::ReassessLocks,
    pub subgraph_indexing_agreements_events_emitter:
        Arc<dyn SubgraphIndexingAgreementEventsProducer>,
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
    // Declared first so it drops last (after the guard and clone below) and can
    // then evict this request's now-unused lock entry to bound the map.
    let _evict = EvictReassessLock {
        locks: ctx.reassess_locks.clone(),
        id: *indexing_request_id,
    };

    // Serialise reassess jobs for this request id: without it two concurrent
    // jobs compare against the same baseline and both create agreements.
    let request_lock = ctx
        .reassess_locks
        .entry(*indexing_request_id)
        .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
        .clone();

    // If another job already holds this request's lock it is reassessing the
    // same request against current state, so this pass is redundant: retry
    // shortly rather than park the worker loop and its DB connection.
    let _reassess_guard = match request_lock.try_lock() {
        Ok(guard) => guard,
        Err(_) => {
            return Err(JobError::Retryable(
                anyhow::anyhow!("a reassessment is already in progress for this request"),
                Duration::from_secs(1),
            ));
        }
    };

    // Gather load balancing context for IISA, including chain/ceiling info
    let mut context = gather_selection_context(
        &ctx.registry,
        deployment_id,
        ctx.agreement_conf.declined_indexer_lookback_days(),
        ctx.agreement_conf.price_rejection_lookback_days(),
        ctx.agreement_conf.transient_rejection_lookback_minutes(),
        ctx.agreement_conf.uncertain_rejection_lookback_days(),
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
    )
    .await?;

    // IISA returned fewer candidates than requested: emit the lifecycle event so
    // downstream consumers see the shortfall (typically declined/denylisted indexers).
    //
    // Gate on `!to_add.is_empty()` so this only fires when the reassessment is
    // actually trying to fill slots and comes up short -- not on every recurring
    // reassessment of an already-synced request. Reassessment runs periodically and
    // is re-triggered from several lifecycle paths; emitting purely on the standing
    // `selected < requested` supply condition would re-fire the event forever.
    //
    // Kept below `resolve_deadline_clock` -- the only fallible step under it -- so a
    // failed bypass-path read retries the job before this emits, not after.
    if !to_add.is_empty() && target_selected.len() < *num_candidates {
        ctx.subgraph_indexing_agreements_events_emitter
            .produce_subgraph_indexing_agreement_n_indexers_unavailable(
                *deployment_id,
                ctx.signer.chain_id(),
                proto::SubgraphIndexingAgreementNIndexersUnavailable {
                    agreements_requested: *num_candidates as i32,
                    candidates_returned: target_selected.len() as i32,
                },
            );
    }

    // Pre-compute old agreements to cancel so we can pair replacements
    // atomically during registration.
    let old_to_cancel: Vec<_> = active_agreements
        .iter()
        .filter(|a| to_cancel.contains(&a.indexer.id))
        .collect();
    let mut old_iter = old_to_cancel.into_iter();

    let mut successful_new_ids: Vec<dipper_core::ids::IndexingAgreementId> = vec![];
    // 0x addresses of the indexers we successfully sent proposals to this cycle,
    // for the `proposed` lifecycle event emitted after the loop.
    let mut proposed_candidates: Vec<String> = vec![];
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
            proposed_candidates.push(indexer_id.to_string());
        }
    }

    // Offers were sent to the IISA candidates (but none accepted on-chain yet):
    // emit the `proposed` lifecycle event with the candidates proposed this cycle
    // and the acceptance deadline. Recurring reassessments re-emit when new
    // additions go out.
    if !proposed_candidates.is_empty() {
        ctx.subgraph_indexing_agreements_events_emitter
            .produce_subgraph_indexing_agreement_proposed(
                *deployment_id,
                ctx.signer.chain_id(),
                proto::SubgraphIndexingAgreementProposed {
                    candidates: proposed_candidates,
                    request_expires_at: now.saturating_add(ctx.agreement_conf.deadline_seconds()),
                },
            );
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

        let mut on_chain_cancel_tx: Option<String> = None;
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
                    on_chain_cancel_tx = Some(tx_hash.to_string());
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

        // Emit `terminated` only for agreements that were accepted on-chain: those
        // are the genuine on-chain terminations (dipper just submitted the cancel
        // tx above). Never-accepted agreements (`!needs_on_chain_cancel`) were never
        // on-chain, so there is nothing to "terminate". The chain_listener won't
        // emit for this cancel because we pre-mark the row terminal here, so this is
        // the only emit for dipper-initiated reassessment cancels of accepted
        // agreements. Count is read after the local cancel is persisted.
        if needs_on_chain_cancel {
            let remaining = crate::registry::remaining_accepted_indexing_agreements(
                &ctx.registry,
                deployment_id,
            )
            .await;
            ctx.subgraph_indexing_agreements_events_emitter
                .produce_subgraph_indexing_agreement_terminated(
                    *deployment_id,
                    ctx.signer.chain_id(),
                    proto::SubgraphIndexingAgreementTerminated {
                        indexer: old_agreement.indexer.id.to_string(),
                        terminated_at: now_secs(),
                        terminated_by: ctx.agreement_conf.recurring_agreement_manager().to_string(),
                        terminated_tx: on_chain_cancel_tx.unwrap_or_default(),
                        remaining_accepted_indexing_agreements: remaining,
                    },
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

#[cfg(test)]
mod lifecycle_event_tests {
    //! Validates the three lifecycle-event emits of [`handle`]:
    //! `n_indexers_unavailable`, `proposed`, and `terminated`.
    //!
    //! Each test drives the real handler against in-memory mocks of every
    //! generic bound (registry, queue, IISA, chain client) plus a
    //! [`CapturingEventsProducer`] so it can assert exactly which events were
    //! emitted (and with which payloads), and that no others fire.

    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use dipper_iisa::{CandidateSelection, SelectedIndexer, SelectionContext, SelectionError};
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::{
        DeploymentId, IndexerId,
        alloy::{
            primitives::{Address, B256, ChainId, U256},
            sol_types::Eip712Domain,
        },
    };
    use time::OffsetDateTime;
    use url::Url;

    use super::{Ctx, Message, handle};
    use crate::{
        chain_client::{ChainClient, ChainClientError},
        config::IndexingAgreementConfig,
        network::{
            provider::NetworkProviderService,
            service::{
                chain_listener::{ChainListenerState, ChainListenerStateRegistry},
                topology,
            },
        },
        registry::{
            AgreementFeeRate, AgreementRegistry, CancelKind, Indexer, IndexerDenylistRegistry,
            IndexingAgreement, IndexingAgreementStatus, IndexingAgreementTerms,
            IndexingAgreementTermsMetadata, IndexingRequest, IndexingRequestRegistry,
            NewAgreementParams, PendingCancellation, PendingCancellationRegistry,
            ReconciliationItem, ReconciliationOutcome, Result as RegistryResult, SetTargetOutcome,
        },
        signing::eip712::Eip712Signer,
        test_support::{CapturedEvent, CapturingEventsProducer},
    };

    const TEST_PROTOCOL_CHAIN_ID: ChainId = 42161;
    const TEST_DEPLOYMENT_CHAIN_ID: ChainId = 1;

    fn deployment() -> DeploymentId {
        "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap()
    }

    fn indexer_id(byte: u8) -> IndexerId {
        IndexerId::from(Address::repeat_byte(byte))
    }

    fn indexer_url() -> Url {
        Url::parse("https://indexer.example").unwrap()
    }

    // ---- Mock: IISA candidate selection -------------------------------------

    /// Returns a fixed list of [`SelectedIndexer`] regardless of arguments.
    struct MockIisa {
        selected: Vec<SelectedIndexer>,
    }

    #[async_trait]
    impl CandidateSelection for MockIisa {
        async fn select_indexers(
            &self,
            _deployment_id: DeploymentId,
            _num_candidates: usize,
            _context: &SelectionContext,
        ) -> std::result::Result<Vec<SelectedIndexer>, SelectionError> {
            Ok(self.selected.clone())
        }
    }

    // ---- Mock: worker queue --------------------------------------------------

    /// Records every `send_indexing_agreement_proposal` call's indexer URL.
    /// Clone shares the buffer so a caller can inspect proposals after `handle`.
    #[derive(Default, Clone)]
    struct MockQueue {
        proposals: Arc<Mutex<Vec<Url>>>,
    }

    #[async_trait]
    impl crate::worker::service::WorkerQueue for MockQueue {
        async fn send_indexing_agreement_proposal(
            &self,
            candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<crate::worker::queue::JobId> {
            self.proposals.lock().unwrap().push(candidate_url);
            Ok(crate::worker::queue::JobId::default())
        }

        async fn reassess_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<crate::worker::queue::JobId> {
            unimplemented!("not exercised by reassess handler")
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<crate::worker::queue::JobId> {
            unimplemented!("not exercised by reassess handler")
        }

        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<crate::worker::queue::JobId> {
            unimplemented!("not exercised by reassess handler")
        }
    }

    // ---- Mock: chain client --------------------------------------------------

    /// Always reports a successful cancel that the post-cancel read confirms.
    #[derive(Default)]
    struct MockChainClient;

    #[async_trait]
    impl ChainClient for MockChainClient {
        async fn offer_via_manager(
            &self,
            _rca: &RecurringCollectionAgreement,
        ) -> std::result::Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> std::result::Result<Option<B256>, ChainClientError> {
            Ok(Some(B256::repeat_byte(0xcd)))
        }

        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> std::result::Result<Option<B256>, ChainClientError> {
            Ok(None)
        }

        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> std::result::Result<bool, ChainClientError> {
            // Cancel confirmed: agreement is no longer active on-chain.
            Ok(false)
        }
    }

    // ---- Mock: registry (all five traits) -----------------------------------

    /// In-memory registry seeded with the active agreements for the request.
    /// Only the methods the exercised reassess path touches return data; the
    /// rest `unimplemented!()`.
    #[derive(Default)]
    struct MockRegistry {
        active_agreements: Vec<IndexingAgreement>,
        accepted_count: i64,
        /// When true, `get_chain_listener_state` errors, driving `resolve_deadline_clock`
        /// onto its retry path -- used to assert the shortfall event isn't emitted first.
        chain_state_lookup_fails: bool,
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> RegistryResult<SetTargetOutcome> {
            unimplemented!()
        }
        async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_indexing_request_by_id(
            &self,
            _id: &IndexingRequestId,
        ) -> RegistryResult<Option<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingRequest>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl AgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<Option<IndexingAgreement>> {
            unimplemented!()
        }
        // gather_selection_context: all active agreements for the deployment.
        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(self.active_agreements.clone())
        }
        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        // gather_selection_context: no pending agreements.
        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> RegistryResult<HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(HashMap::new())
        }
        // gather_selection_context: no declined indexers.
        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _transient_lookback_minutes: i32,
            _uncertain_lookback_days: i32,
        ) -> RegistryResult<HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(HashMap::new())
        }
        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        // The handler's current-state baseline for the diff.
        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(self.active_agreements.clone())
        }
        // Populates the `terminated` event's remaining count.
        async fn count_accepted_agreements_by_deployment(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<i64> {
            Ok(self.accepted_count)
        }
        async fn register_new_indexing_agreement(
            &self,
            params: NewAgreementParams,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(params.agreement_id)
        }
        async fn register_agreement_with_pending_cancellation(
            &self,
            params: NewAgreementParams,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(params.agreement_id)
        }
        async fn mark_indexing_agreement_as_delivery_failed(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn update_offer_tx_hash(
            &self,
            _id: &IndexingAgreementId,
            _tx_hash: &[u8; 32],
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        // Cancel path: pre-mark the local row terminal.
        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }
        async fn apply_reconciliation(
            &self,
            _id: &IndexingAgreementId,
            _apply_accept: bool,
            _cancel: Option<CancelKind>,
        ) -> RegistryResult<ReconciliationOutcome> {
            unimplemented!()
        }
        async fn apply_reconciliation_batch(
            &self,
            _items: &[ReconciliationItem],
        ) -> RegistryResult<HashMap<IndexingAgreementId, ReconciliationOutcome>> {
            unimplemented!()
        }
        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
            _chain_timestamp: u64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn mark_indexing_agreement_as_expired(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn mark_indexing_agreement_as_rejected(
            &self,
            _id: &IndexingAgreementId,
            _rejection_reason: Option<&str>,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn get_accepted_on_chain_agreements(
            &self,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn get_agreements_pending_chain_cancel(
            &self,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn update_agreement_sync_progress(
            &self,
            _id: &IndexingAgreementId,
            _block_height: u64,
            _progress_at: OffsetDateTime,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn count_active_agreements_by_deployment(
            &self,
        ) -> RegistryResult<HashMap<DeploymentId, usize>> {
            unimplemented!()
        }
        async fn mark_indexing_agreement_as_abandoned(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreement> {
            unimplemented!()
        }
        // gather_selection_context: optimistic DIPs fees (none).
        async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl IndexerDenylistRegistry for MockRegistry {
        // gather_selection_context: empty denylist.
        async fn get_indexer_denylist(&self) -> RegistryResult<Vec<IndexerId>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl PendingCancellationRegistry for MockRegistry {
        async fn get_pending_cancellations_by_new_agreement(
            &self,
            _new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<Vec<PendingCancellation>> {
            unimplemented!()
        }
        async fn delete_pending_cancellations_by_new_agreement(
            &self,
            _new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn delete_pending_cancellation(
            &self,
            _new_agreement_id: IndexingAgreementId,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn list_executable_pending_cancellations(
            &self,
            _limit: i64,
        ) -> RegistryResult<Vec<IndexingAgreementId>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl ChainListenerStateRegistry for MockRegistry {
        async fn get_chain_listener_state(
            &self,
            _chain_id: u64,
        ) -> RegistryResult<Option<ChainListenerState>> {
            if self.chain_state_lookup_fails {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            Ok(None)
        }
        async fn update_chain_listener_state(
            &self,
            _chain_id: u64,
            _cursor: &crate::network::service::chain_events::Cursor,
            _last_processed_block_timestamp: Option<u64>,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
    }

    // ---- Test fixtures -------------------------------------------------------

    fn test_signer() -> Eip712Signer {
        Eip712Signer::new(
            Address::repeat_byte(0xaa),
            TEST_PROTOCOL_CHAIN_ID,
            Eip712Domain::new(
                Some("RecurringCollector".into()),
                Some("1".into()),
                Some(U256::from(TEST_PROTOCOL_CHAIN_ID)),
                Some(Address::repeat_byte(0x42)),
                None,
            ),
        )
    }

    fn test_agreement_conf() -> IndexingAgreementConfig {
        IndexingAgreementConfig {
            data_service: Address::repeat_byte(0x03),
            recurring_collector: Address::repeat_byte(0x04),
            recurring_agreement_manager: Address::repeat_byte(0x05),
            max_agreement_grt_per_30_days: 1000.0,
            max_seconds_per_collection: 3600,
            min_seconds_per_collection: 60,
            duration_seconds: 86400,
            deadline_seconds: 3600,
            max_grt_per_30_days: std::collections::BTreeMap::new(),
            max_grt_per_billion_entities_per_30_days: 0.0,
            declined_indexer_lookback_days: 30,
            price_rejection_lookback_days: 1,
            transient_rejection_lookback_minutes: 30,
            uncertain_rejection_lookback_days: 1,
        }
    }

    fn empty_networks_registry() -> graph_networks_registry::NetworksRegistry {
        graph_networks_registry::NetworksRegistry::from_json(
            r#"{
                "$schema": "https://example/schema.json",
                "description": "test",
                "networks": [],
                "title": "test",
                "updatedAt": "2024-01-01T00:00:00Z",
                "version": "0.0.0"
            }"#,
        )
        .expect("empty registry")
    }

    /// Build a `Ctx` wired with the given mocks plus an empty topology snapshot.
    /// Returns the assembled `Ctx`. `events` is shared (clone) so the caller can
    /// assert on it after `handle` runs.
    #[allow(clippy::type_complexity)]
    fn build_ctx(
        registry: MockRegistry,
        iisa: MockIisa,
        queue: MockQueue,
        chain_client: MockChainClient,
        events: CapturingEventsProducer,
        snapshot: topology::Snapshot,
    ) -> Ctx<MockRegistry, MockQueue, MockIisa, MockChainClient> {
        let network = NetworkProviderService::new(topology::Handle::for_test(snapshot));
        Ctx {
            signer: Arc::new(test_signer()),
            agreement_conf: Arc::new(test_agreement_conf()),
            rca_domain: Arc::new(std::sync::RwLock::new(Eip712Domain::new(
                Some("RecurringCollector".into()),
                Some("1".into()),
                Some(U256::from(TEST_PROTOCOL_CHAIN_ID)),
                Some(Address::repeat_byte(0x42)),
                None,
            ))),
            chain_price: Arc::new(std::collections::BTreeMap::new()),
            registry,
            network,
            queue,
            iisa,
            chain_client,
            networks_registry: Arc::new(empty_networks_registry()),
            additional_networks: Arc::new(std::collections::BTreeMap::new()),
            entity_count_cache: crate::network::service::entity_count_cache::new_cache(),
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
            bypass_chain_clock_defenses: false,
            chain_listener_chain_id: None,
            reassess_locks: Arc::new(dashmap::DashMap::new()),
            subgraph_indexing_agreements_events_emitter: Arc::new(events),
        }
    }

    fn test_message(num_candidates: usize) -> Message {
        Message {
            indexing_request_id: IndexingRequestId::new(),
            deployment_id: deployment(),
            deployment_chain_id: TEST_DEPLOYMENT_CHAIN_ID,
            num_candidates,
        }
    }

    /// Build an active agreement for `indexer` in the given status.
    fn agreement(indexer: IndexerId, status: IndexingAgreementStatus) -> IndexingAgreement {
        IndexingAgreement {
            id: IndexingAgreementId::from_bytes(rand::random()),
            nonce_uuid: uuid::Uuid::now_v7(),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status,
            indexing_request_id: IndexingRequestId::new(),
            indexer: Indexer {
                id: indexer,
                url: indexer_url(),
            },
            terms: IndexingAgreementTerms {
                payer: Address::ZERO,
                service_provider: Address::ZERO,
                data_service: Address::ZERO,
                deadline: 0,
                ends_at: 0,
                max_initial_tokens: U256::ZERO,
                max_ongoing_tokens_per_second: U256::ZERO,
                min_seconds_per_collection: 0,
                max_seconds_per_collection: 0,
                conditions: 0,
                metadata: IndexingAgreementTermsMetadata {
                    tokens_per_second: U256::ZERO,
                    tokens_per_entity_per_second: U256::ZERO,
                    subgraph_deployment_id: deployment(),
                    protocol_network: TEST_PROTOCOL_CHAIN_ID,
                    chain_id: TEST_DEPLOYMENT_CHAIN_ID,
                },
            },
            last_block_height: None,
            last_progress_at: None,
            rejection_reason: None,
            // 32-byte hash so the on-chain cancel path can build a version hash.
            terms_version_hash: Some(vec![7u8; 32]),
        }
    }

    /// A `SelectedIndexer` with IISA-provided pricing so `resolve_pricing` succeeds.
    fn selected(id: IndexerId) -> SelectedIndexer {
        SelectedIndexer {
            id,
            min_grt_per_30_days: Some(100.0),
            min_grt_per_billion_entities_per_30_days: Some(1.0),
        }
    }

    // ---- Scenario 1: n_indexers_unavailable ---------------------------------

    #[tokio::test]
    async fn emits_n_indexers_unavailable_when_iisa_returns_fewer() {
        // Request of 3, IISA returns 1 resolvable candidate, no current
        // agreements: the diff has a real addition (to_add = {new}) AND the
        // selection falls short (1 < 3), so the handler emits exactly one
        // NIndexersUnavailable (alongside the Proposed for the queued add).
        let new_idx = indexer_id(0x21);
        let mut snapshot = topology::Snapshot::new();
        snapshot.insert_indexer_for_test(new_idx, indexer_url());

        let events = CapturingEventsProducer::new();
        let ctx = build_ctx(
            MockRegistry::default(),
            MockIisa {
                selected: vec![selected(new_idx)],
            },
            MockQueue::default(),
            MockChainClient,
            events.clone(),
            snapshot,
        );

        handle(ctx, &test_message(3)).await.expect("handler ok");

        let captured = events.events();
        let unavailable: Vec<_> = captured
            .iter()
            .filter_map(|e| match e {
                CapturedEvent::NIndexersUnavailable {
                    deployment: d,
                    chain_id,
                    event,
                } => Some((d, chain_id, event)),
                _ => None,
            })
            .collect();
        assert_eq!(
            unavailable.len(),
            1,
            "exactly one NIndexersUnavailable: {captured:?}"
        );
        let (d, chain_id, event) = unavailable[0];
        assert_eq!(*d, deployment());
        assert_eq!(*chain_id, TEST_PROTOCOL_CHAIN_ID);
        assert_eq!(event.agreements_requested, 3);
        assert_eq!(event.candidates_returned, 1);
    }

    // ---- Scenario 1b: regression -- standing shortfall on a synced request --

    #[tokio::test]
    async fn no_n_indexers_unavailable_when_deadline_clock_read_fails() {
        // The shortfall event is emitted only after `resolve_deadline_clock`. Under
        // bypass, a failed chain_listener-state read errors out before the emit, so a
        // retried job doesn't re-fire the event for the same round.
        let new_idx = indexer_id(0x21);
        let mut snapshot = topology::Snapshot::new();
        snapshot.insert_indexer_for_test(new_idx, indexer_url());

        let events = CapturingEventsProducer::new();
        let mut ctx = build_ctx(
            MockRegistry {
                chain_state_lookup_fails: true,
                ..Default::default()
            },
            MockIisa {
                selected: vec![selected(new_idx)],
            },
            MockQueue::default(),
            MockChainClient,
            events.clone(),
            snapshot,
        );
        ctx.bypass_chain_clock_defenses = true;
        ctx.chain_listener_chain_id = Some(1337);

        // Request 3, IISA returns 1: a real add plus a shortfall, so the event WOULD
        // fire if the handler reached it -- but the clock read fails first.
        let result = handle(ctx, &test_message(3)).await;

        assert!(
            result.is_err(),
            "a failed deadline-clock read must fail the job"
        );
        assert!(
            events.events().is_empty(),
            "no shortfall event may be emitted before the clock resolves: {:?}",
            events.events()
        );
    }

    #[tokio::test]
    async fn no_n_indexers_unavailable_on_synced_request_with_standing_shortfall() {
        // Reviewer's case: a request whose target is already fully synced
        // (current == target) while IISA keeps returning fewer than requested.
        // The diff is empty (no adds, no cancels), so reassessment is a no-op
        // and MUST NOT re-emit n_indexers_unavailable on every recurring cycle.
        let idx = indexer_id(0x11);
        let events = CapturingEventsProducer::new();
        let registry = MockRegistry {
            active_agreements: vec![agreement(idx, IndexingAgreementStatus::AcceptedOnChain)],
            accepted_count: 1,
            chain_state_lookup_fails: false,
        };
        let ctx = build_ctx(
            registry,
            MockIisa {
                selected: vec![selected(idx)],
            },
            MockQueue::default(),
            MockChainClient,
            events.clone(),
            topology::Snapshot::new(),
        );

        // Requested 3, but the only available indexer is already accepted:
        // to_add and to_cancel are both empty -> standing shortfall, no event.
        handle(ctx, &test_message(3)).await.expect("handler ok");

        assert!(
            events.events().is_empty(),
            "synced request with standing shortfall must not re-emit: {:?}",
            events.events()
        );
    }

    // ---- Scenario 2: negative (no shortfall, empty diff) --------------------

    #[tokio::test]
    async fn no_n_indexers_unavailable_when_target_matches_current() {
        // IISA returns >= num_candidates AND the target equals the current
        // agreements, so the diff is empty: no event of any kind.
        let idx = indexer_id(0x11);
        let events = CapturingEventsProducer::new();
        let registry = MockRegistry {
            active_agreements: vec![agreement(idx, IndexingAgreementStatus::AcceptedOnChain)],
            accepted_count: 1,
            chain_state_lookup_fails: false,
        };
        let ctx = build_ctx(
            registry,
            MockIisa {
                selected: vec![selected(idx)],
            },
            MockQueue::default(),
            MockChainClient,
            events.clone(),
            topology::Snapshot::new(),
        );

        handle(ctx, &test_message(1)).await.expect("handler ok");

        assert!(
            events.events().is_empty(),
            "no events when diff is empty and no shortfall: {:?}",
            events.events()
        );
    }

    // ---- Scenario 3: proposed + terminated ----------------------------------

    #[tokio::test]
    async fn emits_proposed_and_terminated_on_add_and_accepted_cancel() {
        // Target = {new}, current = {old_paired, old_unpaired} (both
        // AcceptedOnChain). With one add and two cancels, the add loop pairs the
        // new agreement with the FIRST old agreement (atomic replacement, no
        // `terminated`), and the second, unpaired old agreement reaches the
        // cancel loop. Because it was AcceptedOnChain it emits `terminated`.
        // Net: exactly one `proposed` + one `terminated`.
        let new_idx = indexer_id(0x22);
        let old_paired = indexer_id(0x33);
        let old_unpaired = indexer_id(0x34);

        // Topology must resolve the new indexer or the add is skipped.
        let mut snapshot = topology::Snapshot::new();
        snapshot.insert_indexer_for_test(new_idx, indexer_url());

        let events = CapturingEventsProducer::new();
        let queue = MockQueue::default();
        let registry = MockRegistry {
            // Vec order drives the pairing: first is paired, second is unpaired.
            active_agreements: vec![
                agreement(old_paired, IndexingAgreementStatus::AcceptedOnChain),
                agreement(old_unpaired, IndexingAgreementStatus::AcceptedOnChain),
            ],
            accepted_count: 0,
            chain_state_lookup_fails: false,
        };
        let ctx = build_ctx(
            registry,
            MockIisa {
                selected: vec![selected(new_idx)],
            },
            queue.clone(),
            MockChainClient,
            events.clone(),
            snapshot,
        );

        // Request size 1 == returned 1, so no shortfall event.
        handle(ctx, &test_message(1)).await.expect("handler ok");

        let captured = events.events();
        assert_eq!(
            captured.len(),
            2,
            "exactly proposed + terminated: {captured:?}"
        );

        let proposed: Vec<_> = captured
            .iter()
            .filter_map(|e| match e {
                CapturedEvent::Proposed { event, .. } => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(proposed.len(), 1, "exactly one Proposed");
        assert_eq!(
            proposed[0].candidates,
            vec![new_idx.to_string()],
            "proposed candidate is the new indexer"
        );

        let terminated: Vec<_> = captured
            .iter()
            .filter_map(|e| match e {
                CapturedEvent::Terminated { event, .. } => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(terminated.len(), 1, "exactly one Terminated");
        assert_eq!(
            terminated[0].indexer,
            old_unpaired.to_string(),
            "terminated is the unpaired AcceptedOnChain agreement"
        );

        // The proposal was actually queued to the new indexer's URL.
        assert_eq!(queue.proposals.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn never_accepted_unpaired_cancel_does_not_emit_terminated() {
        // Both old agreements were never accepted on-chain (Created). One add
        // pairs with the first old agreement; the second, unpaired old agreement
        // reaches the cancel loop but, being never-accepted
        // (`!needs_on_chain_cancel`), must NOT emit `terminated`. The add still
        // emits `proposed`. Net: exactly one event, a `proposed`.
        let new_idx = indexer_id(0x44);
        let old_paired = indexer_id(0x55);
        let old_unpaired = indexer_id(0x56);

        let mut snapshot = topology::Snapshot::new();
        snapshot.insert_indexer_for_test(new_idx, indexer_url());

        let events = CapturingEventsProducer::new();
        let registry = MockRegistry {
            active_agreements: vec![
                agreement(old_paired, IndexingAgreementStatus::Created),
                agreement(old_unpaired, IndexingAgreementStatus::Created),
            ],
            accepted_count: 0,
            chain_state_lookup_fails: false,
        };
        let ctx = build_ctx(
            registry,
            MockIisa {
                selected: vec![selected(new_idx)],
            },
            MockQueue::default(),
            MockChainClient,
            events.clone(),
            snapshot,
        );

        handle(ctx, &test_message(1)).await.expect("handler ok");

        let captured = events.events();
        assert_eq!(
            captured.len(),
            1,
            "only Proposed; never-accepted unpaired cancel emits no Terminated: {captured:?}"
        );
        assert!(matches!(captured[0], CapturedEvent::Proposed { .. }));
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
