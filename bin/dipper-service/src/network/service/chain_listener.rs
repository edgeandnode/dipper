//! On-chain agreement state reconciler
//!
//! This service polls a subgraph for changes to indexing agreements and
//! reconciles dipper's local view of each agreement against the on-chain
//! truth. Every poll returns agreements whose `lastStateChangeBlock` has
//! advanced since the last processed block; for each returned snapshot
//! dipper computes the transition from local status to remote state and
//! applies it.
//!
//! ## Transitions
//!
//! | Local status       | Remote state        | Action                                                                 |
//! |--------------------|---------------------|------------------------------------------------------------------------|
//! | Created            | Accepted            | mark AcceptedOnChain, run pending cancellations                         |
//! | Created            | CanceledBy*         | mark AcceptedOnChain, run pending cancellations, then mark canceled    |
//! | AcceptedOnChain    | Accepted            | no-op                                                                  |
//! | AcceptedOnChain    | CanceledBy*         | mark canceled (by requester vs indexer based on `canceled_by` address) |
//! | Expired            | Accepted            | mark AcceptedOnChain (recovery), run pending cancellations              |
//! | Expired            | CanceledBy*         | recover to AcceptedOnChain, run pending cancellations, then cancel     |
//! | Rejected           | Accepted            | queue cancel-on-chain (adversarial path, dead code under proposal-first) |
//! | Canceled*          | *                   | no-op (terminal)                                                       |
//!
//! ## Data Source
//!
//! Snapshots come from the indexing-payments-subgraph's aggregated
//! `IndexingAgreement` entity (see [`AgreementStateSnapshot`]). The subgraph
//! exposes `lastStateChangeBlock` for cursor pagination and `canceledBy`
//! as the actual canceler address, which dipper compares against its own
//! signer to distinguish self-initiated cancels from indexer-initiated
//! cancels.

use std::{future::Future, sync::Arc, time::Duration};

use dipper_core::ids::IndexingAgreementId;
use thegraph_core::alloy::primitives::Address;
use tokio::{
    sync::{Notify, mpsc},
    time::MissedTickBehavior,
};

use super::chain_events::{AgreementStateSnapshot, ChainEventSource};
use crate::{
    config::ChainListenerConfig,
    registry::{
        AgreementRegistry, CancelKind, IndexingAgreementStatus, PendingCancellationRegistry,
    },
    worker::service::WorkerQueue,
};

/// Handle for controlling the chain listener service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the chain listener service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the chain listener service
pub struct Ctx<R, W, E> {
    /// Registry for querying and updating agreements
    pub registry: R,
    /// Worker queue for submitting cancellation jobs
    pub worker_queue: W,
    /// Chain event source (subgraph)
    pub event_source: E,
    /// Service configuration
    pub config: ChainListenerConfig,
    /// The payer/signer address (used to identify who initiated cancellations)
    pub signer_address: thegraph_core::alloy::primitives::Address,
    /// Signalled by the worker when proposals are dispatched, waking the listener
    /// from the slow (300s) idle interval so it starts fast-polling immediately.
    pub chain_listener_notify: Arc<Notify>,
}

/// State persisted in the database
#[derive(Clone)]
pub struct ChainListenerState {
    pub _chain_id: u64,
    pub last_processed_block: u64,
    /// Block timestamp (epoch seconds). Used by the expiration service.
    pub last_processed_block_timestamp: Option<u64>,
}

/// Create a new chain listener service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service polls the subgraph for agreement state snapshots
/// and reconciles them against the local DB.
pub fn new<R, W, E>(ctx: Ctx<R, W, E>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + ChainListenerStateRegistry + PendingCancellationRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    E: ChainEventSource,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        event_source,
        config,
        signer_address,
        chain_listener_notify,
    } = ctx;

    let service = async move {
        tracing::info!(
            poll_interval_secs = config.poll_interval.as_secs(),
            endpoint = %config.subgraph_endpoint,
            "chain listener service started"
        );

        let chain_id = config.chain_id;

        // Get initial state from DB or start from block 0
        let mut last_block = match registry.get_chain_listener_state(chain_id).await {
            Ok(Some(state)) => {
                tracing::info!(
                    last_processed_block = state.last_processed_block,
                    "Resuming from last processed block"
                );
                state.last_processed_block
            }
            Ok(None) => {
                tracing::info!("No previous state, starting from block 0");
                0
            }
            Err(err) => {
                tracing::error!(error = %err, "Failed to get chain listener state");
                return Err(err.into());
            }
        };

        // Adaptive polling: fast (5s) when Created agreements exist, slow (5min) when idle.
        let fast_interval = config.poll_interval;
        let slow_interval = Duration::from_secs(300);
        let mut timer = tokio::time::interval(fast_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut using_fast_interval = true;

        // Track consecutive failures for adaptive backoff
        let mut consecutive_failures: u32 = 0;
        const MAX_CONSECUTIVE_FAILURES: u32 = 10;
        const FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(5);

        // Observability: heartbeat and stall detection
        let mut polls_since_last_event: u64 = 0;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        const STALL_WARN_THRESHOLD: u32 = 5;
        const STALL_ERROR_THRESHOLD: u32 = 15;
        const HEARTBEAT_POLLS: u64 = 60; // ~5min at fast rate, every poll at slow rate

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
                _ = chain_listener_notify.notified() => {
                    // Worker dispatched a proposal -- switch to fast polling immediately
                    if !using_fast_interval {
                        timer = tokio::time::interval(fast_interval);
                        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        timer.tick().await;
                        using_fast_interval = true;
                        tracing::info!(
                            interval_secs = fast_interval.as_secs(),
                            "chain listener woken by proposal dispatch, switching to fast polling"
                        );
                    }
                },
            }

            // Adaptive interval: check if there are Created agreements awaiting acceptance
            let has_pending = registry
                .count_active_agreements_by_deployment()
                .await
                .map(|m| !m.is_empty())
                .unwrap_or(false);

            let want_fast = has_pending;
            if want_fast != using_fast_interval {
                let new_interval = if want_fast {
                    fast_interval
                } else {
                    slow_interval
                };
                timer = tokio::time::interval(new_interval);
                timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                timer.tick().await; // consume the immediate first tick
                using_fast_interval = want_fast;
                tracing::info!(
                    interval_secs = new_interval.as_secs(),
                    has_pending_agreements = has_pending,
                    "Chain listener polling interval changed"
                );
            }

            // Apply adaptive backoff on consecutive failures
            if consecutive_failures > 0 {
                let backoff = FAILURE_BACKOFF_BASE
                    .saturating_mul(2u32.saturating_pow(consecutive_failures.min(6)));
                tracing::debug!(
                    consecutive_failures,
                    backoff_secs = backoff.as_secs(),
                    "Applying failure backoff"
                );
                tokio::time::sleep(backoff).await;
            }

            // Fetch changed agreement snapshots since the last processed block
            let result = match event_source.get_changed_agreements(last_block).await {
                Ok(result) => {
                    if consecutive_failures > 0 {
                        tracing::info!(
                            recovered_after = consecutive_failures,
                            "chain listener recovered from consecutive fetch failures"
                        );
                    }
                    consecutive_failures = 0;
                    result
                }
                Err(err) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        tracing::error!(
                            error = %err,
                            consecutive_failures,
                            "Too many consecutive failures fetching changed agreements"
                        );
                    } else {
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            "Failed to fetch changed agreements from subgraph"
                        );
                    }
                    continue;
                }
            };

            let new_block = result.latest_block;
            let new_cursor = result.cursor_block;
            let new_timestamp = result.latest_block_timestamp;
            let total_changes = result.snapshots.len();

            // Stall detection: subgraph head not advancing
            if new_block == last_subgraph_head && new_block > 0 {
                stall_count += 1;
                if stall_count == STALL_ERROR_THRESHOLD {
                    tracing::error!(
                        subgraph_head = new_block,
                        stall_polls = stall_count,
                        "Subgraph appears stalled — on-chain events may be delayed"
                    );
                } else if stall_count == STALL_WARN_THRESHOLD {
                    tracing::warn!(
                        subgraph_head = new_block,
                        stall_polls = stall_count,
                        "Subgraph has not advanced, may be paused or behind"
                    );
                }
            } else if stall_count > 0 && new_block > last_subgraph_head {
                tracing::info!(
                    previous_head = last_subgraph_head,
                    new_head = new_block,
                    stall_polls = stall_count,
                    "Subgraph recovered from stall"
                );
                stall_count = 0;
            }
            last_subgraph_head = new_block;

            if new_block <= last_block && total_changes == 0 {
                polls_since_last_event += 1;
                if polls_since_last_event.is_multiple_of(HEARTBEAT_POLLS) || !using_fast_interval {
                    tracing::info!(
                        last_processed_block = last_block,
                        subgraph_head = new_block,
                        polls_idle = polls_since_last_event,
                        fast_mode = using_fast_interval,
                        "chain listener heartbeat"
                    );
                }
                continue;
            }

            tracing::debug!(
                from_block = last_block + 1,
                to_block = new_block,
                snapshot_count = total_changes,
                "Reconciling agreement snapshots"
            );

            let mut processed = 0;
            let mut errors = 0;

            for snapshot in result.snapshots {
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("chain listener stopping mid-cycle");
                    return Ok(());
                }

                match reconcile_agreement(&snapshot, &registry, &worker_queue, signer_address).await
                {
                    Ok(()) => processed += 1,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            agreement_id = %snapshot.agreement_id,
                            "Failed to reconcile agreement snapshot"
                        );
                        errors += 1;
                    }
                }
            }

            if processed > 0 || errors > 0 {
                tracing::info!(
                    from_block = last_block + 1,
                    to_block = new_block,
                    processed,
                    errors,
                    "Reconciled agreement snapshots"
                );
                polls_since_last_event = 0;
            }

            // Advance to `new_cursor` (not the subgraph head) so a held-back
            // cursor from a parse failure does not skip dropped entities.
            // The `> last_block` guard prevents rewinding when a failure on
            // the first entity of a batch leaves the cursor at its prior value.
            if new_cursor > last_block {
                if let Err(err) = registry
                    .update_chain_listener_state(chain_id, new_cursor, new_timestamp)
                    .await
                {
                    tracing::error!(error = %err, "Failed to update chain listener state");
                }
                last_block = new_cursor;
            }
        }

        tracing::debug!("chain listener service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Reconcile a single agreement snapshot against dipper's local DB.
///
/// Compares the snapshot's remote state to the local `IndexingAgreementStatus`
/// and applies whatever transitions the diff implies. See the module-level
/// transition table for the full mapping.
async fn reconcile_agreement<R, W>(
    snapshot: &AgreementStateSnapshot,
    registry: &R,
    worker_queue: &W,
    signer_address: Address,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    W: WorkerQueue,
{
    tracing::debug!(
        agreement_id = %snapshot.agreement_id,
        indexer = %snapshot.indexer,
        state = ?snapshot.state,
        last_state_change_block = snapshot.last_state_change_block,
        "Reconciling agreement against snapshot"
    );

    let agreement = match registry
        .get_indexing_agreement_by_id(&snapshot.agreement_id)
        .await?
    {
        Some(a) => a,
        None => {
            tracing::debug!(
                agreement_id = %snapshot.agreement_id,
                "Agreement not found (may be from another payer)"
            );
            return Ok(());
        }
    };

    // Adversarial-indexer guard: if local is Rejected but remote reached an
    // accepted state, queue an on-chain cancel so the indexer does not
    // collect payment. Dead code under proposal-first dispatch (dipper does
    // not post offer() after a gRPC rejection, so on-chain acceptance
    // cannot succeed) but kept as a defensive response to any flow
    // violation. Only queue when the agreement is not already canceled
    // remotely — otherwise the cancel tx would just revert against the
    // already-canceled contract state. Runs before the DB transitions
    // below; it enqueues a worker job rather than writing to the agreement
    // row, so it does not need to share the atomic transaction.
    if matches!(agreement.status, IndexingAgreementStatus::Rejected)
        && snapshot.state.reached_accepted()
        && !snapshot.state.is_canceled()
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            indexer = %snapshot.indexer,
            "Rejected agreement accepted on-chain, queuing cancellation"
        );
        worker_queue
            .cancel_rejected_agreement_on_chain(agreement.id)
            .await?;
    }

    // Compute the transitions to apply:
    //
    // - `apply_accept`: local is Created or Expired and remote reached an
    //   accepted state. We don't mark Rejected -> AcceptedOnChain; the
    //   Rejected branch jumps straight to the terminal cancel status
    //   below.
    // - `cancel`: remote is canceled and local is not already in a
    //   terminal cancel. The canceler identity is derived by comparing
    //   snapshot.canceled_by to our signer address.
    //
    // Both are applied atomically via apply_reconciliation so the
    // Accept-then-Cancel-in-one-snapshot path does not leave an
    // intermediate AcceptedOnChain row visible to concurrent readers.
    let apply_accept = matches!(
        agreement.status,
        IndexingAgreementStatus::Created | IndexingAgreementStatus::Expired,
    ) && snapshot.state.reached_accepted();

    let already_terminal_cancel = matches!(
        agreement.status,
        IndexingAgreementStatus::CanceledByRequester | IndexingAgreementStatus::CanceledByIndexer,
    );
    let cancel_kind = if snapshot.state.is_canceled() && !already_terminal_cancel {
        Some(if snapshot.canceled_by == signer_address {
            CancelKind::ByRequester
        } else {
            CancelKind::ByIndexer
        })
    } else {
        None
    };

    if apply_accept || cancel_kind.is_some() {
        let outcome = registry
            .apply_reconciliation(&agreement.id, apply_accept, cancel_kind)
            .await?;

        if outcome.did_accept {
            let (old_status, reason) = match agreement.status {
                IndexingAgreementStatus::Expired => ("EXPIRED", "recovered_expired_on_chain"),
                _ => ("CREATED", "accepted_on_chain"),
            };
            tracing::info!(
                agreement_id = %agreement.id,
                indexing_request_id = %agreement.indexing_request_id,
                old_status,
                new_status = "ACCEPTED_ON_CHAIN",
                reason,
                "agreement state transition"
            );
        }

        if outcome.did_cancel {
            match cancel_kind {
                Some(CancelKind::ByRequester) => tracing::info!(
                    agreement_id = %agreement.id,
                    "Agreement marked as CanceledByRequester (on-chain confirmation)"
                ),
                Some(CancelKind::ByIndexer) => tracing::info!(
                    agreement_id = %agreement.id,
                    indexer = %snapshot.indexer,
                    "Agreement marked as CanceledByIndexer"
                ),
                None => {}
            }
        }

        // Pending-cancellation bookkeeping is a worker-queue enqueue loop
        // plus per-record deletes; it can't sit inside the atomic tx
        // without holding DB locks across network calls. Run it after
        // commit, gated on `did_accept` so it only fires on a fresh
        // AcceptedOnChain write (not on a no-op where the row was
        // already there from a prior crash-recovered run).
        if outcome.did_accept {
            execute_pending_cancellations(&agreement.id, registry, worker_queue).await?;
        }
    } else if already_terminal_cancel && snapshot.state.is_canceled() {
        tracing::debug!(
            agreement_id = %agreement.id,
            status = %agreement.status,
            "Agreement already canceled, ignoring snapshot"
        );
    }

    Ok(())
}

/// Execute pending cancellations linked to a newly-accepted agreement.
///
/// Called from the Created -> AcceptedOnChain and Expired -> AcceptedOnChain
/// transitions. Each pending cancellation record is deleted individually
/// after successful processing. Transient failures retain the record so
/// the next reconcile pass can retry.
async fn execute_pending_cancellations<R, W>(
    agreement_id: &IndexingAgreementId,
    registry: &R,
    worker_queue: &W,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    W: WorkerQueue,
{
    let pending = registry
        .get_pending_cancellations_by_new_agreement(*agreement_id)
        .await?;

    if pending.is_empty() {
        return Ok(());
    }

    let mut transient_failures: u32 = 0;

    for cancellation in &pending {
        let old_agreement = match registry
            .get_indexing_agreement_by_id(&cancellation.old_agreement_id)
            .await?
        {
            Some(a) => a,
            None => {
                tracing::warn!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    "Pending cancellation references non-existent agreement, cleaning up"
                );
                registry
                    .delete_pending_cancellation(*agreement_id, cancellation.old_agreement_id)
                    .await?;
                continue;
            }
        };

        match registry
            .mark_indexing_agreement_as_canceled_by_requester(&cancellation.old_agreement_id)
            .await
        {
            Ok(()) => {}
            Err(crate::registry::Error::NoRecordsUpdated) => {
                tracing::debug!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    "Old agreement already in terminal state, skipping cancellation"
                );
                registry
                    .delete_pending_cancellation(*agreement_id, cancellation.old_agreement_id)
                    .await?;
                continue;
            }
            Err(err) => {
                tracing::error!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    error = %err,
                    "Failed to cancel old agreement, retaining pending cancellation for retry"
                );
                transient_failures += 1;
                continue;
            }
        }

        registry
            .delete_pending_cancellation(*agreement_id, cancellation.old_agreement_id)
            .await?;

        if let Err(err) = worker_queue
            .send_indexing_agreement_cancellation(
                old_agreement.indexer.url,
                cancellation.indexing_request_id,
                cancellation.old_agreement_id,
            )
            .await
        {
            tracing::error!(
                error = %err,
                old_agreement_id = %cancellation.old_agreement_id,
                "Failed to queue cancellation notification for replaced agreement"
            );
        } else {
            tracing::info!(
                new_agreement_id = %agreement_id,
                old_agreement_id = %cancellation.old_agreement_id,
                "Cancelled old agreement after replacement confirmed on-chain"
            );
        }
    }

    if transient_failures > 0 {
        anyhow::bail!(
            "{transient_failures} pending cancellation(s) failed for agreement {}; \
             records retained for retry",
            agreement_id,
        );
    }

    Ok(())
}

/// Trait for chain listener state persistence
#[async_trait::async_trait]
pub trait ChainListenerStateRegistry {
    /// Get the current chain listener state for a chain
    async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> Result<Option<ChainListenerState>, crate::registry::Error>;

    /// Update the chain listener state
    async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        last_processed_block: u64,
        last_processed_block_timestamp: Option<u64>,
    ) -> Result<(), crate::registry::Error>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};
    use time::OffsetDateTime;
    use url::Url;

    use super::{super::chain_events::AgreementState, *};
    use crate::registry::{
        AgreementFeeRate, Indexer, IndexingAgreement, IndexingAgreementTerms as Terms,
        IndexingAgreementTermsMetadata as TermsMetadata, Result as RegistryResult,
    };

    fn test_terms() -> Terms {
        Terms {
            payer: Address::ZERO,
            service_provider: Address::ZERO,
            data_service: Address::ZERO,
            deadline: 0,
            ends_at: 0,
            max_initial_tokens: thegraph_core::alloy::primitives::U256::ZERO,
            max_ongoing_tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
            min_seconds_per_collection: 0,
            max_seconds_per_collection: 0,
            conditions: 0,
            metadata: TermsMetadata {
                tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
                tokens_per_entity_per_second: thegraph_core::alloy::primitives::U256::ZERO,
                subgraph_deployment_id: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                protocol_network: ChainId::from(42161u64),
                chain_id: ChainId::from(1u64),
            },
        }
    }

    fn make_snapshot(
        agreement_id: IndexingAgreementId,
        state: AgreementState,
        canceled_by: Address,
    ) -> AgreementStateSnapshot {
        AgreementStateSnapshot {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            state,
            canceled_by,
            last_state_change_block: 100,
        }
    }

    #[test]
    fn test_handle_clone() {
        let (tx, _rx) = mpsc::channel(1);
        let handle = Handle { tx_stop: tx };
        let _cloned = handle.clone();
    }

    // Mock registry for testing
    #[derive(Clone, Default)]
    struct MockRegistry {
        state: Arc<Mutex<MockRegistryState>>,
    }

    #[derive(Default)]
    struct MockRegistryState {
        agreements: std::collections::HashMap<IndexingAgreementId, IndexingAgreement>,
        marked_accepted_on_chain: Vec<IndexingAgreementId>,
        marked_canceled_by_requester: Vec<IndexingAgreementId>,
        marked_canceled_by_indexer: Vec<IndexingAgreementId>,
        pending_cancellations: std::collections::HashMap<
            IndexingAgreementId,
            Vec<crate::registry::PendingCancellation>,
        >,
        deleted_pending_cancellations: Vec<(IndexingAgreementId, IndexingAgreementId)>,
        fail_cancel_for: std::collections::HashSet<IndexingAgreementId>,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self::default()
        }

        fn add_agreement(&self, id: IndexingAgreementId, status: IndexingAgreementStatus) {
            let terms = test_terms();
            let agreement = IndexingAgreement {
                id,
                nonce_uuid: uuid::Uuid::now_v7(),
                status,
                indexer: Indexer {
                    id: "0x1234567890123456789012345678901234567890"
                        .parse()
                        .unwrap(),
                    url: "http://indexer.test".parse().unwrap(),
                },
                indexing_request_id: IndexingRequestId::new(),
                terms,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                last_block_height: None,
                last_progress_at: None,
                rejection_reason: None,
            };
            self.state.lock().unwrap().agreements.insert(id, agreement);
        }

        fn add_pending_cancellation(
            &self,
            new_agreement_id: IndexingAgreementId,
            old_agreement_id: IndexingAgreementId,
            indexing_request_id: IndexingRequestId,
        ) {
            let pc = crate::registry::PendingCancellation {
                old_agreement_id,
                indexing_request_id,
            };
            self.state
                .lock()
                .unwrap()
                .pending_cancellations
                .entry(new_agreement_id)
                .or_default()
                .push(pc);
        }

        fn fail_cancel_for(&self, id: IndexingAgreementId) {
            self.state.lock().unwrap().fail_cancel_for.insert(id);
        }

        fn was_marked_accepted_on_chain(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_accepted_on_chain
                .contains(id)
        }

        fn was_marked_canceled_by_requester(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_requester
                .contains(id)
        }

        fn was_marked_canceled_by_indexer(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_indexer
                .contains(id)
        }

        fn was_pending_cancellation_deleted(
            &self,
            new_id: &IndexingAgreementId,
            old_id: &IndexingAgreementId,
        ) -> bool {
            self.state
                .lock()
                .unwrap()
                .deleted_pending_cancellations
                .contains(&(*new_id, *old_id))
        }
    }

    #[async_trait::async_trait]
    impl AgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<Option<IndexingAgreement>> {
            Ok(self.state.lock().unwrap().agreements.get(id).cloned())
        }

        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(std::collections::HashMap::new())
        }

        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _signer_lookback_minutes: i32,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(std::collections::HashMap::new())
        }

        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn register_new_indexing_agreement(
            &self,
            _params: crate::registry::NewAgreementParams,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn register_agreement_with_pending_cancellation(
            &self,
            _params: crate::registry::NewAgreementParams,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn mark_indexing_agreement_as_delivery_failed(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_cancel_for.contains(id) {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "simulated transient failure".into(),
                    )),
                ));
            }
            state.marked_canceled_by_requester.push(*id);
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_indexer(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_indexer
                .push(*id);
            Ok(())
        }

        async fn apply_reconciliation(
            &self,
            id: &IndexingAgreementId,
            apply_accept: bool,
            cancel: Option<crate::registry::CancelKind>,
        ) -> RegistryResult<crate::registry::ReconciliationOutcome> {
            // The mock reuses the existing per-transition tracking lists
            // instead of modelling a real Postgres transaction. That's
            // enough for the chain_listener tests, which only assert which
            // transitions were recorded; real transactional behaviour is
            // exercised by dipper_pgregistry's own integration tests.
            let mut state = self.state.lock().unwrap();

            let mut did_accept = false;
            if apply_accept {
                let agreement = state.agreements.get(id);
                if matches!(
                    agreement.map(|a| a.status),
                    Some(IndexingAgreementStatus::Created | IndexingAgreementStatus::Expired),
                ) {
                    state.marked_accepted_on_chain.push(*id);
                    did_accept = true;
                }
            }

            let mut did_cancel = false;
            if let Some(kind) = cancel {
                if state.fail_cancel_for.contains(id) {
                    return Err(crate::registry::Error::BackendError(
                        dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                            "simulated transient failure".into(),
                        )),
                    ));
                }
                match kind {
                    crate::registry::CancelKind::ByRequester => {
                        state.marked_canceled_by_requester.push(*id);
                    }
                    crate::registry::CancelKind::ByIndexer => {
                        state.marked_canceled_by_indexer.push(*id);
                    }
                }
                did_cancel = true;
            }

            Ok(crate::registry::ReconciliationOutcome {
                did_accept,
                did_cancel,
            })
        }

        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
            _chain_timestamp: u64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn mark_indexing_agreement_as_expired(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_rejected(
            &self,
            _id: &IndexingAgreementId,
            _rejection_reason: Option<&str>,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn get_accepted_on_chain_agreements(
            &self,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn update_agreement_sync_progress(
            &self,
            _id: &IndexingAgreementId,
            _block_height: u64,
            _progress_at: time::OffsetDateTime,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn count_active_agreements_by_deployment(
            &self,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>> {
            Ok(std::collections::HashMap::new())
        }

        async fn mark_indexing_agreement_as_abandoned(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreement> {
            Err(crate::registry::Error::NoRecordsUpdated)
        }

        async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl PendingCancellationRegistry for MockRegistry {
        async fn get_pending_cancellations_by_new_agreement(
            &self,
            new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<Vec<crate::registry::PendingCancellation>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .pending_cancellations
                .get(&new_agreement_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn delete_pending_cancellations_by_new_agreement(
            &self,
            new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state
                .lock()
                .unwrap()
                .pending_cancellations
                .remove(&new_agreement_id);
            Ok(())
        }

        async fn delete_pending_cancellation(
            &self,
            new_agreement_id: IndexingAgreementId,
            old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if let Some(pcs) = state.pending_cancellations.get_mut(&new_agreement_id) {
                pcs.retain(|pc| pc.old_agreement_id != old_agreement_id);
            }
            state
                .deleted_pending_cancellations
                .push((new_agreement_id, old_agreement_id));
            Ok(())
        }
    }

    // Mock worker queue
    #[derive(Clone, Default)]
    struct MockWorkerQueue {
        cancel_jobs: Arc<Mutex<Vec<IndexingAgreementId>>>,
    }

    impl MockWorkerQueue {
        fn was_cancellation_queued(&self, id: &IndexingAgreementId) -> bool {
            self.cancel_jobs.lock().unwrap().contains(id)
        }
    }

    #[async_trait::async_trait]
    impl crate::worker::service::WorkerQueue for MockWorkerQueue {
        async fn process_new_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn send_indexing_agreement_cancellation(
            &self,
            _indexer_url: Url,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn process_indexing_request_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn process_indexing_agreement_requester_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn process_indexing_agreement_indexer_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn reassess_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            self.cancel_jobs.lock().unwrap().push(agreement_id);
            Ok(dipper_pgmq::JobId::default())
        }

        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }
    }

    // -- reconcile_agreement tests --

    #[tokio::test]
    async fn test_reconcile_transitions_created_to_accepted() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, Address::ZERO).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_queues_cancellation_for_rejected() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, Address::ZERO).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_rejected_already_canceled_skips_queue_and_marks_local_cancel() {
        // The Rejected agreement was canceled on-chain between polls. Dipper
        // should not queue another cancel job (it would just revert against
        // the already-canceled contract state), and step 2 should drive the
        // local status from Rejected straight to the terminal cancel matching
        // the canceler address.
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, signer_address).await;

        assert!(result.is_ok());
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_ignores_unknown_agreement() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        // Don't add the agreement to the registry

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, Address::ZERO).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_recovers_expired_agreement() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Expired);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id, request_id);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, Address::ZERO).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_marks_canceled_by_indexer() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_address: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            indexer_address,
        );
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, signer_address).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_marks_canceled_by_requester() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, signer_address).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_ignores_already_canceled() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByIndexer);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            Address::ZERO,
        );
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, signer_address).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_applies_accept_then_cancel_in_one_snapshot() {
        // Transient case: dipper polls after both the accept and cancel
        // landed on-chain. Local is still Created, remote is CanceledByPayer.
        // We should run the acceptance-side bookkeeping (pending cancellations)
        // AND mark the agreement as CanceledByRequester.
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id, request_id);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(&snapshot, &registry, &worker_queue, signer_address).await;

        assert!(result.is_ok());
        // Accepted-side bookkeeping ran
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
        // Cancellation applied on top
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
    }

    // -- execute_pending_cancellations tests --

    #[tokio::test]
    async fn test_pending_cancellations_all_succeed_records_deleted() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id_1 = IndexingAgreementId::from_bytes(rand::random());
        let old_id_2 = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_1, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_2, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id_1, request_id);
        registry.add_pending_cancellation(new_id, old_id_2, request_id);

        let result = execute_pending_cancellations(&new_id, &registry, &worker_queue).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_requester(&old_id_1));
        assert!(registry.was_marked_canceled_by_requester(&old_id_2));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_1));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_2));
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_pending_cancellations_transient_failure_retains_record() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_ok = IndexingAgreementId::from_bytes(rand::random());
        let old_fail = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_ok, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_fail, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_ok, request_id);
        registry.add_pending_cancellation(new_id, old_fail, request_id);
        registry.fail_cancel_for(old_fail);

        let result = execute_pending_cancellations(&new_id, &registry, &worker_queue).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("1 pending cancellation(s) failed"),
            "unexpected error: {err_msg}"
        );

        assert!(registry.was_marked_canceled_by_requester(&old_ok));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_ok));

        assert!(!registry.was_marked_canceled_by_requester(&old_fail));
        assert!(!registry.was_pending_cancellation_deleted(&new_id, &old_fail));

        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].old_agreement_id, old_fail);
    }

    #[tokio::test]
    async fn test_pending_cancellations_nonexistent_agreement_cleans_up() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id, request_id);
        // old_id never added -- simulates a stale pending cancellation whose
        // referenced agreement no longer exists.

        let result = execute_pending_cancellations(&new_id, &registry, &worker_queue).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_pending_cancellations_empty_is_noop() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);

        let result = execute_pending_cancellations(&new_id, &registry, &worker_queue).await;

        assert!(result.is_ok());
    }

    // -- notify wakeup integration test --

    #[async_trait::async_trait]
    impl ChainListenerStateRegistry for MockRegistry {
        async fn get_chain_listener_state(
            &self,
            _chain_id: u64,
        ) -> crate::registry::Result<Option<ChainListenerState>> {
            Ok(Some(ChainListenerState {
                _chain_id: 1337,
                last_processed_block: 0,
                last_processed_block_timestamp: None,
            }))
        }

        async fn update_chain_listener_state(
            &self,
            _chain_id: u64,
            _last_processed_block: u64,
            _last_processed_block_timestamp: Option<u64>,
        ) -> crate::registry::Result<()> {
            Ok(())
        }
    }

    /// A mock event source that records when it was polled.
    struct TimingEventSource {
        poll_times: Arc<Mutex<Vec<tokio::time::Instant>>>,
    }

    #[async_trait::async_trait]
    impl super::super::chain_events::ChainEventSource for TimingEventSource {
        async fn get_changed_agreements(
            &self,
            _since_block: u64,
        ) -> Result<
            super::super::chain_events::ChangedAgreementsResult,
            super::super::chain_events::ChainEventError,
        > {
            self.poll_times
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            Ok(super::super::chain_events::ChangedAgreementsResult {
                snapshots: vec![],
                latest_block: 1,
                latest_block_timestamp: Some(1000),
                cursor_block: 1,
            })
        }
    }

    /// Verify that notify_one() wakes the chain_listener from its idle
    /// 300s interval and triggers a poll within a few seconds.
    #[tokio::test]
    async fn test_notify_wakes_listener_from_idle_interval() {
        let poll_times: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::new(Mutex::new(vec![]));
        let notify = Arc::new(tokio::sync::Notify::new());

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: "http://localhost:8000/subgraphs/name/test".parse().unwrap(),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
        };

        let ctx = Ctx {
            registry: MockRegistry::new(),
            worker_queue: MockWorkerQueue::default(),
            event_source: TimingEventSource {
                poll_times: poll_times.clone(),
            },
            config,
            signer_address: Address::ZERO,
            chain_listener_notify: notify.clone(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_before = poll_times.lock().unwrap().len();

        notify.notify_one();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_after = poll_times.lock().unwrap().len();
        assert!(
            polls_after > polls_before,
            "expected at least one poll after notify_one(), got {polls_before} -> {polls_after}"
        );

        handle.stop().await;
        let _ = svc_handle.await;
    }
}
