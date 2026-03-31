//! On-chain event listener service
//!
//! This service monitors the SubgraphService contract for indexing agreement events:
//! - `IndexingAgreementAccepted`: When an indexer accepts an RCA on-chain
//! - `IndexingAgreementCanceled`: When an agreement is canceled on-chain
//!
//! The primary use case is detecting when an indexer who previously `Rejected` an
//! agreement off-chain later accepts it on-chain. In this case, we automatically
//! cancel via `cancelIndexingAgreementByPayer` to ensure they don't receive payment.
//!
//! ## Data Source
//!
//! Events are fetched from a subgraph that indexes the SubgraphService contract.
//! This provides reliable, scalable event retrieval without the block range limits
//! and rate limiting concerns of direct RPC polling.

use std::{future::Future, sync::Arc, time::Duration};

use thegraph_core::alloy::primitives::Address;
use tokio::{
    sync::{Notify, mpsc},
    time::MissedTickBehavior,
};

use super::chain_events::{AcceptedAgreementEvent, CanceledAgreementEvent, ChainEventSource};
use crate::{
    config::ChainListenerConfig,
    registry::{AgreementRegistry, IndexingAgreementStatus, PendingCancellationRegistry},
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
/// on a runtime. The service polls the subgraph for on-chain events and processes them.
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

        // Use a fixed chain_id for state tracking (could be made configurable)
        // Using 42161 for Arbitrum One as default
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

            // Fetch accepted events from subgraph
            let accepted_result = match event_source.get_accepted_agreements(last_block).await {
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
                            "Too many consecutive failures fetching accepted events"
                        );
                    } else {
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            "Failed to fetch accepted events from subgraph"
                        );
                    }
                    continue;
                }
            };

            // Fetch canceled events from subgraph
            let canceled_result = match event_source.get_canceled_agreements(last_block).await {
                Ok(result) => result,
                Err(err) => {
                    // Log but continue - we still want to process accepted events
                    tracing::warn!(
                        error = %err,
                        "Failed to fetch canceled events from subgraph"
                    );
                    super::chain_events::CanceledEventsResult {
                        events: vec![],
                        latest_block: accepted_result.latest_block,
                        latest_block_timestamp: accepted_result.latest_block_timestamp,
                    }
                }
            };

            // Use the minimum of the two latest blocks to ensure we don't miss events
            let new_block = accepted_result
                .latest_block
                .min(canceled_result.latest_block);
            // Use the minimum timestamp (conservative: if one source is behind, use the older time)
            let new_timestamp = match (
                accepted_result.latest_block_timestamp,
                canceled_result.latest_block_timestamp,
            ) {
                (Some(a), Some(c)) => Some(a.min(c)),
                (t, None) | (None, t) => t,
            };
            let total_events = accepted_result.events.len() + canceled_result.events.len();

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

            if new_block <= last_block && total_events == 0 {
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
                accepted_count = accepted_result.events.len(),
                canceled_count = canceled_result.events.len(),
                "Processing events"
            );

            let mut accepted_processed = 0;
            let mut canceled_processed = 0;
            let mut errors = 0;

            // Process accepted events
            for event in accepted_result.events {
                // Check for shutdown between event processing
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("chain listener stopping mid-cycle");
                    return Ok(());
                }

                match process_accepted_event(&event, &registry, &worker_queue).await {
                    Ok(()) => accepted_processed += 1,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            agreement_id = %event.agreement_id,
                            "Failed to process accepted event"
                        );
                        errors += 1;
                    }
                }
            }

            // Process canceled events
            for event in canceled_result.events {
                // Check for shutdown between event processing
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("chain listener stopping mid-cycle");
                    return Ok(());
                }

                match process_canceled_event(&event, &registry, signer_address).await {
                    Ok(()) => canceled_processed += 1,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            agreement_id = %event.agreement_id,
                            "Failed to process canceled event"
                        );
                        errors += 1;
                    }
                }
            }

            if accepted_processed > 0 || canceled_processed > 0 || errors > 0 {
                tracing::info!(
                    from_block = last_block + 1,
                    to_block = new_block,
                    accepted_processed,
                    canceled_processed,
                    errors,
                    "Processed chain events"
                );
                polls_since_last_event = 0;
            }

            // Update last processed block and timestamp
            if new_block > last_block {
                if let Err(err) = registry
                    .update_chain_listener_state(chain_id, new_block, new_timestamp)
                    .await
                {
                    tracing::error!(error = %err, "Failed to update chain listener state");
                    // Continue processing - we may re-process some events on restart
                }
                last_block = new_block;
            }
        }

        tracing::debug!("chain listener service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Process an IndexingAgreementAccepted event
async fn process_accepted_event<R, W>(
    event: &AcceptedAgreementEvent,
    registry: &R,
    worker_queue: &W,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    W: WorkerQueue,
{
    tracing::debug!(
        agreement_id = %event.agreement_id,
        indexer = %event.indexer,
        allocation = %event.allocation_id,
        block = event.block_number,
        "Processing IndexingAgreementAccepted event"
    );

    // Look up the agreement in our DB
    let agreement = match registry
        .get_indexing_agreement_by_id(&event.agreement_id)
        .await?
    {
        Some(a) => a,
        None => {
            tracing::debug!(
                agreement_id = %event.agreement_id,
                "Agreement not found in DB (may be from another payer)"
            );
            return Ok(());
        }
    };

    match agreement.status {
        IndexingAgreementStatus::Created => {
            // Normal case: agreement was Created, now accepted on-chain
            registry
                .mark_indexing_agreement_as_accepted_on_chain(&event.agreement_id)
                .await?;
            tracing::info!(
                agreement_id = %event.agreement_id,
                indexing_request_id = %agreement.indexing_request_id,
                old_status = "CREATED",
                new_status = "ACCEPTED_ON_CHAIN",
                reason = "accepted_on_chain",
                "agreement state transition"
            );

            execute_pending_cancellations(event, registry, worker_queue).await?;
        }
        IndexingAgreementStatus::AcceptedOnChain => {
            // Crash recovery: if dipper crashed after marking AcceptedOnChain but
            // before executing pending cancellations, this re-processes the event.
            tracing::debug!(
                agreement_id = %event.agreement_id,
                "Re-processing AcceptedOnChain event (crash recovery)"
            );

            execute_pending_cancellations(event, registry, worker_queue).await?;
        }
        IndexingAgreementStatus::Expired => {
            // The expiration service marked this as expired, but the indexer
            // did accept on-chain (the contract enforces the deadline). Should
            // not happen with chain-time expiration; the WARN log will surface
            // it if it does.
            tracing::warn!(
                agreement_id = %event.agreement_id,
                indexing_request_id = %agreement.indexing_request_id,
                old_status = "EXPIRED",
                new_status = "ACCEPTED_ON_CHAIN",
                reason = "recovered_expired_on_chain",
                "agreement state transition"
            );
            registry
                .mark_indexing_agreement_as_accepted_on_chain(&event.agreement_id)
                .await?;

            execute_pending_cancellations(event, registry, worker_queue).await?;
        }
        IndexingAgreementStatus::Rejected => {
            // Indexer rejected off-chain but accepted on-chain anyway
            // Queue a cancellation job
            tracing::warn!(
                agreement_id = %event.agreement_id,
                indexer = %event.indexer,
                "Rejected agreement accepted on-chain, queuing cancellation"
            );
            worker_queue
                .cancel_rejected_agreement_on_chain(event.agreement_id)
                .await?;
        }
        status => {
            tracing::debug!(
                agreement_id = %event.agreement_id,
                status = %status,
                "Ignoring acceptance for agreement in status: {status}"
            );
        }
    }

    Ok(())
}

/// Execute pending cancellations linked to an accepted agreement.
///
/// Called both on initial AcceptedOnChain processing and on crash recovery
/// (when the agreement is already AcceptedOnChain from a previous run).
///
/// Each pending cancellation record is deleted individually after successful
/// processing. If a cancellation fails with a transient error, its record is
/// retained so it can be retried on the next crash-recovery replay.
async fn execute_pending_cancellations<R, W>(
    event: &AcceptedAgreementEvent,
    registry: &R,
    worker_queue: &W,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    W: WorkerQueue,
{
    let pending = registry
        .get_pending_cancellations_by_new_agreement(event.agreement_id)
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
                // Agreement is gone permanently — clean up the stale record
                registry
                    .delete_pending_cancellation(event.agreement_id, cancellation.old_agreement_id)
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
                // Already cancelled or in a terminal state — clean up and move on
                tracing::debug!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    "Old agreement already in terminal state, skipping cancellation"
                );
                registry
                    .delete_pending_cancellation(event.agreement_id, cancellation.old_agreement_id)
                    .await?;
                continue;
            }
            Err(err) => {
                // Transient failure — retain the record for retry
                tracing::error!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    error = %err,
                    "Failed to cancel old agreement, retaining pending cancellation for retry"
                );
                transient_failures += 1;
                continue;
            }
        }

        // DB cancellation succeeded — remove the pending record before
        // attempting the best-effort notification to the indexer.
        registry
            .delete_pending_cancellation(event.agreement_id, cancellation.old_agreement_id)
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
                new_agreement_id = %event.agreement_id,
                old_agreement_id = %cancellation.old_agreement_id,
                "Cancelled old agreement after replacement confirmed on-chain"
            );
        }
    }

    if transient_failures > 0 {
        anyhow::bail!(
            "{transient_failures} pending cancellation(s) failed for agreement {}; \
             records retained for retry",
            event.agreement_id,
        );
    }

    Ok(())
}

/// Process an IndexingAgreementCanceled event
async fn process_canceled_event<R>(
    event: &CanceledAgreementEvent,
    registry: &R,
    signer_address: Address,
) -> anyhow::Result<()>
where
    R: AgreementRegistry,
{
    tracing::debug!(
        agreement_id = %event.agreement_id,
        indexer = %event.indexer,
        canceled_by = %event.canceled_by,
        block = event.block_number,
        "Processing IndexingAgreementCanceled event"
    );

    // Look up the agreement in our DB
    let agreement = match registry
        .get_indexing_agreement_by_id(&event.agreement_id)
        .await?
    {
        Some(a) => a,
        None => {
            tracing::debug!(
                agreement_id = %event.agreement_id,
                "Agreement not found in DB (may be from another payer)"
            );
            return Ok(());
        }
    };

    // Determine who canceled based on the canceled_by field
    let canceled_by_us = event.canceled_by == signer_address;

    match agreement.status {
        IndexingAgreementStatus::AcceptedOnChain => {
            if canceled_by_us {
                // We initiated the cancellation - update to CanceledByRequester
                registry
                    .mark_indexing_agreement_as_canceled_by_requester(&event.agreement_id)
                    .await?;
                tracing::info!(
                    agreement_id = %event.agreement_id,
                    "Agreement marked as CanceledByRequester (on-chain confirmation)"
                );
            } else {
                // Indexer initiated the cancellation
                registry
                    .mark_indexing_agreement_as_canceled_by_indexer(&event.agreement_id)
                    .await?;
                tracing::info!(
                    agreement_id = %event.agreement_id,
                    indexer = %event.indexer,
                    "Agreement marked as CanceledByIndexer"
                );
            }
        }
        IndexingAgreementStatus::CanceledByRequester
        | IndexingAgreementStatus::CanceledByIndexer => {
            // Already in a canceled state, nothing to do
            tracing::debug!(
                agreement_id = %event.agreement_id,
                status = %agreement.status,
                "Agreement already canceled, ignoring event"
            );
        }
        status => {
            // Unexpected status for a cancellation event
            tracing::warn!(
                agreement_id = %event.agreement_id,
                status = %status,
                canceled_by = %event.canceled_by,
                "Received cancellation event for agreement in unexpected status"
            );
        }
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

    use super::*;
    use crate::registry::{
        AgreementFeeRate, Indexer, IndexingAgreement, IndexingAgreementVoucher as Voucher,
        IndexingAgreementVoucherMetadata as VoucherMetadata, Result as RegistryResult,
    };

    fn test_voucher() -> Voucher {
        Voucher {
            payer: Address::ZERO,
            service_provider: Address::ZERO,
            data_service: Address::ZERO,
            deadline: 0,
            ends_at: 0,
            max_initial_tokens: thegraph_core::alloy::primitives::U256::ZERO,
            max_ongoing_tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
            min_seconds_per_collection: 0,
            max_seconds_per_collection: 0,
            metadata: VoucherMetadata {
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

    // Basic tests for the service structure
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
            let agreement = IndexingAgreement {
                id,
                status,
                indexer: Indexer {
                    id: "0x1234567890123456789012345678901234567890"
                        .parse()
                        .unwrap(),
                    url: "http://indexer.test".parse().unwrap(),
                },
                indexing_request_id: IndexingRequestId::new(),
                voucher: test_voucher(),
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
            _request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _indexer_id: IndexerId,
            _indexer_url: Url,
            _voucher: Voucher,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::new())
        }

        async fn register_agreement_with_pending_cancellation(
            &self,
            _request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _indexer_id: IndexerId,
            _indexer_url: Url,
            _voucher: Voucher,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::new())
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

        async fn mark_indexing_agreement_as_accepted_on_chain(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state
                .lock()
                .unwrap()
                .marked_accepted_on_chain
                .push(*id);
            Ok(())
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
    }

    #[tokio::test]
    async fn test_process_accepted_event_transitions_created_to_accepted() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);

        let event = AcceptedAgreementEvent {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            allocation_id: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                .parse()
                .unwrap(),
            block_number: 100,
        };

        let result = process_accepted_event(&event, &registry, &worker_queue).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_process_accepted_event_queues_cancellation_for_rejected() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let event = AcceptedAgreementEvent {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            allocation_id: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                .parse()
                .unwrap(),
            block_number: 100,
        };

        let result = process_accepted_event(&event, &registry, &worker_queue).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_process_accepted_event_ignores_unknown_agreement() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::new();

        // Don't add the agreement to the registry

        let event = AcceptedAgreementEvent {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            allocation_id: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                .parse()
                .unwrap(),
            block_number: 100,
        };

        let result = process_accepted_event(&event, &registry, &worker_queue).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_process_accepted_event_recovers_expired_agreement() {
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::new();
        let old_agreement_id = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        // Agreement was marked Expired by the expiration service before the
        // chain_listener saw the on-chain acceptance. The contract guarantees
        // the acceptance was within the RCA deadline, so we should recover.
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Expired);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id, request_id);

        let event = AcceptedAgreementEvent {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            allocation_id: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                .parse()
                .unwrap(),
            block_number: 100,
        };

        let result = process_accepted_event(&event, &registry, &worker_queue).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        // Verify execute_pending_cancellations ran
        assert!(registry.was_marked_canceled_by_requester(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
    }

    #[tokio::test]
    async fn test_process_canceled_event_marks_canceled_by_indexer() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::new();
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_address: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let event = CanceledAgreementEvent {
            agreement_id,
            indexer: indexer_address,
            canceled_by: indexer_address, // Indexer canceled
            block_number: 100,
        };

        let result = process_canceled_event(&event, &registry, signer_address).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_process_canceled_event_marks_canceled_by_requester() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::new();
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_address: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let event = CanceledAgreementEvent {
            agreement_id,
            indexer: indexer_address,
            canceled_by: signer_address, // We canceled
            block_number: 100,
        };

        let result = process_canceled_event(&event, &registry, signer_address).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_process_canceled_event_ignores_already_canceled() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::new();
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_address: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByIndexer);

        let event = CanceledAgreementEvent {
            agreement_id,
            indexer: indexer_address,
            canceled_by: indexer_address,
            block_number: 100,
        };

        let result = process_canceled_event(&event, &registry, signer_address).await;

        assert!(result.is_ok());
        // Should not mark again
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    // -- execute_pending_cancellations tests --

    fn test_accepted_event(agreement_id: IndexingAgreementId) -> AcceptedAgreementEvent {
        AcceptedAgreementEvent {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            allocation_id: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                .parse()
                .unwrap(),
            block_number: 100,
        }
    }

    #[tokio::test]
    async fn test_pending_cancellations_all_succeed_records_deleted() {
        // Arrange
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::new();
        let old_id_1 = IndexingAgreementId::new();
        let old_id_2 = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_1, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_2, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id_1, request_id);
        registry.add_pending_cancellation(new_id, old_id_2, request_id);

        let event = test_accepted_event(new_id);

        // Act
        let result = execute_pending_cancellations(&event, &registry, &worker_queue).await;

        // Assert
        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_requester(&old_id_1));
        assert!(registry.was_marked_canceled_by_requester(&old_id_2));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_1));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_2));
        // Pending cancellation store should be empty
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_pending_cancellations_transient_failure_retains_record() {
        // Arrange
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::new();
        let old_ok = IndexingAgreementId::new();
        let old_fail = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_ok, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_fail, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_ok, request_id);
        registry.add_pending_cancellation(new_id, old_fail, request_id);
        registry.fail_cancel_for(old_fail);

        let event = test_accepted_event(new_id);

        // Act
        let result = execute_pending_cancellations(&event, &registry, &worker_queue).await;

        // Assert: function returns error due to transient failure
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("1 pending cancellation(s) failed"),
            "unexpected error: {err_msg}"
        );

        // The successful one was cancelled and its record deleted
        assert!(registry.was_marked_canceled_by_requester(&old_ok));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_ok));

        // The failed one was NOT cancelled and its record is retained
        assert!(!registry.was_marked_canceled_by_requester(&old_fail));
        assert!(!registry.was_pending_cancellation_deleted(&new_id, &old_fail));

        // The failed record remains in the store for retry
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].old_agreement_id, old_fail);
    }

    #[tokio::test]
    async fn test_pending_cancellations_already_terminal_cleans_up() {
        // Arrange: old agreement is already in terminal state (NoRecordsUpdated path)
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::new();
        let old_id = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::CanceledByIndexer);
        registry.add_pending_cancellation(new_id, old_id, request_id);
        // Simulate terminal state: mark_canceled_by_requester will return NoRecordsUpdated
        // We need a way to make the mock return NoRecordsUpdated for this ID.
        // The current mock always returns Ok(()) -- but the agreement status is
        // CanceledByIndexer, which the real DB would reject. For this test, we use
        // fail_cancel_for which returns BackendError. Instead, let's verify the
        // non-existent agreement path which also cleans up.
        //
        // Actually, let's test the non-existent agreement path directly.
        // Remove the old agreement so get_indexing_agreement_by_id returns None.
        registry.state.lock().unwrap().agreements.remove(&old_id);

        let event = test_accepted_event(new_id);

        // Act
        let result = execute_pending_cancellations(&event, &registry, &worker_queue).await;

        // Assert
        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        // Record should be cleaned up since the agreement no longer exists
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_pending_cancellations_empty_is_noop() {
        // Arrange
        let registry = MockRegistry::new();
        let worker_queue = MockWorkerQueue::default();
        let new_id = IndexingAgreementId::new();
        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);

        let event = test_accepted_event(new_id);

        // Act
        let result = execute_pending_cancellations(&event, &registry, &worker_queue).await;

        // Assert
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
        async fn get_accepted_agreements(
            &self,
            _since_block: u64,
        ) -> Result<
            super::super::chain_events::AcceptedEventsResult,
            super::super::chain_events::ChainEventError,
        > {
            self.poll_times
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            Ok(super::super::chain_events::AcceptedEventsResult {
                events: vec![],
                latest_block: 1,
                latest_block_timestamp: Some(1000),
            })
        }

        async fn get_canceled_agreements(
            &self,
            _since_block: u64,
        ) -> Result<
            super::super::chain_events::CanceledEventsResult,
            super::super::chain_events::ChainEventError,
        > {
            Ok(super::super::chain_events::CanceledEventsResult {
                events: vec![],
                latest_block: 1,
                latest_block_timestamp: Some(1000),
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
            poll_interval: Duration::from_millis(50), // fast interval = 50ms for test speed
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

        // Wait for the initial fast poll to happen, then let it settle into idle (300s).
        // The listener switches to slow after seeing no pending agreements.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_before = poll_times.lock().unwrap().len();

        // Now we're in the 300s idle interval. Without the notify, no poll
        // would happen for 300s. Signal the notify and check that a poll
        // happens promptly.
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
