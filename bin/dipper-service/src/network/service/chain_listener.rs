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

use std::{future::Future, time::Duration};

use thegraph_core::alloy::primitives::Address;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use super::chain_events::{AcceptedAgreementEvent, CanceledAgreementEvent, ChainEventSource};
use crate::{
    config::ChainListenerConfig,
    registry::{AgreementRegistry, IndexingAgreementStatus},
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
}

/// State persisted in the database
pub struct ChainListenerState {
    pub chain_id: u64,
    pub last_processed_block: u64,
}

/// Create a new chain listener service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service polls the subgraph for on-chain events and processes them.
pub fn new<R, W, E>(ctx: Ctx<R, W, E>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + ChainListenerStateRegistry + Send + Sync,
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

        let mut timer = tokio::time::interval(config.poll_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Track consecutive failures for adaptive backoff
        let mut consecutive_failures: u32 = 0;
        const MAX_CONSECUTIVE_FAILURES: u32 = 10;
        const FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(5);

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
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
                    }
                }
            };

            // Use the minimum of the two latest blocks to ensure we don't miss events
            let new_block = accepted_result
                .latest_block
                .min(canceled_result.latest_block);
            let total_events = accepted_result.events.len() + canceled_result.events.len();

            if new_block <= last_block && total_events == 0 {
                tracing::debug!(
                    latest_block = new_block,
                    last_block,
                    "No new blocks or events"
                );
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
            }

            // Update last processed block
            if new_block > last_block {
                if let Err(err) = registry
                    .update_chain_listener_state(chain_id, new_block)
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
    R: AgreementRegistry,
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
                indexer = %event.indexer,
                "Agreement marked as AcceptedOnChain"
            );
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
            // Agreement is in an unexpected status (e.g., already AcceptedOnChain, Expired, etc.)
            tracing::debug!(
                agreement_id = %event.agreement_id,
                status = %status,
                "Ignoring acceptance for agreement in status: {status}"
            );
        }
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
        Indexer, IndexingAgreement, IndexingAgreementVoucher as Voucher,
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
            };
            self.state.lock().unwrap().agreements.insert(id, agreement);
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
            _lookback_days: i32,
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
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_requester
                .push(*id);
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
        ) -> RegistryResult<()> {
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
}
