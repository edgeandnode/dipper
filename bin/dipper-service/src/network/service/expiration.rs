//! Expires `Created` agreements whose RCA deadline has passed on-chain.
//!
//! Compares deadlines against the chain_listener's block timestamp, not wall
//! clock time. Stays dormant when no chain time is available.

use std::{future::Future, time::Duration};

use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    config::ExpirationConfig,
    network::service::chain_listener::ChainListenerStateRegistry,
    registry::{AgreementRegistry, IndexingRequestRegistry, PendingCancellationRegistry},
    worker::service::WorkerQueue,
};

/// Handle for controlling the expiration service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the expiration service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the expiration service
pub struct Ctx<R, W> {
    /// Registry for querying and updating agreements
    pub registry: R,
    /// Worker queue for submitting reassessment jobs
    pub worker_queue: W,
    /// Service configuration
    pub config: ExpirationConfig,
    /// Chain ID for reading block timestamps. `None` = stay dormant.
    pub chain_id: Option<u64>,
}

/// Create a new expiration service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service periodically queries for `Created` agreements past
/// their deadline, marks them as `Expired`, and queues reassessment jobs.
pub fn new<R, W>(ctx: Ctx<R, W>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry
        + IndexingRequestRegistry
        + PendingCancellationRegistry
        + ChainListenerStateRegistry
        + Send
        + Sync,
    W: WorkerQueue + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        config,
        chain_id,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            batch_size = config.batch_size,
            chain_id,
            "expiration service started (using chain time)"
        );

        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Timeouts for individual operations to prevent hangs
        const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
        const DB_UPDATE_TIMEOUT: Duration = Duration::from_secs(10);
        const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(10);
        const DORMANT_WARN_CYCLES: u32 = 10;

        let mut dormant_cycles: u32 = 0;

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            tracing::debug!("starting expiration scan");

            // Fetch chain time from chain_listener state
            let chain_ts = match chain_id {
                None => None,
                Some(cid) => match registry.get_chain_listener_state(cid).await {
                    Ok(Some(state)) => state.last_processed_block_timestamp,
                    Ok(None) => None,
                    Err(err) => {
                        tracing::error!(error = %err, "Failed to get chain listener state");
                        None
                    }
                },
            };

            let Some(chain_ts) = chain_ts else {
                dormant_cycles += 1;
                if dormant_cycles == DORMANT_WARN_CYCLES {
                    tracing::warn!(
                        cycles = dormant_cycles,
                        "Expiration service has no chain timestamp — agreements cannot expire"
                    );
                }
                continue;
            };

            dormant_cycles = 0;

            // Query expired agreements using chain time (with timeout)
            let query_result = tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.get_expired_created_agreements(config.batch_size, chain_ts),
            )
            .await;

            let expired = match query_result {
                Ok(Ok(agreements)) => agreements,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to query expired agreements");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout querying expired agreements");
                    continue;
                }
            };

            if expired.is_empty() {
                tracing::debug!("expiration scan: no expired agreements");
                continue;
            }

            tracing::info!(
                count = expired.len(),
                "expiration scan: processing agreements"
            );

            let mut marked = 0;
            let mut queued = 0;
            let mut failed = 0;

            for agreement in expired {
                // Check for shutdown between updates to stay responsive
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("expiration service stopping mid-cycle");
                    return Ok(());
                }

                // Mark as expired
                let mark_result = tokio::time::timeout(
                    DB_UPDATE_TIMEOUT,
                    registry.mark_indexing_agreement_as_expired(&agreement.id),
                )
                .await;

                match mark_result {
                    Ok(Ok(())) => {
                        marked += 1;
                        tracing::info!(
                            agreement_id = %agreement.id,
                            indexing_request_id = %agreement.indexing_request_id,
                            old_status = "Created",
                            new_status = "Expired",
                            "agreement state transition"
                        );
                        // Clean up pending cancellations: the replacement expired
                        // before on-chain acceptance, so old agreements stay active.
                        if let Err(err) = registry
                            .delete_pending_cancellations_by_new_agreement(agreement.id)
                            .await
                        {
                            tracing::warn!(
                                agreement_id = %agreement.id,
                                error = %err,
                                "failed to clean up pending cancellations for expired agreement"
                            );
                        }
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            error = %err,
                            "failed to mark agreement as expired"
                        );
                        failed += 1;
                        continue; // Don't queue reassessment if mark failed
                    }
                    Err(_) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            "timeout marking agreement as expired"
                        );
                        failed += 1;
                        continue;
                    }
                }

                // Get the indexing request to fetch num_candidates
                let request_result = tokio::time::timeout(
                    DB_QUERY_TIMEOUT,
                    registry.get_indexing_request_by_id(&agreement.indexing_request_id),
                )
                .await;

                let request = match request_result {
                    Ok(Ok(Some(r))) => r,
                    Ok(Ok(None)) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            indexing_request_id = %agreement.indexing_request_id,
                            "indexing request not found for expired agreement"
                        );
                        continue;
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            error = %err,
                            "failed to fetch indexing request for expired agreement"
                        );
                        continue;
                    }
                    Err(_) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            "timeout fetching indexing request for expired agreement"
                        );
                        continue;
                    }
                };

                // Queue reassessment
                let push_result = tokio::time::timeout(
                    QUEUE_PUSH_TIMEOUT,
                    worker_queue.reassess_indexing_request(
                        agreement.indexing_request_id,
                        agreement.terms.metadata.subgraph_deployment_id,
                        agreement.terms.metadata.chain_id,
                        request.num_candidates,
                    ),
                )
                .await;

                match push_result {
                    Ok(Ok(_job_id)) => {
                        queued += 1;
                        tracing::debug!(
                            agreement_id = %agreement.id,
                            indexing_request_id = %agreement.indexing_request_id,
                            "queued reassessment for expired agreement"
                        );
                    }
                    Ok(Err(err)) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            error = %err,
                            "failed to queue reassessment for expired agreement"
                        );
                    }
                    Err(_) => {
                        tracing::warn!(
                            agreement_id = %agreement.id,
                            "timeout queuing reassessment for expired agreement"
                        );
                    }
                }
            }

            tracing::info!(
                marked = marked,
                queued = queued,
                failed = failed,
                "expiration scan completed"
            );
        }

        tracing::debug!("expiration service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use thegraph_core::{
        DeploymentId, IndexerId,
        alloy::primitives::{Address, ChainId},
    };
    use url::Url;

    use super::*;
    use crate::{
        network::service::chain_listener::ChainListenerState,
        registry::{
            AgreementFeeRate, IndexingAgreement, IndexingRequest, PendingCancellation,
            Result as RegistryResult,
        },
        worker::{queue::JobId, service::WorkerQueue},
    };

    #[test]
    fn test_default_config() {
        let config = ExpirationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(90));
        assert_eq!(config.batch_size, 100);
    }

    // -- Mock registry that tracks chain state and expired agreements --

    #[derive(Clone)]
    struct MockExpirationRegistry {
        state: Arc<Mutex<MockState>>,
    }

    struct MockState {
        chain_state: Option<ChainListenerState>,
        expired_agreements: Vec<IndexingAgreement>,
        marked_expired: Vec<IndexingAgreementId>,
        get_expired_calls: Vec<u64>,
    }

    impl MockExpirationRegistry {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(MockState {
                    chain_state: None,
                    expired_agreements: vec![],
                    marked_expired: vec![],
                    get_expired_calls: vec![],
                })),
            }
        }

        fn set_chain_state(&self, state: Option<ChainListenerState>) {
            self.state.lock().unwrap().chain_state = state;
        }

        fn get_expired_calls(&self) -> Vec<u64> {
            self.state.lock().unwrap().get_expired_calls.clone()
        }

        #[allow(dead_code)] // available for future test assertions
        fn marked_expired(&self) -> Vec<IndexingAgreementId> {
            self.state.lock().unwrap().marked_expired.clone()
        }
    }

    #[async_trait]
    impl ChainListenerStateRegistry for MockExpirationRegistry {
        async fn get_chain_listener_state(
            &self,
            _chain_id: u64,
        ) -> RegistryResult<Option<ChainListenerState>> {
            Ok(self.state.lock().unwrap().chain_state.clone())
        }

        async fn update_chain_listener_state(
            &self,
            _chain_id: u64,
            _cursor: &super::super::chain_events::Cursor,
            _last_processed_block_timestamp: Option<u64>,
        ) -> RegistryResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl AgreementRegistry for MockExpirationRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<Option<IndexingAgreement>> {
            unimplemented!()
        }
        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            unimplemented!()
        }
        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _signer_lookback_minutes: i32,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            unimplemented!()
        }
        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            unimplemented!()
        }
        async fn register_new_indexing_agreement(
            &self,
            _params: crate::registry::NewAgreementParams,
        ) -> RegistryResult<IndexingAgreementId> {
            unimplemented!()
        }
        async fn register_agreement_with_pending_cancellation(
            &self,
            _params: crate::registry::NewAgreementParams,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreementId> {
            unimplemented!()
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
        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn mark_indexing_agreement_as_canceled_by_indexer(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn apply_reconciliation(
            &self,
            _id: &IndexingAgreementId,
            _apply_accept: bool,
            _cancel: Option<crate::registry::CancelKind>,
        ) -> RegistryResult<crate::registry::ReconciliationOutcome> {
            unimplemented!()
        }
        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
            chain_timestamp: u64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            let mut state = self.state.lock().unwrap();
            state.get_expired_calls.push(chain_timestamp);
            Ok(state.expired_agreements.drain(..).collect())
        }
        async fn mark_indexing_agreement_as_expired(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state.lock().unwrap().marked_expired.push(*id);
            Ok(())
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
        async fn update_agreement_sync_progress(
            &self,
            _id: &IndexingAgreementId,
            _block_height: u64,
            _progress_at: time::OffsetDateTime,
        ) -> RegistryResult<()> {
            unimplemented!()
        }
        async fn count_active_agreements_by_deployment(
            &self,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>> {
            unimplemented!()
        }
        async fn mark_indexing_agreement_as_abandoned(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreement> {
            unimplemented!()
        }
        async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockExpirationRegistry {
        async fn register_new_indexing_request(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> RegistryResult<IndexingRequestId> {
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
        async fn mark_indexing_request_as_canceled(
            &self,
            _id: &IndexingRequestId,
        ) -> RegistryResult<()> {
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
    impl PendingCancellationRegistry for MockExpirationRegistry {
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
            Ok(())
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
            Ok(vec![])
        }
    }

    #[derive(Clone, Default)]
    struct MockWorkerQueue;

    #[async_trait]
    impl WorkerQueue for MockWorkerQueue {
        async fn process_new_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn process_indexing_request_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn process_indexing_agreement_requester_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn reassess_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }
        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_skips_when_no_chain_id() {
        let registry = MockExpirationRegistry::new();

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue,
            config: ExpirationConfig {
                enabled: true,
                interval: Duration::from_millis(10),
                batch_size: 100,
            },
            chain_id: None,
        };

        let (handle, service) = new(ctx);
        let svc = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.stop().await;
        svc.await.unwrap().unwrap();

        // No chain_id means no expiration queries
        assert!(registry.get_expired_calls().is_empty());
    }

    #[tokio::test]
    async fn test_skips_when_no_chain_state() {
        let registry = MockExpirationRegistry::new();
        // No chain state set -- should skip expiration

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue,
            config: ExpirationConfig {
                enabled: true,
                interval: Duration::from_millis(10),
                batch_size: 100,
            },
            chain_id: Some(1337),
        };

        let (handle, service) = new(ctx);
        let svc = tokio::spawn(service);

        // Let it run a few cycles
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.stop().await;
        svc.await.unwrap().unwrap();

        // get_expired_created_agreements should never have been called
        assert!(registry.get_expired_calls().is_empty());
    }

    #[tokio::test]
    async fn test_skips_when_chain_timestamp_is_none() {
        let registry = MockExpirationRegistry::new();
        // Chain state exists but timestamp is None (pre-migration data)
        registry.set_chain_state(Some(ChainListenerState {
            _chain_id: 1337,
            last_processed_block: 100,
            last_processed_id: None,
            last_processed_block_timestamp: None,
        }));

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue,
            config: ExpirationConfig {
                enabled: true,
                interval: Duration::from_millis(10),
                batch_size: 100,
            },
            chain_id: Some(1337),
        };

        let (handle, service) = new(ctx);
        let svc = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.stop().await;
        svc.await.unwrap().unwrap();

        // get_expired_created_agreements should never have been called
        assert!(registry.get_expired_calls().is_empty());
    }

    #[tokio::test]
    async fn test_passes_chain_timestamp_to_query() {
        let registry = MockExpirationRegistry::new();
        registry.set_chain_state(Some(ChainListenerState {
            _chain_id: 1337,
            last_processed_block: 500,
            last_processed_id: None,
            last_processed_block_timestamp: Some(1700000000),
        }));

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue,
            config: ExpirationConfig {
                enabled: true,
                interval: Duration::from_millis(10),
                batch_size: 100,
            },
            chain_id: Some(1337),
        };

        let (handle, service) = new(ctx);
        let svc = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.stop().await;
        svc.await.unwrap().unwrap();

        // get_expired_created_agreements should have been called with the chain timestamp
        let calls = registry.get_expired_calls();
        assert!(!calls.is_empty());
        assert!(calls.iter().all(|ts| *ts == 1700000000));
    }
}
