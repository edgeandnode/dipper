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

use tokio::{sync::mpsc, time::MissedTickBehavior};

use super::chain_events::{AcceptedAgreementEvent, ChainEventSource};
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

            // Fetch events from subgraph
            let result = match event_source.get_accepted_agreements(last_block).await {
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
                            "Too many consecutive failures, continuing with backoff"
                        );
                    } else {
                        tracing::warn!(
                            error = %err,
                            consecutive_failures,
                            "Failed to fetch events from subgraph"
                        );
                    }
                    continue;
                }
            };

            let new_block = result.latest_block;
            if new_block <= last_block && result.events.is_empty() {
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
                event_count = result.events.len(),
                "Processing events"
            );

            let mut processed = 0;
            let mut errors = 0;

            for event in result.events {
                // Check for shutdown between event processing
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("chain listener stopping mid-cycle");
                    return Ok(());
                }

                match process_accepted_event(&event, &registry, &worker_queue).await {
                    Ok(()) => processed += 1,
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

            if processed > 0 || errors > 0 {
                tracing::info!(
                    from_block = last_block + 1,
                    to_block = new_block,
                    processed,
                    errors,
                    "Processed acceptance events"
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
    use super::*;

    // Basic tests for the service structure
    #[test]
    fn test_handle_clone() {
        let (tx, _rx) = mpsc::channel(1);
        let handle = Handle { tx_stop: tx };
        let _cloned = handle.clone();
    }
}
