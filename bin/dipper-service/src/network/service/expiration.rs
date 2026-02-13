//! Deadline expiration service for stale Created agreements
//!
//! This service periodically scans for `Created` agreements whose RCA deadline
//! has passed. Once the deadline expires, the indexer can no longer accept on-chain,
//! so we mark these as `Expired` and trigger IISA reassessment to find replacement
//! indexers.

use std::{future::Future, time::Duration};

use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    config::ExpirationConfig,
    registry::{AgreementRegistry, IndexingRequestRegistry},
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
}

/// Create a new expiration service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service periodically queries for `Created` agreements past
/// their deadline, marks them as `Expired`, and queues reassessment jobs.
pub fn new<R, W>(ctx: Ctx<R, W>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        config,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            batch_size = config.batch_size,
            "expiration service started"
        );

        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Timeouts for individual operations to prevent hangs
        const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
        const DB_UPDATE_TIMEOUT: Duration = Duration::from_secs(10);
        const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(10);

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            tracing::debug!("starting expiration scan");

            // Query expired agreements (with timeout)
            let query_result = tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.get_expired_created_agreements(config.batch_size),
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
                        tracing::debug!(
                            agreement_id = %agreement.id,
                            indexing_request_id = %agreement.indexing_request_id,
                            "marked agreement as expired"
                        );
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
                        agreement.voucher.metadata.subgraph_deployment_id,
                        agreement.voucher.metadata.chain_id,
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
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ExpirationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(90));
        assert_eq!(config.batch_size, 100);
    }
}
