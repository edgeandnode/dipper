//! Liveness checker for detecting indexers who silently abandon active agreements.
//!
//! After an indexer accepts an agreement on-chain (`AcceptedOnChain`), they may stop
//! indexing without any on-chain signal. This service polls each indexer's status
//! endpoint every 5 minutes and tracks whether the reported block height for the
//! relevant deployment is advancing.
//!
//! If no upward progress is observed for a threshold window — scaled by how many
//! active agreements the deployment has — the agreement is canceled as payer and
//! reassignment is triggered.
//!
//! ## Threshold
//!
//! | Active agreements on deployment | Tolerance |
//! |---------------------------------|-----------|
//! | 1                               | 1 day     |
//! | 2                               | 2 days    |
//! | 3                               | 3 days    |
//! | 4+                              | max days  |
//!
//! ## Progress tracking
//!
//! Block height is queried via `POST {indexer_url}/status` (no auth required).
//! Agreements are grouped by indexer URL so one HTTP call covers all deployments
//! for a given indexer.
//!
//! - Block height increased → reset `last_progress_at`
//! - Block height decreased (resync) → also reset `last_progress_at`
//! - Block height unchanged → check elapsed time against threshold
//! - Indexer unreachable → skip (temporary outage must not trigger cancellation)
//! - Deployment missing from response → treat as no progress

use std::{collections::HashMap, future::Future, time::Duration};

use thegraph_core::DeploymentId;
use time::OffsetDateTime;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use url::Url;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::LivenessCheckerConfig,
    registry::{AgreementRegistry, IndexingAgreement, IndexingRequestRegistry},
    worker::service::WorkerQueue,
};

/// Handle for controlling the liveness checker service lifecycle.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the liveness checker service gracefully.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the liveness checker service.
pub struct Ctx<R, W, C> {
    /// Registry for querying and updating agreements.
    pub registry: R,
    /// Worker queue for submitting reassessment jobs.
    pub worker_queue: W,
    /// Chain client for canceling agreements on-chain.
    pub chain_client: C,
    /// Service configuration.
    pub config: LivenessCheckerConfig,
}

/// Create a new liveness checker service.
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service periodically polls indexer status endpoints and cancels
/// agreements where no indexing progress is observed within the tolerance window.
pub fn new<R, W, C>(ctx: Ctx<R, W, C>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    C: ChainClient + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        chain_client,
        config,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            max_tolerance_days = config.max_tolerance_days,
            batch_size = config.batch_size,
            "liveness checker started"
        );

        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
        const DB_UPDATE_TIMEOUT: Duration = Duration::from_secs(10);
        const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(10);

        let http_client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            tracing::debug!("starting liveness check");

            // 1. Fetch all AcceptedOnChain agreements
            let agreements = match tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.get_accepted_on_chain_agreements(config.batch_size),
            )
            .await
            {
                Ok(Ok(a)) => a,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to query AcceptedOnChain agreements");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout querying AcceptedOnChain agreements");
                    continue;
                }
            };

            if agreements.is_empty() {
                tracing::debug!("liveness check: no AcceptedOnChain agreements");
                continue;
            }

            // 2. Count active agreements per deployment for threshold calculation
            let active_counts = match tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.count_active_agreements_by_deployment(),
            )
            .await
            {
                Ok(Ok(counts)) => counts,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to count active agreements by deployment");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout counting active agreements by deployment");
                    continue;
                }
            };

            tracing::debug!(
                agreement_count = agreements.len(),
                "liveness check: checking agreements"
            );

            // 3. Group agreements by indexer URL for batched status queries
            let groups = group_by_indexer_url(agreements);

            for (indexer_url, group_agreements) in groups {
                // Check for shutdown between indexer groups
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("liveness checker stopping mid-cycle");
                    return Ok(());
                }

                // 4. Query the indexer status endpoint for all deployments in this group
                let deployment_ids: Vec<String> = group_agreements
                    .iter()
                    .map(|a| a.voucher.metadata.subgraph_deployment_id.to_string())
                    .collect();

                let block_heights =
                    match query_indexer_status(&http_client, &indexer_url, &deployment_ids).await {
                        Ok(heights) => heights,
                        Err(err) => {
                            tracing::warn!(
                                indexer_url = %indexer_url,
                                error = %err,
                                "failed to query indexer status, skipping group"
                            );
                            // Unreachable indexer: skip entire group, do not update progress timestamps
                            continue;
                        }
                    };

                // 5. Process each agreement in the group
                for agreement in group_agreements {
                    let deployment_id_str = agreement
                        .voucher
                        .metadata
                        .subgraph_deployment_id
                        .to_string();
                    let current_block = block_heights.get(&deployment_id_str).copied().flatten();

                    let now = OffsetDateTime::now_utc();

                    match agreement.last_block_height {
                        None => {
                            // First check for this agreement: initialize progress tracking.
                            // Use 0 if the deployment is missing from the response so we give
                            // the indexer a full tolerance window before any cancellation.
                            let block = current_block.unwrap_or(0);
                            record_progress(&registry, &agreement, block, now, DB_UPDATE_TIMEOUT)
                                .await;
                        }
                        Some(last) => {
                            let current = current_block.unwrap_or(0);
                            if current != last {
                                // Block changed (up or down): reset clock
                                record_progress(
                                    &registry,
                                    &agreement,
                                    current,
                                    now,
                                    DB_UPDATE_TIMEOUT,
                                )
                                .await;
                            } else {
                                // No change: check elapsed time against threshold
                                let last_progress = agreement.last_progress_at.unwrap_or(now);
                                let elapsed = now - last_progress;
                                let threshold = tolerance_duration(
                                    agreement.voucher.metadata.subgraph_deployment_id,
                                    &active_counts,
                                    config.max_tolerance_days,
                                );

                                if elapsed > threshold {
                                    tracing::warn!(
                                        agreement_id = %agreement.id,
                                        indexer_url = %indexer_url,
                                        deployment = %deployment_id_str,
                                        last_block = last,
                                        elapsed_hours = elapsed.whole_hours(),
                                        threshold_hours = threshold.whole_hours(),
                                        "agreement stale: no indexing progress detected"
                                    );

                                    cancel_and_reassess(
                                        &agreement,
                                        &registry,
                                        &worker_queue,
                                        &chain_client,
                                        DB_UPDATE_TIMEOUT,
                                        QUEUE_PUSH_TIMEOUT,
                                    )
                                    .await;
                                } else {
                                    tracing::debug!(
                                        agreement_id = %agreement.id,
                                        deployment = %deployment_id_str,
                                        last_block = last,
                                        elapsed_hours = elapsed.whole_hours(),
                                        threshold_hours = threshold.whole_hours(),
                                        "agreement block unchanged but within tolerance"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            tracing::debug!("liveness check completed");
        }

        tracing::debug!("liveness checker stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Group agreements by indexer URL for batched status queries.
fn group_by_indexer_url(
    agreements: Vec<IndexingAgreement>,
) -> HashMap<Url, Vec<IndexingAgreement>> {
    let mut groups: HashMap<Url, Vec<IndexingAgreement>> = HashMap::new();
    for agreement in agreements {
        groups
            .entry(agreement.indexer.url.clone())
            .or_default()
            .push(agreement);
    }
    groups
}

/// Compute the tolerance duration for a deployment based on active agreement count.
///
/// Returns `min(active_count, max_days)` days as a duration.
fn tolerance_duration(
    deployment_id: DeploymentId,
    active_counts: &HashMap<DeploymentId, usize>,
    max_tolerance_days: u32,
) -> time::Duration {
    let count = active_counts.get(&deployment_id).copied().unwrap_or(1);
    let days = (count as u32).min(max_tolerance_days).max(1);
    time::Duration::days(days as i64)
}

/// Update sync progress for an agreement in the DB.
async fn record_progress<R>(
    registry: &R,
    agreement: &IndexingAgreement,
    block_height: u64,
    progress_at: OffsetDateTime,
    timeout: Duration,
) where
    R: AgreementRegistry + Send + Sync,
{
    match tokio::time::timeout(
        timeout,
        registry.update_agreement_sync_progress(&agreement.id, block_height, progress_at),
    )
    .await
    {
        Ok(Ok(())) => {
            tracing::debug!(
                agreement_id = %agreement.id,
                block_height,
                "recorded sync progress"
            );
        }
        Ok(Err(err)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to record sync progress"
            );
        }
        Err(_) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                "timeout recording sync progress"
            );
        }
    }
}

/// Cancel a stale agreement on-chain and queue reassessment.
///
/// If the on-chain cancel fails, the DB is not updated and reassessment is not
/// queued, leaving the agreement in `AcceptedOnChain` for the next cycle to retry.
async fn cancel_and_reassess<R, W, C>(
    agreement: &IndexingAgreement,
    registry: &R,
    worker_queue: &W,
    chain_client: &C,
    db_timeout: Duration,
    queue_timeout: Duration,
) where
    R: AgreementRegistry + IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    C: ChainClient + Send + Sync,
{
    // 1. Cancel on-chain
    match chain_client
        .cancel_indexing_agreement_by_payer(agreement.id)
        .await
    {
        Ok(tx_hash) => {
            tracing::info!(
                agreement_id = %agreement.id,
                tx_hash = %tx_hash,
                "canceled stale agreement on-chain"
            );
        }
        Err(ChainClientError::ConfigError(_)) => {
            // Chain client disabled: still proceed to mark and reassess so the
            // DB reflects the detected abandonment even without an on-chain tx.
            tracing::warn!(
                agreement_id = %agreement.id,
                "chain client not configured, skipping on-chain cancellation"
            );
        }
        Err(err) => {
            tracing::error!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to cancel stale agreement on-chain, will retry next cycle"
            );
            return;
        }
    }

    // 2. Mark as abandoned in DB
    let abandoned = match tokio::time::timeout(
        db_timeout,
        registry.mark_indexing_agreement_as_abandoned(&agreement.id),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(err)) => {
            tracing::error!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to mark agreement as abandoned"
            );
            return;
        }
        Err(_) => {
            tracing::error!(
                agreement_id = %agreement.id,
                "timeout marking agreement as abandoned"
            );
            return;
        }
    };

    // 3. Fetch the indexing request for num_candidates
    let request = match tokio::time::timeout(
        db_timeout,
        registry.get_indexing_request_by_id(&abandoned.indexing_request_id),
    )
    .await
    {
        Ok(Ok(Some(r))) => r,
        Ok(Ok(None)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                indexing_request_id = %abandoned.indexing_request_id,
                "indexing request not found for abandoned agreement"
            );
            return;
        }
        Ok(Err(err)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to fetch indexing request for abandoned agreement"
            );
            return;
        }
        Err(_) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                "timeout fetching indexing request for abandoned agreement"
            );
            return;
        }
    };

    // 4. Queue reassessment
    let push_result = tokio::time::timeout(
        queue_timeout,
        worker_queue.reassess_indexing_request(
            abandoned.indexing_request_id,
            abandoned.voucher.metadata.subgraph_deployment_id,
            abandoned.voucher.metadata.chain_id,
            request.num_candidates,
        ),
    )
    .await;

    match push_result {
        Ok(Ok(_job_id)) => {
            tracing::info!(
                agreement_id = %agreement.id,
                indexing_request_id = %abandoned.indexing_request_id,
                "queued reassessment for abandoned agreement"
            );
        }
        Ok(Err(err)) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "failed to queue reassessment for abandoned agreement"
            );
        }
        Err(_) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                "timeout queuing reassessment for abandoned agreement"
            );
        }
    }
}

/// Query a single indexer's status endpoint for sync progress.
///
/// Returns a map of deployment ID (IPFS hash string) to the latest block height,
/// or `None` if the deployment is not found in the response.
///
/// Returns an error only when the HTTP call itself fails (network error, timeout,
/// non-2xx response). GraphQL-level errors are logged and treated as empty results.
async fn query_indexer_status(
    client: &reqwest::Client,
    indexer_url: &Url,
    deployment_ids: &[String],
) -> anyhow::Result<HashMap<String, Option<u64>>> {
    let status_url = indexer_url
        .join("status")
        .map_err(|e| anyhow::anyhow!("failed to construct status URL from {indexer_url}: {e}"))?;

    // Build GraphQL query listing all deployment IDs
    let ids_str = deployment_ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "{{ indexingStatuses(subgraphs: [{ids_str}]) {{ subgraph chains {{ latestBlock {{ number }} }} }} }}"
    );

    let body = serde_json::json!({ "query": query });

    let response = client
        .post(status_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;

    let json: serde_json::Value = response.json().await?;

    let mut result: HashMap<String, Option<u64>> = HashMap::new();

    let statuses = match json
        .get("data")
        .and_then(|d| d.get("indexingStatuses"))
        .and_then(|s| s.as_array())
    {
        Some(arr) => arr,
        None => {
            tracing::debug!(
                indexer_url = %indexer_url,
                "indexingStatuses not present in response"
            );
            return Ok(result);
        }
    };

    for status in statuses {
        let subgraph = match status.get("subgraph").and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        // Extract latestBlock.number from the first chain entry
        let block_number = status
            .get("chains")
            .and_then(|c| c.as_array())
            .and_then(|arr| arr.first())
            .and_then(|chain| chain.get("latestBlock"))
            .and_then(|lb| lb.get("number"))
            .and_then(|n| {
                // latestBlock.number is a BigInt — may be string or number in JSON
                if let Some(s) = n.as_str() {
                    s.parse::<u64>().ok()
                } else {
                    n.as_u64()
                }
            });

        result.insert(subgraph, block_number);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use thegraph_core::DeploymentId;

    use super::{group_by_indexer_url, tolerance_duration};

    #[test]
    fn test_threshold_scales_with_active_agreements() {
        let deployment: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();

        let max = 4u32;

        // 1 active agreement → 1 day
        let counts = HashMap::from([(deployment, 1usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 1);

        // 2 active agreements → 2 days
        let counts = HashMap::from([(deployment, 2usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 2);

        // 3 active agreements → 3 days
        let counts = HashMap::from([(deployment, 3usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 3);

        // 4 active agreements → capped at max (4 days)
        let counts = HashMap::from([(deployment, 4usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 4);

        // 10 active agreements → still capped at max
        let counts = HashMap::from([(deployment, 10usize)]);
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 4);

        // Deployment not found → defaults to 1 day
        let counts = HashMap::new();
        assert_eq!(tolerance_duration(deployment, &counts, max).whole_days(), 1);
    }
}
