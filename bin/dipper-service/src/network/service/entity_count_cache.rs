//! Periodic background service that fetches entity counts from the
//! indexing-payments subgraph and maintains a shared in-memory cache.
//!
//! Worker jobs read from the cache for optimistic fee estimation.
//! The cache is refreshed on a configurable interval (default 1 hour).

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use thegraph_core::{DeploymentId, IndexerId};
use tokio::sync::{RwLock, mpsc};
use url::Url;

/// Default refresh interval for entity counts.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

/// Timeout for subgraph queries.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Composite key for the entity count cache.
pub type EntityCountKey = (IndexerId, DeploymentId);

/// Shared entity count cache, readable by worker jobs.
pub type EntityCountCache = Arc<RwLock<HashMap<EntityCountKey, u64>>>;

/// Create a new empty entity count cache.
pub fn new_cache() -> EntityCountCache {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Handle for controlling the entity count cache service.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the service gracefully.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context for the entity count cache service.
pub struct Ctx {
    /// The shared cache to populate.
    pub cache: EntityCountCache,
    /// Subgraph endpoint URL.
    pub endpoint: Url,
    /// Refresh interval.
    pub interval: Duration,
}

/// Create a new entity count cache service.
///
/// Returns a handle for lifecycle control and a future to spawn. The
/// service fetches all `IndexerDeploymentLatest` entities from the
/// subgraph on a fixed interval and populates the shared cache.
pub fn new(ctx: Ctx) -> (Handle, impl Future<Output = anyhow::Result<()>>) {
    let (tx_stop, rx_stop) = mpsc::channel(1);
    let handle = Handle { tx_stop };

    let fut = run(ctx.cache, ctx.endpoint, ctx.interval, rx_stop);
    (handle, fut)
}

async fn run(
    cache: EntityCountCache,
    endpoint: Url,
    interval: Duration,
    mut rx_stop: mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    tracing::info!(
        interval_secs = interval.as_secs(),
        endpoint = %endpoint,
        "entity count cache service starting"
    );

    // Fetch immediately on startup
    refresh_cache(&cache, &endpoint).await;

    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first tick (we already fetched above)
    timer.tick().await;

    loop {
        tokio::select! { biased;
            _ = rx_stop.recv() => {
                tracing::debug!("entity count cache service stopping");
                return Ok(());
            }
            _ = timer.tick() => {
                refresh_cache(&cache, &endpoint).await;
            }
        }
    }
}

async fn refresh_cache(cache: &EntityCountCache, endpoint: &Url) {
    // Fetch all IndexerDeploymentLatest entities. We pass an empty
    // list which the fetch function treats as "fetch all" (no filter).
    // TODO: the current fetch_entity_counts takes agreement IDs, not
    // indexer addresses. We need to update it to support fetching all
    // entities or querying by indexer_in. For now, this is a
    // placeholder — the actual pagination query will be implemented
    // when we wire this into the worker context.
    tracing::debug!("refreshing entity count cache");

    let counts = fetch_all_entity_counts(endpoint).await;
    let count = counts.len();

    let mut guard = cache.write().await;
    *guard = counts;
    drop(guard);

    tracing::info!(entries = count, "entity count cache refreshed");
}

/// Fetch all IndexerDeploymentLatest entities from the subgraph with
/// cursor-based pagination.
async fn fetch_all_entity_counts(endpoint: &Url) -> HashMap<EntityCountKey, u64> {
    let mut result = HashMap::new();
    let mut last_id = String::new();
    let client = reqwest::Client::builder()
        .timeout(QUERY_TIMEOUT)
        .build()
        .unwrap_or_default();

    loop {
        let body = serde_json::json!({
            "query": PAGINATED_QUERY,
            "variables": {
                "lastId": last_id,
            },
        });

        let response = match client.post(endpoint.as_str()).json(&body).send().await {
            Ok(resp) => resp,
            Err(err) => {
                tracing::warn!(error = %err, "failed to fetch entity counts page");
                return result;
            }
        };

        let json: PageResponse = match response.json().await {
            Ok(j) => j,
            Err(err) => {
                tracing::warn!(error = %err, "failed to parse entity counts page");
                return result;
            }
        };

        let Some(data) = json.data else {
            if let Some(errors) = json.errors {
                tracing::warn!(errors = ?errors, "subgraph errors in entity count page");
            }
            return result;
        };

        let page_size = data.indexer_deployment_latests.len();

        for entry in data.indexer_deployment_latests {
            if let (Some(indexer_id), Some(deployment_id), Ok(entities)) = (
                parse_address(&entry.indexer),
                parse_deployment_id(&entry.subgraph_deployment_id),
                entry.entities.parse::<u64>(),
            ) {
                result.insert((indexer_id, deployment_id), entities);
            }
            last_id = entry.id;
        }

        if page_size < 1000 {
            break;
        }
    }

    result
}

fn parse_address(hex: &str) -> Option<IndexerId> {
    hex.parse().ok()
}

fn parse_deployment_id(hex: &str) -> Option<DeploymentId> {
    hex.parse().ok()
}

const PAGINATED_QUERY: &str = r#"
    query AllEntityCounts($lastId: ID!) {
        indexerDeploymentLatests(
            first: 1000
            where: { id_gt: $lastId }
            orderBy: id
            orderDirection: asc
        ) {
            id
            indexer
            subgraphDeploymentId
            entities
        }
    }
"#;

#[derive(Debug, serde::Deserialize)]
struct PageResponse {
    data: Option<PageData>,
    errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageData {
    indexer_deployment_latests: Vec<LatestEntity>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct LatestEntity {
    id: String,
    indexer: String,
    subgraph_deployment_id: String,
    entities: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cache_is_empty() {
        let cache = new_cache();
        let guard = cache.try_read().unwrap();
        assert!(guard.is_empty());
    }

    #[tokio::test]
    async fn test_cache_stop_signal() {
        let cache = new_cache();
        let endpoint: Url = "http://localhost:9999/subgraphs/name/test".parse().unwrap();

        let (handle, fut) = new(Ctx {
            cache: cache.clone(),
            endpoint,
            interval: Duration::from_secs(3600),
        });

        let task = tokio::spawn(fut);
        handle.stop().await;

        // Should complete without error
        let result = tokio::time::timeout(Duration::from_secs(2), task).await;
        assert!(result.is_ok());
    }
}
