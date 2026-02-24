//! Fallback filtering for indexer selection when IISA is unavailable.
//!
//! When IISA has been unavailable for an extended period (6+ hours), the dipper
//! falls back to random selection from the network subgraph. This module provides
//! filtering to verify that fallback candidates support the target chain and have
//! valid pricing, mirroring the filtering that IISA normally performs.

use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use thegraph_core::IndexerId;
use tokio::sync::Semaphore;
use url::Url;

use crate::{SelectedIndexer, indexer_client::IndexerInfoClient};

/// Configuration for the fallback filter.
#[derive(Debug, Clone)]
pub struct FallbackFilterConfig {
    /// Timeout for each /dips/info request.
    pub request_timeout: Duration,
    /// Maximum concurrent requests to indexers.
    pub max_concurrent: usize,
}

impl Default for FallbackFilterConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            max_concurrent: 20,
        }
    }
}

/// Filters indexer candidates by fetching /dips/info directly.
///
/// Used during fallback selection to verify chain support and pricing without
/// relying on IISA's cached data.
#[derive(Clone)]
pub struct FallbackFilter {
    client: IndexerInfoClient,
    max_concurrent: usize,
}

impl FallbackFilter {
    /// Create a new fallback filter with the given configuration.
    pub fn new(config: FallbackFilterConfig) -> Self {
        Self {
            client: IndexerInfoClient::new(config.request_timeout),
            max_concurrent: config.max_concurrent,
        }
    }

    /// Filter candidates by fetching /dips/info and checking chain support and pricing.
    ///
    /// For each candidate:
    /// - Fetches /dips/info from the indexer
    /// - Checks that `supported_networks` contains `chain_name`
    /// - Checks that `min_grt_per_30_days` has an entry for `chain_name`
    /// - If `max_grt_per_30_days` is specified, checks that the price is within ceiling
    ///
    /// Returns indexers passing all filters with their advertised pricing populated.
    /// Indexers that don't respond or fail validation are silently excluded.
    pub async fn filter_indexers(
        &self,
        candidates: Vec<(IndexerId, Url)>,
        chain_name: &str,
        max_grt_per_30_days: Option<f64>,
    ) -> Vec<SelectedIndexer> {
        if candidates.is_empty() {
            return Vec::new();
        }

        let candidate_count = candidates.len();
        let semaphore = Semaphore::new(self.max_concurrent);
        let chain_name = chain_name.to_string();

        let mut futures: FuturesUnordered<_> = candidates
            .into_iter()
            .map(|(id, url)| {
                let client = self.client.clone();
                let chain = chain_name.clone();
                let sem = &semaphore;

                async move {
                    let _permit = sem.acquire().await.ok()?;
                    let info = client.fetch_dips_info(&url).await?;

                    // Check chain support
                    if !info.supported_networks.contains(&chain) {
                        tracing::debug!(
                            indexer_id=%id,
                            chain=%chain,
                            "Indexer does not support chain"
                        );
                        return None;
                    }

                    // Check pricing exists for chain
                    let price_str = info.pricing.min_grt_per_30_days.get(&chain)?;
                    let price: f64 = price_str.parse().ok()?;

                    // Check pricing ceiling if specified
                    if let Some(ceiling) = max_grt_per_30_days
                        && price > ceiling
                    {
                        tracing::debug!(
                            indexer_id=%id,
                            price=%price,
                            ceiling=%ceiling,
                            "Indexer price exceeds ceiling"
                        );
                        return None;
                    }

                    // Parse entity pricing
                    let entity_price: f64 = info
                        .pricing
                        .min_grt_per_billion_entities_per_30_days
                        .parse()
                        .ok()?;

                    Some(SelectedIndexer {
                        id,
                        min_grt_per_30_days: Some(price),
                        min_grt_per_billion_entities_per_30_days: Some(entity_price),
                    })
                }
            })
            .collect();

        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            if let Some(indexer) = result {
                results.push(indexer);
            }
        }

        tracing::info!(
            candidates_checked = candidate_count,
            candidates_passed = results.len(),
            "Fallback filter completed"
        );

        results
    }
}

impl Default for FallbackFilter {
    fn default() -> Self {
        Self::new(FallbackFilterConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    fn make_indexer_id(suffix: u8) -> IndexerId {
        format!("0x{:0>40}", suffix).parse().unwrap()
    }

    async fn setup_mock_indexer(
        price: &str,
        chains: Vec<&str>,
        entity_price: &str,
    ) -> (MockServer, Url) {
        let mock = MockServer::start().await;

        let chains_json: Vec<String> = chains.into_iter().map(String::from).collect();
        let mut prices = serde_json::Map::new();
        for chain in &chains_json {
            prices.insert(chain.clone(), serde_json::Value::String(price.to_string()));
        }

        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "pricing": {
                    "min_grt_per_30_days": prices,
                    "min_grt_per_billion_entities_per_30_days": entity_price
                },
                "supported_networks": chains_json
            })))
            .mount(&mock)
            .await;

        let url: Url = mock.uri().parse().unwrap();
        (mock, url)
    }

    #[tokio::test]
    async fn test_filter_indexers_all_valid() {
        let (_mock1, url1) = setup_mock_indexer("450", vec!["arbitrum-one"], "200").await;
        let (_mock2, url2) = setup_mock_indexer("400", vec!["arbitrum-one"], "150").await;

        let filter = FallbackFilter::new(FallbackFilterConfig {
            request_timeout: Duration::from_secs(5),
            max_concurrent: 10,
        });

        let candidates = vec![(make_indexer_id(1), url1), (make_indexer_id(2), url2)];

        let results = filter
            .filter_indexers(candidates, "arbitrum-one", Some(500.0))
            .await;

        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.min_grt_per_30_days.is_some()));
    }

    #[tokio::test]
    async fn test_filter_indexers_chain_not_supported() {
        // Indexer only supports mainnet, not arbitrum-one
        let (_mock, url) = setup_mock_indexer("450", vec!["mainnet"], "200").await;

        let filter = FallbackFilter::default();
        let candidates = vec![(make_indexer_id(1), url)];

        let results = filter
            .filter_indexers(candidates, "arbitrum-one", None)
            .await;

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_filter_indexers_price_exceeds_ceiling() {
        let (_mock, url) = setup_mock_indexer("600", vec!["arbitrum-one"], "200").await;

        let filter = FallbackFilter::default();
        let candidates = vec![(make_indexer_id(1), url)];

        let results = filter
            .filter_indexers(candidates, "arbitrum-one", Some(500.0))
            .await;

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_filter_indexers_price_within_ceiling() {
        let (_mock, url) = setup_mock_indexer("450", vec!["arbitrum-one"], "200").await;

        let filter = FallbackFilter::default();
        let candidates = vec![(make_indexer_id(1), url)];

        let results = filter
            .filter_indexers(candidates, "arbitrum-one", Some(500.0))
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].min_grt_per_30_days, Some(450.0));
        assert_eq!(
            results[0].min_grt_per_billion_entities_per_30_days,
            Some(200.0)
        );
    }

    #[tokio::test]
    async fn test_filter_indexers_no_ceiling() {
        let (_mock, url) = setup_mock_indexer("9999", vec!["arbitrum-one"], "200").await;

        let filter = FallbackFilter::default();
        let candidates = vec![(make_indexer_id(1), url)];

        // No ceiling specified - any price should pass
        let results = filter
            .filter_indexers(candidates, "arbitrum-one", None)
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].min_grt_per_30_days, Some(9999.0));
    }

    #[tokio::test]
    async fn test_filter_indexers_mixed_results() {
        // Good indexer
        let (_mock1, url1) = setup_mock_indexer("400", vec!["arbitrum-one"], "200").await;
        // Price too high
        let (_mock2, url2) = setup_mock_indexer("600", vec!["arbitrum-one"], "200").await;
        // Wrong chain
        let (_mock3, url3) = setup_mock_indexer("300", vec!["mainnet"], "200").await;

        let filter = FallbackFilter::default();
        let candidates = vec![
            (make_indexer_id(1), url1),
            (make_indexer_id(2), url2),
            (make_indexer_id(3), url3),
        ];

        let results = filter
            .filter_indexers(candidates, "arbitrum-one", Some(500.0))
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, make_indexer_id(1));
    }

    #[tokio::test]
    async fn test_filter_indexers_unreachable_excluded() {
        // One reachable, one unreachable
        let (_mock, url1) = setup_mock_indexer("400", vec!["arbitrum-one"], "200").await;
        let url2: Url = "http://127.0.0.1:1/".parse().unwrap();

        let filter = FallbackFilter::new(FallbackFilterConfig {
            request_timeout: Duration::from_secs(1),
            max_concurrent: 10,
        });

        let candidates = vec![(make_indexer_id(1), url1), (make_indexer_id(2), url2)];

        let results = filter
            .filter_indexers(candidates, "arbitrum-one", None)
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, make_indexer_id(1));
    }

    #[tokio::test]
    async fn test_filter_indexers_empty_candidates() {
        let filter = FallbackFilter::default();
        let results = filter.filter_indexers(vec![], "arbitrum-one", None).await;

        assert!(results.is_empty());
    }
}
