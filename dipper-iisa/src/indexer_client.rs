//! HTTP client for fetching /dips/info directly from indexers.
//!
//! Used during fallback selection when IISA is unavailable. Allows the dipper
//! to verify indexer chain support and pricing without relying on IISA's cached data.

use std::time::Duration;

use reqwest::Client;
use url::Url;

use crate::api::DipsInfoResponse;

/// HTTP client for fetching /dips/info from indexers.
#[derive(Clone)]
pub struct IndexerInfoClient {
    client: Client,
    request_timeout: Duration,
}

impl IndexerInfoClient {
    /// Create a new client with the specified request timeout.
    pub fn new(request_timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(request_timeout)
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            request_timeout,
        }
    }

    /// Request timeout configured for this client.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Fetch /dips/info from an indexer.
    ///
    /// Returns `None` if the indexer doesn't respond, returns an error, or
    /// doesn't have the /dips/info endpoint.
    pub async fn fetch_dips_info(&self, base_url: &Url) -> Option<DipsInfoResponse> {
        let url = match base_url.join("dips/info") {
            Ok(u) => u,
            Err(e) => {
                tracing::debug!(base_url=%base_url, error=%e, "Failed to construct /dips/info URL");
                return None;
            }
        };

        match self.client.get(url.clone()).send().await {
            Ok(response) if response.status().is_success() => match response.json().await {
                Ok(info) => Some(info),
                Err(e) => {
                    tracing::debug!(url=%url, error=%e, "Failed to parse /dips/info response");
                    None
                }
            },
            Ok(response) => {
                tracing::debug!(url=%url, status=%response.status(), "Non-success status from /dips/info");
                None
            }
            Err(e) => {
                tracing::debug!(url=%url, error=%e, "Failed to fetch /dips/info");
                None
            }
        }
    }
}

impl Default for IndexerInfoClient {
    fn default() -> Self {
        Self::new(Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    #[tokio::test]
    async fn test_fetch_dips_info_success() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "pricing": {
                    "min_grt_per_30_days": {
                        "arbitrum-one": "450",
                        "mainnet": "45"
                    },
                    "min_grt_per_billion_entities_per_30_days": "200"
                },
                "supported_networks": ["arbitrum-one", "mainnet"]
            })))
            .mount(&mock_server)
            .await;

        let client = IndexerInfoClient::new(Duration::from_secs(5));
        let url: Url = mock_server.uri().parse().unwrap();
        let result = client.fetch_dips_info(&url).await;

        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.supported_networks, vec!["arbitrum-one", "mainnet"]);
        assert_eq!(
            info.pricing.min_grt_per_30_days.get("arbitrum-one"),
            Some(&"450".to_string())
        );
        assert_eq!(info.pricing.min_grt_per_billion_entities_per_30_days, "200");
    }

    #[tokio::test]
    async fn test_fetch_dips_info_not_found() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let client = IndexerInfoClient::new(Duration::from_secs(5));
        let url: Url = mock_server.uri().parse().unwrap();
        let result = client.fetch_dips_info(&url).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_fetch_dips_info_invalid_json() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&mock_server)
            .await;

        let client = IndexerInfoClient::new(Duration::from_secs(5));
        let url: Url = mock_server.uri().parse().unwrap();
        let result = client.fetch_dips_info(&url).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_fetch_dips_info_timeout() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/dips/info"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"pricing": {}, "supported_networks": []}))
                    .set_delay(Duration::from_secs(10)),
            )
            .mount(&mock_server)
            .await;

        // Very short timeout to trigger timeout error
        let client = IndexerInfoClient::new(Duration::from_millis(50));
        let url: Url = mock_server.uri().parse().unwrap();
        let result = client.fetch_dips_info(&url).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_fetch_dips_info_connection_refused() {
        let client = IndexerInfoClient::new(Duration::from_secs(1));
        let url: Url = "http://127.0.0.1:1/".parse().unwrap(); // Port 1 should refuse connections
        let result = client.fetch_dips_info(&url).await;

        assert!(result.is_none());
    }
}
