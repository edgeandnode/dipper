//! HTTP Client for IISA (Indexing Indexer Selection Algorithm)
//!
//! This module implements a Rust HTTP client for communicating with the IISA container service.
//! The client sends indexer selection requests and receives the selected indexer IDs.
//!
//! The IISA container handles:
//! - Fetching performance data from BigQuery
//! - GeoIP resolution for geographic diversity
//! - Calculating weighted scores for each candidate
//! - Running the selection algorithm

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thegraph_core::{DeploymentId, IndexerId};

use crate::api::{CandidateSelection, Indexer, SelectionContext, SelectionError};

/// Configuration for the HTTP client.
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Total timeout for request + response
    pub request_timeout: Duration,
    /// Timeout for TCP connection establishment
    pub connect_timeout: Duration,
    /// Maximum number of retry attempts for transient failures.
    ///
    /// This is the number of *additional* attempts after the initial request fails.
    /// For example, `max_retries = 3` means up to 4 total attempts (1 initial + 3 retries).
    pub max_retries: u32,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retries: 3,
        }
    }
}

/// HTTP client for the IISA container service.
#[derive(Clone)]
pub struct HttpIisaClient {
    client: Client,
    endpoint: String,
    config: HttpClientConfig,
}

/// A candidate indexer with ID and URL for the selection request.
#[derive(Debug, Clone, Serialize)]
struct CandidateIndexer {
    /// Indexer ID as hex string (0x...)
    id: String,
    /// Indexer URL endpoint
    url: String,
}

/// Request body for indexer selection endpoints.
#[derive(Debug, Clone, Serialize)]
struct SelectionRequest {
    /// The deployment ID to select indexers for
    deployment_id: String,

    /// List of candidate indexers with their URLs
    #[serde(skip_serializing_if = "Option::is_none")]
    candidates: Option<Vec<CandidateIndexer>>,

    /// List of existing indexer IDs already assigned to this deployment
    #[serde(skip_serializing_if = "Option::is_none")]
    existing_indexers: Option<Vec<String>>,

    /// Pending agreements: indexer ID -> list of deployment IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_agreements: Option<HashMap<String, Vec<String>>>,

    /// Number of indexers to select (for select-many)
    #[serde(skip_serializing_if = "Option::is_none")]
    num_candidates: Option<usize>,
}

/// Response from the /select-one endpoint.
#[derive(Debug, Deserialize)]
struct SingleSelectionResponse {
    /// The selected indexer ID, or None if no selection was made
    indexer_id: Option<String>,
}

/// Response from the /select-many endpoint.
#[derive(Debug, Deserialize)]
struct MultiSelectionResponse {
    /// List of selected indexer IDs
    indexer_ids: Vec<String>,
}

/// Check if an HTTP error is retryable.
///
/// Retries on:
/// - `is_timeout()`: Request timed out waiting for response
/// - `is_connect()`: Failed to establish TCP connection
/// - `is_request()`: Request building/sending failed. While these are often deterministic
///   (e.g., serialization errors), we include them defensively to handle transient cases
///   like system resource pressure. These errors are rare in practice.
fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request()
}

/// Check if an HTTP status code is retryable (5xx server errors).
fn is_retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error()
}

/// Calculate retry delay with exponential backoff and jitter.
///
/// Base delay is 100ms, capped at 5s. Jitter adds +/- 25% variance to prevent
/// thundering herd problems when multiple clients retry simultaneously.
fn calculate_retry_delay(attempt: u32) -> Duration {
    const BASE_MS: u64 = 100;
    const MAX_MS: u64 = 5000;

    let exponential = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
    let capped = exponential.min(MAX_MS);

    // Add jitter: +/- 25%
    let jitter_range = capped / 4;
    if jitter_range == 0 {
        return Duration::from_millis(capped);
    }

    let mut rng = rand::rng();
    let jitter: u64 = rng.random_range(0..(jitter_range * 2));
    let with_jitter = capped.saturating_sub(jitter_range).saturating_add(jitter);

    Duration::from_millis(with_jitter)
}

impl HttpIisaClient {
    /// Create a new HTTP client for the IISA service with default configuration.
    ///
    /// # Arguments
    /// * `endpoint` - Base URL of the IISA service (e.g., "http://iisa-service:8080")
    pub fn new(endpoint: String) -> Self {
        Self::with_config(endpoint, HttpClientConfig::default())
    }

    /// Create a new HTTP client for the IISA service with custom configuration.
    ///
    /// # Arguments
    /// * `endpoint` - Base URL of the IISA service (e.g., "http://iisa-service:8080")
    /// * `config` - Client configuration for timeouts and retries
    pub fn with_config(endpoint: String, config: HttpClientConfig) -> Self {
        let endpoint = if endpoint.ends_with('/') {
            endpoint
        } else {
            format!("{}/", endpoint)
        };

        let client = Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            endpoint,
            config,
        }
    }

    /// Check if the IISA service is healthy.
    pub async fn health_check(&self) -> Result<bool, SelectionError> {
        let url = format!("{}health", self.endpoint);

        let response =
            self.client.get(&url).send().await.map_err(|e| {
                SelectionError::Error(anyhow::anyhow!("Health check failed: {}", e))
            })?;

        Ok(response.status().is_success())
    }

    /// Execute an HTTP POST request with retry logic.
    ///
    /// Retries on:
    /// - Network errors (timeout, connection failed)
    /// - 5xx server errors
    ///
    /// Does not retry on:
    /// - 4xx client errors
    /// - Parse/deserialization errors
    async fn post_with_retry<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
        request: &SelectionRequest,
    ) -> Result<T, SelectionError> {
        let mut last_error = SelectionError::IisaServiceUnavailable;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay = calculate_retry_delay(attempt);
                tracing::debug!(
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Retrying IISA request after delay"
                );
                tokio::time::sleep(delay).await;
            }

            match self.client.post(url).json(request).send().await {
                Ok(response) if response.status().is_success() => {
                    return response.json().await.map_err(|e| {
                        SelectionError::Error(anyhow::anyhow!("Failed to parse response: {}", e))
                    });
                }
                Ok(response) if is_retryable_status(response.status()) => {
                    tracing::debug!(
                        attempt = attempt,
                        status = %response.status(),
                        "Retryable HTTP status from IISA"
                    );
                    last_error = SelectionError::IisaServiceUnavailable;
                }
                Ok(response) => {
                    // Non-retryable status (4xx)
                    tracing::error!("IISA returned client error status: {}", response.status());
                    return Err(SelectionError::IisaServiceUnavailable);
                }
                Err(e) if is_retryable_error(&e) => {
                    tracing::debug!(
                        attempt = attempt,
                        error = %e,
                        "Retryable HTTP error from IISA"
                    );
                    last_error = SelectionError::IisaServiceUnavailable;
                }
                Err(e) => {
                    tracing::error!("IISA request failed with non-retryable error: {}", e);
                    return Err(SelectionError::Error(e.into()));
                }
            }
        }

        tracing::error!(
            max_retries = self.config.max_retries,
            "IISA request failed after all retries"
        );
        Err(last_error)
    }

    /// Convert Indexer to CandidateIndexer for serialization.
    fn to_candidate(indexer: &Indexer) -> CandidateIndexer {
        CandidateIndexer {
            id: format!("{:#x}", indexer.id),
            url: indexer.url.to_string(),
        }
    }

    /// Format existing indexers from context for the HTTP request.
    ///
    /// Returns `None` if the list is empty to skip serialization.
    fn format_existing_indexers(context: &SelectionContext) -> Option<Vec<String>> {
        if context.existing_indexers.is_empty() {
            None
        } else {
            Some(
                context
                    .existing_indexers
                    .iter()
                    .map(|id| format!("{:#x}", id))
                    .collect(),
            )
        }
    }

    /// Format pending agreements from context for the HTTP request.
    ///
    /// Returns `None` if the map is empty to skip serialization.
    fn format_pending_agreements(
        context: &SelectionContext,
    ) -> Option<HashMap<String, Vec<String>>> {
        if context.pending_agreements.is_empty() {
            None
        } else {
            Some(
                context
                    .pending_agreements
                    .iter()
                    .map(|(indexer_id, deployment_ids)| {
                        (
                            format!("{:#x}", indexer_id),
                            deployment_ids.iter().map(|d| d.to_string()).collect(),
                        )
                    })
                    .collect(),
            )
        }
    }
}

#[async_trait]
impl CandidateSelection for HttpIisaClient {
    async fn select_one(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        context: &SelectionContext,
    ) -> Result<Option<Indexer>, SelectionError> {
        if candidates.is_empty() {
            return Ok(None);
        }

        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            candidates: Some(candidates.iter().map(Self::to_candidate).collect()),
            existing_indexers: Self::format_existing_indexers(context),
            pending_agreements: Self::format_pending_agreements(context),
            num_candidates: None,
        };

        let url = format!("{}select-one", self.endpoint);
        let result: SingleSelectionResponse = self.post_with_retry(&url, &request).await?;

        // Find the selected indexer in the original candidates list
        if let Some(id_str) = result.indexer_id {
            let id: IndexerId = id_str
                .parse()
                .map_err(|e| SelectionError::Error(anyhow::anyhow!("Invalid indexer ID: {}", e)))?;

            Ok(candidates.into_iter().find(|i| i.id == id))
        } else {
            Ok(None)
        }
    }

    async fn select(
        &self,
        deployment_id: DeploymentId,
        candidates: Vec<Indexer>,
        num_candidates: usize,
        context: &SelectionContext,
    ) -> Result<Vec<Indexer>, SelectionError> {
        if candidates.is_empty() || num_candidates == 0 {
            return Ok(Vec::new());
        }

        let request = SelectionRequest {
            deployment_id: deployment_id.to_string(),
            candidates: Some(candidates.iter().map(Self::to_candidate).collect()),
            existing_indexers: Self::format_existing_indexers(context),
            pending_agreements: Self::format_pending_agreements(context),
            num_candidates: Some(num_candidates),
        };

        let url = format!("{}select-many", self.endpoint);
        let result: MultiSelectionResponse = self.post_with_retry(&url, &request).await?;

        // Find selected indexers in the original candidates list
        let mut selected = Vec::with_capacity(result.indexer_ids.len());
        for id_str in result.indexer_ids {
            let id: IndexerId = match id_str.parse() {
                Ok(id) => id,
                Err(e) => {
                    tracing::warn!("Failed to parse indexer ID '{}': {}", id_str, e);
                    continue;
                }
            };

            if let Some(indexer) = candidates.iter().find(|i| i.id == id) {
                selected.push(indexer.clone());
            }
        }

        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    #[test]
    fn test_endpoint_normalization() {
        let client = HttpIisaClient::new("http://localhost:8080".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080/");

        let client = HttpIisaClient::new("http://localhost:8080/".to_string());
        assert_eq!(client.endpoint, "http://localhost:8080/");
    }

    #[test]
    fn test_config_default() {
        let config = HttpClientConfig::default();
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_with_config_sets_timeouts() {
        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(5),
            max_retries: 5,
        };
        let client = HttpIisaClient::with_config("http://localhost:8080".to_string(), config);

        assert_eq!(client.config.request_timeout, Duration::from_secs(60));
        assert_eq!(client.config.connect_timeout, Duration::from_secs(5));
        assert_eq!(client.config.max_retries, 5);
    }

    #[test]
    fn test_calculate_retry_delay_exponential() {
        // Attempt 1: base delay ~100ms
        let delay1 = calculate_retry_delay(1);
        assert!(delay1.as_millis() >= 75 && delay1.as_millis() <= 125);

        // Attempt 2: ~200ms
        let delay2 = calculate_retry_delay(2);
        assert!(delay2.as_millis() >= 150 && delay2.as_millis() <= 250);

        // Attempt 3: ~400ms
        let delay3 = calculate_retry_delay(3);
        assert!(delay3.as_millis() >= 300 && delay3.as_millis() <= 500);

        // Attempt 4: ~800ms
        let delay4 = calculate_retry_delay(4);
        assert!(delay4.as_millis() >= 600 && delay4.as_millis() <= 1000);
    }

    #[test]
    fn test_calculate_retry_delay_capped() {
        // High attempt numbers should cap at 5000ms +/- 25%
        let delay = calculate_retry_delay(10);
        assert!(delay.as_millis() >= 3750 && delay.as_millis() <= 6250);
    }

    #[test]
    fn test_is_retryable_status() {
        assert!(is_retryable_status(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(is_retryable_status(reqwest::StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE
        ));
        assert!(is_retryable_status(reqwest::StatusCode::GATEWAY_TIMEOUT));

        assert!(!is_retryable_status(reqwest::StatusCode::OK));
        assert!(!is_retryable_status(reqwest::StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(reqwest::StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(reqwest::StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn test_format_existing_indexers_empty() {
        let context = SelectionContext::default();
        assert_eq!(HttpIisaClient::format_existing_indexers(&context), None);
    }

    #[test]
    fn test_format_existing_indexers_with_data() {
        let indexer_id: IndexerId = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let context = SelectionContext {
            existing_indexers: vec![indexer_id],
            pending_agreements: HashMap::new(),
        };

        let result = HttpIisaClient::format_existing_indexers(&context);
        assert!(result.is_some());
        let indexers = result.unwrap();
        assert_eq!(indexers.len(), 1);
        assert_eq!(indexers[0], "0x1234567890123456789012345678901234567890");
    }

    #[test]
    fn test_format_pending_agreements_empty() {
        let context = SelectionContext::default();
        assert_eq!(HttpIisaClient::format_pending_agreements(&context), None);
    }

    #[test]
    fn test_format_pending_agreements_with_data() {
        let indexer_id: IndexerId = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let deployment_id: DeploymentId = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
            .parse()
            .unwrap();

        let mut pending = HashMap::new();
        pending.insert(indexer_id, vec![deployment_id]);

        let context = SelectionContext {
            existing_indexers: vec![],
            pending_agreements: pending,
        };

        let result = HttpIisaClient::format_pending_agreements(&context);
        assert!(result.is_some());
        let agreements = result.unwrap();
        assert_eq!(agreements.len(), 1);
        assert!(agreements.contains_key("0x1234567890123456789012345678901234567890"));
    }

    #[test]
    fn test_selection_context_default() {
        let context = SelectionContext::default();
        assert!(context.existing_indexers.is_empty());
        assert!(context.pending_agreements.is_empty());
    }

    #[tokio::test]
    async fn test_health_check_returns_true_when_healthy() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let result = client.health_check().await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_health_check_returns_false_when_unhealthy() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&mock_server)
            .await;

        let client = HttpIisaClient::new(mock_server.uri());
        let result = client.health_check().await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_health_check_returns_error_when_connection_fails() {
        // Use an endpoint that will refuse connections
        let client = HttpIisaClient::new("http://127.0.0.1:1".to_string());
        let result = client.health_check().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, SelectionError::Error(_)));
    }

    #[tokio::test]
    async fn test_retry_on_5xx_then_success() {
        let mock_server = MockServer::start().await;
        let call_count = std::sync::Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        Mock::given(method("POST"))
            .and(path("/select-one"))
            .respond_with(move |_: &wiremock::Request| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                if count < 2 {
                    // First 2 calls return 503
                    ResponseTemplate::new(503)
                } else {
                    // Third call succeeds
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "indexer_id": "0x1234567890123456789012345678901234567890"
                    }))
                }
            })
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let indexer = Indexer {
            id: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            url: "http://indexer.example.com".parse().unwrap(),
        };

        let result = client
            .select_one(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                vec![indexer],
                &SelectionContext::default(),
            )
            .await;

        // Should succeed after 2 retries (3 total calls)
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_no_retry_on_4xx() {
        let mock_server = MockServer::start().await;
        let call_count = std::sync::Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        Mock::given(method("POST"))
            .and(path("/select-one"))
            .respond_with(move |_: &wiremock::Request| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(400)
            })
            .expect(1) // Should only be called once
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let indexer = Indexer {
            id: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            url: "http://indexer.example.com".parse().unwrap(),
        };

        let result = client
            .select_one(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                vec![indexer],
                &SelectionContext::default(),
            )
            .await;

        // Should fail immediately without retry
        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_exhausted_retries_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-one"))
            .respond_with(ResponseTemplate::new(503))
            .expect(4) // Initial + 3 retries
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 3,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let indexer = Indexer {
            id: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            url: "http://indexer.example.com".parse().unwrap(),
        };

        let result = client
            .select_one(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                vec![indexer],
                &SelectionContext::default(),
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SelectionError::IisaServiceUnavailable
        ));
    }

    #[tokio::test]
    async fn test_timeout_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-one"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({
                        "indexer_id": "0x1234567890123456789012345678901234567890"
                    }))
                    .set_delay(Duration::from_secs(2)), // Delay longer than timeout
            )
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_millis(100), // Very short timeout
            connect_timeout: Duration::from_secs(2),
            max_retries: 0, // No retries to speed up test
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let indexer = Indexer {
            id: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            url: "http://indexer.example.com".parse().unwrap(),
        };

        let result = client
            .select_one(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                vec![indexer],
                &SelectionContext::default(),
            )
            .await;

        // Should fail due to timeout
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_malformed_json_response_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/select-one"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not valid json {{{"))
            .mount(&mock_server)
            .await;

        let config = HttpClientConfig {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retries: 0,
        };
        let client = HttpIisaClient::with_config(mock_server.uri(), config);

        let indexer = Indexer {
            id: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            url: "http://indexer.example.com".parse().unwrap(),
        };

        let result = client
            .select_one(
                "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                vec![indexer],
                &SelectionContext::default(),
            )
            .await;

        // Should fail due to JSON parse error
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SelectionError::Error(_)));
    }
}
