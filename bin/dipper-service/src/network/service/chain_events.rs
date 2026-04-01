//! Chain event source abstraction
//!
//! This module provides a trait-based abstraction for fetching on-chain events
//! related to indexing agreements. The primary implementation uses a subgraph
//! for reliable, scalable event retrieval.
//!
//! ## Expected Subgraph Schema
//!
//! The subgraph must index the SubgraphService contract's indexing agreement events.
//! Expected entity schemas (field names use camelCase as per Graph conventions):
//!
//! ```graphql
//! type IndexingAgreementAccepted @entity {
//!   id: ID!
//!   agreementId: Bytes!      # bytes16
//!   indexer: Bytes!          # address
//!   payer: Bytes!            # address
//!   allocationId: Bytes!     # address
//!   blockNumber: BigInt!
//! }
//!
//! type IndexingAgreementCanceled @entity {
//!   id: ID!
//!   agreementId: Bytes!      # bytes16
//!   indexer: Bytes!          # address
//!   payer: Bytes!            # address
//!   canceledBy: Bytes!       # address (who initiated the cancellation)
//!   blockNumber: BigInt!
//! }
//! ```
//!
//! If the actual subgraph schema differs, update the GraphQL queries and response
//! types in this module accordingly.

use std::time::Duration;

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;
use rand::Rng;
use serde::Deserialize;
use thegraph_core::alloy::{hex, primitives::Address};
use url::Url;

/// An on-chain event indicating an indexing agreement was accepted.
#[derive(Debug, Clone)]
pub struct AcceptedAgreementEvent {
    /// The agreement ID (bytes16 from contract)
    pub agreement_id: IndexingAgreementId,
    /// The indexer address
    pub indexer: Address,
    /// The allocation ID
    pub allocation_id: Address,
    /// The block number where this event was emitted
    pub block_number: u64,
}

/// An on-chain event indicating an indexing agreement was canceled.
#[derive(Debug, Clone)]
pub struct CanceledAgreementEvent {
    /// The agreement ID (bytes16 from contract)
    pub agreement_id: IndexingAgreementId,
    /// The indexer address
    pub indexer: Address,
    /// The address that initiated the cancellation (payer or indexer)
    pub canceled_by: Address,
    /// The block number where this event was emitted
    pub block_number: u64,
}

/// Errors that can occur when fetching chain events.
#[derive(Debug, thiserror::Error)]
pub enum ChainEventError {
    /// Transient error - can be retried
    #[error("transient error: {0}")]
    Transient(String),

    /// Permanent error - should not be retried
    #[error("permanent error: {0}")]
    Permanent(String),
}

impl ChainEventError {
    /// Returns true if this error is transient and the operation can be retried.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// Result of fetching accepted agreement events.
pub struct AcceptedEventsResult {
    /// The accepted agreement events found
    pub events: Vec<AcceptedAgreementEvent>,
    /// The latest block number processed (use this for next query)
    pub latest_block: u64,
    /// The timestamp of the latest block (seconds since epoch), if available
    pub latest_block_timestamp: Option<u64>,
}

/// Result of fetching canceled agreement events.
pub struct CanceledEventsResult {
    /// The canceled agreement events found
    pub events: Vec<CanceledAgreementEvent>,
    /// The latest block number processed (use this for next query)
    pub latest_block: u64,
    /// The timestamp of the latest block (seconds since epoch), if available
    pub latest_block_timestamp: Option<u64>,
}

/// Trait for fetching on-chain indexing agreement events.
///
/// Implementations may use different data sources (subgraph, RPC, etc.)
/// but must provide the same interface for the chain listener service.
#[async_trait]
pub trait ChainEventSource: Send + Sync {
    /// Fetch accepted agreement events since the given block.
    ///
    /// Returns events where the payer matches the configured signer address,
    /// along with the latest block number that was processed.
    ///
    /// # Arguments
    /// * `since_block` - Fetch events from blocks after this number
    ///
    /// # Returns
    /// * `Ok(AcceptedEventsResult)` - Events found and the latest processed block
    /// * `Err(ChainEventError::Transient(_))` - Temporary failure, can retry
    /// * `Err(ChainEventError::Permanent(_))` - Permanent failure, should not retry
    async fn get_accepted_agreements(
        &self,
        since_block: u64,
    ) -> Result<AcceptedEventsResult, ChainEventError>;

    /// Fetch canceled agreement events since the given block.
    ///
    /// Returns events where the payer matches the configured signer address,
    /// along with the latest block number that was processed.
    ///
    /// # Arguments
    /// * `since_block` - Fetch events from blocks after this number
    ///
    /// # Returns
    /// * `Ok(CanceledEventsResult)` - Events found and the latest processed block
    /// * `Err(ChainEventError::Transient(_))` - Temporary failure, can retry
    /// * `Err(ChainEventError::Permanent(_))` - Permanent failure, should not retry
    async fn get_canceled_agreements(
        &self,
        since_block: u64,
    ) -> Result<CanceledEventsResult, ChainEventError>;
}

/// Configuration for the subgraph event source.
#[derive(Debug, Clone)]
pub struct SubgraphEventSourceConfig {
    /// The subgraph endpoint URL
    pub endpoint: Url,
    /// API key for authentication (optional for local/test subgraphs)
    pub api_key: Option<String>,
    /// The payer address to filter events by
    pub payer_address: Address,
    /// Request timeout
    pub request_timeout: Duration,
    /// Maximum retry attempts for transient failures
    pub max_retries: u32,
}

/// Subgraph-based implementation of chain event source.
///
/// Queries a subgraph that indexes SubgraphService contract events.
pub struct SubgraphEventSource {
    client: reqwest::Client,
    config: SubgraphEventSourceConfig,
}

/// GraphQL response for accepted agreements query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcceptedAgreementsResponse {
    indexing_agreement_accepteds: Vec<AcceptedAgreementEntity>,
    #[serde(rename = "_meta")]
    meta: SubgraphMeta,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcceptedAgreementEntity {
    /// Hex-encoded agreement ID (bytes16)
    agreement_id: String,
    /// Hex-encoded indexer address
    indexer: String,
    /// Hex-encoded allocation ID
    allocation_id: String,
    /// Block number as string (BigInt in GraphQL)
    block_number: String,
}

#[derive(Debug, Deserialize)]
struct SubgraphMeta {
    block: SubgraphBlock,
}

#[derive(Debug, Deserialize)]
struct SubgraphBlock {
    number: u64,
    timestamp: Option<u64>,
}

/// GraphQL response for canceled agreements query.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanceledAgreementsResponse {
    indexing_agreement_canceleds: Vec<CanceledAgreementEntity>,
    #[serde(rename = "_meta")]
    meta: SubgraphMeta,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanceledAgreementEntity {
    /// Hex-encoded agreement ID (bytes16)
    agreement_id: String,
    /// Hex-encoded indexer address
    indexer: String,
    /// Hex-encoded address of who initiated the cancellation
    canceled_by: String,
    /// Block number as string (BigInt in GraphQL)
    block_number: String,
}

impl SubgraphEventSource {
    /// Create a new subgraph event source.
    pub fn new(config: SubgraphEventSourceConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self { client, config }
    }

    /// Execute a query with exponential backoff retry.
    async fn query_with_retry<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<T, ChainEventError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                let delay = calculate_retry_delay(attempt);
                tracing::debug!(
                    attempt = attempt,
                    delay_ms = delay.as_millis(),
                    "Retrying subgraph query after delay"
                );
                tokio::time::sleep(delay).await;
            }

            match self.execute_query(query, &variables).await {
                Ok(result) => return Ok(result),
                Err(e) if e.is_transient() => {
                    tracing::debug!(
                        attempt = attempt,
                        error = %e,
                        "Transient error querying subgraph"
                    );
                    last_error = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error
            .unwrap_or_else(|| ChainEventError::Permanent("No attempts made".to_string())))
    }

    /// Execute a single GraphQL query.
    async fn execute_query<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: &serde_json::Value,
    ) -> Result<T, ChainEventError> {
        let body = serde_json::json!({
            "query": query,
            "variables": variables,
        });

        let mut request = self.client.post(self.config.endpoint.as_str()).json(&body);

        if let Some(ref api_key) = self.config.api_key {
            request = request.bearer_auth(api_key);
        }

        let response = request.send().await.map_err(|e| {
            if e.is_timeout() || e.is_connect() {
                ChainEventError::Transient(format!("Network error: {}", e))
            } else {
                ChainEventError::Permanent(format!("Request error: {}", e))
            }
        })?;

        if response.status().is_server_error() {
            return Err(ChainEventError::Transient(format!(
                "Server error: {}",
                response.status()
            )));
        }

        if !response.status().is_success() {
            return Err(ChainEventError::Permanent(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct GraphQLResponse<T> {
            data: Option<T>,
            errors: Option<Vec<GraphQLError>>,
        }

        #[derive(Deserialize)]
        struct GraphQLError {
            message: String,
        }

        let graphql_response: GraphQLResponse<T> = response
            .json()
            .await
            .map_err(|e| ChainEventError::Permanent(format!("Failed to parse response: {}", e)))?;

        if let Some(errors) = graphql_response.errors {
            let messages: Vec<_> = errors.iter().map(|e| e.message.as_str()).collect();
            // Check for transient-looking errors
            if messages.iter().any(|m| {
                m.contains("timeout")
                    || m.contains("rate limit")
                    || m.contains("temporarily unavailable")
            }) {
                return Err(ChainEventError::Transient(format!(
                    "GraphQL errors: {:?}",
                    messages
                )));
            }
            return Err(ChainEventError::Permanent(format!(
                "GraphQL errors: {:?}",
                messages
            )));
        }

        graphql_response
            .data
            .ok_or_else(|| ChainEventError::Permanent("No data in response".to_string()))
    }
}

#[async_trait]
impl ChainEventSource for SubgraphEventSource {
    async fn get_accepted_agreements(
        &self,
        since_block: u64,
    ) -> Result<AcceptedEventsResult, ChainEventError> {
        // GraphQL query for accepted agreements
        const QUERY: &str = r#"
            query AcceptedAgreements($payer: Bytes!, $sinceBlock: BigInt!) {
                indexingAgreementAccepteds(
                    where: { payer: $payer, blockNumber_gt: $sinceBlock }
                    orderBy: blockNumber
                    orderDirection: asc
                    first: 1000
                ) {
                    agreementId
                    indexer
                    allocationId
                    blockNumber
                }
                _meta {
                    block {
                        number
                        timestamp
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "payer": format!("{:#x}", self.config.payer_address),
            "sinceBlock": since_block.to_string(),
        });

        let response: AcceptedAgreementsResponse = self.query_with_retry(QUERY, variables).await?;

        let events = response
            .indexing_agreement_accepteds
            .into_iter()
            .filter_map(|entity| {
                let agreement_id = parse_agreement_id(&entity.agreement_id)?;
                let indexer = entity.indexer.parse().ok()?;
                let allocation_id = entity.allocation_id.parse().ok()?;
                let block_number = entity.block_number.parse().ok()?;

                Some(AcceptedAgreementEvent {
                    agreement_id,
                    indexer,
                    allocation_id,
                    block_number,
                })
            })
            .collect();

        Ok(AcceptedEventsResult {
            events,
            latest_block: response.meta.block.number,
            latest_block_timestamp: response.meta.block.timestamp,
        })
    }

    async fn get_canceled_agreements(
        &self,
        since_block: u64,
    ) -> Result<CanceledEventsResult, ChainEventError> {
        // GraphQL query for canceled agreements
        const QUERY: &str = r#"
            query CanceledAgreements($payer: Bytes!, $sinceBlock: BigInt!) {
                indexingAgreementCanceleds(
                    where: { payer: $payer, blockNumber_gt: $sinceBlock }
                    orderBy: blockNumber
                    orderDirection: asc
                    first: 1000
                ) {
                    agreementId
                    indexer
                    canceledBy
                    blockNumber
                }
                _meta {
                    block {
                        number
                        timestamp
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "payer": format!("{:#x}", self.config.payer_address),
            "sinceBlock": since_block.to_string(),
        });

        let response: CanceledAgreementsResponse = self.query_with_retry(QUERY, variables).await?;

        let events = response
            .indexing_agreement_canceleds
            .into_iter()
            .filter_map(|entity| {
                let agreement_id = parse_agreement_id(&entity.agreement_id)?;
                let indexer = entity.indexer.parse().ok()?;
                let canceled_by = entity.canceled_by.parse().ok()?;
                let block_number = entity.block_number.parse().ok()?;

                Some(CanceledAgreementEvent {
                    agreement_id,
                    indexer,
                    canceled_by,
                    block_number,
                })
            })
            .collect();

        Ok(CanceledEventsResult {
            events,
            latest_block: response.meta.block.number,
            latest_block_timestamp: response.meta.block.timestamp,
        })
    }
}

/// Parse a hex-encoded bytes16 agreement ID.
fn parse_agreement_id(hex: &str) -> Option<IndexingAgreementId> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    let bytes = hex::decode(hex).ok()?;
    if bytes.len() != 16 {
        return None;
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes);
    Some(IndexingAgreementId::from_bytes(arr))
}

/// Calculate retry delay with exponential backoff and jitter.
///
/// Base delay is 500ms, capped at 30s. Jitter adds +/- 25% variance.
fn calculate_retry_delay(attempt: u32) -> Duration {
    const BASE_MS: u64 = 500;
    const MAX_MS: u64 = 30_000;

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

/// Mock implementation for testing.
#[cfg(test)]
#[allow(dead_code)]
pub mod mock {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// A mock chain event source for testing.
    pub struct MockEventSource {
        accepted_events: Arc<Mutex<Vec<AcceptedAgreementEvent>>>,
        canceled_events: Arc<Mutex<Vec<CanceledAgreementEvent>>>,
        latest_block: Arc<Mutex<u64>>,
        latest_block_timestamp: Arc<Mutex<Option<u64>>>,
        accepted_error: Arc<Mutex<Option<ChainEventError>>>,
        canceled_error: Arc<Mutex<Option<ChainEventError>>>,
    }

    impl MockEventSource {
        pub fn new() -> Self {
            Self {
                accepted_events: Arc::new(Mutex::new(Vec::new())),
                canceled_events: Arc::new(Mutex::new(Vec::new())),
                latest_block: Arc::new(Mutex::new(0)),
                latest_block_timestamp: Arc::new(Mutex::new(None)),
                accepted_error: Arc::new(Mutex::new(None)),
                canceled_error: Arc::new(Mutex::new(None)),
            }
        }

        /// Add accepted events to return on next query.
        pub fn add_accepted_events(&self, events: Vec<AcceptedAgreementEvent>) {
            self.accepted_events.lock().unwrap().extend(events);
        }

        /// Add canceled events to return on next query.
        pub fn add_canceled_events(&self, events: Vec<CanceledAgreementEvent>) {
            self.canceled_events.lock().unwrap().extend(events);
        }

        /// Set the latest block number.
        pub fn set_latest_block(&self, block: u64) {
            *self.latest_block.lock().unwrap() = block;
        }

        /// Set the latest block timestamp.
        pub fn set_latest_block_timestamp(&self, timestamp: Option<u64>) {
            *self.latest_block_timestamp.lock().unwrap() = timestamp;
        }

        /// Set an error to return on next accepted query.
        pub fn set_accepted_error(&self, error: Option<ChainEventError>) {
            *self.accepted_error.lock().unwrap() = error;
        }

        /// Set an error to return on next canceled query.
        pub fn set_canceled_error(&self, error: Option<ChainEventError>) {
            *self.canceled_error.lock().unwrap() = error;
        }
    }

    impl Default for MockEventSource {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ChainEventSource for MockEventSource {
        async fn get_accepted_agreements(
            &self,
            since_block: u64,
        ) -> Result<AcceptedEventsResult, ChainEventError> {
            // Check for configured error
            if let Some(error) = self.accepted_error.lock().unwrap().take() {
                return Err(error);
            }

            let events: Vec<_> = self
                .accepted_events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.block_number > since_block)
                .cloned()
                .collect();

            let latest_block = *self.latest_block.lock().unwrap();
            let latest_block_timestamp = *self.latest_block_timestamp.lock().unwrap();

            Ok(AcceptedEventsResult {
                events,
                latest_block,
                latest_block_timestamp,
            })
        }

        async fn get_canceled_agreements(
            &self,
            since_block: u64,
        ) -> Result<CanceledEventsResult, ChainEventError> {
            // Check for configured error
            if let Some(error) = self.canceled_error.lock().unwrap().take() {
                return Err(error);
            }

            let events: Vec<_> = self
                .canceled_events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.block_number > since_block)
                .cloned()
                .collect();

            let latest_block = *self.latest_block.lock().unwrap();
            let latest_block_timestamp = *self.latest_block_timestamp.lock().unwrap();

            Ok(CanceledEventsResult {
                events,
                latest_block,
                latest_block_timestamp,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_agreement_id_with_prefix() {
        let hex = "0x0102030405060708090a0b0c0d0e0f10";
        let id = parse_agreement_id(hex).unwrap();
        assert_eq!(
            id.into_bytes(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[test]
    fn test_parse_agreement_id_without_prefix() {
        let hex = "0102030405060708090a0b0c0d0e0f10";
        let id = parse_agreement_id(hex).unwrap();
        assert_eq!(
            id.into_bytes(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
    }

    #[test]
    fn test_parse_agreement_id_wrong_length() {
        let hex = "0x010203";
        assert!(parse_agreement_id(hex).is_none());
    }

    #[test]
    fn test_calculate_retry_delay_exponential() {
        let delay1 = calculate_retry_delay(1);
        assert!(delay1.as_millis() >= 375 && delay1.as_millis() <= 625);

        let delay2 = calculate_retry_delay(2);
        assert!(delay2.as_millis() >= 750 && delay2.as_millis() <= 1250);
    }

    #[test]
    fn test_calculate_retry_delay_capped() {
        let delay = calculate_retry_delay(10);
        assert!(delay.as_millis() >= 22500 && delay.as_millis() <= 37500);
    }

    #[test]
    fn test_chain_event_error_is_transient() {
        assert!(ChainEventError::Transient("test".into()).is_transient());
        assert!(!ChainEventError::Permanent("test".into()).is_transient());
    }
}
