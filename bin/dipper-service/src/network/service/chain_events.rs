//! Chain event source abstraction
//!
//! This module provides a trait-based abstraction for fetching the current
//! state of indexing agreements from a subgraph. `chain_listener` polls
//! agreements whose `lastStateChangeBlock` has advanced since the last poll
//! and reconciles the returned state against dipper's local DB.
//!
//! ## Expected Subgraph Schema
//!
//! The subgraph must expose the aggregated `IndexingAgreement` entity with
//! at least the fields dipper's state-diff logic reads:
//!
//! ```graphql
//! type IndexingAgreement @entity {
//!   id: Bytes!                       # bytes16 agreement ID
//!   payer: Bytes!                    # address
//!   indexer: Bytes!                  # address
//!   allocationId: Bytes!             # address
//!   state: AgreementState!
//!   canceledBy: Bytes!               # address (zero if not canceled)
//!   lastStateChangeBlock: BigInt!    # block of the latest state change
//! }
//!
//! enum AgreementState {
//!   NotAccepted
//!   Accepted
//!   CanceledByServiceProvider
//!   CanceledByPayer
//! }
//! ```
//!
//! `canceledBy` is `Bytes.empty()` for agreements that have not been
//! canceled, which serialises as `"0x"` over GraphQL. The parser here maps
//! that to `Address::ZERO` so consumers can compare against a real canceler
//! address without optional-unwrapping.

use std::time::Duration;

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;
use rand::Rng;
use serde::Deserialize;
use thegraph_core::alloy::primitives::Address;
use url::Url;

/// State of an agreement as recorded by the subgraph. Mirrors the
/// `AgreementState` enum in the indexing-payments-subgraph schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgreementState {
    NotAccepted,
    Accepted,
    CanceledByServiceProvider,
    CanceledByPayer,
}

impl AgreementState {
    /// Whether this state represents a canceled agreement.
    pub fn is_canceled(self) -> bool {
        matches!(
            self,
            AgreementState::CanceledByServiceProvider | AgreementState::CanceledByPayer,
        )
    }

    /// Whether the agreement ever reached `Accepted` (including post-accept
    /// cancellation states). `NotAccepted` is the only case where it hasn't.
    pub fn reached_accepted(self) -> bool {
        !matches!(self, AgreementState::NotAccepted)
    }
}

/// Snapshot of an agreement's current state, pulled from the subgraph. One
/// snapshot per agreement whose `lastStateChangeBlock > sinceBlock`.
#[derive(Debug, Clone)]
pub struct AgreementStateSnapshot {
    /// The agreement ID (bytes16 from contract)
    pub agreement_id: IndexingAgreementId,
    /// The indexer address
    pub indexer: Address,
    /// Current lifecycle state
    pub state: AgreementState,
    /// Address that initiated the cancel. `Address::ZERO` when the
    /// agreement is not canceled.
    pub canceled_by: Address,
    /// Block number of the latest state change. Used as the pagination
    /// cursor for subsequent polls.
    pub last_state_change_block: u64,
}

/// Errors that can occur when fetching snapshots.
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

/// Result of fetching changed agreement snapshots.
pub struct ChangedAgreementsResult {
    /// Snapshots of agreements whose state changed since the last poll,
    /// ordered ascending by `last_state_change_block`.
    pub snapshots: Vec<AgreementStateSnapshot>,
    /// The latest block the subgraph has indexed. Used for stall detection
    /// and idle-heartbeat checks, not for cursor advance.
    pub latest_block: u64,
    /// Timestamp of the latest indexed block (seconds since epoch), if
    /// available. Used by the expiration service for chain-time comparisons.
    pub latest_block_timestamp: Option<u64>,
    /// Safe cursor for the caller to advance to. Holds back before the
    /// earliest parse failure so dropped entities are re-read next poll.
    /// `0` when a failure had no parseable block — caller's
    /// `cursor_block > last_block` guard turns that into "don't advance"
    /// rather than "reset to genesis".
    pub cursor_block: u64,
}

/// Trait for fetching agreement state snapshots from a subgraph.
///
/// Implementations may use different data sources (subgraph, RPC, etc.)
/// but must provide the same interface for the chain listener service.
#[async_trait]
pub trait ChainEventSource: Send + Sync {
    /// Fetch all agreements whose `lastStateChangeBlock > since_block`.
    ///
    /// Results are filtered to agreements where the payer matches the
    /// configured signer address, and ordered by `last_state_change_block`
    /// ascending so the consumer processes transitions in the order they
    /// happened on-chain.
    ///
    /// # Arguments
    /// * `since_block` - Fetch agreements whose last state change happened
    ///   in a block strictly greater than this number.
    ///
    /// # Returns
    /// * `Ok(ChangedAgreementsResult)` - Snapshots and the latest processed block
    /// * `Err(ChainEventError::Transient(_))` - Temporary failure, can retry
    /// * `Err(ChainEventError::Permanent(_))` - Permanent failure, should not retry
    async fn get_changed_agreements(
        &self,
        since_block: u64,
    ) -> Result<ChangedAgreementsResult, ChainEventError>;
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
/// Queries a subgraph that indexes the aggregated `IndexingAgreement` entity.
pub struct SubgraphEventSource {
    client: reqwest::Client,
    config: SubgraphEventSourceConfig,
}

/// GraphQL response wrapping the `indexingAgreements` list and subgraph meta.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangedAgreementsResponse {
    indexing_agreements: Vec<IndexingAgreementEntity>,
    #[serde(rename = "_meta")]
    meta: SubgraphMeta,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexingAgreementEntity {
    /// Hex-encoded bytes16 agreement ID
    id: String,
    /// Hex-encoded indexer address
    indexer: String,
    /// Agreement state enum (as string)
    state: String,
    /// Hex-encoded canceler address (`"0x"` when not canceled)
    canceled_by: String,
    /// Block number as string (BigInt in GraphQL)
    last_state_change_block: String,
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
    async fn get_changed_agreements(
        &self,
        since_block: u64,
    ) -> Result<ChangedAgreementsResult, ChainEventError> {
        const QUERY: &str = r#"
            query ChangedAgreements($payer: Bytes!, $sinceBlock: BigInt!) {
                indexingAgreements(
                    where: { payer: $payer, lastStateChangeBlock_gt: $sinceBlock }
                    orderBy: lastStateChangeBlock
                    orderDirection: asc
                    first: 1000
                ) {
                    id
                    indexer
                    state
                    canceledBy
                    lastStateChangeBlock
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

        let response: ChangedAgreementsResponse = self.query_with_retry(QUERY, variables).await?;

        let latest_block = response.meta.block.number;
        let mut snapshots: Vec<AgreementStateSnapshot> = Vec::new();
        let mut min_failed_block: Option<u64> = None;
        let mut unlocatable_failures: usize = 0;

        for entity in &response.indexing_agreements {
            match parse_snapshot(entity) {
                Some(snapshot) => snapshots.push(snapshot),
                None => match entity.last_state_change_block.parse::<u64>().ok() {
                    Some(block) => {
                        min_failed_block = Some(min_failed_block.map_or(block, |m| m.min(block)));
                        tracing::warn!(
                            entity_id = %entity.id,
                            last_state_change_block = block,
                            "Dropping malformed IndexingAgreement entity; cursor will hold back so it gets re-read"
                        );
                    }
                    None => {
                        unlocatable_failures += 1;
                        tracing::warn!(
                            entity_id = %entity.id,
                            "Dropping malformed IndexingAgreement entity with unparseable block; cursor will not advance"
                        );
                    }
                },
            }
        }

        let cursor_block = if unlocatable_failures > 0 {
            // Can't locate where the failure was, so hold the cursor back. The
            // caller's `cursor_block > last_block` guard turns `0` into
            // "do not advance" without rewinding.
            0
        } else if let Some(failed) = min_failed_block {
            failed.saturating_sub(1)
        } else {
            latest_block
        };

        if cursor_block < latest_block {
            tracing::warn!(
                cursor_block,
                subgraph_head = latest_block,
                unlocatable_failures,
                "Parse failures held the cursor back this poll"
            );
        }

        Ok(ChangedAgreementsResult {
            snapshots,
            latest_block,
            latest_block_timestamp: response.meta.block.timestamp,
            cursor_block,
        })
    }
}

/// Parse a GraphQL `IndexingAgreement` entity into a snapshot. Returns
/// `None` if any field is malformed so the caller can filter it out without
/// halting the whole batch — individual corrupt entities log elsewhere.
fn parse_snapshot(entity: &IndexingAgreementEntity) -> Option<AgreementStateSnapshot> {
    Some(AgreementStateSnapshot {
        agreement_id: entity.id.parse().ok()?,
        indexer: entity.indexer.parse().ok()?,
        state: parse_state(&entity.state)?,
        canceled_by: parse_address_or_zero(&entity.canceled_by)?,
        last_state_change_block: entity.last_state_change_block.parse().ok()?,
    })
}

/// Parse the GraphQL state enum string into the Rust enum.
fn parse_state(s: &str) -> Option<AgreementState> {
    match s {
        "NotAccepted" => Some(AgreementState::NotAccepted),
        "Accepted" => Some(AgreementState::Accepted),
        "CanceledByServiceProvider" => Some(AgreementState::CanceledByServiceProvider),
        "CanceledByPayer" => Some(AgreementState::CanceledByPayer),
        _ => None,
    }
}

/// Parse a hex-encoded address field. Graph-node serializes unset Bytes
/// fields with unpredictable padding (observed: `"0x"`, `"0x00000000"`),
/// so any hex shorter than a 20-byte address is treated as `Address::ZERO`.
fn parse_address_or_zero(hex_str: &str) -> Option<Address> {
    let stripped = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    if stripped.len() < 40 {
        return Some(Address::ZERO);
    }
    hex_str.parse().ok()
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
        snapshots: Arc<Mutex<Vec<AgreementStateSnapshot>>>,
        latest_block: Arc<Mutex<u64>>,
        latest_block_timestamp: Arc<Mutex<Option<u64>>>,
        cursor_block_override: Arc<Mutex<Option<u64>>>,
        error: Arc<Mutex<Option<ChainEventError>>>,
    }

    impl MockEventSource {
        pub fn new() -> Self {
            Self {
                snapshots: Arc::new(Mutex::new(Vec::new())),
                latest_block: Arc::new(Mutex::new(0)),
                latest_block_timestamp: Arc::new(Mutex::new(None)),
                cursor_block_override: Arc::new(Mutex::new(None)),
                error: Arc::new(Mutex::new(None)),
            }
        }

        /// Add snapshots to return on next query.
        pub fn add_snapshots(&self, snapshots: Vec<AgreementStateSnapshot>) {
            self.snapshots.lock().unwrap().extend(snapshots);
        }

        /// Set the latest block number.
        pub fn set_latest_block(&self, block: u64) {
            *self.latest_block.lock().unwrap() = block;
        }

        /// Set the latest block timestamp.
        pub fn set_latest_block_timestamp(&self, timestamp: Option<u64>) {
            *self.latest_block_timestamp.lock().unwrap() = timestamp;
        }

        /// Override the cursor_block the mock returns. When `None` (default),
        /// the mock returns `cursor_block = latest_block`, matching the
        /// happy-path where every entity parsed cleanly. Tests that want to
        /// simulate a held-back cursor (parse failure) set this to a lower
        /// value.
        pub fn set_cursor_block_override(&self, cursor_block: Option<u64>) {
            *self.cursor_block_override.lock().unwrap() = cursor_block;
        }

        /// Set an error to return on next query.
        pub fn set_error(&self, error: Option<ChainEventError>) {
            *self.error.lock().unwrap() = error;
        }
    }

    impl Default for MockEventSource {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ChainEventSource for MockEventSource {
        async fn get_changed_agreements(
            &self,
            since_block: u64,
        ) -> Result<ChangedAgreementsResult, ChainEventError> {
            if let Some(error) = self.error.lock().unwrap().take() {
                return Err(error);
            }

            let snapshots: Vec<_> = self
                .snapshots
                .lock()
                .unwrap()
                .iter()
                .filter(|s| s.last_state_change_block > since_block)
                .cloned()
                .collect();

            let latest_block = *self.latest_block.lock().unwrap();
            let latest_block_timestamp = *self.latest_block_timestamp.lock().unwrap();
            let cursor_block = self
                .cursor_block_override
                .lock()
                .unwrap()
                .unwrap_or(latest_block);

            Ok(ChangedAgreementsResult {
                snapshots,
                latest_block,
                latest_block_timestamp,
                cursor_block,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_or_zero_short_hex_is_zero() {
        // Graph-node may serialize unset Bytes fields as any of these
        // variants depending on internal padding; all represent "no address".
        assert_eq!(parse_address_or_zero("0x").unwrap(), Address::ZERO);
        assert_eq!(parse_address_or_zero("").unwrap(), Address::ZERO);
        assert_eq!(parse_address_or_zero("0x00000000").unwrap(), Address::ZERO);
        assert_eq!(
            parse_address_or_zero("0x00000000000000000000000000000000000000").unwrap(),
            Address::ZERO,
        );
    }

    #[test]
    fn test_parse_address_or_zero_real_address() {
        let hex = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
        assert_ne!(parse_address_or_zero(hex).unwrap(), Address::ZERO);
    }

    #[test]
    fn test_parse_address_or_zero_full_zero_address() {
        let hex = "0x0000000000000000000000000000000000000000";
        assert_eq!(parse_address_or_zero(hex).unwrap(), Address::ZERO);
    }

    #[test]
    fn test_parse_state_known_variants() {
        assert_eq!(
            parse_state("NotAccepted"),
            Some(AgreementState::NotAccepted)
        );
        assert_eq!(parse_state("Accepted"), Some(AgreementState::Accepted));
        assert_eq!(
            parse_state("CanceledByServiceProvider"),
            Some(AgreementState::CanceledByServiceProvider)
        );
        assert_eq!(
            parse_state("CanceledByPayer"),
            Some(AgreementState::CanceledByPayer)
        );
    }

    #[test]
    fn test_parse_state_unknown() {
        assert_eq!(parse_state("Garbage"), None);
    }

    #[test]
    fn test_agreement_state_is_canceled() {
        assert!(!AgreementState::NotAccepted.is_canceled());
        assert!(!AgreementState::Accepted.is_canceled());
        assert!(AgreementState::CanceledByServiceProvider.is_canceled());
        assert!(AgreementState::CanceledByPayer.is_canceled());
    }

    #[test]
    fn test_agreement_state_reached_accepted() {
        assert!(!AgreementState::NotAccepted.reached_accepted());
        assert!(AgreementState::Accepted.reached_accepted());
        assert!(AgreementState::CanceledByServiceProvider.reached_accepted());
        assert!(AgreementState::CanceledByPayer.reached_accepted());
    }

    fn valid_entity() -> IndexingAgreementEntity {
        IndexingAgreementEntity {
            id: "0x0102030405060708090a0b0c0d0e0f10".to_string(),
            indexer: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".to_string(),
            state: "Accepted".to_string(),
            canceled_by: "0x".to_string(),
            last_state_change_block: "150".to_string(),
        }
    }

    #[test]
    fn test_parse_snapshot_happy_path() {
        let snapshot = parse_snapshot(&valid_entity()).expect("should parse");
        assert_eq!(snapshot.last_state_change_block, 150);
        assert_eq!(snapshot.state, AgreementState::Accepted);
        assert_eq!(snapshot.canceled_by, Address::ZERO);
    }

    #[test]
    fn test_parse_snapshot_bad_state_returns_none_but_block_parseable() {
        // A malformed state drops the snapshot, but the block field remains
        // parseable so the caller can hold the cursor back to this block
        // and re-read on the next poll.
        let mut entity = valid_entity();
        entity.state = "UnknownVariant".to_string();
        assert!(parse_snapshot(&entity).is_none());
        assert_eq!(
            entity.last_state_change_block.parse::<u64>().ok(),
            Some(150)
        );
    }

    #[test]
    fn test_parse_snapshot_bad_block_and_bad_state_unlocatable() {
        // Both state AND block malformed: caller has no block to hold the
        // cursor at, so it falls through to `unlocatable_failures`.
        let mut entity = valid_entity();
        entity.state = "UnknownVariant".to_string();
        entity.last_state_change_block = "not-a-number".to_string();
        assert!(parse_snapshot(&entity).is_none());
        assert!(entity.last_state_change_block.parse::<u64>().is_err());
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
