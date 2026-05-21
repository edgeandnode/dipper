//! Dipper service configuration

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use serde_with::serde_as;
use thegraph_core::{
    DeploymentId,
    alloy::{
        primitives::{Address, ChainId, U256},
        signers::k256::SecretKey,
    },
};
use url::Url;

/// The maximum number of candidates to select.
pub const DEFAULT_MAX_CANDIDATES: usize = 3;

/// Load the configuration from a JSON file.
pub fn load_from_file(path: &Path) -> Result<Config, Error> {
    let config_content = std::fs::read_to_string(path)?;
    let config = serde_json::from_str(&config_content)?;
    Ok(config)
}

/// An error that can occur when loading the configuration.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error occurred while reading the configuration file.
    #[error("failed to read configuration file: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred while deserializing the configuration.
    #[error("failed to deserialize configuration: {0}")]
    Deserialize(#[from] serde_json::Error),
}

/// The configuration for the DIPs service
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
pub struct Config {
    /// The DIPs agreement configuration
    pub dips: DipsAgreementConfig,
    /// The Admin RPC server configuration
    pub admin_rpc: AdminRpcConfig,
    /// The database configuration
    pub db: DbConfig,
    /// The network service configuration
    pub network: NetworkConfig,
    /// The signer configuration
    pub signer: SignerConfig,
    /// The IISA (Indexing Indexer Selection Algorithm) service configuration
    pub iisa: IisaConfig,
    /// The indexer gRPC client configuration (for sending RCA proposals)
    #[serde(default)]
    pub indexer_client: IndexerClientConfig,
    /// The reassignment service configuration
    #[serde(default)]
    pub reassignment: Option<ReassignmentConfig>,
    /// The expiration service configuration (marks stale Created agreements as Expired)
    #[serde(default)]
    pub expiration: Option<ExpirationConfig>,
    /// The chain listener service configuration (monitors on-chain events)
    #[serde(default)]
    pub chain_listener: Option<ChainListenerConfig>,
    /// The liveness checker service configuration (detects silent agreement abandonment)
    #[serde(default)]
    pub liveness_checker: Option<LivenessCheckerConfig>,
    /// Additional chain ID to network name mappings for dev/test chains.
    ///
    /// Production chains are resolved via the graph-networks-registry crate.
    /// This map supplements the registry with chains that aren't in the official
    /// registry (e.g. `1337 = "hardhat"` for local development).
    #[serde(default)]
    pub additional_networks: BTreeMap<ChainId, String>,
    /// The chain client configuration (for sending on-chain transactions)
    #[serde(default)]
    pub chain_client: Option<ChainClientConfig>,
}

/// The IISA (Indexing Indexer Selection Algorithm) service configuration
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct IisaConfig {
    /// The IISA service endpoint URL (e.g., "http://iisa-service:8080")
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub endpoint: Url,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Connection timeout in seconds (default: 10)
    #[serde(default = "default_connect_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub connect_timeout: Duration,

    /// Maximum retry attempts for transient failures (default: 3).
    ///
    /// This is the number of *additional* attempts after the initial request fails.
    /// For example, `max_retries = 3` means up to 4 total attempts (1 initial + 3 retries).
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Fallback filter configuration (used when IISA is unavailable for 6+ hours).
    #[serde(default)]
    pub fallback: FallbackFilterConfig,
}

/// Configuration for the fallback filter used when IISA is unavailable.
///
/// When IISA has been unavailable for 6+ hours, the dipper falls back to random
/// selection from the network subgraph. The fallback filter fetches /dips/info
/// directly from candidate indexers to verify chain support and pricing.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct FallbackFilterConfig {
    /// Request timeout per indexer in seconds (default: 5).
    ///
    /// Short timeout since we're hitting many endpoints concurrently.
    #[serde(default = "default_fallback_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Maximum concurrent requests to indexers (default: 20).
    #[serde(default = "default_fallback_max_concurrent")]
    pub max_concurrent: usize,
}

fn default_fallback_request_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_fallback_max_concurrent() -> usize {
    20
}

impl Default for FallbackFilterConfig {
    fn default() -> Self {
        Self {
            request_timeout: default_fallback_request_timeout(),
            max_concurrent: default_fallback_max_concurrent(),
        }
    }
}

fn default_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_max_retries() -> u32 {
    3
}

/// Indexer gRPC client configuration (for sending RCA proposals to indexers)
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct IndexerClientConfig {
    /// Request timeout in seconds (default: 240).
    ///
    /// This must be long enough to cover indexer-rs IPFS retry worst case (190s)
    /// plus buffer. indexer-rs retries IPFS fetches up to 4 times with exponential
    /// backoff (30s timeout + 10s/20s/40s delays = 190s worst case).
    #[serde(default = "default_indexer_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Connection timeout in seconds (default: 10)
    #[serde(default = "default_indexer_connect_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub connect_timeout: Duration,

    /// Maximum retry attempts for transient failures (default: 3).
    ///
    /// With `max_retries = 3`, up to 4 total attempts are made (1 initial + 3 retries).
    /// Retries use exponential backoff (1s, 2s, 4s, ...) and only occur on
    /// transient gRPC errors (UNAVAILABLE, RESOURCE_EXHAUSTED, ABORTED, DEADLINE_EXCEEDED).
    #[serde(default = "default_indexer_max_retries")]
    pub max_retries: u32,
}

fn default_indexer_request_timeout() -> Duration {
    Duration::from_secs(240) // 190s IPFS worst case + 50s buffer
}

fn default_indexer_connect_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_indexer_max_retries() -> u32 {
    3
}

impl Default for IndexerClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: default_indexer_request_timeout(),
            connect_timeout: default_indexer_connect_timeout(),
            max_retries: default_indexer_max_retries(),
        }
    }
}

/// Configuration for the periodic reassignment service
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ReassignmentConfig {
    /// Whether the reassignment service is enabled (default: true)
    #[serde(default = "default_reassignment_enabled")]
    pub enabled: bool,

    /// Interval between reassignment cycles in seconds (default: 86400s / 24 hours)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_reassignment_interval")]
    pub interval: Duration,

    /// Hour of day (UTC, 0-23) to run the reassignment cycle (default: 10, i.e., 10:00 UTC)
    ///
    /// The first cycle will be delayed until this hour, then subsequent cycles
    /// run at the configured interval. This allows alignment with upstream data
    /// refresh schedules (e.g., IISA score computation runs at 09:00 UTC).
    #[serde(
        default = "default_reassignment_run_at_utc_hour",
        deserialize_with = "deserialize_utc_hour"
    )]
    pub run_at_utc_hour: u8,

    /// Maximum number of requests to process per cycle (default: 100, 0 = unlimited)
    #[serde(default = "default_reassignment_batch_size")]
    pub batch_size: i64,

    /// Minimum age of requests to consider for reassessment in seconds (default: 86400s)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_reassignment_min_age")]
    pub min_request_age: Duration,
}

fn default_reassignment_enabled() -> bool {
    true
}

fn default_reassignment_interval() -> Duration {
    Duration::from_secs(86400) // 24 hours
}

fn deserialize_utc_hour<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u8, D::Error> {
    let hour = <u8 as serde::Deserialize>::deserialize(deserializer)?;
    if hour > 23 {
        return Err(serde::de::Error::custom(format!(
            "run_at_utc_hour must be 0-23, got {hour}"
        )));
    }
    Ok(hour)
}

fn default_reassignment_run_at_utc_hour() -> u8 {
    10 // 10:00 UTC, 1 hour after IISA score computation at 09:00 UTC
}

fn default_reassignment_batch_size() -> i64 {
    100
}

fn default_reassignment_min_age() -> Duration {
    Duration::from_secs(86400)
}

impl Default for ReassignmentConfig {
    fn default() -> Self {
        Self {
            enabled: default_reassignment_enabled(),
            interval: default_reassignment_interval(),
            run_at_utc_hour: default_reassignment_run_at_utc_hour(),
            batch_size: default_reassignment_batch_size(),
            min_request_age: default_reassignment_min_age(),
        }
    }
}

/// Configuration for the deadline expiration service.
///
/// This service periodically scans for `Created` agreements whose RCA deadline
/// has passed, marks them as `Expired`, and triggers IISA reassessment to find
/// replacement indexers.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ExpirationConfig {
    /// Whether the expiration service is enabled (default: true)
    #[serde(default = "default_expiration_enabled")]
    pub enabled: bool,

    /// Interval between expiration scans in seconds (default: 90s)
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_expiration_interval")]
    pub interval: Duration,

    /// Maximum agreements to process per cycle (default: 100)
    #[serde(default = "default_expiration_batch_size")]
    pub batch_size: i64,
}

fn default_expiration_enabled() -> bool {
    true
}

fn default_expiration_interval() -> Duration {
    Duration::from_secs(90)
}

fn default_expiration_batch_size() -> i64 {
    100
}

impl Default for ExpirationConfig {
    fn default() -> Self {
        Self {
            enabled: default_expiration_enabled(),
            interval: default_expiration_interval(),
            batch_size: default_expiration_batch_size(),
        }
    }
}

/// Configuration for the liveness checker service.
///
/// This service periodically polls each indexer's status endpoint to verify that
/// indexing is progressing. Agreements where no block height progress is observed
/// within the tolerance window are canceled as payer and reassigned.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct LivenessCheckerConfig {
    /// Whether the liveness checker is enabled (default: false).
    ///
    /// Disabled by default since it requires the chain client to be configured
    /// for on-chain cancellation.
    #[serde(default = "default_liveness_checker_enabled")]
    pub enabled: bool,

    /// Interval between liveness checks in seconds (default: 300s / 5 min).
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_liveness_checker_interval")]
    pub interval: Duration,

    /// Maximum tolerance window in days (default: 4).
    ///
    /// The actual threshold scales with the number of active agreements on the
    /// deployment: `min(active_count, max_tolerance_days)` days.
    #[serde(default = "default_liveness_checker_max_tolerance_days")]
    pub max_tolerance_days: u32,

    /// Timeout per indexer status HTTP request in seconds (default: 10s).
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_liveness_checker_request_timeout")]
    pub request_timeout: Duration,

    /// Maximum agreements to fetch per cycle (default: 500).
    #[serde(default = "default_liveness_checker_batch_size")]
    pub batch_size: i64,
}

fn default_liveness_checker_enabled() -> bool {
    false
}

fn default_liveness_checker_interval() -> Duration {
    Duration::from_secs(300)
}

fn default_liveness_checker_max_tolerance_days() -> u32 {
    4
}

fn default_liveness_checker_request_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_liveness_checker_batch_size() -> i64 {
    500
}

impl Default for LivenessCheckerConfig {
    fn default() -> Self {
        Self {
            enabled: default_liveness_checker_enabled(),
            interval: default_liveness_checker_interval(),
            max_tolerance_days: default_liveness_checker_max_tolerance_days(),
            request_timeout: default_liveness_checker_request_timeout(),
            batch_size: default_liveness_checker_batch_size(),
        }
    }
}

/// Configuration for the on-chain event listener service.
///
/// This service monitors the SubgraphService contract for `IndexingAgreementAccepted`
/// and `IndexingAgreementCanceled` events via a subgraph. When a `Rejected` agreement
/// is accepted on-chain, it triggers automatic cancellation via `cancelIndexingAgreementByPayer`.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChainListenerConfig {
    /// Whether the chain listener service is enabled (default: false)
    ///
    /// Disabled by default since it requires subgraph configuration.
    #[serde(default = "default_chain_listener_enabled")]
    pub enabled: bool,

    /// The subgraph endpoint URL for querying indexing agreement events.
    ///
    /// This should point to a subgraph that indexes the SubgraphService contract's
    /// IndexingAgreementAccepted and IndexingAgreementCanceled events.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub subgraph_endpoint: Url,

    /// API key for subgraph authentication (optional for local/test subgraphs).
    #[serde(default)]
    pub subgraph_api_key: Option<String>,

    /// Chain ID for state tracking (default: 42161 for Arbitrum One)
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,

    /// Poll interval in seconds (default: 30s)
    ///
    /// How often to query the subgraph for new events. Since subgraphs have some
    /// indexing latency, polling more frequently than ~30s provides diminishing returns.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_chain_listener_poll_interval")]
    pub poll_interval: Duration,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_chain_listener_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Maximum retry attempts for transient failures (default: 3)
    #[serde(default = "default_chain_listener_max_retries")]
    pub max_retries: u32,

    /// Number of blocks at the tail of the chain that every poll re-reads,
    /// so a reorg that moves a state change across the cursor boundary is
    /// still picked up. Set to 0 to disable.
    #[serde(default = "default_chain_listener_reorg_buffer_blocks")]
    pub reorg_buffer_blocks: u32,

    /// How far ahead of the host's wall clock the subgraph's reported
    /// chain timestamp may sit before the response is rejected as
    /// corrupt. Default 60s covers typical NTP drift; widen if the
    /// host clock is known to lag.
    #[serde(default = "default_chain_listener_wall_clock_skew_tolerance_secs")]
    pub wall_clock_skew_tolerance_secs: u64,

    /// How much faster than wall-clock the persisted chain timestamp
    /// may advance per poll before the listener caps the advance.
    /// Chain time legitimately moves at ~1s per wall second; the
    /// tolerance covers poll-cadence jitter and subgraph-side rounding.
    /// Widen for environments with choppy poll cadence.
    #[serde(default = "default_chain_listener_chain_ts_drift_tolerance_secs")]
    pub chain_ts_drift_tolerance_secs: u64,
}

fn default_chain_listener_enabled() -> bool {
    false
}

fn default_chain_id() -> u64 {
    42161 // Arbitrum One
}

fn default_chain_listener_poll_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_chain_listener_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_chain_listener_max_retries() -> u32 {
    3
}

fn default_chain_listener_reorg_buffer_blocks() -> u32 {
    20
}

fn default_chain_listener_wall_clock_skew_tolerance_secs() -> u64 {
    60
}

fn default_chain_listener_chain_ts_drift_tolerance_secs() -> u64 {
    10
}

fn default_gas_price_multiplier() -> f64 {
    1.2
}

fn default_max_gas_price_gwei() -> u64 {
    100
}

/// Configuration for the on-chain transaction client.
///
/// This client sends transactions to the blockchain, such as calling
/// `cancelIndexingAgreementByPayer` on the SubgraphService contract.
/// It supports multiple RPC providers with automatic failover and retry.
#[serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChainClientConfig {
    /// Whether the chain client is enabled (default: false)
    ///
    /// Disabled by default since it requires RPC provider and contract configuration.
    #[serde(default = "default_chain_client_enabled")]
    pub enabled: bool,

    /// List of RPC provider URLs (first is primary, rest are fallbacks).
    ///
    /// At least one provider is required when enabled. Providers are tried in order,
    /// rotating to the next on persistent failures.
    pub providers: Vec<Url>,

    /// Request timeout per RPC call in seconds (default: 30s)
    #[serde(default = "default_chain_client_request_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub request_timeout: Duration,

    /// Maximum retry attempts before rotating to next provider (default: 3)
    ///
    /// Uses exponential backoff (1s, 2s, 4s...) between retries.
    #[serde(default = "default_chain_client_max_retries")]
    pub max_retries: u32,

    /// Chain ID (default: 42161 for Arbitrum One)
    #[serde(default = "default_chain_id")]
    pub chain_id: u64,

    /// SubgraphService contract address.
    ///
    /// This is the contract that manages indexing agreements and exposes
    /// `cancelIndexingAgreementByPayer(bytes32)`.
    pub subgraph_service_address: Address,

    /// RecurringCollector contract address.
    ///
    /// This is the contract that stores on-chain RCA offers. Dipper calls
    /// `offer(OFFER_TYPE_NEW, abi.encode(rca), 0)` before dispatching a
    /// proposal. The stored offer mapping lives inside an ERC-7201 namespaced
    /// storage struct and has no auto-generated getter, so crash-recovery
    /// idempotency is handled via the indexing-payments subgraph
    /// (`indexing_payments_subgraph_url` below) rather than an eth_call.
    pub recurring_collector_address: Address,

    /// Indexing-payments-subgraph query URL.
    ///
    /// When set, dipper queries the subgraph for an existing `Offer` entity
    /// before submitting a new `offer()` transaction. This provides
    /// crash-recovery idempotency: after a restart, if dipper's prior
    /// submission already landed on-chain and the subgraph has indexed it,
    /// dipper will skip re-submission rather than wasting gas. When unset,
    /// dipper will log a warning on startup and always submit, trusting
    /// the contract's overwrite semantics to make double-submission harmless.
    #[serde(default)]
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub indexing_payments_subgraph_url: Option<Url>,

    /// Gas price multiplier (default: 1.2)
    ///
    /// Applied to the estimated gas price to ensure timely inclusion.
    #[serde(default = "default_gas_price_multiplier")]
    pub gas_price_multiplier: f64,

    /// Maximum gas price in gwei (default: 100)
    ///
    /// Transactions will fail if the gas price exceeds this limit.
    #[serde(default = "default_max_gas_price_gwei")]
    pub max_gas_price_gwei: u64,

    /// Gas limit buffer multiplier (default: 2.0)
    ///
    /// The estimated gas is multiplied by this value, then bounded by
    /// floor and ceiling.
    #[serde(default = "default_gas_buffer_multiplier")]
    pub gas_buffer_multiplier: f64,

    /// Minimum gas limit floor (default: 100,000)
    ///
    /// Even if the estimate is lower, this floor is applied.
    #[serde(default = "default_gas_floor")]
    pub gas_floor: u64,

    /// Maximum gas addition above estimate (default: 200,000)
    ///
    /// The gas limit is capped at estimate + this value.
    #[serde(default = "default_gas_max_addition")]
    pub gas_max_addition: u64,
}

fn default_chain_client_enabled() -> bool {
    false
}

fn default_chain_client_request_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_chain_client_max_retries() -> u32 {
    3
}

fn default_gas_buffer_multiplier() -> f64 {
    2.0
}

fn default_gas_floor() -> u64 {
    100_000
}

fn default_gas_max_addition() -> u64 {
    200_000
}

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct DipsAgreementConfig {
    /// The data service address (SubgraphService contract).
    pub data_service: Address,
    /// The RecurringCollector contract address. Dipper posts on-chain offers
    /// here via `RecurringCollector.offer()` before dispatching gRPC proposals.
    pub recurring_collector: Address,
    /// Maximum tokens for the initial subgraph sync.
    pub max_initial_tokens: U256,
    /// Maximum tokens per second for ongoing indexing.
    pub max_ongoing_tokens_per_second: U256,
    /// Maximum seconds per collection.
    pub max_seconds_per_collection: u32,
    /// Minimum seconds per collection.
    pub min_seconds_per_collection: u32,
    /// Agreement duration in seconds (None = u64::MAX).
    pub duration_seconds: Option<u64>,
    /// Deadline duration in seconds (how long the indexer has to accept on-chain).
    #[serde(default = "default_deadline_seconds")]
    pub deadline_seconds: u64,

    /// Per-chain pricing table.
    ///
    /// Deprecated: When IISA returns per-indexer prices, this table is only used as
    /// fallback for indexers without advertised prices.
    #[serde(default)]
    pub pricing_table: BTreeMap<ChainId, ChainPrices>,

    /// Maximum GRT per 30 days Dipper will pay, per network (by chain name).
    ///
    /// Used as a ceiling when requesting indexers from IISA. Indexers asking
    /// more than the ceiling for their chain are excluded from selection.
    /// Keys are chain names (e.g. "arbitrum-one", "mainnet").
    #[serde(default = "default_max_grt_per_30_days")]
    pub max_grt_per_30_days: BTreeMap<String, f64>,

    /// Maximum GRT per billion entities per 30 days.
    #[serde(default = "default_max_grt_per_billion_entities_per_30_days")]
    pub max_grt_per_billion_entities_per_30_days: f64,

    /// Number of days to look back for declined indexers (standard exclusion).
    ///
    /// Indexers that declined an agreement (CanceledByIndexer, Expired, or Rejected
    /// with reason OTHER/UNSPECIFIED) within this period will be excluded from
    /// selection for that deployment. Default: 30 days.
    #[serde(default = "default_declined_indexer_lookback_days")]
    pub declined_indexer_lookback_days: i32,

    /// Number of days to look back for PRICE_TOO_LOW rejections.
    ///
    /// Shorter window because IISA refreshes price data daily. Once new prices
    /// are available, the indexer should be reconsidered. Default: 1 day.
    #[serde(default = "default_price_rejection_lookback_days")]
    pub price_rejection_lookback_days: i32,

    /// Number of minutes to look back for SIGNER_NOT_AUTHORISED rejections.
    ///
    /// Very short window because signer authorization is a transient configuration
    /// issue that resolves once the operator registers the signer on the escrow
    /// contract. Default: 5 minutes.
    #[serde(default = "default_signer_rejection_lookback_minutes")]
    pub signer_rejection_lookback_minutes: i32,
}

fn default_deadline_seconds() -> u64 {
    600
}

/// Default ceiling: 10x the indexer-rs minimum defaults.
fn default_max_grt_per_30_days() -> BTreeMap<String, f64> {
    BTreeMap::from([
        ("arbitrum-one".to_string(), 4500.0),
        ("mainnet".to_string(), 450.0),
        ("base".to_string(), 2000.0),
        ("optimism".to_string(), 1500.0),
        ("matic".to_string(), 3000.0),
    ])
}

fn default_max_grt_per_billion_entities_per_30_days() -> f64 {
    2000.0
}

fn default_declined_indexer_lookback_days() -> i32 {
    30
}

fn default_price_rejection_lookback_days() -> i32 {
    1
}

fn default_signer_rejection_lookback_minutes() -> i32 {
    5
}

/// Per-chain pricing for indexing agreements.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ChainPrices {
    /// Tokens per second (base rate) in wei GRT.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub tokens_per_second: U256,
    /// Tokens per entity per second in wei GRT.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub tokens_per_entity_per_second: U256,
}

/// Gateway operator API configuration. Authenticates via EIP-712 signatures.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct AdminRpcConfig {
    /// The RPC server listen address.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub listen_addr: std::net::SocketAddr,

    /// Authorized gateway operator addresses (e.g., Graph Studio).
    #[serde_as(as = "serde_with::SetLastValueWins<_>")]
    pub gateway_operator_allowlist: BTreeSet<Address>,
}

/// The database configuration
#[serde_as]
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
pub struct DbConfig {
    /// The PostgreSQL database URL
    ///
    /// The URL should be in the format `postgres://<host>:<port>/<database>`.
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub url: Url,

    /// The database auth username
    pub username: String,

    /// The database auth password
    pub password: Hidden<String>,

    /// The maximum number of connections to the database
    #[serde(default)]
    pub max_connections: Option<u32>,
}

/// The network service configuration
#[serde_as]
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
pub struct NetworkConfig {
    /// The graph network gateway URL
    #[debug(with = std::fmt::Display::fmt)]
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub gateway_url: Url,

    /// The graph network API key
    pub api_key: Hidden<String>,

    /// The graph network subgraph deployment ID
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub deployment_id: DeploymentId,

    /// The update interval for the network service
    #[serde_as(as = "serde_with::DurationSeconds")]
    pub update_interval: Duration,
}

/// The configuration for the signer
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct SignerConfig {
    /// The signing key to use for authentication
    #[serde_as(as = "HiddenSecretKeyAsHexStr")]
    pub secret_key: Hidden<SecretKey>,

    /// The signer chain ID (protocol chain), e.g. `eip155:42161` (Arbitrum One)
    pub chain_id: ChainId,
}

/// Runtime indexing agreement configuration.
#[derive(Debug)]
pub struct IndexingAgreementConfig {
    /// The data service address (SubgraphService contract).
    pub data_service: Address,
    /// The RecurringCollector contract address.
    pub recurring_collector: Address,
    /// Maximum tokens for the initial subgraph sync.
    pub max_initial_tokens: U256,
    /// Maximum tokens per second for ongoing indexing.
    pub max_ongoing_tokens_per_second: U256,
    /// Maximum seconds per collection.
    pub max_seconds_per_collection: u32,
    /// Minimum seconds per collection.
    pub min_seconds_per_collection: u32,
    /// Agreement duration in seconds.
    pub duration_seconds: u64,
    /// Deadline duration in seconds.
    pub deadline_seconds: u64,
    /// Payment ceiling per chain (GRT per 30 days).
    pub max_grt_per_30_days: BTreeMap<String, f64>,
    /// Payment ceiling for entity pricing (GRT per billion entities per 30 days).
    pub max_grt_per_billion_entities_per_30_days: f64,
    /// Number of days to look back for declined indexers (standard exclusion).
    pub declined_indexer_lookback_days: i32,
    /// Number of days to look back for PRICE_TOO_LOW rejections.
    pub price_rejection_lookback_days: i32,
    /// Number of minutes to look back for SIGNER_NOT_AUTHORISED rejections.
    pub signer_rejection_lookback_minutes: i32,
}

/// Per-chain pricing for indexing agreements (runtime).
#[derive(Debug)]
pub struct IndexingAgreementChainPrices {
    /// Tokens per second (base rate) in wei GRT.
    pub tokens_per_second: U256,
    /// Tokens per entity per second in wei GRT.
    pub tokens_per_entity_per_second: U256,
}

impl IndexingAgreementConfig {
    pub fn data_service(&self) -> Address {
        self.data_service
    }

    pub fn recurring_collector(&self) -> Address {
        self.recurring_collector
    }

    pub fn max_initial_tokens(&self) -> U256 {
        self.max_initial_tokens
    }

    pub fn max_ongoing_tokens_per_second(&self) -> U256 {
        self.max_ongoing_tokens_per_second
    }

    pub fn max_seconds_per_collection(&self) -> u32 {
        self.max_seconds_per_collection
    }

    pub fn min_seconds_per_collection(&self) -> u32 {
        self.min_seconds_per_collection
    }

    pub fn duration_seconds(&self) -> u64 {
        self.duration_seconds
    }

    pub fn deadline_seconds(&self) -> u64 {
        self.deadline_seconds
    }

    pub fn max_grt_per_30_days(&self) -> &BTreeMap<String, f64> {
        &self.max_grt_per_30_days
    }

    pub fn max_grt_per_billion_entities_per_30_days(&self) -> f64 {
        self.max_grt_per_billion_entities_per_30_days
    }

    pub fn declined_indexer_lookback_days(&self) -> i32 {
        self.declined_indexer_lookback_days
    }

    pub fn price_rejection_lookback_days(&self) -> i32 {
        self.price_rejection_lookback_days
    }

    pub fn signer_rejection_lookback_minutes(&self) -> i32 {
        self.signer_rejection_lookback_minutes
    }
}

impl From<DipsAgreementConfig>
    for (
        Arc<IndexingAgreementConfig>,
        Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,
    )
{
    fn from(value: DipsAgreementConfig) -> Self {
        let config = IndexingAgreementConfig {
            data_service: value.data_service,
            recurring_collector: value.recurring_collector,
            max_initial_tokens: value.max_initial_tokens,
            max_ongoing_tokens_per_second: value.max_ongoing_tokens_per_second,
            max_seconds_per_collection: value.max_seconds_per_collection,
            min_seconds_per_collection: value.min_seconds_per_collection,
            duration_seconds: value.duration_seconds.unwrap_or(u64::MAX),
            deadline_seconds: value.deadline_seconds,
            max_grt_per_30_days: value.max_grt_per_30_days,
            max_grt_per_billion_entities_per_30_days: value
                .max_grt_per_billion_entities_per_30_days,
            declined_indexer_lookback_days: value.declined_indexer_lookback_days,
            price_rejection_lookback_days: value.price_rejection_lookback_days,
            signer_rejection_lookback_minutes: value.signer_rejection_lookback_minutes,
        };
        let prices = value
            .pricing_table
            .into_iter()
            .map(|(chain_id, prices)| {
                (
                    chain_id,
                    IndexingAgreementChainPrices {
                        tokens_per_second: prices.tokens_per_second,
                        tokens_per_entity_per_second: prices.tokens_per_entity_per_second,
                    },
                )
            })
            .collect();
        (Arc::new(config), Arc::new(prices))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dips_agreement_config_deserialization() {
        //* Arrange - JSON config with all new field names
        let json = r#"{
            "data_service": "0x1111111111111111111111111111111111111111",
            "recurring_collector": "0x2222222222222222222222222222222222222222",
            "max_initial_tokens": "1000",
            "max_ongoing_tokens_per_second": "100",
            "min_seconds_per_collection": 60,
            "max_seconds_per_collection": 3600,
            "duration_seconds": 86400,
            "deadline_seconds": 300,
            "pricing_table": {
                "1": {
                    "tokens_per_second": "10",
                    "tokens_per_entity_per_second": "2"
                },
                "42161": {
                    "tokens_per_second": "5",
                    "tokens_per_entity_per_second": "1"
                }
            }
        }"#;

        //* Act - Deserialize
        let config: DipsAgreementConfig =
            serde_json::from_str(json).expect("deserialization failed");

        //* Assert - Verify all fields
        use thegraph_core::alloy::primitives::{U256, address};

        assert_eq!(
            config.data_service,
            address!("1111111111111111111111111111111111111111"),
            "data_service mismatch"
        );
        assert_eq!(
            config.recurring_collector,
            address!("2222222222222222222222222222222222222222"),
            "recurring_collector mismatch"
        );
        assert_eq!(
            config.max_initial_tokens,
            U256::from(1000u64),
            "max_initial_tokens mismatch"
        );
        assert_eq!(
            config.max_ongoing_tokens_per_second,
            U256::from(100u64),
            "max_ongoing_tokens_per_second mismatch"
        );
        assert_eq!(
            config.min_seconds_per_collection, 60,
            "min_seconds_per_collection mismatch"
        );
        assert_eq!(
            config.max_seconds_per_collection, 3600,
            "max_seconds_per_collection mismatch"
        );
        assert_eq!(
            config.duration_seconds,
            Some(86400),
            "duration_seconds mismatch"
        );
        assert_eq!(config.deadline_seconds, 300, "deadline_seconds mismatch");

        // Verify pricing table
        assert_eq!(
            config.pricing_table.len(),
            2,
            "pricing_table should have 2 entries"
        );

        let chain_1_prices = config.pricing_table.get(&1).expect("chain 1 not found");
        assert_eq!(
            chain_1_prices.tokens_per_second,
            U256::from(10u64),
            "chain 1 tokens_per_second mismatch"
        );
        assert_eq!(
            chain_1_prices.tokens_per_entity_per_second,
            U256::from(2u64),
            "chain 1 tokens_per_entity_per_second mismatch"
        );

        let chain_42161_prices = config
            .pricing_table
            .get(&42161)
            .expect("chain 42161 not found");
        assert_eq!(
            chain_42161_prices.tokens_per_second,
            U256::from(5u64),
            "chain 42161 tokens_per_second mismatch"
        );
        assert_eq!(
            chain_42161_prices.tokens_per_entity_per_second,
            U256::from(1u64),
            "chain 42161 tokens_per_entity_per_second mismatch"
        );
    }

    #[test]
    fn test_dips_agreement_config_defaults() {
        //* Arrange - Minimal JSON with defaults
        let json = r#"{
            "data_service": "0x1111111111111111111111111111111111111111",
            "recurring_collector": "0x2222222222222222222222222222222222222222",
            "max_initial_tokens": "1000",
            "max_ongoing_tokens_per_second": "100",
            "min_seconds_per_collection": 60,
            "max_seconds_per_collection": 3600,
            "pricing_table": {}
        }"#;

        //* Act
        let config: DipsAgreementConfig =
            serde_json::from_str(json).expect("deserialization failed");

        //* Assert - Check defaults
        assert_eq!(
            config.duration_seconds, None,
            "duration_seconds should default to None"
        );
        assert_eq!(
            config.deadline_seconds, 600,
            "deadline_seconds should default to 600"
        );

        // Test the From conversion - None should map to u64::MAX
        let (agreement_config, _) = <(
            Arc<IndexingAgreementConfig>,
            Arc<BTreeMap<u64, IndexingAgreementChainPrices>>,
        )>::from(config);
        assert_eq!(
            agreement_config.duration_seconds(),
            u64::MAX,
            "duration_seconds None should convert to u64::MAX"
        );
    }
}
