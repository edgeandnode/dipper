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
    DeploymentId, IndexerId,
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
    /// The Indexer RPC server configuration
    pub indexer_rpc: IndexerRpcConfig,
    /// The database configuration
    pub db: DbConfig,
    /// The network service configuration
    pub network: NetworkConfig,
    /// The signer configuration
    pub signer: SignerConfig,
    /// The TAP signer configuration
    pub tap_signer: TapSignerConfig,
    /// The IISA (Indexing Indexer Selection Algorithm) service configuration
    pub iisa: IisaConfig,
    /// The reassignment service configuration
    #[serde(default)]
    pub reassignment: Option<ReassignmentConfig>,
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

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct DipsAgreementConfig {
    /// The data service address (SubgraphService contract).
    pub data_service: Address,
    /// The RecurringCollector contract address (used for EIP-712 signing domain).
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
    pub pricing_table: BTreeMap<ChainId, ChainPrices>,
}

fn default_deadline_seconds() -> u64 {
    300 // 5 minutes
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

/// The Indexer RPC server configuration
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct IndexerRpcConfig {
    /// The RPC server listen address
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub listen_addr: std::net::SocketAddr,

    /// The set of addresses that are allowed to access the RPC server
    #[serde_as(as = "serde_with::SetLastValueWins<_>")]
    pub allowlist: BTreeSet<IndexerId>,
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

/// The configuration for the TAP signer
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct TapSignerConfig {
    /// The signing key to use for authentication
    #[serde_as(as = "HiddenSecretKeyAsHexStr")]
    pub secret_key: Hidden<SecretKey>,

    /// The signer chain ID (protocol chain)
    pub chain_id: ChainId,

    /// The verifier contract address
    pub verifier: Address,
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
