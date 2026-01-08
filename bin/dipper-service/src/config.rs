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

    /// Maximum retry attempts for transient failures (default: 3)
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

#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct DipsAgreementConfig {
    /// The _indexing agreement_'s service address.
    pub service: Address,
    /// The _indexing agreement_'s maximum amount that can be collected for the subgraph initial
    /// sync.
    pub max_initial_amount: U256,
    /// The _indexing agreement_'s maximum amount collectable per epoch.
    pub max_ongoing_amount_per_epoch: U256,
    /// The _indexing agreement_'s maximum epochs per collection.
    pub max_epochs_per_collection: u32,
    /// The _indexing agreement_'s minimum epochs per collection.
    pub min_epochs_per_collection: u32,
    /// The _indexing agreement_'s duration in epochs.
    pub duration_epochs: Option<u32>,

    /// The _indexing agreement_'s per chain pricing table.
    pub pricing_table: BTreeMap<ChainId, ChainPrices>,
}

/// Per-chain prices for the DIPs _indexing agreement_.
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct ChainPrices {
    /// The price per block in wei GRT.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub base_price_per_epoch: U256,
    /// The price per entity in wei GRT per epoch.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub price_per_entity: U256,
}

/// The Admin RPC server configuration
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct AdminRpcConfig {
    /// The RPC server listen address
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub listen_addr: std::net::SocketAddr,

    /// The set of addresses that are allowed to access the RPC server
    #[serde_as(as = "serde_with::SetLastValueWins<_>")]
    pub allowlist: BTreeSet<Address>,
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

/// The _indexing agreement_ configuration.
///
/// It holds the configuration for the _indexing agreements_, e.g., the service address, the
/// maximum amount that can be collected for the subgraph initial sync, the maximum amount
/// collectable per epoch, etc.
#[derive(Debug)]
pub struct IndexingAgreementConfig {
    /// The _indexing agreement_'s service address.
    pub service: Address,
    /// The _indexing agreement_'s maximum amount that can be collected for the subgraph initial
    /// sync.
    pub max_initial_amount: U256,
    /// The _indexing agreement_'s maximum amount collectable per epoch.
    pub max_ongoing_amount_per_epoch: U256,
    /// The _indexing agreement_'s maximum epochs per collection.
    pub max_epochs_per_collection: u32,
    /// The _indexing agreement_'s minimum epochs per collection.
    pub min_epochs_per_collection: u32,
    /// The _indexing agreement_'s duration in epochs.
    pub duration_epochs: Option<u32>,
}

/// The _indexing agreement_'s per-chain prices.
#[derive(Debug)]
pub struct IndexingAgreementChainPrices {
    /// The price per block in wei GRT.
    pub base_price_per_epoch: U256,
    /// The price per entity in wei GRT per epoch.
    pub price_per_entity: U256,
}

impl IndexingAgreementConfig {
    /// Get the _indexing agreement_'s service address.
    pub fn service(&self) -> Address {
        self.service
    }

    /// Get the _indexing agreement_'s maximum amount that can be collected for the subgraph initial
    /// sync.
    pub fn max_initial_amount(&self) -> U256 {
        self.max_initial_amount
    }

    /// Get the _indexing agreement_'s maximum amount collectable per epoch.
    pub fn max_ongoing_amount_per_epoch(&self) -> U256 {
        self.max_ongoing_amount_per_epoch
    }

    /// Get the _indexing agreement_'s maximum epochs per collection.
    pub fn max_epochs_per_collection(&self) -> u32 {
        self.max_epochs_per_collection
    }

    /// Get the _indexing agreement_'s minimum epochs per collection.
    pub fn min_epochs_per_collection(&self) -> u32 {
        self.min_epochs_per_collection
    }

    /// Get the _indexing agreement_'s duration in epochs.
    pub fn duration_epochs(&self) -> u32 {
        self.duration_epochs.unwrap_or(u32::MAX)
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
            service: value.service,
            max_initial_amount: value.max_initial_amount,
            max_ongoing_amount_per_epoch: value.max_ongoing_amount_per_epoch,
            max_epochs_per_collection: value.max_epochs_per_collection,
            min_epochs_per_collection: value.min_epochs_per_collection,
            duration_epochs: value.duration_epochs,
        };
        let prices = value
            .pricing_table
            .into_iter()
            .map(|(chain_id, prices)| {
                (
                    chain_id,
                    IndexingAgreementChainPrices {
                        base_price_per_epoch: prices.base_price_per_epoch,
                        price_per_entity: prices.price_per_entity,
                    },
                )
            })
            .collect();
        (Arc::new(config), Arc::new(prices))
    }
}
