//! Dipper service configuration

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use serde_with::serde_as;
use thegraph_core::{
    alloy::{
        primitives::{Address, ChainId, U256},
        signers::k256::SecretKey,
    },
    DeploymentId, IndexerId,
};
use url::Url;

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
    #[serde_as(as = "serde_with::MapSkipError<_ ,_>")]
    pub pricing_table: BTreeMap<ChainId, ChainPrices>,
}

/// Per-chain prices for the DIPs _indexing agreement_.
#[derive(Debug, serde::Deserialize)]
pub struct ChainPrices {
    /// The price per block in wei GRT.
    pub price_per_block: U256,
    /// The price per entity in wei GRT per epoch.
    pub price_per_entity_per_epoch: U256,
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

    /// The signer chain ID
    pub chain_id: ChainId,
}

/// The configuration for the TAP signer
#[serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct TapSignerConfig {
    /// The signing key to use for authentication
    #[serde_as(as = "HiddenSecretKeyAsHexStr")]
    pub secret_key: Hidden<SecretKey>,

    /// The signer chain ID
    pub chain_id: ChainId,

    /// The verifier contract address
    pub verifier: Address,
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
/// Load the configuration from a JSON file.
pub fn load_from_file(path: &Path) -> Result<Config, Error> {
    let config_content = std::fs::read_to_string(path)?;
    let config = serde_json::from_str(&config_content)?;
    Ok(config)
}
