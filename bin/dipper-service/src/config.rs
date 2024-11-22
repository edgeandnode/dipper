//! Dipper service configuration

use std::{collections::BTreeSet, path::Path, time::Duration};

use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use serde_with::serde_as;
use thegraph_core::{
    alloy::{
        primitives::{Address, ChainId},
        signers::k256::SecretKey,
    },
    DeploymentId,
};
use url::Url;

/// The configuration for the DIPs service
#[derive(custom_debug::CustomDebug, serde::Deserialize)]
pub struct Config {
    /// The Admin RPC server configuration
    pub admin_rpc: AdminRpcConfig,
    /// The Indexer RPC server configuration
    pub indexer_rpc: IndexerRpcConfig,
    /// The database configuration
    pub db: DbConfig,
    /// The Indexing Indexer Selection Algorithm (IISA) configuration
    pub iisa: IisaConfig,
    /// The network service configuration
    pub network: NetworkConfig,
    /// The signer configuration
    pub signer: SignerConfig,
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
    pub allowlist: BTreeSet<Address>,
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

/// The Indexing Indexer Selection Algorithm (IISA) configuration
#[derive(Debug, serde::Deserialize)]
pub struct IisaConfig {
    /// The GeoIP resolver service auth token
    ///
    /// This token is used to authenticate the GeoIP resolver with the `ipinfo.io` service.
    pub geoip_auth: Hidden<String>,

    /// The BigQuery project ID
    pub bigquery_project_id: String,

    /// The BigQuery region
    pub bigquery_region: String,
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
