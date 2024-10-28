//! Dipper service configuration.
//!
//! This module contains the configuration for the Dipper service. The configuration is loaded from
//! a YAML file, and can be overridden by command line arguments.
//!
//! Configuration parameters:
//! - HTTP server configuration (port, etc.)
//! - DB path: Database path for state persistence
//! - Log level
//! - BigQuery config (credentials, etc.) - PY
//! - IP address resolution config (credentials, etc.) - PY?
//! - Network subgraph client config (credentials, etc.)

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use clap::Parser as _;
use serde::Deserialize;
use tracing::level_filters::LevelFilter;

#[derive(Debug, thiserror::Error)]
pub enum StartArgsError {
    #[error("io error: {0}")]
    Io(std::io::Error),

    #[error("serde error: {0}")]
    Serde(serde_yaml::Error),

    #[error("missing config path")]
    MissingConfigPath,
}

#[derive(Debug, clap::Parser, Deserialize)]
#[command(name = "start")]
pub struct StartArgs {
    #[arg(short, long)]
    pub config_path: Option<PathBuf>,

    #[arg(short, long)]
    pub db_path: Option<PathBuf>,

    #[arg(short, long)]
    #[serde(deserialize_with = "deserialize_log_level")]
    pub log_level: Option<LevelFilter>,
}

fn deserialize_log_level<'de, D>(deserializer: D) -> Result<Option<LevelFilter>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let level: Option<String> = Option::deserialize(deserializer)?;
    match level {
        None => Ok(None),
        Some(level) => level.parse().map(Some).map_err(serde::de::Error::custom),
    }
}

impl StartArgs {
    pub fn from_yaml(file_path: &Path) -> Result<Self, StartArgsError> {
        let mut file = File::open(file_path).map_err(StartArgsError::Io)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(StartArgsError::Io)?;
        let config: StartArgs = serde_yaml::from_str(&contents).map_err(StartArgsError::Serde)?;
        Ok(config)
    }

    pub fn merge(self, other: StartArgs) -> Self {
        StartArgs {
            config_path: other.config_path,
            db_path: other.db_path.or(self.db_path),
            log_level: other.log_level.or(self.log_level),
        }
    }

    pub fn parse_and_merge() -> Result<Self, StartArgsError> {
        let default = StartArgs::parse();
        let config = StartArgs::from_yaml(
            default
                .config_path
                .as_ref()
                .ok_or(StartArgsError::MissingConfigPath)?,
        )?;
        Ok(default.merge(config))
    }
}
