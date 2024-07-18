use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use clap::Parser;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StartArgsError {
    #[error("io error: {0}")]
    Io(std::io::Error),

    #[error("serde error: {0}")]
    Serde(serde_yaml::Error),

    #[error("missing config path")]
    MissingConfigPath,
}

#[derive(Parser, Debug, Deserialize, Serialize)]
#[command(name = "start")]
pub struct StartArgs {
    #[arg(short, long)]
    pub config_path: Option<PathBuf>,

    #[arg(short, long)]
    pub db_path: Option<PathBuf>,

    #[arg(short, long)]
    pub log_level: Option<LevelFilter>,
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
