use std::path::PathBuf;

use clap::{arg, value_parser, Arg, ArgMatches, Error, FromArgMatches};
use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use figment::{
    providers::{Env, Serialized},
    Figment,
};
use serde_with::{serde_as, skip_serializing_none, DisplayFromStr};
use thegraph_core::alloy::{
    primitives::{Address, ChainId},
    signers::k256::SecretKey,
};
use url::Url;

use crate::config::Config;

/// The prefix for environment variables
pub const ENV_PREFIX: &str = "DIPS_";

/// Macro rule to prepend the `DIPS_` prefix to environment variable name
#[macro_export]
#[doc(hidden)]
macro_rules! name_prefixed {
    ($name:literal) => {
        concat!("DIPS_", $name)
    };
}

/// Create the `--env-file` CLI argument.
///
/// This argument is used to specify the path to a `.env` file to load configuration from.
///
/// Parse the value as a `PathBuf`.
pub(super) fn env_file_arg() -> Arg {
    arg!(-e --"env-file" <FILE> "The .env file to load configuration from")
        .value_parser(value_parser!(PathBuf))
}

/// Create the `--server-url` CLI argument.
///
/// This argument is used to specify the URL of the DIPs gateway server.
///
/// Parse the value as a `Url`.
pub(super) fn server_url_arg() -> Arg {
    arg!(--"server-url" <URL> "The URL of the DIPs gateway server")
        .env(name_prefixed!("SERVER_URL"))
        .value_parser(value_parser!(Url))
}

/// Create the `--signing-key` CLI argument.
///
/// This argument is used to specify the secret key to sign requests with.
///
/// Parse the value as a `Hidden<SecretKey>`.
pub(super) fn signing_key_arg() -> Arg {
    arg!(--"signing-key" <KEY> "The secret key to sign requests with (hex)")
        .env(name_prefixed!("SIGNING_KEY"))
        .value_parser(dipper_core::config::secret_key_from_str)
}

/// Create the `--chain-id` CLI argument.
///
/// This argument is used to specify the DIPs payment wallet chain ID.
///
/// Parse the value as a `ChainId`.
pub(super) fn chain_id_arg() -> Arg {
    arg!(--"chain-id" <ID> "The chain ID of the DIPs payment wallet")
        .env(name_prefixed!("CHAIN_ID"))
        .value_parser(value_parser!(ChainId))
}

/// Create the `--payer` CLI argument.
///
/// This argument is used to specify the address of the DIPs payment wallet.
///
/// Parse the value as an `Address`.
pub(super) fn payer_arg() -> Arg {
    arg!(-p --payer <ADDRESS> "The address of the payer (hex)")
        .env(name_prefixed!("PAYER"))
        .value_parser(value_parser!(Address))
}

/// Load the configuration
pub fn load_conf(args: &ArgMatches) -> anyhow::Result<Config> {
    // Load the environment variables from the specified env file
    if let Some(env_file) = args.get_one::<PathBuf>("env-file") {
        if let Err(err) = dotenvy::from_path(env_file) {
            return Err(anyhow::anyhow!(
                "Failed to load the env file '{}': {}",
                env_file.display(),
                err
            ));
        } else {
            tracing::debug!("Loaded env file '{}'", env_file.display());
        }
    }

    // Load configuration from the CLI arguments
    let cli_conf = CliConfig::from_arg_matches(args)?;

    // Combine the configuration from the environment and the CLI
    let conf = Figment::new()
        .merge(Env::prefixed(ENV_PREFIX))
        .merge(Serialized::defaults(cli_conf))
        .extract()?;

    Ok(conf)
}

/// The CLI provided config
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct CliConfig {
    /// The URL of the DIPs gateway server
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub server_url: Option<Url>,
    /// The secret key to sign requests with
    #[serde_as(as = "Option<HiddenSecretKeyAsHexStr>")]
    pub signing_key: Option<Hidden<SecretKey>>,
    /// The DIPs payment wallet chain ID
    pub chain_id: Option<ChainId>,
    /// The address of the DIPs payment wallet
    pub payer: Option<Address>,
}

impl FromArgMatches for CliConfig {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, Error> {
        Ok(Self {
            server_url: matches.get_one("server-url").cloned(),
            signing_key: matches.get_one("signing-key").cloned(),
            chain_id: matches.get_one("chain-id").cloned(),
            payer: matches.get_one("payer").cloned(),
        })
    }

    fn update_from_arg_matches(&mut self, _matches: &ArgMatches) -> Result<(), Error> {
        unimplemented!()
    }
}
