use std::path::PathBuf;

use clap::{arg, command, value_parser, ArgGroup, Command};
use url::Url;

/// The `init` command
pub fn run(_args: &clap::ArgMatches) -> anyhow::Result<()> {
    Err(anyhow::anyhow!("init command not implemented yet"))
}

/// Create the `init` DIPs CLI configuration bootstrap command
pub(super) fn init_cmd() -> Command {
    command!("init")
        .about("Bootstrap the DIPs Admin CLI configuration file")
        .args(&[
            arg!(--"server-url" <URL> "The URL of the DIPs gateway server")
                .value_parser(value_parser!(Url)),
            arg!(--"generate-signing-key" "Generate a new secret key to sign requests with"),
            arg!(--"with-signing-key" <KEY> "The secret key to sign requests with")
                .value_parser(dipper_core::config::secret_key_from_str),
            arg!(--"with-signing-key-placeholder" <KEY> "The placeholder for the secret key, e.g., 'op://mainnet/dips/signing-key-1'")
                .value_parser(value_parser!(String)),
            arg!(-o --"output" <FILE> "The output file to write the configuration to")
                .value_parser(value_parser!(PathBuf)),
        ])
        .group(ArgGroup::new("key").args(["generate-signing-key", "with-signing-key", "with-signing-key-placeholder"]))
}
