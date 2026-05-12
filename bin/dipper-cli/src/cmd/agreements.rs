//! Implementation of the `agreements` command and its subcommands.
//!
//! Currently read-only — cancellation goes through `indexings target` with
//! `--num-candidates 0`, which terminates the parent request and fires the
//! on-chain cancel for every agreement under it.

use std::str::FromStr;

use clap::{Command, arg, command};
use dipper_core::ids::IndexingRequestId;

use super::{common, result::Result};
use crate::{client, client::IndexingAgreementsRpcClient, config::Config};

/// The `agreements` command implementation
pub(super) async fn run(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            list(conf, matches).await
        }
        _ => Err(anyhow::anyhow!("No agreements command specified").into()),
    }
}

/// The `agreements list` command
///
/// This function lists all registered agreements for a given indexing request ID.
///
/// This function calls the `get_agreements_by_indexing_request_id` RPC method on the DIPs gateway server.
// TODO(post-mvp): Add support for pagination
async fn list(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let indexing_request_id = matches
        .get_one::<IndexingRequestId>("INDEXING_REQUEST_ID")
        .ok_or_else(|| anyhow::anyhow!("No INDEXING_REQUEST_ID provided"))?;

    let res = rpc_client
        .get_agreements_by_indexing_request_id(*indexing_request_id)
        .await
        .map_err(|err| anyhow::anyhow!("Failed to list agreements: {err}"))?;

    // Print the result as pretty JSON so one can use `jq` to explore the output
    println!(
        "{}",
        serde_json::to_string_pretty(&res)
            .map_err(|err| anyhow::anyhow!("Failed to serialize agreements: {err}"))?
    );

    Ok(())
}

/// Create the `agreements` DIPs agreements admin command
pub(super) fn cmd() -> Command {
    command!("agreements")
        .about("Inspect agreements")
        .args(
            // Common arg options to be used by all subcommands
            [
                common::env_file_arg().global(true),
                common::server_url_arg().global(true),
                common::signing_key_arg().global(true),
            ],
        )
        .subcommands(&[command!("list")
            .alias("ls")
            .about("List all agreements for a given indexing request ID")
            .arg(
                arg!(<INDEXING_REQUEST_ID> "The indexing request ID (UUIDv7)")
                    .value_parser(parse_indexing_request_id),
            )])
}

/// Parses an IndexingRequestId from a string.
fn parse_indexing_request_id(s: &str) -> Result<IndexingRequestId, anyhow::Error> {
    uuid::Uuid::from_str(s)
        .map(Into::into)
        .map_err(|err| anyhow::anyhow!("Invalid Indexing Request ID (UUIDv7) '{s}': {err}"))
}
