//! Implementation of the `agreements` command and its subcommands.

use std::str::FromStr;

use anyhow::anyhow;
use clap::{Command, arg, command};
use dipper_core::ids::IndexingRequestId;
use dipper_rpc::admin::indexing_agreements::IndexingAgreementsRpcClient;
use serde_json;
use uuid::Uuid;

use crate::{client, cmd::common, config::Config};

/// The `agreements list` command
///
/// This function lists all registered agreements for a given indexing request ID.
///
/// This function calls the `get_agreements_by_indexing_request_id` RPC method on the DIPs gateway server.
// TODO(post-mvp): Add support for pagination
pub async fn list(conf: Config, matches: &clap::ArgMatches) -> anyhow::Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let indexing_request_id = matches
        .get_one::<IndexingRequestId>("INDEXING_REQUEST_ID")
        .ok_or_else(|| anyhow!("No INDEXING_REQUEST_ID provided"))?;

    let res = rpc_client
        .get_agreements_by_indexing_request_id(*indexing_request_id)
        .await?;

    // Print the result as pretty JSON so one can use `jq` to explore the output
    println!("{}", serde_json::to_string_pretty(&res)?);

    Ok(())
}

/// Create the `agreements` DIPs agreements admin command
pub(super) fn agreements_cmd() -> Command {
    command!("agreements")
        .about("Manage agreements")
        .args(
            // Common arg options to be used by all subcommands
            &[
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
    Uuid::from_str(s)
        .map(Into::into)
        .map_err(|err| anyhow!("Invalid Indexing Request ID (UUIDv7) '{}': {}", s, err))
}
