//! Implementation of the `agreements` command and its subcommands.

use std::str::FromStr;

use anyhow::anyhow;
use clap::{Command, arg, command};
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_rpc::admin::indexing_agreements::{
    CancelIndexingAgreement, IndexingAgreementsRpcClient,
};
use serde_json;
use thegraph_core::signed_message;
use uuid::Uuid;

use crate::{client, cmd::common, config::Config, signer};

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

/// The `agreements cancel` command
///
/// This function cancels a specific agreement by its ID.
///
/// This function calls the `cancel_indexing_agreement` RPC method on the DIPs gateway server.
pub async fn cancel(conf: Config, matches: &clap::ArgMatches) -> anyhow::Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let agreement_id = matches
        .get_one::<IndexingAgreementId>("AGREEMENT_ID")
        .ok_or_else(|| anyhow!("No AGREEMENT_ID provided"))?;

    // Create signer and domain
    let signer = signer::new_private_key_eip712_signer(&conf.signing_key);
    let signer_eip712_domain = signer::eip712_domain();

    // Create the cancellation payload
    let cancel_payload = CancelIndexingAgreement { id: *agreement_id };

    // Sign the payload
    let req = signed_message::sign(&signer, &signer_eip712_domain, cancel_payload)
        .map_err(|err| anyhow!("Failed to sign cancel agreement request: {}", err))?;

    // Call the correct RPC method with the signed request
    rpc_client
        .cancel_indexing_agreement(req.into())
        .await
        .map_err(|err| anyhow!("Failed to cancel agreement '{}': {}", agreement_id, err))?;

    println!("Agreement {} cancelled successfully.", agreement_id);

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
        .subcommands(&[
            command!("list")
                .alias("ls")
                .about("List all agreements for a given indexing request ID")
                .arg(
                    arg!(<INDEXING_REQUEST_ID> "The indexing request ID (UUIDv7)")
                        .value_parser(parse_indexing_request_id),
                ),
            command!("cancel")
                .about("Cancel a specific agreement by ID")
                .arg(
                    arg!(<AGREEMENT_ID> "The agreement ID (UUIDv7)")
                        .value_parser(parse_agreement_id),
                ),
        ])
}

/// Parses an IndexingRequestId from a string.
fn parse_indexing_request_id(s: &str) -> Result<IndexingRequestId, anyhow::Error> {
    Uuid::from_str(s)
        .map(Into::into)
        .map_err(|err| anyhow!("Invalid Indexing Request ID (UUIDv7) '{}': {}", s, err))
}

/// Parses an IndexingAgreementId from a string.
fn parse_agreement_id(s: &str) -> Result<IndexingAgreementId, anyhow::Error> {
    Uuid::from_str(s)
        .map(Into::into) // Assuming IndexingAgreementId implements From<Uuid>
        .map_err(|err| anyhow!("Invalid Agreement ID (UUIDv7) '{}': {}", s, err))
}
