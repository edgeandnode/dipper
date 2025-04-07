use std::str::FromStr;

use clap::{Command, arg, command, value_parser};
use dipper_core::ids::IndexingRequestId;
use dipper_rpc::admin::indexing_requests::{CancelIndexingRequest, NewIndexingRequest};
use thegraph_core::{DeploymentId, SubgraphId, alloy::primitives::ChainId, signed_message};
use uuid::Uuid;

use super::{common, result::Result};
use crate::{client, client::IndexingRequestsRpcClient, config::Config, signer};

/// The `indexings` command implementation
pub(crate) async fn run(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            list(conf).await
        }
        Some(("status", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            status(conf, matches).await
        }
        Some(("register", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            register(conf, matches).await
        }
        Some(("cancel", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            cancel(conf, matches).await
        }
        _ => Err(anyhow::anyhow!("No indexings command specified").into()),
    }
}

/// The `indexings list` command
///
/// This function lists all registered indexing requests.
///
/// This function calls the `get_all_indexing_requests` RPC method on the DIPs gateway server.
// TODO(post-mvp): Add support for pagination
pub async fn list(conf: Config) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let res = rpc_client
        .get_all_indexing_requests()
        .await
        .map_err(|err| anyhow::anyhow!("Failed to list indexing requests: {err}"))?;

    // Print the result as pretty JSON so one can use `jq` to explore the output
    println!(
        "{}",
        serde_json::to_string_pretty(&res)
            .map_err(|err| anyhow::anyhow!("Failed to serialize indexing requests: {err}"))?
    );

    Ok(())
}

/// The `indexings status` command
pub async fn status(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);

    match matches.get_one::<IndexingRequestSelector>("INDEXING_ID") {
        // ID is an UUIDv7
        Some(IndexingRequestSelector::IndexingRequestId(id)) => {
            let res = rpc_client
                .get_indexing_request_by_id(*id)
                .await
                .map_err(|err| anyhow::anyhow!("Failed to get indexing request by ID: {err}"))?;

            // Print the result as pretty JSON so one can use `jq` to explore the output
            println!(
                "{}",
                serde_json::to_string_pretty(&res).map_err(|err| anyhow::anyhow!(
                    "Failed to serialize indexing request: {err}"
                ))?
            );

            Ok(())
        }
        // ID is a Deployment ID
        Some(IndexingRequestSelector::DeploymentId(id)) => {
            let res = rpc_client
                .get_indexing_requests_by_deployment_id(*id)
                .await
                .map_err(|err| {
                    anyhow::anyhow!("Failed to get indexing requests by deployment ID: {err}")
                })?;

            // Print the result as pretty JSON so one can use `jq` to explore the output
            println!(
                "{}",
                serde_json::to_string_pretty(&res).map_err(|err| anyhow::anyhow!(
                    "Failed to serialize indexing requests: {err}"
                ))?
            );

            Ok(())
        }
        // ID is a Subgraph ID
        Some(IndexingRequestSelector::SubgraphId(id)) => {
            // TODO(post-mvp): Add support for querying by Subgraph ID
            Err(anyhow::anyhow!("Invalid indexing request ID: `{id}`").into())
        }
        None => unreachable!("No ID provided"),
    }
}

/// The `indexings register` command
pub async fn register(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let signer = signer::new_private_key_eip712_signer(&conf.signing_key);
    let signer_eip712_domain = signer::eip712_domain();

    let request_deployment_id = match matches.get_one::<SubgraphIdOrDeploymentId>("SUBGRAPH") {
        // ID is a Deployment ID
        Some(SubgraphIdOrDeploymentId::DeploymentId(id)) => id,

        // ID is a Subgraph ID
        // TODO(post-mvp): Add support for querying by Subgraph ID
        Some(SubgraphIdOrDeploymentId::SubgraphId(id)) => {
            return Err(anyhow::anyhow!("Invalid subgraph ID: `{id}`").into());
        }
        None => unreachable!("No ID provided"),
    };

    let request_chain_id = matches
        .get_one::<ChainId>("CHAIN_ID")
        .ok_or_else(|| anyhow::anyhow!("No chain ID provided"))?;

    let req = signed_message::sign(
        &signer,
        &signer_eip712_domain,
        NewIndexingRequest {
            deployment_id: *request_deployment_id,
            chain_id: *request_chain_id,
        },
    )
    .map_err(|err| anyhow::anyhow!("Failed to sign RPC request: {err}"))?;

    let res = rpc_client.register_new_indexing_request(req.into()).await.map_err(
        |err| anyhow::anyhow!("Failed to register new indexing request for deployment '{request_deployment_id}' : {err}"),
    )?;

    println!("{}", res);

    Ok(())
}

/// The `indexings cancel` command
pub async fn cancel(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let signer = signer::new_private_key_eip712_signer(&conf.signing_key);
    let signer_eip712_domain = signer::eip712_domain();

    match matches.get_one::<IndexingRequestSelector>("INDEXING_ID") {
        // ID is an UUIDv7
        Some(IndexingRequestSelector::IndexingRequestId(id)) => {
            let req = signed_message::sign(
                &signer,
                &signer_eip712_domain,
                CancelIndexingRequest { id: *id },
            )
            .map_err(|err| anyhow::anyhow!("Failed to sign RPC request: {err}"))?;

            rpc_client
                .cancel_indexing_request(req.into())
                .await
                .map_err(|err| {
                    anyhow::anyhow!("Failed to cancel indexing request '{id}' : {err}")
                })?;

            Ok(())
        }
        // ID is a Subgraph ID or Deployment ID
        Some(_) => {
            // TODO(post-mvp): Add support for querying by Subgraph ID or Deployment ID
            Err(anyhow::anyhow!("Invalid indexing request ID").into())
        }
        None => unreachable!("No ID provided"),
    }
}

/// Create the `indexings` DIPs indexing requests admin command
pub(super) fn indexings_cmd() -> Command {
    command!("indexings")
        .about("Manage indexings")
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
                .about("List all indexing requests"),
            command!("status")
                .about("Get an indexing request status")
                .arg(
                    arg!(<INDEXING_ID> "The indexing request's ID (UUID, Subgraph ID or Deployment ID)")
                        .value_parser(value_parser!(IndexingRequestSelector)),
                ),
            command!("register")
                .about("Register a new indexing request")
                .args([
                    arg!(<SUBGRAPH> "The indexing request's Subgraph (or Deployment) ID")
                        .value_parser(value_parser!(SubgraphIdOrDeploymentId)),
                    arg!(<CHAIN_ID> "The ID of the chain indexed by the subgraph")
                        .value_parser(value_parser!(ChainId))
                        .required(true),
                ]),
            command!("cancel")
                .about("Cancel an existing indexing request")
                .arg(
                    arg!(<INDEXING_ID> "The indexing request's ID (UUID, Subgraph ID or Deployment ID)")
                        .value_parser(value_parser!(IndexingRequestSelector)),
                ),
        ])
}

/// A subgraph ID or deployment ID.
///
/// This type is used to parse a subgraph ID or deployment ID from a string.
#[derive(Debug, Clone)]
enum SubgraphIdOrDeploymentId {
    /// A subgraph ID
    SubgraphId(SubgraphId),
    /// A deployment ID
    DeploymentId(DeploymentId),
}

impl FromStr for SubgraphIdOrDeploymentId {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        // First, try to parse the value as a Deployment ID
        if let Ok(id) = val.parse() {
            return Ok(SubgraphIdOrDeploymentId::DeploymentId(id));
        }

        // Otherwise, try to parse the value as a Subgraph ID
        if let Ok(id) = val.parse() {
            return Ok(SubgraphIdOrDeploymentId::SubgraphId(id));
        }

        Err(anyhow::anyhow!("Invalid subgraph ID: {val}"))
    }
}

/// An _indexing request_ selector.
///
/// This type is used to parse an indexing request ID (UUID), subgraph ID or deployment ID from a
/// string.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
enum IndexingRequestSelector {
    /// An indexing request ID (UUIDv7)
    IndexingRequestId(IndexingRequestId),
    /// A subgraph ID
    SubgraphId(SubgraphId),
    /// A deployment ID
    DeploymentId(DeploymentId),
}

impl FromStr for IndexingRequestSelector {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        // First, try to parse the value as an Indexing Request ID (UUIDv7)
        if let Ok(id) = val.parse::<Uuid>().map(Into::into) {
            return Ok(IndexingRequestSelector::IndexingRequestId(id));
        }

        // Next, try to parse the value as a Deployment ID
        if let Ok(id) = val.parse() {
            return Ok(IndexingRequestSelector::DeploymentId(id));
        }

        // Finally, try to parse the value as a Subgraph ID
        if let Ok(id) = val.parse() {
            return Ok(IndexingRequestSelector::SubgraphId(id));
        }

        Err(anyhow::anyhow!("Invalid indexing request selector: {val}"))
    }
}
