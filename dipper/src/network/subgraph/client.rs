//! The Graph network subgraph indexes the Graph network smart contract which is responsible,
//! among other things, to act as an on-chain registry for subgraphs and their deployments.
//!
//! This module contains the logic necessary to query the Graph to get the latest state of the
//! network subgraph.

mod paginated_client;
mod queries;

const NETWORK_SUBGRAPH_QUERY: &str = indoc::indoc! {
    r#"
    subgraphs(
        block: $block
        orderBy: id, orderDirection: asc
        first: $first
        where: {
            id_gt: $last
            entityVersion: 2
            versionCount_gte: 1
        }
    ) {
        id
        versions(orderBy: version, orderDirection: desc) {
            version
            subgraphDeployment {
                ipfsHash
                indexerAllocations(
                    first: 100
                    orderBy: allocatedTokens, orderDirection: desc
                    where: { status: Active }
                ) {
                    id
                    allocatedTokens
                    indexer {
                        id
                        url
                        stakedTokens
                    }
                }
            }
        }
    }"#,
};

/// The Graph network subgraph types.
///
/// <div class="warning">
/// These types are used to deserialize the response from the Graph network subgraph.
/// These types are not meant to be used directly by the gateway logic.
///
/// Please, DO NOT mix or merge them.
/// </div>
///
/// See: https://github.com/graphprotocol/graph-network-subgraph/blob/master/schema.graphql
pub mod types {
    use serde::Deserialize;
    use serde_with::serde_as;
    use thegraph_core::{AllocationId, DeploymentId, IndexerId, SubgraphId};

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Subgraph {
        pub id: SubgraphId,
        pub versions: Vec<SubgraphVersion>,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SubgraphVersion {
        pub version: u32,
        pub subgraph_deployment: SubgraphDeployment,
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SubgraphDeployment {
        #[serde(rename = "ipfsHash")]
        pub id: DeploymentId,
        #[serde(rename = "indexerAllocations")]
        pub allocations: Vec<Allocation>,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Allocation {
        pub id: AllocationId,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub allocated_tokens: u128,
        pub indexer: Indexer,
    }

    #[serde_as]
    #[derive(Debug, Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Indexer {
        pub id: IndexerId,
        pub url: Option<String>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub staked_tokens: u128,
    }
}

/// The Graph network subgraph client.
#[derive(Clone)]
pub struct Client {
    client: paginated_client::Client,
}

impl Client {
    /// Creates a new [`Client`] instance.
    pub fn new(http_client: reqwest::Client, url: reqwest::Url, auth: String) -> Self {
        Self {
            client: paginated_client::Client::new(http_client, url, auth),
        }
    }

    pub async fn fetch(&self) -> anyhow::Result<Vec<types::Subgraph>> {
        self.client
            .paginated_query(NETWORK_SUBGRAPH_QUERY, 1000)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }
}

#[cfg(test)]
mod tests {
    mod it_subgraph_paginated_client;
}
