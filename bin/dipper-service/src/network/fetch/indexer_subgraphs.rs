pub(super) const GRAPHQL_QUERY_FRAGMENT: &str = indoc::indoc! {r#"
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
                    orderBy: closedAtEpoch, orderDirection: desc
                ) {
                    id
                    allocatedTokens
                    createdAtEpoch
                    closedAtEpoch
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

/// The Graph network indexer subgraph query response types.
///
/// <div class="warning">
/// These types are used to deserialize the response from the Graph network subgraph.
/// These types are not meant to be used directly by the project logic.
///
/// Please, DO NOT mix or merge them.
/// </div>
///
/// See: https://github.com/graphprotocol/graph-network-subgraph/blob/master/schema.graphql
pub(super) mod types {
    use serde_with::serde_as;
    use thegraph_core::{AllocationId, DeploymentId, IndexerId, ProofOfIndexing, SubgraphId};

    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Subgraph {
        pub id: SubgraphId,
        pub versions: Vec<SubgraphVersion>,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SubgraphVersion {
        pub version: u32,
        pub subgraph_deployment: SubgraphDeployment,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SubgraphDeployment {
        #[serde(rename = "ipfsHash")]
        pub id: DeploymentId,
        #[serde(rename = "indexerAllocations")]
        pub allocations: Vec<Allocation>,
    }

    #[serde_as]
    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Allocation {
        pub id: AllocationId,
        pub created_at_epoch: u32,
        pub closed_at_epoch: Option<u32>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub allocated_tokens: u128,
        pub indexer: Indexer,
        pub poi: Option<ProofOfIndexing>,
    }

    #[serde_as]
    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Indexer {
        pub id: IndexerId,
        pub url: Option<String>,
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub staked_tokens: u128,
    }
}
