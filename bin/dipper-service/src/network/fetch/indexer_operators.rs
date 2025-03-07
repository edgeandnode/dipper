pub(super) const GRAPHQL_QUERY_FRAGMENT: &str = indoc::indoc! {r#"
    indexers(
        block: $block
        orderBy: id, orderDirection: asc
        first: $first
        where: {
            id_gt: $last
        }
    ) {
        id
        account {
            operators(
                first: 100
                orderBy: id, orderDirection: asc
            ) {
                id
            }
        }
    }"#,
};

/// The Graph network indexer operator query response types.
///
/// <div class="warning">
/// These types are used to deserialize the response from the Graph network subgraph.
/// These types are not meant to be used directly by the project logic.
///
/// Please, DO NOT mix or merge them.
/// </div>
///
/// See: https://github.com/graphprotocol/graph-network-subgraph/blob/master/schema.graphql
pub(in crate::network) mod types {
    use thegraph_core::{IndexerId, alloy::primitives::Address};

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Indexer {
        pub id: IndexerId,
        pub account: Account,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Account {
        pub operators: Vec<Operator>,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct Operator {
        pub id: Address,
    }
}
