pub(super) const GRAPHQL_QUERY: &str = indoc::indoc! {r#"{
    epoches(
      first: 1,
      orderBy: startBlock, orderDirection: desc
    ) {
      id
    }
  }"#,
};

/// The Graph network epoches query response types.
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
    use serde_with::serde_as;

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct EpochesResponse {
        pub epoches: Vec<Epoch>,
    }

    #[serde_as]
    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Epoch {
        #[serde_as(as = "serde_with::DisplayFromStr")]
        pub id: u32,
    }
}
