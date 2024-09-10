use serde::Deserialize;
use thegraph_graphql_http::{
    http::request::IntoRequestParameters,
    http_client::{ReqwestExt, ResponseResult},
};

/// Send an authenticated GraphQL query to a subgraph.
pub async fn send_query<T>(
    client: &reqwest::Client,
    url: reqwest::Url,
    auth: Option<&str>,
    query: impl IntoRequestParameters + Send,
) -> Result<ResponseResult<T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    let mut builder = client.post(url);

    if let Some(auth) = auth {
        builder = builder.bearer_auth(auth)
    }

    let res = builder
        .send_graphql(query)
        .await
        .map_err(|err| err.to_string())?;

    Ok(res)
}

pub async fn send_subgraph_query<T>(
    client: &reqwest::Client,
    subgraph_url: reqwest::Url,
    auth: Option<&str>,
    query: impl IntoRequestParameters + Send,
) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    send_query(client, subgraph_url, auth, query)
        .await
        .map_err(|err| format!("Error sending subgraph graphql query: {}", err))?
        .map_err(|err| err.to_string())
}

/// Subgraphs sometimes fall behind, be it due to failing or the Graph Node may be having issues.
/// The `_meta` field can now be added to any query so that it is possible to determine against
/// which block the query was effectively executed.
pub mod meta {
    use serde::Deserialize;
    use thegraph_core::BlockPointer;

    use super::send_query;

    const SUBGRAPH_META_QUERY_DOCUMENT: &str = r#"{ meta: _meta { block { number hash } } }"#;

    #[derive(Debug, Deserialize)]
    pub struct SubgraphMetaQueryResponse {
        pub meta: Meta,
    }

    #[derive(Debug, Deserialize)]
    pub struct Meta {
        pub block: BlockPointer,
    }

    pub async fn send_bootstrap_meta_query(
        client: &reqwest::Client,
        subgraph_url: reqwest::Url,
        ticket: Option<&str>,
    ) -> Result<SubgraphMetaQueryResponse, String> {
        send_query(client, subgraph_url, ticket, SUBGRAPH_META_QUERY_DOCUMENT)
            .await
            .map_err(|err| format!("Error sending subgraph meta query: {}", err))?
            .map_err(|err| err.to_string())
    }
}

pub mod page {
    use serde::{ser::SerializeMap as _, Deserialize, Serialize, Serializer};
    use serde_json::value::RawValue;
    use thegraph_core::{BlockHash, BlockNumber};
    use thegraph_graphql_http::{
        graphql::{Document, IntoDocument, IntoDocumentWithVariables},
        http_client::ResponseResult,
    };

    use super::{meta::Meta, send_query};

    /// The block at which the query should be executed.
    ///
    /// This is part of the input arguments of the [`SubgraphPageQuery`].
    #[derive(Clone, Debug, Default)]
    pub enum BlockHeight {
        #[default]
        Latest,
        Hash(BlockHash),
        Number(BlockNumber),
        NumberGte(BlockNumber),
    }

    impl Serialize for BlockHeight {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            let mut obj = s.serialize_map(Some(1))?;
            match self {
                Self::Latest => (),
                Self::Hash(hash) => obj.serialize_entry("hash", hash)?,
                Self::Number(number) => obj.serialize_entry("number", number)?,
                Self::NumberGte(number) => obj.serialize_entry("number_gte", number)?,
            }
            obj.end()
        }
    }

    /// The arguments of the [`SubgraphPageQuery`] query.
    #[derive(Clone, Debug, Serialize)]
    pub struct SubgraphPageQueryVars {
        /// The block at which the query should be executed.
        block: BlockHeight,
        /// The maximum number of entities to fetch.
        first: usize,
        /// The ID of the last entity fetched.
        last: String,
    }

    pub struct SubgraphPageQuery {
        query: Document,
        vars: SubgraphPageQueryVars,
    }

    impl SubgraphPageQuery {
        pub fn new(
            query: impl IntoDocument,
            block: BlockHeight,
            first: usize,
            last: String,
        ) -> Self {
            Self {
                query: query.into_document(),
                vars: SubgraphPageQueryVars { block, first, last },
            }
        }
    }

    impl IntoDocumentWithVariables for SubgraphPageQuery {
        type Variables = SubgraphPageQueryVars;

        fn into_document_with_variables(self) -> (Document, Self::Variables) {
            let query = indoc::formatdoc! {
                r#"query ($block: Block_height!, $first: Int!, $last: String!) {{
                    meta: _meta(block: $block) {{ block {{ number hash }} }}
                    results: {query}
                }}"#,
                query = self.query,
            };

            (query.into_document(), self.vars)
        }
    }

    #[derive(Debug, Deserialize)]
    pub struct SubgraphPageQueryResponse {
        pub meta: Meta,
        pub results: Vec<Box<RawValue>>,
    }

    /// An opaque entry in the response of a subgraph page query.
    ///
    /// This is used to determine the ID of the last entity fetched.
    #[derive(Debug, Deserialize)]
    pub struct SubgraphPageQueryResponseOpaqueEntry {
        pub id: String,
    }

    pub async fn send_subgraph_page_query(
        client: &reqwest::Client,
        subgraph_url: reqwest::Url,
        auth: Option<&str>,
        query: impl IntoDocument,
        block_height: BlockHeight,
        batch_size: usize,
        last: Option<String>,
    ) -> Result<ResponseResult<SubgraphPageQueryResponse>, String> {
        send_query(
            client,
            subgraph_url,
            auth,
            SubgraphPageQuery::new(query, block_height, batch_size, last.unwrap_or_default()),
        )
        .await
        .map_err(|err| format!("Error sending subgraph graphql query: {}", err))
    }
}
