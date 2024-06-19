use reqwest::Client as ReqwestClient;
use serde::{Deserialize, Serialize};
use thegraph_core::client::Client;

use dipper_common::models::Indexer;

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("client query error: {0}")]
    Client(String),
}

pub struct NetworkSubgraph {
    client: Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSubgraphQueryResult {
    indexers: Vec<Indexer>,
}

impl NetworkSubgraph {
    pub fn new(api_key: String, url: String) -> Self {
        let client = ReqwestClient::builder().build().unwrap();
        let url = url.parse().unwrap();
        let client = Client::builder(client, url)
            .with_auth_token(Some(api_key.clone()))
            .build();
        Self { client }
    }

    pub async fn query(&self) -> Result<NetworkSubgraphQueryResult, QueryError> {
        let query = NETWORK_SUBGRAPH_DOCUMENT;
        let response: Result<NetworkSubgraphQueryResult, String> = self.client.query(query).await;
        let response = response.map_err(QueryError::Client)?;
        Ok(response)
    }
}

const NETWORK_SUBGRAPH_DOCUMENT: &str = r#"
{
    indexers {
        id
        url
    }
}
"#;
