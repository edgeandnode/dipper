//! The Graph network subgraph indexes the Graph network smart contract which is responsible,
//! among other things, to act as an on-chain registry for subgraphs and their deployments.
//!
//! This module contains the logic necessary to query the Graph to get the latest state of the
//! network subgraph.

use super::{indexer_operators, indexer_subgraphs};

mod paginated_client;
mod queries;

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

    pub async fn fetch_subgraphs(&self) -> anyhow::Result<Vec<indexer_subgraphs::types::Subgraph>> {
        self.client
            .paginated_query(indexer_subgraphs::GRAPHQL_QUERY_FRAGMENT, 1000)
            .await
            .map_err(Into::into)
    }

    pub async fn fetch_indexer_operators(
        &self,
    ) -> anyhow::Result<Vec<indexer_operators::types::Indexer>> {
        self.client
            .paginated_query(indexer_operators::GRAPHQL_QUERY_FRAGMENT, 1000)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    mod it_subgraph_paginated_client;
}
