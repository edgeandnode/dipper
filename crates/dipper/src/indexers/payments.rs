//! Payment-related functionality for indexers.
//!
//! See: https://github.com/graphprotocol/indexer/tree/indexer-payments-cycle-1/packages/indexer-common/src/direct-indexer-payments

use serde::{Deserialize, Serialize};
use thegraph_graphql_http::{
    graphql::{Document, IntoDocument, IntoDocumentWithVariables},
    http_client::ReqwestExt,
};

use super::urls::StatusUrl;

const CREATE_AGREEMENT_MUTATION_DOCUMENT: &str = indoc::indoc! {r#"
     mutation CreateAgreement($signature: String!, $data: String!) {
         createIndexingAgreement(signature: $signature, data: $data) {
             signature
             data
             protocolNetwork
         }
     }
"#};

const CANCEL_AGREEMENT_MUTATION_DOCUMENT: &str = indoc::indoc! {r#"
     mutation CancelAgreement($signature: String!) {
         cancelIndexingAgreement(signature: $signature) {
             signature
         }
     }
"#};

const AGREEMENT_QUERY_DOCUMENT: &str = indoc::indoc! {r#"
     query GetAgreement($signature: String!) {
         agreement(signature: $signature) {
             signature
             data
             protocolNetwork
         }
     }
"#};

const GET_PRICE_QUERY_DOCUMENT: &str = indoc::indoc! {r#"
     query GetPrice($protocolNetwork: String!, $chainId: String!) {
         price(
             protocolNetwork: $protocolNetwork,
             chainId: $chainId,
         ) {
             pricePerBlock
             chainId
             protocolNetwork
         }
     }
"#};

const ALL_PRICES_QUERY_DOCUMENT: &str = indoc::indoc! {r#"
     query GetAllPrices {
         prices {
             pricePerBlock
             chainId
             protocolNetwork
         }
     }
"#};

#[derive(Debug, Serialize)]
struct CreateIndexingAgreementRequest {
    signature: String,
    data: String,
}

impl IntoDocumentWithVariables for CreateIndexingAgreementRequest {
    type Variables = Self;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (CREATE_AGREEMENT_MUTATION_DOCUMENT.into_document(), self)
    }
}

/// Response to the [`create_agreement`] mutation.
#[derive(Debug, Deserialize)]
pub struct CreateAgreementResponse {
    pub signature: String,
    pub data: String,
    #[serde(rename = "protocolNetwork")]
    pub protocol_network: String,
}

/// Create a new agreement with the indexer.
pub async fn create_agreement(
    client: &reqwest::Client,
    url: StatusUrl,
    signature: String,
    data: String,
) -> anyhow::Result<CreateAgreementResponse> {
    let _resp = client
        .post(url.into_inner())
        .send_graphql::<CreateAgreementResponse>(CreateIndexingAgreementRequest { signature, data })
        .await?;

    todo!()
}

#[derive(Debug, Serialize)]
struct CancelIndexingAgreementRequest {
    signature: String,
}

impl IntoDocumentWithVariables for CancelIndexingAgreementRequest {
    type Variables = Self;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (CANCEL_AGREEMENT_MUTATION_DOCUMENT.into_document(), self)
    }
}

/// Cancel an existing agreement with the indexer.
pub async fn cancel_agreement(
    client: &reqwest::Client,
    url: StatusUrl,
    signature: String,
) -> anyhow::Result<()> {
    let _resp = client
        .post(url.into_inner())
        .send_graphql::<serde_json::Value>(CancelIndexingAgreementRequest { signature })
        .await?;

    todo!()
}

#[derive(Debug, Serialize)]
struct GetAgreementRequest {
    signature: String,
}

impl IntoDocumentWithVariables for GetAgreementRequest {
    type Variables = Self;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (AGREEMENT_QUERY_DOCUMENT.into_document(), self)
    }
}

#[derive(Debug, Deserialize)]
pub struct GetAgreementResponse {
    pub signature: String,
    pub data: String,
    #[serde(rename = "protocolNetwork")]
    pub protocol_network: String,
}

/// Get an existing agreement with the indexer.
pub async fn get_agreement(
    client: &reqwest::Client,
    url: StatusUrl,
    signature: String,
) -> anyhow::Result<GetAgreementResponse> {
    let _resp = client
        .post(url.into_inner())
        .send_graphql::<GetAgreementResponse>(GetAgreementRequest { signature })
        .await?;

    todo!()
}

#[derive(Debug, Serialize)]
struct GetPriceRequest {
    #[serde(rename = "protocolNetwork")]
    protocol_network: String,
    #[serde(rename = "chainId")]
    chain_id: String,
}

impl IntoDocumentWithVariables for GetPriceRequest {
    type Variables = Self;

    fn into_document_with_variables(self) -> (Document, Self::Variables) {
        (GET_PRICE_QUERY_DOCUMENT.into_document(), self)
    }
}

/// Response to the [`get_indexing_price`] query.
#[derive(Debug, Deserialize)]
pub struct GetPriceResponse {
    #[serde(rename = "pricePerBlock")]
    pub price_per_block: f64, // Float!
    #[serde(rename = "chainId")]
    pub chain_id: String,
    #[serde(rename = "protocolNetwork")]
    pub protocol_network: String,
}

/// Get the price for indexing on the indexer.
pub async fn get_indexing_price(
    client: &reqwest::Client,
    url: StatusUrl,
    protocol_network: String,
    chain_id: String,
) -> anyhow::Result<GetPriceResponse> {
    let _resp = client
        .post(url.into_inner())
        .send_graphql::<GetPriceResponse>(GetPriceRequest {
            protocol_network,
            chain_id,
        })
        .await?;

    todo!()
}

/// Response to the [`get_all_indexing_prices`] query.
#[derive(Debug, Deserialize)]
pub struct GetAllPricesResponse {
    pub prices: Vec<IndexingPrice>,
}

#[derive(Debug, Deserialize)]
pub struct IndexingPrice {
    #[serde(rename = "pricePerBlock")]
    pub price_per_block: f64,
    #[serde(rename = "chainId")]
    pub chain_id: String,
    #[serde(rename = "protocolNetwork")]
    pub protocol_network: String,
}

/// Get all prices for indexing on the indexer.
pub async fn get_all_indexing_prices(
    client: &reqwest::Client,
    url: StatusUrl,
) -> anyhow::Result<GetAllPricesResponse> {
    let _resp = client
        .post(url.into_inner())
        .send_graphql::<GetAllPricesResponse>(ALL_PRICES_QUERY_DOCUMENT)
        .await?;

    todo!()
}
