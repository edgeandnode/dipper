//! Integration tests for the network subgraph client.

use std::time::Duration;

use reqwest::Url;
use tracing_subscriber::{fmt::TestWriter, EnvFilter};

use crate::network::fetch::Client as NetworkSubgraphClient;

/// Initialize the tests tracing subscriber.
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .compact()
        .with_writer(TestWriter::default())
        .try_init();
}

/// Test helper to get the gateway base url from the environment.
fn test_gateway_base_url() -> Url {
    std::env::var("IT_TEST_ARBITRUM_GATEWAY_URL")
        .expect("Missing IT_TEST_ARBITRUM_GATEWAY_URL")
        .parse()
        .expect("Invalid IT_TEST_ARBITRUM_GATEWAY_URL")
}

/// Test helper to get the test auth token from the environment.
fn test_auth_token() -> String {
    std::env::var("IT_TEST_ARBITRUM_GATEWAY_AUTH").expect("Missing IT_TEST_ARBITRUM_GATEWAY_AUTH")
}

/// Test helper to build the subgraph url with the given subgraph ID.
fn test_subgraph_url(subgraph: impl AsRef<str>) -> Url {
    test_gateway_base_url()
        .join(&format!("api/deployments/id/{}", subgraph.as_ref()))
        .expect("Invalid URL")
}

/// The Graph Network Arbitrum subgraph in the network.
///
/// https://thegraph.com/explorer/subgraphs/DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp?view=About&chain=arbitrum-one
const GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID: &str = "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv";

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_subgraph_data() {
    init_test_tracing();

    //* Given
    let subgraph_url = test_subgraph_url(GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID);
    let auth_token = test_auth_token();

    let network_subgraph_client =
        NetworkSubgraphClient::new(reqwest::Client::new(), subgraph_url, auth_token);

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(30),
        network_subgraph_client.fetch_subgraphs(),
    )
    .await
    .expect("Timeout on network fetch subgraph data query");

    //* Then
    let response = res.expect("Failed to fetch data");

    assert!(!response.is_empty());
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_indexer_operators_data() {
    init_test_tracing();

    //* Given
    let subgraph_url = test_subgraph_url(GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID);
    let auth_token = test_auth_token();

    let network_subgraph_client =
        NetworkSubgraphClient::new(reqwest::Client::new(), subgraph_url, auth_token);

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(30),
        network_subgraph_client.fetch_indexer_operators(),
    )
    .await
    .expect("Timeout on network fetch indexer operators data query");

    //* Then
    let response = res.expect("Failed to fetch data");

    assert!(!response.is_empty());
}

#[test_with::env(IT_TEST_ARBITRUM_GATEWAY_URL, IT_TEST_ARBITRUM_GATEWAY_AUTH)]
#[tokio::test]
async fn fetch_latest_epoch_data() {
    init_test_tracing();

    //* Given
    let subgraph_url = test_subgraph_url(GRAPH_NETWORK_ARBITRUM_DEPLOYMENT_ID);
    let auth_token = test_auth_token();

    let network_subgraph_client =
        NetworkSubgraphClient::new(reqwest::Client::new(), subgraph_url, auth_token);

    //* When
    let res = tokio::time::timeout(
        Duration::from_secs(30),
        network_subgraph_client.fetch_latest_epoch(),
    )
    .await
    .expect("Timeout on network fetch latest epoch data query");

    //* Then
    let response = res.expect("Failed to fetch data");

    // Assert that the epoch ID is valid
    // At this moment, Feb 2025, the epoch ID is greater than 750
    assert!(response.id.0 > 750);
}
