use std::time::Duration;

use pyo3::prelude::*;
use reqwest::Url;
use thegraph_core::{deployment_id, DeploymentId};

use super::common;
use crate::{
    indexer_selection::iisa::{
        PyBigQueryProvider, PyDataManager, PyGeoipResolver, PyNetworkProvider,
    },
    network::{service as network_service, Snapshot, SubgraphClient},
};

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

/// The Graph Network Arbitrum subgraph in the network.
///
/// https://thegraph.com/explorer/subgraphs/DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp?view=About&chain=arbitrum-one
const GRAPH_NETWORK_ARBITRUM_SUBGRAPH_ID: DeploymentId =
    deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");

/// Use the network service to fetch the network subgraph.
pub async fn fetch_network_subgraph(deployment: DeploymentId) -> Snapshot {
    let url = test_gateway_base_url()
        .join(&format!("api/deployments/id/{}", deployment))
        .expect("Invalid URL");
    let auth = test_auth_token();

    let http_client = reqwest::Client::new();
    let subgraph_client = SubgraphClient::new(http_client, url, auth);
    let (mut handle, service) = network_service::new(subgraph_client, Duration::from_secs(60));
    tokio::spawn(service);
    handle
        .wait_ready()
        .await
        .expect("Failed to wait for service ready");

    let snapshot = handle.snapshot();
    snapshot.to_owned()
}

#[ignore = "Requires access to Google BigQuery"]
#[tokio::test]
async fn fetch_data_and_process() {
    common::add_assets_dir_to_sys_path();
    common::init_python_logging("it_iisa::fetch_data_and_process");
    common::init_test_tracing();

    //* Given
    // A graph network subgraph snapshot
    let snapshot = fetch_network_subgraph(GRAPH_NETWORK_ARBITRUM_SUBGRAPH_ID).await;

    Python::with_gil(|py| {
        // Instantiate the DataManager class
        let bigquery_provider = PyBigQueryProvider::new(py, "graph-mainnet", "US")
            .expect("Failed to create a new PyBigQueryProvider instance");
        let network_provider = {
            let geoip_resolver = PyGeoipResolver::new(py).expect("instantiate geoip resolver");
            PyNetworkProvider::new(py, geoip_resolver).expect("convert network provider")
        };

        let data_manager = PyDataManager::new(py, bigquery_provider, network_provider.clone())
            .expect("Failed to create a new PyDataManager instance");

        // Set the network snapshot
        network_provider
            .set_snapshot(py, snapshot.indexers_iter())
            .expect("set network provider snapshot");

        //* When
        // Perform the fetch data and process operation
        tracing::info!("Fetching data and updating");
        data_manager
            .fetch_data_and_update()
            .expect("fetch data and process");

        let data = data_manager.get_data().expect("Failed to get data");
        let indexer_rankings = data_manager
            .get_latency_linear_regression_indexer_rankings()
            .expect("Failed to get indexer rankings");
        let regression_results = data_manager
            .get_latency_linear_regression_results()
            .expect("Failed to get regression results");

        //* Then
        // Assert the data is not empty
        assert!(!data.is_empty().expect("Failed to call data.is_empty"));
        let (rows, _columns) = data.shape().expect("Failed to call data.shape");
        assert!(rows >= 1);

        // Assert the indexer rankings is not empty
        assert!(!indexer_rankings
            .is_empty()
            .expect("Failed to call indexer_rankings.is_empty"));
        let (rows, _columns) = indexer_rankings
            .shape()
            .expect("Failed to call indexer_rankings.shape");
        assert!(rows >= 1);

        // Assert the regression results is not empty
        assert!(!regression_results
            .is_empty()
            .expect("Failed to call regression_results.is_empty"));
        let (rows, _columns) = regression_results
            .shape()
            .expect("Failed to call regression_results.shape");
        assert!(rows >= 1);
    });
}
