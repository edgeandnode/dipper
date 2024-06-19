// Integration tests are special. They rely on local-network being set up and we want to exclude this from the normal flow of `cargo test`.
#[cfg(feature = "integration-tests")]
mod integration_tests {
    use subgraph::NetworkSubgraph;

    #[test]
    fn integration_tests_run() {
        assert_eq!(true, true);
    }

    #[tokio::test]
    async fn async_integration_tests_run() {
        assert_eq!(true, true);
    }

    #[tokio::test]
    async fn local_network_subgraph_query() {
        let api_key = "deadbeefdeadbeefdeadbeefdeadbeef";
        let url = "http://localhost:7700";

        let network_subgraph_client = NetworkSubgraph::new(api_key.to_string(), url.to_string());

        network_subgraph_client.query().await.unwrap();
    }
}
