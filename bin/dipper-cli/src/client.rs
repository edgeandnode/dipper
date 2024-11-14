pub use dipper_core::rpc::indexing_requests::{
    AdminIndexingRequestsRpcClient, IndexingRequestsRpcClient,
};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use url::Url;

/// Create a new JSON-RPC HTTP client.
pub fn new(url: &Url) -> HttpClient {
    HttpClientBuilder::new()
        .set_tcp_no_delay(true)
        .build(url)
        .unwrap()
}
