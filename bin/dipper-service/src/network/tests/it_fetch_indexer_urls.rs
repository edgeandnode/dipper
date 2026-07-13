//! Integration tests for the indexer URLs fetch: drive `fetch_snapshot`
//! over HTTP against a wiremock subgraph, covering auth, pagination
//! across the page boundary, invalid-entry skipping, and error surfacing.

use thegraph_core::IndexerId;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, header, method},
};

use crate::network::service::indexer_urls::fetch_snapshot;

const ZERO_CURSOR: &str = "0x0000000000000000000000000000000000000000";

/// 20-byte hex address with `n` as its numeric value.
fn addr(n: u32) -> String {
    format!("0x{n:040x}")
}

fn page_body(indexers: &[(String, String)]) -> serde_json::Value {
    let entries: Vec<_> = indexers
        .iter()
        .map(|(id, url)| serde_json::json!({ "id": id, "url": url }))
        .collect();
    serde_json::json!({ "data": { "indexers": entries } })
}

#[tokio::test]
async fn fetch_paginates_past_a_full_page_and_skips_invalid_entries() {
    //* Given
    // A full first page of 1,000 indexers forces a second request; the
    // second page mixes a valid entry with an invalid URL and a bad id.
    let server = MockServer::start().await;

    let page1: Vec<_> = (1..=1000)
        .map(|n| (addr(n), format!("https://indexer-{n}.example.com/")))
        .collect();
    let page2 = vec![
        (addr(1001), "https://indexer-1001.example.com/".to_string()),
        (addr(1002), "not a url".to_string()),
        (
            "not-an-address".to_string(),
            "https://x.example.com/".to_string(),
        ),
    ];

    Mock::given(method("POST"))
        .and(header("authorization", "Bearer it-test-key"))
        .and(body_string_contains(ZERO_CURSOR))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&page1)))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer it-test-key"))
        .and(body_string_contains(&addr(1000)))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&page2)))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let snapshot = fetch_snapshot(&client, &endpoint, Some("it-test-key"))
        .await
        .expect("fetch should succeed");

    //* Then
    // 1,000 from page 1 plus the single valid entry from page 2.
    assert_eq!(snapshot.len(), 1_001);
    let valid: IndexerId = addr(1001).parse().unwrap();
    assert_eq!(
        snapshot.get(&valid).map(|url| url.as_str()),
        Some("https://indexer-1001.example.com/")
    );
    let invalid: IndexerId = addr(1002).parse().unwrap();
    assert!(!snapshot.contains_key(&invalid));
}

#[tokio::test]
async fn fetch_without_api_key_sends_no_authorization_header() {
    //* Given
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page_body(&[(
            addr(7),
            "https://indexer-7.example.com/".to_string(),
        )])))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let snapshot = fetch_snapshot(&client, &endpoint, None)
        .await
        .expect("fetch should succeed");

    //* Then
    assert_eq!(snapshot.len(), 1);
    let received = server.received_requests().await.unwrap();
    assert!(
        received
            .iter()
            .all(|req| !req.headers.contains_key("authorization"))
    );
}

#[tokio::test]
async fn fetch_surfaces_graphql_errors() {
    //* Given
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "errors": [{ "message": "Type `Bytes` is not a valid input type" }]
        })))
        .mount(&server)
        .await;

    //* When
    let client = reqwest::Client::new();
    let endpoint = server.uri().parse().unwrap();
    let result = fetch_snapshot(&client, &endpoint, None).await;

    //* Then
    let err = result.expect_err("fetch should fail").to_string();
    assert!(err.contains("subgraph errors"), "unexpected error: {err}");
}
