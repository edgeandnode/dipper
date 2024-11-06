// Integration tests are special. They rely on local-network being set up, and we want to exclude this from the normal flow of `cargo test`.
#![cfg(feature = "integration-tests")]

#[test]
fn integration_tests_run() {
    assert_eq!(true, true);
}

#[tokio::test]
async fn async_integration_tests_run() {
    assert_eq!(true, true);
}
