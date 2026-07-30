//! RPC provider pool with automatic failover and retry.
//!
//! Ported from `rewards-eligibility-oracle/blockchain_client.py`.

use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use thegraph_core::alloy::{
    providers::{
        ProviderBuilder, RootProvider,
        fillers::{BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller},
    },
    transports::{RpcError, TransportError, TransportErrorKind},
};
use url::Url;

use crate::chain_client::ChainClientError;

/// Pull a `ChainClientError` back out of a `TransportError` if a closure
/// boxed it in via `TransportErrorKind::custom` (see `build_and_send_call`).
/// Returns `None` if the transport error came from elsewhere.
fn extract_chain_client_error(err: TransportError) -> Option<ChainClientError> {
    match err {
        RpcError::Transport(TransportErrorKind::Custom(boxed)) => {
            boxed.downcast::<ChainClientError>().ok().map(|b| *b)
        }
        _ => None,
    }
}

/// Error text that indicates a transient failure worth retrying, used only for faults
/// that arrive as prose rather than as a status code or JSON-RPC error object.
const RETRYABLE_ERROR_PATTERNS: &[&str] = &[
    "connection refused",
    "connection reset",
    "connection closed",
    "timeout",
    "timed out",
    "rate limit",
    "too many requests",
    "service unavailable",
    "bad gateway",
];

/// Build a read-only provider for one URL, with the pool's request timeout applied.
fn build_provider(url: Url, request_timeout: Duration) -> Result<HttpProvider, ChainClientError> {
    let client = reqwest::Client::builder()
        .timeout(request_timeout)
        .build()
        .map_err(|e| ChainClientError::ConfigError(format!("Failed to build HTTP client: {e}")))?;

    Ok(ProviderBuilder::new().connect_reqwest(client, url))
}

/// Type alias for the provider with default fillers.
pub type HttpProvider = FillProvider<
    JoinFill<
        thegraph_core::alloy::providers::Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

/// Several RPC endpoints treated as one, retried with exponential backoff on the current
/// endpoint and rotated through on failure.
#[derive(Debug)]
pub struct RpcProviderPool {
    /// Provider URLs (primary first, then fallbacks)
    providers: Vec<Url>,
    /// Current provider index (atomic for thread-safety)
    current_index: AtomicUsize,
    /// Request timeout per RPC call
    request_timeout: Duration,
    /// Maximum retries per provider before rotating
    max_retries: u32,
}

impl RpcProviderPool {
    /// Create a new RPC provider pool. Errors if no providers are configured.
    pub fn new(
        providers: Vec<Url>,
        request_timeout: Duration,
        max_retries: u32,
    ) -> Result<Self, ChainClientError> {
        if providers.is_empty() {
            return Err(ChainClientError::ConfigError(
                "At least one RPC provider URL is required".to_string(),
            ));
        }

        tracing::info!(
            provider_count = providers.len(),
            primary = %providers[0],
            "RPC provider pool initialized"
        );

        Ok(Self {
            providers,
            current_index: AtomicUsize::new(0),
            request_timeout,
            max_retries,
        })
    }

    /// Rotate to the next provider.
    ///
    /// Returns the new provider URL after rotation.
    pub fn rotate(&self) -> &Url {
        let old_idx = self.current_index.fetch_add(1, Ordering::Relaxed);
        self.url_at(old_idx + 1)
    }

    /// The endpoint an unbounded ring position lands on.
    fn url_at(&self, position: usize) -> &Url {
        &self.providers[position % self.providers.len()]
    }

    /// Run an RPC call, retrying the current endpoint with backoff and then rotating on to
    /// the next. `operation` names the call for logging.
    pub async fn execute<F, Fut, T>(&self, operation: &str, f: F) -> Result<T, ChainClientError>
    where
        F: Fn(HttpProvider) -> Fut,
        Fut: Future<Output = Result<T, TransportError>>,
    {
        // What each endpoint said, in the order they were tried. `sign_and_send` matches
        // this text to tell a stale nonce from a transport fault, so a rejection from the
        // first endpoint has to survive a different kind of failure on the next.
        let mut reasons: Vec<String> = Vec::with_capacity(self.providers.len());
        let mut providers_tried = 0;

        // Walk the ring by local offset from wherever the pool points. Re-reading the shared
        // index each time lets a concurrent rotation send this call back to a provider it
        // already tried, so it can give up without ever reaching the healthy one.
        let start = self.current_index.load(Ordering::Relaxed);

        loop {
            let current_url = self.url_at(start + providers_tried).clone();

            // Retry loop for current provider
            let mut endpoint_error: Option<TransportError> = None;
            for attempt in 0..=self.max_retries {
                let outcome = match build_provider(current_url.clone(), self.request_timeout) {
                    Ok(provider) => f(provider).await,
                    Err(e) => Err(TransportErrorKind::custom(e)),
                };
                match outcome {
                    Ok(result) => return Ok(result),
                    Err(e) if Self::is_retryable(&e) && attempt < self.max_retries => {
                        let delay = Self::backoff_delay(attempt);
                        tracing::warn!(
                            operation,
                            provider = %current_url,
                            attempt = attempt + 1,
                            max_retries = self.max_retries,
                            delay_ms = delay.as_millis(),
                            error = %e,
                            "Retryable RPC error, backing off"
                        );
                        tokio::time::sleep(delay).await;
                        endpoint_error = Some(e);
                    }
                    Err(e) => {
                        endpoint_error = Some(e);
                        break;
                    }
                }
            }
            // Every attempt records why it failed before stopping, so the fallback only
            // covers a configuration that somehow allows no attempt at all.
            let endpoint_error = endpoint_error
                .unwrap_or_else(|| TransportErrorKind::custom_str("no attempt was made"));

            reasons.push(format!("{current_url}: {endpoint_error}"));
            providers_tried += 1;

            // Check if we've tried all providers
            if providers_tried >= self.providers.len() {
                // Preserve structured ChainClientError instances boxed in via
                // TransportErrorKind::custom (e.g. ContractRevert from gas
                // estimation). Otherwise fall back to the generic wrap.
                if let Some(typed) = extract_chain_client_error(endpoint_error) {
                    return Err(typed);
                }

                return Err(ChainClientError::RpcError(anyhow::anyhow!(
                    "All {} RPC providers failed for '{}': {}",
                    self.providers.len(),
                    operation,
                    reasons.join("; "),
                )));
            }

            // Advance the shared index as well, so later calls start from a provider that has
            // not just failed rather than repeating this one's discovery.
            self.rotate();
            let next_url = self.url_at(start + providers_tried);
            tracing::warn!(
                operation,
                old_provider = %current_url,
                new_provider = %next_url,
                providers_tried,
                total_providers = self.providers.len(),
                error = %endpoint_error,
                "Rotating RPC provider after failures"
            );
        }
    }

    /// Whether an error is worth trying again rather than giving up on. Each check can only
    /// say yes, so a fault the status and the error code both miss still gets read as text.
    fn is_retryable(error: &TransportError) -> bool {
        // A 5xx is the server failing for its own reasons and 429 is it declining; either
        // can succeed on a retry or another endpoint. A status is worth reading before the
        // text, because the same digits inside a revert reason mean nothing.
        if let RpcError::Transport(kind) = error
            && let Some(http) = kind.as_http_error()
            && (http.status >= 500 || http.status == 429)
        {
            return true;
        }

        // Some providers answer 200 and report being overloaded in the JSON-RPC error
        // instead, each with its own code. Alloy knows those codes, and reports a genuine
        // execution error such as a revert as not worth retrying.
        if let RpcError::ErrorResp(payload) = error
            && payload.is_retry_err()
        {
            return true;
        }

        let error_str = error.to_string().to_lowercase();
        RETRYABLE_ERROR_PATTERNS
            .iter()
            .any(|p| error_str.contains(p))
    }

    /// Calculate backoff delay for a retry attempt.
    ///
    /// Uses exponential backoff: 1s, 2s, 4s, 8s, 16s, capped at 30s.
    pub fn backoff_delay(attempt: u32) -> Duration {
        const BASE_MS: u64 = 1000;
        const MAX_MS: u64 = 30_000;

        let delay_ms = BASE_MS.saturating_mul(1u64 << attempt.min(5));
        Duration::from_millis(delay_ms.min(MAX_MS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 500 is the status a provider answered on 2026-07-29 while an accepted agreement went
    /// unfunded. Reading it here is what earns a retry on that provider before rotating; the
    /// rotation itself is unconditional, so this decides attempts rather than failover.
    #[test]
    fn server_faults_are_retryable_by_status() {
        for status in [500, 502, 503, 504, 429] {
            let err = TransportErrorKind::http_error(status, "provider fault".to_string());
            assert!(
                RpcProviderPool::is_retryable(&err),
                "HTTP {status} should be retryable"
            );
        }
    }

    /// A 4xx other than 429 means the request itself is wrong, so resending it unchanged
    /// to the same or another provider cannot succeed.
    #[test]
    fn client_faults_are_not_retryable_by_status() {
        for status in [400, 401, 403, 404] {
            let err = TransportErrorKind::http_error(status, "bad request".to_string());
            assert!(
                !RpcProviderPool::is_retryable(&err),
                "HTTP {status} should not be retryable"
            );
        }
    }

    /// Some providers and the proxies in front of them report throttling under a 4xx rather
    /// than a 429. The status alone reads as "your request is wrong", so the wording is what
    /// tells this apart from a request that will be refused however often it is sent.
    #[test]
    fn throttling_described_in_a_client_fault_body_is_retryable() {
        let err = TransportErrorKind::http_error(403, "rate limit exceeded".to_string());
        assert!(
            RpcProviderPool::is_retryable(&err),
            "a 403 that explains it is throttling should be retryable"
        );
    }

    /// Alloy recognises the error codes providers use for throttling, but not every
    /// transient fault has one. A gateway that describes a timeout in an otherwise ordinary
    /// response is still worth another go, and only the wording says so.
    #[test]
    fn transient_faults_described_in_a_json_rpc_error_are_retryable() {
        let payload = serde_json::from_str(r#"{"code":-32603,"message":"connection reset"}"#)
            .expect("JSON-RPC error payload");
        let err: TransportError = RpcError::ErrorResp(payload);
        assert!(
            RpcProviderPool::is_retryable(&err),
            "a reset described in the error body should be retryable"
        );
    }

    #[test]
    fn test_backoff_delay_calculation() {
        // 1s, 2s, 4s, 8s, 16s, 32s->30s
        assert_eq!(RpcProviderPool::backoff_delay(0), Duration::from_secs(1));
        assert_eq!(RpcProviderPool::backoff_delay(1), Duration::from_secs(2));
        assert_eq!(RpcProviderPool::backoff_delay(2), Duration::from_secs(4));
        assert_eq!(RpcProviderPool::backoff_delay(3), Duration::from_secs(8));
        assert_eq!(RpcProviderPool::backoff_delay(4), Duration::from_secs(16));
        assert_eq!(RpcProviderPool::backoff_delay(5), Duration::from_secs(30)); // capped at 30s
        assert_eq!(RpcProviderPool::backoff_delay(10), Duration::from_secs(30)); // stays capped
    }

    /// Faults that arrive with no status and no error code, only a description, which is
    /// what a connection that never got a reply looks like.
    #[test]
    fn test_retryable_error_detection() {
        let retryable_errors = [
            "connection refused by remote host",
            "Connection Reset by peer",
            "request TIMEOUT exceeded",
            "Service Unavailable",
            "Bad Gateway",
            "rate limit exceeded",
            "too many requests, slow down",
        ];

        for err_str in retryable_errors {
            let err = TransportErrorKind::custom_str(err_str);
            assert!(
                RpcProviderPool::is_retryable(&err),
                "expected '{err_str}' to be retryable"
            );
        }

        let non_retryable_errors = [
            "nonce too low",
            "insufficient funds",
            "execution reverted",
            "invalid signature",
        ];

        for err_str in non_retryable_errors {
            let err = TransportErrorKind::custom_str(err_str);
            assert!(
                !RpcProviderPool::is_retryable(&err),
                "expected '{err_str}' to not be retryable"
            );
        }
    }

    #[test]
    fn test_provider_pool_requires_at_least_one_provider() {
        let result = RpcProviderPool::new(vec![], Duration::from_secs(30), 3);
        assert!(result.is_err());

        let err = result.unwrap_err();
        match err {
            ChainClientError::ConfigError(msg) => {
                assert!(msg.contains("At least one RPC provider"));
            }
            _ => panic!("Expected ConfigError"),
        }
    }

    #[test]
    fn test_provider_pool_rotation() {
        let providers = vec![
            Url::parse("https://rpc1.example.com").unwrap(),
            Url::parse("https://rpc2.example.com").unwrap(),
            Url::parse("https://rpc3.example.com").unwrap(),
        ];

        let pool = RpcProviderPool::new(providers.clone(), Duration::from_secs(30), 3).unwrap();

        assert_eq!(pool.rotate().as_str(), "https://rpc2.example.com/");
        assert_eq!(pool.rotate().as_str(), "https://rpc3.example.com/");

        // Wraps back round rather than running off the end.
        assert_eq!(pool.rotate().as_str(), "https://rpc1.example.com/");
    }
}
