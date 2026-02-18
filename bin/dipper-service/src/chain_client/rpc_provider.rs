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
    transports::TransportError,
};
use url::Url;

use crate::chain_client::ChainClientError;

/// Error patterns that indicate a transient failure worth retrying.
///
/// These patterns are matched case-insensitively against the error message.
const RETRYABLE_ERROR_PATTERNS: &[&str] = &[
    "connection refused",
    "connection reset",
    "connection closed",
    "timeout",
    "timed out",
    "rate limit",
    "too many requests",
    "503",
    "502",
    "429",
    "service unavailable",
    "bad gateway",
];

/// Type alias for the provider with default fillers.
pub type HttpProvider = FillProvider<
    JoinFill<
        thegraph_core::alloy::providers::Identity,
        JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
    >,
    RootProvider,
>;

/// RPC provider pool with automatic rotation and retry.
///
/// Manages multiple RPC provider URLs and automatically rotates between them
/// on failure. Uses exponential backoff for retries within a single provider.
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
    /// Create a new RPC provider pool.
    ///
    /// # Errors
    ///
    /// Returns an error if no providers are configured.
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

    /// Get the current provider URL.
    pub fn current_url(&self) -> &Url {
        let idx = self.current_index.load(Ordering::Relaxed) % self.providers.len();
        &self.providers[idx]
    }

    /// Build a provider for the current URL.
    pub fn get_provider(&self) -> Result<HttpProvider, ChainClientError> {
        let url = self.current_url();

        // Build HTTP client with timeout
        let client = reqwest::Client::builder()
            .timeout(self.request_timeout)
            .build()
            .map_err(|e| {
                ChainClientError::ConfigError(format!("Failed to build HTTP client: {e}"))
            })?;

        // Build alloy provider using the connect_reqwest method
        let provider = ProviderBuilder::new().connect_reqwest(client, url.clone());

        Ok(provider)
    }

    /// Rotate to the next provider.
    ///
    /// Returns the new provider URL after rotation.
    pub fn rotate(&self) -> &Url {
        let old_idx = self.current_index.fetch_add(1, Ordering::Relaxed);
        let new_idx = (old_idx + 1) % self.providers.len();
        &self.providers[new_idx]
    }

    /// Execute an RPC operation with retry and provider rotation.
    ///
    /// The closure receives a provider and should return a `Result`. On retryable
    /// errors, the operation is retried with exponential backoff. After exhausting
    /// retries, the pool rotates to the next provider and continues.
    ///
    /// # Arguments
    ///
    /// * `operation` - Name of the operation (for logging)
    /// * `f` - Closure that performs the RPC call
    ///
    /// # Type Parameters
    ///
    /// * `F` - Closure type
    /// * `Fut` - Future type returned by the closure
    /// * `T` - Success type
    pub async fn execute<F, Fut, T>(&self, operation: &str, f: F) -> Result<T, ChainClientError>
    where
        F: Fn(HttpProvider) -> Fut,
        Fut: Future<Output = Result<T, TransportError>>,
    {
        let initial_index = self.current_index.load(Ordering::Relaxed);
        let mut last_error: Option<TransportError> = None;
        let mut providers_tried = 0;

        loop {
            // Get current provider
            let provider = self.get_provider()?;
            let current_url = self.current_url().clone();

            // Retry loop for current provider
            for attempt in 0..=self.max_retries {
                match f(provider.clone()).await {
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
                        last_error = Some(e);
                    }
                    Err(e) => {
                        last_error = Some(e);
                        break;
                    }
                }
            }

            providers_tried += 1;

            // Check if we've tried all providers
            if providers_tried >= self.providers.len() {
                let err_msg = last_error
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown error".to_string());

                return Err(ChainClientError::RpcError(anyhow::anyhow!(
                    "All {} RPC providers failed for '{}': {}",
                    self.providers.len(),
                    operation,
                    err_msg
                )));
            }

            // Rotate to next provider
            let new_url = self.rotate();
            tracing::warn!(
                operation,
                old_provider = %current_url,
                new_provider = %new_url,
                providers_tried,
                total_providers = self.providers.len(),
                "Rotating RPC provider after failures"
            );

            // Safety check: ensure we actually rotated
            let new_index = self.current_index.load(Ordering::Relaxed);
            if new_index == initial_index && providers_tried > 0 {
                break;
            }
        }

        // Should not reach here, but handle gracefully
        let err_msg = last_error
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown error".to_string());

        Err(ChainClientError::RpcError(anyhow::anyhow!(
            "RPC operation '{}' failed: {}",
            operation,
            err_msg
        )))
    }

    /// Check if an error is retryable.
    fn is_retryable(error: &TransportError) -> bool {
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

    #[test]
    fn test_retryable_error_detection() {
        // Test retryable patterns
        let retryable_errors = [
            "connection refused by remote host",
            "Connection Reset by peer",
            "request TIMEOUT exceeded",
            "HTTP 429 Too Many Requests",
            "503 Service Unavailable",
            "502 Bad Gateway",
            "rate limit exceeded",
        ];

        for err_str in retryable_errors {
            // Create a mock transport error by using the error message
            // In practice, TransportError wraps various error types
            let is_match = RETRYABLE_ERROR_PATTERNS
                .iter()
                .any(|p| err_str.to_lowercase().contains(p));
            assert!(is_match, "Expected '{}' to be retryable", err_str);
        }

        // Test non-retryable patterns
        let non_retryable_errors = [
            "nonce too low",
            "insufficient funds",
            "execution reverted",
            "invalid signature",
        ];

        for err_str in non_retryable_errors {
            let is_match = RETRYABLE_ERROR_PATTERNS
                .iter()
                .any(|p| err_str.to_lowercase().contains(p));
            assert!(!is_match, "Expected '{}' to NOT be retryable", err_str);
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

        // Initially at index 0
        assert_eq!(pool.current_url().as_str(), "https://rpc1.example.com/");

        // Rotate to index 1
        pool.rotate();
        assert_eq!(pool.current_url().as_str(), "https://rpc2.example.com/");

        // Rotate to index 2
        pool.rotate();
        assert_eq!(pool.current_url().as_str(), "https://rpc3.example.com/");

        // Rotate wraps back to index 0
        pool.rotate();
        assert_eq!(pool.current_url().as_str(), "https://rpc1.example.com/");
    }
}
