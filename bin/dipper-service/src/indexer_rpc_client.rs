//! # Indexer client
//!
//! The indexer client is responsible for communicating directly with the indexers.
//!
//! # Timeouts
//!
//! The gRPC client has configurable timeouts to protect against unresponsive indexers.
//! The default request timeout (240s) is set to cover indexer-rs IPFS retry worst case:
//! - IPFS fetch timeout: 30s per attempt
//! - Retry delays: 10s, 20s, 40s (exponential backoff)
//! - 4 attempts worst case: 30s + 10s + 30s + 20s + 30s + 40s + 30s = 190s
//! - Buffer: 50s
//!
//! If the timeout is too short, legitimate proposals may fail during IPFS retries.

use std::{str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;
use dipper_rpc::indexer::{
    derive_agreement_id,
    indexer_client::{rpc, rpc::SubmitAgreementProposalResponse, sol},
};
use thegraph_core::alloy::{
    primitives::{B256, U256},
    sol_types::SolValue,
};
use url::Url;

use crate::{
    config::IndexerClientConfig, registry::IndexingAgreementTerms,
    signing::eip712::PrivateKeyEip712Signer,
};

/// The indexer client error type for DIPs endpoint
#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum DipsError {
    #[error("Error connecting to the indexer: {0}")]
    ConnectionError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Error sending the request to the indexer: {0}")]
    RequestError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Request signing failed: {0}")]
    SigningError(Box<dyn std::error::Error + Send + Sync>),
}

/// Indexer client's DIPs trait
#[async_trait]
pub trait IndexerClient {
    /// Send an indexing agreement proposal to the indexer.
    ///
    /// Returns the full response including proposal status and rejection reason
    /// on successful delivery, or an error if delivery failed (network issues,
    /// connection errors, etc.).
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
        terms: IndexingAgreementTerms,
        nonce_uuid: uuid::Uuid,
    ) -> Result<SubmitAgreementProposalResponse, DipsError>;

    /// Send an indexing agreement cancel request to the indexer.
    ///
    /// The agreement ID IS the on-chain bytes16, so no separate `on_chain_id` is needed.
    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError>;
}

#[derive(Clone)]
pub struct DipsIndexerClient {
    signer: Arc<PrivateKeyEip712Signer>,
    connect_timeout: Duration,
    request_timeout: Duration,
    max_retries: u32,
}

impl DipsIndexerClient {
    /// Create a new indexer client with custom configuration.
    pub fn with_config(signer: Arc<PrivateKeyEip712Signer>, config: IndexerClientConfig) -> Self {
        Self {
            signer,
            connect_timeout: config.connect_timeout,
            request_timeout: config.request_timeout,
            max_retries: config.max_retries,
        }
    }

    /// Get a client for the given indexer URL
    ///
    /// If the client is not in the pool, create a new instance.
    fn get_client(
        &self,
        indexer_url: &Url,
    ) -> Result<rpc::IndexerDipsServiceClient<tonic::transport::Channel>, DipsError> {
        let indexer_url = indexer_url.as_str();
        let channel = tonic::transport::Endpoint::from_str(indexer_url)
            .map_err(|err| DipsError::ConnectionError(err.into()))?
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .connect_lazy();
        let client = rpc::IndexerDipsServiceClient::new(channel);
        Ok(client)
    }
}

/// Check if a gRPC status code indicates a transient error worth retrying.
///
/// Intentionally conservative - only codes that are clearly transient.
/// `UNKNOWN` is excluded because it could mask permanent failures.
fn is_retryable_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable         // Service unavailable, connection issues
            | tonic::Code::ResourceExhausted // Rate limiting (backoff helps)
            | tonic::Code::Aborted           // Concurrency conflict
            | tonic::Code::DeadlineExceeded // Timeout
    )
}

/// Calculate exponential backoff delay: 1s, 2s, 4s, 8s, ... capped at 30s.
fn calculate_retry_delay(attempt: u32) -> Duration {
    let base_delay_ms = 1000u64;
    let delay_ms = base_delay_ms.saturating_mul(1u64 << attempt.min(5));
    Duration::from_millis(delay_ms.min(30_000))
}

/// Execute a gRPC call with retry on transient failures.
///
/// Takes a client factory and a request executor. Creates a fresh client for each
/// retry attempt (safe since `connect_lazy` doesn't make network calls until use).
///
/// Note: `get_client` errors (e.g., invalid URL) are NOT retried - they fail immediately.
/// This is intentional since URL parsing errors are permanent, not transient.
async fn with_retry<C, F, Fut, T>(
    max_retries: u32,
    indexer: &Url,
    agreement_id: IndexingAgreementId,
    operation: &str,
    get_client: impl Fn() -> Result<C, DipsError>,
    make_request: F,
) -> Result<T, DipsError>
where
    F: Fn(C) -> Fut,
    Fut: std::future::Future<Output = Result<T, tonic::Status>>,
{
    let mut last_error = None;
    for attempt in 0..=max_retries {
        let client = get_client()?;
        match make_request(client).await {
            Ok(result) => return Ok(result),
            Err(status) => {
                if attempt < max_retries && is_retryable_status(&status) {
                    let delay = calculate_retry_delay(attempt);
                    tracing::warn!(
                        indexer = %indexer,
                        agreement_id = %agreement_id,
                        operation = operation,
                        attempt = attempt + 1,
                        max_retries = max_retries,
                        status_code = ?status.code(),
                        delay_ms = delay.as_millis(),
                        "Transient gRPC error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    last_error = Some(status);
                } else {
                    return Err(DipsError::RequestError(status.into()));
                }
            }
        }
    }

    Err(DipsError::RequestError(
        last_error
            .expect("last_error must be set after retry loop")
            .into(),
    ))
}

#[async_trait]
impl IndexerClient for DipsIndexerClient {
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
        terms: IndexingAgreementTerms,
        nonce_uuid: uuid::Uuid,
    ) -> Result<SubmitAgreementProposalResponse, DipsError> {
        // Convert to the RCA solidity data structure
        let (sol_rca, _on_chain_id) = into_sol_rca(nonce_uuid, terms);

        // Sign the RCA with the RecurringCollector EIP-712 domain
        let signed = self
            .signer
            .sign_rca_msg(sol_rca)
            .map_err(|err| DipsError::SigningError(err.into()))?;

        // Serialize the signed RCA to bytes (ABI encoding)
        let sol_signed_rca_bytes: Vec<u8> = sol::SignedRecurringCollectionAgreement {
            agreement: signed.message,
            signature: signed.signature.as_bytes().into(),
        }
        .abi_encode();

        // Send the proposal request with retry on transient failures.
        // Note: signed_voucher is cloned for each retry (~1KB). This is acceptable:
        // - Retries are rare (only on transient failures)
        // - Max 3 retries = 3KB allocations worst case
        // - Negligible vs network I/O cost
        with_retry(
            self.max_retries,
            indexer,
            indexing_agreement_id,
            "submit_proposal",
            || self.get_client(indexer),
            |mut client| {
                let request = tonic::Request::new(rpc::SubmitAgreementProposalRequest {
                    version: 2,
                    signed_voucher: sol_signed_rca_bytes.clone(),
                });
                async move {
                    client
                        .submit_agreement_proposal(request)
                        .await
                        .map(|resp| resp.into_inner())
                }
            },
        )
        .await
    }

    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError> {
        // Convert to the solidity cancellation request data structure
        let sol_cancellation_request =
            into_sol_cancellation_request(indexing_agreement_id.as_bytes());

        // Sign the solidity cancellation request with the appropriate domain
        let signed = self
            .signer
            .sign_dips_cancellation_msg(sol_cancellation_request)
            .map_err(|err| DipsError::SigningError(err.into()))?;

        // Serialize the Solidity signed cancellation request to bytes (ABI encoding)
        let sol_signed_cancellation_request_bytes: Vec<u8> = sol::SignedCancellationRequest {
            request: signed.message,
            signature: signed.signature.as_bytes().into(),
        }
        .abi_encode();

        // Send the cancellation request with retry on transient failures.
        // Clone cost is negligible (see comment in send_indexing_agreement_proposal).
        with_retry(
            self.max_retries,
            indexer,
            indexing_agreement_id,
            "cancel_agreement",
            || self.get_client(indexer),
            |mut client| {
                let request = tonic::Request::new(rpc::CancelAgreementRequest {
                    version: 1,
                    signed_cancellation: sol_signed_cancellation_request_bytes.clone(),
                });
                async move { client.cancel_agreement(request).await.map(|_| ()) }
            },
        )
        .await
    }
}

/// Compute the on-chain agreement ID (bytes16) for an agreement's terms.
///
/// This replicates the contract's derivation:
/// `bytes16(keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce)))`
///
/// The `nonce_uuid` is the UUID v7 used to derive the RCA nonce (placed in the lower
/// 16 bytes of a U256). The returned `IndexingAgreementId` IS the on-chain identity.
pub fn compute_on_chain_id(
    nonce_uuid: uuid::Uuid,
    terms: &IndexingAgreementTerms,
) -> IndexingAgreementId {
    let (_, on_chain_id) = into_sol_rca(nonce_uuid, terms.clone());
    IndexingAgreementId::from_bytes(on_chain_id)
}

/// Convert agreement terms to the on-chain `RecurringCollectionAgreement` sol type.
///
/// Returns the RCA and the derived on-chain agreement ID (bytes16).
#[inline]
fn into_sol_rca(
    nonce_uuid: uuid::Uuid,
    terms: IndexingAgreementTerms,
) -> (sol::RecurringCollectionAgreement, [u8; 16]) {
    // Build the V1 pricing terms
    let sol_terms = sol::IndexingAgreementTermsV1 {
        tokensPerSecond: terms.metadata.tokens_per_second,
        tokensPerEntityPerSecond: terms.metadata.tokens_per_entity_per_second,
    }
    .abi_encode();

    // Build the acceptance metadata (ABI-encoded into the RCA metadata field)
    let metadata = sol::AcceptIndexingAgreementMetadata {
        subgraphDeploymentId: B256::from(terms.metadata.subgraph_deployment_id),
        version: 0, // IndexingAgreementVersion.V1 = 0 (first enum variant in Solidity)
        terms: sol_terms.into(),
    }
    .abi_encode();

    // Derive nonce from the UUID (16-byte UUID -> U256)
    let mut nonce_bytes = [0u8; 32];
    nonce_bytes[16..].copy_from_slice(nonce_uuid.as_bytes());
    let nonce = U256::from_be_bytes(nonce_bytes);

    let rca = sol::RecurringCollectionAgreement {
        deadline: terms.deadline,
        endsAt: terms.ends_at,
        payer: terms.payer,
        dataService: terms.data_service,
        serviceProvider: terms.service_provider,
        maxInitialTokens: terms.max_initial_tokens,
        maxOngoingTokensPerSecond: terms.max_ongoing_tokens_per_second,
        minSecondsPerCollection: terms.min_seconds_per_collection,
        maxSecondsPerCollection: terms.max_seconds_per_collection,
        conditions: terms.conditions,
        nonce,
        metadata: metadata.into(),
    };
    let on_chain_id = derive_agreement_id(&rca);
    (rca, on_chain_id)
}

#[inline]
fn into_sol_cancellation_request(on_chain_id: &[u8; 16]) -> sol::CancellationRequest {
    sol::CancellationRequest {
        agreement_id: on_chain_id.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use thegraph_core::alloy::{primitives::U256, sol_types::SolValue};

    use super::*;
    use crate::registry::IndexingAgreementTermsMetadata;

    #[test]
    fn test_into_sol_rca_conversion() {
        use thegraph_core::{DeploymentId, alloy::primitives::address};

        //* Arrange
        let nonce_uuid = uuid::Uuid::now_v7();
        let deployment_id =
            DeploymentId::from_str("QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9").unwrap();

        let payer = address!("0000000000000000000000000000000000000001");
        let service_provider = address!("0000000000000000000000000000000000000002");
        let data_service = address!("0000000000000000000000000000000000000003");
        let deadline = 1234567890u64;
        let ends_at = 9876543210u64;
        let max_initial_tokens = U256::from(1000u64);
        let max_ongoing_tokens_per_second = U256::from(100u64);
        let min_seconds_per_collection = 60u32;
        let max_seconds_per_collection = 3600u32;
        let tokens_per_second = U256::from(10u64);
        let tokens_per_entity_per_second = U256::from(2u64);

        let terms = IndexingAgreementTerms {
            payer,
            service_provider,
            data_service,
            deadline,
            ends_at,
            max_initial_tokens,
            max_ongoing_tokens_per_second,
            min_seconds_per_collection,
            max_seconds_per_collection,
            conditions: 0,
            metadata: IndexingAgreementTermsMetadata {
                tokens_per_second,
                tokens_per_entity_per_second,
                subgraph_deployment_id: deployment_id,
                protocol_network: 42161,
                chain_id: 1,
            },
        };

        //* Act
        let (rca, _on_chain_id) = into_sol_rca(nonce_uuid, terms);

        //* Assert
        // Verify top-level fields
        assert_eq!(rca.deadline, deadline, "deadline mismatch");
        assert_eq!(rca.endsAt, ends_at, "endsAt mismatch");
        assert_eq!(rca.payer, payer, "payer mismatch");
        assert_eq!(rca.dataService, data_service, "dataService mismatch");
        assert_eq!(
            rca.serviceProvider, service_provider,
            "serviceProvider mismatch"
        );
        assert_eq!(
            rca.maxInitialTokens, max_initial_tokens,
            "maxInitialTokens mismatch"
        );
        assert_eq!(
            rca.maxOngoingTokensPerSecond, max_ongoing_tokens_per_second,
            "maxOngoingTokensPerSecond mismatch"
        );
        assert_eq!(
            rca.minSecondsPerCollection, min_seconds_per_collection,
            "minSecondsPerCollection mismatch"
        );
        assert_eq!(
            rca.maxSecondsPerCollection, max_seconds_per_collection,
            "maxSecondsPerCollection mismatch"
        );

        //* Assert - Verify nested metadata ABI encoding
        let decoded_metadata = sol::AcceptIndexingAgreementMetadata::abi_decode(&rca.metadata)
            .expect("metadata should be valid ABI-encoded AcceptIndexingAgreementMetadata");

        assert_eq!(
            decoded_metadata.subgraphDeploymentId,
            B256::from(deployment_id),
            "subgraphDeploymentId in metadata mismatch"
        );
        assert_eq!(
            decoded_metadata.version, 0,
            "version should be 1 (IndexingAgreementVersion::V1)"
        );

        // Verify nested terms
        let decoded_terms = sol::IndexingAgreementTermsV1::abi_decode(&decoded_metadata.terms)
            .expect("terms should be valid ABI-encoded IndexingAgreementTermsV1");

        assert_eq!(
            decoded_terms.tokensPerSecond, tokens_per_second,
            "tokensPerSecond in nested terms mismatch"
        );
        assert_eq!(
            decoded_terms.tokensPerEntityPerSecond, tokens_per_entity_per_second,
            "tokensPerEntityPerSecond in nested terms mismatch"
        );
    }

    #[test]
    fn test_is_retryable_status_transient_errors() {
        // These should be retried
        assert!(is_retryable_status(&tonic::Status::unavailable(
            "service down"
        )));
        assert!(is_retryable_status(&tonic::Status::resource_exhausted(
            "rate limited"
        )));
        assert!(is_retryable_status(&tonic::Status::aborted("conflict")));
        assert!(is_retryable_status(&tonic::Status::deadline_exceeded(
            "timeout"
        )));
    }

    #[test]
    fn test_is_retryable_status_permanent_errors() {
        // These should NOT be retried
        assert!(!is_retryable_status(&tonic::Status::not_found("missing")));
        assert!(!is_retryable_status(&tonic::Status::invalid_argument(
            "bad request"
        )));
        assert!(!is_retryable_status(&tonic::Status::permission_denied(
            "unauthorized"
        )));
        assert!(!is_retryable_status(&tonic::Status::unimplemented(
            "not supported"
        )));
        // UNKNOWN is intentionally not retried - could mask permanent failures
        assert!(!is_retryable_status(&tonic::Status::unknown("mystery")));
    }

    #[test]
    fn test_calculate_retry_delay_exponential_backoff() {
        // Verify exponential backoff: 1s, 2s, 4s, 8s, 16s
        assert_eq!(calculate_retry_delay(0), Duration::from_secs(1));
        assert_eq!(calculate_retry_delay(1), Duration::from_secs(2));
        assert_eq!(calculate_retry_delay(2), Duration::from_secs(4));
        assert_eq!(calculate_retry_delay(3), Duration::from_secs(8));
        assert_eq!(calculate_retry_delay(4), Duration::from_secs(16));
        // attempt 5 would be 32s but gets capped to 30s
    }

    #[test]
    fn test_calculate_retry_delay_capped_at_30s() {
        // High attempt numbers should be capped at 30 seconds
        assert_eq!(calculate_retry_delay(6), Duration::from_secs(30)); // Would be 64s, capped
        assert_eq!(calculate_retry_delay(10), Duration::from_secs(30));
        assert_eq!(calculate_retry_delay(100), Duration::from_secs(30));
    }
}
