//! # Indexer client
//!
//! The indexer client is responsible for communicating directly with the indexers.

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;
use dipper_rpc::indexer::indexer_client::{rpc, sol};
use thegraph_core::alloy::{
    primitives::{B256, U256},
    sol_types::SolValue,
};
use url::Url;

use crate::{registry::IndexingAgreementVoucher, signing::eip712::PrivateKeyEip712Signer};

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
    /// Send an indexing agreement proposal request to the indexer (fire-and-forget).
    ///
    /// Returns `Ok(())` if the proposal was delivered, or an error if delivery failed.
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
        voucher: IndexingAgreementVoucher,
    ) -> Result<(), DipsError>;

    /// Send an indexing agreement cancel request to the indexer
    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError>;
}

#[derive(Clone)]
pub struct DipsIndexerClient {
    signer: Arc<PrivateKeyEip712Signer>,
}

impl DipsIndexerClient {
    /// Create a new indexer client
    pub fn new(signer: Arc<PrivateKeyEip712Signer>) -> Self {
        Self { signer }
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
            .connect_lazy();
        let client = rpc::IndexerDipsServiceClient::new(channel);
        Ok(client)
    }
}

#[async_trait]
impl IndexerClient for DipsIndexerClient {
    async fn send_indexing_agreement_proposal(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
        voucher: IndexingAgreementVoucher,
    ) -> Result<(), DipsError> {
        // Convert to the RCA solidity data structure
        let sol_rca = into_sol_rca(indexing_agreement_id, voucher);

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

        // Build the SubmitAgreementProposalRequest RPC request message
        let request = tonic::Request::new(rpc::SubmitAgreementProposalRequest {
            version: 2,
            signed_voucher: sol_signed_rca_bytes,
        });

        // Send the proposal request to the indexer (fire-and-forget)
        let mut client = self.get_client(indexer)?;
        client
            .submit_agreement_proposal(request)
            .await
            .map_err(|err| DipsError::RequestError(err.into()))?;

        Ok(())
    }

    async fn send_indexing_agreement_cancellation_notification(
        &self,
        indexer: &Url,
        indexing_agreement_id: IndexingAgreementId,
    ) -> Result<(), DipsError> {
        // Convert to the solidity cancellation request data structure
        let sol_cancellation_request = into_sol_cancellation_request(indexing_agreement_id);

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

        // Build the CancelAgreementRequest RPC request message
        let request = tonic::Request::new(rpc::CancelAgreementRequest {
            version: 1,
            signed_cancellation: sol_signed_cancellation_request_bytes,
        });

        // Send the cancellation request to the indexer
        let mut client = self.get_client(indexer)?;
        client
            .cancel_agreement(request)
            .await
            .map_err(|err| DipsError::RequestError(err.into()))?;

        Ok(())
    }
}

/// Convert an internal voucher to the on-chain `RecurringCollectionAgreement` sol type.
#[inline]
fn into_sol_rca(
    agreement_id: IndexingAgreementId,
    voucher: IndexingAgreementVoucher,
) -> sol::RecurringCollectionAgreement {
    // Build the V1 pricing terms
    let terms = sol::IndexingAgreementTermsV1 {
        tokensPerSecond: voucher.metadata.tokens_per_second,
        tokensPerEntityPerSecond: voucher.metadata.tokens_per_entity_per_second,
    }
    .abi_encode();

    // Build the acceptance metadata (ABI-encoded into the RCA metadata field)
    let metadata = sol::AcceptIndexingAgreementMetadata {
        subgraphDeploymentId: B256::from(voucher.metadata.subgraph_deployment_id),
        version: 0, // IndexingAgreementVersion::V1
        terms: terms.into(),
    }
    .abi_encode();

    sol::RecurringCollectionAgreement {
        agreementId: agreement_id.as_bytes().into(),
        deadline: U256::from(voucher.deadline),
        endsAt: U256::from(voucher.ends_at),
        payer: voucher.payer,
        dataService: voucher.data_service,
        serviceProvider: voucher.service_provider,
        maxInitialTokens: voucher.max_initial_tokens,
        maxOngoingTokensPerSecond: voucher.max_ongoing_tokens_per_second,
        minSecondsPerCollection: voucher.min_seconds_per_collection,
        maxSecondsPerCollection: voucher.max_seconds_per_collection,
        metadata: metadata.into(),
    }
}

#[inline]
fn into_sol_cancellation_request(agreement_id: IndexingAgreementId) -> sol::CancellationRequest {
    sol::CancellationRequest {
        agreement_id: agreement_id.as_bytes().into(),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use thegraph_core::alloy::{primitives::U256, sol_types::SolValue};

    use super::*;
    use crate::registry::IndexingAgreementVoucherMetadata;

    #[test]
    fn test_into_sol_rca_conversion() {
        use thegraph_core::{DeploymentId, alloy::primitives::address};

        //* Arrange
        let agreement_id = IndexingAgreementId::new();
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

        let voucher = IndexingAgreementVoucher {
            payer,
            service_provider,
            data_service,
            deadline,
            ends_at,
            max_initial_tokens,
            max_ongoing_tokens_per_second,
            min_seconds_per_collection,
            max_seconds_per_collection,
            metadata: IndexingAgreementVoucherMetadata {
                tokens_per_second,
                tokens_per_entity_per_second,
                subgraph_deployment_id: deployment_id,
                protocol_network: 42161,
                chain_id: 1,
            },
        };

        //* Act
        let rca = into_sol_rca(agreement_id, voucher);

        //* Assert
        // Verify top-level fields
        use thegraph_core::alloy::primitives::FixedBytes;
        assert_eq!(
            rca.agreementId,
            FixedBytes::<16>::from(*agreement_id.as_bytes()),
            "agreementId mismatch"
        );
        assert_eq!(
            rca.deadline,
            U256::from(deadline),
            "deadline should be converted to U256"
        );
        assert_eq!(
            rca.endsAt,
            U256::from(ends_at),
            "endsAt should be converted to U256"
        );
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
            "version should be 0 (IndexingAgreementVersion::V1)"
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
}
