//! Chain client for sending on-chain transactions
//!
//! This module provides the interface for interacting with the blockchain
//! to manage indexing agreements on-chain.

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;
use thegraph_core::alloy::primitives::B256;

/// Error type for chain client operations
#[derive(Debug, thiserror::Error)]
pub enum ChainClientError {
    /// Transaction failed to submit
    #[error("failed to submit transaction: {0}")]
    SubmitFailed(#[source] anyhow::Error),

    /// Transaction was submitted but failed on-chain
    #[error("transaction reverted: {0}")]
    TransactionReverted(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// RPC error
    #[error("RPC error: {0}")]
    RpcError(#[source] anyhow::Error),
}

/// Trait for sending on-chain transactions related to indexing agreements
#[async_trait]
pub trait ChainClient {
    /// Cancel an indexing agreement as the payer.
    ///
    /// Calls `cancelIndexingAgreementByPayer(agreementId)` on the SubgraphService contract.
    /// This caps the collectible fees at the cancellation timestamp.
    ///
    /// Returns the transaction hash on success.
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> Result<B256, ChainClientError>;
}

/// Stub implementation that returns unimplemented error.
///
/// This is used until the real implementation is added.
#[derive(Clone)]
pub struct StubChainClient;

#[async_trait]
impl ChainClient for StubChainClient {
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> Result<B256, ChainClientError> {
        tracing::error!(
            agreement_id = %agreement_id,
            "ChainClient not implemented - cannot cancel agreement on-chain"
        );
        Err(ChainClientError::ConfigError(
            "ChainClient not implemented".to_string(),
        ))
    }
}
