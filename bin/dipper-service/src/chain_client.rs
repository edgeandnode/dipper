//! Chain client for sending on-chain transactions
//!
//! This module provides the interface for interacting with the blockchain
//! to manage indexing agreements on-chain.
//!
//! ## Implementation
//!
//! The production implementation uses alloy for Ethereum interactions and
//! includes:
//! - RPC provider pool with automatic failover
//! - Exponential backoff retry logic
//! - Gas estimation with safety bounds
//! - Nonce management and error handling
//!
//! See [`AlloyChainClient`] for the production implementation.

mod abi;
mod client;
mod gas;
mod rpc_provider;

use std::sync::Arc;

use async_trait::async_trait;
pub use client::AlloyChainClient;
use thegraph_core::alloy::primitives::B256;

/// Error type for chain client operations
#[derive(Debug, thiserror::Error)]
pub enum ChainClientError {
    /// Transaction failed to submit
    #[error("failed to submit transaction: {0}")]
    SubmitFailed(#[source] anyhow::Error),

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
    /// The `on_chain_id` is the keccak-derived bytes16 stored on-chain, not the internal UUID.
    /// This caps the collectible fees at the cancellation timestamp.
    ///
    /// Returns the transaction hash on success.
    async fn cancel_indexing_agreement_by_payer(
        &self,
        on_chain_id: &[u8; 16],
    ) -> Result<B256, ChainClientError>;
}

/// Blanket impl for Arc-wrapped trait objects.
///
/// This allows using `Arc<dyn ChainClient + Send + Sync>` as a Clone-able
/// chain client, enabling runtime selection between implementations.
#[async_trait]
impl<T: ChainClient + Send + Sync + ?Sized> ChainClient for Arc<T> {
    async fn cancel_indexing_agreement_by_payer(
        &self,
        on_chain_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        (**self)
            .cancel_indexing_agreement_by_payer(on_chain_id)
            .await
    }
}

/// Stub implementation that returns an error.
///
/// Used when the chain client is disabled via configuration.
#[derive(Clone)]
pub struct StubChainClient;

#[async_trait]
impl ChainClient for StubChainClient {
    async fn cancel_indexing_agreement_by_payer(
        &self,
        on_chain_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        tracing::error!(
            on_chain_id = ?on_chain_id,
            "ChainClient not implemented - cannot cancel agreement on-chain"
        );
        Err(ChainClientError::ConfigError(
            "ChainClient not implemented".to_string(),
        ))
    }
}
