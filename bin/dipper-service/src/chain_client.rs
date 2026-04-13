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
use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
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

    /// On-chain offer exists but its hash does not match the locally-computed hash.
    ///
    /// Indicates the agreement terms drifted between stored offer and current
    /// invocation (e.g. a stale nonce or deadline). The offer must be cleared
    /// and re-submitted; dipper aborts the proposal cycle.
    #[error(
        "offer hash mismatch for agreement {agreement_id}: stored={stored}, expected={expected}"
    )]
    OfferHashMismatch {
        agreement_id: String,
        stored: B256,
        expected: B256,
    },
}

/// Trait for sending on-chain transactions related to indexing agreements
#[async_trait]
pub trait ChainClient {
    /// Cancel an indexing agreement as the payer.
    ///
    /// Calls `cancelIndexingAgreementByPayer(agreementId)` on the SubgraphService contract.
    /// The `agreement_id` is the keccak-derived bytes16 stored on-chain.
    /// This caps the collectible fees at the cancellation timestamp.
    ///
    /// Returns the transaction hash on success.
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<B256, ChainClientError>;

    /// Submit an RCA offer on-chain via `RecurringCollector.offer(OFFER_TYPE_NEW, ...)`.
    ///
    /// This is idempotent against on-chain state: if the offer already exists
    /// and its hash matches the locally-computed `hashRCA(rca)`, the method
    /// returns `Ok(None)` without sending a transaction. If the offer exists
    /// but with a different hash, returns `OfferHashMismatch`. Otherwise submits
    /// an `offer()` transaction and returns `Ok(Some(tx_hash))` after the
    /// receipt confirms.
    ///
    /// `msg.sender` of the transaction must equal `rca.payer` or the contract
    /// reverts with `RecurringCollectorUnauthorizedCaller`.
    async fn post_offer(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError>;
}

/// Blanket impl for Arc-wrapped trait objects.
///
/// This allows using `Arc<dyn ChainClient + Send + Sync>` as a Clone-able
/// chain client, enabling runtime selection between implementations.
#[async_trait]
impl<T: ChainClient + Send + Sync + ?Sized> ChainClient for Arc<T> {
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        (**self)
            .cancel_indexing_agreement_by_payer(agreement_id)
            .await
    }

    async fn post_offer(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        (**self).post_offer(rca).await
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
        agreement_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        tracing::error!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            "ChainClient not implemented - cannot cancel agreement on-chain"
        );
        Err(ChainClientError::ConfigError(
            "ChainClient not implemented".to_string(),
        ))
    }

    async fn post_offer(
        &self,
        _rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        tracing::error!("ChainClient not implemented - cannot post offer on-chain");
        Err(ChainClientError::ConfigError(
            "ChainClient not implemented".to_string(),
        ))
    }
}
