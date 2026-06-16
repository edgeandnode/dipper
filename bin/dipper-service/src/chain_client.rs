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
mod eip5267;
mod gas;
mod rpc_provider;

use std::sync::Arc;

use async_trait::async_trait;
pub use client::AlloyChainClient;
use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
pub use eip5267::{fetch_rca_eip712_domain, refresh_rca_eip712_domain};
use thegraph_core::alloy::primitives::{B256, Bytes};

/// Error type for chain client operations
#[derive(Debug, thiserror::Error)]
pub enum ChainClientError {
    /// Transaction failed to submit
    #[error("failed to submit transaction: {0}")]
    SubmitFailed(#[source] anyhow::Error),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// The agreement has no 32-byte `terms_version_hash`, so it cannot be
    /// canceled via the RecurringAgreementManager. Permanent and per-agreement
    /// — distinct from a globally-disabled chain client; never retry or abandon.
    #[error("agreement {agreement_id} has no 32-byte terms_version_hash for manager cancel")]
    MissingTermsVersionHash { agreement_id: String },

    /// RPC error
    #[error("RPC error: {0}")]
    RpcError(#[source] anyhow::Error),

    /// Tx was accepted by the RPC (returned a hash) but no receipt appeared
    /// within the poll window.
    ///
    /// In practice this means the tx was evicted from the mempool before
    /// being mined — typically because another tx from the same sender
    /// claimed the same nonce with a higher fee. Callers should re-sync
    /// their nonce and resubmit; the offer path's subgraph idempotency check
    /// short-circuits if the dropped tx eventually lands.
    #[error("tx {tx_hash} did not mine within the receipt-poll window")]
    TxDropped { tx_hash: B256 },

    /// Tx was mined but reverted on-chain (receipt status = 0).
    #[error("tx {tx_hash} reverted on-chain")]
    TxReverted { tx_hash: B256 },

    /// A contract call reverted with structured revert data during gas
    /// estimation (eth_estimateGas simulated the call and the EVM reverted
    /// before tx submission). The 4-byte selector and full revert payload
    /// are preserved so callers can decode the specific error variant and
    /// decide whether to treat it as fatal or as a known idempotent no-op.
    #[error("contract reverted with selector 0x{:02x}{:02x}{:02x}{:02x}", selector[0], selector[1], selector[2], selector[3])]
    ContractRevert { selector: [u8; 4], data: Bytes },
}

/// Trait for sending on-chain transactions related to indexing agreements
#[async_trait]
pub trait ChainClient {
    /// Offer an RCA via `RecurringAgreementManager.offerAgreement` (manager as
    /// payer); returns the tx hash once mined. No crash-recovery idempotency,
    /// so a re-run re-sends — pending validation of the contract's re-offer path.
    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError>;

    /// Cancel an RCA via `RecurringAgreementManager.cancelAgreement`. Returns
    /// the tx hash when a transaction was submitted, or `Ok(None)` when the
    /// agreement is already canceled on-chain.
    async fn cancel_via_manager(
        &self,
        collector: thegraph_core::alloy::primitives::Address,
        agreement_id: &[u8; 16],
        version_hash: B256,
        options: u16,
    ) -> Result<Option<B256>, ChainClientError>;
}

/// Blanket impl for Arc-wrapped trait objects.
///
/// This allows using `Arc<dyn ChainClient + Send + Sync>` as a Clone-able
/// chain client, enabling runtime selection between implementations.
#[async_trait]
impl<T: ChainClient + Send + Sync + ?Sized> ChainClient for Arc<T> {
    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        (**self).offer_via_manager(rca).await
    }

    async fn cancel_via_manager(
        &self,
        collector: thegraph_core::alloy::primitives::Address,
        agreement_id: &[u8; 16],
        version_hash: B256,
        options: u16,
    ) -> Result<Option<B256>, ChainClientError> {
        (**self)
            .cancel_via_manager(collector, agreement_id, version_hash, options)
            .await
    }
}
