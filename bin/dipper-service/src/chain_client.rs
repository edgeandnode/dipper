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

    /// RPC error
    #[error("RPC error: {0}")]
    RpcError(#[source] anyhow::Error),

    /// Subgraph reports a stored offer whose hash does not match the locally-computed hash.
    ///
    /// Indicates the agreement terms drifted between a prior submission and
    /// the current invocation (e.g. a stale nonce or deadline). Since the
    /// RecurringCollector stores offers inside a namespaced storage struct
    /// with no public getter, dipper treats the indexing-payments-subgraph's
    /// Offer entity as the source of truth for this check. When a mismatch
    /// is detected, dipper marks the agreement as delivery-failed and bails;
    /// the reassignment service finds a replacement.
    #[error(
        "offer hash mismatch for agreement {agreement_id}: stored={stored}, expected={expected}"
    )]
    OfferHashMismatch {
        agreement_id: String,
        stored: B256,
        expected: B256,
    },

    /// Tx was accepted by the RPC (returned a hash) but no receipt appeared
    /// within the poll window.
    ///
    /// In practice this means the tx was evicted from the mempool before
    /// being mined — typically because another tx from the same sender
    /// claimed the same nonce with a higher fee. Callers should re-sync
    /// their nonce and resubmit; `post_offer`'s subgraph idempotency check
    /// will short-circuit if the original tx eventually did land.
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
    /// Cancel an indexing agreement as the payer.
    ///
    /// Calls `cancelIndexingAgreementByPayer(agreementId)` on the SubgraphService contract.
    /// The `agreement_id` is the keccak-derived bytes16 stored on-chain. The call
    /// caps the collectible fees at the cancellation timestamp.
    ///
    /// Returns:
    /// - `Ok(Some(tx_hash))` when a transaction was submitted.
    /// - `Ok(None)` when the agreement is already canceled on-chain (the
    ///   contract reverts gas estimation with `IndexingAgreementNotActive`).
    ///   Callers should treat this as an idempotent success and proceed with
    ///   any local-state cleanup that would have followed a real submission.
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<Option<B256>, ChainClientError>;

    /// Submit an RCA offer on-chain via `RecurringCollector.offer(OFFER_TYPE_NEW, ...)`.
    ///
    /// Crash-recovery idempotency is handled via a query to the
    /// indexing-payments subgraph (not an RPC call): if a matching
    /// `Offer` entity already exists the method returns `Ok(None)` without
    /// sending a transaction. If an offer exists with a different hash,
    /// returns `OfferHashMismatch`. Otherwise submits an `offer()` transaction
    /// and returns `Ok(Some(tx_hash))`. When no subgraph URL is configured,
    /// the idempotency check is skipped and every call unconditionally submits.
    ///
    /// `msg.sender` of the transaction must equal `rca.payer` or the contract
    /// reverts with `RecurringCollectorUnauthorizedCaller`.
    async fn post_offer(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError>;

    /// Offer an RCA through the RecurringAgreementManager (`AgreementManager`
    /// mode). Calls `offerAgreement(collector, OFFER_TYPE_NEW, abi.encode(rca))`
    /// with the manager as payer; same return/error contract as [`post_offer`].
    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError>;

    /// Cancel an RCA through the RecurringAgreementManager (`AgreementManager`
    /// mode) via `cancelAgreement(collector, agreementId, versionHash, options)`;
    /// same contract as [`cancel_indexing_agreement_by_payer`].
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
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<Option<B256>, ChainClientError> {
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
