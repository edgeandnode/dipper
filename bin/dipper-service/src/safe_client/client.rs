use async_trait::async_trait;
use dipper_core::ids::IndexingReceiptId;
use thegraph_core::alloy::primitives::{Address, TxHash};

/// Result type for chain client operations
pub type Result<T> = std::result::Result<T, ChainClientError>;

/// Chain client trait for submitting payments to Safe contracts
#[async_trait]
pub trait ChainClient {
    /// Submit a payment transaction on-chain
    async fn submit_payment(&self, request: PaymentRequest) -> Result<TransactionResult>;
}

/// Safe-based implementation of the chain client
pub struct SafeChainClient {}

#[async_trait]
impl ChainClient for SafeChainClient {
    async fn submit_payment(&self, _request: PaymentRequest) -> Result<TransactionResult> {
        todo!()
    }
}

/// Payment request containing recipient and amount information
#[derive(Debug, Clone)]
pub struct PaymentRequest {
    /// Recipient wallet address
    pub recipient: Address,
    /// Payment amount in smallest unit
    pub amount: u128,
    /// Unique identifier for the payment receipt
    pub receipt_id: IndexingReceiptId,
}

/// Result of a submitted transaction
#[derive(Debug, Clone)]
pub struct TransactionResult {
    /// On-chain transaction hash
    pub transaction_hash: TxHash,
}

/// Errors that can occur during chain client operations
#[derive(Debug, thiserror::Error)]
pub enum ChainClientError {
    /// Network connectivity or RPC errors
    #[error("Network error: {0}")]
    NetworkError(anyhow::Error),
    /// Transaction execution failures
    #[error("Transaction failed: {0}")]
    TransactionFailed(anyhow::Error),
    /// Client configuration errors
    #[error("Configuration error: {0}")]
    ConfigurationError(anyhow::Error),
}
