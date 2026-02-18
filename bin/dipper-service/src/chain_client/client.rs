//! AlloyChainClient implementation.
//!
//! This is the production implementation of the `ChainClient` trait using
//! alloy for Ethereum interactions.

use std::sync::Arc;

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;
use thegraph_core::alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, B256, FixedBytes},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol_types::SolCall,
};

use super::{
    abi::ISubgraphService,
    gas::{GasEstimator, calculate_max_fee, exceeds_max_gas_price, get_gas_prices},
    rpc_provider::RpcProviderPool,
};
use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::ChainClientConfig,
};

/// Error patterns that indicate a nonce-related issue.
///
/// These errors can be resolved by refreshing the nonce and retrying.
const NONCE_ERROR_PATTERNS: &[&str] = &[
    "nonce too low",
    "nonce is too low",
    "invalid nonce",
    "replacement transaction underpriced",
    "already known",
];

/// Check if an error message indicates a nonce problem.
fn is_nonce_error(error: &str) -> bool {
    let lower = error.to_lowercase();
    NONCE_ERROR_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Production implementation of `ChainClient` using alloy.
///
/// This struct is `Clone` via internal `Arc` wrapping, allowing it to be shared
/// across async task contexts.
#[derive(Clone)]
pub struct AlloyChainClient {
    /// Inner state wrapped in Arc for Clone support
    inner: Arc<AlloyChainClientInner>,
}

/// Inner state for AlloyChainClient
struct AlloyChainClientInner {
    /// RPC provider pool with failover
    rpc_pool: RpcProviderPool,
    /// Gas estimator with bounds
    gas_estimator: GasEstimator,
    /// Transaction signer
    signer: PrivateKeySigner,
    /// SubgraphService contract address
    subgraph_service_address: Address,
    /// Chain ID
    chain_id: u64,
    /// Gas price multiplier
    gas_price_multiplier: f64,
    /// Maximum gas price in gwei
    max_gas_price_gwei: u64,
}

impl AlloyChainClient {
    /// Create a new AlloyChainClient from configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No RPC providers are configured
    /// - The signer cannot be constructed
    pub fn new(
        config: &ChainClientConfig,
        secret_key: &[u8; 32],
    ) -> Result<Self, ChainClientError> {
        let signer = PrivateKeySigner::from_bytes(&FixedBytes::from(*secret_key))
            .map_err(|e| ChainClientError::ConfigError(format!("Invalid signing key: {e}")))?;

        let rpc_pool = RpcProviderPool::new(
            config.providers.clone(),
            config.request_timeout,
            config.max_retries,
        )?;

        let gas_estimator = GasEstimator::new(
            config.gas_buffer_multiplier,
            config.gas_floor,
            config.gas_max_addition,
        );

        tracing::info!(
            signer_address = %signer.address(),
            subgraph_service = %config.subgraph_service_address,
            chain_id = config.chain_id,
            "AlloyChainClient initialized"
        );

        Ok(Self {
            inner: Arc::new(AlloyChainClientInner {
                rpc_pool,
                gas_estimator,
                signer,
                subgraph_service_address: config.subgraph_service_address,
                chain_id: config.chain_id,
                gas_price_multiplier: config.gas_price_multiplier,
                max_gas_price_gwei: config.max_gas_price_gwei,
            }),
        })
    }

    /// Encode the calldata for `cancelIndexingAgreementByPayer(bytes16)`.
    fn encode_cancel_call(&self, agreement_id: IndexingAgreementId) -> Vec<u8> {
        let agreement_bytes = FixedBytes::<16>::from_slice(agreement_id.as_bytes());

        ISubgraphService::cancelIndexingAgreementByPayerCall {
            agreementId: agreement_bytes,
        }
        .abi_encode()
    }

    /// Sign and send a transaction with nonce error handling.
    ///
    /// On nonce errors, refreshes the nonce and retries once.
    async fn sign_and_send(
        &self,
        mut tx: TransactionRequest,
        agreement_id: IndexingAgreementId,
    ) -> Result<B256, ChainClientError> {
        const MAX_NONCE_RETRIES: u32 = 2;

        for attempt in 0..MAX_NONCE_RETRIES {
            // Get fresh nonce
            let nonce = self
                .inner
                .rpc_pool
                .execute("get_nonce", |provider| {
                    let addr = self.inner.signer.address();
                    async move { provider.get_transaction_count(addr).await }
                })
                .await?;

            tx = tx.with_nonce(nonce);

            // Send the transaction
            let result = self.send_transaction(&tx).await;

            match result {
                Ok(tx_hash) => {
                    tracing::info!(
                        agreement_id = %agreement_id,
                        tx_hash = %tx_hash,
                        nonce,
                        "Transaction sent successfully"
                    );
                    return Ok(tx_hash);
                }
                Err(e) if is_nonce_error(&e.to_string()) && attempt + 1 < MAX_NONCE_RETRIES => {
                    tracing::warn!(
                        agreement_id = %agreement_id,
                        attempt = attempt + 1,
                        error = %e,
                        "Nonce error, refreshing and retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
            "Failed to send transaction after {} nonce retries",
            MAX_NONCE_RETRIES
        )))
    }

    /// Send a transaction using a wallet-enabled provider.
    ///
    /// Creates a new provider with the wallet attached for signing and sending.
    async fn send_transaction(&self, tx: &TransactionRequest) -> Result<B256, ChainClientError> {
        let wallet = EthereumWallet::from(self.inner.signer.clone());
        let url = self.inner.rpc_pool.current_url().clone();

        // Build HTTP client with timeout
        let client = reqwest::Client::builder()
            .timeout(self.inner.rpc_pool.request_timeout())
            .build()
            .map_err(|e| {
                ChainClientError::ConfigError(format!("Failed to build HTTP client: {e}"))
            })?;

        // Build wallet-enabled provider
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect_reqwest(client, url);

        let pending = provider
            .send_transaction(tx.clone())
            .await
            .map_err(|e| ChainClientError::SubmitFailed(anyhow::anyhow!("Send failed: {e}")))?;

        Ok(*pending.tx_hash())
    }
}

#[async_trait]
impl ChainClient for AlloyChainClient {
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> Result<B256, ChainClientError> {
        tracing::info!(
            agreement_id = %agreement_id,
            contract = %self.inner.subgraph_service_address,
            "Canceling indexing agreement on-chain"
        );

        // 1. Encode the contract call
        let calldata = self.encode_cancel_call(agreement_id);

        // 2. Build initial transaction request
        let tx = TransactionRequest::default()
            .from(self.inner.signer.address())
            .to(self.inner.subgraph_service_address)
            .input(calldata.into());

        // 3. Estimate gas with safety bounds
        let gas_limit = self
            .inner
            .rpc_pool
            .execute("estimate_gas", |provider| {
                let tx = tx.clone();
                let estimator = self.inner.gas_estimator.clone();
                async move {
                    estimator.estimate(&provider, &tx).await.map_err(|e| {
                        thegraph_core::alloy::transports::TransportError::local_usage_str(
                            &e.to_string(),
                        )
                    })
                }
            })
            .await?;

        // 4. Get gas prices
        let (base_fee, priority_fee) = self
            .inner
            .rpc_pool
            .execute("get_gas_prices", |provider| async move {
                get_gas_prices(&provider).await.map_err(|e| {
                    thegraph_core::alloy::transports::TransportError::local_usage_str(
                        &e.to_string(),
                    )
                })
            })
            .await?;

        // 5. Calculate max fee with multiplier
        let max_fee_per_gas =
            calculate_max_fee(base_fee, priority_fee, self.inner.gas_price_multiplier);

        // 6. Check gas price limit
        if exceeds_max_gas_price(max_fee_per_gas, self.inner.max_gas_price_gwei) {
            return Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
                "Gas price {} gwei exceeds maximum {} gwei",
                max_fee_per_gas / 1_000_000_000,
                self.inner.max_gas_price_gwei
            )));
        }

        // 7. Build final transaction
        let tx = tx
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(priority_fee)
            .with_chain_id(self.inner.chain_id);

        tracing::debug!(
            agreement_id = %agreement_id,
            gas_limit,
            base_fee_gwei = base_fee / 1_000_000_000,
            priority_fee_gwei = priority_fee / 1_000_000_000,
            max_fee_gwei = max_fee_per_gas / 1_000_000_000,
            "Transaction parameters"
        );

        // 8. Sign and send with nonce handling
        self.sign_and_send(tx, agreement_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_nonce_error() {
        // Nonce errors
        assert!(is_nonce_error("nonce too low"));
        assert!(is_nonce_error("Nonce Too Low for account"));
        assert!(is_nonce_error("invalid nonce: expected 5, got 3"));
        assert!(is_nonce_error("replacement transaction underpriced"));
        assert!(is_nonce_error("transaction already known"));

        // Non-nonce errors
        assert!(!is_nonce_error("insufficient funds"));
        assert!(!is_nonce_error("execution reverted"));
        assert!(!is_nonce_error("gas limit exceeded"));
        assert!(!is_nonce_error("connection timeout"));
    }

    #[test]
    fn test_encode_cancel_call() {
        let agreement_id = IndexingAgreementId::new();
        let agreement_bytes = FixedBytes::<16>::from_slice(agreement_id.as_bytes());

        let call = ISubgraphService::cancelIndexingAgreementByPayerCall {
            agreementId: agreement_bytes,
        };

        let encoded = call.abi_encode();

        // 4-byte selector + bytes16 argument (right-padded to 32 bytes in ABI encoding)
        assert_eq!(encoded.len(), 4 + 32);
    }
}
