//! AlloyChainClient implementation.
//!
//! This is the production implementation of the `ChainClient` trait using
//! alloy for Ethereum interactions.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use dipper_rpc::indexer::{indexer_client::sol::RecurringCollectionAgreement, rca_eip712_domain};
use thegraph_core::alloy::{
    hex,
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, B256, FixedBytes},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol_types::{SolCall, SolStruct, SolValue},
};
use url::Url;

use super::{
    abi::{IRecurringCollector, ISubgraphService},
    gas::{GasEstimator, calculate_max_fee, exceeds_max_gas_price, get_gas_prices},
    rpc_provider::RpcProviderPool,
};
use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::ChainClientConfig,
};

/// HTTP timeout for the indexing-payments subgraph idempotency query.
/// Kept tight because dipper polls this on every offer submission and a
/// slow response stalls the worker handler.
const SUBGRAPH_QUERY_TIMEOUT_SECS: u64 = 10;

/// OFFER_TYPE_NEW from `RecurringCollector.sol`. Used when submitting a new
/// agreement offer on-chain.
const OFFER_TYPE_NEW: u8 = 0;

/// Time to wait for a tx receipt to appear before declaring the tx dropped
/// from the mempool. On hardhat this is ~15 blocks at 1s each; on Arbitrum
/// at 0.25s block time this is 60 confirmations. Short enough that the
/// pgmq retry budget can recover within the 300s RCA deadline, long enough
/// to tolerate typical network glitches.
const RECEIPT_POLL_TIMEOUT: Duration = Duration::from_secs(15);

/// Interval between `eth_getTransactionReceipt` polls while waiting for a
/// tx to mine. Tight enough to respond quickly on sub-second block times,
/// loose enough to avoid hammering the RPC.
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(500);

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

/// Sentinel value indicating the nonce has not been fetched from chain yet.
const NONCE_UNINITIALIZED: u64 = u64::MAX;

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
    /// RecurringCollector contract address (for RCA offer submission)
    recurring_collector_address: Address,
    /// Chain ID
    chain_id: u64,
    /// Gas price multiplier
    gas_price_multiplier: f64,
    /// Maximum gas price in gwei
    max_gas_price_gwei: u64,
    /// Indexing-payments-subgraph query URL for offer idempotency checks.
    /// When None, the idempotency check is skipped and every call submits.
    indexing_payments_subgraph_url: Option<Url>,
    /// HTTP client used to query the indexing-payments subgraph.
    http_client: reqwest::Client,
    /// In-memory nonce counter. Concurrent callers atomically increment this
    /// to get unique nonces without querying the chain, avoiding
    /// "replacement transaction underpriced" errors when multiple offer()
    /// transactions are submitted in parallel.
    nonce: AtomicU64,
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

        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(SUBGRAPH_QUERY_TIMEOUT_SECS))
            .build()
            .map_err(|e| {
                ChainClientError::ConfigError(format!("Failed to build subgraph HTTP client: {e}"))
            })?;

        if config.indexing_payments_subgraph_url.is_none() {
            tracing::warn!(
                "indexing_payments_subgraph_url not configured; offer submission \
                 will skip crash-recovery idempotency and unconditionally send a \
                 new offer tx. The contract's overwrite semantics make this safe \
                 but wastes gas on re-submission after a crashed restart."
            );
        }

        tracing::info!(
            signer_address = %signer.address(),
            subgraph_service = %config.subgraph_service_address,
            recurring_collector = %config.recurring_collector_address,
            chain_id = config.chain_id,
            indexing_payments_subgraph = ?config.indexing_payments_subgraph_url,
            "AlloyChainClient initialized"
        );

        Ok(Self {
            inner: Arc::new(AlloyChainClientInner {
                rpc_pool,
                gas_estimator,
                signer,
                subgraph_service_address: config.subgraph_service_address,
                recurring_collector_address: config.recurring_collector_address,
                chain_id: config.chain_id,
                gas_price_multiplier: config.gas_price_multiplier,
                max_gas_price_gwei: config.max_gas_price_gwei,
                indexing_payments_subgraph_url: config.indexing_payments_subgraph_url.clone(),
                http_client,
                nonce: AtomicU64::new(NONCE_UNINITIALIZED),
            }),
        })
    }

    /// Encode the calldata for `cancelIndexingAgreementByPayer(bytes16)`.
    fn encode_cancel_call(&self, agreement_id: &[u8; 16]) -> Vec<u8> {
        let agreement_bytes = FixedBytes::<16>::from_slice(agreement_id);

        ISubgraphService::cancelIndexingAgreementByPayerCall {
            agreementId: agreement_bytes,
        }
        .abi_encode()
    }

    /// Build, gas-estimate, and send a call to any contract.
    ///
    /// Shared entry point for `cancelIndexingAgreementByPayer` and `offer`.
    /// `log_agreement_id` is used only for structured logging.
    async fn build_and_send_call(
        &self,
        to: Address,
        calldata: Vec<u8>,
        log_agreement_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        // 1. Build initial transaction request
        let tx = TransactionRequest::default()
            .from(self.inner.signer.address())
            .to(to)
            .input(calldata.into());

        // 2. Estimate gas with safety bounds
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

        // 3. Get gas prices
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

        // 4. Calculate max fee with multiplier
        let max_fee_per_gas =
            calculate_max_fee(base_fee, priority_fee, self.inner.gas_price_multiplier);

        // 5. Check gas price limit
        if exceeds_max_gas_price(max_fee_per_gas, self.inner.max_gas_price_gwei) {
            return Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
                "Gas price {} gwei exceeds maximum {} gwei",
                max_fee_per_gas / 1_000_000_000,
                self.inner.max_gas_price_gwei
            )));
        }

        // 6. Build final transaction
        let tx = tx
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(priority_fee)
            .with_chain_id(self.inner.chain_id);

        tracing::debug!(
            agreement_id = %format_args!("0x{}", log_agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            to = %to,
            gas_limit,
            base_fee_gwei = base_fee / 1_000_000_000,
            priority_fee_gwei = priority_fee / 1_000_000_000,
            max_fee_gwei = max_fee_per_gas / 1_000_000_000,
            "Transaction parameters"
        );

        // 7. Sign and send with nonce handling
        self.sign_and_send(tx, log_agreement_id).await
    }

    /// Query the indexing-payments-subgraph for an existing `Offer` entity.
    ///
    /// Returns `Ok(Some(offerHash))` if the subgraph has indexed a prior
    /// `OfferStored` event for this agreement id, `Ok(None)` if no offer is
    /// present yet, and `Ok(None)` with a warning if the subgraph URL is not
    /// configured. Network or query errors are returned as `RpcError`.
    ///
    /// The agreement id is serialized as a 0x-prefixed 32-char hex string
    /// to match how graph-node stores `Bytes` entity ids.
    async fn read_offer_hash_from_subgraph(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<Option<B256>, ChainClientError> {
        let subgraph_url = match &self.inner.indexing_payments_subgraph_url {
            Some(url) => url,
            None => return Ok(None),
        };

        let id_hex = format!("0x{}", hex::encode(agreement_id));

        let query = r#"query GetOffer($id: Bytes!) { offer(id: $id) { offerHash } }"#;
        let body = serde_json::json!({
            "query": query,
            "variables": { "id": id_hex },
        });

        let response = self
            .inner
            .http_client
            .post(subgraph_url.as_str())
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                ChainClientError::RpcError(anyhow::anyhow!("subgraph POST failed: {e}"))
            })?;

        if !response.status().is_success() {
            return Err(ChainClientError::RpcError(anyhow::anyhow!(
                "subgraph returned HTTP {}",
                response.status()
            )));
        }

        let json: serde_json::Value = response.json().await.map_err(|e| {
            ChainClientError::RpcError(anyhow::anyhow!("decode subgraph body: {e}"))
        })?;

        if let Some(errors) = json.get("errors") {
            return Err(ChainClientError::RpcError(anyhow::anyhow!(
                "subgraph returned errors: {errors}"
            )));
        }

        let offer_hash_hex = match json
            .get("data")
            .and_then(|d| d.get("offer"))
            .and_then(|o| o.get("offerHash"))
            .and_then(|h| h.as_str())
        {
            Some(s) => s,
            None => return Ok(None),
        };

        let stripped = offer_hash_hex.strip_prefix("0x").unwrap_or(offer_hash_hex);
        let bytes = hex::decode(stripped).map_err(|e| {
            ChainClientError::RpcError(anyhow::anyhow!("decode offerHash from subgraph: {e}"))
        })?;
        if bytes.len() != 32 {
            return Err(ChainClientError::RpcError(anyhow::anyhow!(
                "subgraph offerHash is not 32 bytes: len={}",
                bytes.len()
            )));
        }
        Ok(Some(B256::from_slice(&bytes)))
    }

    /// Get the next nonce, initializing from chain on first call.
    ///
    /// Concurrent callers each get a unique nonce via atomic
    /// fetch-and-increment, avoiding the "replacement transaction
    /// underpriced" race when multiple offer() calls fire in parallel.
    async fn next_nonce(&self) -> Result<u64, ChainClientError> {
        let current = self.inner.nonce.load(Ordering::SeqCst);
        if current == NONCE_UNINITIALIZED {
            let chain_nonce = self.fetch_chain_nonce().await?;
            // CAS: if another task already initialized, use its value
            match self.inner.nonce.compare_exchange(
                NONCE_UNINITIALIZED,
                chain_nonce + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(chain_nonce),
                // Another task already initialized; get next unique nonce
                Err(_) => return Ok(self.inner.nonce.fetch_add(1, Ordering::SeqCst)),
            }
        }
        Ok(self.inner.nonce.fetch_add(1, Ordering::SeqCst))
    }

    /// Re-sync the in-memory nonce from chain state.
    ///
    /// Stores `chain_nonce + 1` to reserve `chain_nonce` for the caller —
    /// otherwise a concurrent `next_nonce` would load the same value and
    /// collide. Mirrors the init path in `next_nonce`.
    async fn resync_nonce(&self) -> Result<u64, ChainClientError> {
        let chain_nonce = self.fetch_chain_nonce().await?;
        self.inner.nonce.store(chain_nonce + 1, Ordering::SeqCst);
        Ok(chain_nonce)
    }

    /// Fetch the pending transaction count from chain.
    ///
    /// Uses the "pending" block tag so the count includes transactions
    /// sitting in the mempool from our wallet. Querying "latest" would
    /// return a stale count when prior txs are awaiting confirmation,
    /// causing the next tx to reuse a nonce that's already in-flight.
    async fn fetch_chain_nonce(&self) -> Result<u64, ChainClientError> {
        self.inner
            .rpc_pool
            .execute("get_nonce", |provider| {
                let addr = self.inner.signer.address();
                async move { provider.get_transaction_count(addr).pending().await }
            })
            .await
    }

    /// Sign and send a transaction with nonce error handling.
    ///
    /// Uses the in-memory nonce counter for the first attempt. On nonce
    /// errors, re-syncs from chain and retries.
    async fn sign_and_send(
        &self,
        mut tx: TransactionRequest,
        agreement_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        const MAX_NONCE_RETRIES: u32 = 2;

        for attempt in 0..MAX_NONCE_RETRIES {
            let nonce = if attempt == 0 {
                self.next_nonce().await?
            } else {
                self.resync_nonce().await?
            };

            tx = tx.with_nonce(nonce);

            let result = self.send_transaction(&tx).await;

            match result {
                Ok(tx_hash) => {
                    tracing::info!(
                        agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                        tx_hash = %tx_hash,
                        nonce,
                        "Transaction sent successfully"
                    );
                    return Ok(tx_hash);
                }
                Err(e) if is_nonce_error(&e.to_string()) && attempt + 1 < MAX_NONCE_RETRIES => {
                    tracing::warn!(
                        agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                        attempt = attempt + 1,
                        error = %e,
                        "Nonce error, re-syncing from chain and retrying"
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

    /// Poll `eth_getTransactionReceipt` until the tx has mined or the timeout
    /// elapses. `Ok(Some(status))` reports the receipt's success flag;
    /// `Ok(None)` signals the tx never appeared in time (dropped from the
    /// mempool). Transient RPC errors are retried silently until timeout.
    async fn wait_for_receipt(
        &self,
        tx_hash: B256,
        timeout: Duration,
    ) -> Result<Option<bool>, ChainClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let receipt = self
                .inner
                .rpc_pool
                .execute("get_transaction_receipt", |provider| async move {
                    provider.get_transaction_receipt(tx_hash).await
                })
                .await;

            match receipt {
                Ok(Some(r)) => return Ok(Some(r.status())),
                Ok(None) => {} // not mined yet
                Err(e) => {
                    // Transient RPC error — log and keep polling. If the
                    // error is persistent, the outer handler will see the
                    // eventual timeout as `Ok(None)` and resubmit, which is
                    // the safe default.
                    tracing::debug!(
                        tx_hash = %tx_hash,
                        error = %e,
                        "RPC error polling tx receipt, will retry until timeout"
                    );
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
        }
    }
}

#[async_trait]
impl ChainClient for AlloyChainClient {
    async fn cancel_indexing_agreement_by_payer(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<B256, ChainClientError> {
        tracing::info!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            contract = %self.inner.subgraph_service_address,
            "Canceling indexing agreement on-chain"
        );

        let calldata = self.encode_cancel_call(agreement_id);
        self.build_and_send_call(self.inner.subgraph_service_address, calldata, agreement_id)
            .await
    }

    async fn post_offer(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        // 1. Derive the on-chain agreement ID deterministically from the RCA.
        let agreement_id = dipper_rpc::indexer::derive_agreement_id(rca);

        // 2. Compute the local EIP-712 hash of the RCA; this is what the
        //    contract compares against the stored offer hash when the indexer
        //    later calls `accept(rca, "")`.
        let domain = rca_eip712_domain(self.inner.chain_id, self.inner.recurring_collector_address);
        let local_hash = rca.eip712_signing_hash(&domain);

        // 3. Idempotency via the indexing-payments-subgraph. If the subgraph
        //    has indexed a prior OfferStored for this agreement id with a
        //    matching hash, skip re-submission. If it has indexed one with
        //    a different hash, abort the proposal cycle as a hash conflict.
        //    If the URL isn't configured, this returns None and we submit
        //    unconditionally (see AlloyChainClient::new for the warning).
        //
        //    Note: there is a short window between an offer tx confirming
        //    and the subgraph indexing it. A crashed-restart during that
        //    window will re-submit. The subgraph handler absorbs the
        //    duplicate OfferStored event via an existence check, so the
        //    resulting second write is a no-op at the entity level.
        if let Some(stored) = self.read_offer_hash_from_subgraph(&agreement_id).await? {
            if stored == local_hash {
                tracing::info!(
                    agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    offer_hash = %stored,
                    "Offer already indexed by subgraph with matching hash, skipping submission"
                );
                return Ok(None);
            }
            return Err(ChainClientError::OfferHashMismatch {
                agreement_id: format!(
                    "0x{}",
                    agreement_id
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                ),
                stored,
                expected: local_hash,
            });
        }

        // 4. Encode offer(OFFER_TYPE_NEW, abi.encode(rca), 0).
        let rca_bytes = rca.abi_encode();
        let calldata = IRecurringCollector::offerCall {
            offerType: OFFER_TYPE_NEW,
            data: rca_bytes.into(),
            options: 0,
        }
        .abi_encode();

        tracing::info!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            contract = %self.inner.recurring_collector_address,
            offer_hash = %local_hash,
            "Submitting RCA offer on-chain"
        );

        let tx_hash = self
            .build_and_send_call(
                self.inner.recurring_collector_address,
                calldata,
                &agreement_id,
            )
            .await?;

        // Confirm the tx actually mined. Submission success only means the
        // RPC accepted the tx into the mempool; colliding-nonce or gas
        // spikes can evict it before inclusion. On eviction, resync the
        // in-memory nonce counter and surface `TxDropped` so the caller
        // can resubmit through the worker-queue retry path.
        match self.wait_for_receipt(tx_hash, RECEIPT_POLL_TIMEOUT).await? {
            Some(true) => Ok(Some(tx_hash)),
            Some(false) => Err(ChainClientError::TxReverted { tx_hash }),
            None => {
                tracing::warn!(
                    agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    tx_hash = %tx_hash,
                    timeout_secs = RECEIPT_POLL_TIMEOUT.as_secs(),
                    "Offer tx did not mine within receipt-poll window; treating as dropped"
                );
                self.resync_nonce().await?;
                Err(ChainClientError::TxDropped { tx_hash })
            }
        }
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
        let agreement_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let agreement_bytes = FixedBytes::<16>::from_slice(&agreement_id);

        let call = ISubgraphService::cancelIndexingAgreementByPayerCall {
            agreementId: agreement_bytes,
        };

        let encoded = call.abi_encode();

        // 4-byte selector + bytes16 argument (right-padded to 32 bytes in ABI encoding)
        assert_eq!(encoded.len(), 4 + 32);
    }
}
