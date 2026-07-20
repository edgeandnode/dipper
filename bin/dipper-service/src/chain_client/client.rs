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
use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
use thegraph_core::alloy::{
    eips::BlockNumberOrTag,
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, B256, FixedBytes},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol_types::{SolCall, SolValue},
};
use tokio::sync::Mutex;

use super::{
    abi::{IRecurringAgreementManager, IRecurringCollector},
    gas::{GasEstimator, calculate_max_fee, exceeds_max_gas_price, get_gas_prices},
    rpc_provider::RpcProviderPool,
};
use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::ChainClientConfig,
};

/// OFFER_TYPE_NEW from `RecurringCollector.sol`. Used when submitting a new
/// agreement offer on-chain. The contract defines OFFER_TYPE_NONE=0,
/// OFFER_TYPE_NEW=1, OFFER_TYPE_UPDATE=2; passing 0 reverts with
/// RecurringCollectorInvalidOfferType(0).
const OFFER_TYPE_NEW: u8 = 1;

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

/// VERSION_CURRENT index from `IAgreementCollector.sol`: the active (or
/// pre-acceptance) terms. `getAgreementDetails(id, 0)` reports their state.
const VERSION_CURRENT: u64 = 0;

/// `AgreementDetails.state` flags from `IAgreementCollector.sol` (ACCEPTED=2,
/// NOTICE_GIVEN=4). `getAgreementDetails` keeps ACCEPTED set on a canceled
/// agreement and ORs in NOTICE_GIVEN, so a cancel must clear it, not just lack it.
const STATE_ACCEPTED: u16 = 2;
const STATE_NOTICE_GIVEN: u16 = 4;

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

/// Classify the outcome of a nonce-gap fill submission. Pure so the
/// swallow rule (`is_nonce_error` → `Ok(())`) is unit-testable without
/// an RPC mock.
fn classify_fill_nonce_gap_outcome(
    nonce: u64,
    submission: Result<B256, ChainClientError>,
) -> Result<(), ChainClientError> {
    match submission {
        Ok(tx_hash) => {
            tracing::info!(
                event = "nonce_gap_fill_submitted",
                nonce,
                fill_tx_hash = %tx_hash,
                "Submitted noop self-transfer to fill mempool nonce gap"
            );
            Ok(())
        }
        Err(e) if is_nonce_error(&e.to_string()) => {
            tracing::info!(
                event = "nonce_gap_fill_nonce_rejected",
                nonce,
                error = %e,
                "Nonce-gap fill rejected; original still in flight or gap already filled"
            );
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                event = "nonce_gap_fill_failed",
                nonce,
                error = %e,
                "Nonce-gap fill submission failed; wallet may stay wedged until the original tx clears or another fill succeeds"
            );
            Err(e)
        }
    }
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

/// Tx submission outcome carrying both the hash and the reserved nonce, so
/// callers that need to recover from an evicted tx (`fill_nonce_gap`) know
/// which slot to fill without re-querying the RPC.
#[derive(Debug, Clone, Copy)]
struct SubmittedTx {
    hash: B256,
    nonce: u64,
}

/// Inner state for AlloyChainClient
struct AlloyChainClientInner {
    /// RPC provider pool with failover
    rpc_pool: RpcProviderPool,
    /// Gas estimator with bounds
    gas_estimator: GasEstimator,
    /// Transaction signer
    signer: PrivateKeySigner,
    /// RecurringCollector contract address (for RCA offer submission)
    recurring_collector_address: Address,
    /// RecurringAgreementManager address: the on-chain payer the manager-routed
    /// offer/cancel paths drive.
    recurring_agreement_manager_address: Address,
    /// Chain ID
    chain_id: u64,
    /// Gas price multiplier
    gas_price_multiplier: f64,
    /// Maximum gas price in gwei
    max_gas_price_gwei: u64,
    /// In-memory nonce counter. Concurrent callers atomically increment this
    /// to get unique nonces without querying the chain, avoiding
    /// "replacement transaction underpriced" errors when multiple offer()
    /// transactions are submitted in parallel.
    nonce: AtomicU64,
    /// Serializes nonce reservation through mempool submission so the RPC
    /// sees txs in nonce order and the wallet's queue can never strand a
    /// higher-nonce tx behind an unfilled gap. Released before receipt
    /// polling so multiple confirmations pipeline concurrently.
    submit_lock: Mutex<()>,
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
        chain_id: u64,
        recurring_collector: Address,
        recurring_agreement_manager: Address,
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
            recurring_collector = %recurring_collector,
            recurring_agreement_manager = %recurring_agreement_manager,
            chain_id,
            "AlloyChainClient initialized"
        );

        Ok(Self {
            inner: Arc::new(AlloyChainClientInner {
                rpc_pool,
                gas_estimator,
                signer,
                recurring_collector_address: recurring_collector,
                recurring_agreement_manager_address: recurring_agreement_manager,
                chain_id,
                gas_price_multiplier: config.gas_price_multiplier,
                max_gas_price_gwei: config.max_gas_price_gwei,
                nonce: AtomicU64::new(NONCE_UNINITIALIZED),
                submit_lock: Mutex::new(()),
            }),
        })
    }

    /// Build, gas-estimate, and send a call to any contract.
    ///
    /// Shared entry point for the manager-routed offer and cancel calls.
    /// `log_agreement_id` is used only for structured logging.
    async fn build_and_send_call(
        &self,
        to: Address,
        calldata: Vec<u8>,
        log_agreement_id: &[u8; 16],
    ) -> Result<SubmittedTx, ChainClientError> {
        // 1. Build initial transaction request
        let tx = TransactionRequest::default()
            .from(self.inner.signer.address())
            .to(to)
            .input(calldata.into());

        // 2. Estimate gas with safety bounds.
        //
        // The estimator may surface a structured contract revert (e.g. an
        // already-canceled agreement). Box the typed error through alloy's
        // Custom transport variant so `rpc_pool.execute` can hand it back
        // without losing the selector and revert payload.
        let gas_limit = self
            .inner
            .rpc_pool
            .execute("estimate_gas", |provider| {
                let tx = tx.clone();
                let estimator = self.inner.gas_estimator.clone();
                async move {
                    estimator.estimate(&provider, &tx).await.map_err(|e| {
                        thegraph_core::alloy::transports::TransportErrorKind::custom(e)
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

    /// Ratchet the in-memory nonce counter up to `chain_pending + 1`.
    /// Never decreases the counter, so it is safe to call from any
    /// context without `submit_lock` held: an in-flight reservation
    /// from another caller cannot be invalidated. Callers needing a
    /// reservation call `next_nonce()` after.
    async fn resync_nonce(&self) -> Result<(), ChainClientError> {
        let chain_nonce = self.fetch_chain_nonce().await?;
        self.inner
            .nonce
            .fetch_max(chain_nonce + 1, Ordering::SeqCst);
        Ok(())
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
    /// errors, re-syncs from chain and retries. `submit_lock` spans the
    /// retry loop so concurrent reservations cannot interleave with each
    /// other's submissions; released before receipt polling.
    async fn sign_and_send(
        &self,
        mut tx: TransactionRequest,
        agreement_id: &[u8; 16],
    ) -> Result<SubmittedTx, ChainClientError> {
        const MAX_NONCE_RETRIES: u32 = 2;

        let _submit_guard = self.inner.submit_lock.lock().await;

        for attempt in 0..MAX_NONCE_RETRIES {
            if attempt > 0 {
                self.resync_nonce().await?;
            }
            let nonce = self.next_nonce().await?;

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
                    return Ok(SubmittedTx {
                        hash: tx_hash,
                        nonce,
                    });
                }
                Err(e) if is_nonce_error(&e.to_string()) && attempt + 1 < MAX_NONCE_RETRIES => {
                    tracing::warn!(
                        agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                        attempt = attempt + 1,
                        nonce,
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

    /// Submit a self-transfer of 0 wei at `nonce` so the chain has
    /// something to mine in a slot left empty by an evicted tx,
    /// releasing higher-nonce txs from the same wallet that were
    /// stuck behind the gap. Best-effort: an `is_nonce_error`
    /// rejection means the original is still in flight or the gap is
    /// already filled, so we treat that as success.
    async fn fill_nonce_gap(&self, nonce: u64) -> Result<(), ChainClientError> {
        let _submit_guard = self.inner.submit_lock.lock().await;

        let signer_addr = self.inner.signer.address();
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
        let max_fee_per_gas =
            calculate_max_fee(base_fee, priority_fee, self.inner.gas_price_multiplier);

        let tx = TransactionRequest::default()
            .from(signer_addr)
            .to(signer_addr)
            .value(thegraph_core::alloy::primitives::U256::ZERO)
            .with_gas_limit(21_000)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(priority_fee)
            .with_chain_id(self.inner.chain_id)
            .with_nonce(nonce);

        classify_fill_nonce_gap_outcome(nonce, self.send_transaction(&tx).await)
    }

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
    async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
        let block = self
            .inner
            .rpc_pool
            .execute("get_latest_block", |provider| async move {
                provider.get_block_by_number(BlockNumberOrTag::Latest).await
            })
            .await?
            .ok_or_else(|| {
                ChainClientError::RpcError(anyhow::anyhow!("no latest block returned"))
            })?;
        Ok(block.header.timestamp)
    }

    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        let manager = self.inner.recurring_agreement_manager_address;

        let agreement_id = dipper_rpc::indexer::derive_agreement_id(rca);

        // The manager is the payer; dipper is just the operator submitting the
        // tx, so encode offerAgreement(collector, NEW, abi(rca)).
        let calldata = IRecurringAgreementManager::offerAgreementCall {
            collector: self.inner.recurring_collector_address,
            offerType: OFFER_TYPE_NEW,
            offerData: rca.abi_encode().into(),
        }
        .abi_encode();

        tracing::info!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            manager = %manager,
            collector = %self.inner.recurring_collector_address,
            "Submitting RCA offer via RecurringAgreementManager"
        );

        let submitted = self
            .build_and_send_call(manager, calldata, &agreement_id)
            .await?;

        let SubmittedTx {
            hash: tx_hash,
            nonce: dropped_nonce,
        } = submitted;
        match self.wait_for_receipt(tx_hash, RECEIPT_POLL_TIMEOUT).await? {
            Some(true) => Ok(Some(tx_hash)),
            Some(false) => Err(ChainClientError::TxReverted { tx_hash }),
            None => {
                tracing::warn!(
                    agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    tx_hash = %tx_hash,
                    nonce = dropped_nonce,
                    "Manager offer tx did not mine within receipt-poll window; treating as dropped"
                );
                if let Err(err) = self.fill_nonce_gap(dropped_nonce).await {
                    tracing::warn!(nonce = dropped_nonce, error = %err, "Failed to fill mempool nonce gap");
                }
                Err(ChainClientError::TxDropped { tx_hash })
            }
        }
    }

    async fn cancel_via_manager(
        &self,
        collector: Address,
        agreement_id: &[u8; 16],
        version_hash: B256,
        options: u16,
    ) -> Result<Option<B256>, ChainClientError> {
        let manager = self.inner.recurring_agreement_manager_address;

        let calldata = IRecurringAgreementManager::cancelAgreementCall {
            collector,
            agreementId: FixedBytes::<16>::from_slice(agreement_id),
            versionHash: version_hash,
            options,
        }
        .abi_encode();

        tracing::info!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            manager = %manager,
            options,
            "Canceling agreement via RecurringAgreementManager"
        );

        let submitted = self
            .build_and_send_call(manager, calldata, agreement_id)
            .await?;

        // Wait for the receipt so a returned Ok means the cancel mined, not just
        // that it entered the mempool. The dispatch layer then re-reads on-chain
        // to catch a mined-but-no-op cancel (stale hash, unknown id, terminal).
        let SubmittedTx {
            hash: tx_hash,
            nonce: dropped_nonce,
        } = submitted;
        match self.wait_for_receipt(tx_hash, RECEIPT_POLL_TIMEOUT).await? {
            Some(true) => Ok(Some(tx_hash)),
            Some(false) => Err(ChainClientError::TxReverted { tx_hash }),
            None => {
                tracing::warn!(
                    agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    tx_hash = %tx_hash,
                    nonce = dropped_nonce,
                    "Manager cancel tx did not mine within receipt-poll window; treating as dropped"
                );
                if let Err(err) = self.fill_nonce_gap(dropped_nonce).await {
                    tracing::warn!(nonce = dropped_nonce, error = %err, "Failed to fill mempool nonce gap");
                }
                Err(ChainClientError::TxDropped { tx_hash })
            }
        }
    }

    async fn agreement_still_active(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<bool, ChainClientError> {
        let calldata = IRecurringCollector::getAgreementDetailsCall {
            agreementId: FixedBytes::<16>::from_slice(agreement_id),
            index: thegraph_core::alloy::primitives::U256::from(VERSION_CURRENT),
        }
        .abi_encode();

        let collector = self.inner.recurring_collector_address;
        let output = self
            .inner
            .rpc_pool
            .execute("get_agreement_details", |provider| {
                let calldata = calldata.clone();
                async move {
                    let tx = TransactionRequest::default()
                        .to(collector)
                        .input(calldata.into());
                    provider.call(tx).await
                }
            })
            .await?;

        let details = IRecurringCollector::getAgreementDetailsCall::abi_decode_returns(&output)
            .map_err(|err| {
                ChainClientError::RpcError(anyhow::anyhow!(
                    "undecodable getAgreementDetails from {collector}: {err}"
                ))
            })?;

        // Live iff the terms are accepted and no cancellation notice exists.
        // A cancel sets NOTICE_GIVEN while ACCEPTED stays set, so checking the
        // notice bit is what tells a still-live agreement from a cancelled one.
        let state = details.state;
        Ok(state & STATE_ACCEPTED != 0 && state & STATE_NOTICE_GIVEN == 0)
    }

    async fn reconcile_provider(
        &self,
        collector: Address,
        provider: Address,
    ) -> Result<Option<B256>, ChainClientError> {
        let manager = self.inner.recurring_agreement_manager_address;

        let calldata = IRecurringAgreementManager::reconcileProviderCall {
            collector,
            provider,
        }
        .abi_encode();

        tracing::info!(
            manager = %manager,
            collector = %collector,
            provider = %provider,
            "Reconciling provider escrow via RecurringAgreementManager"
        );

        // No agreement context here; pass a zero id for the shared call's
        // logging field only. The call target is the manager.
        let tx = self
            .build_and_send_call(manager, calldata, &[0u8; 16])
            .await?;
        Ok(Some(tx.hash))
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
    fn test_classify_fill_nonce_gap_outcome_success_returns_ok() {
        let result = classify_fill_nonce_gap_outcome(42, Ok(B256::ZERO));
        assert!(result.is_ok());
    }

    #[test]
    fn test_classify_fill_nonce_gap_outcome_swallows_nonce_error() {
        // Each of these strings flips `is_nonce_error` to true; the gap
        // fill must treat them as success because the original tx is
        // either still in flight or the slot is already filled.
        for msg in [
            "nonce too low",
            "replacement transaction underpriced",
            "already known",
            "invalid nonce",
        ] {
            let err = ChainClientError::SubmitFailed(anyhow::anyhow!("{msg}"));
            let result = classify_fill_nonce_gap_outcome(99, Err(err));
            assert!(
                result.is_ok(),
                "fill_nonce_gap must swallow {msg:?} so a still-live original tx is not treated as a hard failure"
            );
        }
    }

    #[test]
    fn test_classify_fill_nonce_gap_outcome_propagates_other_error() {
        // Errors that don't match `is_nonce_error` mean the noop tx itself
        // failed for a real reason (RPC down, gas estimation broken, etc.),
        // so the wallet may stay wedged. Surface to the caller.
        let err = ChainClientError::SubmitFailed(anyhow::anyhow!("connection timeout"));
        let result = classify_fill_nonce_gap_outcome(99, Err(err));
        assert!(
            result.is_err(),
            "non-nonce errors must propagate so the wedged-wallet path is observable"
        );
    }

    #[tokio::test]
    async fn test_nonce_reservation_unique_under_concurrent_callers() {
        use std::collections::HashSet;

        let counter = Arc::new(AtomicU64::new(NONCE_UNINITIALIZED));
        let lock = Arc::new(Mutex::new(()));
        let chain_pending = Arc::new(AtomicU64::new(100));
        let reserved: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

        const TASKS: usize = 50;
        let mut handles = Vec::with_capacity(TASKS);
        for i in 0..TASKS {
            let counter = counter.clone();
            let lock = lock.clone();
            let chain_pending = chain_pending.clone();
            let reserved = reserved.clone();

            handles.push(tokio::spawn(async move {
                let _guard = lock.lock().await;

                // Mirror the entry shape of `next_nonce`/`resync_nonce`:
                // - First caller initializes via fetch+CAS.
                // - Every seventh subsequent caller hits a "nonce error"
                //   path: ratchets the counter via fetch_max(chain + 1)
                //   then reserves via fetch_add. The ratchet never lowers
                //   the counter, so an in-flight reservation cannot be
                //   invalidated even if the chain reports a lower pending.
                // - Everyone else takes the next slot via fetch_add.
                let nonce = if counter.load(Ordering::SeqCst) == NONCE_UNINITIALIZED {
                    let chain_nonce = chain_pending.load(Ordering::SeqCst);
                    match counter.compare_exchange(
                        NONCE_UNINITIALIZED,
                        chain_nonce + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => chain_nonce,
                        Err(_) => counter.fetch_add(1, Ordering::SeqCst),
                    }
                } else if i % 7 == 0 {
                    let chain_nonce = chain_pending.load(Ordering::SeqCst);
                    counter.fetch_max(chain_nonce + 1, Ordering::SeqCst);
                    counter.fetch_add(1, Ordering::SeqCst)
                } else {
                    counter.fetch_add(1, Ordering::SeqCst)
                };

                // Simulate the time spent signing and submitting to the
                // mempool while the lock is held; this is the window the
                // bug exploited when the lock was missing.
                tokio::time::sleep(Duration::from_micros(50)).await;

                // Simulate the chain accepting the tx into pending: the
                // pending count cannot decrease, so use fetch_max.
                let _ = chain_pending.fetch_update(
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    |cur| Some(cur.max(nonce + 1)),
                );

                let mut reserved = reserved.lock().await;
                assert!(
                    reserved.insert(nonce),
                    "nonce {nonce} reissued to a concurrent caller (counter rewound past in-flight reservation)"
                );
            }));
        }

        for h in handles {
            h.await.expect("worker task panicked");
        }

        let reserved = reserved.lock().await;
        assert_eq!(
            reserved.len(),
            TASKS,
            "expected {TASKS} unique nonces, got {}",
            reserved.len()
        );
    }
}
