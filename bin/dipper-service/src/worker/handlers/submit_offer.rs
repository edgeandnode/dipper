//! Submit an RCA offer on-chain after the indexer has accepted the proposal.
//!
//! This handler runs after `send_indexing_agreement_proposal` receives an
//! Accept response from the indexer. The worker pipeline is:
//!
//! 1. `process_new_indexing_request` registers the agreement in the DB.
//! 2. `send_indexing_agreement_proposal` sends the gRPC proposal to the
//!    indexer, which validates pricing/metadata/networks and responds
//!    Accept or Reject.
//! 3. On Accept, `submit_offer` (this handler) posts the RCA offer on-chain
//!    via `RecurringCollector.offer()`. The indexer-agent then calls
//!    `acceptIndexingAgreement` — the contract checks `rcaOffers`.
//!
//! Idempotency is gated on the indexing-payments-subgraph's `Offer` entity,
//! not an RPC call. The `rcaOffers` mapping on `RecurringCollector` lives
//! inside an ERC-7201 namespaced storage struct with no public getter, so
//! dipper reuses the same subgraph indexer-rs queries to check whether a
//! prior submission already landed. The subgraph handler is idempotent on
//! duplicate `OfferStored` events, so a crashed restart that races the
//! subgraph's indexing lag and re-submits will end up as a no-op at the
//! entity level even if it costs a second on-chain transaction.

use std::time::Duration;

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::{
    chain_client::{ChainClient, ChainClientError},
    indexer_rpc_client::into_sol_rca,
    registry::{AgreementRegistry, IndexingAgreementStatus},
    worker::result::{JobError, JobMeta, JobResult},
};

pub struct Ctx<R, T> {
    pub registry: R,
    pub chain_client: T,
}

/// Submit an RCA offer on-chain.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub agreement_id: IndexingAgreementId,
    pub indexing_request_id: IndexingRequestId,
    pub indexer_url: Url,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
}

pub async fn handle<R, T>(
    ctx: Ctx<R, T>,
    Message {
        agreement_id,
        indexing_request_id,
        indexer_url: _,
        deployment_id: _,
        deployment_chain_id: _,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: AgreementRegistry,
    T: ChainClient,
{
    // Fetch the agreement. Skip silently if it's already been transitioned
    // out of Created (e.g. expired by the reassignment service).
    let agreement = match ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    {
        None => {
            tracing::error!(
                agreement_id = %agreement_id,
                "Agreement not found in registry at submit_offer"
            );
            return Ok(());
        }
        Some(a) if a.status != IndexingAgreementStatus::Created => {
            tracing::warn!(
                agreement_id = %agreement_id,
                status = %a.status,
                "Agreement not in Created status, skipping offer submission"
            );
            return Ok(());
        }
        Some(a) => a,
    };

    // Rebuild the on-chain RCA struct from the agreement's stored terms.
    // This mirrors what send_indexing_agreement_proposal will do when it
    // ABI-encodes the proposal for gRPC dispatch -- the RCA bytes must be
    // byte-for-byte identical so the on-chain offerHash matches what the
    // indexer computes locally.
    let (rca, derived_id) = into_sol_rca(agreement.nonce_uuid, agreement.terms.clone());

    // Sanity check: the derived on-chain ID must match the agreement ID we
    // stored at registration time. If it doesn't, our conversion path has
    // drifted and every downstream step will fail. Treat as fatal.
    if &derived_id != agreement_id.as_bytes() {
        tracing::error!(
            agreement_id = %agreement_id,
            derived = %format_args!("0x{}", derived_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            "Derived on-chain ID does not match stored agreement ID"
        );
        return Err(JobError::Fatal(anyhow::anyhow!(
            "derived on-chain ID drift"
        )));
    }

    tracing::info!(
        indexing_request_id = %indexing_request_id,
        agreement_id = %agreement_id,
        "Submitting RCA offer on-chain"
    );

    match ctx.chain_client.post_offer(&rca).await {
        Ok(None) => {
            tracing::info!(
                agreement_id = %agreement_id,
                "Offer already stored on-chain with matching hash, proceeding to dispatch"
            );
        }
        Ok(Some(tx_hash)) => {
            tracing::info!(
                agreement_id = %agreement_id,
                tx_hash = %tx_hash,
                "Offer submitted on-chain successfully"
            );
            // Observability only: record which tx hash actually mined.
            // Any failure here is non-fatal to the overall flow.
            if let Err(err) = ctx
                .registry
                .update_offer_tx_hash(agreement_id, tx_hash.as_ref())
                .await
            {
                tracing::warn!(
                    agreement_id = %agreement_id,
                    tx_hash = %tx_hash,
                    error = %err,
                    "Failed to persist offer_tx_hash; continuing"
                );
            }
        }
        Err(ChainClientError::OfferHashMismatch {
            stored, expected, ..
        }) => {
            // Stored hash does not match our locally-computed hash. This
            // means someone else submitted an offer for this agreement ID
            // with different terms -- either a dev-state race or a genuine
            // conflict. Mark the agreement delivery-failed and bail; the
            // reassignment service will find a replacement.
            tracing::error!(
                agreement_id = %agreement_id,
                stored = %stored,
                expected = %expected,
                "Offer hash mismatch on-chain, marking agreement as delivery-failed"
            );
            ctx.registry
                .mark_indexing_agreement_as_delivery_failed(agreement_id)
                .await
                .map_err(|err| JobError::Fatal(err.into()))?;
            return Ok(());
        }
        Err(err @ ChainClientError::TxDropped { .. }) => {
            // Accepted by the RPC but never mined — typically evicted by a
            // colliding-nonce tx. `post_offer` already re-synced the nonce
            // counter; re-running the handler will resubmit with a fresh
            // nonce, and the subgraph idempotency check short-circuits if
            // the dropped tx eventually lands before the replay.
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Offer tx dropped from mempool, will retry with fresh nonce"
            );
            return Err(JobError::Retryable(err.into(), Duration::from_secs(5)));
        }
        Err(err) => {
            // Other submission failures (RPC, gas, nonce, on-chain revert).
            // Retry with backoff -- the build_and_send_call path already has
            // bounded nonce retries, so returning Retryable here escalates
            // to the worker-level backoff.
            tracing::warn!(
                agreement_id = %agreement_id,
                error = %err,
                "Failed to submit offer on-chain, will retry"
            );
            return Err(JobError::Retryable(err.into(), Duration::from_secs(30)));
        }
    }

    // Offer is confirmed on-chain (or was already there). The indexer-agent
    // will pick up the pending_rca_proposals row and call
    // acceptIndexingAgreement — the contract checks rcaOffers at that point.
    // No further enqueue needed; chain_listener detects the acceptance event.
    Ok(())
}
