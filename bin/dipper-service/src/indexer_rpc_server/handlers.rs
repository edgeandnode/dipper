use std::{cmp::max, collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingAgreementId, state::FromState};
use dipper_rpc::indexer::gateway_server::{
    dips_cancellation_eip712_domain, dips_collection_eip712_domain, rpc, sol,
};
use thegraph_core::{
    AllocationId, IndexerId, ProofOfIndexing,
    alloy::{
        dyn_abi::SolType,
        primitives::{PrimitiveSignature as Signature, U256},
    },
    signed_message::SignedMessage,
};
use tonic::{Request, Response, Status};

use crate::{
    network::NetworkProvider,
    registry::{AgreementRegistry, IndexingAgreementStatus, ReceiptRegistry, ReportedWork},
    signing::{eip712::PrivateKeyEip712Signer, tap::ReceiptSigner},
    worker::WorkerQueue,
};

/// The substate for the [`DipsGatewayServiceImpl`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct DipsGatewayServiceCtx<R, N, W> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub tap_signer: Arc<ReceiptSigner>,
    pub allowlist: Arc<BTreeSet<IndexerId>>,
    pub registry: R,
    pub network: N,
    pub worker: W,
}

pub struct DipsGatewayServiceImpl<R, N, W>(DipsGatewayServiceCtx<R, N, W>);

impl<R, N, W> DipsGatewayServiceImpl<R, N, W> {
    /// Create a new instance of the [`DipsGatewayServiceImpl`] with the given context.
    pub fn with_context<S>(ctx: &S) -> Self
    where
        DipsGatewayServiceCtx<R, N, W>: FromState<S>,
    {
        Self(FromState::from_state(ctx))
    }
}

#[async_trait]
impl<R, N, W> rpc::GatewayDipsService for DipsGatewayServiceImpl<R, N, W>
where
    R: AgreementRegistry + ReceiptRegistry + Clone + Send + Sync + 'static,
    N: NetworkProvider + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn cancel_agreement(
        &self,
        request: Request<rpc::CancelAgreementRequest>,
    ) -> Result<Response<rpc::CancelAgreementResponse>, Status> {
        let rpc::CancelAgreementRequest {
            version,
            signed_cancellation,
        } = request.into_inner();

        // Check the version of the request, we are only supporting version 0 for the MVP
        if version != 0 {
            return Err(Status::invalid_argument("version not supported"));
        }

        // Deserialize the solidity Signed Cancellation Request struct
        let (sol_cancellation_req, signature) =
            match sol::SignedCancellationRequest::abi_decode(&signed_cancellation, true) {
                Ok(sol::SignedCancellationRequest { request, signature }) => (request, signature),
                Err(err) => return Err(Status::invalid_argument(format!("bad request: {err}"))),
            };

        // Deserialize the signature
        let signature = match Signature::try_from(signature.as_ref()) {
            Ok(signature) => signature,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to parse signature");
                return Err(Status::invalid_argument(format!("bad request: {err}")));
            }
        };

        // Recover the signer from the request (operator wallet address)
        let requested_by = match self.signer.recover_signer_with_domain(
            &dips_cancellation_eip712_domain(),
            &SignedMessage {
                message: sol_cancellation_req.clone(),
                signature,
            },
        ) {
            Ok(requested_by) => requested_by,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(Status::unauthenticated("failed to recover signer"));
            }
        };

        // Resolve the indexer ID from the operator wallet address who signed the request
        // And check if the signer is allowed to make this request
        let requested_by = match self
            .network
            .get_indexer_id_for_operator_address(&requested_by)
        {
            Some(indexer_id) if self.allowlist.contains(&indexer_id) => indexer_id,
            _ => {
                return Err(Status::permission_denied("Unauthorized"));
            }
        };

        let agreement_id = IndexingAgreementId::from(sol_cancellation_req.agreement_id.as_ref());

        // Check if the agreement exists and the indexer is the owner
        let agreement = match self
            .registry
            .get_indexing_agreement_by_id(&agreement_id)
            .await
        {
            Ok(None) => {
                return Err(Status::not_found("agreement not found"));
            }
            Ok(Some(agreement)) if agreement.indexer.id != requested_by => {
                return Err(Status::permission_denied("Unauthorized"));
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreement");
                return Err(Status::internal("Cancellation failed"));
            }
            Ok(Some(agreement)) => {
                // The agreement exists and the requester is authorised
                // Proceed with cancellation
                agreement
            }
        };

        // Process the indexing agreement cancellation
        if let Err(err) = self
            .worker
            .process_indexing_agreement_indexer_cancellation(
                agreement.indexing_request_id,
                agreement.id,
            )
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'process_indexing_agreement_indexer_cancellation'");
            return Err(Status::internal("Cancellation failed"));
        };

        Ok(Response::new(rpc::CancelAgreementResponse {}))
    }

    async fn collect_payment(
        &self,
        request: Request<rpc::CollectPaymentRequest>,
    ) -> Result<Response<rpc::CollectPaymentResponse>, Status> {
        let rpc::CollectPaymentRequest {
            version,
            signed_collection,
        } = request.into_inner();

        // Check the version of the request, we are only supporting version 0 for the MVP
        if version != 0 {
            return Err(Status::invalid_argument("version not supported"));
        }

        // Deserialize the solidity Signed Collection Request struct
        let (sol_collection_req, signature) =
            match sol::SignedCollectionRequest::abi_decode(&signed_collection, true) {
                Ok(sol::SignedCollectionRequest { request, signature }) => (request, signature),
                Err(err) => return Err(Status::invalid_argument(format!("bad request: {err}"))),
            };

        // Deserialize the signature
        let signature = match Signature::try_from(signature.as_ref()) {
            Ok(signature) => signature,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to parse signature");
                return Err(Status::invalid_argument(format!("bad request: {err}")));
            }
        };

        // Recover the signer from the request (operator wallet address)
        let requested_by = match self.signer.recover_signer_with_domain(
            &dips_collection_eip712_domain(),
            &SignedMessage {
                message: sol_collection_req.clone(),
                signature,
            },
        ) {
            Ok(requested_by) => requested_by,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(Status::unauthenticated("failed to recover signer"));
            }
        };

        // Resolve the indexer ID from the operator wallet address who signed the request
        // And check if the signer is allowed to make this request
        let indexer_id = match self
            .network
            .get_indexer_id_for_operator_address(&requested_by)
        {
            Some(indexer_id) if self.allowlist.contains(&indexer_id) => indexer_id,
            _ => {
                return Err(Status::permission_denied("Unauthorized"));
            }
        };

        let sol::CollectionRequest {
            agreement_id,
            allocation_id,
            entity_count,
        } = sol_collection_req;

        let agreement_id = IndexingAgreementId::from(agreement_id.as_ref());
        let allocation_id = AllocationId::from(allocation_id);

        // Get the current epoch
        let current_epoch = self.network.get_current_epoch();

        // Resolve the provided allocation from the network,
        // otherwise return an error if the allocation is not found
        let allocation = match self.network.get_allocation_by_id(&allocation_id) {
            None => {
                return Err(Status::not_found("allocation not found"));
            }
            Some(allocation) => allocation,
        };

        // Ensure the allocation is closed, at the current epoch or before
        // If the allocation is still open, or the closing epoch is in the future,
        // return a "too early" error
        let allocation_closed_at = match allocation.closed_at {
            None => {
                return Ok(Response::new(rpc::CollectPaymentResponse {
                    version, // Use the same version as the request
                    status: rpc::CollectPaymentStatus::ErrTooEarly as i32,
                    tap_receipt: vec![],
                }));
            }
            Some(closed_at) if closed_at > current_epoch => {
                return Ok(Response::new(rpc::CollectPaymentResponse {
                    version, // Use the same version as the request
                    status: rpc::CollectPaymentStatus::ErrTooEarly as i32,
                    tap_receipt: vec![],
                }));
            }
            Some(closed_at) => closed_at,
        };

        // Ensure the allocation has a valid proof of indexing
        // If the proof of indexing is not found, or is the zero value, return an error
        let allocation_poi = match allocation.proof_of_indexing {
            None => {
                return Err(Status::not_found("allocation POI not found"));
            }
            Some(poi) if poi == ProofOfIndexing::ZERO => {
                return Err(Status::not_found("allocation POI invalid"));
            }
            Some(poi) => poi,
        };

        // Fetch the agreement from the registry
        let agreement = match self
            .registry
            .get_indexing_agreement_by_id(&agreement_id)
            .await
        {
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreement");
                return Err(Status::internal("Failed to get indexing agreement"));
            }
            Ok(None) => {
                return Err(Status::not_found("agreement not found"));
            }
            Ok(Some(agreement)) => agreement,
        };

        // Ensure the indexer is the owner of the agreement
        if agreement.indexer.id != indexer_id {
            return Err(Status::permission_denied("Unauthorized"));
        }

        // Ensure the agreement is in an accepted state, otherwise return an error
        let agreement_accept_epoch = match &agreement.status {
            IndexingAgreementStatus::Accepted { at_epoch } => *at_epoch,
            IndexingAgreementStatus::Created | IndexingAgreementStatus::DeliveryFailed => {
                return Err(Status::not_found("agreement not found"));
            }
            IndexingAgreementStatus::Rejected => {
                return Err(Status::failed_precondition("agreement rejected"));
            }
            IndexingAgreementStatus::CanceledByRequester => {
                return Err(Status::failed_precondition(
                    "agreement cancelled by requester",
                ));
            }
            IndexingAgreementStatus::CanceledByIndexer => {
                return Err(Status::failed_precondition("agreement cancelled"));
            }
            IndexingAgreementStatus::Expired => {
                return Err(Status::failed_precondition("agreement expired"));
            }
        };

        // Get the last receipt for the agreement
        let last_receipt = match self
            .registry
            .get_last_receipt_for_agreement_id(&agreement_id)
            .await
        {
            Ok(receipt) => receipt,
            Err(err) => {
                tracing::error!(agreement_id=%agreement_id, error=?err, "Failed to get latest receipt");
                return Err(Status::internal("Failed to get latest receipt"));
            }
        };

        // If one has already redeemed a receipt for a later allocation, they must NOT be
        // allowed to redeem a receipt for an earlier allocation. In that case,
        // return a "too late" error
        if matches!(&last_receipt, Some(receipt) if receipt.reported_work.epoch >= allocation_closed_at)
        {
            return Ok(Response::new(rpc::CollectPaymentResponse {
                version, // Use the same version as the request
                status: rpc::CollectPaymentStatus::ErrTooLate as i32,
                tap_receipt: vec![],
            }));
        }

        // If the agreement was accepted after the allocation was opened, use the agreement's
        // accept epoch as the payment origin epoch, otherwise use the allocation's opening epoch
        let payment_orig_epoch = max(agreement_accept_epoch, allocation.opened_at);
        let payment_end_epoch = allocation_closed_at;
        let payment_epochs_elapsed = payment_end_epoch.saturating_sub(payment_orig_epoch);

        // Compute the amount to be paid for the reported work
        // The amount value must be less than or equal to:
        //  - If "initial-sync payment", the agreement's "max initial amount"
        //  - If "ongoing payment", the agreement's "max ongoing amount per epoch"
        let max_amount = if last_receipt.is_some() {
            agreement
                .voucher
                .max_ongoing_amount_per_epoch
                .saturating_mul(U256::from(payment_epochs_elapsed))
        } else {
            agreement.voucher.max_initial_amount
        };

        // Fee calculation:
        // total = (epochs_elapsed * base_price_per_epoch) + (entity_count * price_per_entity)
        let fee = {
            let mut total = U256::ZERO;
            total = total.saturating_add(
                U256::from(payment_epochs_elapsed)
                    .saturating_mul(U256::from(agreement.voucher.metadata.base_price_per_epoch)),
            );
            total = total.saturating_add(
                U256::from(entity_count)
                    .saturating_mul(U256::from(agreement.voucher.metadata.price_per_entity)),
            );
            total
        };

        // If the amount is out of bounds, return an error
        if fee > max_amount {
            tracing::debug!(
                requested_by=%requested_by,
                indexer_id=%indexer_id,
                agreement_id=%agreement_id,
                amount=%fee,
                max_amount=%max_amount,
                "Amount out of bounds"
            );
            return Ok(Response::new(rpc::CollectPaymentResponse {
                version, // Use the same version as the request
                status: rpc::CollectPaymentStatus::ErrAmountOutOfBounds as i32,
                tap_receipt: vec![],
            }));
        }

        // Create (and sign) the TAP receipt
        // As we are working with _wei GRT_, it is safe to downcast from alloy's U256 to u128
        let tap_receipt_fee = fee.saturating_to::<u128>();

        let tap_receipt = match self
            .tap_signer
            .create_receipt(allocation_id, tap_receipt_fee)
        {
            Ok(receipt) => receipt,
            Err(err) => {
                tracing::error!(error=?err, "Failed to create the TAP receipt");
                return Err(Status::internal("Failed to create the TAP receipt"));
            }
        };

        // Register the new receipt
        match self
            .registry
            .register_new_indexing_receipt(
                agreement_id,
                indexer_id,
                requested_by,
                ReportedWork {
                    epoch: current_epoch,
                    allocation_id,
                    entity_count,
                    poi: allocation_poi,
                },
                fee,
            )
            .await
        {
            Ok(id) => {
                tracing::info!(
                    receipt_id=%id,
                    indexer_id=%indexer_id,
                    allocation_id=%allocation_id,
                    deployment=%agreement.voucher.metadata.subgraph_deployment_id,
                    amount=%fee,
                    "New receipt emitted"
                );
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to create receipt");
                return Err(Status::internal("Failed to create receipt"));
            }
        };

        Ok(Response::new(rpc::CollectPaymentResponse {
            version, // Use the same version as the request
            status: rpc::CollectPaymentStatus::Accept as i32,
            tap_receipt: tap_receipt.serialize().into_bytes(),
        }))
    }
}

impl<R, N, W> std::ops::Deref for DipsGatewayServiceImpl<R, N, W> {
    type Target = DipsGatewayServiceCtx<R, N, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
