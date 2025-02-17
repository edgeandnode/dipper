use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingAgreementId, state::FromState};
use dipper_registry::{IndexingAgreementStatus, Registry};
use dipper_rpc::indexer::gateway_server::{
    dips_cancellation_eip712_domain, dips_collection_eip712_domain, rpc, sol,
};
use thegraph_core::{
    alloy::{dyn_abi::SolType, primitives::PrimitiveSignature as Signature},
    signed_message::SignedMessage,
    IndexerId,
};
use tonic::{Request, Response, Status};

use crate::{
    network::NetworkProvider, signer::PrivateKeyEip712Signer, tap::ReceiptSigner,
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
    R: Registry + Clone + Send + Sync + 'static,
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
        match self
            .registry
            .get_indexing_agreement_by_id(agreement_id)
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
            _ => {
                // The agreement exists and the requester is authorised
                // Proceed with cancellation
            }
        }

        // Process the indexing agreement cancellation
        if let Err(err) = self
            .worker
            .process_indexing_agreement_indexer_cancellation(agreement_id)
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
            allocation_id: _allocation_id,
            entity_count: _entity_count,
        } = sol_collection_req;

        let agreement_id = IndexingAgreementId::from(agreement_id.as_ref());

        // TODO: Check the reported epoch is correct, i.e., check against the network subgraph's
        //  latest reported epoch

        // Retrieve the agreement
        let agreement = match self
            .registry
            .get_indexing_agreement_by_id(agreement_id)
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
        match &agreement.status {
            IndexingAgreementStatus::Created | IndexingAgreementStatus::DeliveryFailed => {
                return Err(Status::not_found("agreement not found"));
            }
            IndexingAgreementStatus::Accepted => { /* OK */ }
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
            IndexingAgreementStatus::Unknown => {
                return Err(Status::data_loss("agreement status unknown"));
            }
        }

        // TODO: Review all of this
        // // TODO(post-mvp): Handle agreement expiration
        // //  Check if the agreement should be marked as expired, and if so, do it
        // //  Then return an error: `Status::failed_precondition("agreement expired")`
        //
        // // Get the latest receipt for the agreement, if any
        // let latest_receipt = match self
        //     .registry
        //     .get_latest_receipt_for_agreement(&agreement_id.into())
        //     .await
        // {
        //     Ok(receipt) => receipt,
        //     Err(err) => {
        //         tracing::error!(agreement_id=%agreement_id, error=?err, "Failed to get latest receipt");
        //         return Err(Status::internal("Failed to get latest receipt"));
        //     }
        // };
        //
        // // Check the reported epoch is greater than the last reported epoch
        // // Only if the agreement is not in the "initial-sync payment" phase
        // if let Some(receipt) = &latest_receipt {
        //     if epoch <= receipt.reported_work.epoch {
        //         return Err(Status::failed_precondition("invalid epoch"));
        //     }
        // }
        //
        // // Check the number of epochs elapsed since the last report is within the agreement's limits
        // // Only if the agreement is not in the "initial-sync payment" phase
        // if let Some(receipt) = &latest_receipt {
        //     let epochs_elapsed = epoch.saturating_sub(receipt.reported_work.epoch);
        //
        //     if epochs_elapsed < agreement.voucher.min_epochs_per_collection {
        //         return Err(Status::failed_precondition("too few epochs"));
        //     }
        //     if epochs_elapsed > agreement.voucher.max_epochs_per_collection {
        //         return Err(Status::failed_precondition("too many epochs"));
        //     }
        // }
        //
        // // Compute the amount to be paid for the reported work
        // // The amount value must be the minimum between the computed value and:
        // //  - If "initial-sync payment", the agreement's "max initial amount"
        // //  - If "ongoing payment", the agreement's "max ongoing amount per epoch"
        // let max_amount = if latest_receipt.is_none() {
        //     agreement.voucher.max_initial_amount
        // } else {
        //     agreement.voucher.max_ongoing_amount_per_epoch
        // };
        //
        // let amount = U256::from(0); // TODO: Compute the amount
        //
        // let receipt_amount = std::cmp::min(amount, max_amount);
        //
        // // Register the new receipt
        // match self
        //     .registry
        //     .register_new_indexing_receipt(
        //         agreement_id.into(),
        //         indexer_id,
        //         requested_by,
        //         IndexingReceiptReportedWork {
        //             epoch,
        //             blocks: report.blocks,
        //             entities: report.entities,
        //             poi,
        //         },
        //         amount,
        //     )
        //     .await
        // {
        //     Ok(id) => {
        //         tracing::info!(
        //             receipt_id=%id,
        //             indexer_id=%indexer_id,
        //             deployment=%agreement.voucher.metadata.deployment_id,
        //             amount=%receipt_amount,
        //             "New receipt emitted"
        //         );
        //     }
        //     Err(err) => {
        //         tracing::error!(error=?err, "Failed to create receipt");
        //         return Err(Status::internal("Failed to create receipt"));
        //     }
        // };
        //
        // // TODO: Sign and respond with the TAP receipt

        todo!()
    }
}

impl<R, N, W> std::ops::Deref for DipsGatewayServiceImpl<R, N, W> {
    type Target = DipsGatewayServiceCtx<R, N, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
