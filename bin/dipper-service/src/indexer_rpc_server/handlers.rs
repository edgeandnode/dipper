use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingAgreementId, state::FromState};
use dipper_rpc::indexer::gateway_server::{rpc, sol};
use thegraph_core::{
    IndexerId,
    alloy::{dyn_abi::SolType, primitives::Signature},
    signed_message::SignedMessage,
};
use tonic::{Request, Response, Status};

use crate::{
    network::NetworkProvider, registry::AgreementRegistry, signing::eip712::PrivateKeyEip712Signer,
    worker::service::WorkerQueue,
};

/// The context shared across all requests.
#[derive(Clone)]
pub struct Ctx<R, N, W> {
    /// The EIP-712 signer
    pub signer: Arc<PrivateKeyEip712Signer>,

    /// The allowlist of indexers that allowed to make requests to the DIPs gateway Network API
    pub allowlist: Arc<BTreeSet<IndexerId>>,

    /// The DIPs registry
    pub registry: R,

    /// The Network provider
    pub network: N,

    /// The message queue worker
    pub worker: W,
}

pub struct RpcServiceImpl<R, N, W>(Ctx<R, N, W>);

impl<R, N, W> RpcServiceImpl<R, N, W> {
    /// Create a new instance of the [`RpcServiceImpl`] with the given context.
    pub fn with_context<S>(ctx: &S) -> Self
    where
        Ctx<R, N, W>: FromState<S>,
    {
        Self(FromState::from_state(ctx))
    }
}

#[async_trait]
impl<R, N, W> rpc::GatewayDipsService for RpcServiceImpl<R, N, W>
where
    R: AgreementRegistry + Clone + Send + Sync + 'static,
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
            match sol::SignedCancellationRequest::abi_decode(&signed_cancellation) {
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
        let requested_by = match self
            .signer
            .recover_dips_cancellation_msg_signer(&SignedMessage {
                message: sol_cancellation_req.clone(),
                signature,
            }) {
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
        _request: Request<rpc::CollectPaymentRequest>,
    ) -> Result<Response<rpc::CollectPaymentResponse>, Status> {
        Err(tonic::Status::unimplemented(
            "collect_payment is not supported in V2; payment is handled on-chain via RecurringCollector",
        ))
    }
}

impl<R, N, W> std::ops::Deref for RpcServiceImpl<R, N, W> {
    type Target = Ctx<R, N, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
