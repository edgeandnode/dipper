use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::state::FromState;
use dipper_registry::{IndexingAgreementStatus, Registry};
use dipper_rpc::indexer::gateway_server::{
    graphprotocol::gateway::dips::{
        dips_service_server::DipsService, CancelAgreementRequest, CancelAgreementResponse,
        ReportProgressRequest, ReportProgressResponse,
    },
    CancelAgreementRequestMessage, ReportProgressRequestMessage,
};
use thegraph_core::{signed_message::SignedMessage, IndexerId};
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
impl<R, N, W> DipsService for DipsGatewayServiceImpl<R, N, W>
where
    R: Registry + Clone + Send + Sync + 'static,
    N: NetworkProvider + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn cancel_agreement(
        &self,
        request: Request<CancelAgreementRequest>,
    ) -> Result<Response<CancelAgreementResponse>, Status> {
        let req: SignedMessage<CancelAgreementRequestMessage> = request
            .into_inner()
            .try_into()
            .map_err(|err| Status::invalid_argument(format!("bad request: {err}")))?;

        // Recover the signer from the request (operator wallet address)
        let requested_by = match self.signer.recover_signer(&req) {
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

        let CancelAgreementRequestMessage { agreement_id } = req.message;

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

        Ok(Response::new(CancelAgreementResponse {}))
    }

    async fn report_progress(
        &self,
        request: Request<ReportProgressRequest>,
    ) -> Result<Response<ReportProgressResponse>, Status> {
        let req: SignedMessage<ReportProgressRequestMessage> = request
            .into_inner()
            .try_into()
            .map_err(|err| Status::invalid_argument(format!("bad request: {err}")))?;

        // Recover the signer from the request (operator wallet address)
        let requested_by = match self.signer.recover_signer(&req) {
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

        let ReportProgressRequestMessage { agreement_id } = req.message;

        // Retrieve the agreement
        let agreement = match self
            .registry
            .get_indexing_agreement_by_id(agreement_id)
            .await
        {
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreement");
                return Err(Status::internal("Cancellation failed"));
            }
            Ok(None) => {
                return Err(Status::not_found("agreement not found"));
            }
            Ok(Some(agreement)) => agreement,
        };

        // Ensure the indexer is the owner of the agreement
        if agreement.indexer.id != requested_by {
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

        // TODO(Post-MVP): Handle agreement expiration
        //  Check if the agreement should be marked as expired, and if so, do it

        todo!()
    }
}

impl<R, N, W> std::ops::Deref for DipsGatewayServiceImpl<R, N, W> {
    type Target = DipsGatewayServiceCtx<R, N, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
