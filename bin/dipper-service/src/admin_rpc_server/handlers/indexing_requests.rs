use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingRequestId, state::FromState};
use dipper_rpc::admin::{
    SignedMessage,
    indexing_requests::{
        CancelIndexingRequest, IndexingRequest, IndexingRequestStatus, IndexingRequestsRpcServer,
        NewIndexingRequest,
    },
};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use thegraph_core::{DeploymentId, alloy::primitives::Address};

use crate::{
    registry::{
        Error as RegistryError, IndexingRequest as IndexingRequestRecord, IndexingRequestRegistry,
        IndexingRequestStatus as IndexingRequestRecordStatus,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::WorkerQueue,
};

/// The substate for the [`IndexingRequestsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct Ctx<R, W> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub allowlist: Arc<BTreeSet<Address>>,
    pub registry: R,
    pub worker: W,
    pub max_candidates: usize,
}

pub struct RpcServerImpl<R, W>(Ctx<R, W>);

impl<R, W> RpcServerImpl<R, W> {
    /// Create a new instance of the `IndexingRequestsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        Ctx<R, W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

#[async_trait]
impl<R, W> IndexingRequestsRpcServer for RpcServerImpl<R, W>
where
    R: IndexingRequestRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn get_all_indexing_requests(&self) -> RpcResult<Vec<IndexingRequest>> {
        let indexing_requests = match self.registry.get_all_indexing_requests().await {
            Ok(res) => res.into_iter().map(into_indexing_request).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get all indexing requests");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        };

        Ok(indexing_requests)
    }

    async fn get_indexing_request_by_id(
        &self,
        id: IndexingRequestId,
    ) -> RpcResult<IndexingRequest> {
        let indexing_request = match self.registry.get_indexing_request_by_id(&id).await {
            Ok(Some(res)) => into_indexing_request(res),
            Ok(None) => {
                return Err(ErrorObject::borrowed(404, "Not found", None));
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing request by id");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        };

        Ok(indexing_request)
    }

    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingRequest>> {
        let indexing_request = match self
            .registry
            .get_indexing_requests_by_deployment_id(&deployment_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_request).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing request by id");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        };

        Ok(indexing_request)
    }

    async fn register_new_indexing_request(
        &self,
        req: SignedMessage<NewIndexingRequest>,
    ) -> RpcResult<IndexingRequestId> {
        // Check if the signer is authorized to make this request
        let requested_by = match self.signer.recover_signer(&req) {
            Ok(requested_by) => requested_by,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(ErrorObject::borrowed(401, "Unauthorized", None));
            }
        };
        if !self.allowlist.contains(&requested_by) {
            return Err(ErrorObject::borrowed(403, "Forbidden", None));
        }

        let NewIndexingRequest {
            deployment_id,
            chain_id,
        } = req.into_message();

        // Register the new indexing request
        let indexing_request_id = match self
            .registry
            .register_new_indexing_request(requested_by, deployment_id, chain_id)
            .await
        {
            Ok(indexing_request_id) => indexing_request_id,
            Err(err) => {
                tracing::error!(error=?err, "Failed to register new indexing request");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        };

        // Process the new indexing request
        if let Err(err) = self
            .worker
            .process_new_indexing_request(
                indexing_request_id,
                deployment_id,
                chain_id,
                self.max_candidates,
            )
            .await
        {
            tracing::error!(error=?err, "Failed queue task: 'process_new_indexing_request'");
            return Err(ErrorObject::borrowed(500, "Internal server error", None));
        };

        Ok(indexing_request_id)
    }

    async fn cancel_indexing_request(
        &self,
        req: SignedMessage<CancelIndexingRequest>,
    ) -> RpcResult<()> {
        // Check if the signer is authorized to make this request
        let requested_by = match self.signer.recover_signer(&req) {
            Ok(requested_by) => requested_by,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(ErrorObject::borrowed(401, "Unauthorized", None));
            }
        };
        if !self.allowlist.contains(&requested_by) {
            return Err(ErrorObject::borrowed(403, "Forbidden", None));
        }

        let CancelIndexingRequest {
            id: indexing_request_id,
        } = req.into_message();

        tracing::debug!(%indexing_request_id, %requested_by, "Canceling indexing request");

        // Check if the indexing request exists
        match self
            .registry
            .get_indexing_request_by_id(&indexing_request_id)
            .await
        {
            Ok(None) => {
                tracing::debug!(
                    %indexing_request_id,
                    %requested_by,
                    "Indexing request not found"
                );
                return Err(ErrorObject::borrowed(404, "Not found", None));
            }
            Err(err) => {
                tracing::error!(
                    %indexing_request_id,
                    %requested_by,
                    error=?err,
                    "Failed to get indexing request"
                );
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
            _ => {
                // The indexing request exists, proceed with cancellation
            }
        }

        // Mark the indexing request as `CANCELED`
        if let Err(RegistryError::BackendError(err)) = self
            .registry
            .mark_indexing_request_as_canceled(&indexing_request_id)
            .await
        {
            tracing::error!(
                %indexing_request_id,
                error=?err,
                "Failed to mark indexing request as canceled"
            );
            return Err(ErrorObject::borrowed(503, "Service unavailable", None));
        };

        // Process the indexing request cancellation
        if let Err(err) = self
            .worker
            .process_indexing_request_cancellation(indexing_request_id)
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'process_indexing_request_cancellation'");
            return Err(ErrorObject::borrowed(500, "Internal server error", None));
        };

        Ok(())
    }
}

impl<R, W> std::ops::Deref for RpcServerImpl<R, W> {
    type Target = Ctx<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn into_indexing_request(request: IndexingRequestRecord) -> IndexingRequest {
    IndexingRequest {
        id: request.id,
        created_at: request.created_at,
        updated_at: request.updated_at,
        status: into_indexing_request_status(request.status),
        requested_by: request.requested_by,
        deployment_id: request.deployment_id,
    }
}

fn into_indexing_request_status(status: IndexingRequestRecordStatus) -> IndexingRequestStatus {
    match status {
        IndexingRequestRecordStatus::Open => IndexingRequestStatus::Open,
        IndexingRequestRecordStatus::Canceled => IndexingRequestStatus::Canceled,
    }
}
