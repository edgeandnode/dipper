use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingRequestId, signed_message::serde::SignedMessage, state::FromState};
use dipper_pgmq::queue::Queue;
use dipper_registry::{
    Error as RegistryError, IndexingRequest as IndexingRequestRecord,
    IndexingRequestStatus as IndexingRequestRecordStatus, Registry,
};
use dipper_rpc::admin::indexing_requests::{
    CancelIndexingRequest, IndexingRequest, IndexingRequestStatus, IndexingRequestsRpcServer,
    NewIndexingRequest,
};
use jsonrpsee::core::RpcResult;
use thegraph_core::{alloy::primitives::Address, DeploymentId};

use crate::{
    rpc_server::context::Ctx,
    signer::PrivateKeyEip712Signer,
    worker::messages::{Message, ProcessIndexingRequestCancellation, ProcessNewIndexingRequest},
};

/// The substate for the [`IndexingRequestsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct IndexingRequestsCtx<R, W> {
    signer: Arc<PrivateKeyEip712Signer>,
    allowlist: Arc<BTreeSet<Address>>,
    registry: R,
    worker: W,
    max_candidates: usize,
}

impl<R, W> FromState<Ctx<R, W>> for IndexingRequestsCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            allowlist: ctx.allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
            max_candidates: ctx.max_candidates,
        }
    }
}

pub struct IndexingRequestsRpcServerImpl<R, W>(IndexingRequestsCtx<R, W>);

impl<R, W> IndexingRequestsRpcServerImpl<R, W> {
    /// Create a new instance of the `IndexingRequestsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        IndexingRequestsCtx<R, W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

#[async_trait]
impl<R, W> IndexingRequestsRpcServer for IndexingRequestsRpcServerImpl<R, W>
where
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    async fn get_all_indexing_requests(&self) -> RpcResult<Vec<IndexingRequest>> {
        let indexing_requests = match self.registry.get_all_indexing_requests().await {
            Ok(res) => res.into_iter().map(into_indexing_request).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get all indexing requests");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
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
                // return Err(StatusCode::NOT_FOUND);
                todo!("Return error");
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing request by id");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
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
            .get_all_indexing_requests_by_deployment_id(&deployment_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_request).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing request by id");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
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
                // return Err(StatusCode::UNAUTHORIZED);
                todo!("Return error");
            }
        };
        if !self.allowlist.contains(&requested_by) {
            // return Err(StatusCode::FORBIDDEN);
            todo!("Return error");
        }

        let NewIndexingRequest {
            deployment_id,
            deployment_chain_id,
        } = req.into_message();

        // TODO: Validate the deployment_id exists (in the network) and chain_id is valid

        // Register the new indexing request
        let indexing_request_id = match self
            .registry
            .register_new_indexing_request(requested_by, deployment_id, deployment_chain_id)
            .await
        {
            Ok(indexing_request_id) => indexing_request_id,
            Err(err) => {
                tracing::error!(error=?err, "Failed to register new indexing request");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
            }
        };

        // Process the new indexing request
        if let Err(err) = self
            .worker
            .push(Message::ProcessNewIndexingRequest(
                ProcessNewIndexingRequest {
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
                    num_candidates: self.max_candidates,
                },
            ))
            .await
        {
            tracing::error!(error=?err, "Failed queue task: 'process_new_indexing_request'");
            // return Err(StatusCode::INTERNAL_SERVER_ERROR);
            todo!("Return error");
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
                // return Err(StatusCode::UNAUTHORIZED);
                todo!("Return error");
            }
        };
        if !self.allowlist.contains(&requested_by) {
            // return Err(StatusCode::FORBIDDEN);
            todo!("Return error");
        }

        let CancelIndexingRequest {
            id: indexing_request_id,
        } = req.into_message();

        // Check if the indexing request exists
        match self
            .registry
            .get_indexing_request_by_id(&indexing_request_id)
            .await
        {
            Ok(None) => {
                // return Err(StatusCode::NOT_FOUND);
                todo!("Return error");
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing request");
                // return Err(StatusCode::INTERNAL_ERROR);
                todo!("Return error");
            }
            _ => {
                // The indexing request exists, proceed with cancellation
            }
        }

        // Mark the indexing request as `CANCELED`
        if let Err(RegistryError::DbError(err)) = self
            .registry
            .mark_indexing_request_as_canceled(&indexing_request_id)
            .await
        {
            tracing::error!(%indexing_request_id, error=?err, "Failed to mark indexing request as canceled");
            // return Err(StatusCode::INTERNAL_SERVER_ERROR);
            todo!("Return error");
        };

        // Process the indexing request cancellation
        if let Err(err) = self
            .worker
            .push(Message::ProcessIndexingRequestCancellation(
                ProcessIndexingRequestCancellation {
                    indexing_request_id,
                },
            ))
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'ProcessIndexingRequestCancellation'");
            // return Err(StatusCode::INTERNAL_SERVER_ERROR);
            todo!("Return error")
        };

        Ok(())
    }
}

impl<R, W> std::ops::Deref for IndexingRequestsRpcServerImpl<R, W> {
    type Target = IndexingRequestsCtx<R, W>;

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
        IndexingRequestRecordStatus::Unknown => IndexingRequestStatus::Unknown,
    }
}
