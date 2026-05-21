use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{ids::IndexingRequestId, state::FromState};
use dipper_rpc::admin::{
    SignedMessage,
    indexing_requests::{
        IndexingRequest, IndexingRequestStatus, IndexingRequestsRpcServer,
        SetIndexingTargetCandidates,
    },
};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use thegraph_core::{DeploymentId, alloy::primitives::Address};

use super::error_handling::{handle_list_result, handle_optional_result};
use crate::{
    registry::{
        IndexingRequest as IndexingRequestRecord, IndexingRequestRegistry,
        IndexingRequestStatus as IndexingRequestRecordStatus, SetTargetOutcome,
    },
    signing::eip712::Eip712Signer,
    worker::service::WorkerQueue,
};

/// The substate for the [`IndexingRequestsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct Ctx<R, W> {
    pub signer: Arc<Eip712Signer>,
    pub gateway_operator_allowlist: Arc<BTreeSet<Address>>,
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
        handle_list_result(
            self.registry.get_all_indexing_requests().await,
            "Failed to get all indexing requests",
            into_indexing_request,
        )
    }

    async fn get_indexing_request_by_id(
        &self,
        id: IndexingRequestId,
    ) -> RpcResult<IndexingRequest> {
        handle_optional_result(
            self.registry.get_indexing_request_by_id(&id).await,
            "Failed to get indexing request by id",
            into_indexing_request,
        )
    }

    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingRequest>> {
        handle_list_result(
            self.registry
                .get_indexing_requests_by_deployment_id(&deployment_id)
                .await,
            "Failed to get indexing requests by deployment id",
            into_indexing_request,
        )
    }

    async fn set_indexing_target_candidates(
        &self,
        req: SignedMessage<SetIndexingTargetCandidates>,
    ) -> RpcResult<Option<IndexingRequestId>> {
        let requested_by = match self.signer.recover_signer(&req) {
            Ok(addr) => addr,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(ErrorObject::borrowed(401, "Unauthorized", None));
            }
        };
        if !self.gateway_operator_allowlist.contains(&requested_by) {
            return Err(ErrorObject::borrowed(403, "Forbidden", None));
        }

        let SetIndexingTargetCandidates {
            deployment_id,
            chain_id,
            num_candidates,
        } = req.into_message();

        let num_candidates = num_candidates.unwrap_or(self.max_candidates);

        let outcome = match self
            .registry
            .set_indexing_target_candidates(requested_by, deployment_id, chain_id, num_candidates)
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::error!(error=?err, "Failed to set indexing target candidates");
                return Err(ErrorObject::borrowed(503, "Service unavailable", None));
            }
        };

        // Translate the outcome into the appropriate follow-up worker job and the
        // wire-level return value.
        let (id_opt, reassess_count): (Option<IndexingRequestId>, Option<usize>) = match outcome {
            SetTargetOutcome::Inserted { id } => {
                tracing::info!(
                    indexing_request_id = %id,
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    num_candidates,
                    "Inserted new indexing request"
                );
                (Some(id), Some(num_candidates))
            }
            SetTargetOutcome::Updated {
                id,
                new_num_candidates,
            } => {
                tracing::info!(
                    indexing_request_id = %id,
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    num_candidates = new_num_candidates,
                    "Updated num_candidates on open indexing request"
                );
                (Some(id), Some(new_num_candidates))
            }
            SetTargetOutcome::NoOp { id } => {
                tracing::debug!(
                    indexing_request_id = %id,
                    "Set target candidates is a no-op (count unchanged)"
                );
                (Some(id), None)
            }
            SetTargetOutcome::Canceled { id } => {
                tracing::info!(
                    indexing_request_id = %id,
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    "Canceled indexing request (target candidates set to zero)"
                );
                (Some(id), Some(0))
            }
            SetTargetOutcome::NoOpAlreadyEmpty => {
                tracing::warn!(
                    %requested_by,
                    %deployment_id,
                    %chain_id,
                    "set_indexing_target_candidates with num_candidates=0 against a key with no open request \
                     - nothing to cancel"
                );
                (None, None)
            }
        };

        // Queue reassessment if the row changed. Reassessment computes the
        // diff between the IISA target group of size `num_candidates` and the
        // current active agreements, then grows or shrinks accordingly. With
        // num_candidates=0 it shrinks to zero, firing the on-chain cancel for
        // every active agreement on the key.
        if let (Some(id), Some(count)) = (id_opt, reassess_count)
            && let Err(err) = self
                .worker
                .reassess_indexing_request(id, deployment_id, chain_id, count)
                .await
        {
            tracing::error!(
                indexing_request_id = %id,
                error = ?err,
                "Failed to queue task: 'reassess_indexing_request'"
            );
            return Err(ErrorObject::borrowed(500, "Internal server error", None));
        }

        Ok(id_opt)
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
