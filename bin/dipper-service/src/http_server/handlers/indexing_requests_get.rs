use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dipper_core::ids::IndexingRequestId;
use dipper_registry::{IndexingRequest, IndexingRequestStatus, Registry};
use serde_with::serde_as;
use thegraph_core::{Address, DeploymentId};
use time::OffsetDateTime;

use crate::http_server::context::Ctx;

/// The substate for the `get_all_indexing_requests` handler.
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct GetIndexingRequestsCtx<R> {
    registry: R,
}

impl<R, W> axum::extract::FromRef<Ctx<R, W>> for GetIndexingRequestsCtx<R>
where
    R: Clone,
{
    fn from_ref(ctx: &Ctx<R, W>) -> Self {
        Self {
            registry: ctx.registry.clone(),
        }
    }
}

#[serde_as]
#[derive(serde::Serialize)]
pub struct IndexingRequestResponse {
    /// The unique identifier of the request.
    pub id: IndexingRequestId,

    /// The indexing request registration time.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub created_at: OffsetDateTime,

    /// The indexing request update time.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub updated_at: OffsetDateTime,

    /// The status of the request.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub status: IndexingRequestStatus,

    /// The indexing request issuer.
    ///
    /// The requester is the Ethereum address of the customer that initiated the request.
    ///
    /// Any interaction with this entity must be signed by the requester's address associated
    /// private key.
    pub requested_by: Address,

    /// The Subgraph deployment ID.
    pub deployment_id: DeploymentId,
}

impl From<IndexingRequest> for IndexingRequestResponse {
    fn from(request: IndexingRequest) -> Self {
        Self {
            id: request.id,
            created_at: request.created_at,
            updated_at: request.updated_at,
            status: request.status,
            requested_by: request.requested_by,
            deployment_id: request.deployment_id,
        }
    }
}

pub async fn get_indexing_request_by_id<R>(
    State(ctx): State<GetIndexingRequestsCtx<R>>,
    Path(indexing_request_id): Path<IndexingRequestId>,
) -> Result<Json<IndexingRequestResponse>, StatusCode>
where
    R: Registry,
{
    // Get indexing request by id
    let indexing_request = match ctx
        .registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
    {
        Ok(Some(res)) => res.into(),
        Ok(None) => {
            return Err(StatusCode::NOT_FOUND);
        }
        Err(err) => {
            tracing::error!(error=?err, "Failed to get indexing request by id");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(indexing_request))
}

pub async fn get_all_indexing_requests<R>(
    State(ctx): State<GetIndexingRequestsCtx<R>>,
) -> Result<Json<Vec<IndexingRequestResponse>>, StatusCode>
where
    R: Registry,
{
    // Get all indexing requests
    let indexing_requests = match ctx.registry.get_all_indexing_requests().await {
        Ok(res) => res.into_iter().map(Into::into).collect(),
        Err(err) => {
            tracing::error!(error=?err, "Failed to get all indexing requests");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    Ok(Json(indexing_requests))
}
