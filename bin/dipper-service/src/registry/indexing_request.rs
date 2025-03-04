//! # Indexing request
//!
//! Indexing Requests are initiated by the customer and are used to request indexing services
//! from indexers. The DIPs Gateway service (Dipper) is responsible for finding appropriate
//! indexers to fulfill the request.

use async_trait::async_trait;
use dipper_core::ids::IndexingRequestId;
use thegraph_core::{
    alloy::primitives::{Address, ChainId},
    DeploymentId,
};
use time::OffsetDateTime;

/// The Indexing Request registry trait.
///
/// This is a subset of the [`Registry`](dipper_registry::api::Registry) trait that is specific to
/// indexing requests.
#[async_trait]
pub trait IndexingRequestRegistry {
    /// Register a new indexing request.
    ///
    /// If successful, the method returns the ID of the newly created indexing request.
    async fn register_new_indexing_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<IndexingRequestId>;

    /// Get all indexing requests.
    async fn get_all_indexing_requests(&self) -> anyhow::Result<Vec<IndexingRequest>>;

    /// Get the indexing request by ID.
    async fn get_indexing_request_by_id(
        &self,
        id: &IndexingRequestId,
    ) -> anyhow::Result<Option<IndexingRequest>>;

    /// Get all indexing requests by Deployment ID.
    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> anyhow::Result<Vec<IndexingRequest>>;

    /// Mark an indexing request as `CANCELED`.
    ///
    /// If there is no indexing request with the given ID, or if the request is not in the
    /// `OPEN` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_request_as_canceled(&self, id: &IndexingRequestId)
        -> anyhow::Result<()>;
}

/// An Indexing Request represents the request for indexing services initiated by the customer.
///
/// The [`IndexingRequest`] is as a Record Data Structure.
#[derive(Debug, Clone)]
pub struct IndexingRequest {
    /// The unique identifier of the request.
    pub id: IndexingRequestId,

    /// The indexing request registration time.
    pub created_at: OffsetDateTime,

    /// The indexing request update time.
    pub updated_at: OffsetDateTime,

    /// The status of the request.
    pub status: Status,

    /// The indexing request issuer.
    ///
    /// The requester is the Ethereum address of the customer that initiated the request.
    ///
    /// Any interaction with this entity must be signed by the requester's address associated
    /// private key.
    pub requested_by: Address,

    /// The Subgraph deployment ID.
    pub deployment_id: DeploymentId,

    /// The Subgraph deployment chain ID.
    pub deployment_chain_id: ChainId,
}

/// The status of the [`IndexingRequest`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum Status {
    /// The indexing request was registered.
    ///
    /// There are no active agreements associated with the request, as no indexers have accepted
    /// the request yet. This is the initial state when the request is created.
    ///
    /// This is an intermediate state when all agreements associated with the request are cancelled
    /// or expired.
    #[default]
    Open,

    /// The indexing request was cancelled by the customer.
    ///
    /// Any associated agreements MUST be marked as `CANCELLED`.
    ///
    /// This is a terminal state.
    Canceled,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let status = match self {
            Status::Open => "OPEN",
            Status::Canceled => "CANCELED",
        };
        f.write_str(status)
    }
}

impl TryFrom<dipper_registry::IndexingRequest> for IndexingRequest {
    type Error = anyhow::Error;

    fn try_from(value: dipper_registry::IndexingRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: value.status.try_into()?,
            requested_by: value.requested_by,
            deployment_id: value.deployment_id,
            deployment_chain_id: value.deployment_chain_id,
        })
    }
}

impl TryFrom<dipper_registry::IndexingRequestStatus> for Status {
    type Error = anyhow::Error;

    fn try_from(value: dipper_registry::IndexingRequestStatus) -> Result<Self, Self::Error> {
        match value {
            dipper_registry::IndexingRequestStatus::Open => Ok(Status::Open),
            dipper_registry::IndexingRequestStatus::Canceled => Ok(Status::Canceled),
            _ => Err(anyhow::anyhow!("invalid indexing request status")),
        }
    }
}
