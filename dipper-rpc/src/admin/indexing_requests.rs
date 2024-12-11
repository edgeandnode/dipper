use dipper_core::ids::IndexingRequestId;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use thegraph_core::{
    alloy::primitives::{Address, ChainId},
    signed_message::ToSolStruct,
    DeploymentId,
};
use time::OffsetDateTime;

use super::message::SignedMessage;

/// The _indexing requests_ RPC methods
#[rpc(server, client)]
pub trait IndexingRequestsRpc {
    /// Get all _indexing requests_
    #[method(name = "get_all_indexing_requests")]
    async fn get_all_indexing_requests(&self) -> RpcResult<Vec<IndexingRequest>>;

    /// Get _indexing request_ by ID.
    ///
    /// Only one _indexing request_ will be returned, if it exists. Otherwise, an error will be
    /// returned.
    #[method(name = "get_indexing_request_by_id")]
    async fn get_indexing_request_by_id(&self, id: IndexingRequestId)
        -> RpcResult<IndexingRequest>;

    /// Get _indexing requests_ by deployment ID
    #[method(name = "get_indexing_requests_by_deployment_id")]
    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingRequest>>;

    /// Register a new _indexing request_
    #[method(name = "register_new_indexing_request")]
    async fn register_new_indexing_request(
        &self,
        req: SignedMessage<NewIndexingRequest>,
    ) -> RpcResult<IndexingRequestId>;

    /// Cancel an _indexing request_
    #[method(name = "cancel_indexing_request")]
    async fn cancel_indexing_request(
        &self,
        req: SignedMessage<CancelIndexingRequest>,
    ) -> RpcResult<()>;
}

/// The new indexing request message
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NewIndexingRequest {
    /// The deployment ID of the subgraph that should be indexed
    pub deployment_id: DeploymentId,
    /// The chain ID of the subgraph that should be indexed
    pub deployment_chain_id: ChainId,
}

impl ToSolStruct<NewIndexingRequestSol> for NewIndexingRequest {
    fn to_sol_struct(&self) -> NewIndexingRequestSol {
        NewIndexingRequestSol {
            deployment_id: self.deployment_id.into(),
        }
    }
}

thegraph_core::alloy::sol! {
    /// The new indexing request message (Solidity version)
    ///
    /// See: [`NewIndexingRequest::to_sol_struct(...)`](struct.NewIndexingRequest.html#method.to_sol_struct)
    struct NewIndexingRequestSol {
        bytes32 deployment_id;
    }
}

/// The cancel indexing request message
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CancelIndexingRequest {
    /// The ID of the indexing request to cancel
    pub id: IndexingRequestId,
}

impl ToSolStruct<CancelIndexingRequestSol> for CancelIndexingRequest {
    fn to_sol_struct(&self) -> CancelIndexingRequestSol {
        CancelIndexingRequestSol {
            id: self.id.as_bytes().into(),
        }
    }
}

thegraph_core::alloy::sol! {
    /// The cancel indexing request message (Solidity version)
    ///
    /// See: [`CancelIndexingRequest::to_sol_struct(...)`](struct.CancelIndexingRequest.html#method.to_sol_struct)
    struct CancelIndexingRequestSol {
        bytes16 id;
    }
}

/// The _indexing request_ response entity
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexingRequest {
    /// The unique identifier of the request.
    pub id: IndexingRequestId,

    /// The indexing request registration time.
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,

    /// The indexing request update time.
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,

    /// The status of the request.
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

/// The status of the [`IndexingRequest`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum IndexingRequestStatus {
    /// The indexing request was registered.
    ///
    /// There are no active agreements associated with the request, as no indexers have accepted
    /// the request yet. This is the initial state when the request is created.
    ///
    /// This is an intermediate state when all agreements associated with the request are cancelled
    /// or expired.
    Open,

    /// The indexing request was cancelled by the customer.
    ///
    /// Any associated agreements MUST be marked as `CANCELLED`.
    ///
    /// This is a terminal state.
    Canceled,

    /// The indexing request is in an unknown state.
    Unknown,
}

impl serde::Serialize for IndexingRequestStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let status = match self {
            IndexingRequestStatus::Open => "OPEN",
            IndexingRequestStatus::Canceled => "CANCELED",
            IndexingRequestStatus::Unknown => "UNKNOWN",
        };
        serializer.serialize_str(status)
    }
}

impl<'de> serde::Deserialize<'de> for IndexingRequestStatus {
    fn deserialize<D>(deserializer: D) -> Result<IndexingRequestStatus, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let status = String::deserialize(deserializer)?;
        let status = if status.eq_ignore_ascii_case("OPEN") {
            IndexingRequestStatus::Open
        } else if status.eq_ignore_ascii_case("CANCELED") {
            IndexingRequestStatus::Canceled
        } else {
            IndexingRequestStatus::Unknown
        };
        Ok(status)
    }
}
