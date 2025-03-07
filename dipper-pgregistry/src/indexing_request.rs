//! # Indexing request
//!
//! Indexing Requests are initiated by the customer and are used to request indexing services
//! from indexers. The DIPs Gateway service (Dipper) is responsible for finding appropriate
//! indexers to fulfill the request.

use std::convert::Infallible;

use dipper_core::ids::IndexingRequestId;
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};
use time::OffsetDateTime;

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

impl IndexingRequest {
    /// Create a new [`IndexingRequest`].
    ///
    /// The request is created with the status [`Status::Open`],
    /// the creation and update times are set to the current time.
    pub fn new(
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> Self {
        Self {
            id: IndexingRequestId::new(),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status: Default::default(),
            requested_by,
            deployment_id,
            deployment_chain_id,
        }
    }

    /// Mark the [`IndexingRequest`] as [`Status::Canceled`].
    pub fn mark_as_canceled(&mut self) {
        self.status = Status::Canceled;
        self.updated_at = OffsetDateTime::now_utc();
    }
}

/// The status of the [`IndexingRequest`].
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Default,
    num_derive::FromPrimitive,
    num_derive::ToPrimitive,
)]
#[repr(i32)]
pub enum Status {
    /// The indexing request was registered.
    ///
    /// There are no active agreements associated with the request, as no indexers have accepted
    /// the request yet. This is the initial state when the request is created.
    ///
    /// This is an intermediate state when all agreements associated with the request are cancelled
    /// or expired.
    #[default]
    Open = 0,

    /// The indexing request was cancelled by the customer.
    ///
    /// Any associated agreements MUST be marked as `CANCELLED`.
    ///
    /// This is a terminal state.
    Canceled = 1,

    /// The indexing request is in an unknown state.
    Unknown = i32::MAX,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let status = match self {
            Status::Open => "OPEN",
            Status::Canceled => "CANCELED",
            Status::Unknown => "UNKNOWN",
        };
        f.write_str(status)
    }
}

impl std::str::FromStr for Status {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let status = match s {
            "OPEN" => Status::Open,
            "CANCELED" => Status::Canceled,
            _ => Status::Unknown,
        };
        Ok(status)
    }
}
