//! # Indexing request
//!
//! Indexing Requests are initiated by the customer and are used to request indexing services
//! from indexers. The DIPs Gateway service (Dipper) is responsible for finding appropriate
//! indexers to fulfill the request.

use std::convert::Infallible;

use dipper_core::ids::IndexingRequestId;
use sqlx::{postgres::PgRow, Error, Row as _};
use thegraph_core::{alloy::primitives::Address, DeploymentId};
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
}

impl IndexingRequest {
    /// Create a new [`IndexingRequest`].
    ///
    /// The request is created with the status [`Status::Open`],
    /// the creation and update times are set to the current time.
    pub fn new(requested_by: Address, deployment_id: DeploymentId) -> Self {
        Self {
            id: IndexingRequestId::new(),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            requested_by,
            deployment_id,
            status: Default::default(),
        }
    }

    /// Mark the [`IndexingRequest`] as [`Status::Canceled`].
    pub fn mark_as_canceled(&mut self) {
        self.status = Status::Canceled;
        self.updated_at = OffsetDateTime::now_utc();
    }
}

impl sqlx::FromRow<'_, PgRow> for IndexingRequest {
    fn from_row(row: &'_ PgRow) -> Result<Self, Error> {
        // Parse the status column
        let status = {
            let status: i32 = row.try_get("status")?;
            status.into()
        };

        // Parse the requested by column
        let requested_by = {
            let requested_by: String = row.try_get("requested_by")?;
            requested_by
                .parse()
                .map_err(|err| Error::Decode(Box::new(err)))
        }?;

        // Parse the deployment ID column
        let deployment_id = {
            let deployment_id: String = row.try_get("deployment_id")?;
            deployment_id
                .parse()
                .map_err(|err| Error::Decode(Box::new(err)))
        }?;

        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            status,
            requested_by,
            deployment_id,
        })
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
    sqlx::Type,
    num_derive::FromPrimitive,
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

impl From<i32> for Status {
    fn from(value: i32) -> Self {
        num_traits::FromPrimitive::from_i32(value).unwrap_or(Status::Unknown)
    }
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
