//! # Indexing Agreements
//!
//! Indexer Agreements MUST be associated with one Indexing Request, and represent the contract
//! between the DIPs Gateway (Dipper) and the indexer to index the data.
//!
//! - An agreement MUST be associated with an *indexing request*.
//! - Agreements MUST be explicitly accepted (or rejected) by an indexer.
//! - An agreement is in effect until the indexer indexes the data or the agreement is cancelled.
//!   It can be cancelled by the customer or the indexer.
//! - An agreement can also expire if the indexer does not accept the agreement within a predefine
//!   time frame.
//!
//! An Indexer Agreement is created every time the Dipper runs the *Indexing Indexer Selection
//! Algorithm (IISA)* and finds an indexer to fulfill the *indexing request*.

use std::{str::FromStr, time::Duration};

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use sqlx::{postgres::PgRow, Error, Row as _};
use thegraph_core::{Address, IndexerId};
use time::OffsetDateTime;
use url::Url;

/// An Indexing Agreement represents the contract between the DIPs Gateway (Dipper) and the indexer
/// to index the data.
///
/// The [`IndexingAgreement`] is as a Data Transfer Object (DTO).
#[derive(Debug, Clone)]
pub struct IndexingAgreement {
    /// The indexing agreement unique ID.
    pub id: IndexingAgreementId,

    /// The indexing agreement creation time.
    pub created_at: OffsetDateTime,

    // The indexing agreement update time.
    pub updated_at: OffsetDateTime,

    /// The indexing agreement status.
    pub status: Status,

    /// The indexing agreement associated indexing request
    pub indexing_request_id: IndexingRequestId,

    /// The indexer's address.
    pub indexer_id: IndexerId,

    /// The indexer's URL.
    pub indexer_url: Url,

    /// The agreement duration.
    pub duration: Duration,
}

impl sqlx::FromRow<'_, PgRow> for IndexingAgreement {
    fn from_row(row: &'_ PgRow) -> Result<Self, Error> {
        // Parse the indexer ID column
        let indexer_id = {
            let indexer_id: String = row.try_get("indexer_id")?;
            let indexer_id: Address = indexer_id
                .parse()
                .map_err(|err| Error::Decode(Box::new(err)))?;
            IndexerId::new(indexer_id)
        };

        // Parse the indexer URL column
        let indexer_url = {
            let indexer_url: String = row.try_get("indexer_url")?;
            Url::from_str(&indexer_url).map_err(|err| Error::Decode(Box::new(err)))?
        };

        // Parse the duration column
        let duration = {
            let duration: i64 = row.try_get("duration")?;
            let duration: u64 = duration
                .try_into()
                .map_err(|err| Error::Decode(Box::new(err)))?;
            Duration::from_secs(duration)
        };

        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            status: row.try_get("status")?,
            indexing_request_id: row.try_get("indexing_request_id")?,
            indexer_id,
            indexer_url,
            duration,
        })
    }
}

/// The status of the [`IndexingAgreement`].
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
    /// The [`IndexingAgreement`] was created, but has not been sent to the indexer, yet.
    #[default]
    Created = -1,

    /// The [`IndexingAgreement`] was registered, but the agreement request failed.
    ///
    /// This is a terminal state.
    DeliveryFailed = 1,

    /// The [`IndexingAgreement`] is in effect.
    ///
    /// The indexer responded back accepting the agreement.
    Accepted = 0,

    /// The [`IndexingAgreement`] was rejected.
    ///
    /// The indexer responded back rejecting the agreement.
    ///
    /// This is a terminal state.
    Rejected = 2,

    /// The associated [`IndexingRequest`] got cancelled.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByRequester = 3,

    /// The indexer canceled the indexer agreement.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByIndexer = 4,

    /// The [`IndexingAgreement`] is expired.
    ///
    /// The indexer indexed the data and the agreement is no longer in effect.
    ///
    /// This is a terminal state.
    Expired = 5,

    /// A fallback for unknown status values.
    Unknown = i32::MAX,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Status::Created => "CREATED",
            Status::DeliveryFailed => "DELIVERY_FAILED",
            Status::Accepted => "ACCEPTED",
            Status::Rejected => "REJECTED",
            Status::CanceledByRequester => "CANCELED_BY_REQUESTER",
            Status::CanceledByIndexer => "CANCELED_BY_INDEXER",
            Status::Expired => "EXPIRED",
            Status::Unknown => "UNKNOWN",
        };
        f.write_str(status)
    }
}

impl From<i32> for Status {
    fn from(value: i32) -> Self {
        num_traits::FromPrimitive::from_i32(value).unwrap_or(Status::Unknown)
    }
}
