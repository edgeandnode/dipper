//! # Indexing Receipt
//!
//! The indexing receipt tracks redeemable receipts associated with a given indexing agreement.
//!
//! An indexer must provide the Proof-of-Indexing signed by the one of the allocations.

use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId};
use sqlx::{postgres::PgRow, Error, Row as _};
use thegraph_core::{
    alloy::primitives::{Address, B256},
    AllocationId, ProofOfIndexing,
};
use time::OffsetDateTime;

/// An Indexing Receipt represents the redeemable receipt associated with a given indexing
/// agreement.
///
/// The [`IndexingReceipt`] is as a Data Transfer Object (DTO).
#[derive(Debug, Clone)]
pub struct IndexingReceipt {
    /// The indexing receipt ID
    pub id: IndexingReceiptId,

    /// The indexing receipt creation time
    pub created_at: OffsetDateTime,

    /// The indexing receipt update time
    pub updated_at: OffsetDateTime,

    /// The indexing agreement associated with the receipt
    pub indexing_agreement_id: IndexingAgreementId,

    /// The allocation address associated with the receipt
    pub allocation_id: AllocationId,

    /// The indexing receipt fee
    pub fee: i64, // TODO: Review this 'fee' field

    /// The Proof-Of-Indexing, POI, provided by the indexer when redeeming the receipt
    ///
    /// If the POI is present, the receipt is considered redeemed.
    pub poi: Option<ProofOfIndexing>,
}

impl sqlx::FromRow<'_, PgRow> for IndexingReceipt {
    fn from_row(row: &'_ PgRow) -> Result<Self, Error> {
        // Parse the allocation ID column
        let allocation_id = {
            let allocation_id: String = row.try_get("allocation_id")?;
            let allocation_id: Address = allocation_id
                .parse()
                .map_err(|err| Error::Decode(Box::new(err)))?;
            AllocationId::new(allocation_id)
        };

        // Parse the Proof-Of-Indexing column
        let poi = {
            let poi: Option<String> = row.try_get("poi")?;
            match poi {
                None => None,
                Some(poi) => {
                    let poi: B256 = poi.parse().map_err(|err| Error::Decode(Box::new(err)))?;
                    Some(ProofOfIndexing::new(poi))
                }
            }
        };

        Ok(Self {
            id: row.try_get("id")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            indexing_agreement_id: row.try_get("indexing_agreement_id")?,
            allocation_id,
            fee: row.try_get("fee")?,
            poi,
        })
    }
}
