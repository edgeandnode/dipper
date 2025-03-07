//! # Indexing Receipt
//!
//! The indexing receipt tracks redeemable receipts associated with a given indexing agreement.
//!
//! An indexer must provide the Proof-of-Indexing signed by the one of the allocations.

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId};
use thegraph_core::{
    AllocationId, IndexerId, ProofOfIndexing,
    alloy::primitives::{Address, U256},
};
use time::OffsetDateTime;

use super::result::Result as RegistryResult;

#[async_trait]
pub trait ReceiptRegistry {
    /// Register a new indexing receipt.
    async fn register_new_indexing_receipt(
        &self,
        agreement_id: IndexingAgreementId,
        indexer_id: IndexerId,
        indexer_operator_id: Address,
        reported_work: ReportedWork,
        amount: U256,
    ) -> RegistryResult<IndexingReceiptId>;

    /// Get the latest receipt for the given agreement ID.
    async fn get_last_receipt_for_agreement_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<Option<IndexingReceipt>>;
}

/// An Indexing Receipt represents the redeemable receipt associated with a given indexing
/// agreement.
///
/// The [`IndexingReceipt`] is as a Data Transfer Object (DTO).
#[derive(Debug, Clone)]
pub struct IndexingReceipt {
    /// The _indexing receipt_ ID
    pub id: IndexingReceiptId,
    /// The _indexing receipt_ creation time
    pub created_at: OffsetDateTime,
    /// The _indexing receipt_ update time
    pub updated_at: OffsetDateTime,

    /// The _indexing agreement_ associated with the _indexing receipt_
    pub indexing_agreement_id: IndexingAgreementId,
    /// The indexer ID that collected the _indexing receipt_
    pub indexer_id: IndexerId,
    /// The indexer operator address that collected the _indexing receipt_
    pub indexer_operator_id: Address,

    /// The work reported by the indexer
    pub reported_work: ReportedWork,
    /// The _indexing receipt_ amount in _wei GRT_
    pub amount: U256,
}

/// The _indexing receipt_ information reported by the indexer.
///
/// It is used to calculate the amount of GRT tokens that the indexer can redeem.
#[derive(Debug, Clone)]
pub struct ReportedWork {
    /// The collection epoch.
    ///
    /// This is the epoch timestamp for this collection.
    pub epoch: u32,

    /// The allocation ID that the indexer reported work for.
    pub allocation_id: AllocationId,

    /// The number of entities stored.
    ///
    /// This is the absolute number of subgraph entities stored, not the number of entities stored
    /// since the last collection.
    pub entity_count: u64,

    /// The Proof-Of-Indexing (POI) provided by the indexer when collecting the _indexing receipt_.
    pub poi: ProofOfIndexing,
}

impl From<dipper_pgregistry::IndexingReceiptReportedWork> for ReportedWork {
    fn from(value: dipper_pgregistry::IndexingReceiptReportedWork) -> Self {
        Self {
            epoch: value.epoch,
            allocation_id: value.allocation_id,
            entity_count: value.entity_count,
            poi: value.poi,
        }
    }
}

impl From<ReportedWork> for dipper_pgregistry::IndexingReceiptReportedWork {
    fn from(value: ReportedWork) -> Self {
        Self {
            epoch: value.epoch,
            allocation_id: value.allocation_id,
            entity_count: value.entity_count,
            poi: value.poi,
        }
    }
}

impl From<dipper_pgregistry::IndexingReceipt> for IndexingReceipt {
    fn from(value: dipper_pgregistry::IndexingReceipt) -> Self {
        Self {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            indexing_agreement_id: value.indexing_agreement_id,
            indexer_id: value.indexer_id,
            indexer_operator_id: value.indexer_operator_id,
            reported_work: value.reported_work.into(),
            amount: value.amount,
        }
    }
}

/// The _indexing receipt_ [`fake`] implementation for test data generation.
#[cfg(test)]
pub mod fake_impl {
    use fake::{Dummy, Faker, Rng};

    use super::*;

    impl Dummy<Faker> for ReportedWork {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            ReportedWork {
                epoch: u32::dummy_with_rng(config, rng),
                allocation_id: AllocationId::dummy_with_rng(config, rng),
                entity_count: u64::dummy_with_rng(config, rng),
                poi: ProofOfIndexing::dummy_with_rng(config, rng),
            }
        }
    }
}
