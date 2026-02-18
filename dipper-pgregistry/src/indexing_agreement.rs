//! # Indexing Agreements
//!
//! Indexer Agreements MUST be associated with one Indexing Request, and represent the contract
//! between the DIPs Gateway (Dipper) and the indexer to index the data.
//!
//! - An agreement MUST be associated with an *indexing request*.
//! - An agreement is in effect once accepted on-chain, or until the RCA deadline expires.
//!   It can be cancelled by the customer or the indexer.
//!
//! An Indexer Agreement is created every time the Dipper runs the *Indexing Indexer Selection
//! Algorithm (IISA)* and finds an indexer to fulfill the *indexing request*.

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId, U256},
};
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

    /// The indexer.
    pub indexer: Indexer,

    /// The agreement voucher.
    ///
    /// It contains the agreement terms and conditions.
    pub voucher: Voucher,

    /// The last observed block height for the subgraph deployment.
    ///
    /// `None` until the first liveness check fires for this agreement.
    pub last_block_height: Option<u64>,

    /// When the block height was last observed to change (progress or resync).
    ///
    /// `None` until the first liveness check fires for this agreement.
    pub last_progress_at: Option<OffsetDateTime>,
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

    /// The [`IndexingAgreement`] was accepted on-chain.
    ///
    /// The on-chain `IndexingAgreementAccepted` event was observed for this agreement.
    AcceptedOnChain = 6,

    /// The indexer rejected the agreement proposal off-chain.
    ///
    /// The indexer may still accept on-chain before the deadline. If they do,
    /// Dipper will cancel the agreement via `cancelIndexingAgreementByPayer`.
    Rejected = 7,

    /// The liveness checker detected no indexing progress within the tolerance window.
    ///
    /// Dipper canceled the agreement via `cancelIndexingAgreementByPayer` and will
    /// trigger reassignment to find a replacement indexer.
    ///
    /// This is a terminal state.
    AbandonedByIndexer = 8,

    /// A fallback for unknown status values.
    Unknown = i32::MAX,
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Status::Created => "CREATED",
            Status::DeliveryFailed => "DELIVERY_FAILED",
            Status::CanceledByRequester => "CANCELED_BY_REQUESTER",
            Status::CanceledByIndexer => "CANCELED_BY_INDEXER",
            Status::Expired => "EXPIRED",
            Status::AcceptedOnChain => "ACCEPTED_ON_CHAIN",
            Status::Rejected => "REJECTED",
            Status::AbandonedByIndexer => "ABANDONED_BY_INDEXER",
            Status::Unknown => "UNKNOWN",
        };
        f.write_str(status)
    }
}

/// The _indexing agreement_ indexer information.
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer's ID (ETH address)
    pub id: IndexerId,
    /// The indexer's URL
    pub url: Url,
}

/// The agreement terms, stored as JSON in the `voucher` column.
///
/// Field names align with the on-chain `RecurringCollectionAgreement` type.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Voucher {
    /// The agreement payer (signer address).
    pub payer: Address,
    /// The indexer (service provider).
    pub service_provider: Address,
    /// The data service address (SubgraphService contract).
    pub data_service: Address,

    /// Deadline for on-chain acceptance (unix timestamp).
    pub deadline: u64,
    /// When the agreement expires (unix timestamp).
    pub ends_at: u64,

    /// Maximum tokens for the initial subgraph sync.
    pub max_initial_tokens: U256,
    /// Maximum tokens per second for ongoing indexing.
    pub max_ongoing_tokens_per_second: U256,

    /// Minimum seconds per collection.
    pub min_seconds_per_collection: u32,
    /// Maximum seconds per collection.
    pub max_seconds_per_collection: u32,

    /// The agreement metadata.
    pub metadata: VoucherMetadata,
}

/// Pricing and deployment metadata for the agreement.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VoucherMetadata {
    /// Tokens per second (base rate) in wei GRT.
    pub tokens_per_second: U256,
    /// Tokens per entity per second in wei GRT.
    pub tokens_per_entity_per_second: U256,

    /// The Subgraph deployment ID to index.
    pub subgraph_deployment_id: DeploymentId,

    /// The protocol network, e.g. `eip155:42161` (Arbitrum).
    pub protocol_network: ChainId,
    /// Indexed chain, e.g., `eip155:1` (Ethereum Mainnet).
    pub chain_id: ChainId,
}

/// The _indexing agreement_ [`fake`] implementation for test data generation.
#[cfg(feature = "fake")]
pub mod fake_impl {
    use fake::{Dummy, Faker, Rng};

    use super::*;

    impl Dummy<Faker> for Indexer {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                id: IndexerId::dummy_with_rng(config, rng),
                url: Url::dummy_with_rng(config, rng),
            }
        }
    }

    /// Generate a random u64 that fits within PostgreSQL bigint range.
    /// PostgreSQL bigint is signed 64-bit, max value is 2^63 - 1.
    fn bigint_safe_u64<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> u64 {
        i64::dummy_with_rng(config, rng).unsigned_abs()
    }

    /// Generate a random U256 that fits within PostgreSQL bigint range.
    fn bigint_safe_u256<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> U256 {
        U256::from(bigint_safe_u64(config, rng))
    }

    impl Dummy<Faker> for Voucher {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                payer: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                service_provider: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                data_service: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                // deadline and ends_at are cast to bigint in queries, so constrain them
                deadline: bigint_safe_u64(config, rng),
                ends_at: bigint_safe_u64(config, rng),
                max_initial_tokens: bigint_safe_u256(config, rng),
                max_ongoing_tokens_per_second: bigint_safe_u256(config, rng),
                min_seconds_per_collection: u32::dummy_with_rng(config, rng),
                max_seconds_per_collection: u32::dummy_with_rng(config, rng),
                metadata: VoucherMetadata::dummy_with_rng(config, rng),
            }
        }
    }

    impl Dummy<Faker> for VoucherMetadata {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                tokens_per_second: bigint_safe_u256(config, rng),
                tokens_per_entity_per_second: bigint_safe_u256(config, rng),
                subgraph_deployment_id: DeploymentId::dummy_with_rng(config, rng),
                protocol_network: ChainId::dummy_with_rng(config, rng),
                chain_id: ChainId::dummy_with_rng(config, rng),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;
    use thegraph_core::alloy::primitives::{U256, address};

    use super::*;

    #[test]
    fn test_voucher_serde_round_trip() {
        use std::str::FromStr;

        //* Arrange
        let voucher = Voucher {
            payer: address!("1111111111111111111111111111111111111111"),
            service_provider: address!("2222222222222222222222222222222222222222"),
            data_service: address!("3333333333333333333333333333333333333333"),
            deadline: 1234567890,
            ends_at: 9876543210,
            max_initial_tokens: U256::from(4096u64),
            max_ongoing_tokens_per_second: U256::from(512u64),
            min_seconds_per_collection: 60,
            max_seconds_per_collection: 3600,
            metadata: VoucherMetadata {
                tokens_per_second: U256::from(10u64),
                tokens_per_entity_per_second: U256::from(2u64),
                subgraph_deployment_id: DeploymentId::from_str(
                    "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9",
                )
                .unwrap(),
                protocol_network: 42161,
                chain_id: 1,
            },
        };

        //* Act - Serialize to JSON
        let json = serde_json::to_string(&voucher).expect("serialization failed");

        //* Act - Deserialize from JSON
        let deserialized: Voucher = serde_json::from_str(&json).expect("deserialization failed");

        //* Assert - Field-by-field comparison
        assert_eq!(deserialized.payer, voucher.payer, "payer mismatch");
        assert_eq!(
            deserialized.service_provider, voucher.service_provider,
            "service_provider mismatch"
        );
        assert_eq!(
            deserialized.data_service, voucher.data_service,
            "data_service mismatch"
        );
        assert_eq!(deserialized.deadline, voucher.deadline, "deadline mismatch");
        assert_eq!(deserialized.ends_at, voucher.ends_at, "ends_at mismatch");
        assert_eq!(
            deserialized.max_initial_tokens, voucher.max_initial_tokens,
            "max_initial_tokens mismatch"
        );
        assert_eq!(
            deserialized.max_ongoing_tokens_per_second, voucher.max_ongoing_tokens_per_second,
            "max_ongoing_tokens_per_second mismatch"
        );
        assert_eq!(
            deserialized.min_seconds_per_collection, voucher.min_seconds_per_collection,
            "min_seconds_per_collection mismatch"
        );
        assert_eq!(
            deserialized.max_seconds_per_collection, voucher.max_seconds_per_collection,
            "max_seconds_per_collection mismatch"
        );

        // Assert metadata fields
        assert_eq!(
            deserialized.metadata.tokens_per_second, voucher.metadata.tokens_per_second,
            "tokens_per_second mismatch"
        );
        assert_eq!(
            deserialized.metadata.tokens_per_entity_per_second,
            voucher.metadata.tokens_per_entity_per_second,
            "tokens_per_entity_per_second mismatch"
        );
        assert_eq!(
            deserialized.metadata.subgraph_deployment_id, voucher.metadata.subgraph_deployment_id,
            "subgraph_deployment_id mismatch"
        );
        assert_eq!(
            deserialized.metadata.protocol_network, voucher.metadata.protocol_network,
            "protocol_network mismatch"
        );
        assert_eq!(
            deserialized.metadata.chain_id, voucher.metadata.chain_id,
            "chain_id mismatch"
        );
    }
}
