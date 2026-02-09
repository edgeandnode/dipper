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

    impl Dummy<Faker> for Voucher {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                payer: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                service_provider: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                data_service: Address::new(<[u8; 20]>::dummy_with_rng(config, rng)),
                deadline: u64::dummy_with_rng(config, rng),
                ends_at: u64::dummy_with_rng(config, rng),
                max_initial_tokens: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng)),
                max_ongoing_tokens_per_second: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(
                    config, rng,
                )),
                min_seconds_per_collection: u32::dummy_with_rng(config, rng),
                max_seconds_per_collection: u32::dummy_with_rng(config, rng),
                metadata: VoucherMetadata::dummy_with_rng(config, rng),
            }
        }
    }

    impl Dummy<Faker> for VoucherMetadata {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            Self {
                tokens_per_second: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng)),
                tokens_per_entity_per_second: U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(
                    config, rng,
                )),
                subgraph_deployment_id: DeploymentId::dummy_with_rng(config, rng),
                protocol_network: ChainId::dummy_with_rng(config, rng),
                chain_id: ChainId::dummy_with_rng(config, rng),
            }
        }
    }
}
