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

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{
    alloy::primitives::{Address, ChainId, U256},
    DeploymentId, IndexerId,
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

    /// The epoch when the agreement was accepted.
    pub accepted_at_epoch: Option<u32>,

    /// The indexing agreement associated indexing request
    pub indexing_request_id: IndexingRequestId,

    /// The indexer.
    pub indexer: Indexer,

    /// The agreement voucher.
    ///
    /// It contains the agreement terms and conditions.
    pub voucher: Voucher,
}

/// The _indexing agreement_ indexer information.
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer's ID (ETH address).
    pub id: IndexerId,
    /// The indexer's URL.
    pub url: Url,
}

/// The _indexing agreement_ proposal voucher.
#[derive(Debug, Clone)]
pub struct Voucher {
    /// The agreement payer.
    ///
    /// It should coincide with the voucher signer address.
    pub payer: Address,
    /// The voucher recipient address. The indexer ID.
    pub recipient: Address,
    /// Data service that will initiate the payment collection.
    pub service: Address,

    /// The duration of the agreement in epochs.
    pub duration_epochs: u32,

    /// The maximum amount, in _wei GRT_, that can be collected for the initial subgraph sync.
    pub max_initial_amount: U256,
    /// The maximum amount, in _wei GRT_, that can be collected per epoch (after the initial sync).
    pub max_ongoing_amount_per_epoch: U256,

    /// The minimum number of epochs that can be collected at once.
    pub min_epochs_per_collection: u32,
    /// The maximum number of epochs that can be collected at once.
    pub max_epochs_per_collection: u32,

    /// The deadline for the indexer to accept the agreement.
    // TODO(v2): Review this
    pub deadline: u64,

    /// The voucher metadata
    pub metadata: VoucherMetadata,
}

/// The _indexing agreement_ proposal voucher metadata
#[derive(Debug, Clone)]
pub struct VoucherMetadata {
    /// The base price per epoch in _wei GRT_.
    pub base_price_per_epoch: U256,
    /// The price per entity in _wei GRT_.
    pub price_per_entity: U256,

    /// The Subgraph deployment ID to index.
    pub subgraph_deployment_id: DeploymentId,

    /// The protocol network, e.g. `eip155:42161` (Arbitrum).
    pub protocol_network: ChainId,
    /// Indexed chain, e.g., `eip155:1` (Ethereum Mainnet).
    pub chain_id: ChainId,
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
            let payer = Address::new(<[u8; 20]>::dummy_with_rng(config, rng));
            let recipient = Address::new(<[u8; 20]>::dummy_with_rng(config, rng));
            let service = Address::new(<[u8; 20]>::dummy_with_rng(config, rng));

            let duration_epochs = u32::dummy_with_rng(config, rng);

            let max_initial_amount = U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng));
            let max_ongoing_amount_per_epoch =
                U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng));

            let max_epochs_per_collection = u32::dummy_with_rng(config, rng);
            let min_epochs_per_collection = u32::dummy_with_rng(config, rng);

            let deadline = u64::dummy_with_rng(config, rng);

            let metadata = VoucherMetadata::dummy_with_rng(config, rng);

            Self {
                payer,
                recipient,
                service,
                duration_epochs,
                max_initial_amount,
                max_ongoing_amount_per_epoch,
                max_epochs_per_collection,
                min_epochs_per_collection,
                deadline,
                metadata,
            }
        }
    }

    impl Dummy<Faker> for VoucherMetadata {
        fn dummy_with_rng<R: Rng + ?Sized>(config: &Faker, rng: &mut R) -> Self {
            let base_price_per_epoch = U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng));
            let price_per_entity = U256::from_be_bytes(<[u8; 32]>::dummy_with_rng(config, rng));
            let subgraph_deployment_id = DeploymentId::dummy_with_rng(config, rng);
            let protocol_network = ChainId::dummy_with_rng(config, rng);
            let chain_id = ChainId::dummy_with_rng(config, rng);

            Self {
                base_price_per_epoch,
                price_per_entity,
                subgraph_deployment_id,
                protocol_network,
                chain_id,
            }
        }
    }
}
