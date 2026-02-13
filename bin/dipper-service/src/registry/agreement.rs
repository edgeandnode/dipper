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

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId, U256},
};
use time::OffsetDateTime;
use url::Url;

use super::result::Result as RegistryResult;

#[async_trait]
pub trait AgreementRegistry {
    /// Get agreement by ID.
    async fn get_indexing_agreement_by_id(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<Option<IndexingAgreement>>;

    /// Get all agreements by deployment ID.
    async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get all agreements by indexer ID.
    async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get aggregated deployment-to-indexers mapping for active agreements.
    ///
    /// Returns agreements that are in `Created` or `AcceptedOnChain` status
    /// for any of the provided indexer IDs, grouped by deployment. This performs database-side
    /// aggregation, returning only the deployment IDs and their associated indexer IDs rather
    /// than full agreement objects.
    ///
    /// Returns a map where keys are deployment IDs and values are lists of indexer IDs
    /// that have active agreements for that deployment.
    async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>;

    /// Get declined indexers grouped by deployment within a lookback period.
    ///
    /// Returns indexers with `CanceledByIndexer` or `Expired` status within the
    /// specified number of days, grouped by deployment. This is used to avoid
    /// re-offering agreements to indexers that recently declined or let the
    /// deadline pass without accepting.
    ///
    /// Returns a map where keys are deployment IDs and values are lists of indexer IDs
    /// that declined agreements for that deployment.
    async fn get_declined_indexers_by_deployment(
        &self,
        lookback_days: i32,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>;

    /// Get all agreements by associated indexing request ID.
    async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Get the active agreements for an indexing request.
    ///
    /// Agreements are considered active if they are in `CREATED` or `ACCEPTED_ON_CHAIN` status.
    async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Register a new indexing agreement.
    async fn register_new_indexing_agreement(
        &self,
        request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        indexer_id: IndexerId,
        indexer_url: Url,
        voucher: Voucher,
    ) -> RegistryResult<IndexingAgreementId>;

    /// Mark an indexing agreement as `DELIVERY_FAILED`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_delivery_failed(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Mark an indexing agreement as `CANCELED_BY_REQUESTER`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` or `ACCEPTED_ON_CHAIN` state, this method returns a
    /// [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Mark an indexing agreement as `CANCELED_BY_INDEXER`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `ACCEPTED_ON_CHAIN` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated)
    /// error.
    async fn mark_indexing_agreement_as_canceled_by_indexer(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Mark an indexing agreement as `ACCEPTED_ON_CHAIN`.
    ///
    /// The on-chain `IndexingAgreementAccepted` event was observed for this agreement.
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_accepted_on_chain(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;

    /// Get `Created` agreements whose RCA deadline has passed.
    ///
    /// These agreements are eligible for expiration since the indexer can no longer
    /// accept on-chain. Results are ordered by deadline ascending (oldest first).
    async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>>;

    /// Mark an indexing agreement as `EXPIRED`.
    ///
    /// The RCA deadline passed without on-chain acceptance.
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_expired(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()>;
}

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

/// The _indexing agreement_ indexer information.
#[derive(Debug, Clone)]
pub struct Indexer {
    /// The indexer's ID (ETH address).
    pub id: IndexerId,
    /// The indexer's URL.
    pub url: Url,
}

/// The agreement terms. Field names align with the on-chain `RecurringCollectionAgreement`.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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

/// The status of the [`IndexingAgreement`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub enum Status {
    /// The [`IndexingAgreement`] was created, but has not been sent to the indexer, yet.
    #[default]
    Created,

    /// The [`IndexingAgreement`] was registered, but the agreement request failed.
    ///
    /// This is a terminal state.
    DeliveryFailed,

    /// The associated [`IndexingRequest`] got cancelled.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByRequester,

    /// The indexer canceled the indexer agreement.
    ///
    /// The [`IndexingAgreement`] is cancelled and no longer in effect.
    ///
    /// This is a terminal state.
    CanceledByIndexer,

    /// The [`IndexingAgreement`] is expired.
    ///
    /// The indexer indexed the data and the agreement is no longer in effect.
    ///
    /// This is a terminal state.
    Expired,

    /// The [`IndexingAgreement`] was accepted on-chain.
    ///
    /// The on-chain `IndexingAgreementAccepted` event was observed for this agreement.
    AcceptedOnChain,
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
        };
        f.write_str(status)
    }
}

impl TryFrom<dipper_pgregistry::IndexingAgreement> for IndexingAgreement {
    type Error = anyhow::Error;

    fn try_from(value: dipper_pgregistry::IndexingAgreement) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            status: match value.status {
                dipper_pgregistry::IndexingAgreementStatus::Created => Status::Created,
                dipper_pgregistry::IndexingAgreementStatus::DeliveryFailed => {
                    Status::DeliveryFailed
                }
                dipper_pgregistry::IndexingAgreementStatus::CanceledByRequester => {
                    Status::CanceledByRequester
                }
                dipper_pgregistry::IndexingAgreementStatus::CanceledByIndexer => {
                    Status::CanceledByIndexer
                }
                dipper_pgregistry::IndexingAgreementStatus::Expired => Status::Expired,
                dipper_pgregistry::IndexingAgreementStatus::AcceptedOnChain => {
                    Status::AcceptedOnChain
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid status: {:?}", value.status));
                }
            },
            indexing_request_id: value.indexing_request_id,
            indexer: value.indexer.into(),
            voucher: value.voucher.into(),
        })
    }
}

impl From<dipper_pgregistry::IndexingAgreementIndexer> for Indexer {
    fn from(value: dipper_pgregistry::IndexingAgreementIndexer) -> Self {
        Self {
            id: value.id,
            url: value.url,
        }
    }
}

impl From<dipper_pgregistry::IndexingAgreementVoucher> for Voucher {
    fn from(value: dipper_pgregistry::IndexingAgreementVoucher) -> Self {
        Self {
            payer: value.payer,
            service_provider: value.service_provider,
            data_service: value.data_service,
            deadline: value.deadline,
            ends_at: value.ends_at,
            max_initial_tokens: value.max_initial_tokens,
            max_ongoing_tokens_per_second: value.max_ongoing_tokens_per_second,
            min_seconds_per_collection: value.min_seconds_per_collection,
            max_seconds_per_collection: value.max_seconds_per_collection,
            metadata: value.metadata.into(),
        }
    }
}

impl From<dipper_pgregistry::IndexingAgreementVoucherMetadata> for VoucherMetadata {
    fn from(value: dipper_pgregistry::IndexingAgreementVoucherMetadata) -> Self {
        Self {
            tokens_per_second: value.tokens_per_second,
            tokens_per_entity_per_second: value.tokens_per_entity_per_second,
            subgraph_deployment_id: value.subgraph_deployment_id,
            protocol_network: value.protocol_network,
            chain_id: value.chain_id,
        }
    }
}

impl From<Voucher> for dipper_pgregistry::IndexingAgreementVoucher {
    fn from(value: Voucher) -> Self {
        Self {
            payer: value.payer,
            service_provider: value.service_provider,
            data_service: value.data_service,
            deadline: value.deadline,
            ends_at: value.ends_at,
            max_initial_tokens: value.max_initial_tokens,
            max_ongoing_tokens_per_second: value.max_ongoing_tokens_per_second,
            min_seconds_per_collection: value.min_seconds_per_collection,
            max_seconds_per_collection: value.max_seconds_per_collection,
            metadata: value.metadata.into(),
        }
    }
}

impl From<VoucherMetadata> for dipper_pgregistry::IndexingAgreementVoucherMetadata {
    fn from(value: VoucherMetadata) -> Self {
        Self {
            tokens_per_second: value.tokens_per_second,
            tokens_per_entity_per_second: value.tokens_per_entity_per_second,
            subgraph_deployment_id: value.subgraph_deployment_id,
            protocol_network: value.protocol_network,
            chain_id: value.chain_id,
        }
    }
}

/// The _indexing agreement_ [`fake`] implementation for test data generation.
#[cfg(test)]
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
