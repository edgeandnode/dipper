use std::{collections::BTreeMap, sync::Arc};

use thegraph_core::alloy::primitives::{Address, ChainId, U256};

use crate::{config, signer::PrivateKeyEip712Signer};

/// Context is a struct that holds all the dependencies that a worker needs to run.
#[derive(Clone)]
pub struct Context<Q, N, R, C, I> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub queue: Q,
    pub network: N,
    pub registry: R,
    pub indexer_client: C,
    pub iisa: I,
}

/// The _indexing agreement_ configuration.
///
/// It holds the configuration for the _indexing agreements_, e.g., the service address, the
/// maximum amount that can be collected for the subgraph initial sync, the maximum amount
/// collectable per epoch, etc.
#[derive(Debug)]
pub struct IndexingAgreementConfig {
    /// The _indexing agreement_'s service address.
    service: Address,
    /// The _indexing agreement_'s maximum amount that can be collected for the subgraph initial
    /// sync.
    max_initial_amount: U256,
    /// The _indexing agreement_'s maximum amount collectable per epoch.
    max_ongoing_amount_per_epoch: U256,
    /// The _indexing agreement_'s maximum epochs per collection.
    max_epochs_per_collection: u32,
    /// The _indexing agreement_'s minimum epochs per collection.
    min_epochs_per_collection: u32,
    /// The _indexing agreement_'s duration in epochs.
    duration_epochs: Option<u32>,

    /// The _indexing agreement_'s per chain pricing table.
    pricing_table: BTreeMap<ChainId, IndexingAgreementChainPrices>,
}

impl IndexingAgreementConfig {
    /// Get the _indexing agreement_'s service address.
    pub fn service(&self) -> Address {
        self.service
    }

    /// Get the _indexing agreement_'s maximum amount that can be collected for the subgraph initial
    /// sync.
    pub fn max_initial_amount(&self) -> U256 {
        self.max_initial_amount
    }

    /// Get the _indexing agreement_'s maximum amount collectable per epoch.
    pub fn max_ongoing_amount_per_epoch(&self) -> U256 {
        self.max_ongoing_amount_per_epoch
    }

    /// Get the _indexing agreement_'s maximum epochs per collection.
    pub fn max_epochs_per_collection(&self) -> u32 {
        self.max_epochs_per_collection
    }

    /// Get the _indexing agreement_'s minimum epochs per collection.
    pub fn min_epochs_per_collection(&self) -> u32 {
        self.min_epochs_per_collection
    }

    /// Get the _indexing agreement_'s duration in epochs.
    pub fn duration_epochs(&self) -> u32 {
        self.duration_epochs.unwrap_or(u32::MAX)
    }

    /// Get the chain-specific pricing for the given chain.
    ///
    /// If no pricing is available for the chain, an error is returned.
    pub fn chain_price(&self, chain_id: &ChainId) -> anyhow::Result<&IndexingAgreementChainPrices> {
        self.pricing_table.get(chain_id).ok_or(anyhow::anyhow!(
            "No pricing information for chain {chain_id}"
        ))
    }
}

impl From<config::DipsAgreementConfig> for IndexingAgreementConfig {
    fn from(value: config::DipsAgreementConfig) -> Self {
        Self {
            service: value.service,
            max_initial_amount: value.max_initial_amount,
            max_ongoing_amount_per_epoch: value.max_ongoing_amount_per_epoch,
            max_epochs_per_collection: value.max_epochs_per_collection,
            min_epochs_per_collection: value.min_epochs_per_collection,
            duration_epochs: value.duration_epochs,
            pricing_table: value
                .pricing_table
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

/// The _indexing agreement_'s per-chain prices.
#[derive(Debug)]
pub struct IndexingAgreementChainPrices {
    /// The price per block in wei GRT.
    pub price_per_block: U256,
    /// The price per entity in wei GRT per epoch.
    pub price_per_entity_per_epoch: U256,
}

impl From<config::ChainPrices> for IndexingAgreementChainPrices {
    fn from(value: config::ChainPrices) -> Self {
        Self {
            price_per_block: value.price_per_block,
            price_per_entity_per_epoch: value.price_per_entity_per_epoch,
        }
    }
}
