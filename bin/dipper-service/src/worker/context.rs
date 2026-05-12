use std::{collections::BTreeMap, sync::Arc};

use dipper_core::state::FromState;
use dipper_iisa::FallbackFilter;
use graph_networks_registry::NetworksRegistry;
use thegraph_core::alloy::primitives::ChainId;
use tokio::sync::Notify;

use super::handlers::{
    CancelRejectedAgreementOnChainCtx, ReassessIndexingRequestCtx,
    SendIndexingAgreementProposalCtx, SubmitOfferCtx,
};
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    network::service::entity_count_cache::EntityCountCache,
    signing::eip712::Eip712Signer,
};

/// Generates a `FromState<InnerCtx<...>>` impl for a handler context type.
///
/// The macro maps InnerCtx fields to handler context fields, supporting field renaming.
///
/// Syntax: `impl_from_state!(TargetType<generics> { field_mappings })`
///
/// Field mappings can be:
/// - `field` - maps `state.field` to `self.field`
/// - `target_field: source_field` - maps `state.source_field` to `self.target_field`
macro_rules! impl_from_state {
    (
        $target:ident < $($gen:ident),* > {
            $( $field:ident $(: $source:ident)? ),* $(,)?
        }
    ) => {
        impl<R, N, W, C, I, T> FromState<InnerCtx<R, N, W, C, I, T>>
            for $target < $($gen),* >
        where
            $( $gen: Clone, )*
        {
            #[inline]
            fn from_state(state: &InnerCtx<R, N, W, C, I, T>) -> Self {
                Self {
                    $( $field: impl_from_state!(@clone state, $field $(, $source)?), )*
                }
            }
        }
    };

    // Clone from a renamed source field
    (@clone $state:ident, $field:ident, $source:ident) => {
        $state.$source.clone()
    };

    // Clone from a field with the same name
    (@clone $state:ident, $field:ident) => {
        $state.$field.clone()
    };
}

/// The worker context
///
/// This is a input context for the worker service
#[derive(Clone)]
pub struct Ctx<Q, R, N, C, I, T> {
    /// The message queue worker
    pub queue: Q,

    /// The EIP-712 signer
    pub signer: Arc<Eip712Signer>,

    /// The _indexing agreement_ configuration
    pub agreement_conf: Arc<IndexingAgreementConfig>,

    /// The _indexing agreement_ per-chain pricing table
    pub pricing_table: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,

    /// The DIPs registry
    pub registry: R,

    /// The Network provider
    pub network: N,

    /// The indexer client
    pub client: C,

    /// The Indexing Indexer Selection Algorithm (IISA) service
    pub iisa: I,

    /// The chain client for on-chain transactions
    pub chain_client: T,

    /// The fallback filter for direct indexer /dips/info queries
    pub fallback_filter: Arc<FallbackFilter>,

    /// The graph networks registry (maps chain IDs to network names)
    pub networks_registry: Arc<NetworksRegistry>,

    /// Additional chain ID to network name mappings for dev/test chains
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,

    /// Shared entity count cache for optimistic fee estimation
    pub entity_count_cache: EntityCountCache,

    /// Wakes the chain_listener when proposals are dispatched
    pub chain_listener_notify: Arc<Notify>,
}

/// The inner worker context.
///
/// This is a shared context across all message handlers.
#[derive(Clone)]
pub(super) struct InnerCtx<R, N, W, C, I, T> {
    /// The EIP-712 signer
    pub signer: Arc<Eip712Signer>,

    /// The _indexing agreement_ configuration
    pub agreement_conf: Arc<IndexingAgreementConfig>,

    /// The _indexing agreement_ per-chain pricing table
    pub pricing_table: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,

    /// The DIPs registry
    pub registry: R,

    /// The Network provider
    pub network: N,

    /// The message queue worker
    pub worker: W,

    /// The indexer client
    pub client: C,

    /// The Indexing Indexer Selection Algorithm (IISA) service
    pub iisa: I,

    /// The chain client for on-chain transactions
    pub chain_client: T,

    /// The fallback filter for direct indexer /dips/info queries
    pub fallback_filter: Arc<FallbackFilter>,

    /// The graph networks registry (maps chain IDs to network names)
    pub networks_registry: Arc<NetworksRegistry>,

    /// Additional chain ID to network name mappings for dev/test chains
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,

    /// Shared entity count cache for optimistic fee estimation
    pub entity_count_cache: EntityCountCache,

    /// Wakes the chain_listener when proposals are dispatched
    pub chain_listener_notify: Arc<Notify>,
}

impl_from_state!(ReassessIndexingRequestCtx<R, N, W, I, T> {
    signer,
    agreement_conf,
    chain_price: pricing_table,
    registry,
    network,
    queue: worker,
    iisa,
    chain_client,
    networks_registry,
    additional_networks,
    entity_count_cache,
    chain_listener_notify,
});

impl_from_state!(SendIndexingAgreementProposalCtx<R, W, C> {
    registry,
    queue: worker,
    indexer_client: client,
});

impl_from_state!(CancelRejectedAgreementOnChainCtx<R, T> {
    registry,
    chain_client,
});

impl_from_state!(SubmitOfferCtx<R, T> {
    registry,
    chain_client,
});
