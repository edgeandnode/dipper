use std::{collections::BTreeMap, sync::Arc};

use dipper_core::state::FromState;
use thegraph_core::alloy::primitives::ChainId;

use super::handlers::{
    ProcessIndexingAgreementCancellationCtx, ProcessIndexingRequestCancellationCtx,
    ProcessNewIndexingRequestCtx, ReassessIndexingRequestCtx,
    SendIndexingAgreementCancellationCtx, SendIndexingAgreementProposalCtx,
};
use crate::{
    config::{IndexingAgreementChainPrices, IndexingAgreementConfig},
    signing::eip712::PrivateKeyEip712Signer,
};

/// The worker context
///
/// This is a input context for the worker service
#[derive(Clone)]
pub struct Ctx<Q, R, N, C, I> {
    /// The message queue worker
    pub queue: Q,

    /// The EIP-712 signer
    pub signer: Arc<PrivateKeyEip712Signer>,

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
}

/// The inner worker context.
///
/// This is a shared context across all message handlers.
#[derive(Clone)]
pub(super) struct InnerCtx<R, N, W, C, I> {
    /// The EIP-712 signer
    pub signer: Arc<PrivateKeyEip712Signer>,

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
}

impl<R, N, W, C, I> FromState<InnerCtx<R, N, W, C, I>>
    for ReassessIndexingRequestCtx<R, N, W, I>
where
    R: Clone,
    N: Clone,
    W: Clone,
    I: Clone,
{
    #[inline]
    fn from_state(state: &InnerCtx<R, N, W, C, I>) -> Self {
        Self {
            signer: state.signer.clone(),
            agreement_conf: state.agreement_conf.clone(),
            chain_price: state.pricing_table.clone(),
            registry: state.registry.clone(),
            network: state.network.clone(),
            queue: state.worker.clone(),
            iisa: state.iisa.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<InnerCtx<R, N, W, C, I>>
    for SendIndexingAgreementCancellationCtx<R, C>
where
    R: Clone,
    C: Clone,
{
    #[inline]
    fn from_state(state: &InnerCtx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            indexer_client: state.client.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<InnerCtx<R, N, W, C, I>>
    for ProcessIndexingRequestCancellationCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    #[inline]
    fn from_state(state: &InnerCtx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            queue: state.worker.clone(),
        }
    }
}

impl<W, N, R, C, I> FromState<InnerCtx<R, N, W, C, I>> for ProcessNewIndexingRequestCtx<R, N, W, I>
where
    R: Clone,
    N: Clone,
    W: Clone,
    I: Clone,
{
    #[inline]
    fn from_state(state: &InnerCtx<R, N, W, C, I>) -> Self {
        Self {
            signer: state.signer.clone(),
            agreement_conf: state.agreement_conf.clone(),
            chain_price: state.pricing_table.clone(),
            registry: state.registry.clone(),
            network: state.network.clone(),
            queue: state.worker.clone(),
            iisa: state.iisa.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<InnerCtx<R, N, W, C, I>>
    for SendIndexingAgreementProposalCtx<R, N, W, C>
where
    R: Clone,
    N: Clone,
    W: Clone,
    C: Clone,
{
    #[inline]
    fn from_state(state: &InnerCtx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            network: state.network.clone(),
            queue: state.worker.clone(),
            indexer_client: state.client.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<InnerCtx<R, N, W, C, I>>
    for ProcessIndexingAgreementCancellationCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    #[inline]
    fn from_state(state: &InnerCtx<R, N, W, C, I>) -> Self {
        Self {
            queue: state.worker.clone(),
            registry: state.registry.clone(),
        }
    }
}
