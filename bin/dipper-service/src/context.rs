use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use dipper_registry::Registry;
use thegraph_core::{
    alloy::primitives::{Address, ChainId, U256},
    IndexerId,
};

use crate::{
    admin_rpc_server,
    indexer_rpc_client::IndexerClient,
    indexer_rpc_server,
    network::NetworkProvider,
    signing::{eip712::PrivateKeyEip712Signer, tap::ReceiptSigner},
    worker,
    worker::WorkerQueue,
};

/// The maximum number of candidates to select.
pub const DEFAULT_MAX_CANDIDATES: usize = 3;

/// The context shared across all requests.
#[derive(Clone)]
pub struct Ctx<R, N, W, C, I> {
    /// The EIP-712 signer
    signer: Arc<PrivateKeyEip712Signer>,

    /// The TAP receipt signer
    tap_signer: Arc<ReceiptSigner>,

    /// The allowlist of addresses that are allowed to make requests to the DIPs gateway Admin API
    admin_allowlist: Arc<BTreeSet<Address>>,

    /// The allowlist of indexers that allowed to make requests to the DIPs gateway Network API
    network_allowlist: Arc<BTreeSet<IndexerId>>,

    /// The _indexing agreement_ configuration
    agreement_conf: Arc<IndexingAgreementConfig>,

    /// The _indexing agreement_ per-chain pricing table
    pricing_table: Arc<BTreeMap<ChainId, IndexingAgreementChainPrices>>,

    /// The maximum number of candidates to select
    max_candidates: usize,

    /// The DIPs registry
    registry: R,

    /// The Network provider
    network: N,

    /// The message queue worker
    worker: W,

    /// The indexer client
    client: C,

    /// The Indexing Indexer Selection Algorithm (IISA) service
    iisa: I,
}

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>> for admin_rpc_server::IndexingRequestsCtx<R, N, W>
where
    R: Clone,
    N: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            allowlist: ctx.admin_allowlist.clone(),
            registry: ctx.registry.clone(),
            network: ctx.network.clone(),
            worker: ctx.worker.clone(),
            max_candidates: ctx.max_candidates,
        }
    }
}

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>> for admin_rpc_server::IndexingAgreementsCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            allowlist: ctx.admin_allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>>
    for indexer_rpc_server::DipsGatewayServiceCtx<R, N, W>
where
    R: Clone,
    N: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            tap_signer: ctx.tap_signer.clone(),
            allowlist: ctx.network_allowlist.clone(),
            registry: ctx.registry.clone(),
            network: ctx.network.clone(),
            worker: ctx.worker.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>>
    for worker::FindIndexerForIndexingRequestCtx<R, N, W, I>
where
    R: Clone,
    N: Clone,
    W: Clone,
    I: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
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

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>>
    for worker::SendIndexingAgreementCancellationCtx<R, C>
where
    R: Clone,
    C: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            indexer_client: state.client.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>>
    for worker::ProcessIndexingRequestCancellationCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            queue: state.worker.clone(),
        }
    }
}

impl<W, N, R, C, I> FromState<Ctx<R, N, W, C, I>>
    for worker::ProcessNewIndexingRequestCtx<R, N, W, I>
where
    R: Clone,
    N: Clone,
    W: Clone,
    I: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
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

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>>
    for worker::SendIndexingAgreementProposalCtx<R, N, W, C>
where
    R: Clone,
    N: Clone,
    W: Clone,
    C: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
            network: state.network.clone(),
            queue: state.worker.clone(),
            indexer_client: state.client.clone(),
        }
    }
}

impl<R, N, W, C, I> FromState<Ctx<R, N, W, C, I>>
    for worker::ProcessIndexingAgreementCancellationCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            queue: state.worker.clone(),
            registry: state.registry.clone(),
        }
    }
}

/// The _indexing agreement_ configuration.
///
/// It holds the configuration for the _indexing agreements_, e.g., the service address, the
/// maximum amount that can be collected for the subgraph initial sync, the maximum amount
/// collectable per epoch, etc.
#[derive(Debug)]
pub struct IndexingAgreementConfig {
    /// The _indexing agreement_'s service address.
    pub service: Address,
    /// The _indexing agreement_'s maximum amount that can be collected for the subgraph initial
    /// sync.
    pub max_initial_amount: U256,
    /// The _indexing agreement_'s maximum amount collectable per epoch.
    pub max_ongoing_amount_per_epoch: U256,
    /// The _indexing agreement_'s maximum epochs per collection.
    pub max_epochs_per_collection: u32,
    /// The _indexing agreement_'s minimum epochs per collection.
    pub min_epochs_per_collection: u32,
    /// The _indexing agreement_'s duration in epochs.
    pub duration_epochs: Option<u32>,
}

/// The _indexing agreement_'s per-chain prices.
#[derive(Debug)]
pub struct IndexingAgreementChainPrices {
    /// The price per block in wei GRT.
    pub base_price_per_epoch: U256,
    /// The price per entity in wei GRT per epoch.
    pub price_per_entity: U256,
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
}

/// The HTTP server context builder.
pub struct CtxBuilder<S, T, A, R, N, W, C, I> {
    signer: S,
    tap_signer: T,
    admin_allowlist: BTreeSet<Address>,
    network_allowlist: BTreeSet<IndexerId>,
    agreement_config: A,
    max_candidates: usize,
    registry: R,
    network: N,
    worker: W,
    client: C,
    iisa: I,
}

#[doc(hidden)]
pub struct NotSet;

#[doc(hidden)]
pub struct SignerSet(Arc<PrivateKeyEip712Signer>);

#[doc(hidden)]
pub struct TapSignerSet(Arc<ReceiptSigner>);

#[doc(hidden)]
pub struct AgreementConfigSet {
    config: IndexingAgreementConfig,
    prices: BTreeMap<ChainId, IndexingAgreementChainPrices>,
}

#[doc(hidden)]
pub struct RegistrySet<R>(R);

#[doc(hidden)]
pub struct NetworkSet<N>(N);

#[doc(hidden)]
pub struct WorkerSet<W>(W);

#[doc(hidden)]
pub struct ClientSet<C>(C);

#[doc(hidden)]
pub struct IisaSet<I>(I);

impl CtxBuilder<NotSet, NotSet, NotSet, NotSet, NotSet, NotSet, NotSet, NotSet> {
    /// Creates a new [`CtxBuilder`].
    pub fn new() -> Self {
        Self {
            signer: NotSet,
            tap_signer: NotSet,
            admin_allowlist: Default::default(),
            network_allowlist: Default::default(),
            agreement_config: NotSet,
            max_candidates: DEFAULT_MAX_CANDIDATES,
            registry: NotSet,
            network: NotSet,
            worker: NotSet,
            client: NotSet,
            iisa: NotSet,
        }
    }
}

impl<S, T, A, R, N, W, C, I> CtxBuilder<S, T, A, R, N, W, C, I> {
    /// Sets the list of addresses that are allowed to make requests to the DIPs gateway Admin API.
    pub fn with_admin_allowlist(
        self,
        allowlist: BTreeSet<Address>,
    ) -> CtxBuilder<S, T, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }

    /// Sets the list of indexers that are allowed to make requests to the DIPs gateway Network API.
    pub fn with_network_allowlist(
        self,
        allowlist: BTreeSet<IndexerId>,
    ) -> CtxBuilder<S, T, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }

    /// Sets the maximum number of candidates to select.
    pub fn with_max_candidates(self, max_candidates: usize) -> CtxBuilder<S, T, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<T, R, A, N, W, C, I> CtxBuilder<NotSet, T, A, R, N, W, C, I> {
    /// Sets the EIP-712 signer.
    pub fn with_signer(
        self,
        signer: Arc<PrivateKeyEip712Signer>,
    ) -> CtxBuilder<SignerSet, T, A, R, N, W, C, I> {
        CtxBuilder {
            signer: SignerSet(signer),
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<S, R, A, N, W, C, I> CtxBuilder<S, NotSet, A, R, N, W, C, I> {
    /// Sets the EIP-712 signer.
    pub fn with_tap_signer(
        self,
        signer: Arc<ReceiptSigner>,
    ) -> CtxBuilder<S, TapSignerSet, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
            tap_signer: TapSignerSet(signer),
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<S, T, R, N, W, C, I> CtxBuilder<S, T, NotSet, R, N, W, C, I> {
    /// Sets the _indexing agreement_ configuration.
    pub fn with_agreement_config(
        self,
        config: impl Into<(
            IndexingAgreementConfig,
            BTreeMap<ChainId, IndexingAgreementChainPrices>,
        )>,
    ) -> CtxBuilder<S, T, AgreementConfigSet, R, N, W, C, I> {
        let (config, prices) = config.into();
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: AgreementConfigSet { config, prices },
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<S, T, A, N, W, C, I> CtxBuilder<S, T, A, NotSet, N, W, C, I> {
    /// Sets the DIPs registry.
    pub fn with_registry<R>(self, registry: R) -> CtxBuilder<S, T, A, RegistrySet<R>, N, W, C, I>
    where
        R: Registry + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: RegistrySet(registry),
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<S, T, A, R, W, C, I> CtxBuilder<S, T, A, R, NotSet, W, C, I> {
    /// Sets the network provider.
    pub fn with_network_provider<N>(
        self,
        network: N,
    ) -> CtxBuilder<S, T, A, R, NetworkSet<N>, W, C, I>
    where
        N: NetworkProvider + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: NetworkSet(network),
            worker: self.worker,
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<S, T, A, R, N, C, I> CtxBuilder<S, T, A, R, N, NotSet, C, I> {
    /// Sets the message queue worker.
    pub fn with_worker<W>(self, worker: W) -> CtxBuilder<S, T, A, R, N, WorkerSet<W>, C, I>
    where
        W: WorkerQueue + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: WorkerSet(worker),
            client: self.client,
            iisa: self.iisa,
        }
    }
}

impl<S, T, A, R, N, W, I> CtxBuilder<S, T, A, R, N, W, NotSet, I> {
    /// Sets the indexer client.
    pub fn with_indexer_client<C>(self, client: C) -> CtxBuilder<S, T, A, R, N, W, ClientSet<C>, I>
    where
        C: IndexerClient + 'static,
    {
        CtxBuilder {
            signer: self.signer,

            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: ClientSet(client),
            iisa: self.iisa,
        }
    }
}

impl<S, T, A, R, N, W, C> CtxBuilder<S, T, A, R, N, W, C, NotSet> {
    /// Sets the Indexing Indexer Selection Algorithm (IISA) service.
    pub fn with_iisa<I>(self, iisa: I) -> CtxBuilder<S, T, A, R, N, W, C, IisaSet<I>>
    where
        I: CandidateSelection + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            tap_signer: self.tap_signer,
            admin_allowlist: self.admin_allowlist,
            network_allowlist: self.network_allowlist,
            agreement_config: self.agreement_config,
            max_candidates: self.max_candidates,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
            client: self.client,
            iisa: IisaSet(iisa),
        }
    }
}

impl<R, N, W, C, I>
    CtxBuilder<
        SignerSet,
        TapSignerSet,
        AgreementConfigSet,
        RegistrySet<R>,
        NetworkSet<N>,
        WorkerSet<W>,
        ClientSet<C>,
        IisaSet<I>,
    >
{
    /// Builds the [`Ctx`] instance.
    pub fn build(self) -> Ctx<R, N, W, C, I> {
        Ctx {
            signer: self.signer.0,
            tap_signer: self.tap_signer.0,
            admin_allowlist: Arc::new(self.admin_allowlist),
            network_allowlist: Arc::new(self.network_allowlist),
            agreement_conf: Arc::new(self.agreement_config.config),
            pricing_table: Arc::new(self.agreement_config.prices),
            max_candidates: self.max_candidates,
            registry: self.registry.0,
            network: self.network.0,
            worker: self.worker.0,
            client: self.client.0,
            iisa: self.iisa.0,
        }
    }
}
