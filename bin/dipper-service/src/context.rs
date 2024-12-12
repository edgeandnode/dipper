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
    admin_rpc_server, indexer_rpc_server, indexers::DipsClient, network::NetworkProvider,
    signer::PrivateKeyEip712Signer, worker, worker::WorkerQueue,
};

/// The maximum number of candidates to select.
pub const DEFAULT_MAX_CANDIDATES: usize = 3;

/// The context shared across all requests.
#[derive(Clone)]
pub struct Ctx<R, N, W, C, I> {
    /// The EIP-712 signer
    signer: Arc<PrivateKeyEip712Signer>,

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
    for worker::SendIndexingAgreementProposalCtx<R, W, C>
where
    R: Clone,
    W: Clone,
    C: Clone,
{
    fn from_state(state: &Ctx<R, N, W, C, I>) -> Self {
        Self {
            registry: state.registry.clone(),
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
    pub price_per_block: U256,
    /// The price per entity in wei GRT per epoch.
    pub price_per_entity_per_epoch: U256,
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
pub struct CtxBuilder<S, A, R, N, W, C, I> {
    signer: S,
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

impl CtxBuilder<NotSet, NotSet, NotSet, NotSet, NotSet, NotSet, NotSet> {
    /// Creates a new [`CtxBuilder`].
    pub fn new() -> Self {
        Self {
            signer: NotSet,
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

impl<S, A, R, N, W, C, I> CtxBuilder<S, A, R, N, W, C, I> {
    /// Sets the list of addresses that are allowed to make requests to the DIPs gateway Admin API.
    pub fn with_admin_allowlist(
        self,
        allowlist: BTreeSet<Address>,
    ) -> CtxBuilder<S, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
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
    ) -> CtxBuilder<S, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
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
    pub fn with_max_candidates(self, max_candidates: usize) -> CtxBuilder<S, A, R, N, W, C, I> {
        CtxBuilder {
            signer: self.signer,
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

impl<R, A, N, W, C, I> CtxBuilder<NotSet, A, R, N, W, C, I> {
    /// Sets the EIP-712 signer.
    pub fn with_signer(
        self,
        signer: Arc<PrivateKeyEip712Signer>,
    ) -> CtxBuilder<SignerSet, A, R, N, W, C, I> {
        CtxBuilder {
            signer: SignerSet(signer),
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

impl<S, R, N, W, C, I> CtxBuilder<S, NotSet, R, N, W, C, I> {
    /// Sets the _indexing agreement_ configuration.
    pub fn with_agreement_config(
        self,
        config: impl Into<(
            IndexingAgreementConfig,
            BTreeMap<ChainId, IndexingAgreementChainPrices>,
        )>,
    ) -> CtxBuilder<S, AgreementConfigSet, R, N, W, C, I> {
        let (config, prices) = config.into();
        CtxBuilder {
            signer: self.signer,
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

impl<S, A, N, W, C, I> CtxBuilder<S, A, NotSet, N, W, C, I> {
    /// Sets the DIPs registry.
    pub fn with_registry<R>(self, registry: R) -> CtxBuilder<S, A, RegistrySet<R>, N, W, C, I>
    where
        R: Registry + 'static,
    {
        CtxBuilder {
            signer: self.signer,
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

impl<S, A, R, W, C, I> CtxBuilder<S, A, R, NotSet, W, C, I> {
    /// Sets the network provider.
    pub fn with_network_provider<N>(self, network: N) -> CtxBuilder<S, A, R, NetworkSet<N>, W, C, I>
    where
        N: NetworkProvider + 'static,
    {
        CtxBuilder {
            signer: self.signer,
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

impl<S, A, R, N, C, I> CtxBuilder<S, A, R, N, NotSet, C, I> {
    /// Sets the message queue worker.
    pub fn with_worker<W>(self, worker: W) -> CtxBuilder<S, A, R, N, WorkerSet<W>, C, I>
    where
        W: WorkerQueue + 'static,
    {
        CtxBuilder {
            signer: self.signer,
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

impl<S, A, R, N, W, I> CtxBuilder<S, A, R, N, W, NotSet, I> {
    /// Sets the indexer client.
    pub fn with_indexer_client<C>(self, client: C) -> CtxBuilder<S, A, R, N, W, ClientSet<C>, I>
    where
        C: DipsClient + 'static,
    {
        CtxBuilder {
            signer: self.signer,
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

impl<S, A, R, N, W, C> CtxBuilder<S, A, R, N, W, C, NotSet> {
    /// Sets the Indexing Indexer Selection Algorithm (IISA) service.
    pub fn with_iisa<I>(self, iisa: I) -> CtxBuilder<S, A, R, N, W, C, IisaSet<I>>
    where
        I: CandidateSelection + 'static,
    {
        CtxBuilder {
            signer: self.signer,
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
    use dipper_iisa::{CandidateSelection, SelectionError};
    use dipper_registry::{
        Error, IndexingAgreement, IndexingAgreementVoucher, IndexingReceipt,
        IndexingReceiptReportedWork, IndexingRequest, Registry,
    };
    use thegraph_core::{
        alloy::{
            primitives::{address, b256, ChainId, U256},
            signers::local::PrivateKeySigner,
            sol_types::{eip712_domain, private::Address},
        },
        indexer_id, DeploymentId, IndexerId,
    };
    use url::Url;

    use super::{CtxBuilder, IndexingAgreementConfig};
    use crate::{
        indexers::{AgreementProposalResponse, DipsClient, DipsError},
        network::{Deployment, Indexer, NetworkProvider},
        signer::PrivateKeyEip712Signer,
        worker::WorkerQueue,
    };

    struct DummyRegistry;

    #[async_trait]
    impl Registry for DummyRegistry {
        async fn register_new_indexing_request(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> Result<IndexingRequestId, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_requests(&self) -> Result<Vec<IndexingRequest>, Error> {
            unimplemented!()
        }

        async fn get_indexing_request_by_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> Result<Option<IndexingRequest>, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> Result<Vec<IndexingRequest>, Error> {
            unimplemented!()
        }

        async fn get_indexing_request_active_indexing_agreements(
            &self,
            _request_id: &IndexingRequestId,
        ) -> Result<Vec<IndexingAgreement>, Error> {
            unimplemented!()
        }

        async fn get_indexing_request_rejected_indexing_agreements(
            &self,
            _request_id: &IndexingRequestId,
        ) -> Result<Vec<IndexingAgreement>, Error> {
            unimplemented!()
        }

        async fn mark_indexing_request_as_canceled(
            &self,
            _request_id: &IndexingRequestId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn register_new_indexing_agreement(
            &self,
            _request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _indexer_id: IndexerId,
            _indexer_url: Url,
            _voucher: IndexingAgreementVoucher,
        ) -> Result<IndexingAgreementId, Error> {
            unimplemented!()
        }

        async fn get_indexing_agreement_by_id(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> Result<Option<IndexingAgreement>, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> Result<Vec<IndexingAgreement>, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> Result<Vec<IndexingAgreement>, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> Result<Vec<IndexingAgreement>, Error> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_delivery_failed(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_accepted(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_rejected(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_canceled_by_indexer(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn mark_indexing_agreement_as_expired(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn register_new_indexing_receipt(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexer_id: IndexerId,
            _indexer_operator_id: Address,
            _reported_work: IndexingReceiptReportedWork,
            _amount: U256,
        ) -> Result<IndexingReceiptId, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_receipts_by_indexing_agreement_id(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<Vec<IndexingReceipt>, Error> {
            unimplemented!()
        }

        async fn get_indexing_receipt_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> Result<Option<IndexingReceipt>, Error> {
            unimplemented!()
        }
    }

    struct DummyNetworkProvider;

    #[async_trait]
    impl NetworkProvider for DummyNetworkProvider {
        fn get_deployment_by_id(&self, _deployment_id: &DeploymentId) -> Option<Deployment> {
            unimplemented!()
        }

        fn get_indexer_by_id(&self, _indexer_id: &IndexerId) -> Option<Indexer> {
            unimplemented!()
        }

        fn get_indexers_not_indexing_a_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> Vec<Indexer> {
            unimplemented!()
        }

        fn get_indexer_id_for_operator_address(
            &self,
            _operator_address: &Address,
        ) -> Option<IndexerId> {
            unimplemented!()
        }
    }

    struct DummyWorker;

    #[async_trait]
    impl WorkerQueue for DummyWorker {
        async fn process_new_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn find_indexer_for_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn send_indexing_agreement_cancellation(
            &self,
            _indexer_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn process_indexing_request_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn process_indexing_agreement_requester_cancellation(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn process_indexing_agreement_indexer_cancellation(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }
    }

    pub struct DummyClient;

    #[async_trait]
    impl DipsClient for DummyClient {
        async fn send_indexing_agreement_proposal(
            &self,
            _indexer: Url,
            _indexing_agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
        ) -> Result<AgreementProposalResponse, DipsError> {
            unimplemented!()
        }

        async fn send_indexing_agreement_cancellation_notification(
            &self,
            _indexer: Url,
            _indexing_agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
        ) -> Result<(), DipsError> {
            unimplemented!()
        }
    }

    struct DummyIisa;

    #[async_trait]
    impl CandidateSelection for DummyIisa {
        async fn select_one(
            &self,
            _deployment_id: DeploymentId,
            _candidates: Vec<dipper_iisa::Indexer>,
        ) -> Result<Option<dipper_iisa::Indexer>, SelectionError> {
            unimplemented!()
        }

        async fn select(
            &self,
            _deployment_id: DeploymentId,
            _candidates: Vec<dipper_iisa::Indexer>,
            _num_candidates: usize,
        ) -> Result<Vec<dipper_iisa::Indexer>, SelectionError> {
            unimplemented!()
        }
    }

    #[test]
    fn ctx_builder() {
        //* Given
        let signer = {
            let signer = PrivateKeySigner::random();
            let signer_address = signer.address();
            let domain = eip712_domain! {
                name: "Test domain",
                version: "1",
                chain_id: 1,
                verifying_contract: address!("a83682bbe91c0d2d48a13fd751b2da8e989fe421"),
                salt: b256!("66eb090e6dbb9668c7d32c0ee7ba5e8f08d84385804485d316dd5f5692273593")
            };

            Arc::new(PrivateKeyEip712Signer::new(signer, signer_address, domain))
        };
        let admin_allowlist =
            BTreeSet::from([address!("2c46937bc028c31b7bb463796c9737793a45d464")]);
        let network_allowlist =
            BTreeSet::from([indexer_id!("a3a933720d7bae63a102e70869d1ca96e2329428")]);
        let agreement_config = IndexingAgreementConfig {
            service: address!("2c46937bc028c31b7bb463796c9737793a45d464"),
            max_initial_amount: U256::from(100),
            max_ongoing_amount_per_epoch: U256::from(10),
            max_epochs_per_collection: 28,
            min_epochs_per_collection: 1,
            duration_epochs: None,
        };
        let pricing_table = BTreeMap::new();
        let registry = DummyRegistry;
        let network = DummyNetworkProvider;
        let worker = DummyWorker;
        let client = DummyClient;
        let iisa = DummyIisa;

        //* When
        let ctx = CtxBuilder::new()
            .with_signer(signer)
            .with_worker(worker)
            .with_registry(registry)
            .with_network_provider(network)
            .with_indexer_client(client)
            .with_iisa(iisa)
            .with_admin_allowlist(admin_allowlist)
            .with_network_allowlist(network_allowlist)
            .with_max_candidates(5)
            .with_agreement_config((agreement_config, pricing_table))
            .build();

        //* Then
        assert_eq!(ctx.admin_allowlist.len(), 1);
        assert_eq!(ctx.network_allowlist.len(), 1);
        assert_eq!(ctx.max_candidates, 5);
    }
}
