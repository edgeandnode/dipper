use std::{collections::BTreeSet, sync::Arc};

use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use thegraph_core::IndexerId;

use crate::{network::NetworkProvider, signer::PrivateKeyEip712Signer, worker::messages::Message};

/// The maximum number of candidates to select.
const DEFAULT_MAX_CANDIDATES: usize = 3;

/// The context shared across all requests.
#[derive(Clone)]
pub struct Ctx<R, N, W> {
    /// The EIP-712 signer
    pub(super) signer: Arc<PrivateKeyEip712Signer>,

    /// The allowlist of addresses that are allowed to make requests to the DIPs gateway
    pub(super) allowlist: Arc<BTreeSet<IndexerId>>,

    /// The DIPs registry
    pub(super) registry: R,

    /// The Network provider
    pub(super) network: N,

    /// The message queue worker
    pub(super) worker: W,
}

/// The HTTP server context builder.
pub struct CtxBuilder<S, R, N, W> {
    signer: S,
    allowlist: BTreeSet<IndexerId>,
    registry: R,
    network: N,
    worker: W,
}

#[doc(hidden)]
pub struct NotSet;

#[doc(hidden)]
pub struct SignerSet(Arc<PrivateKeyEip712Signer>);

#[doc(hidden)]
pub struct RegistrySet<R>(R);

#[doc(hidden)]
pub struct NetworkSet<N>(N);

#[doc(hidden)]
pub struct WorkerSet<W>(W);

impl CtxBuilder<NotSet, NotSet, NotSet, NotSet> {
    /// Creates a new [`CtxBuilder`].
    pub fn new() -> Self {
        Self {
            signer: NotSet,
            allowlist: Default::default(),
            registry: NotSet,
            network: NotSet,
            worker: NotSet,
        }
    }
}

impl<S, R, N, W> CtxBuilder<S, R, N, W> {
    /// Sets the list of addresses that are allowed to make requests to the DIPs gateway.
    pub fn with_allowlist(self, allowlist: BTreeSet<IndexerId>) -> CtxBuilder<S, R, N, W> {
        CtxBuilder {
            signer: self.signer,
            allowlist,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
        }
    }
}

impl<R, N, W> CtxBuilder<NotSet, R, N, W> {
    /// Sets the EIP-712 signer.
    pub fn with_signer(
        self,
        signer: Arc<PrivateKeyEip712Signer>,
    ) -> CtxBuilder<SignerSet, R, N, W> {
        CtxBuilder {
            signer: SignerSet(signer),
            allowlist: self.allowlist,
            registry: self.registry,
            network: self.network,
            worker: self.worker,
        }
    }
}

impl<S, N, W> CtxBuilder<S, NotSet, N, W> {
    /// Sets the DIPs registry.
    pub fn with_registry<R>(self, registry: R) -> CtxBuilder<S, RegistrySet<R>, N, W>
    where
        R: Registry + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            allowlist: self.allowlist,
            registry: RegistrySet(registry),
            network: self.network,
            worker: self.worker,
        }
    }
}

impl<S, R, W> CtxBuilder<S, R, NotSet, W> {
    /// Sets the network provider.
    pub fn with_network<N>(self, network: N) -> CtxBuilder<S, R, NetworkSet<N>, W>
    where
        N: NetworkProvider + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            allowlist: self.allowlist,
            registry: self.registry,
            network: NetworkSet(network),
            worker: self.worker,
        }
    }
}

impl<S, R, N> CtxBuilder<S, R, N, NotSet> {
    /// Sets the message queue worker.
    pub fn with_worker<W>(self, worker: W) -> CtxBuilder<S, R, N, WorkerSet<W>>
    where
        W: Queue<Message> + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            allowlist: self.allowlist,
            registry: self.registry,
            network: self.network,
            worker: WorkerSet(worker),
        }
    }
}

impl<R, N, W> CtxBuilder<SignerSet, RegistrySet<R>, NetworkSet<N>, WorkerSet<W>>
where
    R: Registry,
    N: NetworkProvider,
    W: Queue<Message>,
{
    /// Builds the [`Ctx`] instance.
    pub fn build(self) -> Ctx<R, N, W> {
        Ctx {
            signer: self.signer.0,
            allowlist: Arc::new(self.allowlist),
            registry: self.registry.0,
            network: self.network.0,
            worker: self.worker.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
    use dipper_pgmq::queue::{Job, Queue};
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
    use time::OffsetDateTime;
    use url::Url;
    use uuid::Uuid;

    use super::CtxBuilder;
    use crate::{
        network::{api::Indexer, NetworkProvider},
        signer::PrivateKeyEip712Signer,
    };

    pub struct DummyRegistry;

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

    pub struct DummyNetworkProvider;

    #[async_trait]
    impl NetworkProvider for DummyNetworkProvider {
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

    pub struct DummyQueueWorker;

    #[async_trait]
    impl<M> Queue<M> for DummyQueueWorker
    where
        M: Send + Sync + 'static,
    {
        async fn push(&self, _job: M) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn push_scheduled(
            &self,
            _job: M,
            _scheduled_for: OffsetDateTime,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn pull(&self, _number_of_jobs: usize) -> anyhow::Result<Vec<Job<M>>> {
            unimplemented!()
        }

        async fn remove(&self, _id: Uuid) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn fail_job(
            &self,
            _id: Uuid,
            _scheduled_for: Option<OffsetDateTime>,
        ) -> anyhow::Result<()> {
            unimplemented!()
        }

        async fn clear(&self) -> anyhow::Result<()> {
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
        let allowlist = BTreeSet::from([indexer_id!("A3A933720D7BAE63a102e70869D1Ca96E2329428")]);
        let registry = DummyRegistry;
        let network = DummyNetworkProvider;
        let worker = DummyQueueWorker;

        //* When
        let ctx = CtxBuilder::new()
            .with_signer(signer)
            .with_worker(worker)
            .with_registry(registry)
            .with_network(network)
            .with_allowlist(allowlist)
            .build();

        //* Then
        assert_eq!(ctx.allowlist.len(), 1);
    }
}
