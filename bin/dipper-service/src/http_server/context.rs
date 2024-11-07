use std::{collections::BTreeSet, sync::Arc};

use alloy_signer_local::PrivateKeySigner;
use dipper_core::signed_message::Eip712Signer;
use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use thegraph_core::Address;

use crate::worker::messages::Message;

/// The maximum number of candidates to select.
const DEFAULT_MAX_CANDIDATES: usize = 3;

/// The context shared across all requests.
#[derive(Clone)]
pub struct Ctx<R, W> {
    /// The EIP-712 signer
    pub(super) signer: Arc<Eip712Signer<PrivateKeySigner>>,

    /// The allowlist of addresses that are allowed to make requests to the DIPs gateway
    pub(super) allowlist: Arc<BTreeSet<Address>>,

    /// The DIPs registry
    pub(super) registry: R,

    /// The message queue worker
    pub(super) worker: W,

    /// The maximum number of candidates to select
    pub(super) max_candidates: usize,
}

/// The HTTP server context builder.
pub struct CtxBuilder<S, R, W> {
    signer: S,
    allowlist: BTreeSet<Address>,
    registry: R,
    worker: W,
    max_candidates: usize,
}

struct NotSet;

struct SignerSet(Eip712Signer<PrivateKeySigner>);

struct RegistrySet<R>(R);

struct WorkerSet<W>(W);

impl CtxBuilder<NotSet, NotSet, NotSet> {
    /// Creates a new [`CtxBuilder`].
    pub fn new() -> Self {
        Self {
            signer: NotSet,
            allowlist: Default::default(),
            registry: NotSet,
            worker: NotSet,
            max_candidates: DEFAULT_MAX_CANDIDATES,
        }
    }
}

impl<S, R, W> CtxBuilder<S, R, W> {
    /// Sets the list of addresses that are allowed to make requests to the DIPs gateway.
    pub fn with_allowlist(self, allowlist: BTreeSet<Address>) -> CtxBuilder<S, R, W> {
        CtxBuilder {
            signer: self.signer,
            allowlist,
            registry: self.registry,
            worker: self.worker,
            max_candidates: self.max_candidates,
        }
    }

    /// Sets the maximum number of candidates to select.
    pub fn with_max_candidates(self, max_candidates: usize) -> CtxBuilder<S, R, W> {
        CtxBuilder {
            signer: self.signer,
            allowlist: self.allowlist,
            registry: self.registry,
            worker: self.worker,
            max_candidates,
        }
    }
}

impl<R, W> CtxBuilder<NotSet, R, W> {
    /// Sets the EIP-712 signer.
    pub fn with_signer(
        self,
        signer: Eip712Signer<PrivateKeySigner>,
    ) -> CtxBuilder<SignerSet, R, W> {
        CtxBuilder {
            signer: SignerSet(signer),
            allowlist: self.allowlist,
            registry: self.registry,
            worker: self.worker,
            max_candidates: self.max_candidates,
        }
    }
}

impl<S, W> CtxBuilder<S, NotSet, W> {
    /// Sets the DIPs registry.
    pub fn with_registry<R>(self, registry: R) -> CtxBuilder<S, RegistrySet<R>, W>
    where
        R: Registry + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            allowlist: self.allowlist,
            registry: RegistrySet(registry),
            worker: self.worker,
            max_candidates: self.max_candidates,
        }
    }
}

impl<S, R> CtxBuilder<S, R, NotSet> {
    /// Sets the message queue worker.
    pub fn with_worker<W>(self, worker: W) -> CtxBuilder<S, R, WorkerSet<W>>
    where
        W: Queue<Message> + 'static,
    {
        CtxBuilder {
            signer: self.signer,
            allowlist: self.allowlist,
            registry: self.registry,
            worker: WorkerSet(worker),
            max_candidates: self.max_candidates,
        }
    }
}

impl<R, W> CtxBuilder<SignerSet, RegistrySet<R>, WorkerSet<W>>
where
    R: Registry,
    W: Queue<Message>,
{
    /// Builds the [`Ctx`] instance.
    pub fn build(self) -> Ctx<R, W> {
        Ctx {
            signer: Arc::new(self.signer.0),
            allowlist: Arc::new(self.allowlist),
            registry: self.registry.0,
            worker: self.worker.0,
            max_candidates: self.max_candidates,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use alloy_signer_local::PrivateKeySigner;
    use alloy_sol_types::{eip712_domain, private::Address};
    use async_trait::async_trait;
    use dipper_core::{
        ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId},
        signed_message::Eip712Signer,
    };
    use dipper_pgmq::queue::{Job, Queue};
    use dipper_registry::{Error, IndexingAgreement, IndexingReceipt, IndexingRequest, Registry};
    use thegraph_core::{
        address, alloy_primitives::b256, AllocationId, DeploymentId, IndexerId, ProofOfIndexing,
    };
    use time::OffsetDateTime;
    use url::Url;
    use uuid::Uuid;

    use super::CtxBuilder;

    pub struct DummyRegistry;

    #[async_trait]
    impl Registry for DummyRegistry {
        async fn register_new_indexing_request(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
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
            _indexer_id: IndexerId,
            _indexer_url: Url,
            _duration: Duration,
        ) -> Result<IndexingAgreementId, Error> {
            unimplemented!()
        }

        async fn get_indexing_agreement(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> Result<Option<IndexingAgreement>, Error> {
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
            _allocation_id: AllocationId,
            _fee: i64,
        ) -> Result<IndexingReceiptId, Error> {
            unimplemented!()
        }

        async fn get_all_indexing_receipts_by_indexing_agreement_id(
            &self,
            _agreement_id: &IndexingAgreementId,
        ) -> Result<Vec<IndexingReceipt>, Error> {
            unimplemented!()
        }

        async fn get_indexing_receipt_by_allocation_id(
            &self,
            _allocation_id: &AllocationId,
        ) -> Result<Option<IndexingReceipt>, Error> {
            unimplemented!()
        }

        async fn redeem_indexing_receipt(
            &self,
            _allocation_id: AllocationId,
            _poi: ProofOfIndexing,
        ) -> Result<(), Error> {
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

            Eip712Signer::new(signer, signer_address, domain)
        };
        let allowlist = BTreeSet::from([address!("A3A933720D7BAE63a102e70869D1Ca96E2329428")]);
        let registry = DummyRegistry;
        let worker = DummyQueueWorker;

        //* When
        let ctx = CtxBuilder::new()
            .with_signer(signer)
            .with_worker(worker)
            .with_registry(registry)
            .with_allowlist(allowlist)
            .with_max_candidates(10)
            .build();

        //* Then
        assert_eq!(ctx.max_candidates, 10);
        assert_eq!(ctx.allowlist.len(), 1);
    }
}
