use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use super::{
    handlers::{
        CancelRejectedAgreementOnChain, ProcessIndexingAgreementCancellation,
        ProcessIndexingRequestCancellation, ProcessNewIndexingRequest, ReassessIndexingRequest,
        SendIndexingAgreementCancellation, SendIndexingAgreementProposal, SubmitOffer,
    },
    messages::Message,
    queue::{JobId, Queue},
};

#[async_trait]
pub trait WorkerQueue {
    async fn process_new_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> anyhow::Result<JobId>;

    async fn send_indexing_agreement_proposal(
        &self,
        candidate_url: Url,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<JobId>;

    async fn send_indexing_agreement_cancellation(
        &self,
        indexer_url: Url,
        indexing_request_id: IndexingRequestId,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId>;

    async fn process_indexing_request_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
    ) -> anyhow::Result<JobId>;

    async fn process_indexing_agreement_requester_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId>;

    async fn process_indexing_agreement_indexer_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId>;

    async fn reassess_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> anyhow::Result<JobId>;

    /// Cancel a rejected agreement on-chain.
    ///
    /// When an indexer rejected off-chain but accepted on-chain, this cancels
    /// the agreement via `cancelIndexingAgreementByPayer`.
    async fn cancel_rejected_agreement_on_chain(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId>;

    /// Submit an RCA offer on-chain as the first step of a new proposal.
    async fn submit_offer(
        &self,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        indexer_url: Url,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<JobId>;
}

/// The worker that processes messages from the queue.
#[derive(Clone)]
pub struct WorkerQueueHandle<Q> {
    queue: Q,
}

impl<Q> WorkerQueueHandle<Q> {
    /// Create a new instance of the worker queue handle
    pub(super) fn new(queue: Q) -> Self {
        Self { queue }
    }
}

#[async_trait]
impl<Q> WorkerQueue for WorkerQueueHandle<Q>
where
    Q: Queue<Message> + Send + Sync,
{
    async fn process_new_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ProcessNewIndexingRequest(
                ProcessNewIndexingRequest {
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
                    num_candidates,
                },
            ))
            .await
    }

    async fn send_indexing_agreement_proposal(
        &self,
        indexer_url: Url,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::SendIndexingAgreementProposal(
                SendIndexingAgreementProposal {
                    indexer_url,
                    agreement_id,
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
                },
            ))
            .await
    }

    async fn send_indexing_agreement_cancellation(
        &self,
        indexer_url: Url,
        indexing_request_id: IndexingRequestId,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::SendIndexingAgreementCancellation(
                SendIndexingAgreementCancellation {
                    indexer_url,
                    indexing_request_id,
                    agreement_id,
                },
            ))
            .await
    }

    async fn process_indexing_request_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ProcessIndexingRequestCancellation(
                ProcessIndexingRequestCancellation {
                    indexing_request_id,
                },
            ))
            .await
    }

    async fn process_indexing_agreement_requester_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ProcessIndexingAgreementRequesterCancellation(
                ProcessIndexingAgreementCancellation {
                    indexing_request_id,
                    agreement_id,
                },
            ))
            .await
    }

    async fn process_indexing_agreement_indexer_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ProcessIndexingAgreementIndexerCancellation(
                ProcessIndexingAgreementCancellation {
                    indexing_request_id,
                    agreement_id,
                },
            ))
            .await
    }

    async fn reassess_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ReassessIndexingRequest(ReassessIndexingRequest {
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
                num_candidates,
            }))
            .await
    }

    async fn cancel_rejected_agreement_on_chain(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::CancelRejectedAgreementOnChain(
                CancelRejectedAgreementOnChain { agreement_id },
            ))
            .await
    }

    async fn submit_offer(
        &self,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        indexer_url: Url,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::SubmitOffer(SubmitOffer {
                agreement_id,
                indexing_request_id,
                indexer_url,
                deployment_id,
                deployment_chain_id,
            }))
            .await
    }
}
