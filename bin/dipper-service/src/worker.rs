mod api;
mod handlers;
mod messages;
mod result;
pub mod service;

pub use api::WorkerQueue;
use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_pgmq::{JobId, PgQueue, Queue};
pub use handlers::{
    FindIndexerForIndexingRequestCtx, ProcessIndexingAgreementCancellationCtx,
    ProcessIndexingRequestCancellationCtx, ProcessNewIndexingRequestCtx,
    SendIndexingAgreementCancellationCtx, SendIndexingAgreementProposalCtx,
};
use messages::{
    FindIndexerForIndexingRequest, Message, ProcessIndexingAgreementCancellation,
    ProcessIndexingRequestCancellation, ProcessNewIndexingRequest,
    SendIndexingAgreementCancellation, SendIndexingAgreementProposal,
};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

/// The worker that processes messages from the queue.
#[derive(Clone)]
pub struct Worker {
    queue: PgQueue,
}

impl Worker {
    /// Create a new instance of the worker
    pub fn new(queue: PgQueue) -> Self {
        Self { queue }
    }
}

#[async_trait]
impl WorkerQueue for Worker {
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

    async fn find_indexer_for_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::FindIndexerForIndexingRequest(
                FindIndexerForIndexingRequest {
                    indexing_request_id,
                    deployment_id,
                    deployment_chain_id,
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
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::SendIndexingAgreementCancellation(
                SendIndexingAgreementCancellation {
                    indexer_url,
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
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ProcessIndexingAgreementRequesterCancellation(
                ProcessIndexingAgreementCancellation { agreement_id },
            ))
            .await
    }

    async fn process_indexing_agreement_indexer_cancellation(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<JobId> {
        self.queue
            .push(Message::ProcessIndexingAgreementIndexerCancellation(
                ProcessIndexingAgreementCancellation { agreement_id },
            ))
            .await
    }
}
