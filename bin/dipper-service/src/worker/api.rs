use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use thegraph_core::{alloy::primitives::ChainId, DeploymentId};
use url::Url;

#[async_trait]
pub trait WorkerQueue {
    async fn process_new_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: usize,
    ) -> anyhow::Result<()>;

    async fn find_indexer_for_indexing_request(
        &self,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<()>;

    async fn send_indexing_agreement_proposal(
        &self,
        candidate_url: Url,
        agreement_id: IndexingAgreementId,
        indexing_request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> anyhow::Result<()>;

    async fn send_indexing_agreement_cancellation(
        &self,
        indexer_url: Url,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<()>;

    async fn process_indexing_request_cancellation(
        &self,
        indexing_request_id: IndexingRequestId,
    ) -> anyhow::Result<()>;

    async fn process_indexing_agreement_requester_cancellation(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<()>;

    async fn process_indexing_agreement_indexer_cancellation(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> anyhow::Result<()>;
}
