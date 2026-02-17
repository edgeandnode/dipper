use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_rpc::indexer::indexer_client::rpc::ProposalResponse;
use serde_with::serde_as;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::{
    config::DEFAULT_MAX_CANDIDATES,
    indexer_rpc_client::IndexerClient,
    registry::{AgreementRegistry, IndexingAgreementStatus, IndexingRequestRegistry},
    worker::{
        result::{JobError, JobMeta, JobResult},
        service::WorkerQueue,
    },
};

pub struct Ctx<R, W, C> {
    pub registry: R,
    pub queue: W,
    pub indexer_client: C,
}

/// Send an indexing agreement proposal to the indexer.
#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Message {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub indexer_url: Url,

    pub agreement_id: IndexingAgreementId,
    pub indexing_request_id: IndexingRequestId,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
}

/// Send an indexing agreement proposal to the indexer.
///
/// This function sends a SignedRCA to the indexer and processes the response:
/// - `Accept`: The indexer received the proposal and may accept on-chain before the deadline.
///   Agreement stays in `Created` until an on-chain acceptance event is observed.
/// - `Reject`: The indexer explicitly rejected the proposal. Agreement is marked as
///   `DeliveryFailed` and the indexing request is reassessed to find replacement indexers.
/// - Network error: Same handling as `Reject` - mark failed and reassess.
pub async fn handle<R, W, C>(
    ctx: Ctx<R, W, C>,
    Message {
        indexer_url,
        agreement_id,
        indexing_request_id,
        deployment_id,
        deployment_chain_id,
    }: &Message,
    _job_meta: JobMeta,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
    C: IndexerClient,
{
    // TODO: THIS IS A HACK
    let indexer_url = {
        let mut url = indexer_url.clone();
        url.set_port(Some(7602)).unwrap();
        url
    };

    // Check the status of the agreement before sending the proposal
    let agreement = match ctx
        .registry
        .get_indexing_agreement_by_id(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
    {
        None => {
            tracing::error!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                "Indexing agreement not found"
            );
            return Ok(());
        }
        Some(agreement) => match agreement.status {
            IndexingAgreementStatus::Created => agreement,
            status => {
                tracing::error!(
                    indexing_request_id=%indexing_request_id,
                    agreement_id=%agreement_id,
                    "Not sending agreement proposal. Invalid agreement status: {status}",
                );
                return Ok(());
            }
        },
    };

    tracing::debug!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        deployment_id=%deployment_id,
        indexer_url=%indexer_url,
        "Sending indexing agreement proposal"
    );

    let response = ctx
        .indexer_client
        .send_indexing_agreement_proposal(&indexer_url, *agreement_id, agreement.voucher)
        .await;

    match response {
        Ok(ProposalResponse::Accept) => {
            tracing::debug!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                deployment_id=%deployment_id,
                indexer_url=%indexer_url,
                "Agreement proposal accepted by indexer"
            );
            // Agreement stays in Created, waiting for on-chain acceptance
        }
        Ok(ProposalResponse::Reject) => {
            tracing::info!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                deployment_id=%deployment_id,
                indexer_url=%indexer_url,
                "Agreement proposal rejected by indexer"
            );
            // Mark as Rejected and reassess. The indexer may still accept on-chain,
            // in which case the chain listener will trigger cancellation.
            mark_rejected_and_reassess(
                &ctx,
                agreement_id,
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
            )
            .await?;
        }
        Err(err) => {
            tracing::error!(
                indexing_request_id=%indexing_request_id,
                agreement_id=%agreement_id,
                error=?err,
                "Failed to send indexing agreement proposal"
            );
            mark_failed_and_reassess(
                &ctx,
                agreement_id,
                indexing_request_id,
                deployment_id,
                deployment_chain_id,
            )
            .await?;
        }
    }

    Ok(())
}

/// Mark an agreement as rejected and queue reassessment.
///
/// The indexer rejected the proposal off-chain. We mark as Rejected and find a replacement.
/// If the indexer later accepts on-chain anyway, the chain listener will cancel it.
async fn mark_rejected_and_reassess<R, W, C>(
    ctx: &Ctx<R, W, C>,
    agreement_id: &IndexingAgreementId,
    indexing_request_id: &IndexingRequestId,
    deployment_id: &DeploymentId,
    deployment_chain_id: &ChainId,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
    C: IndexerClient,
{
    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        "Marking indexing agreement as REJECTED"
    );
    ctx.registry
        .mark_indexing_agreement_as_rejected(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    queue_reassessment(ctx, indexing_request_id, deployment_id, deployment_chain_id).await
}

/// Mark an agreement as delivery failed and queue reassessment.
async fn mark_failed_and_reassess<R, W, C>(
    ctx: &Ctx<R, W, C>,
    agreement_id: &IndexingAgreementId,
    indexing_request_id: &IndexingRequestId,
    deployment_id: &DeploymentId,
    deployment_chain_id: &ChainId,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
    C: IndexerClient,
{
    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        agreement_id=%agreement_id,
        "Marking indexing agreement as DELIVERY_FAILED"
    );
    ctx.registry
        .mark_indexing_agreement_as_delivery_failed(agreement_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    queue_reassessment(ctx, indexing_request_id, deployment_id, deployment_chain_id).await
}

/// Queue a reassessment job for the indexing request.
async fn queue_reassessment<R, W, C>(
    ctx: &Ctx<R, W, C>,
    indexing_request_id: &IndexingRequestId,
    deployment_id: &DeploymentId,
    deployment_chain_id: &ChainId,
) -> JobResult<()>
where
    R: IndexingRequestRegistry + AgreementRegistry,
    W: WorkerQueue,
    C: IndexerClient,
{
    tracing::trace!(
        indexing_request_id=%indexing_request_id,
        "Reassessing indexing request after failure"
    );
    let indexing_request = ctx
        .registry
        .get_indexing_request_by_id(indexing_request_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    let num_candidates = indexing_request
        .map(|r| r.num_candidates)
        .unwrap_or(DEFAULT_MAX_CANDIDATES);
    if let Err(err) = ctx
        .queue
        .reassess_indexing_request(
            *indexing_request_id,
            *deployment_id,
            *deployment_chain_id,
            num_candidates,
        )
        .await
    {
        tracing::error!(error=%err, "Failed to queue task: 'reassess_indexing_request'");
        return Err(JobError::Fatal(err));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use dipper_core::ids::IndexingRequestId;
    use thegraph_core::{DeploymentId, IndexerId, deployment_id, indexer_id};

    use super::*;
    use crate::{
        indexer_rpc_client::DipsError,
        registry::{
            IndexingAgreement, IndexingAgreementVoucher, IndexingAgreementVoucherMetadata,
            IndexingRequest, IndexingRequestStatus,
        },
        worker::queue::JobId,
    };

    // =========================================================================
    // Mock implementations
    // =========================================================================

    #[derive(Default)]
    struct MockRegistryState {
        agreement: Option<IndexingAgreement>,
        request: Option<IndexingRequest>,
        marked_rejected: Vec<IndexingAgreementId>,
        marked_failed: Vec<IndexingAgreementId>,
    }

    struct MockRegistry {
        state: Arc<Mutex<MockRegistryState>>,
    }

    impl MockRegistry {
        fn new(state: Arc<Mutex<MockRegistryState>>) -> Self {
            Self { state }
        }
    }

    #[async_trait]
    impl AgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<Option<IndexingAgreement>> {
            Ok(self.state.lock().unwrap().agreement.clone())
        }

        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> crate::registry::Result<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>
        {
            Ok(std::collections::HashMap::new())
        }

        async fn get_declined_indexers_by_deployment(
            &self,
            _lookback_days: i32,
        ) -> crate::registry::Result<std::collections::HashMap<DeploymentId, Vec<IndexerId>>>
        {
            Ok(std::collections::HashMap::new())
        }

        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn register_new_indexing_agreement(
            &self,
            _request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _indexer_id: IndexerId,
            _indexer_url: Url,
            _voucher: IndexingAgreementVoucher,
        ) -> crate::registry::Result<IndexingAgreementId> {
            Ok(IndexingAgreementId::new())
        }

        async fn mark_indexing_agreement_as_delivery_failed(
            &self,
            id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            self.state.lock().unwrap().marked_failed.push(*id);
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_indexer(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_accepted_on_chain(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn mark_indexing_agreement_as_expired(
            &self,
            _id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_rejected(
            &self,
            id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            self.state.lock().unwrap().marked_rejected.push(*id);
            Ok(())
        }
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn register_new_indexing_request(
            &self,
            _requested_by: thegraph_core::alloy::primitives::Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> crate::registry::Result<IndexingRequestId> {
            Ok(IndexingRequestId::new())
        }

        async fn get_all_indexing_requests(&self) -> crate::registry::Result<Vec<IndexingRequest>> {
            Ok(vec![])
        }

        async fn get_indexing_request_by_id(
            &self,
            _id: &IndexingRequestId,
        ) -> crate::registry::Result<Option<IndexingRequest>> {
            Ok(self.state.lock().unwrap().request.clone())
        }

        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> crate::registry::Result<Vec<IndexingRequest>> {
            Ok(vec![])
        }

        async fn mark_indexing_request_as_canceled(
            &self,
            _id: &IndexingRequestId,
        ) -> crate::registry::Result<()> {
            Ok(())
        }

        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingRequest>> {
            Ok(vec![])
        }
    }

    #[derive(Default)]
    struct MockQueueState {
        reassess_calls: Vec<(IndexingRequestId, DeploymentId, ChainId, usize)>,
    }

    struct MockQueue {
        state: Arc<Mutex<MockQueueState>>,
    }

    impl MockQueue {
        fn new(state: Arc<Mutex<MockQueueState>>) -> Self {
            Self { state }
        }
    }

    #[async_trait]
    impl WorkerQueue for MockQueue {
        async fn process_new_indexing_request(
            &self,
            _request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn send_indexing_agreement_proposal(
            &self,
            _indexer_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn reassess_indexing_request(
            &self,
            request_id: IndexingRequestId,
            deployment_id: DeploymentId,
            chain_id: ChainId,
            num_candidates: usize,
        ) -> anyhow::Result<JobId> {
            self.state.lock().unwrap().reassess_calls.push((
                request_id,
                deployment_id,
                chain_id,
                num_candidates,
            ));
            Ok(JobId::default())
        }

        async fn process_indexing_request_cancellation(
            &self,
            _request_id: IndexingRequestId,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn process_indexing_agreement_requester_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn process_indexing_agreement_indexer_cancellation(
            &self,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn send_indexing_agreement_cancellation(
            &self,
            _indexer_url: Url,
            _indexing_request_id: IndexingRequestId,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<JobId> {
            Ok(JobId::default())
        }
    }

    enum MockResponse {
        Accept,
        Reject,
        Fail,
    }

    struct MockIndexerClient {
        response: MockResponse,
    }

    impl MockIndexerClient {
        fn accepting() -> Self {
            Self {
                response: MockResponse::Accept,
            }
        }

        fn rejecting() -> Self {
            Self {
                response: MockResponse::Reject,
            }
        }

        fn failing() -> Self {
            Self {
                response: MockResponse::Fail,
            }
        }
    }

    #[async_trait]
    impl IndexerClient for MockIndexerClient {
        async fn send_indexing_agreement_proposal(
            &self,
            _indexer: &Url,
            _indexing_agreement_id: IndexingAgreementId,
            _voucher: IndexingAgreementVoucher,
        ) -> Result<ProposalResponse, DipsError> {
            match self.response {
                MockResponse::Accept => Ok(ProposalResponse::Accept),
                MockResponse::Reject => Ok(ProposalResponse::Reject),
                MockResponse::Fail => Err(DipsError::ConnectionError(
                    "connection failed".to_string().into(),
                )),
            }
        }

        async fn send_indexing_agreement_cancellation_notification(
            &self,
            _indexer: &Url,
            _indexing_agreement_id: IndexingAgreementId,
        ) -> Result<(), DipsError> {
            Ok(())
        }
    }

    fn make_test_agreement(
        id: IndexingAgreementId,
        status: IndexingAgreementStatus,
    ) -> IndexingAgreement {
        use thegraph_core::alloy::primitives::{Address, U256};
        use time::OffsetDateTime;

        IndexingAgreement {
            id,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status,
            indexing_request_id: IndexingRequestId::new(),
            indexer: crate::registry::Indexer {
                id: indexer_id!("1111111111111111111111111111111111111111"),
                url: "https://indexer.example.com".parse().unwrap(),
            },
            voucher: IndexingAgreementVoucher {
                payer: Address::ZERO,
                service_provider: Address::ZERO,
                data_service: Address::ZERO,
                deadline: 0,
                ends_at: 0,
                max_initial_tokens: U256::ZERO,
                max_ongoing_tokens_per_second: U256::ZERO,
                min_seconds_per_collection: 0,
                max_seconds_per_collection: 0,
                metadata: IndexingAgreementVoucherMetadata {
                    tokens_per_second: U256::ZERO,
                    tokens_per_entity_per_second: U256::ZERO,
                    subgraph_deployment_id: deployment_id!(
                        "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"
                    ),
                    protocol_network: 1,
                    chain_id: 1,
                },
            },
        }
    }

    fn make_test_request(id: IndexingRequestId) -> IndexingRequest {
        use thegraph_core::alloy::primitives::Address;
        use time::OffsetDateTime;

        IndexingRequest {
            id,
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status: IndexingRequestStatus::Open,
            requested_by: Address::ZERO,
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: 1,
            num_candidates: 3,
        }
    }

    fn test_job_meta() -> JobMeta {
        JobMeta {
            created_at: time::OffsetDateTime::now_utc(),
            failed_attempts: 0,
        }
    }

    // =========================================================================
    // Tests
    // =========================================================================

    #[tokio::test]
    async fn test_accept_response_leaves_agreement_created() {
        let agreement_id = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        let registry_state = Arc::new(Mutex::new(MockRegistryState {
            agreement: Some(make_test_agreement(
                agreement_id,
                IndexingAgreementStatus::Created,
            )),
            request: Some(make_test_request(request_id)),
            ..Default::default()
        }));
        let queue_state = Arc::new(Mutex::new(MockQueueState::default()));

        let ctx = Ctx {
            registry: MockRegistry::new(registry_state.clone()),
            queue: MockQueue::new(queue_state.clone()),
            indexer_client: MockIndexerClient::accepting(),
        };

        let message = Message {
            indexer_url: "https://indexer.example.com".parse().unwrap(),
            agreement_id,
            indexing_request_id: request_id,
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: 1,
        };

        let result = handle(ctx, &message, test_job_meta()).await;

        assert!(result.is_ok());
        // Should not mark rejected or failed
        assert!(registry_state.lock().unwrap().marked_rejected.is_empty());
        assert!(registry_state.lock().unwrap().marked_failed.is_empty());
        // Should not queue reassessment
        assert!(queue_state.lock().unwrap().reassess_calls.is_empty());
    }

    #[tokio::test]
    async fn test_reject_response_marks_rejected_and_reassesses() {
        let agreement_id = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        let registry_state = Arc::new(Mutex::new(MockRegistryState {
            agreement: Some(make_test_agreement(
                agreement_id,
                IndexingAgreementStatus::Created,
            )),
            request: Some(make_test_request(request_id)),
            ..Default::default()
        }));
        let queue_state = Arc::new(Mutex::new(MockQueueState::default()));

        let ctx = Ctx {
            registry: MockRegistry::new(registry_state.clone()),
            queue: MockQueue::new(queue_state.clone()),
            indexer_client: MockIndexerClient::rejecting(),
        };

        let message = Message {
            indexer_url: "https://indexer.example.com".parse().unwrap(),
            agreement_id,
            indexing_request_id: request_id,
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: 1,
        };

        let result = handle(ctx, &message, test_job_meta()).await;

        assert!(result.is_ok());
        // Should mark as rejected
        let state = registry_state.lock().unwrap();
        assert_eq!(state.marked_rejected.len(), 1);
        assert_eq!(state.marked_rejected[0], agreement_id);
        assert!(state.marked_failed.is_empty());
        drop(state);
        // Should queue reassessment
        let qstate = queue_state.lock().unwrap();
        assert_eq!(qstate.reassess_calls.len(), 1);
        assert_eq!(qstate.reassess_calls[0].0, request_id);
    }

    #[tokio::test]
    async fn test_network_error_marks_failed_and_reassesses() {
        let agreement_id = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        let registry_state = Arc::new(Mutex::new(MockRegistryState {
            agreement: Some(make_test_agreement(
                agreement_id,
                IndexingAgreementStatus::Created,
            )),
            request: Some(make_test_request(request_id)),
            ..Default::default()
        }));
        let queue_state = Arc::new(Mutex::new(MockQueueState::default()));

        let ctx = Ctx {
            registry: MockRegistry::new(registry_state.clone()),
            queue: MockQueue::new(queue_state.clone()),
            indexer_client: MockIndexerClient::failing(),
        };

        let message = Message {
            indexer_url: "https://indexer.example.com".parse().unwrap(),
            agreement_id,
            indexing_request_id: request_id,
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: 1,
        };

        let result = handle(ctx, &message, test_job_meta()).await;

        assert!(result.is_ok());
        // Should mark as failed (not rejected)
        let state = registry_state.lock().unwrap();
        assert!(state.marked_rejected.is_empty());
        assert_eq!(state.marked_failed.len(), 1);
        assert_eq!(state.marked_failed[0], agreement_id);
        drop(state);
        // Should queue reassessment
        let qstate = queue_state.lock().unwrap();
        assert_eq!(qstate.reassess_calls.len(), 1);
    }

    #[tokio::test]
    async fn test_non_created_status_skips_sending() {
        let agreement_id = IndexingAgreementId::new();
        let request_id = IndexingRequestId::new();

        let registry_state = Arc::new(Mutex::new(MockRegistryState {
            agreement: Some(make_test_agreement(
                agreement_id,
                IndexingAgreementStatus::AcceptedOnChain,
            )),
            request: Some(make_test_request(request_id)),
            ..Default::default()
        }));
        let queue_state = Arc::new(Mutex::new(MockQueueState::default()));

        let ctx = Ctx {
            registry: MockRegistry::new(registry_state.clone()),
            queue: MockQueue::new(queue_state.clone()),
            // This would reject, but should never be called
            indexer_client: MockIndexerClient::rejecting(),
        };

        let message = Message {
            indexer_url: "https://indexer.example.com".parse().unwrap(),
            agreement_id,
            indexing_request_id: request_id,
            deployment_id: deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"),
            deployment_chain_id: 1,
        };

        let result = handle(ctx, &message, test_job_meta()).await;

        assert!(result.is_ok());
        // Should not mark anything or queue reassessment
        assert!(registry_state.lock().unwrap().marked_rejected.is_empty());
        assert!(registry_state.lock().unwrap().marked_failed.is_empty());
        assert!(queue_state.lock().unwrap().reassess_calls.is_empty());
    }
}
