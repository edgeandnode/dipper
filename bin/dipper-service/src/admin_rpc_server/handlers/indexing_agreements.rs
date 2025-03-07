use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{
    ids::{IndexingAgreementId, IndexingRequestId},
    state::FromState,
};
use dipper_rpc::admin::{
    SignedMessage,
    indexing_agreements::{
        CancelIndexingAgreement, IndexingAgreement, IndexingAgreementsRpcServer,
        Status as IndexingAgreementStatus,
    },
};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::Address};

use crate::{
    registry::{
        AgreementRegistry, IndexingAgreement as IndexingAgreementRecord,
        IndexingAgreementStatus as IndexingAgreementRecordStatus,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::WorkerQueue,
};

/// The substate for the [`IndexingAgreementsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct IndexingAgreementsCtx<R, W> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub allowlist: Arc<BTreeSet<Address>>,
    pub registry: R,
    pub worker: W,
}

pub struct IndexingAgreementsRpcServerImpl<R, W>(IndexingAgreementsCtx<R, W>);

impl<R, W> IndexingAgreementsRpcServerImpl<R, W> {
    /// Create a new instance of the `IndexingAgreementsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        IndexingAgreementsCtx<R, W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

impl<R, W> std::ops::Deref for IndexingAgreementsRpcServerImpl<R, W> {
    type Target = IndexingAgreementsCtx<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<R, W> IndexingAgreementsRpcServer for IndexingAgreementsRpcServerImpl<R, W>
where
    R: AgreementRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn get_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> RpcResult<IndexingAgreement> {
        let indexing_agreement = match self
            .registry
            .get_indexing_agreement_by_id(&agreement_id)
            .await
        {
            Ok(Some(res)) => into_indexing_agreement(res),
            Ok(None) => {
                return Err(ErrorObject::borrowed(404, "Not found", None));
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreement by id");
                return Err(ErrorObject::borrowed(503, "Internal error", None));
            }
        };

        Ok(indexing_agreement)
    }

    async fn get_agreements_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        let indexing_agreements = match self
            .registry
            .get_indexing_agreements_by_deployment_id(&deployment_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreements by deployment id");
                return Err(ErrorObject::borrowed(503, "Internal error", None));
            }
        };

        Ok(indexing_agreements)
    }

    async fn get_agreements_by_indexer_id(
        &self,
        indexer_id: IndexerId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        let indexing_agreements = match self
            .registry
            .get_indexing_agreements_by_indexer_id(&indexer_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreements by indexer id");
                return Err(ErrorObject::borrowed(503, "Internal error", None));
            }
        };

        Ok(indexing_agreements)
    }

    async fn get_agreements_by_indexing_request_id(
        &self,
        request_id: IndexingRequestId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        let indexing_agreements = match self
            .registry
            .get_indexing_agreements_by_indexing_request_id(&request_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreements by indexer id");
                return Err(ErrorObject::borrowed(503, "Internal error", None));
            }
        };

        Ok(indexing_agreements)
    }

    async fn cancel_indexing_agreement(
        &self,
        req: SignedMessage<CancelIndexingAgreement>,
    ) -> RpcResult<()> {
        // Check if the signer is authorized to make this request
        let requested_by = match self.signer.recover_signer(&req) {
            Ok(requested_by) => requested_by,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                return Err(ErrorObject::borrowed(401, "Unauthorized", None));
            }
        };
        if !self.allowlist.contains(&requested_by) {
            return Err(ErrorObject::borrowed(403, "Forbidden", None));
        }

        let CancelIndexingAgreement { id: agreement_id } = req.into_message();

        // Check if the agreement exists
        match self
            .registry
            .get_indexing_agreement_by_id(&agreement_id)
            .await
        {
            Ok(None) => {
                return Err(ErrorObject::borrowed(404, "Not found", None));
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreement");
                return Err(ErrorObject::borrowed(503, "Internal error", None));
            }
            _ => {
                // The agreement exists, proceed with cancellation
            }
        }

        // Process the indexing request cancellation
        if let Err(err) = self
            .worker
            .process_indexing_agreement_requester_cancellation(agreement_id)
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'process_indexing_agreement_requester_cancellation'");
            return Err(ErrorObject::borrowed(500, "Internal error", None));
        };

        Ok(())
    }
}

#[inline]
fn into_indexing_agreement(agreement: IndexingAgreementRecord) -> IndexingAgreement {
    IndexingAgreement {
        id: agreement.id,
        created_at: agreement.created_at,
        updated_at: agreement.updated_at,
        status: into_indexing_agreement_status(agreement.status),
        indexing_request_id: agreement.indexing_request_id,
        indexer_id: agreement.indexer.id,
        indexer_url: agreement.indexer.url,
        duration_epochs: agreement.voucher.duration_epochs,
    }
}

#[inline]
fn into_indexing_agreement_status(
    status: IndexingAgreementRecordStatus,
) -> IndexingAgreementStatus {
    match status {
        IndexingAgreementRecordStatus::Created => IndexingAgreementStatus::Created,
        IndexingAgreementRecordStatus::DeliveryFailed => IndexingAgreementStatus::DeliveryFailed,
        IndexingAgreementRecordStatus::Accepted { .. } => IndexingAgreementStatus::Accepted,
        IndexingAgreementRecordStatus::Rejected => IndexingAgreementStatus::Rejected,
        IndexingAgreementRecordStatus::CanceledByRequester => {
            IndexingAgreementStatus::CanceledByRequester
        }
        IndexingAgreementRecordStatus::CanceledByIndexer => {
            IndexingAgreementStatus::CanceledByIndexer
        }
        IndexingAgreementRecordStatus::Expired => IndexingAgreementStatus::Expired,
    }
}
