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

use super::error_handling::{handle_list_result, handle_optional_result, require_exists};
use crate::{
    registry::{
        AgreementRegistry, IndexingAgreement as IndexingAgreementRecord,
        IndexingAgreementStatus as IndexingAgreementRecordStatus,
    },
    signing::eip712::PrivateKeyEip712Signer,
    worker::service::WorkerQueue,
};

/// The substate for the [`IndexingAgreementsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct Ctx<R, W> {
    pub signer: Arc<PrivateKeyEip712Signer>,
    pub gateway_operator_allowlist: Arc<BTreeSet<Address>>,
    pub registry: R,
    pub worker: W,
}

pub struct RpcServerImpl<R, W>(Ctx<R, W>);

impl<R, W> RpcServerImpl<R, W> {
    /// Create a new instance of the `IndexingAgreementsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        Ctx<R, W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

impl<R, W> std::ops::Deref for RpcServerImpl<R, W> {
    type Target = Ctx<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<R, W> IndexingAgreementsRpcServer for RpcServerImpl<R, W>
where
    R: AgreementRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
{
    async fn get_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> RpcResult<IndexingAgreement> {
        handle_optional_result(
            self.registry
                .get_indexing_agreement_by_id(&agreement_id)
                .await,
            "Failed to get indexing agreement by id",
            into_indexing_agreement,
        )
    }

    async fn get_agreements_by_deployment_id(
        &self,
        deployment_id: DeploymentId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        handle_list_result(
            self.registry
                .get_indexing_agreements_by_deployment_id(&deployment_id)
                .await,
            "Failed to get indexing agreements by deployment id",
            into_indexing_agreement,
        )
    }

    async fn get_agreements_by_indexer_id(
        &self,
        indexer_id: IndexerId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        handle_list_result(
            self.registry
                .get_indexing_agreements_by_indexer_id(&indexer_id)
                .await,
            "Failed to get indexing agreements by indexer id",
            into_indexing_agreement,
        )
    }

    async fn get_agreements_by_indexing_request_id(
        &self,
        request_id: IndexingRequestId,
    ) -> RpcResult<Vec<IndexingAgreement>> {
        handle_list_result(
            self.registry
                .get_indexing_agreements_by_indexing_request_id(&request_id)
                .await,
            "Failed to get indexing agreements by indexing request id",
            into_indexing_agreement,
        )
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
        if !self.gateway_operator_allowlist.contains(&requested_by) {
            return Err(ErrorObject::borrowed(403, "Forbidden", None));
        }

        let CancelIndexingAgreement { id: agreement_id } = req.into_message();

        tracing::debug!(%agreement_id, %requested_by, "Canceling indexing agreement");

        // Check if the agreement exists
        let agreement = require_exists(
            self.registry
                .get_indexing_agreement_by_id(&agreement_id)
                .await,
            "Failed to get indexing agreement",
        )?;

        // Process the indexing request cancellation
        if let Err(err) = self
            .worker
            .process_indexing_agreement_requester_cancellation(
                agreement.indexing_request_id,
                agreement.id,
            )
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
        deadline: agreement.voucher.deadline,
        ends_at: agreement.voucher.ends_at,
        rejection_reason: agreement.rejection_reason,
    }
}

#[inline]
fn into_indexing_agreement_status(
    status: IndexingAgreementRecordStatus,
) -> IndexingAgreementStatus {
    match status {
        IndexingAgreementRecordStatus::Created => IndexingAgreementStatus::Created,
        IndexingAgreementRecordStatus::DeliveryFailed => IndexingAgreementStatus::DeliveryFailed,
        IndexingAgreementRecordStatus::CanceledByRequester => {
            IndexingAgreementStatus::CanceledByRequester
        }
        IndexingAgreementRecordStatus::CanceledByIndexer => {
            IndexingAgreementStatus::CanceledByIndexer
        }
        IndexingAgreementRecordStatus::Expired => IndexingAgreementStatus::Expired,
        IndexingAgreementRecordStatus::AcceptedOnChain => IndexingAgreementStatus::AcceptedOnChain,
        IndexingAgreementRecordStatus::Rejected => IndexingAgreementStatus::Rejected,
        IndexingAgreementRecordStatus::AbandonedByIndexer => {
            IndexingAgreementStatus::AbandonedByIndexer
        }
    }
}
