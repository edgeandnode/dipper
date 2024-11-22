use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dipper_core::{
    ids::{IndexingAgreementId, IndexingRequestId},
    rpc::indexing_agreements::{
        AdminIndexingAgreementsRpcServer, CancelIndexingAgreement,
        IndexerIndexingAgreementsRpcServer, IndexingAgreement, IndexingAgreementsRpcServer,
        Status as IndexingAgreementStatus,
    },
    signed_message::serde::SignedMessage,
    state::FromState,
};
use dipper_pgmq::queue::Queue;
use dipper_registry::{
    IndexingAgreement as IndexingAgreementRecord,
    IndexingAgreementStatus as IndexingAgreementRecordStatus, Registry,
};
use jsonrpsee::core::RpcResult;
use thegraph_core::{alloy::primitives::Address, DeploymentId, IndexerId};

use crate::{
    rpc_server::Ctx,
    signer::PrivateKeyEip712Signer,
    worker::messages::{Message, ProcessIndexingAgreementCancellation},
};

/// The substate for the [`IndexingAgreementsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct IndexingAgreementsCtx<R> {
    registry: R,
}

impl<R, W> FromState<Ctx<R, W>> for IndexingAgreementsCtx<R>
where
    R: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            registry: ctx.registry.clone(),
        }
    }
}

pub struct IndexingAgreementsRpcServerImpl<R>(IndexingAgreementsCtx<R>);

impl<R> IndexingAgreementsRpcServerImpl<R> {
    /// Create a new instance of the `IndexingAgreementsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        IndexingAgreementsCtx<R>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

impl<R> std::ops::Deref for IndexingAgreementsRpcServerImpl<R> {
    type Target = IndexingAgreementsCtx<R>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<R> IndexingAgreementsRpcServer for IndexingAgreementsRpcServerImpl<R>
where
    R: Registry + Clone + Send + Sync + 'static,
{
    async fn get_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> RpcResult<IndexingAgreement> {
        let indexing_agreement = match self.registry.get_indexing_agreement(agreement_id).await {
            Ok(Some(res)) => into_indexing_agreement(res),
            Ok(None) => {
                // return Err(StatusCode::NOT_FOUND);
                todo!("Return error");
            }
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreement by id");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
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
            .get_all_indexing_agreements_by_deployment_id(&deployment_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreements by deployment id");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
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
            .get_all_indexing_agreements_by_indexer_id(&indexer_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreements by indexer id");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
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
            .get_all_indexing_agreements_by_indexing_request_id(&request_id)
            .await
        {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get indexing agreements by indexer id");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
            }
        };

        Ok(indexing_agreements)
    }
}

/// The substate for the [`AdminIndexingAgreementsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct AdminIndexingAgreementsCtx<R, W> {
    signer: Arc<PrivateKeyEip712Signer>,
    allowlist: Arc<BTreeSet<Address>>,
    registry: R,
    worker: W,
}

impl<R, W> FromState<Ctx<R, W>> for AdminIndexingAgreementsCtx<R, W>
where
    R: Clone,
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            allowlist: ctx.allowlist.clone(),
            registry: ctx.registry.clone(),
            worker: ctx.worker.clone(),
        }
    }
}

pub struct AdminIndexingAgreementsRpcServerImpl<R, W>(AdminIndexingAgreementsCtx<R, W>);

impl<R, W> AdminIndexingAgreementsRpcServerImpl<R, W> {
    /// Create a new instance of the `AdminIndexingAgreementsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        AdminIndexingAgreementsCtx<R, W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

impl<R, W> std::ops::Deref for AdminIndexingAgreementsRpcServerImpl<R, W> {
    type Target = AdminIndexingAgreementsCtx<R, W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<R, W> AdminIndexingAgreementsRpcServer for AdminIndexingAgreementsRpcServerImpl<R, W>
where
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    async fn get_all_indexing_agreements(&self) -> RpcResult<Vec<IndexingAgreement>> {
        let agreements = match self.registry.get_all_indexing_agreements().await {
            Ok(res) => res.into_iter().map(into_indexing_agreement).collect(),
            Err(err) => {
                tracing::error!(error=?err, "Failed to get all indexing agreements");
                // return Err(StatusCode::INTERNAL_SERVER_ERROR);
                todo!("Return error");
            }
        };

        Ok(agreements)
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
                // return Err(StatusCode::UNAUTHORIZED);
                todo!("Return error");
            }
        };
        if !self.allowlist.contains(&requested_by) {
            // return Err(StatusCode::FORBIDDEN);
            todo!("Return error");
        }

        let CancelIndexingAgreement { id: agreement_id } = req.into_message();

        // Process the indexing request cancellation
        if let Err(err) = self
            .worker
            .push(Message::ProcessIndexingAgreementRequesterCancellation(
                ProcessIndexingAgreementCancellation { agreement_id },
            ))
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'process_indexing_agreement_requester_cancellation'");
            // return Err(StatusCode::INTERNAL_ERROR);
            todo!("Return error")
        };

        Ok(())
    }
}

/// The substate for the [`IndexerIndexingAgreementsRpc`] handler
///
/// See: https://docs.rs/axum/0.7.7/axum/extract/struct.State.html#substates
pub struct IndexerIndexingAgreementsCtx<W> {
    signer: Arc<PrivateKeyEip712Signer>,
    allowlist: Arc<BTreeSet<Address>>,
    worker: W,
}

impl<R, W> FromState<Ctx<R, W>> for IndexerIndexingAgreementsCtx<W>
where
    W: Clone,
{
    fn from_state(ctx: &Ctx<R, W>) -> Self {
        Self {
            signer: ctx.signer.clone(),
            allowlist: ctx.allowlist.clone(),
            worker: ctx.worker.clone(),
        }
    }
}

pub struct IndexerIndexingAgreementsRpcServerImpl<W>(IndexerIndexingAgreementsCtx<W>);

impl<W> IndexerIndexingAgreementsRpcServerImpl<W> {
    /// Create a new instance of the `IndexerIndexingAgreementsRpcServerImpl` with the given context
    pub fn with_context<C>(ctx: &C) -> Self
    where
        IndexerIndexingAgreementsCtx<W>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

impl<W> std::ops::Deref for IndexerIndexingAgreementsRpcServerImpl<W> {
    type Target = IndexerIndexingAgreementsCtx<W>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl<W> IndexerIndexingAgreementsRpcServer for IndexerIndexingAgreementsRpcServerImpl<W>
where
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    async fn cancel_indexing_agreement(
        &self,
        req: SignedMessage<CancelIndexingAgreement>,
    ) -> RpcResult<()> {
        // Check if the signer is authorized to make this request
        let requested_by = match self.signer.recover_signer(&req) {
            Ok(requested_by) => requested_by,
            Err(err) => {
                tracing::debug!(error=?err, "Failed to recover signer");
                // return Err(StatusCode::UNAUTHORIZED);
                todo!("Return error");
            }
        };
        if !self.allowlist.contains(&requested_by) {
            // return Err(StatusCode::FORBIDDEN);
            todo!("Return error");
        }

        let CancelIndexingAgreement { id: agreement_id } = req.into_message();

        // Process the indexing request cancellation
        if let Err(err) = self
            .worker
            .push(Message::ProcessIndexingAgreementIndexerCancellation(
                ProcessIndexingAgreementCancellation { agreement_id },
            ))
            .await
        {
            tracing::error!(error=?err, "Failed to queue task: 'process_indexing_agreement_indexer_cancellation'");
            // return Err(StatusCode::INTERNAL_ERROR);
            todo!("Return error")
        };

        Ok(())
    }
}

fn into_indexing_agreement(agreement: IndexingAgreementRecord) -> IndexingAgreement {
    IndexingAgreement {
        id: agreement.id,
        created_at: agreement.created_at,
        updated_at: agreement.updated_at,
        status: into_indexing_agreement_status(agreement.status),
        indexing_request_id: agreement.indexing_request_id,
        indexer_id: agreement.indexer_id,
        indexer_url: agreement.indexer_url,
        duration: agreement.duration,
    }
}

fn into_indexing_agreement_status(
    status: IndexingAgreementRecordStatus,
) -> IndexingAgreementStatus {
    match status {
        IndexingAgreementRecordStatus::Created => IndexingAgreementStatus::Created,
        IndexingAgreementRecordStatus::DeliveryFailed => IndexingAgreementStatus::DeliveryFailed,
        IndexingAgreementRecordStatus::Accepted => IndexingAgreementStatus::Accepted,
        IndexingAgreementRecordStatus::Rejected => IndexingAgreementStatus::Rejected,
        IndexingAgreementRecordStatus::CanceledByRequester => {
            IndexingAgreementStatus::CanceledByRequester
        }
        IndexingAgreementRecordStatus::CanceledByIndexer => {
            IndexingAgreementStatus::CanceledByIndexer
        }
        IndexingAgreementRecordStatus::Expired => IndexingAgreementStatus::Expired,
        IndexingAgreementRecordStatus::Unknown => IndexingAgreementStatus::Unknown,
    }
}
