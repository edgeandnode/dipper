mod agreement;
mod indexing_request;
mod receipt;
mod result;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use dipper_pgregistry::PgRegistry;
use sqlx::{Pool, Postgres};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId, U256},
};
use url::Url;

use self::result::Result as RegistryResult;
pub use self::{
    agreement::{
        AgreementRegistry, IndexingAgreement, Status as IndexingAgreementStatus,
        Voucher as IndexingAgreementVoucher, VoucherMetadata as IndexingAgreementVoucherMetadata,
    },
    indexing_request::{IndexingRequest, IndexingRequestRegistry, Status as IndexingRequestStatus},
    receipt::{IndexingReceipt, ReceiptRegistry, ReportedWork},
    result::{Error, Result},
};

/// A service for interacting with the registry.
///
/// This service provides a set of methods for interacting with the registry,
/// including registering new indexing requests, indexing agreements, and indexing receipts.
#[derive(Clone)]
pub struct RegistryProvider {
    inner: PgRegistry,
}

impl RegistryProvider {
    /// Creates a new registry service.
    pub fn new(db: Pool<Postgres>) -> Self {
        Self {
            inner: PgRegistry::new(db),
        }
    }
}

#[async_trait]
impl IndexingRequestRegistry for RegistryProvider {
    async fn register_new_indexing_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> RegistryResult<IndexingRequestId> {
        self.inner
            .register_new_indexing_request(requested_by, deployment_id, deployment_chain_id)
            .await
            .map_err(Into::into)
    }

    async fn get_all_indexing_requests(&self) -> RegistryResult<Vec<IndexingRequest>> {
        Ok(self
            .inner
            .get_all_indexing_requests()
            .await?
            .into_iter()
            .map(IndexingRequest::try_from)
            .filter_map(Result::ok)
            .collect())
    }

    async fn get_indexing_request_by_id(
        &self,
        id: &IndexingRequestId,
    ) -> RegistryResult<Option<IndexingRequest>> {
        Ok(self
            .inner
            .get_indexing_request_by_id(id)
            .await?
            .map(TryInto::try_into)
            .and_then(Result::ok))
    }

    async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingRequest>> {
        Ok(self
            .inner
            .get_indexing_requests_by_deployment_id(deployment_id)
            .await?
            .into_iter()
            .map(IndexingRequest::try_from)
            .filter_map(Result::ok)
            .collect())
    }

    async fn mark_indexing_request_as_canceled(
        &self,
        id: &IndexingRequestId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_request_as_canceled(id)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl AgreementRegistry for RegistryProvider {
    async fn get_indexing_agreement_by_id(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<Option<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreement_by_id(id)
            .await?
            .map(TryInto::try_into)
            .and_then(Result::ok))
    }

    async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreements_by_deployment_id(deployment_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(Result::ok)
            .collect())
    }

    async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreements_by_indexer_id(indexer_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(Result::ok)
            .collect())
    }

    async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
        Ok(self
            .inner
            .get_pending_agreement_indexers_by_deployment(indexer_ids)
            .await?)
    }

    async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_indexing_agreements_by_indexing_request_id(request_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(Result::ok)
            .collect())
    }
    async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_active_indexing_agreements_by_indexing_request_id(request_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(Result::ok)
            .collect())
    }
    async fn get_rejected_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_rejected_indexing_agreements_by_indexing_request_id(request_id)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(Result::ok)
            .collect())
    }

    async fn register_new_indexing_agreement(
        &self,
        request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        indexer_id: IndexerId,
        indexer_url: Url,
        voucher: IndexingAgreementVoucher,
    ) -> RegistryResult<IndexingAgreementId> {
        self.inner
            .register_new_indexing_agreement(
                request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                voucher.into(),
            )
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_delivery_failed(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_delivery_failed(id)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_accepted(
        &self,
        id: &IndexingAgreementId,
        epoch: u32,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_accepted(id, epoch)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_rejected(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_rejected(id)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_canceled_by_requester(id)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_canceled_by_indexer(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_canceled_by_indexer(id)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl ReceiptRegistry for RegistryProvider {
    async fn register_new_indexing_receipt(
        &self,
        agreement_id: IndexingAgreementId,
        indexer_id: IndexerId,
        indexer_operator_id: Address,
        reported_work: ReportedWork,
        amount: U256,
    ) -> RegistryResult<IndexingReceiptId> {
        self.inner
            .register_new_indexing_receipt(
                agreement_id,
                indexer_id,
                indexer_operator_id,
                reported_work.into(),
                amount,
            )
            .await
            .map_err(Into::into)
    }

    async fn get_last_receipt_for_agreement_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> RegistryResult<Option<IndexingReceipt>> {
        Ok(self
            .inner
            .get_last_receipt_for_agreement_id(agreement_id)
            .await?
            .map(Into::into))
    }
}
