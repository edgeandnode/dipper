mod agreement;
mod indexer_denylist;
mod indexing_request;
mod result;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_pgregistry::PgRegistry;
use sqlx::{Pool, Postgres};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId},
};
use url::Url;

// Re-export for tests only
#[cfg(test)]
pub use self::agreement::Indexer;
use self::result::Result as RegistryResult;
pub use self::{
    agreement::{
        AgreementRegistry, IndexingAgreement, Status as IndexingAgreementStatus,
        Voucher as IndexingAgreementVoucher, VoucherMetadata as IndexingAgreementVoucherMetadata,
    },
    indexer_denylist::IndexerDenylistRegistry,
    indexing_request::{IndexingRequest, IndexingRequestRegistry, Status as IndexingRequestStatus},
    result::{Error, Result},
};

/// Filter and log conversion errors instead of silently dropping them.
///
/// This is a replacement for `.filter_map(filter_map_with_logging)` that logs warnings
/// when conversions fail, making debugging easier.
fn filter_map_with_logging<T, E: std::fmt::Display>(
    result: std::result::Result<T, E>,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::warn!(error = %e, "skipping record with conversion error");
            None
        }
    }
}

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
        num_candidates: usize,
    ) -> RegistryResult<IndexingRequestId> {
        self.inner
            .register_new_indexing_request(
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates as i32,
            )
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
            .filter_map(filter_map_with_logging)
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
            .filter_map(filter_map_with_logging)
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

    async fn get_open_indexing_requests_for_reassessment(
        &self,
        min_age_seconds: i64,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingRequest>> {
        Ok(self
            .inner
            .get_open_indexing_requests_for_reassessment(min_age_seconds, batch_size)
            .await?
            .into_iter()
            .map(IndexingRequest::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
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
            .filter_map(filter_map_with_logging)
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
            .filter_map(filter_map_with_logging)
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

    async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        signer_lookback_minutes: i32,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
        Ok(self
            .inner
            .get_declined_indexers_by_deployment(
                default_lookback_days,
                price_lookback_days,
                signer_lookback_minutes,
            )
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
            .filter_map(filter_map_with_logging)
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
            .filter_map(filter_map_with_logging)
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

    async fn mark_indexing_agreement_as_accepted_on_chain(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_accepted_on_chain(id)
            .await
            .map_err(Into::into)
    }

    async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_expired_created_agreements(batch_size)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn mark_indexing_agreement_as_expired(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_expired(id)
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_rejected(
        &self,
        id: &IndexingAgreementId,
        rejection_reason: Option<&str>,
    ) -> RegistryResult<()> {
        self.inner
            .mark_indexing_agreement_as_rejected(id, rejection_reason)
            .await
            .map_err(Into::into)
    }

    async fn get_accepted_on_chain_agreements(
        &self,
        batch_size: i64,
    ) -> RegistryResult<Vec<IndexingAgreement>> {
        Ok(self
            .inner
            .get_accepted_on_chain_agreements(batch_size)
            .await?
            .into_iter()
            .map(IndexingAgreement::try_from)
            .filter_map(filter_map_with_logging)
            .collect())
    }

    async fn update_agreement_sync_progress(
        &self,
        id: &IndexingAgreementId,
        block_height: u64,
        progress_at: time::OffsetDateTime,
    ) -> RegistryResult<()> {
        self.inner
            .update_agreement_sync_progress(id, block_height, progress_at)
            .await
            .map_err(Into::into)
    }

    async fn count_active_agreements_by_deployment(
        &self,
    ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>> {
        self.inner
            .count_active_agreements_by_deployment()
            .await
            .map_err(Into::into)
    }

    async fn mark_indexing_agreement_as_abandoned(
        &self,
        id: &IndexingAgreementId,
    ) -> RegistryResult<IndexingAgreement> {
        let raw = self.inner.mark_indexing_agreement_as_abandoned(id).await?;
        // The conversion only fails for Unknown status; since we just wrote
        // AbandonedByIndexer, this cannot fail in practice.
        IndexingAgreement::try_from(raw)
            .map_err(|_| dipper_pgregistry::Error::NoRecordsUpdated.into())
    }
}

#[async_trait]
impl IndexerDenylistRegistry for RegistryProvider {
    async fn get_indexer_denylist(&self) -> RegistryResult<Vec<IndexerId>> {
        self.inner.get_indexer_denylist().await.map_err(Into::into)
    }
}

#[async_trait]
impl crate::network::service::chain_listener::ChainListenerStateRegistry for RegistryProvider {
    async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> RegistryResult<Option<crate::network::service::chain_listener::ChainListenerState>> {
        Ok(self.inner.get_chain_listener_state(chain_id).await?.map(
            |(chain_id, last_processed_block)| {
                crate::network::service::chain_listener::ChainListenerState {
                    _chain_id: chain_id,
                    last_processed_block,
                }
            },
        ))
    }

    async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        last_processed_block: u64,
    ) -> RegistryResult<()> {
        self.inner
            .update_chain_listener_state(chain_id, last_processed_block)
            .await
            .map_err(Into::into)
    }
}
