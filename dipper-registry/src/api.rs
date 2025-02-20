use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use thegraph_core::{
    alloy::primitives::{Address, ChainId, U256},
    DeploymentId, IndexerId,
};
use url::Url;

use super::{
    indexing_agreement::{IndexingAgreement, Voucher},
    indexing_receipt::{IndexingReceipt, ReportedWork},
    indexing_request::IndexingRequest,
};

/// Errors that can occur when interacting with the [`Registry`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The DB update query failed as no records matching the criteria were found.
    #[error("No records were updated")]
    NoRecordsUpdated,

    /// An error occurred while interacting with the database.
    #[error(transparent)]
    DbError(#[from] sqlx::Error),
}

/// The registry trait.
#[async_trait]
pub trait Registry {
    /// Register a new indexing request.
    ///
    /// If successful, the method returns the ID of the newly created indexing request.
    async fn register_new_indexing_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> Result<IndexingRequestId, Error>;

    /// Get all indexing requests.
    async fn get_all_indexing_requests(&self) -> Result<Vec<IndexingRequest>, Error>;

    /// Get the indexing request by ID.
    async fn get_indexing_request_by_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Option<IndexingRequest>, Error>;

    /// Get all indexing requests by Deployment ID
    async fn get_all_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingRequest>, Error>;

    /// Get the active agreements for an indexing request.
    ///
    /// Agreements are considered active if they are in `CREATED` or `ACCEPTED` status.
    async fn get_indexing_request_active_indexing_agreements(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error>;

    /// Get the rejected (and canceled by indexer) agreements for an indexing request.
    ///
    /// Agreements are considered rejected if they are in `REJECTED` or `CANCELLED_BY_INDEXER` status.
    async fn get_indexing_request_rejected_indexing_agreements(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error>;

    /// Mark an indexing request as `CANCELED`.
    ///
    /// If there is no indexing request with the given ID, or if the request is not in the
    /// `OPEN` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_request_as_canceled(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<(), Error>;

    /// Register a new indexing agreement.
    async fn register_new_indexing_agreement(
        &self,
        request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        indexer_id: IndexerId,
        indexer_url: Url,
        voucher: Voucher,
    ) -> Result<IndexingAgreementId, Error>;

    /// Get agreement by ID.
    async fn get_indexing_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>, Error>;

    /// Get all agreements by deployment ID.
    async fn get_all_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>, Error>;

    /// Get all agreements by indexer ID.
    async fn get_all_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> Result<Vec<IndexingAgreement>, Error>;

    /// Get all agreements by associated indexing request ID.
    async fn get_all_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error>;

    /// Mark an indexing agreement as `DELIVERY_FAILED`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_delivery_failed(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error>;

    /// Mark an indexing agreement as `ACCEPTED`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_accepted(
        &self,
        agreement_id: &IndexingAgreementId,
        epoch: u32,
    ) -> Result<(), Error>;

    /// Mark an indexing agreement as `REJECTED`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_rejected(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error>;

    /// Mark an indexing agreement as `CANCELED_BY_REQUESTER`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `CREATED` or `ACCEPTED` state, this method returns a
    /// [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error>;

    /// Mark an indexing agreement as `CANCELED_BY_INDEXER`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `ACCEPTED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_canceled_by_indexer(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error>;

    /// Mark an indexing agreement as `EXPIRED`.
    ///
    /// If there is no indexing agreement with the given ID, or if the agreement is not in the
    /// `ACCEPTED` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn mark_indexing_agreement_as_expired(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error>;

    /// Register a new indexing receipt.
    async fn register_new_indexing_receipt(
        &self,
        agreement_id: IndexingAgreementId,
        indexer_id: IndexerId,
        indexer_operator_id: Address,
        reported_work: ReportedWork,
        amount: U256,
    ) -> Result<IndexingReceiptId, Error>;

    /// Get all indexing receipts by indexing agreement ID.
    async fn get_all_indexing_receipts_by_indexing_agreement_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Vec<IndexingReceipt>, Error>;

    /// Get the indexing receipt by the given indexer ID.
    async fn get_indexing_receipt_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> Result<Option<IndexingReceipt>, Error>;

    /// Get the latest receipt for the given agreement ID.
    async fn get_last_receipt_for_agreement(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Option<IndexingReceipt>, Error>;
}
