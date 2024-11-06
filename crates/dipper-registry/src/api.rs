use std::time::Duration;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use thegraph_core::{Address, AllocationId, DeploymentId, IndexerId, ProofOfIndexing};
use url::Url;

use super::{
    indexing_agreement::IndexingAgreement, indexing_receipt::IndexingReceipt,
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
    // TODO: Add price limit parameter
    async fn register_new_indexing_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
    ) -> Result<IndexingRequestId, Error>;

    /// Get all indexing requests.
    async fn get_all_indexing_requests(&self) -> Result<Vec<IndexingRequest>, Error>;

    /// Get the indexing request by ID.
    async fn get_indexing_request_by_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Option<IndexingRequest>, Error>;

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
        indexer_id: IndexerId,
        indexer_url: Url,
        duration: Duration,
    ) -> Result<IndexingAgreementId, Error>;

    /// Get agreement by ID.
    async fn get_indexing_agreement(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>, Error>;

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
        allocation_id: AllocationId,
        fee: i64, // TODO: Review fee field
    ) -> Result<IndexingReceiptId, Error>;

    /// Get all indexing receipts by indexing agreement ID.
    async fn get_all_indexing_receipts_by_indexing_agreement_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Vec<IndexingReceipt>, Error>;

    /// Get the indexing receipt by the given allocation ID.
    async fn get_indexing_receipt_by_allocation_id(
        &self,
        allocation_id: &AllocationId,
    ) -> Result<Option<IndexingReceipt>, Error>;

    /// Redeem an indexing receipt by providing the Proof-of-Indexing (POI).
    ///
    /// If the receipt is not found, or if the receipt is already redeemed, this method returns a
    /// [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    async fn redeem_indexing_receipt(
        &self,
        allocation_id: AllocationId,
        poi: ProofOfIndexing,
    ) -> Result<(), Error>;
}
