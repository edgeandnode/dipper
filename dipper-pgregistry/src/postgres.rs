//! PostgreSQL implementation of the registry

use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use sqlx::{Pool, Postgres};
use thegraph_core::{
    alloy::primitives::{Address, ChainId, U256},
    DeploymentId, IndexerId,
};
use url::Url;

use self::common::{
    PgAddress, PgAllocationId, PgDeploymentId, PgIndexerId, PgProofOfIndexing, PgU256, PgU32,
    PgU64, PgUrl,
};
use super::{
    indexing_agreement::{IndexingAgreement, Status as IndexingAgreementStatus, Voucher},
    indexing_receipt::IndexingReceipt,
    indexing_request::{IndexingRequest, Status as IndexingRequestStatus},
    result::Error,
    IndexingReceiptReportedWork,
};

mod common;
mod indexing_agreement;
mod indexing_receipt;
mod indexing_request;

/// A registry that stores indexing requests, agreements, and receipts in a PostgreSQL database.
#[derive(Clone)]
pub struct PgRegistry {
    pool: Pool<Postgres>,
}

impl PgRegistry {
    /// Create a new instance of the registry with the given PostgreSQL connection pool.
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

impl PgRegistry {
    pub async fn register_new_indexing_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> Result<IndexingRequestId, Error> {
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_requests (
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id
            )
            VALUES ($1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(IndexingRequestId::new())
        .bind(IndexingRequestStatus::default())
        .bind(PgAddress(requested_by))
        .bind(PgDeploymentId(deployment_id))
        .bind(PgU64(deployment_chain_id))
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    pub async fn get_all_indexing_requests(&self) -> Result<Vec<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id
            FROM dipper_reg_indexing_requests
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_request_by_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Option<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id
            FROM dipper_reg_indexing_requests
            WHERE id = $1
            "#,
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id
            FROM dipper_reg_indexing_requests
            WHERE deployment_id = $1
            "#,
        )
        .bind(PgDeploymentId(*deployment_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            FROM dipper_reg_indexing_agreements
            WHERE indexing_request_id = $1 AND status IN ($2, $3)
            "#,
        )
        .bind(request_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::Accepted)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_request_rejected_indexing_agreements(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            FROM dipper_reg_indexing_agreements
            WHERE id = $1 AND status IN ($2, $3)
            "#,
        )
        .bind(request_id)
        .bind(IndexingAgreementStatus::Rejected)
        .bind(IndexingAgreementStatus::CanceledByIndexer)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Mark an indexing request as `CANCELED`.
    ///
    /// If there is no indexing request with the given ID, or if the request is not in the
    /// `OPEN` state, this method returns a [`NoRecordUpdated`](Error::NoRecordsUpdated) error.
    pub async fn mark_indexing_request_as_canceled(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<(), Error> {
        let request_id: Option<(IndexingRequestId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_requests
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingRequestStatus::Canceled)
        .bind(request_id)
        .bind(IndexingRequestStatus::Open)
        .fetch_optional(&self.pool)
        .await?;

        if request_id.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn register_new_indexing_agreement(
        &self,
        request_id: IndexingRequestId,
        deployment_id: DeploymentId,
        indexer_id: IndexerId,
        indexer_url: Url,
        voucher: Voucher,
    ) -> Result<IndexingAgreementId, Error> {
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_agreements (
                id,
                created_at,
                updated_at,
                status,
                accepted_at_epoch,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            )
            VALUES (
                $1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementId::new())
        .bind(IndexingAgreementStatus::default())
        .bind(None::<PgU32>)
        .bind(request_id)
        .bind(PgDeploymentId(deployment_id))
        .bind(PgIndexerId(indexer_id))
        .bind(PgUrl(indexer_url))
        .bind(PgAddress(voucher.payer))
        .bind(PgAddress(voucher.recipient))
        .bind(PgAddress(voucher.service))
        .bind(PgU32(voucher.duration_epochs))
        .bind(PgU256(voucher.max_initial_amount))
        .bind(PgU256(voucher.max_ongoing_amount_per_epoch))
        .bind(PgU32(voucher.min_epochs_per_collection))
        .bind(PgU32(voucher.max_epochs_per_collection))
        .bind(PgU64(voucher.deadline))
        .bind(PgU256(voucher.metadata.base_price_per_epoch))
        .bind(PgU256(voucher.metadata.price_per_entity))
        .bind(PgDeploymentId(voucher.metadata.subgraph_deployment_id))
        .bind(PgU64(voucher.metadata.protocol_network))
        .bind(PgU64(voucher.metadata.chain_id))
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreement_by_id(
        &self,
        agreement_id: IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                accepted_at_epoch,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            FROM dipper_reg_indexing_agreements
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                accepted_at_epoch,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            FROM dipper_reg_indexing_agreements
            WHERE deployment_id = $1
            "#,
        )
        .bind(PgDeploymentId(*deployment_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                accepted_at_epoch,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            FROM dipper_reg_indexing_agreements
            WHERE indexer_id = $1
            "#,
        )
        .bind(PgIndexerId(*indexer_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                accepted_at_epoch,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,

                voucher_payer,
                voucher_recipient,
                voucher_service,
                voucher_duration_epochs,
                voucher_max_initial_amount,
                voucher_max_ongoing_amount_per_epoch,
                voucher_min_epochs_per_collection,
                voucher_max_epochs_per_collection,
                voucher_deadline,
                voucher_metadata_base_price_per_epoch,
                voucher_metadata_price_per_entity,
                voucher_metadata_deployment_id,
                voucher_metadata_protocol_network,
                voucher_metadata_chain_id
            FROM dipper_reg_indexing_agreements
            WHERE indexing_request_id = $1
            "#,
        )
        .bind(request_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn mark_indexing_agreement_as_delivery_failed(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::DeliveryFailed)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn mark_indexing_agreement_as_accepted(
        &self,
        agreement_id: &IndexingAgreementId,
        epoch: u32,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                accepted_at_epoch = $2,
                updated_at = timezone('UTC', now())
            WHERE id = $3 AND status = $4
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::Accepted)
        .bind(Some(PgU32(epoch)))
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn mark_indexing_agreement_as_rejected(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::Rejected)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status IN ($3, $4)
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByRequester)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::Accepted)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn mark_indexing_agreement_as_canceled_by_indexer(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByIndexer)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Accepted)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn register_new_indexing_receipt(
        &self,
        agreement_id: IndexingAgreementId,
        indexer_id: IndexerId,
        indexer_operator_id: Address,
        reported_work: IndexingReceiptReportedWork,
        amount: U256,
    ) -> Result<IndexingReceiptId, Error> {
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_receipts (
                id,
                created_at,
                updated_at,
                indexing_agreement_id,
                indexer_id,
                indexer_operator_id,
                reported_work_epoch,
                reported_work_allocation_id,
                reported_work_entity_count,
                reported_work_poi,
                amount
            )
            VALUES (
                $1, timezone('UTC', now()), timezone('UTC', now()),
                $2, $3, $4, $5, $6, $7, $8, $9
            )
            RETURNING id
            "#,
        )
        .bind(IndexingReceiptId::new())
        .bind(agreement_id)
        .bind(PgIndexerId(indexer_id))
        .bind(PgAddress(indexer_operator_id))
        .bind(PgU32(reported_work.epoch))
        .bind(PgAllocationId(reported_work.allocation_id))
        .bind(PgU64(reported_work.entity_count))
        .bind(PgProofOfIndexing(reported_work.poi))
        .bind(PgU256(amount))
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    pub async fn get_all_indexing_receipts_by_indexing_agreement_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Vec<IndexingReceipt>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                indexing_agreement_id,
                indexer_id,
                indexer_operator_id,
                reported_work_epoch,
                reported_work_allocation_id,
                reported_work_entity_count,
                reported_work_poi,
                amount
            FROM dipper_reg_indexing_receipts
            WHERE indexing_agreement_id = $1
            "#,
        )
        .bind(agreement_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_last_receipt_for_agreement_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Option<IndexingReceipt>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                indexing_agreement_id,
                indexer_id,
                indexer_operator_id,
                reported_work_epoch,
                reported_work_allocation_id,
                reported_work_entity_count,
                reported_work_poi,
                amount
            FROM dipper_reg_indexing_receipts
            WHERE indexing_agreement_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(agreement_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }
}
