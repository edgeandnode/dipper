use std::time::Duration;

use async_trait::async_trait;
use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use sqlx::{Pool, Postgres};
use thegraph_core::{
    alloy::primitives::Address, AllocationId, DeploymentId, IndexerId, ProofOfIndexing,
};
use url::Url;

use super::{
    api::{Error, Registry},
    indexing_agreement::{IndexingAgreement, Status as IndexingAgreementStatus},
    indexing_receipt::IndexingReceipt,
    indexing_request::{IndexingRequest, Status as IndexingRequestStatus},
};

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

#[async_trait]
impl Registry for PgRegistry {
    async fn register_new_indexing_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
    ) -> Result<IndexingRequestId, Error> {
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_requests (
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id
            )
            VALUES ($1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(IndexingRequestId::new())
        .bind(IndexingRequestStatus::default())
        .bind(format!("{:#x}", requested_by))
        .bind(format!("{}", deployment_id))
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    async fn get_all_indexing_requests(&self) -> Result<Vec<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id
            FROM dipper_reg_indexing_requests
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn get_indexing_request_by_id(
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
                deployment_id
            FROM dipper_reg_indexing_requests
            WHERE id = $1
            "#,
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn get_all_indexing_requests_by_deployment_id(
        &self,
        _deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingRequest>, Error> {
        todo!("Return all indexing requests associated with a deployment id");
    }

    async fn get_indexing_request_active_indexing_agreements(
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
                indexer_id,
                indexer_url,
                duration
            FROM dipper_reg_indexing_agreements
            WHERE indexing_request_id = $1 AND status IN ($2, $3)
            ORDER BY id ASC
            "#,
        )
        .bind(request_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::Accepted)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn get_indexing_request_rejected_indexing_agreements(
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
                indexer_id,
                indexer_url,
                duration
            FROM dipper_reg_indexing_agreements
            WHERE id = $1 AND status IN ($2, $3)
            ORDER BY id ASC
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
    async fn mark_indexing_request_as_canceled(
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

    async fn register_new_indexing_agreement(
        &self,
        request_id: IndexingRequestId,
        indexer_id: IndexerId,
        indexer_url: Url,
        duration: Duration,
    ) -> Result<IndexingAgreementId, Error> {
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_agreements (
                id,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                indexer_id,
                indexer_url,
                duration
            )
            VALUES ($1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4, $5, $6)
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementId::new())
        .bind(IndexingAgreementStatus::default())
        .bind(request_id)
        .bind(format!("{:#x}", indexer_id))
        .bind(indexer_url.as_str())
        .bind::<i64>(duration.as_secs().try_into().expect("Duration overflow"))
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    async fn get_indexing_agreement_by_id(
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
                indexing_request_id,
                indexer_id,
                indexer_url,
                duration
            FROM dipper_reg_indexing_agreements
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn get_all_indexing_agreements_by_deployment_id(
        &self,
        _deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        todo!("Return all indexing agreements by deployment ID")
    }

    async fn get_all_indexing_agreements_by_indexer_id(
        &self,
        _indexer_id: &IndexerId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        todo!("Return all indexing agreements by Indexer ID")
    }

    async fn get_all_indexing_agreements_by_indexing_request_id(
        &self,
        _request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        todo!("Return all indexing agreements by Indexing Request ID");
    }

    async fn mark_indexing_agreement_as_delivery_failed(
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

    async fn mark_indexing_agreement_as_accepted(
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
        .bind(IndexingAgreementStatus::Accepted)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    async fn mark_indexing_agreement_as_rejected(
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

    async fn mark_indexing_agreement_as_canceled_by_requester(
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

    async fn mark_indexing_agreement_as_canceled_by_indexer(
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

    async fn mark_indexing_agreement_as_expired(
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
        .bind(IndexingAgreementStatus::Expired)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Accepted)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    async fn register_new_indexing_receipt(
        &self,
        agreement_id: IndexingAgreementId,
        allocation_id: AllocationId,
        fee: i64,
    ) -> Result<IndexingReceiptId, Error> {
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_receipts (
                id,
                created_at,
                updated_at,
                indexing_agreement_id,
                allocation_id,
                fee,
                poi
            )
            VALUES ($1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4, NULL)
            RETURNING id
            "#,
        )
        .bind(IndexingReceiptId::new())
        .bind(agreement_id)
        .bind(format!("{:#x}", allocation_id))
        .bind(fee)
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    async fn get_all_indexing_receipts_by_indexing_agreement_id(
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
                allocation_id,
                fee,
                poi
            FROM dipper_reg_indexing_receipts
            WHERE indexing_agreement_id = $1
            "#,
        )
        .bind(agreement_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn get_indexing_receipt_by_allocation_id(
        &self,
        allocation_id: &AllocationId,
    ) -> Result<Option<IndexingReceipt>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                indexing_agreement_id,
                allocation_id,
                fee,
                poi
            FROM dipper_reg_indexing_receipts
            WHERE allocation_id = $1
            RETURNING *
            "#,
        )
        .bind(format!("{:#x}", allocation_id))
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    async fn redeem_indexing_receipt(
        &self,
        allocation_id: AllocationId,
        poi: ProofOfIndexing,
    ) -> Result<(), Error> {
        let record: Option<(IndexingReceiptId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_receipts
            SET
                poi = $1,
                updated_at = timezone('UTC', now())
            WHERE allocation_id = $3 AND poi IS NULL
            RETURNING id
            "#,
        )
        .bind(format!("{}", poi))
        .bind(format!("{:#x}", allocation_id))
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }
}
