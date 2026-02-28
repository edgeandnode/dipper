//! PostgreSQL implementation of the registry

use std::collections::HashMap;

use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use sqlx::{Pool, Postgres, types::Json};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId, U256},
};
use url::Url;

use self::common::{
    PgAddress, PgAllocationId, PgDeploymentId, PgIndexerId, PgProofOfIndexing, PgU32, PgU64,
    PgU256, PgUrl,
};
use super::{
    IndexingReceiptReportedWork,
    indexing_agreement::{IndexingAgreement, Status as IndexingAgreementStatus, Voucher},
    indexing_receipt::IndexingReceipt,
    indexing_request::{IndexingRequest, Status as IndexingRequestStatus},
    result::Error,
};

pub(crate) mod common;
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
        num_candidates: i32,
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
                deployment_chain_id,
                num_candidates
            )
            VALUES ($1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4, $5, $6)
            RETURNING id
            "#,
        )
        .bind(IndexingRequestId::new())
        .bind(IndexingRequestStatus::default())
        .bind(PgAddress(requested_by))
        .bind(PgDeploymentId(deployment_id))
        .bind(PgU64(deployment_chain_id))
        .bind(num_candidates)
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
                deployment_chain_id,
                num_candidates
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
                deployment_chain_id,
                num_candidates
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
                deployment_chain_id,
                num_candidates
            FROM dipper_reg_indexing_requests
            WHERE deployment_id = $1
            "#,
        )
        .bind(PgDeploymentId(*deployment_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Returns all indexing agreements associated with an indexing request that are in an active
    /// state: `CREATED` or `ACCEPTED_ON_CHAIN`.
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
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
            FROM dipper_reg_indexing_agreements
            WHERE indexing_request_id = $1 AND status IN ($2, $3)
            "#,
        )
        .bind(request_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
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
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                voucher
            )
            VALUES (
                $1, timezone('UTC', now()), timezone('UTC', now()), $2, $3, $4, $5,
                $6, $7
            )
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementId::new())
        .bind(IndexingAgreementStatus::default())
        .bind(request_id)
        .bind(PgDeploymentId(deployment_id))
        .bind(PgIndexerId(indexer_id))
        .bind(PgUrl(indexer_url))
        .bind(Json(voucher))
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreement_by_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>, Error> {
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
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
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
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
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
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
            FROM dipper_reg_indexing_agreements
            WHERE indexer_id = $1
            "#,
        )
        .bind(PgIndexerId(*indexer_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Get aggregated deployment-to-indexers mapping for active agreements.
    ///
    /// Returns agreements that are in `CREATED` or `ACCEPTED_ON_CHAIN` status
    /// for any of the provided indexer IDs, grouped by deployment. This performs database-side
    /// aggregation, returning only the deployment IDs and their associated indexer IDs rather
    /// than full agreement objects.
    ///
    /// Returns a map where keys are deployment IDs and values are lists of indexer IDs
    /// that have active agreements for that deployment.
    pub async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>, Error> {
        if indexer_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let pg_indexer_ids: Vec<PgIndexerId> =
            indexer_ids.iter().map(|id| PgIndexerId(*id)).collect();

        let rows: Vec<(PgDeploymentId, Vec<PgIndexerId>)> = sqlx::query_as(
            r#"
            SELECT
                deployment_id,
                array_agg(indexer_id) as indexer_ids
            FROM dipper_reg_indexing_agreements
            WHERE indexer_id = ANY($1) AND status IN ($2, $3)
            GROUP BY deployment_id
            "#,
        )
        .bind(&pg_indexer_ids[..])
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(deployment, indexers)| {
                (deployment.0, indexers.into_iter().map(|i| i.0).collect())
            })
            .collect())
    }

    /// Get declined indexers grouped by deployment with differentiated lookback windows.
    ///
    /// Returns indexers with `CanceledByIndexer`, `Expired`, or `Rejected` status
    /// within lookback windows that depend on the rejection reason:
    /// - `PRICE_TOO_LOW` rejections: `price_lookback_days` (shorter, allows retry after IISA refresh)
    /// - `SIGNER_NOT_AUTHORISED` rejections: `signer_lookback_minutes` (very short, transient auth issue)
    /// - All other statuses/reasons: `default_lookback_days` (standard exclusion)
    ///
    /// Returns a map where keys are deployment IDs and values are lists of indexer IDs
    /// that declined agreements for that deployment.
    pub async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        signer_lookback_minutes: i32,
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>, Error> {
        use crate::rejection_reason::{PRICE_TOO_LOW, SIGNER_NOT_AUTHORISED};

        let rows: Vec<(PgDeploymentId, Vec<PgIndexerId>)> = sqlx::query_as(
            r#"
            SELECT
                deployment_id,
                array_agg(DISTINCT indexer_id) as indexer_ids
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2, $3)
              AND (
                -- PRICE_TOO_LOW rejections: shorter lookback (until next IISA refresh)
                (rejection_reason = $6
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $4))
                OR
                -- SIGNER_NOT_AUTHORISED rejections: very short lookback (transient auth issue)
                (rejection_reason = $7
                 AND updated_at >= timezone('UTC', now()) - make_interval(mins => $8))
                OR
                -- All other rejections/expirations/cancellations: standard lookback
                (COALESCE(rejection_reason, '') NOT IN ($6, $7)
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $5))
              )
            GROUP BY deployment_id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByIndexer)
        .bind(IndexingAgreementStatus::Expired)
        .bind(IndexingAgreementStatus::Rejected)
        .bind(price_lookback_days)
        .bind(default_lookback_days)
        .bind(PRICE_TOO_LOW)
        .bind(SIGNER_NOT_AUTHORISED)
        .bind(signer_lookback_minutes)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(deployment, indexers)| {
                (deployment.0, indexers.into_iter().map(|i| i.0).collect())
            })
            .collect())
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
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
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
            WHERE id = $2 AND status IN ($3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByRequester)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(IndexingAgreementStatus::Rejected)
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
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    pub async fn mark_indexing_agreement_as_accepted_on_chain(
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
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
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

    // =========================================================================
    // Reassignment operations
    // =========================================================================

    /// Get open indexing requests eligible for reassessment.
    ///
    /// Returns requests that are in the `OPEN` status and were created at least
    /// `min_age_seconds` ago. Results are ordered by `updated_at` ascending to
    /// prioritize requests that haven't been reassessed recently.
    ///
    /// If `batch_size` is greater than 0, limits the number of results.
    /// If `batch_size` is 0 or negative, returns all matching requests.
    pub async fn get_open_indexing_requests_for_reassessment(
        &self,
        min_age_seconds: i64,
        batch_size: i64,
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
                deployment_chain_id,
                num_candidates
            FROM dipper_reg_indexing_requests
            WHERE status = $1
              AND created_at < timezone('UTC', now()) - ($2 * interval '1 second')
            ORDER BY updated_at ASC
            LIMIT CASE WHEN $3 > 0 THEN $3 ELSE NULL END
            "#,
        )
        .bind(IndexingRequestStatus::Open)
        .bind(min_age_seconds)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    // =========================================================================
    // Deadline expiration operations
    // =========================================================================

    /// Get `Created` agreements whose RCA deadline has passed.
    ///
    /// The deadline is stored in the `voucher` JSONB column. Once the deadline passes,
    /// the indexer can no longer accept on-chain, so we mark these as `Expired`.
    /// Results are ordered by deadline ascending (oldest first).
    pub async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
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
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
              AND CAST(voucher->>'deadline' AS bigint) < EXTRACT(epoch FROM timezone('UTC', now()))
            ORDER BY CAST(voucher->>'deadline' AS bigint) ASC
            LIMIT CASE WHEN $2 > 0 THEN $2 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Mark an agreement as `Expired` (deadline passed, never accepted on-chain).
    ///
    /// Only transitions from `Created` status. Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated)
    /// if the agreement doesn't exist or isn't in `Created` status.
    pub async fn mark_indexing_agreement_as_expired(
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
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    /// Mark an agreement as `Rejected` (indexer rejected the proposal off-chain).
    ///
    /// Only transitions from `Created` status. The indexer may still accept on-chain
    /// before the deadline, in which case Dipper will cancel via `cancelIndexingAgreementByPayer`.
    ///
    /// Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated) if the agreement doesn't exist
    /// or isn't in `Created` status.
    pub async fn mark_indexing_agreement_as_rejected(
        &self,
        agreement_id: &IndexingAgreementId,
        rejection_reason: Option<&str>,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now()),
                rejection_reason = $4
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::Rejected)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(rejection_reason)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    // =========================================================================
    // Liveness tracking operations
    // =========================================================================

    /// Get all `AcceptedOnChain` agreements for liveness checking.
    ///
    /// Returns agreements ordered by `last_progress_at` ascending (NULLs first),
    /// so agreements that have never been checked are processed first.
    pub async fn get_accepted_on_chain_agreements(
        &self,
        batch_size: i64,
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
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
            ORDER BY last_progress_at ASC NULLS FIRST
            LIMIT CASE WHEN $2 > 0 THEN $2 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Update the sync progress for an agreement.
    ///
    /// Called when the liveness checker observes the block height has changed
    /// (either advancing or resetting due to a resync).
    pub async fn update_agreement_sync_progress(
        &self,
        agreement_id: &IndexingAgreementId,
        block_height: u64,
        progress_at: time::OffsetDateTime,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                last_block_height = $1,
                last_progress_at = $2
            WHERE id = $3
            "#,
        )
        .bind(block_height as i64)
        .bind(progress_at)
        .bind(agreement_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Count active agreements per deployment.
    ///
    /// Returns a map of deployment ID to count of `Created` or `AcceptedOnChain`
    /// agreements. Used by the liveness checker to determine the tolerance threshold
    /// for each deployment.
    pub async fn count_active_agreements_by_deployment(
        &self,
    ) -> Result<HashMap<DeploymentId, usize>, Error> {
        let rows: Vec<(PgDeploymentId, i64)> = sqlx::query_as(
            r#"
            SELECT deployment_id, COUNT(*) as count
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2)
            GROUP BY deployment_id
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(deployment, count)| (deployment.0, count as usize))
            .collect())
    }

    /// Mark an agreement as `AbandonedByIndexer`.
    ///
    /// Transitions `AcceptedOnChain → AbandonedByIndexer`. Returns the full
    /// agreement for use in the subsequent reassessment call.
    ///
    /// Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated) if the agreement
    /// doesn't exist or isn't in `AcceptedOnChain` status.
    pub async fn mark_indexing_agreement_as_abandoned(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<IndexingAgreement, Error> {
        let record: Option<IndexingAgreement> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING
                id,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                voucher,
                last_block_height,
                last_progress_at,
                rejection_reason
            "#,
        )
        .bind(IndexingAgreementStatus::AbandonedByIndexer)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_optional(&self.pool)
        .await?;

        record.ok_or(Error::NoRecordsUpdated)
    }

    // =========================================================================
    // Indexer denylist operations
    // =========================================================================

    /// Get all active (non-expired) denied indexer IDs.
    ///
    /// Entries with an expiration date in the past are excluded.
    pub async fn get_indexer_denylist(&self) -> Result<Vec<IndexerId>, Error> {
        let rows: Vec<(PgIndexerId,)> = sqlx::query_as(
            r#"
            SELECT indexer_id
            FROM dipper_indexer_denylist
            WHERE expires_at IS NULL OR expires_at > timezone('UTC', now())
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id.0).collect())
    }

    // =========================================================================
    // Chain listener state operations
    // =========================================================================

    /// Get the chain listener state for a given chain ID.
    ///
    /// Returns `None` if no state exists for the chain (first run).
    pub async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> Result<Option<(u64, u64)>, Error> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            r#"
            SELECT chain_id, last_processed_block
            FROM dipper_chain_listener_state
            WHERE chain_id = $1
            "#,
        )
        .bind(chain_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(chain_id, block)| (chain_id as u64, block as u64)))
    }

    /// Update the chain listener state for a given chain ID.
    ///
    /// Creates the record if it doesn't exist (upsert).
    pub async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        last_processed_block: u64,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO dipper_chain_listener_state (chain_id, last_processed_block, updated_at)
            VALUES ($1, $2, timezone('UTC', now()))
            ON CONFLICT (chain_id)
            DO UPDATE SET
                last_processed_block = EXCLUDED.last_processed_block,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(chain_id as i64)
        .bind(last_processed_block as i64)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
