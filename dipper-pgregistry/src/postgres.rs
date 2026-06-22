//! PostgreSQL implementation of the registry

use std::collections::HashMap;

use dipper_core::ids::{IndexingAgreementId, IndexingReceiptId, IndexingRequestId};
use sqlx::{Pool, Postgres, types::Json};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId, U256},
};
use url::Url;

/// Parameters for registering a new indexing agreement.
pub struct NewAgreementParams {
    pub agreement_id: IndexingAgreementId,
    pub nonce_uuid: uuid::Uuid,
    pub request_id: IndexingRequestId,
    pub deployment_id: DeploymentId,
    pub indexer_id: IndexerId,
    pub indexer_url: Url,
    pub terms: crate::IndexingAgreementTerms,
    pub terms_version_hash: Option<Vec<u8>>,
}

use self::common::{
    PgAddress, PgAllocationId, PgDeploymentId, PgIndexerId, PgProofOfIndexing, PgU32, PgU64,
    PgU256, PgUrl,
};
use super::{
    IndexingReceiptReportedWork,
    indexing_agreement::{IndexingAgreement, Status as IndexingAgreementStatus},
    indexing_receipt::IndexingReceipt,
    indexing_request::{
        IndexingRequest, SetTargetOutcome as IndexingRequestSetTargetOutcome,
        Status as IndexingRequestStatus,
    },
    result::Error,
};

pub(crate) mod common;
mod indexing_agreement;
mod indexing_receipt;
mod indexing_request;

/// Chain listener state row from the database.
#[derive(Debug, Clone)]
pub struct ChainListenerStateRow {
    pub chain_id: u64,
    pub last_processed_block: u64,
    /// `id` of the last consumed entity at `last_processed_block`. `None`
    /// means the cursor sits at a block boundary (genesis or after a
    /// strict block advance). Stored as `BYTEA` and surfaced here as the
    /// strongly-typed `IndexingAgreementId`.
    pub last_processed_id: Option<dipper_core::ids::IndexingAgreementId>,
    pub last_processed_block_timestamp: Option<u64>,
}

/// Which party cancelled an agreement, for the atomic reconciliation
/// transition written by `apply_reconciliation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelKind {
    /// The reconciliation initiator (dipper's configured signer) cancelled.
    ByRequester,
    /// The indexer cancelled.
    ByIndexer,
}

/// Outcome of an atomic `apply_reconciliation` call. The two booleans
/// report which transitions actually affected a row, so the caller can
/// gate post-commit side effects (e.g. running pending-cancellation
/// bookkeeping only when a fresh `AcceptedOnChain` write happened).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconciliationOutcome {
    pub did_accept: bool,
    pub did_cancel: bool,
}

/// One row's reconciliation request inside a batched apply.
#[derive(Debug, Clone, Copy)]
pub struct ReconciliationItem {
    pub agreement_id: dipper_core::ids::IndexingAgreementId,
    pub apply_accept: bool,
    pub cancel: Option<CancelKind>,
}

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
    /// Set the target number of indexer candidates for the key
    /// `(requested_by, deployment_id, deployment_chain_id)`.
    ///
    /// Atomic upsert: a fresh key inserts a new Open row; an existing Open row
    /// has its `num_candidates` adjusted (or transitions to Canceled when the
    /// new target is zero). See [`IndexingRequestSetTargetOutcome`] for the
    /// returned discriminator.
    pub async fn set_indexing_target_candidates(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: i32,
    ) -> Result<IndexingRequestSetTargetOutcome, Error> {
        let mut tx = self.pool.begin().await?;

        let existing: Option<(IndexingRequestId, i32)> = sqlx::query_as(
            r#"
            SELECT id, num_candidates
            FROM dipper_reg_indexing_requests
            WHERE requested_by = $1
              AND deployment_id = $2
              AND deployment_chain_id = $3
              AND status = $4
            FOR UPDATE
            "#,
        )
        .bind(PgAddress(requested_by))
        .bind(PgDeploymentId(deployment_id))
        .bind(PgU64(deployment_chain_id))
        .bind(IndexingRequestStatus::Open)
        .fetch_optional(&mut *tx)
        .await?;

        let outcome = match existing {
            None if num_candidates == 0 => IndexingRequestSetTargetOutcome::NoOpAlreadyEmpty,
            None => {
                let new_id = IndexingRequestId::new();
                sqlx::query(
                    r#"
                    INSERT INTO dipper_reg_indexing_requests (
                        id, created_at, updated_at, status,
                        requested_by, deployment_id, deployment_chain_id, num_candidates
                    )
                    VALUES (
                        $1, timezone('UTC', now()), timezone('UTC', now()), $2,
                        $3, $4, $5, $6
                    )
                    "#,
                )
                .bind(new_id)
                .bind(IndexingRequestStatus::Open)
                .bind(PgAddress(requested_by))
                .bind(PgDeploymentId(deployment_id))
                .bind(PgU64(deployment_chain_id))
                .bind(num_candidates)
                .execute(&mut *tx)
                .await?;
                IndexingRequestSetTargetOutcome::Inserted { id: new_id }
            }
            Some((id, existing_count)) if existing_count == num_candidates => {
                IndexingRequestSetTargetOutcome::NoOp { id }
            }
            Some((id, _)) if num_candidates == 0 => {
                sqlx::query(
                    r#"
                    UPDATE dipper_reg_indexing_requests
                    SET status = $1, updated_at = timezone('UTC', now())
                    WHERE id = $2
                    "#,
                )
                .bind(IndexingRequestStatus::Canceled)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                IndexingRequestSetTargetOutcome::Canceled { id }
            }
            Some((id, _)) => {
                sqlx::query(
                    r#"
                    UPDATE dipper_reg_indexing_requests
                    SET num_candidates = $1, updated_at = timezone('UTC', now())
                    WHERE id = $2
                    "#,
                )
                .bind(num_candidates)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                IndexingRequestSetTargetOutcome::Updated {
                    id,
                    new_num_candidates: num_candidates,
                }
            }
        };

        tx.commit().await?;
        Ok(outcome)
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
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
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

    pub async fn register_new_indexing_agreement(
        &self,
        params: NewAgreementParams,
    ) -> Result<IndexingAgreementId, Error> {
        let NewAgreementParams {
            agreement_id,
            nonce_uuid,
            request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms,
            terms_version_hash,
        } = params;
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_agreements (
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                terms_version_hash
            )
            VALUES (
                $1, $2, timezone('UTC', now()), timezone('UTC', now()), $3, $4, $5, $6,
                $7, $8, $9
            )
            RETURNING id
            "#,
        )
        .bind(agreement_id)
        .bind(nonce_uuid)
        .bind(IndexingAgreementStatus::default())
        .bind(request_id)
        .bind(PgDeploymentId(deployment_id))
        .bind(PgIndexerId(indexer_id))
        .bind(PgUrl(indexer_url))
        .bind(Json(terms))
        .bind(terms_version_hash)
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
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Batch lookup of agreements by id. Missing ids are absent from the
    /// returned map. Single round-trip (`WHERE id = ANY($1)`) so the
    /// chain listener's per-page reconcile prep doesn't issue one SELECT
    /// per snapshot.
    pub async fn get_indexing_agreements_by_ids(
        &self,
        agreement_ids: &[IndexingAgreementId],
    ) -> Result<HashMap<IndexingAgreementId, IndexingAgreement>, Error> {
        if agreement_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<IndexingAgreement> = sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE id = ANY($1)
            "#,
        )
        .bind(agreement_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|a| (a.id, a)).collect())
    }

    pub async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
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
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
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

    /// Get declined `CanceledByIndexer`/`Expired`/`Rejected` indexers grouped by
    /// deployment (deployment id -> indexer ids). Each rejection reason gets its own
    /// exclusion window (price, transient, escrow, uncertain, default); see the constants.
    pub async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        transient_lookback_minutes: i32,
        escrow_lookback_minutes: i32,
        uncertain_lookback_days: i32,
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>, Error> {
        use crate::rejection_reason::{
            AGREEMENT_EXPIRED, CAPACITY_EXCEEDED, DEADLINE_EXPIRED, INDEXER_UNAVAILABLE,
            INSUFFICIENT_ESCROW, INVALID_SIGNATURE, PRICE_TOO_LOW, REPLAY_DETECTED,
            SENDER_NOT_TRUSTED, SUBGRAPH_MANIFEST_UNAVAILABLE, UNEXPECTED_SERVICE_PROVIDER,
            UNSPECIFIED, UNSUPPORTED_METADATA_VERSION,
        };

        let rows: Vec<(PgDeploymentId, Vec<PgIndexerId>)> = sqlx::query_as(
            r#"
            SELECT
                deployment_id,
                array_agg(DISTINCT indexer_id) as indexer_ids
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2, $3)
              AND (
                -- PRICE_TOO_LOW: shorter lookback (until next IISA refresh)
                (rejection_reason = $6
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $4))
                OR
                -- Transient, not-indexer's-fault, or dipper-side faults that
                -- clear once dipper is fixed: very short lookback
                (rejection_reason IN ($8, $9, $10, $11, $12, $13, $14, $17, $18)
                 AND updated_at >= timezone('UTC', now()) - make_interval(mins => $7))
                OR
                -- INSUFFICIENT_ESCROW: medium lookback (clears when payer tops up)
                (rejection_reason = $16
                 AND updated_at >= timezone('UTC', now()) - make_interval(mins => $15))
                OR
                -- Uncertain reasons (sender not trusted, unspecified/unknown):
                -- may clear within about a day, so a 1-day lookback
                (rejection_reason IN ($20, $21)
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $19))
                OR
                -- All other rejections/expirations/cancellations: standard lookback
                (COALESCE(rejection_reason, '') NOT IN ($6, $8, $9, $10, $11, $12, $13, $14, $16, $17, $18, $20, $21)
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $5))
              )
            GROUP BY deployment_id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByIndexer) // $1
        .bind(IndexingAgreementStatus::Expired) // $2
        .bind(IndexingAgreementStatus::Rejected) // $3
        .bind(price_lookback_days) // $4
        .bind(default_lookback_days) // $5
        .bind(PRICE_TOO_LOW) // $6
        .bind(transient_lookback_minutes) // $7
        .bind(DEADLINE_EXPIRED) // $8
        .bind(SUBGRAPH_MANIFEST_UNAVAILABLE) // $9
        .bind(UNEXPECTED_SERVICE_PROVIDER) // $10
        .bind(AGREEMENT_EXPIRED) // $11
        .bind(UNSUPPORTED_METADATA_VERSION) // $12
        .bind(CAPACITY_EXCEEDED) // $13
        .bind(INDEXER_UNAVAILABLE) // $14
        .bind(escrow_lookback_minutes) // $15
        .bind(INSUFFICIENT_ESCROW) // $16
        .bind(INVALID_SIGNATURE) // $17
        .bind(REPLAY_DETECTED) // $18
        .bind(uncertain_lookback_days) // $19
        .bind(SENDER_NOT_TRUSTED) // $20
        .bind(UNSPECIFIED) // $21
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
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
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

    /// Persist the on-chain tx hash of the most recent `offer()` submission
    /// for this agreement. Overwrites any prior value, so a resubmit after
    /// mempool eviction records the live hash rather than the dropped one.
    /// Observability-only: no status transition is performed here.
    ///
    /// Guarded on `status IN (Created, AcceptedOnChain)` so a delayed
    /// receipt-confirmation cannot stamp `offer_tx_hash` onto a row that
    /// has since transitioned to `Expired`, `DeliveryFailed`, `Rejected`,
    /// or one of the cancel states. The caller treats any failure here
    /// as non-fatal and just logs; a no-match result is also non-fatal
    /// and silently skipped.
    pub async fn update_offer_tx_hash(
        &self,
        agreement_id: &IndexingAgreementId,
        tx_hash: &[u8; 32],
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                offer_tx_hash = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status IN ($3, $4)
            "#,
        )
        .bind(&tx_hash[..])
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .execute(&self.pool)
        .await?;
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

    /// Atomically apply a reconciliation-driven state transition (accept
    /// and/or cancel) in a single database transaction so the
    /// chain_listener's Accept-then-Cancel-in-one-snapshot path does not
    /// leave an intermediate `AcceptedOnChain` row visible to concurrent
    /// readers.
    ///
    /// `did_accept` is false when the agreement was not in `Created` or
    /// `Expired` at UPDATE time; callers gate side effects like
    /// `execute_pending_cancellations` on this so it only fires on a fresh
    /// accept write.
    ///
    /// Roll back with `NoRecordsUpdated` only when an accept landed in
    /// this tx but the paired cancel matched no row — committing then
    /// would leave the AcceptedOnChain write visible without its
    /// follow-up cancel, which the Accept-then-Cancel-in-one-snapshot
    /// invariant forbids. When both writes find no matching row (caller
    /// passed `apply_accept = false` and the cancel filter rejected, e.g.
    /// the row is in a terminal-but-not-cancel state like `DeliveryFailed`
    /// that the chain_listener's Rust-side guard does not catch), commit
    /// the empty tx and return `Ok` with both flags false. The
    /// chain_listener treats that as a successful no-op rather than a
    /// hard error.
    pub async fn apply_reconciliation(
        &self,
        agreement_id: &IndexingAgreementId,
        apply_accept: bool,
        cancel: Option<CancelKind>,
    ) -> Result<ReconciliationOutcome, Error> {
        let mut tx = self.pool.begin().await?;

        let did_accept = if apply_accept {
            update_status_from(
                &mut tx,
                agreement_id,
                IndexingAgreementStatus::AcceptedOnChain,
                &[
                    IndexingAgreementStatus::Created,
                    IndexingAgreementStatus::Expired,
                ],
            )
            .await?
        } else {
            false
        };

        let mut did_cancel = false;
        if let Some(kind) = cancel {
            let (new_status, allowed_from): (_, &[IndexingAgreementStatus]) = match kind {
                CancelKind::ByRequester => (
                    IndexingAgreementStatus::CanceledByRequester,
                    &[
                        IndexingAgreementStatus::Created,
                        IndexingAgreementStatus::AcceptedOnChain,
                        IndexingAgreementStatus::Rejected,
                    ],
                ),
                CancelKind::ByIndexer => (
                    IndexingAgreementStatus::CanceledByIndexer,
                    &[IndexingAgreementStatus::AcceptedOnChain],
                ),
            };
            did_cancel =
                update_status_from(&mut tx, agreement_id, new_status, allowed_from).await?;
            if did_accept && !did_cancel {
                return Err(Error::NoRecordsUpdated);
            }
        }

        tx.commit().await?;

        Ok(ReconciliationOutcome {
            did_accept,
            did_cancel,
        })
    }

    /// Batched `apply_reconciliation`. Collapses single-transition items
    /// into one transaction with at most three batched `UPDATE`s
    /// (accept, cancel-by-requester, cancel-by-indexer); items combining
    /// accept+cancel still run per-row in the same tx so the rollback
    /// rule on a paired-cancel miss is preserved. Every input id appears
    /// in the returned map, with both flags `false` when no row flipped.
    ///
    /// Caller contract: input ids must be unique. Duplicates would leave
    /// stale outcome flags from the first iteration; debug_asserted.
    pub async fn apply_reconciliation_batch(
        &self,
        items: &[ReconciliationItem],
    ) -> Result<HashMap<IndexingAgreementId, ReconciliationOutcome>, Error> {
        debug_assert!(
            {
                let mut seen: std::collections::HashSet<IndexingAgreementId> =
                    std::collections::HashSet::with_capacity(items.len());
                items.iter().all(|i| seen.insert(i.agreement_id))
            },
            "apply_reconciliation_batch requires unique agreement_ids; duplicates would leave \
             stale outcome flags from the first iteration",
        );

        let mut outcomes: HashMap<IndexingAgreementId, ReconciliationOutcome> =
            HashMap::with_capacity(items.len());
        for item in items {
            outcomes.insert(item.agreement_id, ReconciliationOutcome::default());
        }

        if items.is_empty() {
            return Ok(outcomes);
        }

        // Single-pass partition into the four item shapes.
        let mut paired: Vec<&ReconciliationItem> = Vec::new();
        let mut accept_only: Vec<IndexingAgreementId> = Vec::new();
        let mut cancel_by_requester: Vec<IndexingAgreementId> = Vec::new();
        let mut cancel_by_indexer: Vec<IndexingAgreementId> = Vec::new();
        for item in items {
            match (item.apply_accept, item.cancel) {
                (true, Some(_)) => paired.push(item),
                (true, None) => accept_only.push(item.agreement_id),
                (false, Some(CancelKind::ByRequester)) => {
                    cancel_by_requester.push(item.agreement_id)
                }
                (false, Some(CancelKind::ByIndexer)) => cancel_by_indexer.push(item.agreement_id),
                (false, None) => {}
            }
        }

        let mut tx = self.pool.begin().await?;

        for item in paired {
            let cancel_kind = item.cancel.expect("paired implies Some");
            let did_accept = update_status_from(
                &mut tx,
                &item.agreement_id,
                IndexingAgreementStatus::AcceptedOnChain,
                &[
                    IndexingAgreementStatus::Created,
                    IndexingAgreementStatus::Expired,
                ],
            )
            .await?;
            let (new_status, allowed_from): (_, &[IndexingAgreementStatus]) = match cancel_kind {
                CancelKind::ByRequester => (
                    IndexingAgreementStatus::CanceledByRequester,
                    &[
                        IndexingAgreementStatus::Created,
                        IndexingAgreementStatus::AcceptedOnChain,
                        IndexingAgreementStatus::Rejected,
                    ],
                ),
                CancelKind::ByIndexer => (
                    IndexingAgreementStatus::CanceledByIndexer,
                    &[IndexingAgreementStatus::AcceptedOnChain],
                ),
            };
            let did_cancel =
                update_status_from(&mut tx, &item.agreement_id, new_status, allowed_from).await?;
            if did_accept && !did_cancel {
                return Err(Error::NoRecordsUpdated);
            }
            outcomes.insert(
                item.agreement_id,
                ReconciliationOutcome {
                    did_accept,
                    did_cancel,
                },
            );
        }

        for id in batch_update_status_from(
            &mut tx,
            &accept_only,
            IndexingAgreementStatus::AcceptedOnChain,
            &[
                IndexingAgreementStatus::Created,
                IndexingAgreementStatus::Expired,
            ],
        )
        .await?
        {
            outcomes.entry(id).or_default().did_accept = true;
        }

        for id in batch_update_status_from(
            &mut tx,
            &cancel_by_requester,
            IndexingAgreementStatus::CanceledByRequester,
            &[
                IndexingAgreementStatus::Created,
                IndexingAgreementStatus::AcceptedOnChain,
                IndexingAgreementStatus::Rejected,
            ],
        )
        .await?
        {
            outcomes.entry(id).or_default().did_cancel = true;
        }

        for id in batch_update_status_from(
            &mut tx,
            &cancel_by_indexer,
            IndexingAgreementStatus::CanceledByIndexer,
            &[IndexingAgreementStatus::AcceptedOnChain],
        )
        .await?
        {
            outcomes.entry(id).or_default().did_cancel = true;
        }

        tx.commit().await?;

        Ok(outcomes)
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
    /// Compares `terms.deadline` against `chain_timestamp` (block time).
    pub async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
        chain_timestamp: u64,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
              AND CAST(terms->>'deadline' AS bigint) < $3
            ORDER BY CAST(terms->>'deadline' AS bigint) ASC
            LIMIT CASE WHEN $2 > 0 THEN $2 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(batch_size)
        .bind(chain_timestamp as i64)
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
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
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

    /// Get agreements still `AcceptedOnChain` whose parent request is `Canceled`.
    ///
    /// Used by the chain listener's periodic orphan-cancel sweep to retry
    /// on-chain `cancelIndexingAgreementByPayer` calls that failed during the
    /// reassessment that flipped the request row.
    pub async fn get_agreements_pending_chain_cancel(
        &self,
        batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                a.id,
                a.nonce_uuid,
                a.created_at,
                a.updated_at,
                a.status,
                a.indexing_request_id,
                a.deployment_id,
                a.indexer_id,
                a.indexer_url,
                a.terms,
                a.last_block_height,
                a.last_progress_at,
                a.rejection_reason,
                a.terms_version_hash
            FROM dipper_reg_indexing_agreements a
            JOIN dipper_reg_indexing_requests r
              ON a.indexing_request_id = r.id
            WHERE a.status = $1
              AND r.status = $2
            ORDER BY a.updated_at ASC
            LIMIT CASE WHEN $3 > 0 THEN $3 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(IndexingRequestStatus::Canceled)
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

    /// Whether any agreement is in `Created` or `AcceptedOnChain` status.
    ///
    /// Cheap `EXISTS` probe used by the chain listener's adaptive-interval
    /// gate every poll; the per-deployment `count_active_agreements_by_deployment`
    /// would scan the full active set just to discard the counts.
    pub async fn exists_active_agreements(&self) -> Result<bool, Error> {
        let (exists,): (bool,) = sqlx::query_as(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM dipper_reg_indexing_agreements
                WHERE status IN ($1, $2)
                LIMIT 1
            )
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
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
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
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
    // Optimistic DIPs fees
    // =========================================================================

    /// Returns (agreement_id, indexer_id, deployment_id, base_rate_wei,
    /// entity_rate_wei) per active agreement for optimistic fee estimation.
    ///
    /// Queries all `Created` or `AcceptedOnChain` agreements and extracts
    /// both rate fields from the terms metadata.
    pub async fn get_agreement_fee_rates(
        &self,
    ) -> Result<Vec<(IndexingAgreementId, IndexerId, DeploymentId, f64, f64)>, Error> {
        let rows: Vec<(
            IndexingAgreementId,
            PgIndexerId,
            sqlx::types::Json<super::indexing_agreement::Terms>,
        )> = sqlx::query_as(
            r#"
                SELECT id, indexer_id, terms
                FROM dipper_reg_indexing_agreements
                WHERE status IN ($1, $2)
                "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await?;

        let rates = rows
            .into_iter()
            .map(|(agreement_id, pg_indexer_id, terms_json)| {
                let meta = &terms_json.0.metadata;
                (
                    agreement_id,
                    pg_indexer_id.0,
                    meta.subgraph_deployment_id,
                    meta.tokens_per_second.to::<u128>() as f64,
                    meta.tokens_per_entity_per_second.to::<u128>() as f64,
                )
            })
            .collect();

        Ok(rates)
    }

    // =========================================================================
    // Chain listener state operations
    // =========================================================================

    /// Get the chain listener state for a given chain ID.
    /// Returns `None` if no state exists for the chain (first run).
    pub async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> Result<Option<ChainListenerStateRow>, Error> {
        let row: Option<(
            i64,
            i64,
            Option<dipper_core::ids::IndexingAgreementId>,
            Option<i64>,
        )> = sqlx::query_as(
            r#"
            SELECT chain_id, last_processed_block, last_processed_id, last_processed_block_timestamp
            FROM dipper_chain_listener_state
            WHERE chain_id = $1
            "#,
        )
        .bind(chain_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(chain_id, block, id, ts)| ChainListenerStateRow {
            chain_id: chain_id as u64,
            last_processed_block: block as u64,
            last_processed_id: id,
            last_processed_block_timestamp: ts.map(|t| t as u64),
        }))
    }

    /// Update the chain listener state for a given chain ID.
    ///
    /// Creates the record if it doesn't exist (upsert). `last_processed_id`
    /// is the keyset's `id` discriminator at `last_processed_block`; `None`
    /// means the cursor sits at a block boundary.
    pub async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        last_processed_block: u64,
        last_processed_id: Option<dipper_core::ids::IndexingAgreementId>,
        last_processed_block_timestamp: Option<u64>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO dipper_chain_listener_state
                (chain_id, last_processed_block, last_processed_id, last_processed_block_timestamp, updated_at)
            VALUES ($1, $2, $3, $4, timezone('UTC', now()))
            ON CONFLICT (chain_id)
            DO UPDATE SET
                last_processed_block = EXCLUDED.last_processed_block,
                last_processed_id = EXCLUDED.last_processed_id,
                last_processed_block_timestamp = EXCLUDED.last_processed_block_timestamp,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(chain_id as i64)
        .bind(last_processed_block as i64)
        .bind(last_processed_id)
        .bind(last_processed_block_timestamp.map(|t| t as i64))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // -- Pending cancellations --

    /// Register a new agreement and record a pending cancellation in a single
    /// transaction. Guarantees that if the agreement row exists, the pending
    /// cancellation linking it to the old agreement also exists.
    pub async fn register_agreement_with_pending_cancellation(
        &self,
        params: NewAgreementParams,
        old_agreement_id: IndexingAgreementId,
    ) -> Result<IndexingAgreementId, Error> {
        let NewAgreementParams {
            agreement_id,
            nonce_uuid,
            request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms,
            terms_version_hash,
        } = params;
        let mut tx = self.pool.begin().await?;

        let (new_id,): (IndexingAgreementId,) = sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_agreements (
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                terms_version_hash
            )
            VALUES (
                $1, $2, timezone('UTC', now()), timezone('UTC', now()), $3, $4, $5, $6,
                $7, $8, $9
            )
            RETURNING id
            "#,
        )
        .bind(agreement_id)
        .bind(nonce_uuid)
        .bind(IndexingAgreementStatus::default())
        .bind(request_id)
        .bind(PgDeploymentId(deployment_id))
        .bind(PgIndexerId(indexer_id))
        .bind(PgUrl(indexer_url))
        .bind(Json(terms))
        .bind(terms_version_hash)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO dipper_pending_cancellations
                (new_agreement_id, old_agreement_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(new_id)
        .bind(old_agreement_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(new_id)
    }

    /// Get all pending cancellations linked to a new agreement.
    pub async fn get_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> Result<Vec<IndexingAgreementId>, Error> {
        let rows: Vec<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            SELECT old_agreement_id
            FROM dipper_pending_cancellations
            WHERE new_agreement_id = $1
            "#,
        )
        .bind(new_agreement_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Delete all pending cancellation records for a new agreement.
    /// Called when the new agreement fails (old agreements stay active).
    pub async fn delete_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM dipper_pending_cancellations
            WHERE new_agreement_id = $1
            "#,
        )
        .bind(new_agreement_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete a single pending cancellation record after it has been processed.
    pub async fn delete_pending_cancellation(
        &self,
        new_agreement_id: IndexingAgreementId,
        old_agreement_id: IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM dipper_pending_cancellations
            WHERE new_agreement_id = $1 AND old_agreement_id = $2
            "#,
        )
        .bind(new_agreement_id)
        .bind(old_agreement_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// List the distinct `new_agreement_id`s of pending cancellation rows
    /// whose linked agreement has reached `AcceptedOnChain` locally.
    ///
    /// Each ID returned is a candidate for `execute_pending_cancellations`
    /// re-run. The chain_listener uses this as a periodic sweep to recover
    /// from a partial-progress crash inside that function: the local row
    /// was transitioned to `AcceptedOnChain` but the cancellation fan-out
    /// did not complete, so the rows linger here. Without the sweep the
    /// next reconcile pass for the same agreement would not re-enter the
    /// fan-out path (the gate at `chain_listener.rs:494` only fires on a
    /// fresh transition, not on a no-op `AcceptedOnChain` row).
    ///
    /// `execute_pending_cancellations` is idempotent against
    /// already-canceled old agreements and against deleted pending rows,
    /// so feeding the same ID through it twice is safe.
    pub async fn list_executable_pending_cancellations(
        &self,
        limit: i64,
    ) -> Result<Vec<IndexingAgreementId>, Error> {
        let rows: Vec<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT pc.new_agreement_id
            FROM dipper_pending_cancellations pc
            INNER JOIN dipper_reg_indexing_agreements a
                ON a.id = pc.new_agreement_id
            WHERE a.status = $1
            ORDER BY pc.new_agreement_id
            LIMIT $2
            "#,
        )
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

/// Batched form of `update_status_from`: transitions all rows whose `id`
/// is in `agreement_ids` and whose current status is in `allowed_from` to
/// `new_status`, in one statement. Returns the ids of the rows that
/// actually flipped (matched the CAS guard) so callers can build per-id
/// outcome maps. Empty input is a fast-path no-op.
async fn batch_update_status_from(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    agreement_ids: &[IndexingAgreementId],
    new_status: IndexingAgreementStatus,
    allowed_from: &[IndexingAgreementStatus],
) -> Result<Vec<IndexingAgreementId>, Error> {
    if agreement_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = (0..allowed_from.len())
        .map(|i| format!("${}", i + 3))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = $1, updated_at = timezone('UTC', now())
        WHERE id = ANY($2) AND status IN ({placeholders})
        RETURNING id
        "#
    );
    let mut query = sqlx::query_as::<_, (IndexingAgreementId,)>(&sql)
        .bind(new_status)
        .bind(agreement_ids);
    for status in allowed_from {
        query = query.bind(*status);
    }
    let rows: Vec<(IndexingAgreementId,)> = query.fetch_all(&mut **tx).await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Transition an agreement's status inside a transaction. Thin wrapper
/// over `batch_update_status_from` for the single-row case.
async fn update_status_from(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    agreement_id: &IndexingAgreementId,
    new_status: IndexingAgreementStatus,
    allowed_from: &[IndexingAgreementStatus],
) -> Result<bool, Error> {
    Ok(!batch_update_status_from(
        tx,
        std::slice::from_ref(agreement_id),
        new_status,
        allowed_from,
    )
    .await?
    .is_empty())
}
