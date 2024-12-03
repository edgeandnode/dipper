//! Postgres data conversion for the `indexing_agreement` table.
//!
//! Data conversion between the database and the Rust types is done using the `FromRow` trait from
//! the `sqlx` crate.

use sqlx::{postgres::PgRow, Row};

use super::common::{PgAddress, PgDeploymentId, PgIndexerId, PgU256, PgU32, PgUrl};
use crate::indexing_agreement::{Indexer, IndexingAgreement, Status, Voucher, VoucherMetadata};

impl sqlx::FromRow<'_, PgRow> for IndexingAgreement {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let id = row.try_get("id")?;

        let created_at = row.try_get("created_at")?;
        let updated_at = row.try_get("updated_at")?;

        let status = row.try_get("status")?;

        let indexing_request_id = row.try_get("indexing_request_id")?;

        let indexer = sqlx::FromRow::from_row(row)?;
        let voucher = sqlx::FromRow::from_row(row)?;

        Ok(Self {
            id,
            created_at,
            updated_at,
            status,
            indexing_request_id,
            indexer,
            voucher,
        })
    }
}

impl sqlx::FromRow<'_, PgRow> for Indexer {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgIndexerId(id) = row.try_get("indexer_id")?;
        let PgUrl(url) = row.try_get("indexer_url")?;

        Ok(Self { id, url })
    }
}

impl sqlx::FromRow<'_, PgRow> for Voucher {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgAddress(payer) = row.try_get("voucher_payer")?;
        let PgAddress(recipient) = row.try_get("voucher_recipient")?;
        let PgAddress(service) = row.try_get("voucher_service")?;
        let PgU32(duration_epochs) = row.try_get("voucher_duration_epochs")?;
        let PgU256(max_initial_amount) = row.try_get("voucher_max_initial_amount")?;
        let PgU256(max_ongoing_amount_per_epoch) =
            row.try_get("voucher_max_ongoing_amount_per_epoch")?;
        let PgU32(max_epochs_per_collection) = row.try_get("voucher_max_epochs_per_collection")?;
        let PgU32(min_epochs_per_collection) = row.try_get("voucher_min_epochs_per_collection")?;
        let metadata = sqlx::FromRow::from_row(row)?;

        Ok(Self {
            payer,
            recipient,
            service,
            duration_epochs,
            max_initial_amount,
            max_ongoing_amount_per_epoch,
            max_epochs_per_collection,
            min_epochs_per_collection,
            metadata,
        })
    }
}

impl sqlx::FromRow<'_, PgRow> for VoucherMetadata {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgDeploymentId(deployment_id) = row.try_get("voucher_metadata_deployment_id")?;
        let PgU256(price_per_block) = row.try_get("voucher_metadata_price_per_block")?;
        let PgU256(price_per_entity_per_epoch) =
            row.try_get("voucher_metadata_price_per_entity_per_epoch")?;

        Ok(Self {
            deployment_id,
            price_per_block,
            price_per_entity_per_epoch,
        })
    }
}

impl From<i32> for Status {
    fn from(value: i32) -> Self {
        num_traits::FromPrimitive::from_i32(value).unwrap_or(Status::Unknown)
    }
}
