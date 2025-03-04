//! Postgres data conversion for the `indexing_agreement` table.
//!
//! Data conversion between the database and the Rust types is done using the `FromRow` trait from
//! the `sqlx` crate.

use sqlx::{postgres::PgRow, Row};

use super::common::{PgAddress, PgDeploymentId, PgIndexerId, PgU256, PgU32, PgU64, PgUrl};
use crate::indexing_agreement::{Indexer, IndexingAgreement, Status, Voucher, VoucherMetadata};

impl sqlx::FromRow<'_, PgRow> for IndexingAgreement {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let id = row.try_get("id")?;

        let created_at = row.try_get("created_at")?;
        let updated_at = row.try_get("updated_at")?;

        let status = row.try_get("status")?;
        let accepted_at_epoch = row
            .try_get::<Option<PgU32>, _>("accepted_at_epoch")?
            .map(|PgU32(x)| x);

        let indexing_request_id = row.try_get("indexing_request_id")?;

        let indexer = sqlx::FromRow::from_row(row)?;
        let voucher = sqlx::FromRow::from_row(row)?;

        Ok(Self {
            id,
            created_at,
            updated_at,
            status,
            accepted_at_epoch,
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
        let PgU64(deadline) = row.try_get("voucher_deadline")?;
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
            deadline,
            metadata,
        })
    }
}

impl sqlx::FromRow<'_, PgRow> for VoucherMetadata {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgU256(base_price_per_epoch) = row.try_get("voucher_metadata_base_price_per_epoch")?;
        let PgU256(price_per_entity) = row.try_get("voucher_metadata_price_per_entity")?;
        let PgDeploymentId(subgraph_deployment_id) =
            row.try_get("voucher_metadata_deployment_id")?;
        let PgU64(protocol_network) = row.try_get("voucher_metadata_protocol_network")?;
        let PgU64(chain_id) = row.try_get("voucher_metadata_chain_id")?;

        Ok(Self {
            base_price_per_epoch,
            price_per_entity,
            subgraph_deployment_id,
            protocol_network,
            chain_id,
        })
    }
}

impl From<i32> for Status {
    fn from(value: i32) -> Self {
        num_traits::FromPrimitive::from_i32(value).unwrap_or(Status::Unknown)
    }
}
