use sqlx::{postgres::PgRow, Row};

use super::common::{PgAddress, PgIndexerId, PgProofOfIndexing, PgU256, PgU32, PgU64};
use crate::indexing_receipt::{IndexingReceipt, ReportedWork};

impl sqlx::FromRow<'_, PgRow> for IndexingReceipt {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let id = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;
        let updated_at = row.try_get("updated_at")?;
        let indexing_agreement_id = row.try_get("indexing_agreement_id")?;
        let PgIndexerId(indexer_id) = row.try_get("indexer_id")?;
        let PgAddress(indexer_operator_id) = row.try_get("indexer_operator_id")?;
        let reported_work = sqlx::FromRow::from_row(row)?;
        let PgU256(amount) = row.try_get("amount")?;

        Ok(Self {
            id,
            created_at,
            updated_at,
            indexing_agreement_id,
            indexer_id,
            indexer_operator_id,
            reported_work,
            amount,
        })
    }
}

impl sqlx::FromRow<'_, PgRow> for ReportedWork {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgU32(epoch) = row.try_get("reported_work_epoch")?;
        let PgU64(blocks) = row.try_get("reported_work_blocks")?;
        let PgU64(entities) = row.try_get("reported_work_entities")?;
        let PgProofOfIndexing(poi) = row.try_get("reported_work_poi")?;

        Ok(Self {
            epoch,
            blocks,
            entities,
            poi,
        })
    }
}
