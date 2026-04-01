//! Postgres data conversion for the `indexing_agreement` table.
//!
//! Data conversion between the database and the Rust types is done using the `FromRow` trait from
//! the `sqlx` crate.

use sqlx::{Row, postgres::PgRow};

use super::common::{PgIndexerId, PgUrl};
use crate::indexing_agreement::{Indexer, IndexingAgreement, Status};

impl sqlx::FromRow<'_, PgRow> for IndexingAgreement {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let id = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;
        let updated_at = row.try_get("updated_at")?;
        let status = row.try_get("status")?;
        let indexing_request_id = row.try_get("indexing_request_id")?;
        let indexer = sqlx::FromRow::from_row(row)?;
        let sqlx::types::Json(voucher) = row.try_get("voucher")?;
        let last_block_height: Option<i64> = row.try_get("last_block_height")?;
        let last_progress_at = row.try_get("last_progress_at")?;
        let rejection_reason: Option<String> = row.try_get("rejection_reason")?;
        let on_chain_id_vec: Vec<u8> = row.try_get("on_chain_id")?;
        let on_chain_id: [u8; 16] =
            on_chain_id_vec
                .try_into()
                .map_err(|_| sqlx::Error::ColumnDecode {
                    index: "on_chain_id".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "on_chain_id must be exactly 16 bytes",
                    )),
                })?;

        Ok(Self {
            id,
            created_at,
            updated_at,
            status,
            indexing_request_id,
            indexer,
            voucher,
            last_block_height: last_block_height.map(|v| v as u64),
            last_progress_at,
            rejection_reason,
            on_chain_id,
        })
    }
}

impl From<i32> for Status {
    fn from(value: i32) -> Self {
        num_traits::FromPrimitive::from_i32(value).unwrap_or(Status::Unknown)
    }
}

impl sqlx::FromRow<'_, PgRow> for Indexer {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgIndexerId(id) = row.try_get("indexer_id")?;
        let PgUrl(url) = row.try_get("indexer_url")?;

        Ok(Self { id, url })
    }
}
