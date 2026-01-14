//! Indexer denylist types for administratively blocked indexers.

use serde::{Deserialize, Serialize};
use sqlx::{Row, postgres::PgRow};
use thegraph_core::IndexerId;
use time::OffsetDateTime;

use crate::postgres::common::PgIndexerId;

/// A denied indexer entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerDenylistEntry {
    /// The denied indexer's ID.
    pub indexer_id: IndexerId,
    /// When the indexer was added to the denylist.
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    /// Optional reason for denying.
    pub reason: Option<String>,
}

impl sqlx::FromRow<'_, PgRow> for IndexerDenylistEntry {
    fn from_row(row: &'_ PgRow) -> Result<Self, sqlx::Error> {
        let PgIndexerId(indexer_id) = row.try_get("indexer_id")?;
        let created_at = row.try_get("created_at")?;
        let reason = row.try_get("reason")?;

        Ok(Self {
            indexer_id,
            created_at,
            reason,
        })
    }
}
