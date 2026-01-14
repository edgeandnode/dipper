//! Indexer denylist registry trait.

use async_trait::async_trait;
use thegraph_core::IndexerId;

use super::result::Result as RegistryResult;

/// Trait for reading the indexer denylist.
///
/// Write operations (add/remove) are performed via direct database access.
#[async_trait]
pub trait IndexerDenylistRegistry {
    /// Get all denied indexer IDs for exclusion from selection.
    async fn get_indexer_denylist(&self) -> RegistryResult<Vec<IndexerId>>;
}
