//! Indexer denylist registry trait and types.

use async_trait::async_trait;
use thegraph_core::IndexerId;

use super::result::Result as RegistryResult;

pub use dipper_pgregistry::IndexerDenylistEntry;

/// Trait for indexer denylist operations.
#[async_trait]
pub trait IndexerDenylistRegistry {
    /// Get all denied indexer IDs.
    async fn get_indexer_denylist(&self) -> RegistryResult<Vec<IndexerId>>;

    /// Get all denylist entries with full details.
    async fn get_indexer_denylist_entries(&self) -> RegistryResult<Vec<IndexerDenylistEntry>>;

    /// Add an indexer to the denylist.
    async fn add_to_indexer_denylist(
        &self,
        indexer_id: IndexerId,
        reason: Option<&str>,
    ) -> RegistryResult<()>;

    /// Remove an indexer from the denylist.
    async fn remove_from_indexer_denylist(&self, indexer_id: IndexerId) -> RegistryResult<()>;
}
