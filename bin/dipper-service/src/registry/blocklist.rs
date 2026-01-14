//! Blocklist registry trait and types.

use async_trait::async_trait;
use thegraph_core::IndexerId;

use super::result::Result as RegistryResult;

// Re-export the canonical type
pub use dipper_pgregistry::BlocklistEntry;

/// Trait for blocklist operations.
#[async_trait]
pub trait BlocklistRegistry {
    /// Get all blocklisted indexer IDs.
    async fn get_blocklist(&self) -> RegistryResult<Vec<IndexerId>>;

    /// Get all blocklist entries with full details.
    async fn get_blocklist_entries(&self) -> RegistryResult<Vec<BlocklistEntry>>;

    /// Add an indexer to the blocklist.
    async fn add_to_blocklist(
        &self,
        indexer_id: IndexerId,
        reason: Option<&str>,
    ) -> RegistryResult<()>;

    /// Remove an indexer from the blocklist.
    async fn remove_from_blocklist(&self, indexer_id: IndexerId) -> RegistryResult<()>;
}
