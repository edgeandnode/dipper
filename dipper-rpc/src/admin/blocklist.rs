//! Blocklist RPC methods for admin operations.

use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use thegraph_core::IndexerId;

// Re-export the canonical BlocklistEntry type
pub use dipper_pgregistry::BlocklistEntry;

/// The _blocklist_ RPC methods for admin operations.
#[rpc(server, client)]
pub trait BlocklistRpc {
    /// Get all blocklisted indexers.
    #[method(name = "blocklist_get_all")]
    async fn blocklist_get_all(&self) -> RpcResult<Vec<BlocklistEntry>>;

    /// Add an indexer to the blocklist.
    #[method(name = "blocklist_add")]
    async fn blocklist_add(&self, indexer_id: IndexerId, reason: Option<String>) -> RpcResult<()>;

    /// Remove an indexer from the blocklist.
    #[method(name = "blocklist_remove")]
    async fn blocklist_remove(&self, indexer_id: IndexerId) -> RpcResult<()>;
}
