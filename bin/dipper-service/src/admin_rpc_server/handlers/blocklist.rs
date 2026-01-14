//! Blocklist RPC handler implementation.

use async_trait::async_trait;
use dipper_core::state::FromState;
use dipper_rpc::admin::blocklist::{BlocklistEntry, BlocklistRpcServer};
use jsonrpsee::{core::RpcResult, types::ErrorObject};
use thegraph_core::IndexerId;

use crate::registry::{BlocklistRegistry, Error as RegistryError};

/// The substate for the [`BlocklistRpc`] handler.
pub struct Ctx<R> {
    pub registry: R,
}

pub struct RpcServerImpl<R>(Ctx<R>);

impl<R> RpcServerImpl<R> {
    /// Create a new instance of the `BlocklistRpcServerImpl` with the given context.
    pub fn with_context<C>(ctx: &C) -> Self
    where
        Ctx<R>: FromState<C>,
    {
        Self(FromState::from_state(ctx))
    }
}

#[async_trait]
impl<R> BlocklistRpcServer for RpcServerImpl<R>
where
    R: BlocklistRegistry + Clone + Send + Sync + 'static,
{
    async fn blocklist_get_all(&self) -> RpcResult<Vec<BlocklistEntry>> {
        self.0
            .registry
            .get_blocklist_entries()
            .await
            .map_err(|err| {
                tracing::error!(error=?err, "Failed to get blocklist");
                ErrorObject::borrowed(503, "Service unavailable", None)
            })
    }

    async fn blocklist_add(&self, indexer_id: IndexerId, reason: Option<String>) -> RpcResult<()> {
        match self
            .0
            .registry
            .add_to_blocklist(indexer_id, reason.as_deref())
            .await
        {
            Ok(()) => {
                tracing::info!(%indexer_id, ?reason, "Added indexer to blocklist");
                Ok(())
            }
            Err(err) => {
                tracing::error!(error=?err, %indexer_id, "Failed to add indexer to blocklist");
                Err(ErrorObject::borrowed(503, "Service unavailable", None))
            }
        }
    }

    async fn blocklist_remove(&self, indexer_id: IndexerId) -> RpcResult<()> {
        match self.0.registry.remove_from_blocklist(indexer_id).await {
            Ok(()) => {
                tracing::info!(%indexer_id, "Removed indexer from blocklist");
                Ok(())
            }
            Err(RegistryError::NoRecordsUpdated) => {
                Err(ErrorObject::borrowed(404, "Indexer not in blocklist", None))
            }
            Err(err) => {
                tracing::error!(error=?err, %indexer_id, "Failed to remove indexer from blocklist");
                Err(ErrorObject::borrowed(503, "Service unavailable", None))
            }
        }
    }
}
