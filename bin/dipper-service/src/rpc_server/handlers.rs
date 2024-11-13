use dipper_core::{
    rpc::indexing_requests::{AdminIndexingRequestsRpcServer, IndexingRequestsRpcServer},
    state::FromState,
};
use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use jsonrpsee::RpcModule;

use self::indexing_requests::{
    AdminIndexingRequestsCtx, AdminIndexingRequestsRpcServerImpl, IndexingRequestsCtx,
    IndexingRequestsRpcServerImpl,
};
use crate::worker::messages::Message;

mod indexing_requests;

/// Create a new RPC module with all the admin handlers.
pub(super) fn admin_rpc_handlers<C, R, W>(ctx: C) -> RpcModule<C>
where
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
    AdminIndexingRequestsCtx<R, W>: FromState<C>,
    IndexingRequestsCtx<R>: FromState<C>,
{
    // Indexing requests
    let admin_indexing_requests = AdminIndexingRequestsRpcServerImpl::with_context(&ctx);
    let indexing_requests = IndexingRequestsRpcServerImpl::with_context(&ctx);

    // Indexing agreements
    // TODO: Register the indexing agreements RPC handlers

    // Indexing receipts
    // TODO: Register the indexing receipts RPC handlers

    let mut module = RpcModule::new(ctx);
    module
        .merge(admin_indexing_requests.into_rpc())
        .expect("registration of 'indexing requests (admin)' RPC handlers failed");
    module
        .merge(indexing_requests.into_rpc())
        .expect("registration of 'indexing requests' RPC handlers failed");
    module
}
