use dipper_core::state::FromState;
use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use dipper_rpc::admin::{
    indexing_agreements::IndexingAgreementsRpcServer, indexing_requests::IndexingRequestsRpcServer,
};
use jsonrpsee::RpcModule;

use self::indexing_requests::{IndexingRequestsCtx, IndexingRequestsRpcServerImpl};
use crate::{
    rpc_server::handlers::indexing_agreements::{
        IndexingAgreementsCtx, IndexingAgreementsRpcServerImpl,
    },
    worker::messages::Message,
};

mod indexing_agreements;
mod indexing_requests;

/// Create a new RPC module with all the admin handlers.
pub(super) fn admin_rpc_handlers<C, R, W>(ctx: C) -> RpcModule<C>
where
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
    IndexingRequestsCtx<R, W>: FromState<C>,
    IndexingAgreementsCtx<R, W>: FromState<C>,
{
    // Indexing requests
    let indexing_requests = IndexingRequestsRpcServerImpl::with_context(&ctx);

    // Indexing agreements
    let indexing_agreements = IndexingAgreementsRpcServerImpl::with_context(&ctx);

    // Indexing receipts
    // TODO: Register the indexing receipts RPC handlers

    let mut module = RpcModule::new(ctx);
    module
        .merge(indexing_requests.into_rpc())
        .expect("registration of 'indexing requests' RPC handlers failed");
    module
        .merge(indexing_agreements.into_rpc())
        .expect("registration of 'indexing agreements' RPC handlers failed");
    module
}
