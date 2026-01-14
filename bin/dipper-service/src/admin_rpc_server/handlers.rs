use dipper_core::state::FromState;
use dipper_rpc::admin::{
    blocklist::BlocklistRpcServer, indexing_agreements::IndexingAgreementsRpcServer,
    indexing_requests::IndexingRequestsRpcServer,
};
use jsonrpsee::RpcModule;

use crate::{
    registry::{AgreementRegistry, BlocklistRegistry, IndexingRequestRegistry},
    worker::service::WorkerQueue,
};

mod blocklist;
mod indexing_agreements;
mod indexing_requests;

pub use self::{
    blocklist::Ctx as BlocklistCtx, indexing_agreements::Ctx as IndexingAgreementsCtx,
    indexing_requests::Ctx as IndexingRequestsCtx,
};

/// Create a new RPC module with all the admin handlers.
pub(super) fn rpc_handlers<S, R, W>(ctx: S) -> RpcModule<S>
where
    R: IndexingRequestRegistry
        + AgreementRegistry
        + BlocklistRegistry
        + Clone
        + Send
        + Sync
        + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
    IndexingRequestsCtx<R, W>: FromState<S>,
    IndexingAgreementsCtx<R, W>: FromState<S>,
    BlocklistCtx<R>: FromState<S>,
{
    let indexing_requests = indexing_requests::RpcServerImpl::with_context(&ctx);
    let indexing_agreements = indexing_agreements::RpcServerImpl::with_context(&ctx);
    let blocklist = blocklist::RpcServerImpl::with_context(&ctx);

    let mut module = RpcModule::new(ctx);
    module
        .merge(indexing_requests.into_rpc())
        .expect("registration of 'indexing requests' RPC handlers failed");
    module
        .merge(indexing_agreements.into_rpc())
        .expect("registration of 'indexing agreements' RPC handlers failed");
    module
        .merge(blocklist.into_rpc())
        .expect("registration of 'blocklist' RPC handlers failed");
    module
}
