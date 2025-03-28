use dipper_core::state::FromState;
use dipper_rpc::admin::{
    indexing_agreements::IndexingAgreementsRpcServer, indexing_requests::IndexingRequestsRpcServer,
};
use jsonrpsee::RpcModule;

use self::{
    indexing_agreements::IndexingAgreementsRpcServerImpl,
    indexing_requests::IndexingRequestsRpcServerImpl,
};
use crate::{
    registry::{AgreementRegistry, IndexingRequestRegistry},
    worker::WorkerQueue,
};

mod indexing_agreements;
mod indexing_requests;

pub use self::{
    indexing_agreements::IndexingAgreementsCtx, indexing_requests::IndexingRequestsCtx,
};

/// Create a new RPC module with all the admin handlers.
pub(super) fn rpc_handlers<S, R, W>(ctx: S) -> RpcModule<S>
where
    R: IndexingRequestRegistry + AgreementRegistry + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
    IndexingRequestsCtx<R, W>: FromState<S>,
    IndexingAgreementsCtx<R, W>: FromState<S>,
{
    // Indexing requests
    let indexing_requests = IndexingRequestsRpcServerImpl::with_context(&ctx);

    // Indexing agreements
    let indexing_agreements = IndexingAgreementsRpcServerImpl::with_context(&ctx);

    // Indexing receipts
    // TODO(post-mvp): Register the indexing receipts RPC handlers

    let mut module = RpcModule::new(ctx);
    module
        .merge(indexing_requests.into_rpc())
        .expect("registration of 'indexing requests' RPC handlers failed");
    module
        .merge(indexing_agreements.into_rpc())
        .expect("registration of 'indexing agreements' RPC handlers failed");
    module
}
