use dipper_core::{
    rpc::{
        indexing_agreements::{
            AdminIndexingAgreementsRpcServer, IndexerIndexingAgreementsRpcServer,
            IndexingAgreementsRpcServer,
        },
        indexing_requests::{AdminIndexingRequestsRpcServer, IndexingRequestsRpcServer},
    },
    state::FromState,
};
use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use jsonrpsee::RpcModule;

use self::indexing_requests::{
    AdminIndexingRequestsCtx, AdminIndexingRequestsRpcServerImpl, IndexingRequestsCtx,
    IndexingRequestsRpcServerImpl,
};
use crate::{
    rpc_server::handlers::indexing_agreements::{
        AdminIndexingAgreementsCtx, AdminIndexingAgreementsRpcServerImpl,
        IndexerIndexingAgreementsCtx, IndexerIndexingAgreementsRpcServerImpl,
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
    IndexingRequestsCtx<R>: FromState<C>,
    AdminIndexingRequestsCtx<R, W>: FromState<C>,
    IndexingAgreementsCtx<R>: FromState<C>,
    AdminIndexingAgreementsCtx<R, W>: FromState<C>,
{
    // Indexing requests
    let indexing_requests = IndexingRequestsRpcServerImpl::with_context(&ctx);
    let admin_indexing_requests = AdminIndexingRequestsRpcServerImpl::with_context(&ctx);

    // Indexing agreements
    let indexing_agreements = IndexingAgreementsRpcServerImpl::with_context(&ctx);
    let admin_indexing_agreements = AdminIndexingAgreementsRpcServerImpl::with_context(&ctx);

    // Indexing receipts
    // TODO: Register the indexing receipts RPC handlers

    let mut module = RpcModule::new(ctx);
    module
        .merge(indexing_requests.into_rpc())
        .expect("registration of 'indexing requests' RPC handlers failed");
    module
        .merge(admin_indexing_requests.into_rpc())
        .expect("registration of 'indexing requests (admin)' RPC handlers failed");
    module
        .merge(indexing_agreements.into_rpc())
        .expect("registration of 'indexing agreements' RPC handlers failed");
    module
        .merge(admin_indexing_agreements.into_rpc())
        .expect("registration of 'indexing agreements (admin)' RPC handlers failed");
    module
}

/// Create a new RPC module with all the indexer handlers.
pub(super) fn indexers_rpc_handlers<C, W>(ctx: C) -> RpcModule<C>
where
    W: Queue<Message> + Clone + Send + Sync + 'static,
    IndexerIndexingAgreementsCtx<W>: FromState<C>,
{
    // Indexing agreements
    let indexer_indexing_agreements = IndexerIndexingAgreementsRpcServerImpl::with_context(&ctx);

    // Indexing receipts
    // TODO: Register the indexing receipts RPC handlers

    let mut module = RpcModule::new(ctx);
    module
        .merge(indexer_indexing_agreements.into_rpc())
        .expect("registration of 'indexing agreements (indexer)' RPC handlers failed");
    module
}
