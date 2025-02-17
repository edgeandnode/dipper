use std::{future::Future, net::SocketAddr};

use dipper_core::state::FromState;
use dipper_registry::Registry;
use dipper_rpc::indexer::gateway_server::rpc::GatewayDipsServiceServer;
use tokio::sync::mpsc;
use tonic::transport::Server;

use crate::{
    indexer_rpc_server::handlers::{DipsGatewayServiceCtx, DipsGatewayServiceImpl},
    network::NetworkProvider,
    worker::WorkerQueue,
};

/// RPC server configuration.
#[derive(Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
}

/// The RPC server service handle.
///
/// If all handles are dropped, the RPC server will be stopped.
#[derive(Clone)]
pub struct Handle {
    /// A channel to stop the RPC server
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the RPC server
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Create a new Indexer RPC server service
pub fn new<S, R, N, W>(conf: Config, ctx: S) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: Registry + Clone + Send + Sync + 'static,
    N: NetworkProvider + Clone + Send + Sync + 'static,
    W: WorkerQueue + Clone + Send + Sync + 'static,
    DipsGatewayServiceCtx<R, N, W>: FromState<S>,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let fut = async move {
        tracing::info!(listen_addr=%conf.listen_addr, "Starting indexer RPC server");

        let service_impl = DipsGatewayServiceImpl::with_context(&ctx);

        // Start the RPC server
        Server::builder()
            .add_service(GatewayDipsServiceServer::new(service_impl))
            .serve_with_shutdown(conf.listen_addr, async move {
                tracing::debug!("Stopping admin RPC server");
                let _ = rx_stop.recv().await;
            })
            .await?;

        Ok(())
    };

    (Handle { tx_stop }, fut)
}
