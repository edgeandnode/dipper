use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
};

use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use jsonrpsee::server::Server;
use tokio::sync::mpsc;

use super::context::Ctx;
use crate::{rpc_server::handlers::admin_rpc_handlers, worker::messages::Message};

/// RPC server HTTP configuration.
#[derive(Debug)]
pub struct HttpConfig {
    pub http_port: u16,
}

/// The RPC server service handle.
///
/// If all handles are dropped, the RPC server will be stopped.
#[derive(Clone)]
pub struct Handle {
    /// A channel to stop the RPC server
    stop_tx: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the RPC server
    pub async fn stop(self) {
        if self.stop_tx.is_closed() {
            return;
        }

        let _ = self.stop_tx.send(()).await;
    }
}

/// Create a new Admin RPC server service.
pub fn new_admin_rpc_service<R, W>(
    config: HttpConfig,
    state: Ctx<R, W>,
) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    let (stop_tx, mut stop_rx) = mpsc::channel(1);

    let handle = Handle { stop_tx };

    let fut = async move {
        tracing::info!("Starting RPC server at '0.0.0.0:{}'", config.http_port);

        // Start the RPC server
        let server = Server::builder()
            .http_only()
            .max_request_body_size(1024 * 1024) // 1 MB
            .set_tcp_no_delay(true)
            .build(SocketAddr::new(
                IpAddr::from([0, 0, 0, 0]),
                config.http_port,
            ))
            .await?;

        let handle = server.start(admin_rpc_handlers(state));
        let svc_handle = handle.clone();

        // Wait for either the server to stop or a stop signal
        tokio::select! {biased;
            _ = svc_handle.stopped() => {}
            _ = stop_rx.recv() => {
                tracing::info!("Stopping RPC server");
                let _ = handle.stop();
            }
        }

        Ok(())
    };

    (handle, fut)
}
