use std::{future::Future, net::SocketAddr};

use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use jsonrpsee::server::Server;
use tokio::sync::mpsc;

use super::context::Ctx;
use crate::{rpc_server::handlers::admin_rpc_handlers, worker::messages::Message};

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

/// Create a new Admin RPC server service
pub fn new_admin_rpc_service<R, W>(
    conf: Config,
    ctx: Ctx<R, W>,
) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let fut = async move {
        tracing::info!(listen_addr=%conf.listen_addr, "Starting admin RPC server");

        // Start the RPC server
        let server = Server::builder()
            .http_only()
            .max_request_body_size(1024 * 1024) // 1 MB
            .set_tcp_no_delay(true)
            .build(conf.listen_addr)
            .await?;

        let handle = server.start(admin_rpc_handlers(ctx));
        let svc_handle = handle.clone();

        // Wait for either the server to stop, or a stop signal
        tokio::select! {biased;
            _ = svc_handle.stopped() => {}
            _ = rx_stop.recv() => {
                tracing::debug!("Stopping admin RPC server");

                // Notify the server and wait for it to stop
                if let Ok(()) = handle.stop() {
                    handle.stopped().await;
                } else {
                    tracing::warn!("The admin RPC server is already stopped")
                }
            }
        }

        Ok(())
    };

    (Handle { tx_stop }, fut)
}
