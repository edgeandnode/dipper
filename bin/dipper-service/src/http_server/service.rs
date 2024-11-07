use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
};

use axum::{extract::FromRef, routing, Router};
use dipper_pgmq::queue::Queue;
use dipper_registry::Registry;
use tokio::sync::mpsc;

use crate::{
    http_server::{context::Ctx, handlers},
    worker::messages::Message,
};

#[derive(Debug)]
pub struct HttpConfig {
    pub http_port: u16,
}

/// The HTTP server service handle.
///
/// If all handles are dropped, the HTTP server will be stopped.
#[derive(Clone)]
pub struct Handle {
    /// A channel to stop the HTTP server
    stop_tx: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the HTTP server
    pub fn stop(self) {
        let _ = self.stop_tx.try_send(());
    }
}

/// Create a new HTTP server service.
pub fn new<R, W>(
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
        tracing::info!("Starting HTTP server at '0.0.0.0:{}'", config.http_port);

        // Router
        // TODO: Add readiness and liveness probes
        let router = Router::new()
            .nest("/indexings", indexing_requests_router::<_, R, W>())
            .nest("/agreements", indexing_agreements_router::<_, R, W>())
            .nest("/receipts", indexing_receipts_router::<_, R, W>())
            .with_state(state);

        // Start the HTTP server
        let listener = tokio::net::TcpListener::bind(SocketAddr::new(
            IpAddr::from([0, 0, 0, 0]),
            config.http_port,
        ))
        .await?;

        axum::serve(listener, router)
            .tcp_nodelay(true)
            .with_graceful_shutdown(async move {
                // Wait for the stop signal, i.e., wait for a message (or the channel to be closed)
                let _ = stop_rx.recv().await;

                tracing::info!("Shutting down HTTP server");
            })
            .await?;

        Ok(())
    };

    (handle, fut)
}

/// Create a new [`Router`] for the `/indexings` resource.
///
/// [`Router`]: axum::routing::Router
fn indexing_requests_router<S, R, W>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
    handlers::GetIndexingRequestsCtx<R>: FromRef<S>,
    handlers::NewIndexingRequestCtx<R, W>: FromRef<S>,
    handlers::CancelIndexingRequestCtx<R, W>: FromRef<S>,
{
    Router::new()
        .route("/", routing::get(handlers::get_all_indexing_requests))
        .route(
            "/:indexing_request_id",
            routing::get(handlers::get_indexing_request_by_id),
        )
        .route("/", routing::post(handlers::register_new_indexing_request))
        .route(
            "/:indexing_request_id/cancel",
            routing::put(handlers::cancel_indexing_request),
        )
}

/// Create a new [`Router`] for the `/agreements` resource.
///
/// [`Router`]: axum::routing::Router
#[allow(clippy::extra_unused_type_parameters)] // TODO: Remove this once the router is implemented
fn indexing_agreements_router<S, R, W>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    Router::new()
    // TODO: Add "get index agreement by id" GET route
    // TODO: Add "cancel indexing agreement" PUT route (Indexer only)
}

/// Create a new [`Router`] for the `/receipts` resource.
///
/// [`Router`]: axum::routing::Router
#[allow(clippy::extra_unused_type_parameters)] // TODO: Remove this once the router is implemented
fn indexing_receipts_router<S, R, W>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    R: Registry + Clone + Send + Sync + 'static,
    W: Queue<Message> + Clone + Send + Sync + 'static,
{
    Router::new()
    // TODO: Add "get receipt by id" GET route
    // TODO: Add "get receipts by indexing agreement id" GET route
    // TODO: Add "get receipts by allocation id" GET route
    // TODO: Add "redeem receipt" PUT route (Indexer only)
}
