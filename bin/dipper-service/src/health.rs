//! Worker liveness watermark and a minimal HTTP health endpoint.
//!
//! Exit-based supervision catches a worker that *exits* (panic or error
//! return). It cannot catch a worker that is *wedged* — alive but making no
//! progress (e.g. parked on an await that nothing bounds). This module closes
//! that gap: the worker ticks a progress watermark every loop iteration, and a
//! small health server reports 503 once the watermark goes stale so an external
//! orchestrator (k8s liveness probe) can restart the wedged process.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use time::OffsetDateTime;
use tokio::{net::TcpListener, sync::mpsc};

/// Default staleness threshold after which the worker is considered wedged.
///
/// It must comfortably exceed the worst-case gap between two worker progress
/// ticks: the loop ticks the watermark once per iteration, so the longest a
/// healthy worker can go without ticking is however long a single job takes to
/// process. 600s leaves ample headroom so a legitimately slow job never trips
/// the probe.
pub const DEFAULT_HEALTH_THRESHOLD: Duration = Duration::from_secs(600);

/// Shared liveness watermark: the unix-seconds timestamp at which the worker
/// last made progress. The worker ticks it via [`Liveness::record_progress`];
/// the health server reads it via [`Liveness::is_healthy`].
#[derive(Clone)]
pub struct Liveness {
    last_progress: Arc<AtomicI64>,
}

fn now_unix() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

impl Liveness {
    /// Creates a watermark seeded to now, so a freshly started worker is
    /// considered live (startup grace).
    pub fn new() -> Self {
        Self {
            last_progress: Arc::new(AtomicI64::new(now_unix())),
        }
    }

    /// Records that the worker just made progress.
    pub fn record_progress(&self) {
        self.last_progress.store(now_unix(), Ordering::Relaxed);
    }

    /// Whether the worker made progress within `threshold` of `now_unix`.
    ///
    /// Pure in its inputs so it is unit testable without touching the clock.
    pub fn is_healthy_at(&self, now_unix: i64, threshold: Duration) -> bool {
        let age = now_unix.saturating_sub(self.last_progress.load(Ordering::Relaxed));
        age <= threshold.as_secs() as i64
    }

    /// [`Liveness::is_healthy_at`] against the current clock.
    pub fn is_healthy(&self, threshold: Duration) -> bool {
        self.is_healthy_at(now_unix(), threshold)
    }
}

impl Default for Liveness {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to stop the health server.
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stops the health server.
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Binds the health server and returns a stop handle plus its run future.
///
/// The listener is bound eagerly so a bind failure surfaces at startup (and so
/// callers/tests can read the actual bound address).
pub async fn new(
    addr: SocketAddr,
    liveness: Liveness,
    threshold: Duration,
) -> anyhow::Result<(
    Handle,
    SocketAddr,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (tx_stop, rx_stop) = mpsc::channel(1);
    let fut = serve(listener, liveness, threshold, rx_stop);
    Ok((Handle { tx_stop }, local_addr, fut))
}

/// State shared with the [`health`] handler.
#[derive(Clone)]
struct HealthState {
    liveness: Liveness,
    threshold: Duration,
}

/// `GET /health`: 200 while the worker watermark is fresh, 503 once it has gone
/// stale (the worker is wedged and the orchestrator should restart it).
async fn health(State(state): State<HealthState>) -> impl IntoResponse {
    if state.liveness.is_healthy(state.threshold) {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "worker stalled")
    }
}

/// Serves health requests until `stop_rx` fires.
///
/// Backed by axum/hyper (already in the dependency tree via the RPC servers),
/// so request parsing, per-connection isolation, connection timeouts and
/// graceful shutdown are handled by the HTTP stack rather than by hand.
async fn serve(
    listener: TcpListener,
    liveness: Liveness,
    threshold: Duration,
    mut stop_rx: mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "health server listening");
    let app = Router::new()
        .route("/health", get(health))
        .with_state(HealthState {
            liveness,
            threshold,
        });
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = stop_rx.recv().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;

    #[test]
    fn fresh_watermark_is_healthy() {
        let liveness = Liveness::new();
        let now = now_unix();
        assert!(liveness.is_healthy_at(now, Duration::from_secs(600)));
    }

    #[test]
    fn stale_watermark_is_unhealthy() {
        let liveness = Liveness::new();
        // Pretend "now" is well past the threshold since the watermark was set.
        let now = now_unix() + 10_000;
        assert!(
            !liveness.is_healthy_at(now, Duration::from_secs(600)),
            "a watermark older than the threshold must report unhealthy"
        );
    }

    #[test]
    fn boundary_is_inclusive_healthy() {
        let liveness = Liveness::new();
        let base = now_unix();
        // Exactly at the threshold is still healthy; one second past is not.
        assert!(liveness.is_healthy_at(base + 600, Duration::from_secs(600)));
        assert!(!liveness.is_healthy_at(base + 601, Duration::from_secs(600)));
    }

    /// Reads an HTTP response's status line from a fresh connection to the
    /// server.
    async fn probe(addr: SocketAddr) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // `Connection: close` so the server closes the socket after responding
        // and `read_to_end` returns rather than blocking on HTTP/1.1 keep-alive.
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        text.lines().next().unwrap_or_default().to_string()
    }

    #[tokio::test]
    async fn server_reports_503_when_stalled() {
        let liveness = Liveness::new();
        // A zero threshold makes the (initially fresh) watermark immediately
        // stale, so we exercise the unhealthy branch deterministically.
        let (handle, addr, fut) = new("127.0.0.1:0".parse().unwrap(), liveness, Duration::ZERO)
            .await
            .unwrap();
        // Ensure the watermark is at least a second old so age > 0.
        let server = tokio::spawn(fut);

        // Wait a moment so the seeded watermark is strictly in the past.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let status = probe(addr).await;
        assert!(
            status.contains("503"),
            "expected 503 when stalled, got: {status:?}"
        );

        handle.stop().await;
        let _ = server.await;
    }

    #[tokio::test]
    async fn server_reports_200_when_live() {
        let liveness = Liveness::new();
        let (handle, addr, fut) = new(
            "127.0.0.1:0".parse().unwrap(),
            liveness,
            Duration::from_secs(600),
        )
        .await
        .unwrap();
        let server = tokio::spawn(fut);

        let status = probe(addr).await;
        assert!(
            status.contains("200"),
            "expected 200 when live, got: {status:?}"
        );

        handle.stop().await;
        let _ = server.await;
    }

    /// A client that connects but never sends must not wedge the server: other
    /// probes are still answered promptly, and graceful shutdown still
    /// completes. hyper isolates each connection, but this guards against a
    /// future regression that serves connections without that isolation.
    #[tokio::test]
    async fn stalled_client_does_not_block_other_probes() {
        let liveness = Liveness::new();
        let (handle, addr, fut) = new(
            "127.0.0.1:0".parse().unwrap(),
            liveness,
            Duration::from_secs(600),
        )
        .await
        .unwrap();
        let server = tokio::spawn(fut);

        // Open a connection and never write to it; its server-side read blocks.
        let _stalled = TcpStream::connect(addr).await.unwrap();

        // A well-behaved probe must still get a response without waiting on the
        // stalled client.
        let status = tokio::time::timeout(Duration::from_secs(2), probe(addr))
            .await
            .expect("a stalled client blocked an independent probe");
        assert!(status.contains("200"), "expected 200, got: {status:?}");

        // Shutdown must not wait on the stalled connection either.
        tokio::time::timeout(Duration::from_secs(2), handle.stop())
            .await
            .expect("a stalled client blocked shutdown");
        let _ = server.await;
    }
}
