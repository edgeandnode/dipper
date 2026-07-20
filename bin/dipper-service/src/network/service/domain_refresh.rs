//! Periodic re-read of the RecurringCollector's EIP-712 domain, so a running
//! dipper follows an in-place contract upgrade without a restart. A failed
//! refresh is logged and skipped; the domain in use stays until one succeeds.

use std::{future::Future, time::Duration};

use tokio::sync::mpsc;

use crate::chain_client::ChainClientError;

/// A backstop on how long `stop` waits for the loop to acknowledge the signal.
/// The loop drops an in-flight refresh as soon as it is told to stop, so
/// reaching this means something is wedged and must not hold up shutdown.
const STOP_TIMEOUT: Duration = Duration::from_secs(15);

/// Handle for controlling the domain refresh service.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the service gracefully, waiting up to [`STOP_TIMEOUT`] for the loop
    /// to exit. On expiry the rest of shutdown proceeds; the task tree still
    /// joins the task before the process exits.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        if tokio::time::timeout(STOP_TIMEOUT, self.tx_stop.closed())
            .await
            .is_err()
        {
            tracing::warn!(
                timeout_secs = STOP_TIMEOUT.as_secs(),
                "RCA domain refresh did not stop in time; continuing shutdown without it"
            );
        }
    }
}

/// Create a new domain refresh service, returning a handle for lifecycle
/// control and a future to spawn. `refresh` is invoked on each tick of
/// `interval`; it is generic so the wiring is testable without a live chain.
pub fn new<F, Fut>(
    interval: Duration,
    refresh: F,
) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, ChainClientError>>,
{
    let (tx_stop, rx_stop) = mpsc::channel(1);
    (Handle { tx_stop }, run(interval, rx_stop, refresh))
}

/// Runs the refresh until `stop_rx` fires, returning `Ok(())` on stop. A stop
/// arriving mid-refresh drops that attempt; the first (immediate) tick is
/// skipped and a failing `refresh` is logged only.
async fn run<F, Fut>(
    interval: Duration,
    mut stop_rx: mpsc::Receiver<()>,
    mut refresh: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, ChainClientError>>,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! { biased;
            _ = stop_rx.recv() => return Ok(()),
            _ = ticker.tick() => {
                // Stop wins over an in-flight refresh: dropping it costs one
                // read-only round trip, and waiting for it can cost minutes.
                tokio::select! { biased;
                    _ = stop_rx.recv() => return Ok(()),
                    res = refresh() => {
                        if let Err(err) = res {
                            tracing::warn!(
                                error = %err,
                                "RCA EIP-712 domain refresh failed; keeping the current domain"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    /// Wait on a virtual clock until `calls` reaches `target`, or fail. Virtual
    /// time advances whenever the loop is idle, so this returns near instantly.
    async fn wait_for_calls(calls: &Arc<AtomicUsize>, target: usize) {
        tokio::time::timeout(Duration::from_secs(3600), async {
            while calls.load(Ordering::SeqCst) < target {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "refresh reached only {} of {target} calls",
                calls.load(Ordering::SeqCst)
            )
        });
    }

    /// Every tick must actually invoke the refresh, otherwise the domain would
    /// silently never follow a contract upgrade.
    #[tokio::test(start_paused = true)]
    async fn refresh_fires_on_each_tick() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();

        let (handle, fut) = new(Duration::from_secs(60), move || {
            let calls = calls_in.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            }
        });
        let task = tokio::spawn(fut);

        wait_for_calls(&calls, 3).await;

        handle.stop().await;
        task.await
            .expect("refresh task panicked")
            .expect("refresh loop returned an error");
    }

    /// A failed refresh must not end the loop: the current domain is kept and
    /// the next tick tries again, rather than the service going quiet forever.
    #[tokio::test(start_paused = true)]
    async fn refresh_loop_survives_a_failed_refresh() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();

        let (handle, fut) = new(Duration::from_secs(60), move || {
            let calls = calls_in.clone();
            async move {
                // Fail every attempt, the worst case for loop survival.
                calls.fetch_add(1, Ordering::SeqCst);
                Err(ChainClientError::ConfigError("refresh unavailable".into()))
            }
        });
        let task = tokio::spawn(fut);

        wait_for_calls(&calls, 3).await;
        assert!(!task.is_finished(), "a failed refresh ended the loop");

        handle.stop().await;
        task.await
            .expect("refresh task panicked")
            .expect("refresh loop returned an error");
    }

    /// A stop arriving mid-refresh must drop that refresh. Waiting for it lets
    /// a wedged provider hold up shutdown for minutes, because the task tree
    /// joins this task before the process exits.
    #[tokio::test(start_paused = true)]
    async fn stop_drops_an_in_flight_refresh() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();

        let (handle, fut) = new(Duration::from_secs(60), move || {
            let calls = calls_in.clone();
            async move {
                // A refresh that never returns, standing in for a provider
                // that accepts the request and then goes quiet.
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<bool, ChainClientError>>().await
            }
        });
        let task = tokio::spawn(fut);

        wait_for_calls(&calls, 1).await;

        handle.stop().await;
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("stop did not drop the in-flight refresh")
            .expect("refresh task panicked")
            .expect("refresh loop returned an error");
    }

    /// The refresh loop must exit promptly when stopped, even mid-wait, so it
    /// participates in graceful shutdown instead of being a detached task.
    #[tokio::test]
    async fn refresh_loop_stops_on_signal() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();

        // A long interval so no refresh tick fires during the test; the stop
        // arm is what must end the loop.
        let (handle, fut) = new(Duration::from_secs(3600), move || {
            let calls = calls_in.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            }
        });
        let task = tokio::spawn(fut);

        handle.stop().await;
        let result = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("refresh loop did not stop on signal")
            .expect("refresh task panicked");

        assert!(result.is_ok());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "no refresh should have fired before the stop signal"
        );
    }
}
