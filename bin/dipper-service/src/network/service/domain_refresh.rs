//! Periodic re-read of the RecurringCollector's EIP-712 domain, so a running
//! dipper follows an in-place contract upgrade without a restart. A failed
//! refresh is logged and skipped; the domain in use stays until one succeeds.

use std::{future::Future, time::Duration};

use tokio::sync::mpsc;

use crate::chain_client::ChainClientError;

/// Runs the refresh until `stop_rx` fires, returning `Ok(())` on stop. Generic
/// over the refresh action so the stop wiring is testable without a live chain.
/// The first (immediate) tick is skipped; a failing `refresh` is logged only.
pub async fn run<F, Fut>(
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
                if let Err(err) = refresh().await {
                    tracing::warn!(
                        error = %err,
                        "RCA EIP-712 domain refresh failed; keeping the current domain"
                    );
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

    /// The refresh loop must exit promptly when stopped, even mid-wait, so it
    /// participates in graceful shutdown instead of being a detached task.
    #[tokio::test]
    async fn refresh_loop_stops_on_signal() {
        let (tx, rx) = mpsc::channel(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();

        // A long interval so no refresh tick fires during the test; the stop
        // arm is what must end the loop.
        let handle = tokio::spawn(run(Duration::from_secs(3600), rx, move || {
            let calls = calls_in.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            }
        }));

        tx.send(()).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
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
