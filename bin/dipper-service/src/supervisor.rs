//! Process supervision for the task tree.
//!
//! Every long-running service is spawned into a single [`JoinSet`]. In normal
//! operation none of them ever completes on its own; they run until the
//! shutdown sequence stops them. So a task that finishes *before* shutdown was
//! deliberately requested is, by definition, an unexpected critical-task exit
//! (a panic, or a loop returning `Err` for an unrecoverable reason).
//!
//! Leaving the process running in that state (other services up, the daemon
//! looking healthy, but a critical task dead) is exactly the silent-stall
//! failure mode we refuse to allow. [`supervise`] therefore treats any such
//! exit as fatal: it requests shutdown so the rest of the tree is torn down
//! cleanly, then returns an error so the process exits non-zero and the
//! orchestrator restarts it.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, task::JoinSet};

/// Shared shutdown coordination between [`supervise`] and the task that runs
/// the graceful stop sequence.
///
/// Cloneable; all clones observe the same state.
#[derive(Clone, Default)]
pub struct Shutdown {
    requested: Arc<AtomicBool>,
    trigger: Arc<Notify>,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks shutdown as requested and wakes whoever awaits
    /// [`Shutdown::requested_signal`]. Idempotent.
    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
        // `notify_waiters`, not `notify_one`: multiple tasks may be parked in
        // `requested_signal`, and shutdown must wake all of them. It stores no
        // permit for future waiters, but none is needed: a waiter that arrives
        // after this sees the flag via the register-then-check in
        // `requested_signal` and returns without parking.
        self.trigger.notify_waiters();
    }

    /// Whether shutdown has been requested (by a signal or by an unexpected
    /// task exit).
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    /// Resolves once shutdown has been requested.
    ///
    /// Uses the register-then-check ordering so a [`Shutdown::request`] racing
    /// between the flag read and the wait can't be lost.
    pub async fn requested_signal(&self) {
        let notified = self.trigger.notified();
        tokio::pin!(notified);
        // Register interest before re-reading the flag.
        notified.as_mut().enable();
        if self.is_requested() {
            return;
        }
        notified.await;
    }
}

/// Drains `task_tree`, treating any task that finishes before shutdown was
/// requested as a fatal unexpected exit.
///
/// On the first such exit it logs, requests shutdown (so the stop-sequence task
/// tears the rest of the tree down), and keeps draining. A task that instead
/// fails or panics (whether or not shutdown is already underway) is also fatal:
/// a crash during teardown must never be masked into a clean exit. Returns
/// `Err` if any unexpected exit, failure, panic, or teardown stall occurred,
/// otherwise `Ok(())` (a deliberate, fully clean shutdown).
///
/// Once shutdown has been requested the wait for each remaining task is bounded
/// by `teardown_grace`. The graceful stop sequence itself runs inside one of
/// these tasks, so if that task is the casualty (it panics before stopping the
/// others) nobody is left to stop the remaining services and they would run
/// forever, leaving this drain blocked indefinitely: the exact silent stall
/// this module exists to prevent. The bound is a no-progress watchdog, reset
/// each time a task finishes, so a teardown that is genuinely making progress
/// (services stopping one after another, each taking its own time) is never cut
/// short; it only trips when nothing finishes for the whole grace window, at
/// which point the remaining tasks are aborted and the process exits non-zero.
pub async fn supervise(
    mut task_tree: JoinSet<anyhow::Result<()>>,
    shutdown: &Shutdown,
    teardown_grace: Duration,
) -> anyhow::Result<()> {
    // What actually went wrong first. This is the last line in the logs before a
    // restart, so it names the task rather than just saying something died.
    let mut first_failure: Option<String> = None;

    loop {
        // Before shutdown, tasks are meant to run forever, so wait unbounded.
        // Once shutdown is underway, time-box the wait so a dead stop sequence
        // can't wedge the drain (see the function docs).
        let next = if shutdown.is_requested() {
            match tokio::time::timeout(teardown_grace, task_tree.join_next_with_id()).await {
                Ok(next) => next,
                Err(_elapsed) => {
                    tracing::error!(
                        grace_secs = teardown_grace.as_secs(),
                        "teardown stalled: no task finished within the grace period after \
                         shutdown was requested. Aborting the remaining tasks and exiting \
                         non-zero so the orchestrator restarts the process rather than hanging"
                    );
                    task_tree.abort_all();
                    anyhow::bail!(
                        "teardown stalled: no task finished within {}s of shutdown being requested",
                        teardown_grace.as_secs()
                    );
                }
            }
        } else {
            tokio::select! {
                next = task_tree.join_next_with_id() => next,
                // Shutdown was requested with no task finishing to wake us. Loop
                // so the next wait is the bounded one and the watchdog starts;
                // `join_next_with_id` is cancel-safe, so dropping it loses none.
                _ = shutdown.requested_signal() => continue,
            }
        };

        let Some(res) = next else { break };

        let id = match &res {
            Ok((id, _)) => *id,
            Err(err) => err.id(),
        };

        match &res {
            Ok((_, Ok(()))) => {
                tracing::debug!(task_id = %id, "task completed");
            }
            Ok((_, Err(err))) => {
                // A task returning `Err` is a failure regardless of whether
                // shutdown is already underway; never let it pass as clean.
                tracing::error!(task_id = %id, error = ?err, "task failed");
                first_failure.get_or_insert_with(|| format!("task {id} failed: {err:#}"));
            }
            Err(err) => {
                // A panic (join error) is likewise fatal in either state, so a
                // crash while the tree is being torn down still exits non-zero.
                tracing::error!(task_id = %id, error = ?err, "task join error");
                first_failure.get_or_insert_with(|| format!("task {id} panicked: {err}"));
            }
        }

        if !shutdown.is_requested() {
            tracing::error!(
                task_id = %id,
                "a critical task exited unexpectedly; initiating shutdown so the process \
                 restarts rather than running on with a dead task"
            );
            first_failure
                .get_or_insert_with(|| format!("task {id} exited before shutdown was requested"));
            shutdown.request();
        }
    }

    if let Some(cause) = first_failure {
        anyhow::bail!("a critical task exited unexpectedly: {cause}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::task::JoinSet;

    use super::{Shutdown, supervise};

    /// Bound on how long `supervise` may run in a test. A broken impl that
    /// never requests shutdown would otherwise hang the coordinator stand-in
    /// forever; this turns that into a deterministic failure.
    const SUPERVISE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Teardown grace passed to `supervise` in tests. Short so the stall test
    /// resolves quickly; the clean-path tests finish well within it.
    const TEST_TEARDOWN_GRACE: Duration = Duration::from_millis(200);

    /// A task finishing before shutdown was requested is fatal: `supervise`
    /// returns an error and requests shutdown so the rest of the tree is torn
    /// down. Regression test for the join loop that merely logged a dead task
    /// and let the process keep running.
    #[tokio::test]
    async fn unexpected_task_exit_is_fatal_and_triggers_shutdown() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the stop-sequence task: drains once shutdown is asked
        // for. If `supervise` fails to request shutdown, this never completes
        // and the test hangs (caught by the harness timeout).
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.requested_signal().await;
            Ok(())
        });

        // A critical task that exits on its own, with nobody having asked for
        // shutdown.
        task_tree.spawn(async { Ok(()) });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not request shutdown; coordinator never drained");

        assert!(result.is_err(), "an unexpected task exit must be fatal");
        assert!(
            shutdown.is_requested(),
            "an unexpected task exit must request shutdown"
        );
    }

    /// When shutdown was deliberately requested, tasks completing afterwards is
    /// the normal path and must not be reported as fatal.
    #[tokio::test]
    async fn deliberate_shutdown_is_not_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // The "signal handler": requests shutdown, then completes.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // Other services that exit only after shutdown is requested.
        for _ in 0..3 {
            let s = shutdown.clone();
            task_tree.spawn(async move {
                s.requested_signal().await;
                Ok(())
            });
        }

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not drain after a deliberate shutdown");

        assert!(
            result.is_ok(),
            "a deliberate shutdown must not be reported as fatal: {result:?}"
        );
    }

    /// If the task that runs the stop sequence dies (or otherwise never stops
    /// the remaining services) after shutdown was requested, the leftover
    /// services would run forever and the drain would block indefinitely.
    /// `supervise` must instead give up after the grace period, abort what's
    /// left, and return an error so the process exits non-zero rather than
    /// hanging in the silent-stall state. Regression test for the teardown
    /// watchdog.
    #[tokio::test]
    async fn teardown_stall_is_bounded_and_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the stop-sequence task: it requests shutdown and then
        // exits without stopping anything (as if it had panicked mid-teardown).
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // A service that never gets a stop signal, so it runs forever. Without
        // the watchdog the drain would wait on this task with no end.
        task_tree.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise hung on a stalled teardown instead of bounding it");

        assert!(
            result.is_err(),
            "a stalled teardown must be fatal, not a silent hang"
        );
    }

    /// The same stall with the stop sequence wedging on its first step, so *no*
    /// task finishes. The test above lets its coordinator return, which wakes the
    /// drain; here the watchdog has to start off the shutdown flag alone.
    #[tokio::test]
    async fn teardown_stall_before_any_task_finishes_is_bounded() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // Stand-in for the signal handler: asks for shutdown, then wedges on its
        // first stop step and never returns.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            std::future::pending::<()>().await;
            Ok(())
        });

        // A service nobody is left to stop.
        task_tree.spawn(async {
            std::future::pending::<()>().await;
            Ok(())
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise hung: the teardown watchdog never engaged");

        assert!(
            result.is_err(),
            "a teardown that stalls before any task finishes must still be fatal"
        );
    }

    /// A task that panics (or returns `Err`) while the tree is being torn down
    /// must still make the process exit non-zero, not be masked into a clean
    /// exit just because shutdown was already requested. Regression test for
    /// the arms that logged a failed/panicked task without flagging it fatal.
    #[tokio::test]
    async fn panic_during_shutdown_is_fatal() {
        let shutdown = Shutdown::new();
        let mut task_tree: JoinSet<anyhow::Result<()>> = JoinSet::new();

        // The "signal handler": requests shutdown, then completes cleanly.
        let coordinator = shutdown.clone();
        task_tree.spawn(async move {
            coordinator.request();
            Ok(())
        });

        // A service that panics once shutdown is underway.
        let s = shutdown.clone();
        task_tree.spawn(async move {
            s.requested_signal().await;
            panic!("boom during teardown");
        });

        let result = tokio::time::timeout(
            SUPERVISE_TIMEOUT,
            supervise(task_tree, &shutdown, TEST_TEARDOWN_GRACE),
        )
        .await
        .expect("supervise did not drain after a panic during shutdown");

        let err = result.expect_err("a panic during teardown must be fatal, not masked");
        assert!(
            err.to_string().contains("panicked"),
            "the error must name what actually died, not just say something did: {err}"
        );
    }

    /// A single `request()` must wake *every* parked waiter, not just one.
    ///
    /// Both waiters are polled to their suspended (registered) state while the
    /// flag is still false, then `request()` is called once. Regression test
    /// for `notify_one`, which wakes only the first waiter and leaves the rest
    /// hanging forever.
    #[test]
    fn request_wakes_all_concurrent_waiters() {
        use std::{
            future::Future,
            pin::pin,
            task::{Context, Poll},
        };

        let shutdown = Shutdown::new();
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        let mut w1 = pin!(shutdown.requested_signal());
        let mut w2 = pin!(shutdown.requested_signal());

        // With the flag still false, both register on the notify and suspend.
        assert!(matches!(w1.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(w2.as_mut().poll(&mut cx), Poll::Pending));

        shutdown.request();

        assert!(
            matches!(w1.as_mut().poll(&mut cx), Poll::Ready(())),
            "the first parked waiter must be woken by request()"
        );
        assert!(
            matches!(w2.as_mut().poll(&mut cx), Poll::Ready(())),
            "every parked waiter must be woken by request(), not just the first"
        );
    }
}
