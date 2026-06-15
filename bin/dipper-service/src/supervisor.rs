//! Process supervision for the task tree.
//!
//! Every long-running service is spawned into a single [`JoinSet`]. In normal
//! operation none of them ever completes on its own — they run until the
//! shutdown sequence stops them. So a task that finishes *before* shutdown was
//! deliberately requested is, by definition, an unexpected critical-task exit
//! (a panic, or a loop returning `Err` for an unrecoverable reason).
//!
//! Leaving the process running in that state — other services up, the daemon
//! looking healthy, but a critical task dead — is exactly the silent-stall
//! failure mode we refuse to allow. [`supervise`] therefore treats any such
//! exit as fatal: it requests shutdown so the rest of the tree is torn down
//! cleanly, then returns an error so the process exits non-zero and the
//! orchestrator restarts it.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
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
        self.trigger.notify_one();
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
/// tears the rest of the tree down), and keeps draining. Returns `Err` if any
/// unexpected exit occurred, otherwise `Ok(())` (a deliberate shutdown).
pub async fn supervise(
    mut task_tree: JoinSet<anyhow::Result<()>>,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    let mut unexpected_exit = false;

    while let Some(res) = task_tree.join_next_with_id().await {
        match res {
            Ok((id, Ok(()))) => {
                tracing::debug!(task_id = %id, "task completed");
            }
            Ok((id, Err(err))) => {
                tracing::error!(task_id = %id, error = ?err, "task failed");
            }
            Err(err) => {
                tracing::error!(task_id = %err.id(), error = ?err, "task join error");
            }
        }

        if !shutdown.is_requested() {
            tracing::error!(
                "a critical task exited unexpectedly; initiating shutdown so the process \
                 restarts rather than running on with a dead task"
            );
            unexpected_exit = true;
            shutdown.request();
        }
    }

    if unexpected_exit {
        anyhow::bail!("a critical task exited unexpectedly");
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

        let result = tokio::time::timeout(SUPERVISE_TIMEOUT, supervise(task_tree, &shutdown))
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

        let result = tokio::time::timeout(SUPERVISE_TIMEOUT, supervise(task_tree, &shutdown))
            .await
            .expect("supervise did not drain after a deliberate shutdown");

        assert!(
            result.is_ok(),
            "a deliberate shutdown must not be reported as fatal: {result:?}"
        );
    }
}
