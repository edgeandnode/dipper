use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres, postgres::PgListener};

/// The channel the queue notifies when a job becomes available.
const NOTIFICATION_CHANNEL: &str = "pgmq_jobs_available";

/// How many reconnects in a row, inside [`FAILURE_WINDOW`], before we give up
/// and report it. One or two is ordinary churn; a run of them means the
/// connection cannot be opened, or is killed as fast as it is opened.
const MAX_CONSECUTIVE_FAILURES: u32 = 3;

/// How close together failures must be to count as the same run. A long-lived
/// connection reaped once an hour is healthy, so only failures inside this
/// window of each other are treated as one continuing problem.
const FAILURE_WINDOW: Duration = Duration::from_secs(60);

/// Deadline for opening a connection and issuing its `LISTEN`. The pool bounds
/// its own checkout at 30 seconds, but the `LISTEN` round-trip after it is
/// unbounded, and a blackholed socket takes roughly 15 minutes to fail.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// Tracks how many times in a row the listener has recently had to reconnect.
#[derive(Debug, Default)]
struct FailureRun {
    count: u32,
    last: Option<Instant>,
}

impl FailureRun {
    /// Counts a failure, starting a fresh run if the previous one was long
    /// enough ago, and returns the run's new length.
    fn record(&mut self, now: Instant) -> u32 {
        let continues_run = self
            .last
            .is_some_and(|last| now.duration_since(last) < FAILURE_WINDOW);
        self.count = if continues_run {
            self.count.saturating_add(1)
        } else {
            1
        };
        self.last = Some(now);
        self.count
    }

    /// Clears the run, once the connection has proved itself by delivering a
    /// notification, or the problem has been reported.
    fn clear(&mut self) {
        self.count = 0;
        self.last = None;
    }
}

/// A PostgreSQL queue listener that waits for job availability notifications.
/// Reconnection is handled here rather than left to sqlx, so every attempt is
/// bounded, counted, and survives the caller cancelling the wait.
pub struct PgQueueListener {
    /// Kept so a lost connection can be rebuilt without the caller's help.
    pool: Pool<Postgres>,
    /// `None` once the connection is known lost, until it is rebuilt.
    listener: Option<PgListener>,
    /// Lives on the struct rather than in `wait_for_notification` because
    /// callers routinely race that future against a timer and drop it, which
    /// would discard a local counter before it ever reached the limit.
    failures: FailureRun,
}

impl PgQueueListener {
    /// Creates a new PostgreSQL queue listener. Returns an error if connecting
    /// or subscribing fails, or takes longer than [`SUBSCRIBE_TIMEOUT`].
    pub(crate) async fn new(pool: &Pool<Postgres>) -> anyhow::Result<Self> {
        let listener = tokio::time::timeout(SUBSCRIBE_TIMEOUT, subscribe(pool))
            .await
            .map_err(|_| anyhow::anyhow!("subscribing took longer than {SUBSCRIBE_TIMEOUT:?}"))??;
        Ok(Self {
            pool: pool.clone(),
            listener: Some(listener),
            failures: FailureRun::default(),
        })
    }

    /// Waits for a job availability notification, reconnecting as needed. Errors
    /// once the connection has needed 3 reconnects within 60 seconds of each
    /// other. Anything sent while it was down is lost, so callers must poll too.
    pub async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
        loop {
            if self.listener.is_none() {
                // Recorded before the reconnect it describes, so a caller that
                // races this against a timer and cancels it still leaves the
                // evidence behind rather than retrying from zero forever.
                let failures = self.failures.record(Instant::now());
                if failures >= MAX_CONSECUTIVE_FAILURES {
                    self.failures.clear();
                    anyhow::bail!(
                        "job-available notification connection needed {failures} reconnects within {} seconds",
                        FAILURE_WINDOW.as_secs()
                    );
                }

                match tokio::time::timeout(SUBSCRIBE_TIMEOUT, subscribe(&self.pool)).await {
                    Ok(Ok(listener)) => self.listener = Some(listener),
                    Ok(Err(err)) => {
                        tracing::debug!(
                            consecutive_failures = failures,
                            error = ?err,
                            "could not re-open the job-available notification connection"
                        );
                        continue;
                    }
                    Err(_elapsed) => {
                        tracing::debug!(
                            consecutive_failures = failures,
                            "re-opening the job-available notification connection timed out"
                        );
                        continue;
                    }
                }
            }

            let Some(listener) = self.listener.as_mut() else {
                continue;
            };
            match listener.try_recv().await {
                Ok(Some(_notification)) => {
                    self.failures.clear();
                    return Ok(());
                }
                // sqlx reporting the connection died. With eager reconnection
                // off it returns without awaiting, so the loss is recorded
                // above before any cancellable work runs.
                Ok(None) => self.listener = None,
                Err(err) => {
                    self.listener = None;
                    return Err(err.into());
                }
            }
        }
    }
}

/// Connects to the PostgreSQL server and subscribes to the notification channel
async fn subscribe(pool: &Pool<Postgres>) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    // Left on, sqlx rebuilds the connection inside `try_recv` before telling us
    // it broke. A caller cancelling the wait on a timer throws that work away
    // every time, so the loss goes unreported and the path stays quietly dead.
    listener.eager_reconnect(false);
    listener.listen(NOTIFICATION_CHANNEL).await?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{FAILURE_WINDOW, FailureRun, MAX_CONSECUTIVE_FAILURES};

    #[test]
    fn failures_in_quick_succession_build_towards_the_limit() {
        let mut run = FailureRun::default();
        let start = Instant::now();

        for i in 1..=MAX_CONSECUTIVE_FAILURES {
            let count = run.record(start + Duration::from_millis(100) * i);
            assert_eq!(count, i);
        }
    }

    #[test]
    fn a_failure_after_a_quiet_spell_starts_a_fresh_run() {
        let mut run = FailureRun::default();
        let start = Instant::now();

        run.record(start);
        assert_eq!(run.record(start + Duration::from_millis(10)), 2);

        assert_eq!(
            run.record(start + FAILURE_WINDOW + Duration::from_secs(1)),
            1,
            "a connection reaped occasionally is healthy, not a fault"
        );
    }

    #[test]
    fn a_delivered_notification_clears_the_run() {
        let mut run = FailureRun::default();
        let start = Instant::now();

        run.record(start);
        run.record(start + Duration::from_millis(10));
        run.clear();

        assert_eq!(
            run.record(start + Duration::from_millis(20)),
            1,
            "a working connection should not carry old failures forward"
        );
    }
}
