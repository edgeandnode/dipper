use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres, postgres::PgListener};

/// How many connection drops in a row before we stop waiting and report it.
///
/// sqlx rebuilds a dropped `LISTEN` connection itself and tells us by returning
/// an empty receive. One or two in a row is ordinary churn worth absorbing
/// quietly. A run of them means something is killing the connection as fast as
/// it can be opened (a pooler that will not hold `LISTEN`, say), and retrying
/// here would spin at whatever rate the network allows, with the caller none
/// the wiser. Handing that back instead lets the caller pace and log it.
const MAX_CONSECUTIVE_DROPS: u32 = 3;

/// How close together drops must be to count as the same run.
///
/// A long-lived connection reaped once an hour is healthy, and those drops
/// should never accumulate into a fault. Only drops arriving inside this window
/// of each other are treated as one continuing problem.
const DROP_WINDOW: Duration = Duration::from_secs(60);

/// Tracks how many times in a row the connection has dropped recently.
#[derive(Debug, Default)]
struct DropRun {
    count: u32,
    last: Option<Instant>,
}

impl DropRun {
    /// Counts a drop, starting a fresh run if the previous one was long enough
    /// ago, and returns the run's new length.
    fn record(&mut self, now: Instant) -> u32 {
        let continues_run = self
            .last
            .is_some_and(|last| now.duration_since(last) < DROP_WINDOW);
        self.count = if continues_run {
            self.count.saturating_add(1)
        } else {
            1
        };
        self.last = Some(now);
        self.count
    }

    /// Clears the run, once the connection has proved itself or the problem has
    /// been reported.
    fn clear(&mut self) {
        self.count = 0;
        self.last = None;
    }
}

/// A PostgreSQL queue listener that waits for job availability notifications.
///
/// Wraps a `sqlx::postgres::PgListener` subscribed to the "pgmq_jobs_available"
/// channel. sqlx reconnects and re-subscribes on its own when the connection
/// drops, so the job of this type is to notice when that keeps happening and
/// report it, rather than to retry forever.
pub struct PgQueueListener {
    listener: PgListener,
    /// Lives on the struct rather than in `wait_for_notification` because
    /// callers routinely race that future against a timer and drop it, which
    /// would discard a local counter before it ever reached the limit.
    drops: DropRun,
}

impl PgQueueListener {
    /// Creates a new PostgreSQL queue listener.
    ///
    /// Returns an error if the connection or the subscription fails.
    pub(crate) async fn new(pool: &Pool<Postgres>) -> anyhow::Result<Self> {
        Ok(Self {
            listener: connect_and_subscribe(pool).await?,
            drops: DropRun::default(),
        })
    }

    /// Waits for a new job availability notification.
    ///
    /// Blocks until a notification arrives. An error means either that the
    /// connection could not be restored, or that it was restored repeatedly
    /// without ever delivering anything, which is reported rather than retried
    /// in place because the caller owns re-subscription and can pace it.
    ///
    /// Safe to cancel and call again: the drop count lives on the struct, so a
    /// caller racing this against a timer neither loses nor double-counts.
    pub async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
        loop {
            // `Ok(None)` is sqlx reporting that it lost the connection and
            // rebuilt it, so no notification came through this time.
            if self.listener.try_recv().await?.is_some() {
                self.drops.clear();
                return Ok(());
            }

            let drops = self.drops.record(Instant::now());
            tracing::debug!(
                consecutive_drops = drops,
                "job-available notification connection dropped and was re-established"
            );

            if drops >= MAX_CONSECUTIVE_DROPS {
                self.drops.clear();
                anyhow::bail!(
                    "job-available notification connection dropped {drops} times in quick succession"
                );
            }
        }
    }
}

/// Connects to the PostgreSQL server and subscribes to the notification channel
async fn connect_and_subscribe(pool: &Pool<Postgres>) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("pgmq_jobs_available").await?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{DROP_WINDOW, DropRun, MAX_CONSECUTIVE_DROPS};

    #[test]
    fn drops_in_quick_succession_build_towards_the_limit() {
        let mut run = DropRun::default();
        let start = Instant::now();

        for i in 1..=MAX_CONSECUTIVE_DROPS {
            let count = run.record(start + Duration::from_millis(100) * i);
            assert_eq!(count, i);
        }
    }

    #[test]
    fn a_drop_after_a_quiet_spell_starts_a_fresh_run() {
        let mut run = DropRun::default();
        let start = Instant::now();

        run.record(start);
        assert_eq!(run.record(start + Duration::from_millis(10)), 2);

        assert_eq!(
            run.record(start + DROP_WINDOW + Duration::from_secs(1)),
            1,
            "a connection reaped occasionally is healthy, not a fault"
        );
    }

    #[test]
    fn a_delivered_notification_clears_the_run() {
        let mut run = DropRun::default();
        let start = Instant::now();

        run.record(start);
        run.record(start + Duration::from_millis(10));
        run.clear();

        assert_eq!(
            run.record(start + Duration::from_millis(20)),
            1,
            "a working connection should not carry old drops forward"
        );
    }
}
