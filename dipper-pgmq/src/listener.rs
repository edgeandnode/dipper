use sqlx::{Pool, Postgres, postgres::PgListener};

/// A PostgreSQL queue listener that waits for job availability notifications.
///
/// This struct wraps a sqlx::postgres::PgListener and provides a simple async method
/// to wait for job availability notifications on the "pgmq_jobs_available" channel.
/// It handles reconnections gracefully by re-subscribing once on dropped connections.
pub struct PgQueueListener {
    /// The DB connection pool for reconnections
    pool: Pool<Postgres>,
    /// The underlying PostgreSQL listener
    listener: PgListener,
}

impl PgQueueListener {
    /// Creates a new PostgreSQL queue listener.
    ///
    /// Returns `Ok(PgQueueListener)` on success, or a `sqlx::Error` if the connection or
    /// subscription fails.
    pub(crate) async fn new(pool: Pool<Postgres>) -> anyhow::Result<Self> {
        let listener = connect_and_subscribe(&pool).await?;
        Ok(Self { pool, listener })
    }

    /// Waits for a new job availability notification.
    ///
    /// This method blocks until a notification is received on the "pgmq_jobs_available" channel.
    /// If the connection is dropped, it will attempt to reconnect and re-subscribe once.
    ///
    /// Returns `Ok(())` when a notification is received, or a `sqlx::Error` if the
    /// operation fails after attempting reconnection.
    pub async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
        loop {
            match self.try_recv_notification().await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    // Only retry on connection-related errors
                    if !self.should_retry_on_error(&err) {
                        return Err(err.into());
                    }

                    // Attempt to reconnect and continue the loop to retry
                    self.listener = reconnect(&self.pool).await?;
                }
            }
        }
    }

    /// Attempts to receive a notification from the listener.
    async fn try_recv_notification(&mut self) -> Result<(), sqlx::Error> {
        // Wait for any notification on the subscribed channel
        let _notification = self.listener.recv().await?;
        Ok(())
    }

    /// Determines if an error warrants a retry with reconnection.
    fn should_retry_on_error(&self, error: &sqlx::Error) -> bool {
        match error {
            // Database connection errors that might be recovered by reconnecting
            sqlx::Error::Database(_) => false, // Database errors are usually not connection issues
            sqlx::Error::Io(_) => true,        // IO errors often indicate connection problems
            sqlx::Error::Tls(_) => true,       // TLS errors might be connection-related
            sqlx::Error::Protocol(_) => true,  // Protocol errors might indicate connection issues
            sqlx::Error::PoolTimedOut => false, // Pool timeout is not a connection error
            sqlx::Error::PoolClosed => true,   // Pool closed indicates connection problem
            sqlx::Error::WorkerCrashed => true, // Worker crashed might need reconnection
            _ => false,                        // For other errors, don't retry
        }
    }
}

/// Connects to the PostgreSQL server and subscribes to the notification channel
async fn connect_and_subscribe(pool: &Pool<Postgres>) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen("pgmq_jobs_available").await?;
    Ok(listener)
}

/// Attempts to reconnect to the PostgreSQL server and re-subscribe to the notification channel
async fn reconnect(pool: &Pool<Postgres>) -> Result<PgListener, sqlx::Error> {
    connect_and_subscribe(pool).await
}
