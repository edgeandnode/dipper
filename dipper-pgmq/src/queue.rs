use sqlx::{Pool, Postgres};
use time::OffsetDateTime;

use super::{
    id::JobId,
    job::{Job, JobGuard},
    postgres,
};

/// The default maximum number of attempts before a job is considered as failed.
const DEFAULT_MAX_ATTEMPTS: i32 = 3;

/// A PostgreSQL message queue
#[derive(Debug, Clone)]
pub struct PgQueue {
    /// The DB connection pool.
    pool: Pool<Postgres>,
    /// The maximum number of attempts before a job is considered failed
    max_attempts: i32,
}

impl PgQueue {
    /// Creates a new PostgreSQL message queue.
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self {
            pool,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }

    /// Creates a new PostgreSQL message queue with a custom maximum number of attempts.
    pub fn with_max_attempts(pool: Pool<Postgres>, max_attempts: u32) -> Self {
        Self {
            pool,
            max_attempts: max_attempts.try_into().unwrap_or(i32::MAX),
        }
    }
}

impl PgQueue {
    /// Pushes a job into the queue
    pub async fn push<M>(&self, job: M) -> anyhow::Result<JobId>
    where
        M: serde::Serialize,
    {
        postgres::push(&self.pool, job, self.max_attempts).await
    }

    /// Pushes a job into the queue with a scheduled time
    pub async fn push_scheduled<M>(
        &self,
        msg: M,
        scheduled_for: OffsetDateTime,
    ) -> anyhow::Result<JobId>
    where
        M: serde::Serialize,
    {
        postgres::push_scheduled(&self.pool, msg, scheduled_for, self.max_attempts).await
    }

    /// Pulls a job from the queue
    pub async fn pop<M>(&self) -> anyhow::Result<Option<JobGuard<'_, M>>>
    where
        M: serde::de::DeserializeOwned + Send + 'static,
        Job<M>: TryFrom<postgres::PgJob>,
    {
        postgres::pop(&self.pool).await
    }

    /// Clears the queue
    pub async fn clear(&self) -> anyhow::Result<()> {
        postgres::clear(&self.pool).await
    }
}
