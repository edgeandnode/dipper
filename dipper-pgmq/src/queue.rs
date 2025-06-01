use sqlx::{Pool, Postgres};
use time::OffsetDateTime;

use super::{
    id::JobId,
    job::{JobGuard, JobInner},
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
    pub async fn push<J, T>(&self, job: J) -> anyhow::Result<JobId>
    where
        J: Into<JobBuilder<T>>,
        T: serde::Serialize,
    {
        let JobBuilder {
            desc,
            max_attempts,
            scheduled_for,
        } = job.into();

        if let Some(scheduled_for) = scheduled_for {
            postgres::push_scheduled(
                &self.pool,
                desc,
                max_attempts.unwrap_or(self.max_attempts),
                scheduled_for,
            )
            .await
        } else {
            postgres::push(&self.pool, desc, max_attempts.unwrap_or(self.max_attempts)).await
        }
    }

    /// Pulls a job from the queue
    pub async fn pop<T>(&self) -> anyhow::Result<Option<JobGuard<'_, T>>>
    where
        T: for<'de> serde::Deserialize<'de> + Send + Unpin + 'static,
    {
        let mut tx = self.pool.begin().await?;

        let res = postgres::pop(tx.as_mut()).await?.map(|job| {
            let job = JobInner {
                id: job.id,
                created_at: job.created_at,
                updated_at: job.updated_at,
                desc: job.descriptor.0,
                max_attempts: job.max_attempts as u32,
                failed_attempts: job.attempt_count as u32,
            };
            JobGuard::new(tx, job)
        });
        Ok(res)
    }

    /// Clears the queue
    pub async fn clear(&self) -> anyhow::Result<()> {
        postgres::clear(&self.pool).await
    }
}

pub struct JobBuilder<T> {
    /// The job descriptor
    desc: T,
    /// The maximum number of attempts before a job is considered failed
    max_attempts: Option<i32>,
    /// The scheduled time for the job
    scheduled_for: Option<OffsetDateTime>,
}

impl<T> JobBuilder<T> {
    /// Creates a new job input
    pub fn new(desc: T) -> Self {
        Self {
            desc,
            max_attempts: None,
            scheduled_for: None,
        }
    }

    /// Sets the maximum number of attempts before a job is considered failed
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts.try_into().unwrap_or(i32::MAX));
        self
    }

    /// Sets the scheduled time for the job
    pub fn schedule_at(mut self, schedule: OffsetDateTime) -> Self {
        self.scheduled_for = Some(schedule);
        self
    }
}
impl<T> From<T> for JobBuilder<T>
where
    T: serde::Serialize,
{
    fn from(desc: T) -> Self {
        Self::new(desc)
    }
}
