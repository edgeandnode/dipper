//! PostgreSQL-backed message queue implementation.

use async_trait::async_trait;
use serde::Serialize;
use sqlx::{types::JsonValue, Pool, Postgres};
use time::OffsetDateTime;
use uuid::Uuid;

use super::queue::{Job, Queue};

/// The default maximum number of attempts before a job is considered failed.
const DEFAULT_MAX_ATTEMPTS: i32 = 3;

/// A PostgreSQL message queue.
#[derive(Debug, Clone)]
pub struct PgQueue {
    /// The DB connection pool.
    pool: Pool<Postgres>,

    /// The maximum number of attempts before a job is considered failed.
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

    /// Initializes the PostgreSQL message queue.
    ///
    /// This method creates the necessary tables and indexes in the database by running the
    /// migration SQL scripts.
    pub async fn init(&self) -> anyhow::Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
impl<M> Queue<M> for PgQueue
where
    M: Serialize + Send + 'static,
    Job<M>: TryFrom<PgJob>,
{
    async fn push(&self, job: M) -> anyhow::Result<()> {
        let message = serde_json::to_value(&job)?;
        let job = PgJob::new(message);

        sqlx::query!(
            r#"INSERT INTO pgmq_queue (
                id, 
                created_at, 
                updated_at, 
                scheduled_for, 
                status, 
                failed_attempts,
                message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            job.id,
            job.created_at,
            job.updated_at,
            job.scheduled_for,
            job.status as i32,
            job.failed_attempts,
            job.message,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn push_scheduled(&self, job: M, scheduled_for: OffsetDateTime) -> anyhow::Result<()> {
        let message = serde_json::to_value(&job)?;
        let job = PgJob::with_schedule(message, scheduled_for);

        sqlx::query!(
            r#"INSERT INTO pgmq_queue (
                id, 
                created_at, 
                updated_at, 
                scheduled_for, 
                status, 
                failed_attempts,
                message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
            job.id,
            job.created_at,
            job.updated_at,
            job.scheduled_for,
            job.status as i32,
            job.failed_attempts,
            job.message,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn pull(&self, number_of_jobs: usize) -> anyhow::Result<Vec<Job<M>>> {
        let number_of_jobs = number_of_jobs.min(100);
        let now = OffsetDateTime::now_utc();

        let pg_jobs = sqlx::query_as!(
            PgJob,
            r#"UPDATE pgmq_queue
            SET status = $1, updated_at = $2
            WHERE id IN (
                SELECT id
                FROM pgmq_queue
                WHERE status = $3 AND scheduled_for <= $4 AND failed_attempts < $5
                ORDER BY scheduled_for
                FOR UPDATE SKIP LOCKED
                LIMIT $6
            )
            RETURNING *"#,
            PgJobStatus::Running as i32,
            now,
            PgJobStatus::Queued as i32,
            now,
            self.max_attempts as i32,
            number_of_jobs as i64,
        )
        .fetch_all(&self.pool)
        .await?;

        // Deserialize the message JSON value into the message type
        // Ignore any messages that fail to deserialize
        let jobs = pg_jobs
            .into_iter()
            .filter_map(|pg_job| pg_job.try_into().ok())
            .collect::<Vec<_>>();
        Ok(jobs)
    }

    async fn remove(&self, id: Uuid) -> anyhow::Result<()> {
        sqlx::query!("DELETE FROM pgmq_queue WHERE id = $1", id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn fail_job(
        &self,
        id: Uuid,
        scheduled_for: Option<OffsetDateTime>,
    ) -> anyhow::Result<()> {
        let now = OffsetDateTime::now_utc();
        let scheduled_for = scheduled_for.unwrap_or(now);

        // Update the job status and increment the number of failed attempts
        // If the number of failed attempts is greater than the maximum number of attempts,
        // mark the job as failed and do not reschedule it
        // Otherwise, reschedule the job for the next execution date
        // TODO(post-mvp): Return the updated job status and failed attempts, so the caller can check if the
        //  job was marked as failed
        sqlx::query!(
            r#"UPDATE pgmq_queue
            SET 
                failed_attempts = failed_attempts + 1,
                status = (CASE
                    WHEN failed_attempts + 1 >= $1 THEN $2::int ELSE $3::int
                END),
                scheduled_for = (CASE
                    WHEN failed_attempts + 1 < $1 THEN $4 ELSE scheduled_for
                END),
                updated_at = $5
            WHERE id = $6"#,
            self.max_attempts as i32,
            PgJobStatus::Failed as i32,
            PgJobStatus::Queued as i32,
            scheduled_for,
            now,
            id,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn clear(&self) -> anyhow::Result<()> {
        sqlx::query!("DELETE FROM pgmq_queue")
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct PgJob {
    /// The job ID.
    id: Uuid,
    /// The job creation timestamp.
    created_at: OffsetDateTime,
    /// The job last update timestamp.
    updated_at: OffsetDateTime,
    /// The job scheduled execution date.
    scheduled_for: OffsetDateTime,

    /// The job status (queued, running, failed).
    status: PgJobStatus,
    /// The number of failed execution attempts.
    failed_attempts: i32,

    /// The job message (serialized).
    message: JsonValue,
}

impl PgJob {
    /// Creates a new job with the given message and scheduled execution date.
    fn new(message: JsonValue) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id: Uuid::now_v7(),
            created_at: now,
            updated_at: now,
            scheduled_for: now,
            status: Default::default(),
            failed_attempts: 0,
            message,
        }
    }

    /// Creates a new job with the given message and scheduled execution date.
    fn with_schedule(message: JsonValue, scheduled_for: OffsetDateTime) -> Self {
        let now = OffsetDateTime::now_utc();
        Self {
            id: Uuid::now_v7(),
            created_at: now,
            updated_at: now,
            scheduled_for,
            status: Default::default(),
            failed_attempts: 0,
            message,
        }
    }
}

impl<M> TryFrom<PgJob> for Job<M>
where
    M: for<'de> serde::Deserialize<'de>,
{
    type Error = serde_json::Error;

    fn try_from(value: PgJob) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            message: serde_json::from_value(value.message)?,
        })
    }
}

/// The job status.
///
/// We use a postgres `INT` representation as an optimization.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default, sqlx::Type)]
#[repr(i32)]
enum PgJobStatus {
    /// The job is queued.
    #[default]
    Queued = 0,

    /// The job is being executed by a worker.
    Running = -1,

    /// The job execution has failed.
    ///
    /// The job will be retried until the maximum number of attempts is reached. If the maximum
    /// number of attempts is reached, the job will be marked as failed.
    Failed = 1,
}

impl From<i32> for PgJobStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::Queued,
            -1 => Self::Running,
            1 => Self::Failed,
            _ => Self::Queued,
        }
    }
}
