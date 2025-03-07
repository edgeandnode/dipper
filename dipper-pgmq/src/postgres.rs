//! PostgreSQL-backed message queue implementation.

use anyhow::Context;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, types::JsonValue};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    id::JobId,
    job::{Job, JobGuard},
    queue::Queue,
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

#[async_trait]
impl<M> Queue<M> for PgQueue
where
    M: serde::Serialize + Send + 'static,
    Job<M>: TryFrom<PgJob>,
{
    async fn push(&self, job: M) -> anyhow::Result<JobId> {
        let id = Uuid::now_v7();
        let message = serde_json::to_value(&job)?;

        sqlx::query!(
            r#"INSERT INTO pgmq_queue (
                id,
                created_at,
                updated_at,
                scheduled_for,
                status,
                retry_max,
                retry_count,
                message
            ) VALUES (
                $1, timezone('UTC', now()), timezone('UTC', now()),
                timezone('UTC', now()), $2, $3, 0::int, $4
            )"#,
            id,
            PgJobStatus::default() as i32,
            self.max_attempts,
            message,
        )
        .execute(&self.pool)
        .await?;

        Ok(JobId::from_uuid(id))
    }

    async fn push_scheduled(&self, msg: M, scheduled_for: OffsetDateTime) -> anyhow::Result<JobId> {
        let id = Uuid::now_v7();
        let message = serde_json::to_value(&msg)?;

        sqlx::query!(
            r#"INSERT INTO pgmq_queue (
                id,
                created_at,
                updated_at,
                scheduled_for,
                status,
                retry_max,
                retry_count,
                message
            ) VALUES (
                $1, timezone('UTC', now()), timezone('UTC', now()),
                $2, $3, $4, 0::int, $5
            )"#,
            id,
            scheduled_for,
            PgJobStatus::default() as i32,
            self.max_attempts,
            message,
        )
        .execute(&self.pool)
        .await?;

        Ok(JobId::from_uuid(id))
    }

    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>> {
        let mut tx = self.pool.begin().await?;

        // Get one job from the queue
        let res = sqlx::query_as!(
            PgJob,
            r#"UPDATE pgmq_queue
            SET
                updated_at = timezone('UTC', now()),
                status = $1
            WHERE id IN (
                SELECT id
                FROM pgmq_queue
                WHERE status = $2 AND scheduled_for <= timezone('UTC', now())
                ORDER BY scheduled_for
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *"#,
            PgJobStatus::Running as i32,
            PgJobStatus::Queued as i32,
        )
        .fetch_optional(&mut *tx)
        .await?
        .and_then(|pg_job| pg_job.try_into().ok())
        .map(|job| JobGuard::new(tx, job));

        Ok(res)
    }

    async fn clear(&self) -> anyhow::Result<()> {
        sqlx::query!("DELETE FROM pgmq_queue")
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

pub(crate) async fn remove<'q, E>(executor: E, id: &JobId) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = sqlx::Postgres>,
{
    sqlx::query!("DELETE FROM pgmq_queue WHERE id = $1", id.as_ref())
        .execute(executor)
        .await?;

    Ok(())
}

// TODO(post-mvp): Return the updated job status and failed attempts, so the caller can check if the
//  job was marked as failed
pub(crate) async fn mark_as_failed<'q, E>(
    executor: E,
    id: &JobId,
    scheduled_for: Option<OffsetDateTime>,
) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    let scheduled_for = scheduled_for.unwrap_or_else(OffsetDateTime::now_utc);

    // Update the job status and increment the number of failed attempts
    // If the number of failed attempts is greater than the maximum number of attempts,
    // mark the job as failed and do not reschedule it
    // Otherwise, reschedule the job for the next execution date
    sqlx::query!(
        r#"UPDATE pgmq_queue
           SET
               updated_at = timezone('UTC', now()),
               retry_count = retry_count + 1,
               status = (CASE
                   WHEN retry_count + 1 >= retry_max THEN $2::int ELSE $3::int
               END),
               scheduled_for = (CASE
                   WHEN retry_count + 1 < retry_max THEN $4 ELSE scheduled_for
               END)
           WHERE id = $1"#,
        id.as_ref(),
        PgJobStatus::Failed as i32,
        PgJobStatus::Queued as i32,
        scheduled_for,
    )
    .execute(executor)
    .await?;

    Ok(())
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
    /// The maximum number of execution attempts.
    retry_max: i32,
    /// The number of execution attempts.
    retry_count: i32,

    /// The job message (serialized).
    message: JsonValue,
}

impl<M> TryFrom<PgJob> for Job<M>
where
    M: for<'de> serde::Deserialize<'de>,
{
    type Error = anyhow::Error;

    fn try_from(value: PgJob) -> Result<Self, Self::Error> {
        Ok(Self {
            id: JobId::from_uuid(value.id),
            message: serde_json::from_value(value.message)
                .context("Failed job JSON message deserialization")?,
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
            i32::MIN..=-1_i32 => Self::Running,
            0 => Self::Queued,
            1_i32..=i32::MAX => Self::Failed,
        }
    }
}
