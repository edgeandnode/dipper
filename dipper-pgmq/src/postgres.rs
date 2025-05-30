//! PostgreSQL message queue queries

use anyhow::Context as _;
use sqlx::{
    Acquire, Postgres,
    migrate::{Migrate, MigrateError},
    types::JsonValue,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{
    id::JobId,
    job::{Job, JobGuard},
};

/// Run the DB migrations.
///
/// It is used to ensure that the database is up to date with the latest migrations.
pub async fn run_db_migrations<'a, A>(conn: A) -> Result<(), MigrateError>
where
    A: Acquire<'a>,
    <A::Connection as std::ops::Deref>::Target: Migrate,
{
    sqlx::migrate!("./migrations").run(conn).await?;
    Ok(())
}

/// Push a job into the queue
pub async fn push<'q, E, M>(executor: E, job: M, max_attempts: i32) -> anyhow::Result<JobId>
where
    E: sqlx::Executor<'q, Database = sqlx::Postgres>,
    M: serde::Serialize,
{
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
        max_attempts,
        message,
    )
    .execute(executor)
    .await?;

    Ok(JobId::from_uuid(id))
}

/// Push a scheduled job into the queue
pub async fn push_scheduled<'q, E, M>(
    executor: E,
    job: M,
    scheduled_for: OffsetDateTime,
    max_attempts: i32,
) -> anyhow::Result<JobId>
where
    E: sqlx::Executor<'q, Database = sqlx::Postgres>,
    M: serde::Serialize,
{
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
                $2, $3, $4, 0::int, $5
            )"#,
        id,
        scheduled_for,
        PgJobStatus::default() as i32,
        max_attempts,
        message,
    )
    .execute(executor)
    .await?;

    Ok(JobId::from_uuid(id))
}

/// Pop a job from the queue
pub async fn pop<'q, E, M>(executor: E) -> anyhow::Result<Option<JobGuard<'q, M>>>
where
    E: sqlx::Acquire<'q, Database = sqlx::Postgres>,
    M: serde::de::DeserializeOwned + Send + 'static,
    Job<M>: TryFrom<PgJob>,
{
    let mut tx = executor.begin().await?;

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

/// Clear the queue
pub async fn clear<'q, E>(executor: E) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = sqlx::Postgres>,
{
    sqlx::query!("DELETE FROM pgmq_queue")
        .execute(executor)
        .await?;
    Ok(())
}

/// Remove a job from the queue
pub async fn remove<'q, E>(executor: E, id: &JobId) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = sqlx::Postgres>,
{
    sqlx::query!("DELETE FROM pgmq_queue WHERE id = $1", id.as_ref())
        .execute(executor)
        .await?;

    Ok(())
}

/// Mark a job as failed
// TODO(post-mvp): Return the updated job status and failed attempts, so the caller can check if the
//  job was marked as failed
pub async fn mark_as_failed<'q, E>(
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

#[derive(sqlx::FromRow)]
pub struct PgJob {
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
