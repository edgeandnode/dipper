//! PostgreSQL message queue queries

use sqlx::{
    Postgres,
    migrate::{Migrate, MigrateError},
    types::Json,
};
use time::OffsetDateTime;
use uuid::Uuid;

use super::id::JobId;

/// Run the DB migrations.
///
/// It is used to ensure that the database is up to date with the latest migrations.
pub async fn run_db_migrations<'a, A>(conn: A) -> Result<(), MigrateError>
where
    A: sqlx::Acquire<'a>,
    <A::Connection as std::ops::Deref>::Target: Migrate,
{
    sqlx::migrate!("./migrations").run(conn).await?;
    Ok(())
}

/// Push a job into the queue
pub async fn push<'q, E, T>(executor: E, desc: T, max_attempts: i32) -> anyhow::Result<JobId>
where
    E: sqlx::Executor<'q, Database = Postgres>,
    T: serde::Serialize,
{
    let id = Uuid::now_v7();

    sqlx::query(
        r#"INSERT INTO pgmq_queue (
                id,
                status,
                max_attempts,
                descriptor
            ) VALUES (
                $1, $2, $3, $4
            )"#,
    )
    .bind(id)
    .bind(PgJobStatus::default())
    .bind(max_attempts)
    .bind(Json(desc))
    .execute(executor)
    .await?;

    Ok(JobId::from_uuid(id))
}

/// Push a scheduled job into the queue
pub async fn push_scheduled<'q, E, T>(
    executor: E,
    desc: T,
    max_attempts: i32,
    scheduled_for: OffsetDateTime,
) -> anyhow::Result<JobId>
where
    E: sqlx::Executor<'q, Database = Postgres>,
    T: serde::Serialize,
{
    let id = JobId::from_uuid(Uuid::now_v7());
    sqlx::query(
        r#"INSERT INTO pgmq_queue (
                id,
                scheduled_for,
                status,
                max_attempts,
                descriptor
            ) VALUES (
                $1, $2, $3, $4, $5
            )"#,
    )
    .bind(id)
    .bind(scheduled_for)
    .bind(PgJobStatus::default())
    .bind(max_attempts)
    .bind(Json(desc))
    .execute(executor)
    .await?;

    Ok(id)
}

/// Pop a job from the queue
pub async fn pop<'q, E, T>(executor: E) -> anyhow::Result<Option<PgJob<T>>>
where
    E: sqlx::Executor<'q, Database = Postgres>,
    T: for<'de> serde::Deserialize<'de> + Send + Unpin + 'static,
{
    let res = sqlx::query_as(
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
    )
    .bind(PgJobStatus::Running)
    .bind(PgJobStatus::Queued)
    .fetch_optional(executor)
    .await?;
    Ok(res)
}

/// Clear the queue
pub async fn clear<'q, E>(executor: E) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    sqlx::query("DELETE FROM pgmq_queue")
        .execute(executor)
        .await?;
    Ok(())
}

/// Remove a job from the queue
pub async fn remove<'q, E>(executor: E, id: &JobId) -> anyhow::Result<()>
where
    E: sqlx::Executor<'q, Database = Postgres>,
{
    sqlx::query("DELETE FROM pgmq_queue WHERE id = $1")
        .bind(id)
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
    // If the number of failed attempts is greater than or equal to the maximum number of attempts,
    // mark the job as failed and do not reschedule it
    // Otherwise, reschedule the job for the next execution date
    sqlx::query(
        r#"UPDATE pgmq_queue
           SET
               updated_at = timezone('UTC', now()),
               attempt_count = attempt_count + 1,
               status = (CASE
                   WHEN attempt_count + 1 >= max_attempts THEN $2 ELSE $3
               END),
               scheduled_for = (CASE
                   WHEN attempt_count + 1 < max_attempts THEN $4 ELSE scheduled_for
               END)
           WHERE id = $1"#,
    )
    .bind(id)
    .bind(PgJobStatus::Failed)
    .bind(PgJobStatus::Queued)
    .bind(scheduled_for)
    .execute(executor)
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
pub struct PgJob<T> {
    /// The job ID.
    pub(crate) id: JobId,
    /// The job creation timestamp.
    pub(crate) created_at: OffsetDateTime,
    /// The job last update timestamp.
    pub(crate) updated_at: OffsetDateTime,

    /// The maximum number of execution attempts.
    pub(crate) max_attempts: i32,
    /// The number of execution attempts made so far.
    pub(crate) attempt_count: i32,

    /// The job descriptor (serialized).
    pub(crate) descriptor: Json<T>,
}

/// The job status.
///
/// We use a postgres `INT` representation as an optimization.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
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

impl sqlx::Type<Postgres> for PgJobStatus {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("INT")
    }
}

impl sqlx::Encode<'_, Postgres> for PgJobStatus {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        sqlx::Encode::<Postgres>::encode_by_ref(&(*self as i32), buf)
    }
}

impl sqlx::Decode<'_, Postgres> for PgJobStatus {
    fn decode(
        value: <Postgres as sqlx::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: i32 = sqlx::Decode::<Postgres>::decode(value)?;
        Ok(value.into())
    }
}
