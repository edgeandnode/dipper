use super::{
    id::JobId,
    postgres::{mark_as_failed, remove},
};

/// A job in the queue
#[derive(Debug, Clone)]
pub struct Job<M> {
    /// The job ID
    pub id: JobId,
    /// The job message
    pub message: M,
}

/// A guard for a job in the queue
///
/// This struct is used to ensure that the transaction is committed when the job is removed, marked
/// as failed, or rescheduled.
pub struct JobGuard<'c, M> {
    tx: sqlx::Transaction<'c, sqlx::Postgres>,
    job: Job<M>,
}

impl<'c, M> JobGuard<'c, M> {
    /// Creates a new job guard
    pub(crate) fn new(tx: sqlx::Transaction<'c, sqlx::Postgres>, job: Job<M>) -> Self {
        Self { tx, job }
    }

    /// The job ID
    pub fn id(&self) -> &JobId {
        &self.job.id
    }

    /// The job message
    pub fn message(&self) -> &M {
        &self.job.message
    }
}

impl<M> JobGuard<'_, M> {
    /// Remove the job from the queue
    pub async fn remove(mut self) -> anyhow::Result<()> {
        remove(self.tx.as_mut(), &self.job.id).await?;
        self.tx.commit().await?;
        Ok(())
    }

    /// Mark the job as failed
    pub async fn mark_as_failed(mut self) -> anyhow::Result<()> {
        mark_as_failed(self.tx.as_mut(), &self.job.id, None).await?;
        self.tx.commit().await?;
        Ok(())
    }

    /// Mark the job as failed and reschedule it
    pub async fn mark_as_failed_and_reschedule(
        mut self,
        schedule: time::OffsetDateTime,
    ) -> anyhow::Result<()> {
        mark_as_failed(self.tx.as_mut(), &self.job.id, Some(schedule)).await?;
        self.tx.commit().await?;
        Ok(())
    }
}
