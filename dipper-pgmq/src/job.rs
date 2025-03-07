use super::{
    id::JobId,
    postgres::{mark_as_failed, remove},
};

/// A job in the queue
#[derive(Debug, Clone)]
pub struct Job<M> {
    pub id: JobId,
    pub message: M,
}

pub struct JobGuard<'a, M> {
    tx: sqlx::Transaction<'a, sqlx::Postgres>,
    job: Job<M>,
}

impl<'a, M> JobGuard<'a, M> {
    pub(crate) fn new(tx: sqlx::Transaction<'a, sqlx::Postgres>, job: Job<M>) -> Self {
        Self { tx, job }
    }

    pub fn id(&self) -> &JobId {
        &self.job.id
    }

    pub fn message(&self) -> &M {
        &self.job.message
    }
}

impl<M> JobGuard<'_, M> {
    pub async fn remove(mut self) -> anyhow::Result<()> {
        remove(self.tx.as_mut(), &self.job.id).await?;
        self.tx.commit().await?;
        Ok(())
    }

    pub async fn mark_as_failed(mut self) -> anyhow::Result<()> {
        mark_as_failed(self.tx.as_mut(), &self.job.id, None).await?;
        self.tx.commit().await?;
        Ok(())
    }

    pub async fn mark_as_failed_and_reschedule(
        mut self,
        schedule: time::OffsetDateTime,
    ) -> anyhow::Result<()> {
        mark_as_failed(self.tx.as_mut(), &self.job.id, Some(schedule)).await?;
        self.tx.commit().await?;
        Ok(())
    }
}
