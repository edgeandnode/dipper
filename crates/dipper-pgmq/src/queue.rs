use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

pub mod postgres;

/// A job in the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job<M> {
    pub id: Uuid,
    pub message: M,
}

/// A message queue.
///
/// This trait is used to interact with the message queue.
#[async_trait]
pub trait Queue<M>: Send + Sync + 'static {
    /// Pushes a job to the queue.
    ///
    /// The job can be scheduled for a specific date.
    async fn push(&self, job: M, scheduled_for: Option<OffsetDateTime>) -> anyhow::Result<()>;

    /// Pulls a job from the queue.
    ///
    /// The number of jobs to pull is a hint to the queue, but the queue may return fewer jobs.
    async fn pull(&self, number_of_jobs: usize) -> anyhow::Result<Vec<Job<M>>>;

    /// Remove a job from the queue by its ID.
    async fn remove(&self, id: Uuid) -> anyhow::Result<()>;

    /// Mark a job as failed.
    ///
    /// This will increment the number of attempts and set the job as failed.
    ///
    /// If `scheduled_for` is provided, the job will be rescheduled for that date.
    async fn fail_job(&self, id: Uuid, scheduled_for: Option<OffsetDateTime>)
        -> anyhow::Result<()>;

    /// Clear the queue.
    ///
    /// This will remove all jobs from the queue.
    async fn clear(&self) -> anyhow::Result<()>;
}
