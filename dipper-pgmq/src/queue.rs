use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// A message queue.
///
/// This trait is used to interact with the message queue.
#[async_trait]
pub trait Queue<M>: Send + Sync + 'static {
    /// Pushes a message to the queue for immediate processing
    async fn push(&self, msg: M) -> anyhow::Result<JobId>;

    /// Pushes a message to the queue to be scheduled for later
    ///
    /// If `OffsetDateTime` is in the past, the job will be executed immediately.
    async fn push_scheduled(&self, msg: M, scheduled_for: OffsetDateTime) -> anyhow::Result<JobId>;

    /// Pulls a job from the queue
    ///
    /// The number of jobs to pull is a hint to the queue, but the queue may return fewer jobs.
    ///
    /// The jobs pulled from the queue are not removed until `remove` is called, they remain in
    /// "RUNNING" state. They cannot be pulled again unless they are marked as failed.
    async fn pull(&self, jobs: usize) -> anyhow::Result<Vec<Job<M>>>;

    /// Remove a job from the queue by its ID
    async fn remove(&self, id: JobId) -> anyhow::Result<()>;

    /// Mark a job as failed
    ///
    /// This will increment the number of attempts and set the job as failed.
    ///
    /// If `scheduled_for` is provided, the job will be rescheduled for that date.
    async fn mark_job_as_failed(
        &self,
        id: JobId,
        scheduled_for: Option<OffsetDateTime>,
    ) -> anyhow::Result<()>;

    /// Clear the queue.
    ///
    /// This will remove all jobs from the queue.
    async fn clear(&self) -> anyhow::Result<()>;
}

/// A job in the queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job<M> {
    pub id: JobId,
    pub message: M,
}

/// A job ID
///
/// This is a unique identifier for a job in the queue.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Create a new `JobId` from a `Uuid`.
    ///
    /// This is for internal use only.
    pub(crate) fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl AsRef<Uuid> for JobId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}
