use async_trait::async_trait;
use time::OffsetDateTime;

use crate::{JobId, job::JobGuard};

/// A message queue.
///
/// This trait is used to interact with the message queue.
#[async_trait]
pub trait Queue<M>: Send + Sync + 'static
where
    M: serde::Serialize + Send,
{
    /// Pushes a message to the queue for immediate processing
    async fn push(&self, msg: M) -> anyhow::Result<JobId>;

    /// Pushes a message to the queue to be scheduled for later
    ///
    /// If `OffsetDateTime` is in the past, the job will be executed immediately.
    async fn push_scheduled(&self, msg: M, scheduled_for: OffsetDateTime) -> anyhow::Result<JobId>;

    /// Pulls a job from the queue
    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>>;

    /// Clear the queue.
    ///
    /// This will remove all jobs from the queue.
    async fn clear(&self) -> anyhow::Result<()>;
}
