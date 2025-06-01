use async_trait::async_trait;
use dipper_pgmq::PgQueue;
pub use dipper_pgmq::{JobGuard, JobId};
use sqlx::PgPool;

/// A message queue.
///
/// This trait is used to interact with the message queue.
#[async_trait]
pub trait Queue<M>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Pushes a message to the queue for immediate processing
    async fn push(&self, msg: M) -> anyhow::Result<JobId>;

    /// Pulls a job from the queue
    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>>;
}

#[derive(Clone)]
pub struct QueueImpl {
    inner: PgQueue,
}

impl QueueImpl {
    pub fn new(db_conn: PgPool) -> Self {
        Self {
            inner: PgQueue::with_max_attempts(db_conn, 3),
        }
    }
}

#[async_trait]
impl<M> Queue<M> for QueueImpl
where
    M: serde::Serialize + serde::de::DeserializeOwned + Send + Unpin + 'static,
{
    async fn push(&self, msg: M) -> anyhow::Result<JobId> {
        self.inner.push(msg).await
    }

    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>> {
        self.inner.pop().await
    }
}
