use async_trait::async_trait;
pub use dipper_pgmq::{JobBuilder, JobGuard, JobId, JobPriority, PgQueue, PgQueueListener};
use sqlx::PgPool;

/// A message queue.
///
/// This trait is used to interact with the message queue.
#[async_trait]
pub trait Queue<M>
where
    M: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Pushes a message to the queue for immediate processing at `priority`.
    async fn push(&self, msg: M, priority: JobPriority) -> anyhow::Result<JobId>;

    /// Pulls a job from the queue
    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>>;

    /// Subscribes to the `pgmq_jobs_available` channel
    async fn subscribe(&self) -> anyhow::Result<QueueImplListener>;
}

#[derive(Clone)]
pub struct QueueImpl {
    inner: PgQueue,
}

impl QueueImpl {
    pub fn new(db_conn: PgPool) -> Self {
        Self {
            inner: PgQueue::with_max_retries(db_conn, 2),
        }
    }
}

#[async_trait]
impl<M> Queue<M> for QueueImpl
where
    M: serde::Serialize + serde::de::DeserializeOwned + Send + Unpin + 'static,
{
    async fn push(&self, msg: M, priority: JobPriority) -> anyhow::Result<JobId> {
        self.inner
            .push(JobBuilder::new(msg).priority(priority))
            .await
    }

    async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, M>>> {
        self.inner.pop().await
    }

    async fn subscribe(&self) -> anyhow::Result<QueueImplListener> {
        self.inner.subscribe().await.map(QueueImplListener)
    }
}

/// A listener for the queue job available notification
pub struct QueueImplListener(PgQueueListener);

impl QueueImplListener {
    /// Waits for a new job available notification
    pub async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
        self.0.wait_for_notification().await
    }
}
