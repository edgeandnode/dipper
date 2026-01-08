use std::time::Duration;

use time::OffsetDateTime;

/// The result of processing a job.
pub type JobResult<T, E = JobError> = Result<T, E>;

/// Metadata about the current job being processed.
///
/// This is passed to handlers to enable time-based fallback logic.
#[derive(Debug, Clone, Copy)]
pub struct JobMeta {
    /// When the job was first created
    pub created_at: OffsetDateTime,
    /// Number of failed attempts so far
    pub failed_attempts: u32,
}

impl JobMeta {
    /// Check if the job has been retrying for longer than the specified duration.
    pub fn age_exceeds(&self, duration: time::Duration) -> bool {
        let age = OffsetDateTime::now_utc() - self.created_at;
        age > duration
    }
}

/// The error type for job processing.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// A retryable error occurred.
    ///
    /// The job will be retried after the specified duration.
    #[error("retryable error: {0}")]
    Retryable(#[source] anyhow::Error, Duration),

    /// A non-recoverable error occurred.
    ///
    /// The job will be removed from the queue.
    #[error("fatal error: {0}")]
    Fatal(#[source] anyhow::Error),
}
