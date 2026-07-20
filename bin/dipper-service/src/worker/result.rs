use std::time::Duration;

/// The result of processing a job.
pub type JobResult<T, E = JobError> = Result<T, E>;

/// Calculate retry delay with exponential backoff: `base * 2^attempt` for the
/// first 5 attempts, then a fixed 5 minutes. With base 5s the sequence is 5, 10,
/// 20, 40, 80, 300, 300, ... seconds.
pub fn calculate_backoff_delay(base_delay: Duration, attempt: u32) -> Duration {
    if attempt < 5 {
        base_delay.saturating_mul(2u32.pow(attempt))
    } else {
        Duration::from_secs(300) // 5 minutes
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

    /// The job couldn't run right now (e.g. a global lock is held). Re-queued
    /// after a flat delay without counting an attempt: the work is still needed,
    /// so it retries until it can run. The worker logs each deferral at info.
    #[error("deferred for {0:?}")]
    Deferred(Duration),

    /// A non-recoverable error occurred.
    ///
    /// The job will be removed from the queue.
    #[error("fatal error: {0}")]
    Fatal(#[source] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_exponential_phase() {
        let base = Duration::from_secs(5);

        // Attempt 0: 5 * 2^0 = 5s
        assert_eq!(calculate_backoff_delay(base, 0), Duration::from_secs(5));

        // Attempt 1: 5 * 2^1 = 10s
        assert_eq!(calculate_backoff_delay(base, 1), Duration::from_secs(10));

        // Attempt 2: 5 * 2^2 = 20s
        assert_eq!(calculate_backoff_delay(base, 2), Duration::from_secs(20));

        // Attempt 3: 5 * 2^3 = 40s
        assert_eq!(calculate_backoff_delay(base, 3), Duration::from_secs(40));

        // Attempt 4: 5 * 2^4 = 80s
        assert_eq!(calculate_backoff_delay(base, 4), Duration::from_secs(80));
    }

    #[test]
    fn test_backoff_fixed_phase() {
        let base = Duration::from_secs(5);

        // Attempt 5+: fixed 5 minutes
        assert_eq!(calculate_backoff_delay(base, 5), Duration::from_secs(300));
        assert_eq!(calculate_backoff_delay(base, 6), Duration::from_secs(300));
        assert_eq!(calculate_backoff_delay(base, 100), Duration::from_secs(300));
    }

    #[test]
    fn test_backoff_handles_overflow() {
        // Very large base delay should saturate rather than overflow
        let base = Duration::from_secs(u64::MAX / 2);

        // Should saturate to max duration, not panic or wrap
        let result = calculate_backoff_delay(base, 4);
        assert!(result >= base);
    }
}
