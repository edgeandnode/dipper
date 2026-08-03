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

/// Upper bound on what [`retries_within_window`] will return, so a window set
/// absurdly long cannot hand a job an unbounded budget. Attempts are 5 minutes
/// apart by then, so the ones given up buy little.
const MAX_RETRIES_PER_WINDOW: u32 = 64;

/// How many retries fit inside `window`, each waiting
/// [`calculate_backoff_delay`] longer than the last. Ties a job's budget to the
/// deadline its work has to meet, not a fixed count that can run out early.
pub fn retries_within_window(window: Duration, base_delay: Duration) -> u32 {
    let mut elapsed = Duration::ZERO;

    for retries in 0..MAX_RETRIES_PER_WINDOW {
        let next = elapsed.saturating_add(calculate_backoff_delay(base_delay, retries));
        if next > window {
            return retries;
        }
        elapsed = next;
    }

    MAX_RETRIES_PER_WINDOW
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

    /// The live case: a 600 second acceptance window and the 30 second base
    /// delay an offer submission retries on. The 4 retries fall at 30, 90, 210
    /// and 450 seconds, and a 5th would wait until 930, past the deadline.
    #[test]
    fn test_retries_fill_the_default_acceptance_window() {
        let window = Duration::from_secs(600);
        let base = Duration::from_secs(30);

        assert_eq!(retries_within_window(window, base), 4);

        let mut elapsed = Duration::ZERO;
        for retry in 0..4 {
            elapsed += calculate_backoff_delay(base, retry);
        }
        assert_eq!(elapsed, Duration::from_secs(450));
        assert!(elapsed + calculate_backoff_delay(base, 4) > window);

        // The budget is sized on the shorter 5 second base, which fits 6.
        assert_eq!(retries_within_window(window, Duration::from_secs(5)), 6);
    }

    #[test]
    fn test_no_retries_fit_a_window_shorter_than_the_first_delay() {
        let window = Duration::from_secs(10);
        let base = Duration::from_secs(30);

        assert_eq!(retries_within_window(window, base), 0);
    }

    /// A retry landing exactly on the deadline still has time to be made.
    #[test]
    fn test_a_retry_landing_on_the_boundary_counts() {
        let base = Duration::from_secs(30);

        assert_eq!(retries_within_window(Duration::from_secs(30), base), 1);
        assert_eq!(retries_within_window(Duration::from_secs(29), base), 0);
    }

    /// Past the exponential phase the delay is flat, so an unbounded window
    /// would otherwise keep counting; the cap stops it.
    #[test]
    fn test_retries_within_window_is_capped() {
        let huge = Duration::from_secs(u64::MAX / 2);

        assert_eq!(
            retries_within_window(huge, Duration::from_secs(30)),
            MAX_RETRIES_PER_WINDOW
        );
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
