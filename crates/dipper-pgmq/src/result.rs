use std::time::Duration;

#[derive(Debug)]
pub enum JobResult<T> {
    /// The task was processed successfully.
    Ok(T),
    /// Retry the task after the specified duration.
    Retry(Duration, anyhow::Error),
}
