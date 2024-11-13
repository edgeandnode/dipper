use std::{future::Future, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use dipper_pgmq::{queue::Queue, result::JobResult};
use dipper_registry::Registry;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use super::{handlers, messages::Message};
use crate::{indexers::DipsClient, network::api::NetworkProvider};

/// Default period to pull tasks from the queue.
const DEFAULT_TASK_PULL_PERIOD: Duration = Duration::from_secs(1);

/// Default number of tasks that can be processed concurrently.
const DEFAULT_TASK_BATCH_SIZE: usize = 5;

/// The worker service handle.
#[derive(Clone)]
pub struct ServiceHandle {
    /// A channel to stop the worker
    tx: mpsc::Sender<()>,
}

impl ServiceHandle {
    /// Stop the worker.
    pub async fn stop(self) {
        if self.tx.is_closed() {
            return;
        }

        let _ = self.tx.send(()).await;
    }
}

/// Create a new worker and a future that processes tasks from the queue.
///
/// The worker pulls tasks from the queue and processes them concurrently every 10 seconds.
pub fn worker<S, Q, SQ, SN, SR, SC, SI>(
    state: S,
    queue: Q,
) -> (ServiceHandle, impl Future<Output = anyhow::Result<()>>)
where
    S: Clone + 'static,
    Q: Queue<Message> + Clone + 'static,
    SQ: Queue<Message> + FromState<S>,
    SN: NetworkProvider + FromState<S>,
    SR: Registry + FromState<S>,
    SC: DipsClient + FromState<S>,
    SI: CandidateSelection + FromState<S>,
{
    let (tx, rx) = mpsc::channel(1);

    let handle = ServiceHandle { tx };

    let fut = async move {
        let state = state;
        let queue = queue;

        let mut stop_rx = rx;
        loop {
            tokio::select! { biased;
                _ = stop_rx.recv() => {
                    break;
                }
                _ = tokio::time::sleep(DEFAULT_TASK_PULL_PERIOD) => {
                    let Ok(tasks) = queue.pull(DEFAULT_TASK_BATCH_SIZE).await else {
                        continue
                    };

                    // Process the tasks sequentially
                    for task in tasks {
                        let _span = tracing::debug_span!("process_task", task = %task.id).entered();

                        match process_task::<S, SQ, SN, SR, SC, SI>(&state, task.message).await {
                            Ok(JobResult::Ok(_)) => {
                                // Remove the task from the queue
                                let _ = queue.remove(task.id).await;
                            }
                            Ok(JobResult::Retry(duration, err)) => {
                                tracing::debug!(error=?err, "Rescheduling task after failure");

                                // Retry the task after the specified duration
                                let scheduled_for = OffsetDateTime::now_utc() + duration;
                                let _ = queue.fail_job(task.id, Some(scheduled_for)).await;
                            }
                            Err(err) => {
                                tracing::debug!(error=?err, "Failed to process task");

                                // Remove the task from the queue as it failed and
                                // should not be retried
                                let _ = queue.remove(task.id).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    };

    (handle, fut)
}

async fn process_task<S, Q, N, R, C, I>(
    state: &S,
    message: Message,
) -> anyhow::Result<JobResult<()>>
where
    Q: Queue<Message> + FromState<S>,
    N: NetworkProvider + FromState<S>,
    R: Registry + FromState<S>,
    C: DipsClient + FromState<S>,
    I: CandidateSelection + FromState<S>,
{
    let res = match message {
        Message::ProcessNewIndexingRequest(msg) => {
            handlers::process_new_indexing_request(
                Q::from_state(state),
                N::from_state(state),
                R::from_state(state),
                I::from_state(state),
                msg,
            )
            .await?
        }
        Message::ProcessIndexingRequestCancellation(msg) => {
            handlers::process_indexing_request_cancellation(
                Q::from_state(state),
                R::from_state(state),
                msg,
            )
            .await?
        }
        Message::FindIndexerForIndexingRequest(msg) => {
            handlers::find_indexer_for_indexing_request(
                Q::from_state(state),
                N::from_state(state),
                R::from_state(state),
                I::from_state(state),
                msg,
            )
            .await?
        }
        Message::SendIndexingAgreementProposal(msg) => {
            handlers::send_indexing_agreement_proposal(
                Q::from_state(state),
                R::from_state(state),
                C::from_state(state),
                msg,
            )
            .await?
        }
        Message::SendIndexingAgreementCancellation(msg) => {
            handlers::send_indexing_agreement_cancellation(
                R::from_state(state),
                C::from_state(state),
                msg,
            )
            .await?
        }
        Message::ProcessIndexingAgreementCancellation(msg) => {
            handlers::process_indexing_agreement_cancellation(
                Q::from_state(state),
                R::from_state(state),
                msg,
            )
            .await?
        }
    };
    Ok(res)
}
