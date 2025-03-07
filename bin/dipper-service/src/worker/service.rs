use std::{future::Future, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use dipper_pgmq::Queue;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use super::{
    WorkerQueue, handlers,
    handlers::{
        FindIndexerForIndexingRequestCtx, ProcessIndexingAgreementCancellationCtx,
        ProcessIndexingRequestCancellationCtx, ProcessNewIndexingRequestCtx,
        SendIndexingAgreementCancellationCtx, SendIndexingAgreementProposalCtx,
    },
    messages::Message,
    result::JobResult,
};
use crate::{
    indexer_rpc_client::IndexerClient,
    network::NetworkProvider,
    registry::{AgreementRegistry, IndexingRequestRegistry, ReceiptRegistry},
};

/// Default period to pull tasks from the queue.
const DEFAULT_TASK_PULL_PERIOD: Duration = Duration::from_secs(1);

/// Default number of tasks that can be processed concurrently.
const DEFAULT_TASK_BATCH_SIZE: usize = 5;

/// The worker service handle.
#[derive(Clone)]
pub struct Handle {
    /// A channel to stop the worker
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the worker.
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;

        // Wait for the channel to close
        self.tx_stop.closed().await;
    }
}

/// Create a new worker and a future that processes tasks from the queue.
///
/// The worker pulls tasks from the queue and processes them concurrently every 10 seconds.
pub fn new<S, Q, R, N, W, C, I>(
    queue: Q,
    state: S,
) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    Q: Queue<Message> + Clone + Send + Sync,
    R: IndexingRequestRegistry + AgreementRegistry + ReceiptRegistry + Clone + Send + Sync,
    N: NetworkProvider + Clone + Send + Sync,
    W: WorkerQueue + Clone + Send + Sync,
    C: IndexerClient + Clone + Send + Sync,
    I: CandidateSelection + Clone + Send + Sync,
    ProcessNewIndexingRequestCtx<R, N, W, I>: FromState<S>,
    ProcessIndexingRequestCancellationCtx<R, W>: FromState<S>,
    FindIndexerForIndexingRequestCtx<R, N, W, I>: FromState<S>,
    SendIndexingAgreementProposalCtx<R, N, W, C>: FromState<S>,
    SendIndexingAgreementCancellationCtx<R, C>: FromState<S>,
    ProcessIndexingAgreementCancellationCtx<R, W>: FromState<S>,
{
    let (tx_stop, rx_stop) = mpsc::channel(1);

    let handle = Handle { tx_stop };

    let fut = async move {
        let state = state;

        let mut stop_rx = rx_stop;
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
                       let _span = tracing::debug_span!("process_task", task = %task.id);

                        match process_task(&state, task.message).await {
                            Ok(JobResult::Ok(_)) => {
                                // Remove the task from the queue
                                let _ = queue.remove(task.id).await;
                            }
                            Ok(JobResult::Retry(duration, err)) => {
                                tracing::debug!(error=?err, "Rescheduling task after failure");

                                // Retry the task after the specified duration
                                let scheduled_for = OffsetDateTime::now_utc() + duration;
                                let _ = queue.mark_job_as_failed(task.id, Some(scheduled_for)).await;
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

async fn process_task<S, W, N, R, C, I>(
    state: &S,
    message: Message,
) -> anyhow::Result<JobResult<()>>
where
    R: IndexingRequestRegistry + AgreementRegistry + ReceiptRegistry,
    N: NetworkProvider,
    W: WorkerQueue,
    C: IndexerClient,
    I: CandidateSelection,
    ProcessNewIndexingRequestCtx<R, N, W, I>: FromState<S>,
    ProcessIndexingRequestCancellationCtx<R, W>: FromState<S>,
    FindIndexerForIndexingRequestCtx<R, N, W, I>: FromState<S>,
    SendIndexingAgreementProposalCtx<R, N, W, C>: FromState<S>,
    SendIndexingAgreementCancellationCtx<R, C>: FromState<S>,
    ProcessIndexingAgreementCancellationCtx<R, W>: FromState<S>,
{
    /// Dispatch a message to the appropriate message handler, based on the message type, with
    /// the given state.
    macro_rules! _dispatch {
        ($state:expr, $message:expr, {$($msg_pat:path => $handler_fn:path),* $(,)?}) => {
            match $message {
                $(
                    $msg_pat(msg) => $handler_fn(FromState::from_state($state), msg).await.map_err(Into::into),
                )*
            }
        };
    }

    _dispatch!(state, message, {
        Message::ProcessNewIndexingRequest => handlers::process_new_indexing_request,
        Message::ProcessIndexingRequestCancellation => handlers::process_indexing_request_cancellation,
        Message::FindIndexerForIndexingRequest => handlers::find_indexer_for_indexing_request,
        Message::SendIndexingAgreementProposal => handlers::send_indexing_agreement_proposal,
        Message::SendIndexingAgreementCancellation => handlers::send_indexing_agreement_cancellation,
        Message::ProcessIndexingAgreementIndexerCancellation => handlers::process_indexing_agreement_indexer_cancellation,
        Message::ProcessIndexingAgreementRequesterCancellation => handlers::process_indexing_agreement_requester_cancellation,
    })
}
