use std::{future::Future, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use time::OffsetDateTime;
use tokio::sync::mpsc;

pub use super::service_queue::{WorkerQueue, WorkerQueueHandle};
use super::{
    context::{Ctx, InnerCtx},
    handlers::{
        self, FindIndexerForIndexingRequestCtx, ProcessIndexingAgreementCancellationCtx,
        ProcessIndexingRequestCancellationCtx, ProcessNewIndexingRequestCtx,
        SendIndexingAgreementCancellationCtx, SendIndexingAgreementProposalCtx,
    },
    messages::Message,
    queue::Queue,
    result::{JobError, JobResult},
};
use crate::{
    indexer_rpc_client::IndexerClient,
    network::NetworkProvider,
    registry::{AgreementRegistry, IndexingRequestRegistry, ReceiptRegistry},
};

/// Default period to poll the queue for new jobs
const DEFAULT_QUEUE_POLL_PERIOD: Duration = Duration::from_secs(1);

/// Create a new worker and a future that processes jobs from the queue.
///
/// The worker pulls jobs from the queue and processes them concurrently every 1 second.
pub fn new<S, Q, R, N, C, I>(state: S) -> (Handle<Q>, impl Future<Output = anyhow::Result<()>>)
where
    Q: Queue<Message> + Clone + Send + Sync,
    R: IndexingRequestRegistry + AgreementRegistry + ReceiptRegistry + Clone + Send + Sync,
    N: NetworkProvider + Clone + Send + Sync,
    C: IndexerClient + Clone + Send + Sync,
    I: CandidateSelection + Clone + Send + Sync,
    S: Into<Ctx<Q, R, N, C, I>>,
{
    let Ctx {
        queue,
        signer,
        agreement_conf,
        pricing_table,
        registry,
        network,
        client,
        iisa,
    } = state.into();

    let (tx_stop, rx_stop) = mpsc::channel(1);

    let handle = Handle {
        tx_stop,
        worker_queue_handle: WorkerQueueHandle::new(queue.clone()),
    };
    let fut = async move {
        let state = InnerCtx {
            signer,
            agreement_conf,
            pricing_table,
            registry,
            network,
            client,
            iisa,
            worker: WorkerQueueHandle::new(queue.clone()),
        };

        let mut stop_rx = rx_stop;
        let mut listener = queue.subscribe().await?;
        loop {
            tokio::select! { biased;
                _ = stop_rx.recv() => {
                    return Ok(());
                }
                res = listener.wait_for_notification() => {
                    if let Err(err) = res {
                        tracing::error!(error=?err, "Failed to wait for job available notification");
                        panic!("An unexpected error occurred while waiting for job available notification");
                    }
                }
                _ = tokio::time::sleep(DEFAULT_QUEUE_POLL_PERIOD) => {}
            }

            // Process the job
            let job = match queue.pop().await {
                Ok(Some(job)) => job,
                Ok(None) => continue,
                Err(err) => {
                    tracing::debug!(error=?err, "Failed to get next job from queue");
                    continue;
                }
            };

            let _span = tracing::debug_span!("process_job", job = %job.id());
            match process_job(&state, job.desc()).await {
                Ok(..) => {
                    if let Err(err) = job.remove().await {
                        tracing::debug!(error=?err, "Failed to remove job from queue");
                    }
                }
                Err(JobError::Retryable(err, delay)) => {
                    tracing::debug!(error=?err, "Rescheduling job after failure");

                    // Retry the job after the specified duration
                    let scheduled_for = OffsetDateTime::now_utc() + delay;
                    if let Err(err) = job.mark_as_failed_and_reschedule(scheduled_for).await {
                        tracing::error!(error=?err, "Failed to reschedule job");
                    }
                }
                Err(JobError::Fatal(err)) => {
                    tracing::debug!(error=?err, "Failed to process job");

                    // Remove the job from the queue as it failed and
                    // should not be retried
                    if let Err(err) = job.remove().await {
                        tracing::error!(error=?err, "Failed to remove job from queue");
                    }
                }
            }
        }
    };

    (handle, fut)
}

async fn process_job<S, W, N, R, C, I>(state: &S, message: &Message) -> JobResult<()>
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
                    $msg_pat(msg) => $handler_fn(FromState::from_state($state), msg).await,
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

/// The worker service handle
#[derive(Clone)]
pub struct Handle<Q> {
    /// A channel to stop the worker
    tx_stop: mpsc::Sender<()>,

    /// A handle to the worker's queue
    worker_queue_handle: WorkerQueueHandle<Q>,
}

impl<Q> Handle<Q> {
    /// Get a handle to the worker's queue
    pub fn queue(&self) -> &WorkerQueueHandle<Q> {
        &self.worker_queue_handle
    }

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
