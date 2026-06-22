use std::{future::Future, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use time::OffsetDateTime;
use tokio::sync::mpsc;

pub use super::service_queue::{WorkerQueue, WorkerQueueHandle};
use super::{
    context::{Ctx, InnerCtx},
    handlers::{
        self, CancelRejectedAgreementOnChainCtx, ReassessIndexingRequestCtx,
        SendIndexingAgreementProposalCtx, SubmitOfferCtx,
    },
    messages::Message,
    queue::Queue,
    result::{JobError, JobResult, calculate_backoff_delay},
};
use crate::{
    chain_client::ChainClient,
    indexer_rpc_client::IndexerClient,
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingRequestRegistry,
        PendingCancellationRegistry,
    },
};

/// Default period to poll the queue for new jobs
const DEFAULT_QUEUE_POLL_PERIOD: Duration = Duration::from_secs(1);

/// Create a new worker and a future that processes jobs from the queue.
///
/// The worker pulls jobs from the queue and processes them concurrently every 1 second.
pub fn new<S, Q, R, C, I, T>(state: S) -> (Handle<Q>, impl Future<Output = anyhow::Result<()>>)
where
    Q: Queue<Message> + Clone + Send + Sync,
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Clone
        + Send
        + Sync,
    C: IndexerClient + Clone + Send + Sync,
    I: CandidateSelection + Clone + Send + Sync,
    T: ChainClient + Clone + Send + Sync,
    S: Into<Ctx<Q, R, C, I, T>>,
{
    let Ctx {
        queue,
        signer,
        agreement_conf,
        rca_domain,
        pricing_table,
        registry,
        network,
        client,
        iisa,
        chain_client,
        networks_registry,
        additional_networks,
        entity_count_cache,
        chain_listener_notify,
        bypass_chain_clock_defenses,
        chain_listener_chain_id,
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
            rca_domain,
            pricing_table,
            registry,
            network,
            client,
            iisa,
            chain_client,
            networks_registry,
            additional_networks,
            entity_count_cache,
            chain_listener_notify,
            worker: WorkerQueueHandle::new(queue.clone()),
            bypass_chain_clock_defenses,
            chain_listener_chain_id,
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
                Err(JobError::Retryable(err, base_delay)) => {
                    let attempt = job.failed_attempts();
                    let delay = calculate_backoff_delay(base_delay, attempt);

                    tracing::debug!(
                        error=?err,
                        attempt=%attempt,
                        delay_secs=%delay.as_secs(),
                        "Rescheduling job after failure with backoff"
                    );

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

async fn process_job<S, W, R, C, I, T>(state: &S, message: &Message) -> JobResult<()>
where
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry,
    W: WorkerQueue,
    C: IndexerClient,
    I: CandidateSelection,
    T: ChainClient,
    ReassessIndexingRequestCtx<R, W, I, T>: FromState<S>,
    SendIndexingAgreementProposalCtx<R, W, C>: FromState<S>,
    CancelRejectedAgreementOnChainCtx<R, T>: FromState<S>,
    SubmitOfferCtx<R, T>: FromState<S>,
{
    /// Dispatch a message to the appropriate message handler, based on the message type.
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
        Message::ReassessIndexingRequest => handlers::reassess_indexing_request,
        Message::SendIndexingAgreementProposal => handlers::send_indexing_agreement_proposal,
        Message::CancelRejectedAgreementOnChain => handlers::cancel_rejected_agreement_on_chain,
        Message::SubmitOffer => handlers::submit_offer,
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
