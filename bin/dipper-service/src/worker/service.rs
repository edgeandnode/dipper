use std::{future::Future, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use time::OffsetDateTime;
use tokio::{sync::watch, task::JoinSet};

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

/// Create a worker plus a future that runs `concurrency` (>=1) loops, each
/// draining the same queue. The queue's `FOR UPDATE SKIP LOCKED` pop hands
/// every loop a distinct job; the default of 1 is a single serial loop.
pub fn new<S, Q, R, C, I, T>(state: S) -> (Handle<Q>, impl Future<Output = anyhow::Result<()>>)
where
    Q: Queue<Message> + Clone + Send + Sync + 'static,
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Clone
        + Send
        + Sync
        + 'static,
    C: IndexerClient + Clone + Send + Sync + 'static,
    I: CandidateSelection + Clone + Send + Sync + 'static,
    T: ChainClient + Clone + Send + Sync + 'static,
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
        reassess_locks,
        concurrency,
    } = state.into();

    // A watch channel fans the stop signal out to every loop; a single mpsc
    // receiver could only wake one of them.
    let (stop_tx, stop_rx) = watch::channel(false);

    let handle = Handle {
        stop_tx,
        worker_queue_handle: WorkerQueueHandle::new(queue.clone()),
    };
    let fut = async move {
        // Built once, cloned per loop. Every field is Arc/Clone, so the clones
        // share the same registry, caches and reassess locks.
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
            reassess_locks,
        };

        let mut set = JoinSet::new();
        for _ in 0..concurrency.max(1) {
            set.spawn(run_loop(state.clone(), queue.clone(), stop_rx.clone()));
        }
        // Drop the supervisor's own receiver so `Handle::stop`'s `closed()`
        // tracks only the loops.
        drop(stop_rx);

        let mut first_err: Option<anyhow::Error> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(error=?err, "Worker loop exited with error");
                    first_err.get_or_insert(err);
                }
                Err(err) => {
                    // A panicking loop must also fail the worker, not be swallowed
                    // into an Ok — otherwise a single loop death looks like success.
                    tracing::error!(error=?err, "Worker loop task panicked");
                    first_err.get_or_insert_with(|| anyhow::Error::new(err));
                }
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    };

    (handle, fut)
}

/// One worker loop: drains jobs until the stop signal fires. Each loop owns its
/// own queue notification listener because the underlying `LISTEN`/`NOTIFY` is
/// per-connection and can't be shared across tasks.
async fn run_loop<Q, R, C, I, T>(
    state: InnerCtx<R, WorkerQueueHandle<Q>, C, I, T>,
    queue: Q,
    mut stop_rx: watch::Receiver<bool>,
) -> anyhow::Result<()>
where
    Q: Queue<Message> + Clone + Send + Sync + 'static,
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Clone
        + Send
        + Sync
        + 'static,
    C: IndexerClient + Clone + Send + Sync + 'static,
    I: CandidateSelection + Clone + Send + Sync + 'static,
    T: ChainClient + Clone + Send + Sync + 'static,
{
    let mut listener = queue.subscribe().await?;
    loop {
        tokio::select! { biased;
            res = stop_rx.changed() => {
                // Sender dropped (Err) or value flipped to true: shut down.
                if res.is_err() || *stop_rx.borrow() {
                    return Ok(());
                }
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
    /// Broadcasts the stop signal to every worker loop
    stop_tx: watch::Sender<bool>,

    /// A handle to the worker's queue
    worker_queue_handle: WorkerQueueHandle<Q>,
}

impl<Q> Handle<Q> {
    /// Get a handle to the worker's queue
    pub fn queue(&self) -> &WorkerQueueHandle<Q> {
        &self.worker_queue_handle
    }

    /// Stop the worker and wait for every loop to drain.
    pub async fn stop(self) {
        if self.stop_tx.is_closed() {
            return;
        }

        // One send wakes all loops; closed() resolves once they've all exited.
        let _ = self.stop_tx.send(true);
        self.stop_tx.closed().await;
    }
}
