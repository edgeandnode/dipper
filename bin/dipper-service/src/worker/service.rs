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
    queue::{JobNotifications, Queue},
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

/// Backstop timeout for processing a single job.
///
/// Every external call `process_job` makes is already individually bounded
/// (IISA HTTP, indexer RPC, chain RPC + receipt polling), so the legitimate
/// worst case is their sum — on the order of a couple of minutes. This timeout
/// sits comfortably above that and only fires if a dependency accepts the
/// connection but never responds, defeating the per-call timeouts. Critically,
/// the worker holds the pgmq transaction (and the row's `Running` lock and a
/// pooled DB connection) open for the whole `process_job` call, so an
/// unbounded hang would pin those resources and wedge the single worker
/// forever. On elapse the in-flight `process_job` future is cancelled (dropped)
/// and the job is rescheduled via its `JobGuard`, releasing the pinned
/// resources. Recovery is idempotent (chain-as-source-of-truth), so re-running
/// a job whose handler was cancelled mid-flight is safe.
pub(crate) const PROCESS_JOB_TIMEOUT: Duration = Duration::from_secs(300);

/// Base backoff for a job rescheduled after hitting [`PROCESS_JOB_TIMEOUT`].
const JOB_TIMEOUT_RETRY_BASE_DELAY: Duration = Duration::from_secs(30);

/// Runs a `process_job` future under [`PROCESS_JOB_TIMEOUT`]. On elapse it
/// cancels the in-flight future and returns a retryable error so the worker
/// reschedules the job (via its `JobGuard`) rather than blocking indefinitely.
async fn run_job_with_timeout<F>(timeout: Duration, fut: F) -> JobResult<()>
where
    F: std::future::Future<Output = JobResult<()>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => res,
        Err(_elapsed) => Err(JobError::Retryable(
            anyhow::anyhow!("job processing exceeded the {timeout:?} backstop timeout"),
            JOB_TIMEOUT_RETRY_BASE_DELAY,
        )),
    }
}

/// What the worker should do after waiting for the next trigger.
#[derive(Debug, PartialEq, Eq)]
enum Tick {
    /// A stop signal was received; the worker loop should exit.
    Stop,
    /// Attempt to pop and process the next job.
    Poll,
}

/// Waits for the next trigger to attempt a queue poll: a stop signal, a
/// `LISTEN`/`NOTIFY` notification, or the poll-interval fallback.
///
/// On a listener error the listener is dropped (`*listener = None`) and the
/// worker degrades to poll-only operation. This is correct, not a degraded
/// state to be feared: `queue.pop()` is independent of `LISTEN`/`NOTIFY` — the
/// notification only wakes the loop earlier than the poll interval, a latency
/// optimisation — so a listener fault never stops job processing and never
/// leaves the worker in an uncertain state. The caller re-subscribes on a
/// later poll.
async fn await_next_tick<N: JobNotifications>(
    stop_rx: &mut mpsc::Receiver<()>,
    listener: &mut Option<N>,
    poll_period: Duration,
) -> Tick {
    match listener {
        Some(l) => {
            tokio::select! { biased;
                _ = stop_rx.recv() => Tick::Stop,
                res = l.wait_for_notification() => {
                    if let Err(err) = res {
                        tracing::warn!(
                            error=?err,
                            "job-available listener failed; degrading to poll-only until it can be re-established"
                        );
                        *listener = None;
                    }
                    Tick::Poll
                }
                _ = tokio::time::sleep(poll_period) => Tick::Poll,
            }
        }
        None => {
            tokio::select! { biased;
                _ = stop_rx.recv() => Tick::Stop,
                _ = tokio::time::sleep(poll_period) => Tick::Poll,
            }
        }
    }
}

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
        liveness,
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
        // `Some` while LISTEN/NOTIFY is healthy; `None` once it has degraded to
        // poll-only operation (see `await_next_tick`).
        let mut listener = Some(queue.subscribe().await?);
        loop {
            match await_next_tick(&mut stop_rx, &mut listener, DEFAULT_QUEUE_POLL_PERIOD).await {
                Tick::Stop => return Ok(()),
                Tick::Poll => {}
            }

            // Tick the liveness watermark on every iteration (including idle
            // polls) so the health endpoint sees a live worker. A job that runs
            // up to PROCESS_JOB_TIMEOUT is the longest gap between ticks.
            liveness.record_progress();

            // If the listener degraded on a previous tick, try to re-establish
            // it. This is bounded to at most once per poll period and is
            // non-fatal: a failure keeps us in correct poll-only mode.
            if listener.is_none() {
                match queue.subscribe().await {
                    Ok(l) => {
                        tracing::info!("re-subscribed to job-available notifications");
                        listener = Some(l);
                    }
                    Err(err) => {
                        tracing::debug!(
                            error=?err,
                            "job-available listener re-subscription failed; staying in poll-only mode"
                        );
                    }
                }
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
            match run_job_with_timeout(PROCESS_JOB_TIMEOUT, process_job(&state, job.desc())).await {
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

#[cfg(test)]
mod tests {
    use std::future;

    use async_trait::async_trait;
    use tokio::sync::mpsc;

    use super::{JobError, Tick, await_next_tick, run_job_with_timeout};
    use crate::worker::queue::JobNotifications;

    /// A listener stub whose `wait_for_notification` always errors. Models a
    /// dropped/broken `LISTEN`/`NOTIFY` connection.
    struct FailingNotifier;

    #[async_trait]
    impl JobNotifications for FailingNotifier {
        async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("simulated listener failure")
        }
    }

    /// A listener stub that never notifies (its future stays pending).
    struct SilentNotifier;

    #[async_trait]
    impl JobNotifications for SilentNotifier {
        async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
            future::pending().await
        }
    }

    /// A listener error must degrade to poll-only (drop the listener) and return
    /// `Poll`, never panic or stop. This is the regression test for the worker
    /// `panic!` that turned a recoverable listener fault into a silent total
    /// stall.
    #[tokio::test]
    async fn listener_error_degrades_to_poll_without_panicking() {
        // Hold the sender so the stop branch stays pending.
        let (_tx, mut rx) = mpsc::channel::<()>(1);
        let mut listener = Some(FailingNotifier);

        let tick =
            await_next_tick(&mut rx, &mut listener, std::time::Duration::from_secs(60)).await;

        assert_eq!(tick, Tick::Poll, "a listener error should still poll");
        assert!(
            listener.is_none(),
            "a listener error should degrade to poll-only (listener dropped)"
        );
    }

    /// A pending stop signal wins over a silent listener (biased select).
    #[tokio::test]
    async fn stop_signal_wins() {
        let (tx, mut rx) = mpsc::channel::<()>(1);
        tx.send(()).await.unwrap();
        let mut listener = Some(SilentNotifier);

        let tick =
            await_next_tick(&mut rx, &mut listener, std::time::Duration::from_secs(60)).await;

        assert_eq!(tick, Tick::Stop);
    }

    /// With no listener (already degraded), the poll-interval fallback drives
    /// the loop so job processing continues.
    #[tokio::test]
    async fn poll_only_mode_ticks_on_interval() {
        let (_tx, mut rx) = mpsc::channel::<()>(1);
        let mut listener: Option<SilentNotifier> = None;

        let tick =
            await_next_tick(&mut rx, &mut listener, std::time::Duration::from_millis(10)).await;

        assert_eq!(tick, Tick::Poll);
    }

    /// A job that hangs past the timeout must be turned into a retryable error
    /// (so it reschedules and the open transaction rolls back), not awaited
    /// indefinitely. Bounded by a test-level timeout so a regression fails fast
    /// instead of hanging.
    #[tokio::test]
    async fn hung_job_times_out_as_retryable() {
        let hung = future::pending::<super::JobResult<()>>();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_job_with_timeout(std::time::Duration::from_millis(50), hung),
        )
        .await
        .expect("run_job_with_timeout did not bound the hung job");

        assert!(
            matches!(result, Err(JobError::Retryable(..))),
            "a timed-out job should be retryable, got {result:?}"
        );
    }

    /// A job that completes within the timeout passes its result through
    /// unchanged (including a Fatal classification).
    #[tokio::test]
    async fn fast_job_result_passes_through() {
        let ok = run_job_with_timeout(std::time::Duration::from_secs(60), async { Ok(()) }).await;
        assert!(ok.is_ok());

        let fatal = run_job_with_timeout(std::time::Duration::from_secs(60), async {
            Err(JobError::Fatal(anyhow::anyhow!("boom")))
        })
        .await;
        assert!(matches!(fatal, Err(JobError::Fatal(..))));
    }
}
