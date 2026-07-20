use std::{future::Future, time::Duration};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use time::OffsetDateTime;
use tokio::{sync::watch, task::JoinSet};

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
pub use super::{
    queue::JobPriority,
    service_queue::{WorkerQueue, WorkerQueueHandle},
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
    stop_rx: &mut watch::Receiver<bool>,
    listener: &mut Option<N>,
    poll_period: Duration,
) -> Tick {
    match listener {
        Some(l) => {
            tokio::select! { biased;
                res = stop_rx.changed() => {
                    // Sender dropped (Err) or value flipped to true: shut down.
                    if res.is_err() || *stop_rx.borrow() { Tick::Stop } else { Tick::Poll }
                }
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
                res = stop_rx.changed() => {
                    if res.is_err() || *stop_rx.borrow() { Tick::Stop } else { Tick::Poll }
                }
                _ = tokio::time::sleep(poll_period) => Tick::Poll,
            }
        }
    }
}

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
        reassess_lock,
        unresponsive_breaker,
        dips_accepting_cache,
        concurrency,
        subgraph_indexing_agreements_events_emitter,
    } = state.into();

    // A watch channel fans the stop signal out to every loop; a single mpsc
    // receiver could only wake one of them.
    let (stop_tx, stop_rx) = watch::channel(false);

    let handle = Handle {
        stop_tx: stop_tx.clone(),
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
            reassess_lock,
            unresponsive_breaker,
            dips_accepting_cache,
            subgraph_indexing_agreements_events_emitter,
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
            // One loop ended (shutdown or death); stop the rest too so the worker
            // drains and resolves instead of running on at reduced capacity.
            let _ = stop_tx.send(true);
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
    // `Some` while LISTEN/NOTIFY is healthy; `None` once it has degraded to
    // poll-only operation (see `await_next_tick`).
    let mut listener = Some(queue.subscribe().await?);
    loop {
        // Whether the listener was still healthy going into this tick. Lets us
        // tell a listener that just failed on this very tick apart from one that
        // already degraded on an earlier, poll-period-paced tick.
        let listener_was_present = listener.is_some();

        match await_next_tick(&mut stop_rx, &mut listener, DEFAULT_QUEUE_POLL_PERIOD).await {
            Tick::Stop => return Ok(()),
            Tick::Poll => {}
        }

        // If the listener has degraded to poll-only, try to re-establish it,
        // but never on the same tick the failure fired. A failing
        // `wait_for_notification` returns immediately, so re-subscribing right
        // away and landing on a connection that fails just as fast (for example
        // a pooled connection that accepts LISTEN but never delivers NOTIFY)
        // would spin the loop at full CPU: fail, re-subscribe, pop, fail, with
        // no poll-period sleep anywhere. Waiting until the next tick, when the
        // listener is already `None` and the wait falls through to the
        // poll-period sleep, paces the retry to at most once per poll period. A
        // failure here keeps us in correct poll-only mode.
        if listener.is_none() && !listener_was_present {
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
                // An unexpected DB failure (the empty queue is Ok(None) above);
                // surface it so a DB outage doesn't leave every loop spinning
                // silently while the worker still looks healthy.
                tracing::warn!(error=?err, "Failed to get next job from queue");
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
            Err(JobError::Deferred(delay)) => {
                // Couldn't run now (another reassessment holds the global lock);
                // re-queue at a flat delay without counting a failed attempt.
                // Logged at info so sustained contention is visible per job id.
                let scheduled_for = OffsetDateTime::now_utc() + delay;
                tracing::info!(
                    job = %job.id(),
                    delay_secs = %delay.as_secs(),
                    "Deferring job; another reassessment holds the global lock, will retry"
                );
                if let Err(err) = job.reschedule(scheduled_for).await {
                    tracing::error!(error=?err, "Failed to reschedule deferred job");
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
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Sync,
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

#[cfg(test)]
mod tests {
    use std::future;

    use async_trait::async_trait;
    use tokio::sync::watch;

    use super::{Tick, await_next_tick};
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
        let (_tx, mut rx) = watch::channel(false);
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
        let (tx, mut rx) = watch::channel(false);
        tx.send(true).unwrap();
        let mut listener = Some(SilentNotifier);

        let tick =
            await_next_tick(&mut rx, &mut listener, std::time::Duration::from_secs(60)).await;

        assert_eq!(tick, Tick::Stop);
    }

    /// With no listener (already degraded), the poll-interval fallback drives
    /// the loop so job processing continues.
    #[tokio::test]
    async fn poll_only_mode_ticks_on_interval() {
        let (_tx, mut rx) = watch::channel(false);
        let mut listener: Option<SilentNotifier> = None;

        let tick =
            await_next_tick(&mut rx, &mut listener, std::time::Duration::from_millis(10)).await;

        assert_eq!(tick, Tick::Poll);
    }
}
