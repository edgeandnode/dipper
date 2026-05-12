//! On-chain agreement state reconciler
//!
//! This service polls a subgraph for changes to indexing agreements and
//! reconciles dipper's local view of each agreement against the on-chain
//! truth. Every poll returns agreements whose `lastStateChangeBlock` has
//! advanced since the last processed block; for each returned snapshot
//! dipper computes the transition from local status to remote state and
//! applies it.
//!
//! ## Transitions
//!
//! | Local status       | Remote state        | Action                                                                 |
//! |--------------------|---------------------|------------------------------------------------------------------------|
//! | Created            | Accepted            | mark AcceptedOnChain, run pending cancellations                         |
//! | Created            | CanceledBy*         | mark AcceptedOnChain, run pending cancellations, then mark canceled    |
//! | AcceptedOnChain    | Accepted            | no-op                                                                  |
//! | AcceptedOnChain    | CanceledBy*         | mark canceled (by requester vs indexer based on `canceled_by` address) |
//! | Expired            | Accepted            | mark AcceptedOnChain (recovery), run pending cancellations              |
//! | Expired            | CanceledBy*         | recover to AcceptedOnChain, run pending cancellations, then cancel     |
//! | Rejected           | Accepted            | queue cancel-on-chain (adversarial path, dead code under proposal-first) |
//! | Canceled*          | *                   | no-op (terminal)                                                       |
//!
//! ## Data Source
//!
//! Snapshots come from the indexing-payments-subgraph's aggregated
//! `IndexingAgreement` entity (see [`AgreementStateSnapshot`]). The subgraph
//! exposes `lastStateChangeBlock` for cursor pagination and `canceledBy`
//! as the actual canceler address, which dipper compares against its own
//! signer to distinguish self-initiated cancels from indexer-initiated
//! cancels.

use std::{future::Future, sync::Arc, time::Duration};

use dipper_core::ids::IndexingAgreementId;
use thegraph_core::alloy::primitives::Address;
use tokio::{
    sync::{Notify, mpsc},
    time::MissedTickBehavior,
};

use super::chain_events::{AgreementStateSnapshot, ChainEventSource, Cursor};
use crate::{
    chain_client::ChainClient,
    config::ChainListenerConfig,
    registry::{
        AgreementRegistry, CancelKind, IndexingAgreement, IndexingAgreementStatus,
        PendingCancellationRegistry, ReconciliationItem,
    },
    worker::service::WorkerQueue,
};

/// Idle interval used when no `Created` agreements are awaiting acceptance.
const SLOW_POLL_INTERVAL: Duration = Duration::from_secs(300);
/// How often the heartbeat info-line fires while idle. ~5 min at the fast
/// rate, every poll at the slow rate.
const HEARTBEAT_POLLS: u64 = 60;
/// After this many back-to-back fetch failures the listener escalates the
/// log line from `warn` to `error`.
const MAX_CONSECUTIVE_FAILURES: u32 = 10;
/// Base for the exponential failure-backoff sleep applied between ticks
/// while `consecutive_failures > 0`.
const FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(5);
/// Subgraph-stall warn / error escalation: ticks-without-head-advance.
const STALL_WARN_THRESHOLD: u32 = 5;
const STALL_ERROR_THRESHOLD: u32 = 15;
/// Sanity bound on a single tick's drain — not an operating point;
/// steady-state drains are 0–1 pages.
const MAX_PAGES_PER_DRAIN: u32 = 1000;
/// Cap on the cancellation-sweep batch so a backlog drains across polls
/// instead of blocking one tick.
const SWEEP_BATCH_SIZE: i64 = 1000;
/// How often the cancellation sweep runs. The sweep is purely
/// crash-recovery; the steady-state fan-out fires from finalize on a
/// fresh accept, so per-poll execution is wasted DB work.
const SWEEP_POLLS: u64 = 60;

/// Handle for controlling the chain listener service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the chain listener service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the chain listener service
pub struct Ctx<R, W, E, T> {
    /// Registry for querying and updating agreements
    pub registry: R,
    /// Worker queue (still used by reconciliation paths that hand work back to the worker)
    pub worker_queue: W,
    /// Chain event source (subgraph)
    pub event_source: E,
    /// Chain client used to fire on-chain `cancelIndexingAgreementByPayer` when
    /// a replacement agreement is confirmed accepted on-chain.
    pub chain_client: T,
    /// Service configuration
    pub config: ChainListenerConfig,
    /// The payer/signer address (used to identify who initiated cancellations)
    pub signer_address: thegraph_core::alloy::primitives::Address,
    /// Signalled by the worker when proposals are dispatched, waking the listener
    /// from the slow (300s) idle interval so it starts fast-polling immediately.
    pub chain_listener_notify: Arc<Notify>,
}

/// State persisted in the database
#[derive(Clone)]
pub struct ChainListenerState {
    pub _chain_id: u64,
    pub last_processed_block: u64,
    /// `id` of the last consumed entity at `last_processed_block`. `None`
    /// means the cursor sits at a block boundary. Stored alongside the
    /// block to support keyset pagination across same-block ties.
    pub last_processed_id: Option<dipper_core::ids::IndexingAgreementId>,
    /// Block timestamp (epoch seconds). Used by the expiration service.
    pub last_processed_block_timestamp: Option<u64>,
}

/// Create a new chain listener service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service polls the subgraph for agreement state snapshots
/// and reconciles them against the local DB.
pub fn new<R, W, E, T>(ctx: Ctx<R, W, E, T>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + ChainListenerStateRegistry + PendingCancellationRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    E: ChainEventSource,
    T: ChainClient + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        event_source,
        chain_client,
        config,
        signer_address,
        chain_listener_notify,
    } = ctx;

    let service = async move {
        tracing::info!(
            poll_interval_secs = config.poll_interval.as_secs(),
            endpoint = %config.subgraph_endpoint,
            "chain listener service started"
        );

        let chain_id = config.chain_id;
        let reorg_buffer_blocks = config.reorg_buffer_blocks;
        let chain_ts_drift_tolerance_secs = config.chain_ts_drift_tolerance_secs;

        // Get initial state from DB or start at genesis. We track
        // `(block, id)` together so a same-block-tie that crashed mid-page
        // is resumed from the right entity rather than re-played from the
        // block boundary.
        let (mut cursor, mut last_persisted_timestamp) =
            match registry.get_chain_listener_state(chain_id).await {
                Ok(Some(state)) => {
                    tracing::info!(
                        last_processed_block = state.last_processed_block,
                        last_processed_id = ?state.last_processed_id.map(|id| id.to_string()),
                        "Resuming from last processed cursor"
                    );
                    (
                        Cursor {
                            block: state.last_processed_block,
                            id: state.last_processed_id,
                        },
                        state.last_processed_block_timestamp,
                    )
                }
                Ok(None) => {
                    tracing::info!("No previous state, starting from genesis");
                    (Cursor::genesis(), None)
                }
                Err(err) => {
                    tracing::error!(error = %err, "Failed to get chain listener state");
                    return Err(err.into());
                }
            };

        // Adaptive polling: fast (config.poll_interval) when Created
        // agreements exist, slow (`SLOW_POLL_INTERVAL`) when idle.
        let fast_interval = config.poll_interval;
        let slow_interval = SLOW_POLL_INTERVAL;
        let mut timer = tokio::time::interval(fast_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut using_fast_interval = true;

        // Track consecutive failures for adaptive backoff
        let mut consecutive_failures: u32 = 0;

        // Observability: heartbeat and stall detection
        let mut polls_since_last_event: u64 = 0;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        // Counts up to SWEEP_POLLS, then trips the cancellation sweep.
        // Starts at SWEEP_POLLS so the first poll runs the sweep,
        // recovering any pre-startup orphans.
        let mut polls_since_sweep: u64 = SWEEP_POLLS;
        // Wall-clock instant of the last successful chain-timestamp
        // persist. Used to bound how fast the persisted timestamp can
        // advance: a hostile response that stays just under the skew
        // tolerance every poll would otherwise drift the timestamp
        // forward by tens of seconds per tick, expiring agreements
        // prematurely. Persisted in memory only; on restart it resets
        // and the first poll's bound is strict.
        let mut last_chain_ts_persist_wall: std::time::Instant = std::time::Instant::now();

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
                _ = chain_listener_notify.notified() => {
                    // Worker dispatched a proposal -- switch to fast polling immediately
                    if !using_fast_interval {
                        timer = tokio::time::interval(fast_interval);
                        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        timer.tick().await;
                        using_fast_interval = true;
                        tracing::info!(
                            interval_secs = fast_interval.as_secs(),
                            "chain listener woken by proposal dispatch, switching to fast polling"
                        );
                    }
                },
            }

            // Adaptive interval: check if there are Created agreements awaiting acceptance
            let has_pending = registry.exists_active_agreements().await.unwrap_or(false);

            let want_fast = has_pending;
            if want_fast != using_fast_interval {
                let new_interval = if want_fast {
                    fast_interval
                } else {
                    slow_interval
                };
                timer = tokio::time::interval(new_interval);
                timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                timer.tick().await; // consume the immediate first tick
                using_fast_interval = want_fast;
                tracing::info!(
                    interval_secs = new_interval.as_secs(),
                    has_pending_agreements = has_pending,
                    "Chain listener polling interval changed"
                );
            }

            // Apply adaptive backoff on consecutive failures
            if consecutive_failures > 0 {
                let backoff = FAILURE_BACKOFF_BASE
                    .saturating_mul(2u32.saturating_pow(consecutive_failures.min(6)));
                tracing::debug!(
                    consecutive_failures,
                    backoff_secs = backoff.as_secs(),
                    "Applying failure backoff"
                );
                tokio::time::sleep(backoff).await;
            }

            let outcome = match drain_once(
                &mut cursor,
                &mut last_persisted_timestamp,
                &mut last_chain_ts_persist_wall,
                &mut last_subgraph_head,
                &mut stall_count,
                &mut consecutive_failures,
                chain_id,
                reorg_buffer_blocks,
                chain_ts_drift_tolerance_secs,
                signer_address,
                &registry,
                &worker_queue,
                &chain_client,
                &event_source,
                &mut rx_stop,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(()) => continue,
            };

            if outcome.stopped {
                tracing::debug!("chain listener stopping mid-cycle");
                return Ok(());
            }

            if outcome.processed > 0 || outcome.errors > 0 {
                tracing::info!(
                    pages_per_tick = outcome.pages,
                    drain_duration_ms = outcome.duration_ms,
                    subgraph_lag_seconds = outcome.subgraph_lag_seconds,
                    processed = outcome.processed,
                    errors = outcome.errors,
                    "Reconciled agreement snapshots (drain)"
                );
                polls_since_last_event = 0;
            } else if outcome.latest_block <= cursor.block {
                polls_since_last_event += 1;
                if polls_since_last_event.is_multiple_of(HEARTBEAT_POLLS) || !using_fast_interval {
                    tracing::info!(
                        pages_per_tick = outcome.pages,
                        drain_duration_ms = outcome.duration_ms,
                        subgraph_lag_seconds = outcome.subgraph_lag_seconds,
                        last_processed_block = cursor.block,
                        subgraph_head = outcome.latest_block,
                        polls_idle = polls_since_last_event,
                        fast_mode = using_fast_interval,
                        "chain listener heartbeat"
                    );
                }
            }

            polls_since_sweep += 1;
            if polls_since_sweep >= SWEEP_POLLS {
                sweep_executable_pending_cancellations(&registry, &chain_client).await;
                polls_since_sweep = 0;
            }
        }

        tracing::debug!("chain listener service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

struct DrainOutcome {
    pages: u32,
    processed: u64,
    errors: u64,
    latest_block: u64,
    duration_ms: u64,
    subgraph_lag_seconds: i64,
    /// True when `rx_stop` fired mid-drain; the outer loop returns
    /// gracefully rather than starting another tick.
    stopped: bool,
}

/// Run one drain cycle: page through new agreement snapshots from the
/// subgraph, apply reconciliation transitions, persist cursor + timestamp
/// progress per-page, and emit observability fields. Mutates the long-lived
/// state in place; the returned [`DrainOutcome`] carries only stats. `Err(())`
/// signals the subgraph fetch itself failed so the outer loop skips the
/// heartbeat block.
#[allow(clippy::too_many_arguments)]
async fn drain_once<R, W, E, T>(
    cursor: &mut Cursor,
    last_persisted_timestamp: &mut Option<u64>,
    last_chain_ts_persist_wall: &mut std::time::Instant,
    last_subgraph_head: &mut u64,
    stall_count: &mut u32,
    consecutive_failures: &mut u32,
    chain_id: u64,
    reorg_buffer_blocks: u32,
    chain_ts_drift_tolerance_secs: u64,
    signer_address: Address,
    registry: &R,
    worker_queue: &W,
    chain_client: &T,
    event_source: &E,
    rx_stop: &mut mpsc::Receiver<()>,
) -> Result<DrainOutcome, ()>
where
    R: AgreementRegistry + ChainListenerStateRegistry + PendingCancellationRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    E: ChainEventSource,
    T: ChainClient + Send + Sync,
{
    // Read-side cursor backs off by `reorg_buffer_blocks` so a
    // reorg that moves a state change across the boundary is still
    // re-read. The committed cursor only moves forward; idempotent
    // reconcile makes the duplicate work a no-op. The keyset `id`
    // is intentionally dropped on backoff (the resume must re-read
    // every row in the buffer window, not just the tail of the
    // tied block at `cursor.block`).
    let mut effective_cursor = if cursor.block > 0
        && reorg_buffer_blocks > 0
        && cursor.block > u64::from(reorg_buffer_blocks)
    {
        Cursor::at_block(cursor.block.saturating_sub(u64::from(reorg_buffer_blocks)))
    } else {
        cursor.clone()
    };

    let drain_start = std::time::Instant::now();
    let mut pages_per_tick: u32 = 0;
    let mut pinned_block: Option<u64> = None;
    let mut drain_processed: u64 = 0;
    let mut drain_errors: u64 = 0;
    let mut drain_latest_block: u64;
    let mut drain_latest_timestamp: Option<u64>;
    let mut drain_stopped = false;

    'drain: loop {
        let result = match event_source
            .get_changed_agreements(&effective_cursor, pinned_block)
            .await
        {
            Ok(result) => {
                if *consecutive_failures > 0 {
                    tracing::info!(
                        recovered_after = *consecutive_failures,
                        "chain listener recovered from consecutive fetch failures"
                    );
                }
                *consecutive_failures = 0;
                result
            }
            Err(err) => {
                *consecutive_failures = consecutive_failures.saturating_add(1);
                if *consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    tracing::error!(
                        error = %err,
                        consecutive_failures = *consecutive_failures,
                        pages_drained = pages_per_tick,
                        "Too many consecutive failures fetching changed agreements"
                    );
                } else {
                    tracing::warn!(
                        error = %err,
                        consecutive_failures = *consecutive_failures,
                        pages_drained = pages_per_tick,
                        "Failed to fetch changed agreements from subgraph"
                    );
                }
                return Err(());
            }
        };

        // Pin to page 1's snapshot so later pages in this drain
        // see the same chain state.
        if pinned_block.is_none() {
            pinned_block = Some(result.latest_block);
        }

        drain_latest_block = result.latest_block;
        drain_latest_timestamp = result.latest_block_timestamp;
        let new_cursor = result.cursor.clone();
        let new_timestamp = result.latest_block_timestamp;
        let count = result.snapshots.len();

        // Page 1 is the only meaningful sample for stall detection;
        // pinned pages return the same `latest_block` by construction.
        if pages_per_tick == 0 {
            if result.latest_block == *last_subgraph_head && result.latest_block > 0 {
                *stall_count += 1;
                if *stall_count == STALL_ERROR_THRESHOLD {
                    tracing::error!(
                        subgraph_head = result.latest_block,
                        stall_polls = *stall_count,
                        "Subgraph appears stalled — on-chain events may be delayed"
                    );
                } else if *stall_count == STALL_WARN_THRESHOLD {
                    tracing::warn!(
                        subgraph_head = result.latest_block,
                        stall_polls = *stall_count,
                        "Subgraph has not advanced, may be paused or behind"
                    );
                }
            } else if *stall_count > 0 && result.latest_block > *last_subgraph_head {
                tracing::info!(
                    previous_head = *last_subgraph_head,
                    new_head = result.latest_block,
                    stall_polls = *stall_count,
                    "Subgraph recovered from stall"
                );
                *stall_count = 0;
            }
            *last_subgraph_head = result.latest_block;
        }

        tracing::debug!(
            from_block = effective_cursor.block + 1,
            to_block = result.latest_block,
            snapshot_count = count,
            page = pages_per_tick + 1,
            "Reconciling agreement snapshots (page)"
        );

        // Single-shot batch lookup for every agreement referenced in this
        // page. Snapshots whose id is not in the local registry (e.g.
        // another payer's) drop out as `None` below.
        let snapshot_ids: Vec<IndexingAgreementId> =
            result.snapshots.iter().map(|s| s.agreement_id).collect();
        let mut agreements_by_id =
            match registry.get_indexing_agreements_by_ids(&snapshot_ids).await {
                Ok(m) => m,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        page = pages_per_tick + 1,
                        snapshots = snapshot_ids.len(),
                        "Failed to batch-fetch agreements for page; counting snapshots as errors"
                    );
                    drain_errors += snapshot_ids.len() as u64;
                    std::collections::HashMap::new()
                }
            };

        let mut prepared: Vec<PreparedReconciliation> = Vec::with_capacity(count);
        for snapshot in result.snapshots {
            if rx_stop.try_recv().is_ok() {
                drain_stopped = true;
                break 'drain;
            }

            let agreement = agreements_by_id.remove(&snapshot.agreement_id);
            match prepare_reconciliation(&snapshot, agreement, worker_queue, signer_address).await {
                Ok(Some(prep)) => prepared.push(prep),
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        agreement_id = %snapshot.agreement_id,
                        "Failed to prepare reconciliation for snapshot"
                    );
                    drain_errors += 1;
                }
            }
        }

        // On batch error the tx rolls back; the next tick re-reads
        // these rows via the reorg buffer. The successful path
        // pre-fills every input id in `outcomes` (with `default()`
        // for rows whose CAS guard didn't match), so a missing id
        // is an unambiguous signal that the whole batch failed.
        let items: Vec<ReconciliationItem> = prepared.iter().map(|p| p.item).collect();
        let outcomes = if items.is_empty() {
            std::collections::HashMap::new()
        } else {
            match registry.apply_reconciliation_batch(&items).await {
                Ok(o) => o,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        page = pages_per_tick + 1,
                        items = items.len(),
                        "Batched apply_reconciliation failed; counting page items as errors"
                    );
                    drain_errors += items.len() as u64;
                    std::collections::HashMap::new()
                }
            }
        };

        for prep in prepared {
            // Items absent from outcomes are batch-failures, already
            // counted as errors above — finalize would double-count.
            let Some(outcome) = outcomes.get(&prep.agreement.id).copied() else {
                continue;
            };
            match finalize_reconciliation(&prep, outcome, registry, chain_client).await {
                Ok(()) => drain_processed += 1,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        agreement_id = %prep.agreement.id,
                        "Failed to finalize reconciliation outcome"
                    );
                    drain_errors += 1;
                }
            }
        }

        pages_per_tick += 1;

        // Persisted timestamp never decreases so a rolled-back subgraph
        // can't drag expiration's clock backwards. Forward advances are
        // bounded by wall-clock elapsed so a hostile response that stays
        // just under the per-poll skew tolerance can't ratchet the
        // persisted timestamp forward faster than real time.
        let now_instant = std::time::Instant::now();
        let wall_elapsed_secs = now_instant
            .duration_since(*last_chain_ts_persist_wall)
            .as_secs();
        let max_chain_advance = wall_elapsed_secs.saturating_add(chain_ts_drift_tolerance_secs);
        let ratchet_timestamp = match (new_timestamp, *last_persisted_timestamp) {
            (Some(new_ts), Some(cached_ts)) if new_ts < cached_ts => {
                tracing::warn!(
                    new_timestamp_secs = new_ts,
                    persisted_timestamp_secs = cached_ts,
                    "Subgraph reported timestamp below persisted value; ignoring (ratchet up only)"
                );
                Some(cached_ts)
            }
            (Some(new_ts), Some(cached_ts)) => {
                let max_allowed = cached_ts.saturating_add(max_chain_advance);
                if new_ts > max_allowed {
                    tracing::warn!(
                        event = "chain_ts_drift_capped",
                        new_timestamp_secs = new_ts,
                        persisted_timestamp_secs = cached_ts,
                        max_allowed_secs = max_allowed,
                        wall_elapsed_secs,
                        "Chain timestamp advance exceeds wall-clock elapsed; capping"
                    );
                    Some(max_allowed)
                } else {
                    Some(new_ts)
                }
            }
            (Some(new_ts), None) => Some(new_ts),
            (None, _) => *last_persisted_timestamp,
        };

        // Persist per-page so a crash mid-drain replays at most
        // one page. Skip the write when neither cursor nor
        // ratcheted timestamp moved.
        let advance_cursor = *cursor < new_cursor;
        let timestamp_changed = ratchet_timestamp != *last_persisted_timestamp;
        if advance_cursor || timestamp_changed {
            let cursor_to_persist = if advance_cursor {
                &new_cursor
            } else {
                &*cursor
            };
            match registry
                .update_chain_listener_state(chain_id, cursor_to_persist, ratchet_timestamp)
                .await
            {
                Ok(()) => {
                    *last_persisted_timestamp = ratchet_timestamp;
                    *last_chain_ts_persist_wall = now_instant;
                }
                Err(err) => {
                    tracing::error!(error = %err, "Failed to update chain listener state");
                }
            }
        }
        if advance_cursor {
            *cursor = new_cursor.clone();
            effective_cursor = new_cursor;
        } else if count == 0 {
            // Cursor didn't advance and the page was empty —
            // nothing to do this tick.
            break 'drain;
        } else {
            // Held-back cursor (parse failure path); reconcile
            // already ran on what we did read, retry next tick.
            break 'drain;
        }

        // Drained?
        if count < super::chain_events::SUBGRAPH_PAGE_SIZE {
            break 'drain;
        }

        if pages_per_tick >= MAX_PAGES_PER_DRAIN {
            tracing::warn!(
                pages_per_tick,
                max_pages = MAX_PAGES_PER_DRAIN,
                "Drain hit per-tick page ceiling; deferring remaining snapshots to next tick"
            );
            break 'drain;
        }
    }

    let duration_ms: u64 = drain_start
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);

    // Positive = subgraph trailing wall clock; growing = falling
    // behind. Sourced from `_meta.block.timestamp`.
    let subgraph_lag_seconds: i64 = drain_latest_timestamp
        .map(|ts| (dipper_core::time::now_secs() as i64).saturating_sub(ts as i64))
        .unwrap_or(0);

    Ok(DrainOutcome {
        pages: pages_per_tick,
        processed: drain_processed,
        errors: drain_errors,
        latest_block: drain_latest_block,
        duration_ms,
        subgraph_lag_seconds,
        stopped: drain_stopped,
    })
}

/// Decision computed for one snapshot, plus the resolved agreement and
/// indexer address that the post-apply log/fan-out paths need.
struct PreparedReconciliation {
    item: ReconciliationItem,
    agreement: IndexingAgreement,
    indexer: Address,
}

/// Compute the apply decision for one snapshot and run any side effects
/// that don't belong inside the apply transaction. Returns `None` when
/// no DB write is needed. Caller pre-fetches the agreement (in batch for
/// the chain_listener loop, single-row for tests) and passes `None`
/// when the snapshot is for an agreement we don't track locally.
async fn prepare_reconciliation<W>(
    snapshot: &AgreementStateSnapshot,
    agreement: Option<IndexingAgreement>,
    worker_queue: &W,
    signer_address: Address,
) -> anyhow::Result<Option<PreparedReconciliation>>
where
    W: WorkerQueue,
{
    tracing::debug!(
        agreement_id = %snapshot.agreement_id,
        indexer = %snapshot.indexer,
        state = ?snapshot.state,
        last_state_change_block = snapshot.last_state_change_block,
        "Preparing reconciliation against snapshot"
    );

    let agreement = match agreement {
        Some(a) => a,
        None => {
            tracing::debug!(
                agreement_id = %snapshot.agreement_id,
                "Agreement not found (may be from another payer)"
            );
            return Ok(None);
        }
    };

    // Defensive guard against a state proposal-first dispatch makes
    // impossible: dipper gates `offer()` submission on local status, and
    // the contract requires an offer before allowing on-chain accept, so
    // a Rejected agreement should never reach Accepted. The `warn!` below
    // is therefore an alarm — investigate the offer-submission path
    // (race with gRPC rejection or stale read), not the indexer.
    // Skipped when remote is already canceled because the cancel tx
    // would revert against canceled contract state.
    if matches!(agreement.status, IndexingAgreementStatus::Rejected)
        && snapshot.state.reached_accepted()
        && !snapshot.state.is_canceled()
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            indexer = %snapshot.indexer,
            "Rejected agreement accepted on-chain, queuing cancellation"
        );
        worker_queue
            .cancel_rejected_agreement_on_chain(agreement.id)
            .await?;
    }

    // Both transitions are applied atomically downstream so the
    // Accept-then-Cancel-in-one-snapshot path can't leak an intermediate
    // AcceptedOnChain to concurrent readers.
    let apply_accept = matches!(
        agreement.status,
        IndexingAgreementStatus::Created | IndexingAgreementStatus::Expired,
    ) && snapshot.state.reached_accepted();

    let already_terminal_cancel = matches!(
        agreement.status,
        IndexingAgreementStatus::CanceledByRequester | IndexingAgreementStatus::CanceledByIndexer,
    );
    let cancel_kind = if snapshot.state.is_canceled() && !already_terminal_cancel {
        Some(if snapshot.canceled_by == signer_address {
            CancelKind::ByRequester
        } else {
            CancelKind::ByIndexer
        })
    } else {
        None
    };

    if apply_accept || cancel_kind.is_some() {
        Ok(Some(PreparedReconciliation {
            item: ReconciliationItem {
                agreement_id: agreement.id,
                apply_accept,
                cancel: cancel_kind,
            },
            agreement,
            indexer: snapshot.indexer,
        }))
    } else {
        if already_terminal_cancel && snapshot.state.is_canceled() {
            tracing::debug!(
                agreement_id = %agreement.id,
                status = %agreement.status,
                "Agreement already canceled, ignoring snapshot"
            );
        }
        Ok(None)
    }
}

/// Log the transition that landed and, on fresh accepts, fan out the
/// linked pending cancellations.
async fn finalize_reconciliation<R, T>(
    prep: &PreparedReconciliation,
    outcome: crate::registry::ReconciliationOutcome,
    registry: &R,
    chain_client: &T,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    T: ChainClient,
{
    if outcome.did_accept {
        let (old_status, reason) = match prep.agreement.status {
            IndexingAgreementStatus::Expired => ("EXPIRED", "recovered_expired_on_chain"),
            _ => ("CREATED", "accepted_on_chain"),
        };
        tracing::info!(
            agreement_id = %prep.agreement.id,
            indexing_request_id = %prep.agreement.indexing_request_id,
            old_status,
            new_status = "ACCEPTED_ON_CHAIN",
            reason,
            "agreement state transition"
        );
    }

    if outcome.did_cancel {
        match prep.item.cancel {
            Some(CancelKind::ByRequester) => tracing::info!(
                agreement_id = %prep.agreement.id,
                "Agreement marked as CanceledByRequester (on-chain confirmation)"
            ),
            Some(CancelKind::ByIndexer) => tracing::info!(
                agreement_id = %prep.agreement.id,
                indexer = %prep.indexer,
                "Agreement marked as CanceledByIndexer"
            ),
            None => {}
        }
    }

    // Gated on `did_accept` so it only fires on a fresh AcceptedOnChain
    // write — repeating it on a CAS no-op would re-enqueue work that
    // already ran in a prior poll.
    if outcome.did_accept {
        execute_pending_cancellations(&prep.agreement.id, registry, chain_client).await?;
    }

    Ok(())
}

/// Reconcile a single agreement snapshot against dipper's local DB.
///
/// Compares the snapshot's remote state to the local `IndexingAgreementStatus`
/// and applies whatever transitions the diff implies. See the module-level
/// transition table for the full mapping.
#[cfg(test)]
async fn reconcile_agreement<R, W, T>(
    snapshot: &AgreementStateSnapshot,
    registry: &R,
    worker_queue: &W,
    chain_client: &T,
    signer_address: Address,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    W: WorkerQueue,
    T: ChainClient,
{
    let agreement = registry
        .get_indexing_agreement_by_id(&snapshot.agreement_id)
        .await?;
    let Some(prep) =
        prepare_reconciliation(snapshot, agreement, worker_queue, signer_address).await?
    else {
        return Ok(());
    };

    let outcome = registry
        .apply_reconciliation(&prep.agreement.id, prep.item.apply_accept, prep.item.cancel)
        .await?;

    finalize_reconciliation(&prep, outcome, registry, chain_client).await
}

/// Execute pending cancellations linked to a newly-accepted agreement.
///
/// Called from the Created -> AcceptedOnChain and Expired -> AcceptedOnChain
/// transitions. For each pending cancellation, fires
/// `cancelIndexingAgreementByPayer` against the RecurringCollector contract,
/// then flips the dipper DB row to `CanceledByRequester`. Each pending row is
/// deleted individually after both steps succeed; transient failures retain
/// the record so the next reconcile pass can retry.
async fn execute_pending_cancellations<R, T>(
    agreement_id: &IndexingAgreementId,
    registry: &R,
    chain_client: &T,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry,
    T: ChainClient,
{
    let pending = registry
        .get_pending_cancellations_by_new_agreement(*agreement_id)
        .await?;

    if pending.is_empty() {
        return Ok(());
    }

    let mut transient_failures: u32 = 0;

    for cancellation in &pending {
        if registry
            .get_indexing_agreement_by_id(&cancellation.old_agreement_id)
            .await?
            .is_none()
        {
            tracing::warn!(
                old_agreement_id = %cancellation.old_agreement_id,
                "Pending cancellation references non-existent agreement, cleaning up"
            );
            registry
                .delete_pending_cancellation(*agreement_id, cancellation.old_agreement_id)
                .await?;
            continue;
        }

        match chain_client
            .cancel_indexing_agreement_by_payer(cancellation.old_agreement_id.as_bytes())
            .await
        {
            Ok(Some(tx_hash)) => {
                tracing::info!(
                    new_agreement_id = %agreement_id,
                    old_agreement_id = %cancellation.old_agreement_id,
                    %tx_hash,
                    "Submitted on-chain cancellation for replaced agreement"
                );
            }
            Ok(None) => {
                tracing::info!(
                    new_agreement_id = %agreement_id,
                    old_agreement_id = %cancellation.old_agreement_id,
                    "Agreement already canceled on-chain; proceeding with local cleanup"
                );
            }
            Err(err) => {
                tracing::warn!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    error = %err,
                    "On-chain cancel failed, retaining pending cancellation for retry"
                );
                transient_failures += 1;
                continue;
            }
        }

        match registry
            .mark_indexing_agreement_as_canceled_by_requester(&cancellation.old_agreement_id)
            .await
        {
            Ok(()) => {}
            Err(crate::registry::Error::NoRecordsUpdated) => {
                tracing::debug!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    "Old agreement already in terminal state, skipping local cancel flip"
                );
                registry
                    .delete_pending_cancellation(*agreement_id, cancellation.old_agreement_id)
                    .await?;
                continue;
            }
            Err(err) => {
                tracing::error!(
                    old_agreement_id = %cancellation.old_agreement_id,
                    error = %err,
                    "On-chain cancel succeeded but DB update failed, retaining pending row"
                );
                transient_failures += 1;
                continue;
            }
        }

        registry
            .delete_pending_cancellation(*agreement_id, cancellation.old_agreement_id)
            .await?;

        tracing::info!(
            new_agreement_id = %agreement_id,
            old_agreement_id = %cancellation.old_agreement_id,
            "Canceled old agreement on-chain and in dipper DB after replacement confirmed"
        );
    }

    if transient_failures > 0 {
        anyhow::bail!(
            "{transient_failures} pending cancellation(s) failed for agreement {}; \
             records retained for retry",
            agreement_id,
        );
    }

    Ok(())
}

/// Recover stranded pending cancellations from a partial-progress crash.
///
/// `execute_pending_cancellations` is invoked from `reconcile_agreement`
/// only on a fresh `Created`/`Expired` -> `AcceptedOnChain` transition.
/// If the process crashes after the local row is committed as
/// `AcceptedOnChain` but before the cancellation fan-out completes, the
/// pending_cancellation rows are left behind and the cursor advances
/// past the snapshot that would have re-triggered them. This sweep runs
/// once per chain_listener poll and re-feeds any such IDs through
/// `execute_pending_cancellations`, which is idempotent on already-canceled
/// old agreements and on already-deleted pending rows.
///
/// Per-orphan failures are logged and swallowed so one stuck cancellation
/// cannot block the rest of the sweep.
async fn sweep_executable_pending_cancellations<R, T>(registry: &R, chain_client: &T)
where
    R: AgreementRegistry + PendingCancellationRegistry,
    T: ChainClient,
{
    let targets = match registry
        .list_executable_pending_cancellations(SWEEP_BATCH_SIZE)
        .await
    {
        Ok(targets) => targets,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to list executable pending cancellations for sweep"
            );
            return;
        }
    };

    if targets.is_empty() {
        return;
    }

    tracing::debug!(
        count = targets.len(),
        "Sweeping pending cancellations on AcceptedOnChain agreements"
    );

    for new_agreement_id in targets {
        if let Err(err) =
            execute_pending_cancellations(&new_agreement_id, registry, chain_client).await
        {
            tracing::warn!(
                error = %err,
                new_agreement_id = %new_agreement_id,
                "Sweep failed to execute pending cancellations; will retry next poll"
            );
        }
    }
}

/// Trait for chain listener state persistence
#[async_trait::async_trait]
pub trait ChainListenerStateRegistry {
    /// Get the current chain listener state for a chain
    async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> Result<Option<ChainListenerState>, crate::registry::Error>;

    /// Update the chain listener state
    async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        cursor: &Cursor,
        last_processed_block_timestamp: Option<u64>,
    ) -> Result<(), crate::registry::Error>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};
    use time::OffsetDateTime;
    use url::Url;

    use super::{super::chain_events::AgreementState, *};
    use crate::registry::{
        AgreementFeeRate, Indexer, IndexingAgreement, IndexingAgreementTerms as Terms,
        IndexingAgreementTermsMetadata as TermsMetadata, Result as RegistryResult,
    };

    fn test_terms() -> Terms {
        Terms {
            payer: Address::ZERO,
            service_provider: Address::ZERO,
            data_service: Address::ZERO,
            deadline: 0,
            ends_at: 0,
            max_initial_tokens: thegraph_core::alloy::primitives::U256::ZERO,
            max_ongoing_tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
            min_seconds_per_collection: 0,
            max_seconds_per_collection: 0,
            conditions: 0,
            metadata: TermsMetadata {
                tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
                tokens_per_entity_per_second: thegraph_core::alloy::primitives::U256::ZERO,
                subgraph_deployment_id: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                protocol_network: ChainId::from(42161u64),
                chain_id: ChainId::from(1u64),
            },
        }
    }

    fn make_snapshot(
        agreement_id: IndexingAgreementId,
        state: AgreementState,
        canceled_by: Address,
    ) -> AgreementStateSnapshot {
        AgreementStateSnapshot {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            state,
            canceled_by,
            last_state_change_block: 100,
        }
    }

    #[test]
    fn test_handle_clone() {
        let (tx, _rx) = mpsc::channel(1);
        let handle = Handle { tx_stop: tx };
        let _cloned = handle.clone();
    }

    // Mock registry for testing
    #[derive(Clone, Default)]
    struct MockRegistry {
        state: Arc<Mutex<MockRegistryState>>,
    }

    #[derive(Default)]
    struct MockRegistryState {
        agreements: std::collections::HashMap<IndexingAgreementId, IndexingAgreement>,
        marked_accepted_on_chain: Vec<IndexingAgreementId>,
        marked_canceled_by_requester: Vec<IndexingAgreementId>,
        marked_canceled_by_indexer: Vec<IndexingAgreementId>,
        pending_cancellations: std::collections::HashMap<
            IndexingAgreementId,
            Vec<crate::registry::PendingCancellation>,
        >,
        deleted_pending_cancellations: Vec<(IndexingAgreementId, IndexingAgreementId)>,
        fail_cancel_for: std::collections::HashSet<IndexingAgreementId>,
        chain_listener_state_updates: Vec<(Cursor, Option<u64>)>,
        initial_last_processed_block: u64,
        fail_batch: bool,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self::default()
        }

        fn add_agreement(&self, id: IndexingAgreementId, status: IndexingAgreementStatus) {
            let terms = test_terms();
            let agreement = IndexingAgreement {
                id,
                nonce_uuid: uuid::Uuid::now_v7(),
                status,
                indexer: Indexer {
                    id: "0x1234567890123456789012345678901234567890"
                        .parse()
                        .unwrap(),
                    url: "http://indexer.test".parse().unwrap(),
                },
                indexing_request_id: IndexingRequestId::new(),
                terms,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                last_block_height: None,
                last_progress_at: None,
                rejection_reason: None,
            };
            self.state.lock().unwrap().agreements.insert(id, agreement);
        }

        fn add_pending_cancellation(
            &self,
            new_agreement_id: IndexingAgreementId,
            old_agreement_id: IndexingAgreementId,
            indexing_request_id: IndexingRequestId,
        ) {
            let pc = crate::registry::PendingCancellation {
                old_agreement_id,
                indexing_request_id,
            };
            self.state
                .lock()
                .unwrap()
                .pending_cancellations
                .entry(new_agreement_id)
                .or_default()
                .push(pc);
        }

        fn fail_cancel_for(&self, id: IndexingAgreementId) {
            self.state.lock().unwrap().fail_cancel_for.insert(id);
        }

        fn was_marked_accepted_on_chain(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_accepted_on_chain
                .contains(id)
        }

        fn was_marked_canceled_by_requester(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_requester
                .contains(id)
        }

        fn was_marked_canceled_by_indexer(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_indexer
                .contains(id)
        }

        fn was_pending_cancellation_deleted(
            &self,
            new_id: &IndexingAgreementId,
            old_id: &IndexingAgreementId,
        ) -> bool {
            self.state
                .lock()
                .unwrap()
                .deleted_pending_cancellations
                .contains(&(*new_id, *old_id))
        }

        fn set_initial_last_processed_block(&self, block: u64) {
            self.state.lock().unwrap().initial_last_processed_block = block;
        }

        fn chain_listener_state_updates(&self) -> Vec<(Cursor, Option<u64>)> {
            self.state
                .lock()
                .unwrap()
                .chain_listener_state_updates
                .clone()
        }

        fn set_fail_batch(&self, fail: bool) {
            self.state.lock().unwrap().fail_batch = fail;
        }
    }

    #[async_trait::async_trait]
    impl AgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<Option<IndexingAgreement>> {
            Ok(self.state.lock().unwrap().agreements.get(id).cloned())
        }

        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(std::collections::HashMap::new())
        }

        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _signer_lookback_minutes: i32,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(std::collections::HashMap::new())
        }

        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn register_new_indexing_agreement(
            &self,
            _params: crate::registry::NewAgreementParams,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn register_agreement_with_pending_cancellation(
            &self,
            _params: crate::registry::NewAgreementParams,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn mark_indexing_agreement_as_delivery_failed(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn update_offer_tx_hash(
            &self,
            _id: &IndexingAgreementId,
            _tx_hash: &[u8; 32],
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_cancel_for.contains(id) {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "simulated transient failure".into(),
                    )),
                ));
            }
            state.marked_canceled_by_requester.push(*id);
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_indexer(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_indexer
                .push(*id);
            Ok(())
        }

        async fn apply_reconciliation(
            &self,
            id: &IndexingAgreementId,
            apply_accept: bool,
            cancel: Option<crate::registry::CancelKind>,
        ) -> RegistryResult<crate::registry::ReconciliationOutcome> {
            let mut state = self.state.lock().unwrap();

            if let Some(kind_check) = cancel {
                let _ = kind_check;
                if state.fail_cancel_for.contains(id) {
                    return Err(crate::registry::Error::BackendError(
                        dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                            "simulated transient failure".into(),
                        )),
                    ));
                }
            }

            let original_status = state.agreements.get(id).map(|a| a.status);

            let did_accept = apply_accept
                && matches!(
                    original_status,
                    Some(IndexingAgreementStatus::Created | IndexingAgreementStatus::Expired),
                );

            // The cancel UPDATE in the real tx sees the post-accept status
            // because the accept's UPDATE landed first inside the same
            // transaction.
            let effective_status_for_cancel = if did_accept {
                Some(IndexingAgreementStatus::AcceptedOnChain)
            } else {
                original_status
            };

            let did_cancel = match cancel {
                Some(crate::registry::CancelKind::ByRequester) => matches!(
                    effective_status_for_cancel,
                    Some(
                        IndexingAgreementStatus::Created
                            | IndexingAgreementStatus::AcceptedOnChain
                            | IndexingAgreementStatus::Rejected,
                    ),
                ),
                Some(crate::registry::CancelKind::ByIndexer) => matches!(
                    effective_status_for_cancel,
                    Some(IndexingAgreementStatus::AcceptedOnChain),
                ),
                None => false,
            };

            // Roll back only when an accept landed in this tx but the
            // paired cancel matched no row. When both writes matched no
            // row, commit the empty tx and return Ok(no-op).
            if did_accept && !did_cancel && cancel.is_some() {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }

            if did_accept {
                state.marked_accepted_on_chain.push(*id);
            }
            if did_cancel {
                match cancel.expect("did_cancel implies cancel is Some") {
                    crate::registry::CancelKind::ByRequester => {
                        state.marked_canceled_by_requester.push(*id);
                    }
                    crate::registry::CancelKind::ByIndexer => {
                        state.marked_canceled_by_indexer.push(*id);
                    }
                }
            }

            Ok(crate::registry::ReconciliationOutcome {
                did_accept,
                did_cancel,
            })
        }

        async fn apply_reconciliation_batch(
            &self,
            items: &[crate::registry::ReconciliationItem],
        ) -> RegistryResult<
            std::collections::HashMap<IndexingAgreementId, crate::registry::ReconciliationOutcome>,
        > {
            if self.state.lock().unwrap().fail_batch {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "forced batch failure (test)".into(),
                    )),
                ));
            }
            let mut outcomes = std::collections::HashMap::with_capacity(items.len());
            for item in items {
                let outcome = self
                    .apply_reconciliation(&item.agreement_id, item.apply_accept, item.cancel)
                    .await?;
                outcomes.insert(item.agreement_id, outcome);
            }
            Ok(outcomes)
        }

        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
            _chain_timestamp: u64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn mark_indexing_agreement_as_expired(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_rejected(
            &self,
            _id: &IndexingAgreementId,
            _rejection_reason: Option<&str>,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn get_accepted_on_chain_agreements(
            &self,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn update_agreement_sync_progress(
            &self,
            _id: &IndexingAgreementId,
            _block_height: u64,
            _progress_at: time::OffsetDateTime,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn count_active_agreements_by_deployment(
            &self,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>> {
            let mut counts: std::collections::HashMap<DeploymentId, usize> =
                std::collections::HashMap::new();
            for agreement in self.state.lock().unwrap().agreements.values() {
                if matches!(
                    agreement.status,
                    IndexingAgreementStatus::Created | IndexingAgreementStatus::AcceptedOnChain,
                ) {
                    *counts
                        .entry(agreement.terms.metadata.subgraph_deployment_id)
                        .or_insert(0) += 1;
                }
            }
            Ok(counts)
        }

        async fn mark_indexing_agreement_as_abandoned(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreement> {
            Err(crate::registry::Error::NoRecordsUpdated)
        }

        async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl PendingCancellationRegistry for MockRegistry {
        async fn get_pending_cancellations_by_new_agreement(
            &self,
            new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<Vec<crate::registry::PendingCancellation>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .pending_cancellations
                .get(&new_agreement_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn delete_pending_cancellations_by_new_agreement(
            &self,
            new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state
                .lock()
                .unwrap()
                .pending_cancellations
                .remove(&new_agreement_id);
            Ok(())
        }

        async fn delete_pending_cancellation(
            &self,
            new_agreement_id: IndexingAgreementId,
            old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if let Some(pcs) = state.pending_cancellations.get_mut(&new_agreement_id) {
                pcs.retain(|pc| pc.old_agreement_id != old_agreement_id);
            }
            state
                .deleted_pending_cancellations
                .push((new_agreement_id, old_agreement_id));
            Ok(())
        }

        async fn list_executable_pending_cancellations(
            &self,
            limit: i64,
        ) -> RegistryResult<Vec<IndexingAgreementId>> {
            let state = self.state.lock().unwrap();
            let mut ids: Vec<IndexingAgreementId> = state
                .pending_cancellations
                .iter()
                .filter(|(_, pcs)| !pcs.is_empty())
                .filter_map(|(new_id, _)| {
                    state
                        .agreements
                        .get(new_id)
                        .filter(|a| matches!(a.status, IndexingAgreementStatus::AcceptedOnChain))
                        .map(|_| *new_id)
                })
                .collect();
            ids.sort();
            ids.truncate(limit.max(0) as usize);
            Ok(ids)
        }
    }

    // Mock worker queue
    #[derive(Clone, Default)]
    struct MockWorkerQueue {
        cancel_jobs: Arc<Mutex<Vec<IndexingAgreementId>>>,
    }

    impl MockWorkerQueue {
        fn was_cancellation_queued(&self, id: &IndexingAgreementId) -> bool {
            self.cancel_jobs.lock().unwrap().contains(id)
        }
    }

    /// Minimal `ChainClient` mock for chain_listener tests. Records every
    /// on-chain cancel attempt. Tests can mark specific agreements as
    /// already-canceled-on-chain (cancel returns `Ok(None)`); unmarked
    /// agreements get a successful `Ok(Some(zero))`. `post_offer` is not
    /// exercised in this module.
    #[derive(Clone, Default)]
    struct MockChainClient {
        cancels: Arc<Mutex<Vec<[u8; 16]>>>,
        already_canceled: Arc<Mutex<Vec<[u8; 16]>>>,
    }

    impl MockChainClient {
        fn was_on_chain_cancel_attempted(&self, id: &IndexingAgreementId) -> bool {
            self.cancels.lock().unwrap().contains(id.as_bytes())
        }

        fn mark_already_canceled_on_chain(&self, id: &IndexingAgreementId) {
            self.already_canceled.lock().unwrap().push(*id.as_bytes());
        }
    }

    #[async_trait::async_trait]
    impl crate::chain_client::ChainClient for MockChainClient {
        async fn cancel_indexing_agreement_by_payer(
            &self,
            agreement_id: &[u8; 16],
        ) -> Result<
            Option<thegraph_core::alloy::primitives::B256>,
            crate::chain_client::ChainClientError,
        > {
            self.cancels.lock().unwrap().push(*agreement_id);
            // Test mock surfaces the call to the recorder; the dummy hash
            // distinguishes "submitted" from "already-canceled" (Ok(None)),
            // letting tests assert either path explicitly per-agreement.
            if self.already_canceled.lock().unwrap().contains(agreement_id) {
                Ok(None)
            } else {
                Ok(Some(thegraph_core::alloy::primitives::B256::ZERO))
            }
        }

        async fn post_offer(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<
            Option<thegraph_core::alloy::primitives::B256>,
            crate::chain_client::ChainClientError,
        > {
            Ok(None)
        }
    }

    #[async_trait::async_trait]
    impl crate::worker::service::WorkerQueue for MockWorkerQueue {

        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }



        async fn reassess_indexing_request(
            &self,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            agreement_id: IndexingAgreementId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            self.cancel_jobs.lock().unwrap().push(agreement_id);
            Ok(dipper_pgmq::JobId::default())
        }

        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            Ok(dipper_pgmq::JobId::default())
        }
    }

    // -- reconcile_agreement tests --

    #[tokio::test]
    async fn test_reconcile_transitions_created_to_accepted() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            Address::ZERO,
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_queues_cancellation_for_rejected() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            Address::ZERO,
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_rejected_already_canceled_skips_queue_and_marks_local_cancel() {
        // The Rejected agreement was canceled on-chain between polls. Dipper
        // should not queue another cancel job (it would just revert against
        // the already-canceled contract state), and step 2 should drive the
        // local status from Rejected straight to the terminal cancel matching
        // the canceler address.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            signer_address,
        )
        .await;

        assert!(result.is_ok());
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_ignores_unknown_agreement() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        // Don't add the agreement to the registry

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            Address::ZERO,
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!worker_queue.was_cancellation_queued(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_recovers_expired_agreement() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Expired);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id, request_id);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            Address::ZERO,
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_marks_canceled_by_indexer() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_address: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            indexer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            signer_address,
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_marks_canceled_by_requester() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            signer_address,
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_ignores_already_canceled() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByIndexer);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            Address::ZERO,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            signer_address,
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_no_op_when_local_status_blocks_cancel_filter() {
        // Local is in a terminal-but-not-cancel status (DeliveryFailed),
        // remote snapshot says canceled. The chain_listener's Rust-side
        // already_terminal_cancel guard does not catch DeliveryFailed,
        // so apply_reconciliation is invoked with apply_accept=false and
        // cancel=Some(...). The cancel UPDATE matches no row because
        // DeliveryFailed is not in the allowed_from list. The function
        // must commit the empty tx and return Ok with both flags false,
        // so the chain_listener treats the snapshot as a successful no-op
        // rather than incrementing its `errors` counter.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::DeliveryFailed);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            signer_address,
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_applies_accept_then_cancel_in_one_snapshot() {
        // Transient case: dipper polls after both the accept and cancel
        // landed on-chain. Local is still Created, remote is CanceledByPayer.
        // We should run the acceptance-side bookkeeping (pending cancellations)
        // AND mark the agreement as CanceledByRequester.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let worker_queue = MockWorkerQueue::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id, request_id);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &worker_queue,
            &chain_client,
            signer_address,
        )
        .await;

        assert!(result.is_ok());
        // Accepted-side bookkeeping ran
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
        // Cancellation applied on top
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
    }

    // -- execute_pending_cancellations tests --

    #[tokio::test]
    async fn test_pending_cancellations_all_succeed_records_deleted() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id_1 = IndexingAgreementId::from_bytes(rand::random());
        let old_id_2 = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_1, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_2, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id_1, request_id);
        registry.add_pending_cancellation(new_id, old_id_2, request_id);

        let result = execute_pending_cancellations(&new_id, &registry, &chain_client).await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_requester(&old_id_1));
        assert!(registry.was_marked_canceled_by_requester(&old_id_2));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_1));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_2));
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_pending_cancellations_transient_failure_retains_record() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_ok = IndexingAgreementId::from_bytes(rand::random());
        let old_fail = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_ok, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_fail, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_ok, request_id);
        registry.add_pending_cancellation(new_id, old_fail, request_id);
        registry.fail_cancel_for(old_fail);

        let result = execute_pending_cancellations(&new_id, &registry, &chain_client).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("1 pending cancellation(s) failed"),
            "unexpected error: {err_msg}"
        );

        assert!(registry.was_marked_canceled_by_requester(&old_ok));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_ok));

        assert!(!registry.was_marked_canceled_by_requester(&old_fail));
        assert!(!registry.was_pending_cancellation_deleted(&new_id, &old_fail));

        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].old_agreement_id, old_fail);
    }

    #[tokio::test]
    async fn test_pending_cancellations_nonexistent_agreement_cleans_up() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id, request_id);
        // old_id never added -- simulates a stale pending cancellation whose
        // referenced agreement no longer exists.

        let result = execute_pending_cancellations(&new_id, &registry, &chain_client).await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_pending_cancellations_empty_is_noop() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);

        let result = execute_pending_cancellations(&new_id, &registry, &chain_client).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pending_cancellations_already_canceled_on_chain_succeeds() {
        // Crash-recovery edge case: the cancel tx confirmed on-chain on a
        // prior pass, but dipper crashed before deleting the pending row.
        // On the next sweep the chain call surfaces as Ok(None) (the
        // SubgraphService contract reverts with IndexingAgreementNotActive;
        // the chain client translates that into "already canceled"). The
        // handler must still flip the local row to CanceledByRequester and
        // delete the pending row, not loop forever.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id, request_id);
        chain_client.mark_already_canceled_on_chain(&old_id);

        let result = execute_pending_cancellations(&new_id, &registry, &chain_client).await;

        assert!(
            result.is_ok(),
            "expected idempotent success, got {result:?}"
        );
        assert!(chain_client.was_on_chain_cancel_attempted(&old_id));
        assert!(registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_sweep_clears_already_canceled_pending_rows() {
        // The sweep is the long-lived recovery path. If the on-chain cancel
        // has already happened but the pending row survived, the sweep must
        // tear it down on the next poll cycle without an error.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id, request_id);
        chain_client.mark_already_canceled_on_chain(&old_id);

        sweep_executable_pending_cancellations(&registry, &chain_client).await;

        assert!(registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    // -- sweep_executable_pending_cancellations tests --

    #[tokio::test]
    async fn test_sweep_recovers_orphaned_pending_cancellations() {
        // Crash recovery scenario: a prior reconcile committed the new
        // agreement to AcceptedOnChain but execute_pending_cancellations
        // never ran (or crashed mid-fanout), so the pending row lingers
        // and the old agreement is still alive. The sweep must complete
        // the cancellation without needing another snapshot to arrive.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id, request_id);

        sweep_executable_pending_cancellations(&registry, &chain_client).await;

        assert!(registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_sweep_skips_when_new_agreement_not_accepted() {
        // The sweep must not act on pending rows whose new agreement is
        // still Created — those belong to the normal reconcile path and
        // running the cancellation early would prematurely kill the old
        // agreement before the replacement is on-chain.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(new_id, IndexingAgreementStatus::Created);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id, request_id);

        sweep_executable_pending_cancellations(&registry, &chain_client).await;

        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(!registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_sweep_no_orphans_is_noop() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();

        sweep_executable_pending_cancellations(&registry, &chain_client).await;
    }

    // -- notify wakeup integration test --

    #[async_trait::async_trait]
    impl ChainListenerStateRegistry for MockRegistry {
        async fn get_chain_listener_state(
            &self,
            _chain_id: u64,
        ) -> crate::registry::Result<Option<ChainListenerState>> {
            let last_processed_block = self.state.lock().unwrap().initial_last_processed_block;
            Ok(Some(ChainListenerState {
                _chain_id: 1337,
                last_processed_block,
                last_processed_id: None,
                last_processed_block_timestamp: None,
            }))
        }

        async fn update_chain_listener_state(
            &self,
            _chain_id: u64,
            cursor: &Cursor,
            last_processed_block_timestamp: Option<u64>,
        ) -> crate::registry::Result<()> {
            self.state
                .lock()
                .unwrap()
                .chain_listener_state_updates
                .push((cursor.clone(), last_processed_block_timestamp));
            Ok(())
        }
    }

    /// A mock event source that records when it was polled.
    struct TimingEventSource {
        poll_times: Arc<Mutex<Vec<tokio::time::Instant>>>,
    }

    #[async_trait::async_trait]
    impl super::super::chain_events::ChainEventSource for TimingEventSource {
        async fn get_changed_agreements(
            &self,
            _since: &Cursor,
            _pinned_block: Option<u64>,
        ) -> Result<
            super::super::chain_events::ChangedAgreementsResult,
            super::super::chain_events::ChainEventError,
        > {
            self.poll_times
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            Ok(super::super::chain_events::ChangedAgreementsResult {
                snapshots: vec![],
                latest_block: 1,
                latest_block_timestamp: Some(1000),
                cursor: Cursor::at_block(1),
            })
        }
    }

    /// Persist the freshly-fetched chain timestamp even when a parse
    /// failure holds the cursor back. The expiration service reads the
    /// persisted timestamp to decide which agreements are past their
    /// deadline; coupling the timestamp write to a cursor advance lets
    /// expiration go dormant whenever the cursor stalls.
    #[tokio::test]
    async fn test_persists_timestamp_when_cursor_held_back() {
        let registry = MockRegistry::new();
        // Pin the initial cursor at block 100 so the held-back cursor
        // (also 100, no id) is not strictly greater and `advance_cursor`
        // is false.
        registry.set_initial_last_processed_block(100);

        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        event_source.set_cursor_override(Some(Cursor::at_block(100)));

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: "http://localhost:8000/subgraphs/name/test".parse().unwrap(),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            // Disable the reorg buffer for this test so the read-side
            // cursor stays at the persisted value and the held-back
            // assertion is unambiguous.
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
        };

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue::default(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            signer_address: Address::ZERO,
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(200)).await;

        handle.stop().await;
        let _ = svc_handle.await;

        let updates = registry.chain_listener_state_updates();
        assert!(
            !updates.is_empty(),
            "expected at least one update_chain_listener_state call despite held-back cursor"
        );
        for (cursor, ts) in &updates {
            assert_eq!(
                cursor.block, 100,
                "cursor must stay at 100 when get_changed_agreements returns a held-back cursor"
            );
            assert_eq!(
                *ts,
                Some(1_700_000_000),
                "timestamp must be persisted on every poll while the subgraph reports it"
            );
        }
    }

    /// Drives a multi-page drain through the listener loop and asserts
    /// every snapshot past the per-query cap still gets reconciled.
    #[tokio::test]
    async fn test_chain_listener_paginates_across_subgraph_page_cap() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        // Cap of 2 per poll forces multiple pages over the 5 snapshots.
        event_source.set_page_size(Some(2));

        let agreement_ids: Vec<IndexingAgreementId> = (0..5)
            .map(|_| IndexingAgreementId::from_bytes(rand::random()))
            .collect();
        for (i, id) in agreement_ids.iter().enumerate() {
            registry.add_agreement(*id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![AgreementStateSnapshot {
                agreement_id: *id,
                indexer: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                state: super::super::chain_events::AgreementState::Accepted,
                canceled_by: Address::ZERO,
                last_state_change_block: ((i + 1) as u64) * 10,
            }]);
        }

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: "http://localhost:8000/subgraphs/name/test".parse().unwrap(),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
        };

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue::default(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            signer_address: Address::ZERO,
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        // Allow several poll intervals: at page_size=2 over 5 snapshots
        // the loop needs ~3 polls to consume them all, plus one extra
        // for the final cursor advance to latest_block.
        tokio::time::sleep(Duration::from_millis(500)).await;

        handle.stop().await;
        let _ = svc_handle.await;

        for id in &agreement_ids {
            assert!(
                registry.was_marked_accepted_on_chain(id),
                "agreement {id} past the page cap was never reconciled; \
                 cursor advanced past it"
            );
        }
    }

    /// When N agreements share one `lastStateChangeBlock` and N exceeds
    /// the page cap, the keyset cursor's `id` discriminator must drain
    /// the tie across multiple pages.
    #[tokio::test]
    async fn test_chain_listener_drains_same_block_tied_entries() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        // Cap of 2 forces multiple pages over the 5 tied entries.
        event_source.set_page_size(Some(2));

        // Five agreements all at the same block. With distinct random ids
        // they carry the implicit `(block, id)` order graph-node uses.
        let agreement_ids: Vec<IndexingAgreementId> = (0..5)
            .map(|_| IndexingAgreementId::from_bytes(rand::random()))
            .collect();
        for id in &agreement_ids {
            registry.add_agreement(*id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![AgreementStateSnapshot {
                agreement_id: *id,
                indexer: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                state: super::super::chain_events::AgreementState::Accepted,
                canceled_by: Address::ZERO,
                last_state_change_block: 50,
            }]);
        }

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: "http://localhost:8000/subgraphs/name/test".parse().unwrap(),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
        };

        let ctx = Ctx {
            registry: registry.clone(),
            worker_queue: MockWorkerQueue::default(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            signer_address: Address::ZERO,
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        // 5 entries at cap=2 needs ~3 polls plus one tail tick.
        tokio::time::sleep(Duration::from_millis(500)).await;

        handle.stop().await;
        let _ = svc_handle.await;

        for id in &agreement_ids {
            assert!(
                registry.was_marked_accepted_on_chain(id),
                "agreement {id} sharing block 50 with the others was never reconciled; \
                 keyset cursor failed to drain the tie"
            );
        }
    }

    /// Drives the drain through a forced `apply_reconciliation_batch`
    /// failure and asserts the row counters do not double-count: every
    /// item lands in `errors` and none in `processed`. Without the fix
    /// in `drain_once`, the per-prep finalize loop would tick
    /// `processed += 1` for the same items already counted as errors.
    #[tokio::test]
    async fn test_drain_counters_no_double_count_on_batch_failure() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        registry.set_fail_batch(true);

        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));

        const ITEMS: usize = 4;
        let agreement_ids: Vec<IndexingAgreementId> = (0..ITEMS)
            .map(|_| IndexingAgreementId::from_bytes(rand::random()))
            .collect();
        for (i, id) in agreement_ids.iter().enumerate() {
            registry.add_agreement(*id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![AgreementStateSnapshot {
                agreement_id: *id,
                indexer: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                state: super::super::chain_events::AgreementState::Accepted,
                canceled_by: Address::ZERO,
                last_state_change_block: ((i + 1) as u64) * 10,
            }]);
        }

        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_chain_ts_persist_wall = std::time::Instant::now();
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);

        let outcome = drain_once(
            &mut cursor,
            &mut last_persisted_timestamp,
            &mut last_chain_ts_persist_wall,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            Address::ZERO,
            &registry,
            &MockWorkerQueue::default(),
            &chain_client,
            &event_source,
            &mut rx_stop,
        )
        .await
        .expect("subgraph fetch itself succeeded; only the batch apply failed");

        assert_eq!(
            outcome.errors, ITEMS as u64,
            "every batch-failed item must be counted exactly once as an error"
        );
        assert_eq!(
            outcome.processed, 0,
            "items whose batch failed must not also count as processed (double-count regression)"
        );

        // Side-effect cross-check: nothing was actually applied.
        for id in &agreement_ids {
            assert!(
                !registry.was_marked_accepted_on_chain(id),
                "batch failure must not leave per-row mock side-effects"
            );
        }
    }

    /// Drives a hostile timestamp drift through `drain_once` and asserts
    /// the persisted chain timestamp is capped near
    /// `CHAIN_TS_DRIFT_TOLERANCE_SECS` instead of accepting the huge
    /// jump. Without the cap, a subgraph response that stays just under
    /// the per-poll skew tolerance would advance the persisted timestamp
    /// far faster than wall clock.
    #[tokio::test]
    async fn test_chain_ts_drift_capped_against_wall_clock() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        let baseline_ts = 1_700_000_000u64;
        let attempted_jump_secs = 100_000u64;

        // First poll establishes the persisted timestamp baseline.
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(baseline_ts));

        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_chain_ts_persist_wall = std::time::Instant::now();
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);

        drain_once(
            &mut cursor,
            &mut last_persisted_timestamp,
            &mut last_chain_ts_persist_wall,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            Address::ZERO,
            &registry,
            &MockWorkerQueue::default(),
            &chain_client,
            &event_source,
            &mut rx_stop,
        )
        .await
        .expect("first poll must succeed");

        assert_eq!(last_persisted_timestamp, Some(baseline_ts));

        // Hostile second poll: wall clock has barely moved, but subgraph
        // claims chain time advanced by ~28 hours.
        event_source.set_latest_block(101);
        event_source.set_latest_block_timestamp(Some(baseline_ts + attempted_jump_secs));

        drain_once(
            &mut cursor,
            &mut last_persisted_timestamp,
            &mut last_chain_ts_persist_wall,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            Address::ZERO,
            &registry,
            &MockWorkerQueue::default(),
            &chain_client,
            &event_source,
            &mut rx_stop,
        )
        .await
        .expect("second poll must succeed");

        let persisted = last_persisted_timestamp.expect("must be set");
        let attempted = baseline_ts + attempted_jump_secs;
        assert!(
            persisted < attempted,
            "drift must be capped; persisted={persisted}, attempted={attempted}"
        );
        // wall_elapsed in this synchronous test is sub-second, so the cap
        // is dominated by the static tolerance. Allow generous slack for
        // slow CI machines but still assert "nowhere near the attempted
        // jump".
        assert!(
            persisted <= baseline_ts + 1_000,
            "persisted ({persisted}) should be within seconds of baseline ({baseline_ts}), \
             not near the attempted jump ({attempted})"
        );
    }

    /// Simulates a restart after a long downtime: the persisted timestamp
    /// is loaded from the DB at its pre-downtime value, the in-memory
    /// wall-clock tracker is set to "downtime ago", and the subgraph
    /// reports a chain timestamp that has legitimately advanced by the
    /// downtime amount. A single poll must catch up fully because the
    /// chain advance is within the wall-elapsed + tolerance bound.
    #[tokio::test]
    async fn test_chain_ts_restart_catches_up_within_wall_elapsed() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        let baseline_ts = 1_700_000_000u64;
        let downtime_secs = 3_600u64; // 1 hour

        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(baseline_ts + downtime_secs));

        // Restart state: persisted timestamp loaded from the DB at the
        // pre-downtime value; in-memory wall tracker reflects the time
        // the previous instance last persisted.
        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp: Option<u64> = Some(baseline_ts);
        let mut last_chain_ts_persist_wall = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(downtime_secs))
            .expect("now - 1h must be representable");
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);

        drain_once(
            &mut cursor,
            &mut last_persisted_timestamp,
            &mut last_chain_ts_persist_wall,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            Address::ZERO,
            &registry,
            &MockWorkerQueue::default(),
            &chain_client,
            &event_source,
            &mut rx_stop,
        )
        .await
        .expect("post-restart poll must succeed");

        let persisted = last_persisted_timestamp.expect("must be set");
        assert_eq!(
            persisted,
            baseline_ts + downtime_secs,
            "after {downtime_secs}s downtime, a single poll must catch up \
             fully because the chain advance is within wall_elapsed + tolerance"
        );
    }

    /// Verify that notify_one() wakes the chain_listener from its idle
    /// 300s interval and triggers a poll within a few seconds.
    #[tokio::test]
    async fn test_notify_wakes_listener_from_idle_interval() {
        let poll_times: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::new(Mutex::new(vec![]));
        let notify = Arc::new(tokio::sync::Notify::new());

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: "http://localhost:8000/subgraphs/name/test".parse().unwrap(),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
        };

        let ctx = Ctx {
            registry: MockRegistry::new(),
            worker_queue: MockWorkerQueue::default(),
            chain_client: MockChainClient::default(),
            event_source: TimingEventSource {
                poll_times: poll_times.clone(),
            },
            config,
            signer_address: Address::ZERO,
            chain_listener_notify: notify.clone(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_before = poll_times.lock().unwrap().len();

        notify.notify_one();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_after = poll_times.lock().unwrap().len();
        assert!(
            polls_after > polls_before,
            "expected at least one poll after notify_one(), got {polls_before} -> {polls_after}"
        );

        handle.stop().await;
        let _ = svc_handle.await;
    }
}
