-- Event-emission tracking for subgraph-indexing-agreement lifecycle events.
--
-- The lifecycle events (accepted / terminated / expired) must not be lost on a
-- crash between the durable state change and the Kafka send. Rather than a
-- separate outbox table, we treat the agreement row itself as the durable
-- record: audit columns capture the event payload at reconcile time, and a
-- per-event "emitted_at" marker lets a sweep re-derive and re-send any event
-- whose marker is still NULL. Emission becomes self-healing / at-least-once.

-- Audit columns: the on-chain payload observed at reconcile time. Timestamps are
-- chain seconds (BIGINT); tx hashes and canceled_by are stored as their 0x hex
-- string form (matching the event wire representation). All nullable: a row only
-- carries the fields relevant to the transitions it has been through.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN accepted_at  BIGINT,
    ADD COLUMN accepted_tx  TEXT,
    ADD COLUMN canceled_at  BIGINT,
    ADD COLUMN canceled_by  TEXT,
    ADD COLUMN canceled_tx  TEXT;

-- Emission markers: NULL until the corresponding event is confirmed sent to the
-- broker. A sweep emits for rows whose marker is NULL, then stamps it.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN accepted_event_emitted_at   TIMESTAMPTZ,
    ADD COLUMN terminated_event_emitted_at TIMESTAMPTZ,
    ADD COLUMN expired_event_emitted_at    TIMESTAMPTZ;

-- Coverage-shortfall latch for the request. Drives the transition-based shortage
-- signal (fire once on entering shortfall, clear on recovery) instead of the
-- standing-condition emit that re-fired every reassessment.
ALTER TABLE dipper_reg_indexing_requests
    ADD COLUMN shortfall_active BOOLEAN NOT NULL DEFAULT FALSE;

-- Sweep-scan indexes. Partial, so they only cover rows still awaiting emission
-- (normally none), keeping them tiny.
--
-- terminated: only agreements that were genuinely accepted on-chain
-- (accepted_at IS NOT NULL) are eligible -- a never-accepted agreement that was
-- canceled locally has nothing on-chain to have terminated. Statuses:
-- CanceledByRequester (3), CanceledByIndexer (4), AbandonedByIndexer (8).
CREATE INDEX idx_agreements_pending_terminated_event
    ON dipper_reg_indexing_agreements (id)
    WHERE status IN (3, 4, 8)
      AND accepted_at IS NOT NULL
      AND terminated_event_emitted_at IS NULL;

-- accepted: rows whose accept was observed by the reconcile path (accepted_at IS
-- NOT NULL, so pre-feature rows are never backfilled) and not yet announced. NOT
-- gated on status: an agreement accepted-then-cancelled in one snapshot ends up
-- terminal with accepted_at set and must still emit its accepted event.
CREATE INDEX idx_agreements_pending_accepted_event
    ON dipper_reg_indexing_agreements (id)
    WHERE accepted_at IS NOT NULL AND accepted_event_emitted_at IS NULL;

-- expired: Expired (5) rows not yet announced.
CREATE INDEX idx_agreements_pending_expired_event
    ON dipper_reg_indexing_agreements (id)
    WHERE status = 5 AND expired_event_emitted_at IS NULL;
