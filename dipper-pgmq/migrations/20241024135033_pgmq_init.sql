-- Table: pgmq_queue
CREATE TABLE pgmq_queue
(
    id              UUID PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_for   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    status          INT NOT NULL DEFAULT 0, -- The job status. See `PgJobStatus` for details
    max_attempts    INT NOT NULL DEFAULT 3, -- The maximum number of total attempts (including initial attempt)
    attempt_count   INT NOT NULL DEFAULT 0, -- The number of attempts made so far

    descriptor      JSON NOT NULL -- The job descriptor (serialized)
);

-- Indexes
-- Composite index for efficient pop() operations that filter by status and scheduled_for
CREATE INDEX pgmq_index_on_status_scheduled_for ON pgmq_queue (status, scheduled_for);
