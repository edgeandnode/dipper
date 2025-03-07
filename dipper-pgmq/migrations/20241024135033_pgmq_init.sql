-- Table: pgmq_queue
CREATE TABLE pgmq_queue
(
    id              UUID PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    scheduled_for   TIMESTAMPTZ NOT NULL,

    status          INT         NOT NULL,
    retry_max       INT         NOT NULL, -- The retry limit
    retry_count     INT         NOT NULL, -- The number of times the job has been retried

    message         JSON        NOT NULL
);

-- Indexes
CREATE INDEX pgmq_index_on_scheduled_for ON pgmq_queue (scheduled_for);
CREATE INDEX pgmq_index_on_status ON pgmq_queue (status);
