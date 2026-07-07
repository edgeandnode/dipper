-- Add a job priority so interactive work (admin RPC, Studio) outranks the
-- background reassignment sweep, which enqueues large batches that would
-- otherwise beat interactive traffic to the workers. Default 0 = Background.
ALTER TABLE pgmq_queue ADD COLUMN priority SMALLINT NOT NULL DEFAULT 0;

-- pop() now orders by (priority DESC, created_at, id), so prepend priority to
-- the created_at index and drop the old one: the ordered pop stays index-only
-- under a burst instead of falling back to a sort.
DROP INDEX IF EXISTS pgmq_index_on_status_created_at;
CREATE INDEX IF NOT EXISTS pgmq_index_on_status_priority_created_at
    ON pgmq_queue (status, priority DESC, created_at, id);
