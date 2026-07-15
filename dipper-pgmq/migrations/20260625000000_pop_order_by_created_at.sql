-- pop() now orders eligible jobs by (created_at, id) instead of scheduled_for,
-- so a deferred job keeps its place in line and same-timestamp ties resolve by
-- the monotonic v7 id. Index those columns alongside status so the ordered pop
-- stays index-only under a burst.
CREATE INDEX IF NOT EXISTS pgmq_index_on_status_created_at ON pgmq_queue (status, created_at, id);
