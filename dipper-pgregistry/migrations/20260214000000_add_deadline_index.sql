-- Index for expiration service: efficiently find Created agreements past deadline
--
-- This partial index speeds up the get_expired_created_agreements query by:
-- 1. Only indexing rows where status = -1 (Created)
-- 2. Pre-computing (voucher->>'deadline')::bigint in the index
--
-- Without this index, PostgreSQL must scan the entire table and parse JSONB for each row.

-- Run outside transaction to allow CONCURRENTLY (avoids blocking writes during index creation)
-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agreements_created_deadline
ON dipper_reg_indexing_agreements ((voucher->>'deadline')::bigint)
WHERE status = -1;
