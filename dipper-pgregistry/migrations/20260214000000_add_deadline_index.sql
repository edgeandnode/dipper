-- no-transaction
-- Index for expiration service: efficiently find Created agreements past deadline
--
-- This partial index speeds up the get_expired_created_agreements query by:
-- 1. Only indexing rows where status = -1 (Created)
-- 2. Pre-computing (voucher->>'deadline')::bigint in the index
--
-- Without this index, PostgreSQL must scan the entire table and parse JSONB for each row.
-- The directive above tells sqlx to run this migration outside a transaction,
-- which is required for CREATE INDEX CONCURRENTLY.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agreements_created_deadline
ON dipper_reg_indexing_agreements (CAST(voucher->>'deadline' AS bigint))
WHERE status = -1;
