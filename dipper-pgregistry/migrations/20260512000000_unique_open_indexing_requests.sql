-- no-transaction
-- Partial unique index enforcing one Open request per (payer, deployment, chain).
--
-- The new `set_indexing_target_candidates` admin method upserts on this key:
-- a second call for the same requester/deployment/chain updates the existing
-- Open row's num_candidates instead of inserting a parallel row. The partial
-- predicate (WHERE status = 0) lets a previously-Canceled row coexist with a
-- fresh Open row for the same key, which is the supported re-registration
-- pattern after a deliberate cancel.
--
-- Operators with existing duplicate Open rows must dedupe before this
-- migration can apply. See `scripts/dedupe-open-indexing-requests.sql`.
--
-- The `-- no-transaction` directive above lets sqlx run CREATE INDEX
-- CONCURRENTLY, which avoids locking the table during build.

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS
  uniq_open_indexing_requests
ON dipper_reg_indexing_requests
  (requested_by, deployment_id, deployment_chain_id)
WHERE status = 0;
