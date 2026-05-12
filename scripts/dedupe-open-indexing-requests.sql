-- One-shot dedupe script: enforce one Open indexing request per
-- (requested_by, deployment_id, deployment_chain_id).
--
-- Run this BEFORE applying the
-- 20260512000000_unique_open_indexing_requests migration. The migration
-- adds a partial UNIQUE INDEX over the same key tuple where status = 0,
-- and will fail if duplicate Open rows exist.
--
-- Strategy: within each duplicate group, keep the most recently updated
-- row and flip the others to Canceled (status = 1). The kept row's
-- attached agreements stay; the others become historical-only records.
--
-- Safe to run multiple times: only acts on rows with status = 0 that
-- have at least one Open sibling sharing the key tuple.
--
-- Local-network: this is a no-op (data is wiped on every deploy).
-- Staging/testnet operators: run once before the migration, manually.

BEGIN;

WITH ranked AS (
    SELECT
        id,
        updated_at,
        row_number() OVER (
            PARTITION BY requested_by, deployment_id, deployment_chain_id
            ORDER BY updated_at DESC, id ASC
        ) AS rn
    FROM dipper_reg_indexing_requests
    WHERE status = 0
)
UPDATE dipper_reg_indexing_requests
SET status = 1,
    updated_at = timezone('UTC', now())
WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

COMMIT;
