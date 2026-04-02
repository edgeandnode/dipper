-- Replace the UUID primary key with the on-chain agreement ID (BYTEA).
--
-- The on-chain ID is derived from
--   keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce))[0..16]
-- and is already stored in the `on_chain_id` column added in the previous migration.
-- This migration promotes it to primary key and demotes the old UUID to `nonce_uuid`.
--
-- Dipper has no production data yet. If this migration runs against an existing
-- database with rows, the NOT NULL / PK constraints will be enforced on existing data.

-- 1. Drop FK constraints that reference the old UUID PK.
ALTER TABLE dipper_reg_indexing_receipts
    DROP CONSTRAINT IF EXISTS dipper_reg_indexing_receipts_indexing_agreement_id_fkey;

ALTER TABLE dipper_pending_cancellations
    DROP CONSTRAINT IF EXISTS dipper_pending_cancellations_new_agreement_id_fkey;

ALTER TABLE dipper_pending_cancellations
    DROP CONSTRAINT IF EXISTS dipper_pending_cancellations_old_agreement_id_fkey;

-- 2. Drop the existing PK on `id` (UUID column).
ALTER TABLE dipper_reg_indexing_agreements
    DROP CONSTRAINT dipper_reg_indexing_agreements_pkey;

-- 3. Rename columns: old `id` -> `nonce_uuid`, old `on_chain_id` -> `id`.
ALTER TABLE dipper_reg_indexing_agreements
    RENAME COLUMN id TO nonce_uuid;

ALTER TABLE dipper_reg_indexing_agreements
    RENAME COLUMN on_chain_id TO id;

-- 4. Add new PK on the BYTEA `id` column.
ALTER TABLE dipper_reg_indexing_agreements
    ADD PRIMARY KEY (id);

-- 5. Drop the unique index on `on_chain_id` (now redundant with PK).
DROP INDEX IF EXISTS idx_agreements_on_chain_id;

-- 6. Migrate FK columns in `dipper_reg_indexing_receipts` from UUID to BYTEA.
--    Add new BYTEA column, populate from the agreements table, drop old, rename.
ALTER TABLE dipper_reg_indexing_receipts
    ADD COLUMN indexing_agreement_id_new BYTEA;

UPDATE dipper_reg_indexing_receipts r
    SET indexing_agreement_id_new = a.id
    FROM dipper_reg_indexing_agreements a
    WHERE r.indexing_agreement_id = a.nonce_uuid;

ALTER TABLE dipper_reg_indexing_receipts
    DROP COLUMN indexing_agreement_id;

ALTER TABLE dipper_reg_indexing_receipts
    RENAME COLUMN indexing_agreement_id_new TO indexing_agreement_id;

ALTER TABLE dipper_reg_indexing_receipts
    ALTER COLUMN indexing_agreement_id SET NOT NULL;

ALTER TABLE dipper_reg_indexing_receipts
    ADD CONSTRAINT dipper_reg_indexing_receipts_indexing_agreement_id_fkey
    FOREIGN KEY (indexing_agreement_id) REFERENCES dipper_reg_indexing_agreements(id);

-- 7. Migrate FK columns in `dipper_pending_cancellations` from UUID to BYTEA.
ALTER TABLE dipper_pending_cancellations
    ADD COLUMN new_agreement_id_new BYTEA,
    ADD COLUMN old_agreement_id_new BYTEA;

UPDATE dipper_pending_cancellations pc
    SET new_agreement_id_new = a.id
    FROM dipper_reg_indexing_agreements a
    WHERE pc.new_agreement_id = a.nonce_uuid;

UPDATE dipper_pending_cancellations pc
    SET old_agreement_id_new = a.id
    FROM dipper_reg_indexing_agreements a
    WHERE pc.old_agreement_id = a.nonce_uuid;

ALTER TABLE dipper_pending_cancellations
    DROP COLUMN new_agreement_id,
    DROP COLUMN old_agreement_id;

ALTER TABLE dipper_pending_cancellations
    RENAME COLUMN new_agreement_id_new TO new_agreement_id;

ALTER TABLE dipper_pending_cancellations
    RENAME COLUMN old_agreement_id_new TO old_agreement_id;

ALTER TABLE dipper_pending_cancellations
    ALTER COLUMN new_agreement_id SET NOT NULL,
    ALTER COLUMN old_agreement_id SET NOT NULL;

ALTER TABLE dipper_pending_cancellations
    ADD CONSTRAINT dipper_pending_cancellations_new_agreement_id_fkey
    FOREIGN KEY (new_agreement_id) REFERENCES dipper_reg_indexing_agreements(id);

ALTER TABLE dipper_pending_cancellations
    ADD CONSTRAINT dipper_pending_cancellations_old_agreement_id_fkey
    FOREIGN KEY (old_agreement_id) REFERENCES dipper_reg_indexing_agreements(id);
