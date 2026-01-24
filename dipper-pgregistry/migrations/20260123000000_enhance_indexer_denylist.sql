-- Add additional columns to dipper_indexer_denylist for expiration and audit
-- All fields are required - for "permanent" denials, use a far future date

ALTER TABLE dipper_indexer_denylist
    ADD COLUMN updated_at  TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    ADD COLUMN expires_at  TIMESTAMPTZ NOT NULL DEFAULT '3000-01-01'::timestamptz,
    ADD COLUMN created_by  TEXT NOT NULL DEFAULT 'unknown';

-- Make reason required (set default for any existing rows, then add constraint)
UPDATE dipper_indexer_denylist SET reason = 'unspecified' WHERE reason IS NULL;
ALTER TABLE dipper_indexer_denylist ALTER COLUMN reason SET NOT NULL;

-- Remove defaults after migration (force explicit values on insert)
ALTER TABLE dipper_indexer_denylist ALTER COLUMN expires_at DROP DEFAULT;
ALTER TABLE dipper_indexer_denylist ALTER COLUMN created_by DROP DEFAULT;

-- Index for efficient expired entry queries
CREATE INDEX idx_indexer_denylist_expires_at ON dipper_indexer_denylist (expires_at);

-- Helper procedures for admin use

-- Add an indexer to the denylist
-- Usage: CALL deny_indexer('0x1234...', 'reason', 'admin@example.com', '30 days');
--        CALL deny_indexer('0x1234...', 'reason', 'admin@example.com', '3000-01-01');
CREATE OR REPLACE PROCEDURE deny_indexer(
    address TEXT,
    reason TEXT,
    created_by TEXT,
    expires_in TEXT  -- interval like '30 days' or timestamp like '3000-01-01'
)
LANGUAGE plpgsql AS $$
DECLARE
    indexer_bytes BYTEA;
    expiry TIMESTAMPTZ;
BEGIN
    -- Convert 0x address to bytea
    indexer_bytes := decode(replace(address, '0x', ''), 'hex');

    -- Parse expiry - try as interval first, then as timestamp
    BEGIN
        expiry := timezone('UTC', now()) + expires_in::interval;
    EXCEPTION WHEN OTHERS THEN
        expiry := expires_in::timestamptz;
    END;

    INSERT INTO dipper_indexer_denylist (indexer_id, reason, created_by, expires_at)
    VALUES (indexer_bytes, reason, created_by, expiry)
    ON CONFLICT (indexer_id) DO UPDATE SET
        reason = EXCLUDED.reason,
        created_by = EXCLUDED.created_by,
        expires_at = EXCLUDED.expires_at,
        updated_at = timezone('UTC', now());
END;
$$;

-- Remove an indexer from the denylist
-- Usage: CALL undeny_indexer('0x1234...');
CREATE OR REPLACE PROCEDURE undeny_indexer(address TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM dipper_indexer_denylist
    WHERE indexer_id = decode(replace(address, '0x', ''), 'hex');
END;
$$;
