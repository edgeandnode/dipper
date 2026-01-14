-- Table: dipper_blocklist
-- Stores indexers that have been administratively blocked from selection.
-- Blocked indexers will be excluded from all IISA selections until removed.
CREATE TABLE dipper_blocklist
(
    indexer_id BYTEA PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    reason     TEXT
);
