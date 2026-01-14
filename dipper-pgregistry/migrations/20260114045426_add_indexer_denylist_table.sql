-- Table: dipper_indexer_denylist
-- Stores indexers that have been administratively denied from selection.
-- Denied indexers will be excluded from all IISA selections until removed.
CREATE TABLE dipper_indexer_denylist
(
    indexer_id BYTEA PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    reason     TEXT
);

CREATE INDEX idx_indexer_denylist_created_at ON dipper_indexer_denylist (created_at DESC);
