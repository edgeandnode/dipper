-- no-transaction (required for CREATE INDEX CONCURRENTLY)
-- Partial index for the offer-pacing count, which runs every reassessment poll.
-- Created rows are bounded by the in-flight caps, so this stays tiny, unlike a
-- wider partial index that also covers accepted rows.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agreements_created_by_indexer
ON dipper_reg_indexing_agreements (indexer_id)
WHERE status = -1;
