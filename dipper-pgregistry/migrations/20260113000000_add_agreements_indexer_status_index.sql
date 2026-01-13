-- Add composite index for efficient lookups on indexing agreements by indexer_id and status
-- This optimizes queries like get_pending_agreement_indexers_by_deployment which filter
-- by indexer_id and status

CREATE INDEX idx_agreements_indexer_status
ON dipper_reg_indexing_agreements (indexer_id, status);

-- Add index on deployment_id for efficient GROUP BY operations
CREATE INDEX idx_agreements_deployment
ON dipper_reg_indexing_agreements (deployment_id);

-- Enforce that an indexer can only have one active agreement per deployment
-- This prevents duplicate payments for the same work
-- Status values: Created = -1, Accepted = 0
CREATE UNIQUE INDEX idx_unique_active_agreement_per_indexer_deployment
ON dipper_reg_indexing_agreements (indexer_id, deployment_id)
WHERE status IN (-1, 0);
