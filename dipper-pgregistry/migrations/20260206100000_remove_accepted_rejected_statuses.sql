-- Remove Accepted (0) and Rejected (2) statuses from the V2 on-chain flow.
--
-- With V2, agreements go directly from Created to AcceptedOnChain (on-chain
-- event observed) or Expired (deadline passed). The off-chain ACCEPT/REJECT
-- step has been removed.
--
-- Safety: convert any stale Accepted rows to AcceptedOnChain and Rejected
-- rows to Expired, in case any exist from testing.
UPDATE dipper_reg_indexing_agreements SET status = 6 WHERE status = 0;
UPDATE dipper_reg_indexing_agreements SET status = 5 WHERE status = 2;

-- Update the unique constraint to only cover Created (-1) and AcceptedOnChain (6).
DROP INDEX IF EXISTS idx_unique_active_agreement_per_indexer_deployment;

CREATE UNIQUE INDEX idx_unique_active_agreement_per_indexer_deployment
ON dipper_reg_indexing_agreements (indexer_id, deployment_id)
WHERE status IN (-1, 6);

-- Drop the accepted_at_epoch column (no longer used).
ALTER TABLE dipper_reg_indexing_agreements DROP COLUMN IF EXISTS accepted_at_epoch;
