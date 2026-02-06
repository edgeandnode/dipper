-- Add AcceptedOnChain (status = 6) as an active agreement status.
--
-- Agreements in AcceptedOnChain state have been confirmed on-chain via the
-- IndexingAgreementAccepted event. They should be treated as active to prevent
-- duplicate assignments during the reassignment cycle.

-- Update the unique constraint to include AcceptedOnChain as an active status.
-- The previous constraint only covered Created (-1) and Accepted (0).
DROP INDEX IF EXISTS idx_unique_active_agreement_per_indexer_deployment;

CREATE UNIQUE INDEX idx_unique_active_agreement_per_indexer_deployment
ON dipper_reg_indexing_agreements (indexer_id, deployment_id)
WHERE status IN (-1, 0, 6);
