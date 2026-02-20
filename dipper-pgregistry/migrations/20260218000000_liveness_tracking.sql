-- Add liveness tracking columns for the indexer abandonment detection feature.
--
-- last_block_height: The last observed block height for the subgraph deployment.
-- last_progress_at:  Timestamp when the block height was last observed to change
--                    (upward progress or resync from a lower value). NULL until
--                    the first liveness check fires for this agreement.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN last_block_height BIGINT,
    ADD COLUMN last_progress_at  TIMESTAMPTZ;
