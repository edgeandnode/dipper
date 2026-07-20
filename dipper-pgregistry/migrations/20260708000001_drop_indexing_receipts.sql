-- Drop the unused dipper_reg_indexing_receipts table.
--
-- The table belonged to the abandoned off-chain receipt design, where indexers
-- reported work to dipper and were paid by redeeming stored receipts. Payment
-- moved fully on-chain (RecurringCollector / SubgraphService) before deployment,
-- so nothing ever wrote to it in production and no code reads it.

DROP TABLE IF EXISTS dipper_reg_indexing_receipts;
