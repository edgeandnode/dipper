ALTER TABLE dipper_reg_indexing_agreements RENAME COLUMN voucher TO terms;

-- Recreate the deadline index to reference the renamed column
DROP INDEX IF EXISTS idx_agreements_created_deadline;

CREATE INDEX idx_agreements_created_deadline
ON dipper_reg_indexing_agreements (CAST(terms->>'deadline' AS bigint))
WHERE status = -1;
