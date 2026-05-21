-- Drop the unused indexing_request_id column from dipper_pending_cancellations.
--
-- The column was carried over from the legacy gRPC dead-letter cancel
-- fan-out, which the chain listener no longer fires. No code reads or writes
-- it any more; INSERTs now record only the (new_agreement_id, old_agreement_id)
-- pair. Postgres keeps the dropped column's storage until the next
-- VACUUM FULL but the column disappears from the schema immediately.

ALTER TABLE dipper_pending_cancellations
  DROP COLUMN IF EXISTS indexing_request_id;
