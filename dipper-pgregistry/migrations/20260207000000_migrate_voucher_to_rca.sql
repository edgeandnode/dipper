-- Voucher JSON schema changed from epoch-based to seconds-based RCA.
--
-- Field renames:
--   recipient       -> service_provider
--   service         -> data_service
--   duration_epochs -> ends_at (unix timestamp)
--   max_initial_amount         -> max_initial_tokens
--   max_ongoing_amount_per_epoch -> max_ongoing_tokens_per_second
--   min_epochs_per_collection  -> min_seconds_per_collection
--   max_epochs_per_collection  -> max_seconds_per_collection
--   base_price_per_epoch       -> tokens_per_second (metadata)
--   price_per_entity           -> tokens_per_entity_per_second (metadata)
--
-- DIPs is not yet deployed, so no production data needs preserving.
-- Truncate for a clean start with the new schema.

TRUNCATE TABLE dipper_reg_indexing_receipts;
TRUNCATE TABLE dipper_reg_indexing_agreements CASCADE;
