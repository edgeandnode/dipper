-- Composite cursor for chain listener pagination
--
-- The chain listener pages through `IndexingAgreement` snapshots from the
-- subgraph, ordered by `(lastStateChangeBlock, id)`. graph-node implicitly
-- tiebreaks by `id` ascending, giving us a total order on `(block, id)`.
-- Recording only `last_processed_block` cannot resume from inside a tied
-- block; recording the id of the last consumed entity at that block lets
-- the next poll re-issue the keyset query
-- `lastStateChangeBlock = $b AND id_gt: $id` to drain the rest of the tie
-- before advancing to the next block.
--
-- NULL means "no specific id boundary" — the cursor is at a block boundary
-- (or genesis) and the listener should query `lastStateChangeBlock_gt: $b`
-- without any id constraint.

ALTER TABLE dipper_chain_listener_state
    ADD COLUMN IF NOT EXISTS last_processed_id BYTEA NULL;

COMMENT ON COLUMN dipper_chain_listener_state.last_processed_id IS
    'bytes16 IndexingAgreement id of the last entity consumed at last_processed_block. NULL when the cursor is at a block boundary.';
