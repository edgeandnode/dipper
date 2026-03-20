-- Add block timestamp to chain_listener_state for chain-time expiration.
--
-- The expiration service compares agreement deadlines against this timestamp
-- instead of wall-clock time, making it immune to subgraph lag.
ALTER TABLE dipper_chain_listener_state
    ADD COLUMN last_processed_block_timestamp BIGINT;
