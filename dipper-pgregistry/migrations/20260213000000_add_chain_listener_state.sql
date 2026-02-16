-- Chain listener state table
--
-- Stores the last processed block for each chain to support resumable event polling.
-- This ensures we don't miss events after a restart and don't reprocess old events.

CREATE TABLE IF NOT EXISTS dipper_chain_listener_state (
    -- Chain ID (e.g., 1 for mainnet, 42161 for Arbitrum)
    chain_id BIGINT PRIMARY KEY,
    -- Last block that was fully processed
    last_processed_block BIGINT NOT NULL,
    -- Timestamps for auditing
    created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('UTC', now())
);

COMMENT ON TABLE dipper_chain_listener_state IS 'Tracks the last processed block for the on-chain event listener per chain';
COMMENT ON COLUMN dipper_chain_listener_state.chain_id IS 'Ethereum chain ID (1=mainnet, 42161=Arbitrum, etc.)';
COMMENT ON COLUMN dipper_chain_listener_state.last_processed_block IS 'Last block number that was fully processed by the chain listener';
