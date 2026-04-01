-- NOT NULL without DEFAULT: assumes the table is empty at migration time.
-- Dipper has no production data yet. If this migration runs against an
-- existing database with rows, it will fail. In that case, use a two-phase
-- approach: add as nullable, backfill, then alter to NOT NULL.
ALTER TABLE dipper_reg_indexing_agreements ADD COLUMN on_chain_id BYTEA NOT NULL;
CREATE UNIQUE INDEX idx_agreements_on_chain_id ON dipper_reg_indexing_agreements (on_chain_id);
