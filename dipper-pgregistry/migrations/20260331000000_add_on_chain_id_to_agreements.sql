ALTER TABLE dipper_reg_indexing_agreements ADD COLUMN on_chain_id BYTEA NOT NULL;
CREATE UNIQUE INDEX idx_agreements_on_chain_id ON dipper_reg_indexing_agreements (on_chain_id);
