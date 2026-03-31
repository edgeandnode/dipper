ALTER TABLE dipper_reg_indexing_agreements ADD COLUMN on_chain_id BYTEA;
CREATE INDEX idx_agreements_on_chain_id ON dipper_reg_indexing_agreements (on_chain_id);
