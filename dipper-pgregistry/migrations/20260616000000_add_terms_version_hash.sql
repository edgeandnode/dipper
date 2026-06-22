-- Add terms_version_hash column to dipper_reg_indexing_agreements.
--
-- Purpose: dipper cancels an agreement through the RecurringAgreementManager's
-- cancelAgreement(), which requires the EIP-712 terms hash the RecurringCollector
-- stored when the offer landed.
-- Dipper computes that hash from the RCA at registration time and persists it
-- here so the cancel path can supply it without re-deriving from chain state.
--
-- The column is nullable and stays null only for agreements created before this
-- migration.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN terms_version_hash BYTEA;
