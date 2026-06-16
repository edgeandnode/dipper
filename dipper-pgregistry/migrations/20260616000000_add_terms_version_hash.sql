-- Add terms_version_hash column to dipper_reg_indexing_agreements.
--
-- Purpose: in protocol-managed (RecurringAgreementManager) payer mode, dipper
-- cancels an agreement through the manager's cancelAgreement(), which requires
-- the EIP-712 terms hash the RecurringCollector stored when the offer landed.
-- Dipper computes that hash from the RCA at registration time and persists it
-- here so the cancel path can supply it without re-deriving from chain state.
--
-- The column is nullable and stays null for agreements created before this
-- migration and for the default external-payer mode, which does not use it.
ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN terms_version_hash BYTEA;
