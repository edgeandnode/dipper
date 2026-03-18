-- Pending cancellations track agreements that should be cancelled only after
-- their replacement is confirmed on-chain (IndexingAgreementAccepted event).
--
-- This prevents under-allocation during reassessment: old agreements stay
-- active until replacements are proven. If a replacement fails (rejected,
-- expired, delivery failed), the pending cancellation record is deleted
-- and the old agreement continues serving.
--
-- The table is advisory, not authoritative. The agreement status in
-- dipper_reg_indexing_agreements is the source of truth. Stale records
-- referencing agreements in terminal states are harmlessly ignored.
CREATE TABLE IF NOT EXISTS dipper_pending_cancellations (
    new_agreement_id UUID NOT NULL REFERENCES dipper_reg_indexing_agreements(id),
    old_agreement_id UUID NOT NULL REFERENCES dipper_reg_indexing_agreements(id),
    indexing_request_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (new_agreement_id, old_agreement_id)
);

-- Reverse lookups: find pending cancellations targeting an old agreement
-- (used to detect duplicates across reassessment cycles and for cleanup).
-- The PK already covers lookups by new_agreement_id (leading column).
CREATE INDEX idx_pending_cancellations_old_agreement
    ON dipper_pending_cancellations(old_agreement_id);
