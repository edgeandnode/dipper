-- Add rejection_reason column to track why an agreement was rejected.
-- This enables differentiated lookback windows: PRICE_TOO_LOW rejections
-- use a 1-day window (until IISA refreshes prices) while other rejections
-- keep the standard 30-day exclusion.
ALTER TABLE dipper_reg_indexing_agreements
ADD COLUMN rejection_reason TEXT;
