-- Add offer_tx_hash column to dipper_reg_indexing_agreements.
--
-- Purpose: observability for the on-chain offer submission path. When dipper
-- calls RecurringCollector.offer() as part of building a new agreement, it
-- records the submitted transaction hash here for debugging and correlation
-- with the indexing-payments-subgraph.
--
-- This column is NOT load-bearing: idempotency for offer submission is gated
-- on the indexing-payments-subgraph's `Offer` entity (the `rcaOffers` mapping
-- on `RecurringCollector` lives inside a namespaced storage struct and has
-- no public getter, so an RPC-level check is unavailable). The column is
-- nullable and remains null for any agreement created before this migration
-- or for agreements where the offer tx failed to submit.

ALTER TABLE dipper_reg_indexing_agreements
    ADD COLUMN offer_tx_hash BYTEA;
