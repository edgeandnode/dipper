-- Table: dipper_reg_indexing_requests
CREATE TABLE dipper_reg_indexing_requests
(
    id            UUID PRIMARY KEY,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,

    status        INT         NOT NULL,
    requested_by  BYTEA NOT NULL,
    deployment_id       TEXT  NOT NULL,
    deployment_chain_id BYTEA NOT NULL
);

CREATE INDEX dipper_reg_indexing_requests_requested_by_idx ON dipper_reg_indexing_requests (requested_by);
CREATE INDEX dipper_reg_indexing_requests_status_idx ON dipper_reg_indexing_requests (status);
CREATE INDEX dipper_reg_indexing_requests_deployment_id_idx ON dipper_reg_indexing_requests (deployment_id);

-- Table: dipper_reg_indexing_agreements
CREATE TABLE dipper_reg_indexing_agreements
(
    id                  UUID PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,

    status              INT         NOT NULL,
    indexing_request_id UUID REFERENCES dipper_reg_indexing_requests (id),
    deployment_id                               TEXT   NOT NULL,

    -- Indexer information
    indexer_id          TEXT        NOT NULL,
    indexer_url         TEXT        NOT NULL,

    -- Voucher information
    voucher_payer                               TEXT   NOT NULL,
    voucher_recipient                           TEXT   NOT NULL,
    voucher_service                             TEXT   NOT NULL,
    voucher_duration_epochs                     BIGINT NOT NULL,
    voucher_max_initial_amount                  BYTEA  NOT NULL,
    voucher_max_ongoing_amount_per_epoch        BYTEA  NOT NULL,
    voucher_max_epochs_per_collection           BIGINT NOT NULL,
    voucher_min_epochs_per_collection           BIGINT NOT NULL,
    voucher_metadata_deployment_id              TEXT   NOT NULL,
    voucher_metadata_price_per_block            BYTEA  NOT NULL,
    voucher_metadata_price_per_entity_per_epoch BYTEA  NOT NULL
);

CREATE INDEX dipper_reg_indexing_agreements_status_idx ON dipper_reg_indexing_agreements (status);
CREATE INDEX dipper_reg_indexing_agreements_indexing_request_id_idx ON dipper_reg_indexing_agreements (indexing_request_id);
CREATE INDEX dipper_reg_indexing_agreements_indexer_id_idx ON dipper_reg_indexing_agreements (indexer_id);
CREATE INDEX dipper_reg_indexing_agreements_deployment_id_idx ON dipper_reg_indexing_agreements (deployment_id);

-- Table: dipper_reg_indexing_receipts
CREATE TABLE dipper_reg_indexing_receipts
(
    id                    UUID PRIMARY KEY,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,

    indexing_agreement_id UUID REFERENCES dipper_reg_indexing_agreements (id),
    indexer_id             TEXT   NOT NULL,
    indexer_operator_id    TEXT   NOT NULL,

    -- Collection information
    reported_work_epoch BIGINT NOT NULL,
    reported_work_blocks   BYTEA  NOT NULL,
    reported_work_entities BYTEA  NOT NULL,
    reported_work_poi   BYTEA  NOT NULL,

    amount              BYTEA  NOT NULL
);

CREATE INDEX dipper_reg_indexing_indexing_agreement_id_idx ON dipper_reg_indexing_receipts (indexing_agreement_id);
