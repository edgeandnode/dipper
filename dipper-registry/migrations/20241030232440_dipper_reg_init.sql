-- Table: dipper_reg_indexing_requests
CREATE TABLE dipper_reg_indexing_requests
(
    id            UUID PRIMARY KEY,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,

    status        INT         NOT NULL,
    requested_by  TEXT        NOT NULL,
    deployment_id TEXT        NOT NULL
);

CREATE INDEX dipper_reg_indexing_requests_status_idx ON dipper_reg_indexing_requests (status);

-- Table: dipper_reg_indexing_agreements
CREATE TABLE dipper_reg_indexing_agreements
(
    id                  UUID PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    status              INT         NOT NULL,

    indexing_request_id UUID REFERENCES dipper_reg_indexing_requests (id),
    indexer_id          TEXT        NOT NULL,
    indexer_url         TEXT        NOT NULL,
    duration            BIGINT      NOT NULL
);

CREATE INDEX dipper_reg_indexing_agreements_status_idx ON dipper_reg_indexing_agreements (status);

-- Table: dipper_reg_indexing_receipts
CREATE TABLE dipper_reg_indexing_receipts
(
    id                    UUID PRIMARY KEY,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL,

    indexing_agreement_id UUID REFERENCES dipper_reg_indexing_agreements (id),
    allocation_id         TEXT        NOT NULL,
    fee                   BIGINT      NOT NULL,
    poi                   TEXT        NULL
);

CREATE INDEX dipper_reg_indexing_indexing_agreement_id_idx ON dipper_reg_indexing_receipts (indexing_agreement_id);
CREATE INDEX dipper_reg_indexing_receipts_allocation_id_idx ON dipper_reg_indexing_receipts (allocation_id);
