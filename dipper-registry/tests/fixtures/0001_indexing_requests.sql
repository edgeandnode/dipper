-- Insert multiple indexing requests
-- Indexing request #1: OPEN
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id)
VALUES ('019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingRequestStatus::Open
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\x000000000000a4b1'::bytea); -- arbitrum-one (chain_id: 42161)

-- Indexing request #2: CANCELED
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id)
VALUES ('01930105-d664-79ad-8535-5b82b0ad1aab'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        1, -- IndexingRequestStatus::Canceled
        '\x132b60d867f7d1148a14108e1c3ee70c2acff14d'::bytea,
        'QmZtNN8NbxjJ1KD5uKBYa7Gj29CT8xypSXnAmXbrLNTQgX',
        '\x000000000000a4b1'::bytea); -- arbitrum-one (chain_id: 42161)

-- Indexing request #3: Random state (should map to UNKNOWN)
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id)
VALUES ('01930108-5942-7515-bd5e-2cba9c7027b7'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        22, -- IndexingRequestStatus::Unknown
        '\xbe00782710cdf47168a386dfa79299729f076df6'::bytea,
        'QmZ5EcVesbdDidvgdMtd4h5xugVkEQWBgJ84CEouZrHGEq',
        '\x000000000000a4b1'::bytea); -- arbitrum-one (chain_id: 42161)
