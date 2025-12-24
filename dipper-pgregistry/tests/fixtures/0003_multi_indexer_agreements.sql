-- Fixture for testing batch queries across multiple indexers
-- Creates agreements for 3 different indexers with various statuses

-- Insert an indexing request
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id)
VALUES ('01930100-0000-7000-8000-000000000001'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingRequestStatus::Open
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\x000000000000a4b1'::bytea);

-- Indexer A: 0x1111111111111111111111111111111111111111
-- Has 2 active agreements (1 Created, 1 Accepted)
INSERT INTO dipper_reg_indexing_agreements (id, created_at, updated_at, status, indexing_request_id, deployment_id, accepted_at_epoch, indexer_id, indexer_url, voucher)
VALUES ('01930100-0001-7000-8000-000000000001'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        -1, -- Created
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        NULL,
        '\x1111111111111111111111111111111111111111'::bytea,
        'https://indexer-a.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "recipient": "0x1111111111111111111111111111111111111111", "service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "duration_epochs": 100, "max_initial_amount": "1000", "max_ongoing_amount_per_epoch": "100", "min_epochs_per_collection": 1, "max_epochs_per_collection": 10, "deadline": 0, "metadata": {"base_price_per_epoch": "10", "price_per_entity": "1", "subgraph_deployment_id": "QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "protocol_network": 1, "chain_id": 1}}'::json);

INSERT INTO dipper_reg_indexing_agreements (id, created_at, updated_at, status, indexing_request_id, deployment_id, accepted_at_epoch, indexer_id, indexer_url, voucher)
VALUES ('01930100-0001-7000-8000-000000000002'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        0, -- Accepted
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB',
        12345,
        '\x1111111111111111111111111111111111111111'::bytea,
        'https://indexer-a.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "recipient": "0x1111111111111111111111111111111111111111", "service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "duration_epochs": 100, "max_initial_amount": "1000", "max_ongoing_amount_per_epoch": "100", "min_epochs_per_collection": 1, "max_epochs_per_collection": 10, "deadline": 0, "metadata": {"base_price_per_epoch": "10", "price_per_entity": "1", "subgraph_deployment_id": "QmBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "protocol_network": 1, "chain_id": 1}}'::json);

-- Indexer A also has 1 rejected agreement (should NOT be returned by batch query)
INSERT INTO dipper_reg_indexing_agreements (id, created_at, updated_at, status, indexing_request_id, deployment_id, accepted_at_epoch, indexer_id, indexer_url, voucher)
VALUES ('01930100-0001-7000-8000-000000000003'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        2, -- Rejected
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC',
        NULL,
        '\x1111111111111111111111111111111111111111'::bytea,
        'https://indexer-a.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "recipient": "0x1111111111111111111111111111111111111111", "service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "duration_epochs": 100, "max_initial_amount": "1000", "max_ongoing_amount_per_epoch": "100", "min_epochs_per_collection": 1, "max_epochs_per_collection": 10, "deadline": 0, "metadata": {"base_price_per_epoch": "10", "price_per_entity": "1", "subgraph_deployment_id": "QmCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC", "protocol_network": 1, "chain_id": 1}}'::json);

-- Indexer B: 0x2222222222222222222222222222222222222222
-- Has 1 active agreement (Created)
INSERT INTO dipper_reg_indexing_agreements (id, created_at, updated_at, status, indexing_request_id, deployment_id, accepted_at_epoch, indexer_id, indexer_url, voucher)
VALUES ('01930100-0002-7000-8000-000000000001'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        -1, -- Created
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD',
        NULL,
        '\x2222222222222222222222222222222222222222'::bytea,
        'https://indexer-b.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "recipient": "0x2222222222222222222222222222222222222222", "service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "duration_epochs": 100, "max_initial_amount": "1000", "max_ongoing_amount_per_epoch": "100", "min_epochs_per_collection": 1, "max_epochs_per_collection": 10, "deadline": 0, "metadata": {"base_price_per_epoch": "10", "price_per_entity": "1", "subgraph_deployment_id": "QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", "protocol_network": 1, "chain_id": 1}}'::json);

-- Indexer C: 0x3333333333333333333333333333333333333333
-- Has NO active agreements (only expired)
INSERT INTO dipper_reg_indexing_agreements (id, created_at, updated_at, status, indexing_request_id, deployment_id, accepted_at_epoch, indexer_id, indexer_url, voucher)
VALUES ('01930100-0003-7000-8000-000000000001'::uuid, timezone('UTC', now()), timezone('UTC', now()),
        5, -- Expired
        '01930100-0000-7000-8000-000000000001'::uuid,
        'QmEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE',
        99999,
        '\x3333333333333333333333333333333333333333'::bytea,
        'https://indexer-c.com',
        '{"payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "recipient": "0x3333333333333333333333333333333333333333", "service": "0x442a24985444cdc6a4db9503d354918d27b5ea97", "duration_epochs": 100, "max_initial_amount": "1000", "max_ongoing_amount_per_epoch": "100", "min_epochs_per_collection": 1, "max_epochs_per_collection": 10, "deadline": 0, "metadata": {"base_price_per_epoch": "10", "price_per_entity": "1", "subgraph_deployment_id": "QmEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE", "protocol_network": 1, "chain_id": 1}}'::json);
