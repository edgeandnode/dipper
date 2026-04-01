-- Insert an indexing request
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingRequestStatus::Open
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\x000000000000a4b1'::bytea, -- arbitrum-one (chain_id: 42161)
        3);

-- Insert multiple indexing agreements
-- Indexing agreement #1: CREATED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300d4-65e3-7d2d-8736-7ba90cee9b69'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        -1, -- IndexingAgreementStatus::Created
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000001'::bytea);

-- Indexing agreement #2: DELIVERY_FAILED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300db-ffea-7e1f-95f2-2561bcfeecf3'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        1, -- IndexingAgreementStatus::DeliveryFailed
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000002'::bytea);

-- Indexing agreement #3: ACCEPTED_ON_CHAIN (different indexer to comply with unique constraint)
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300e1-0c52-72b0-ae96-5eed9a9bd77a'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        6, -- IndexingAgreementStatus::AcceptedOnChain
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xd609e9fdd6ce53e5a26278c50486dd6791d4d705'::bytea,
        'https://indexer2.example.com/graphql',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000003'::bytea);

-- Indexing agreement #4: CANCELED_BY_INDEXER
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300e1-4527-7dd5-a3af-07c84c929cc2'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        4, -- IndexingAgreementStatus::CanceledByIndexer
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000004'::bytea);

-- Indexing agreement #5: CANCELLED_BY_REQUESTER
-- The cancellation of an indexing agreement is done by the requester
-- It implies that all agreements must be cancelled

-- Indexing agreement #6: CANCELLED_BY_INDEXER
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300e1-6568-751d-b006-420bb5dc1b9e'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        4, -- IndexingAgreementStatus::CancelledByIndexer
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000005'::bytea);

-- Indexing agreement #7: EXPIRED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300e1-9458-7f60-a9d6-39921e0647d9'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        5, -- IndexingAgreementStatus::Expired
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000006'::bytea);

-- Indexing agreement #8: Random state (should map to UNKNOWN)
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher,
                                            on_chain_id)
VALUES ('019300e5-ce09-77b8-a7cd-ae9d0d347a8f'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        666, -- IndexingAgreementStatus::Unknown
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        '{
            "payer": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "service_provider": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "data_service": "0x442a24985444cdc6a4db9503d354918d27b5ea97",
            "deadline": 1700000300,
            "ends_at": 1700086400,
            "max_initial_tokens": "4096",
            "max_ongoing_tokens_per_second": "1",
            "min_seconds_per_collection": 86400,
            "max_seconds_per_collection": 2419200,
            "metadata": {
                "tokens_per_second": "1",
                "tokens_per_entity_per_second": "1",
                "subgraph_deployment_id": "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv",
                "protocol_network": 1,
                "chain_id": 1
            }
        }'::json,
        '\x00000000000000000000000000000007'::bytea);
