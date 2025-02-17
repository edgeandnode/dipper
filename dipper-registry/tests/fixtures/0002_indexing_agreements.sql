-- Insert an indexing request
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
        '\x000000000000a4b1'::bytea);
-- arbitrum-one (chain_id: 42161)

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
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
VALUES ('019300d4-65e3-7d2d-8736-7ba90cee9b69'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        -1, -- IndexingAgreementStatus::Created
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch
        1::bigint, -- min epochs per collection
        28::bigint, -- max epochs per collection
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id


-- Indexing agreement #2: DELIVERY_FAILED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
VALUES ('019300db-ffea-7e1f-95f2-2561bcfeecf3'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        1, -- IndexingAgreementStatus::DeliveryFailed
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch
        1::bigint, -- min epochs per collection
        28::bigint, -- max epochs per collection
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network        
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id

-- Indexing agreement #3: ACCEPTED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
VALUES ('019300e1-0c52-72b0-ae96-5eed9a9bd77a'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingAgreementStatus::Accepted
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch
        1::bigint, -- min epochs per collection
        28::bigint, -- max epochs per collection
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id

-- Indexing agreement #4: REJECTED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
VALUES ('019300e1-4527-7dd5-a3af-07c84c929cc2'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        2, -- IndexingAgreementStatus::Rejected
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch
        1::bigint, -- min epochs per collection
        28::bigint, -- max epochs per collection
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id

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
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
                                            voucher_metadata_price_per_block,
                                            voucher_metadata_price_per_entity_per_epoch)
VALUES ('019300e1-6568-751d-b006-420bb5dc1b9e'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        4, -- IndexingAgreementStatus::CancelledByIndexer
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch
        1::bigint, -- min epochs per collection
        28::bigint, -- max epochs per collection
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id

-- Indexing agreement #7: EXPIRED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
VALUES ('019300e1-9458-7f60-a9d6-39921e0647d9'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        5, -- IndexingAgreementStatus::Expired
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch
        1::bigint, -- min epochs per collection
        28::bigint, -- max epochs per collection
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity        
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id

-- Indexing agreement #8: Random state (should map to UNKNOWN)
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            deployment_id,
                                            indexer_id,
                                            indexer_url,
                                            voucher_payer,
                                            voucher_recipient,
                                            voucher_service,
                                            voucher_duration_epochs,
                                            voucher_max_initial_amount,
                                            voucher_max_ongoing_amount_per_epoch,
                                            voucher_min_epochs_per_collection,
                                            voucher_max_epochs_per_collection,
                                            voucher_deadline,
                                            voucher_metadata_base_price_per_epoch,
                                            voucher_metadata_price_per_entity,
                                            voucher_metadata_deployment_id,
                                            voucher_metadata_protocol_network,
                                            voucher_metadata_chain_id)
VALUES ('019300e5-ce09-77b8-a7cd-ae9d0d347a8f'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        666, -- IndexingAgreementStatus::Unknown
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv',
        '\xc509d8fdd5bd42d4915167b49375cc5680c3c604'::bytea,
        'https://qyxrksoqsm.com/yrmgcijervj',
        -- voucher
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- payer
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- recipient
        '\x442a24985444cdc6a4db9503d354918d27b5ea97'::bytea, -- service
        4294967295::bigint, -- duration epochs
        '\x0000000000000000000000000000000000000000000000000000000000001000'::bytea, -- max initial amount
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- max ongoing amount per epoch    
        28::bigint, -- max epochs per collection        
        1::bigint, -- min epochs per collection 
        0::bigint, -- deadline
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- base price per epoch
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- price per entity
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv', -- deployment id
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea, -- protocol network
        '\x0000000000000000000000000000000000000000000000000000000000000001'::bytea); -- chain id       
