-- Insert an indexing request
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id)
VALUES ('019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingRequestStatus::Open
        '0x442a24985444cdc6a4db9503d354918d27b5ea97',
        'QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv');

-- Insert multiple indexing agreements
-- Indexing agreement #1: CREATED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300d4-65e3-7d2d-8736-7ba90cee9b69'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        -1, -- IndexingAgreementStatus::Created
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0xc509d8fdd5bd42d4915167b49375cc5680c3c604',
        'https://qyxrksoqsm.com/yrmgcijervj',
        7776000::bigint);

-- Indexing agreement #2: DELIVERY_FAILED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300db-ffea-7e1f-95f2-2561bcfeecf3'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        1, -- IndexingAgreementStatus::DeliveryFailed
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0xcd0392da67c76fd7902b1816ea6b98273536fcd5',
        'https://qyxrksoqsm.com/yrmgcijervj',
        7776000::bigint);

-- Indexing agreement #3: ACCEPTED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300e1-0c52-72b0-ae96-5eed9a9bd77a'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        0, -- IndexingAgreementStatus::Accepted
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0x60d8600c86f114b0f5f7ff17a4c8d0bf753b6cab',
        'http://ynphvdraakymgt.com/uwvwjfqcnwbkuc',
        7776000::bigint);

-- Indexing agreement #4: REJECTED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300e1-4527-7dd5-a3af-07c84c929cc2'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        2, -- IndexingAgreementStatus::Rejected
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0x032b1d027d9a1f0467a2d3b2ee9ca21be143a50d',
        'http://btixgalde.com/txminx',
        7776000::bigint);

-- Indexing agreement #5: CANCELLED_BY_REQUESTER
-- The cancellation of an indexing agreement is done by the requester
-- It implies that all agreements must be cancelled

-- Indexing agreement #6: CANCELLED_BY_INDEXER
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300e1-6568-751d-b006-420bb5dc1b9e'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        4, -- IndexingAgreementStatus::CancelledByIndexer
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0x79ec460c6412ff3816743399dc23a664a5406892',
        'http://eikklvt.com/kwlffzwufbo',
        7776000::bigint);

-- Indexing agreement #7: EXPIRED
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300e1-9458-7f60-a9d6-39921e0647d9'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        5, -- IndexingAgreementStatus::Expired
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0x1dd34e5cb3bf63d20eab53fad65d72a08fb32756',
        'http://eikklvt.com/kwlffzwufbo',
        7776000::bigint);

-- Indexing agreement #8: Random state (should map to UNKNOWN)
INSERT INTO dipper_reg_indexing_agreements (id,
                                            created_at,
                                            updated_at,
                                            status,
                                            indexing_request_id,
                                            indexer_id,
                                            indexer_url,
                                            duration)
VALUES ('019300e5-ce09-77b8-a7cd-ae9d0d347a8f'::uuid,
        timezone('UTC', now()),
        timezone('UTC', now()),
        666, -- IndexingAgreementStatus::Unknown
        '019300ce-4751-780e-b58c-bf696b67eb23'::uuid,
        '0x2f34681ac7077f71572862eb83f74f5e5b9d239b',
        'https://hunbfkaq.com/rwpzywmbnvabnt',
        7776000::bigint);
