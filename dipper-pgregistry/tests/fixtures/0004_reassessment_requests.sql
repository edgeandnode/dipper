-- Fixture for testing get_open_indexing_requests_for_reassessment

-- Request #1: OPEN, created 2 hours ago (eligible for reassessment with 1 hour min age)
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('01940001-0001-7000-0001-000000000001'::uuid,
        timezone('UTC', now()) - interval '2 hours',
        timezone('UTC', now()) - interval '30 minutes',
        0, -- IndexingRequestStatus::Open
        '\x1111111111111111111111111111111111111111'::bytea,
        'QmAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1a',
        '\x000000000000a4b1'::bytea,
        3);

-- Request #2: OPEN, created 3 hours ago (eligible, older updated_at should come first)
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('01940002-0002-7000-0002-000000000002'::uuid,
        timezone('UTC', now()) - interval '3 hours',
        timezone('UTC', now()) - interval '2 hours',
        0, -- IndexingRequestStatus::Open
        '\x2222222222222222222222222222222222222222'::bytea,
        'QmBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB2b',
        '\x000000000000a4b1'::bytea,
        3);

-- Request #3: OPEN, created 5 minutes ago (NOT eligible - too new)
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('01940003-0003-7000-0003-000000000003'::uuid,
        timezone('UTC', now()) - interval '5 minutes',
        timezone('UTC', now()) - interval '5 minutes',
        0, -- IndexingRequestStatus::Open
        '\x3333333333333333333333333333333333333333'::bytea,
        'QmCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC3c',
        '\x000000000000a4b1'::bytea,
        3);

-- Request #4: CANCELED, created 2 hours ago (NOT eligible - wrong status)
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('01940004-0004-7000-0004-000000000004'::uuid,
        timezone('UTC', now()) - interval '2 hours',
        timezone('UTC', now()) - interval '1 hour',
        1, -- IndexingRequestStatus::Canceled
        '\x4444444444444444444444444444444444444444'::bytea,
        'QmDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD4d',
        '\x000000000000a4b1'::bytea,
        3);

-- Request #5: OPEN, created 4 hours ago (eligible, most stale updated_at)
INSERT INTO dipper_reg_indexing_requests (id,
                                          created_at,
                                          updated_at,
                                          status,
                                          requested_by,
                                          deployment_id,
                                          deployment_chain_id,
                                          num_candidates)
VALUES ('01940005-0005-7000-0005-000000000005'::uuid,
        timezone('UTC', now()) - interval '4 hours',
        timezone('UTC', now()) - interval '3 hours',
        0, -- IndexingRequestStatus::Open
        '\x5555555555555555555555555555555555555555'::bytea,
        'QmEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE5e',
        '\x000000000000a4b1'::bytea,
        3);
