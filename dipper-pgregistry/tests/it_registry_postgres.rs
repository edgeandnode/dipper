#![cfg(feature = "fake")]

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_pgregistry::{
    Error, IndexingAgreementStatus, IndexingAgreementVoucher, IndexingReceiptReportedWork,
    IndexingRequestStatus, PgRegistry,
};
use fake::{Fake, Faker};
use sqlx::{Pool, Postgres};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{ChainId, address},
    deployment_id,
    fake_impl::alloy::Alloy as FakeAlloy,
    indexer_id,
};
use url::Url;
use uuid::uuid;

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_request(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request
    let requested_by = FakeAlloy.fake();
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = Faker.fake::<ChainId>();

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_request(requested_by, deployment_id, deployment_chain_id)
        .await;

    //* Then
    let _indexing_request_id = res.expect("Failed to register new indexing request");

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn get_all_indexing_requests(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let registry = PgRegistry::new(db);

    //* When
    let indexing_requests = registry.get_all_indexing_requests().await;

    //* Then
    let indexing_requests = indexing_requests.expect("Failed to get all indexing requests");
    assert_eq!(indexing_requests.len(), 3);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_get_by_id(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request #1: OPEN
    let indexing_request_id = uuid!("019300ce-4751-780e-b58c-bf696b67eb23").into();

    let registry = PgRegistry::new(db);

    //* When
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID");

    //* Then
    let indexing_request = indexing_request.expect("No indexing request with the given ID");

    assert_eq!(indexing_request.id, indexing_request_id);
    assert_eq!(indexing_request.status, IndexingRequestStatus::Open);
    assert_eq!(
        indexing_request.requested_by,
        address!("442a24985444cdc6a4db9503d354918d27b5ea97")
    );
    assert_eq!(
        indexing_request.deployment_id,
        deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv")
    );

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_get_by_id_not_found(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Non-existent indexing request
    let indexing_request_id = uuid!("01930119-9a0e-7ea2-8dad-691515451655").into();

    let registry = PgRegistry::new(db);

    //* When
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID");

    //* Then
    assert!(indexing_request.is_none());

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_get_by_id_unknown_status(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request #3: Random state (should map to UNKNOWN)
    let indexing_request_id = uuid!("01930108-5942-7515-bd5e-2cba9c7027b7").into();

    let registry = PgRegistry::new(db);

    //* When
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID");

    //* Then
    let indexing_request = indexing_request.expect("No indexing request with the given ID");

    assert_eq!(indexing_request.id, indexing_request_id);
    assert_eq!(indexing_request.status, IndexingRequestStatus::Unknown);
    assert_eq!(
        indexing_request.requested_by,
        address!("be00782710cdf47168a386dfa79299729f076df6")
    );
    assert_eq!(
        indexing_request.deployment_id,
        deployment_id!("QmZ5EcVesbdDidvgdMtd4h5xugVkEQWBgJ84CEouZrHGEq")
    );

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_mark_open_as_canceled(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request #1: OPEN
    let indexing_request_id = uuid!("019300ce-4751-780e-b58c-bf696b67eb23").into();

    let registry = PgRegistry::new(db);

    //* When
    registry
        .mark_indexing_request_as_canceled(&indexing_request_id)
        .await
        .expect("Failed to mark indexing request as canceled");

    //* Then
    // Assert the indexing request has been marked as CANCELED
    let indexing_request = registry
        .get_indexing_request_by_id(&indexing_request_id)
        .await
        .expect("Failed to get indexing request by ID")
        .expect("No indexing request with the given ID");

    assert_eq!(indexing_request.id, indexing_request_id);
    assert_eq!(indexing_request.status, IndexingRequestStatus::Canceled);

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_mark_canceled_as_canceled(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request #2: CANCELED
    let indexing_request_id = uuid!("01930105-d664-79ad-8535-5b82b0ad1aab").into();

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .mark_indexing_request_as_canceled(&indexing_request_id)
        .await;

    //* Then
    // Assert a `NoRecordsUpdated` error is returned
    let err = res.expect_err("Expected error when marking CANCELED indexing request as CANCELED");
    assert!(matches!(err, Error::NoRecordsUpdated));

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_mark_unknown_as_canceled(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request #3: Random state (should map to UNKNOWN)
    let indexing_request_id = uuid!("01930108-5942-7515-bd5e-2cba9c7027b7").into();

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .mark_indexing_request_as_canceled(&indexing_request_id)
        .await;

    //* Then
    // Assert a `NoRecordsUpdated` error is returned
    let err = res.expect_err("Expected error when marking CANCELED indexing request as CANCELED");
    assert!(matches!(err, Error::NoRecordsUpdated));

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0001_indexing_requests"))]
async fn indexing_request_mark_non_existent_as_canceled(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Non-existent indexing request
    let indexing_request_id = uuid!("0193010f-e202-7c8f-b41c-505c01b5d5dd").into();

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .mark_indexing_request_as_canceled(&indexing_request_id)
        .await;

    //* Then
    // Assert a `NoRecordsUpdated` error is returned
    let err =
        res.expect_err("Expected error when marking non-existent indexing request as CANCELED");
    assert!(matches!(err, Error::NoRecordsUpdated));

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_agreement_no_indexing_request(
    db: Pool<Postgres>,
) -> sqlx::Result<()> {
    //* Given
    // Indexing agreement
    let indexing_request_id = IndexingRequestId::new(); // Random ID
    let deployment_id = Faker.fake::<DeploymentId>();
    let indexer_id = Faker.fake::<IndexerId>();
    let indexer_url = Faker.fake::<Url>();

    let agreement_voucher = Faker.fake::<IndexingAgreementVoucher>();

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_agreement(
            indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            agreement_voucher,
        )
        .await;

    //* Then
    let _error = res.expect_err("Expected error when registering agreement");

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_agreement(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request
    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = 42161; // arbitrum-one (0xa4b1)

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");

    // Indexing agreement voucher
    let agreement_voucher = Faker.fake::<IndexingAgreementVoucher>();

    let registry = PgRegistry::new(db);

    // Register a new indexing request
    let indexing_request_id = registry
        .register_new_indexing_request(requested_by, deployment_id, deployment_chain_id)
        .await
        .expect("Failed to register new indexing request");

    //* When
    let res = registry
        .register_new_indexing_agreement(
            indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            agreement_voucher,
        )
        .await;

    //* Then
    let _indexing_agreement_id = res.expect("Failed to register new indexing agreement");

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_and_get_indexing_agreement_by_id(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let registry = PgRegistry::new(db);

    // Indexing request
    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = 42161; // arbitrum-one (0xa4b1)

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");
    let agreement_voucher = {
        let mut voucher = Faker.fake::<IndexingAgreementVoucher>();
        voucher.metadata.subgraph_deployment_id = deployment_id;
        voucher
    };

    // Register a new indexing request
    let indexing_request_id = registry
        .register_new_indexing_request(requested_by, deployment_id, deployment_chain_id)
        .await
        .expect("Failed to register new indexing request");

    // Register a new indexing agreement
    let indexing_agreement_id = registry
        .register_new_indexing_agreement(
            indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            agreement_voucher,
        )
        .await
        .expect("Failed to register new indexing agreement");

    //* When
    let indexing_agreement = registry
        .get_indexing_agreement_by_id(&indexing_agreement_id)
        .await
        .expect("Failed to get indexing agreement by ID")
        .expect("Agreement not found");

    //* Then
    assert_eq!(indexing_agreement.id, indexing_agreement_id);
    assert_eq!(indexing_agreement.indexing_request_id, indexing_request_id);
    assert_eq!(indexing_agreement.status, IndexingAgreementStatus::Created);
    assert_eq!(indexing_agreement.indexer.id, indexer_id);
    assert_eq!(
        indexing_agreement.voucher.metadata.subgraph_deployment_id,
        deployment_id
    );

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0002_indexing_agreements"))]
async fn get_indexing_agreements_by_indexing_request_id(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    let registry = PgRegistry::new(db);

    let indexing_request_id = uuid!("019300ce-4751-780e-b58c-bf696b67eb23").into();

    //* When
    let res = registry
        .get_active_indexing_agreements_by_indexing_request_id(&indexing_request_id)
        .await;

    //* Then
    let agreements = res.expect("Failed to get indexing agreements by indexing request ID");
    assert_eq!(agreements.len(), 2);

    // Assert the agreements are in the expected state
    assert!(
        agreements
            .iter()
            .all(|agreement| { agreement.indexing_request_id == indexing_request_id }),
        "Expected all agreements to be associated with the given indexing request"
    );
    assert!(
        agreements.iter().all(|agreement| {
            matches!(
                agreement.status,
                IndexingAgreementStatus::Created | IndexingAgreementStatus::Accepted
            )
        }),
        "Expected all agreements to be in CREATED or ACCEPTED state"
    );

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test(fixtures("0002_indexing_agreements"))]
async fn get_rejected_indexing_agreements_by_indexing_request_id(
    db: Pool<Postgres>,
) -> sqlx::Result<()> {
    //* Given
    let registry = PgRegistry::new(db);

    let indexing_request_id = uuid!("019300ce-4751-780e-b58c-bf696b67eb23").into();

    //* When
    let res = registry
        .get_rejected_indexing_agreements_by_indexing_request_id(&indexing_request_id)
        .await;

    //* Then
    let agreements = res.expect("Failed to get indexing agreements by indexing request ID");
    assert_eq!(agreements.len(), 2);

    // Assert the agreements are in the expected state
    assert!(
        agreements
            .iter()
            .all(|agreement| { agreement.indexing_request_id == indexing_request_id }),
        "Expected all agreements to be associated with the given indexing request"
    );
    assert!(
        agreements.iter().all(|agreement| {
            matches!(
                agreement.status,
                IndexingAgreementStatus::Rejected | IndexingAgreementStatus::CanceledByIndexer
            )
        }),
        "Expected all agreements to be in REJECTED state"
    );

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_receipt_no_indexing_agreement(
    db: Pool<Postgres>,
) -> sqlx::Result<()> {
    //* Given
    // Indexing agreement
    let indexing_agreement_id = Faker.fake::<IndexingAgreementId>();
    let indexer_id = Faker.fake::<IndexerId>();
    let indexer_operator_id = FakeAlloy.fake();
    let reported_work = Faker.fake::<IndexingReceiptReportedWork>();
    let amount = FakeAlloy.fake();

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_receipt(
            indexing_agreement_id,
            indexer_id,
            indexer_operator_id,
            reported_work,
            amount,
        )
        .await;

    //* Then
    let _error = res.expect_err("Expected error when registering receipt");

    Ok(())
}

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_receipt(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request
    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");
    let deployment_chain_id = 42161; // arbitrum-one (0xa4b1)

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");
    let agreement_voucher = Faker.fake::<IndexingAgreementVoucher>();

    // Indexing receipt
    let indexer_operator_id = address!("f027cfe07afa186afec8144eb20e53715d7f33b2");
    let reported_work = Faker.fake::<IndexingReceiptReportedWork>();
    let amount = FakeAlloy.fake();

    let registry = PgRegistry::new(db);

    // Register a new indexing request
    let indexing_request_id = registry
        .register_new_indexing_request(requested_by, deployment_id, deployment_chain_id)
        .await
        .expect("Failed to register new indexing request");

    // Register a new indexing agreement
    let indexing_agreement_id = registry
        .register_new_indexing_agreement(
            indexing_request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            agreement_voucher,
        )
        .await
        .expect("Failed to register new indexing agreement");

    //* When
    let res = registry
        .register_new_indexing_receipt(
            indexing_agreement_id,
            indexer_id,
            indexer_operator_id,
            reported_work,
            amount,
        )
        .await;

    //* Then
    let _indexing_receipt_id = res.expect("Failed to register new indexing receipt");

    Ok(())
}
