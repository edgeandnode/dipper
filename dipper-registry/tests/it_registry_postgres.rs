use std::time::Duration;

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use dipper_registry::{postgres::PgRegistry, Error, IndexingRequestStatus, Registry};
use sqlx::{Pool, Postgres};
use thegraph_core::{address, allocation_id, deployment_id, indexer_id};
use uuid::uuid;

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_request(db: Pool<Postgres>) -> sqlx::Result<()> {
    //* Given
    // Indexing request
    let requested_by = address!("8f8c426f956876325b1e037c6eae9b189952994c");
    let deployment_id = deployment_id!("QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv");

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_request(requested_by, deployment_id)
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
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");
    let duration = Duration::from_secs(60 * 24 * 60 * 60); // 60 days

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_agreement(indexing_request_id, indexer_id, indexer_url, duration)
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

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");
    let duration = Duration::from_secs(60 * 24 * 60 * 60); // 60 days

    let registry = PgRegistry::new(db);

    // Register a new indexing request
    let indexing_request_id = registry
        .register_new_indexing_request(requested_by, deployment_id)
        .await
        .expect("Failed to register new indexing request");

    //* When
    let res = registry
        .register_new_indexing_agreement(indexing_request_id, indexer_id, indexer_url, duration)
        .await;

    //* Then
    let _indexing_agreement_id = res.expect("Failed to register new indexing agreement");

    Ok(())
}

// TODO: Add tests covering "get" and "mark" methods for indexing agreements

#[test_with::env(DATABASE_URL)]
#[sqlx::test]
async fn register_new_indexing_receipt_no_indexing_agreement(
    db: Pool<Postgres>,
) -> sqlx::Result<()> {
    //* Given
    // Indexing agreement
    let indexing_agreement_id = IndexingAgreementId::new(); // Random ID
    let allocation_id = allocation_id!("f349a67a71e5ab13e46216cf11494722440e4bd3");
    let fee = 100_i64;

    let registry = PgRegistry::new(db);

    //* When
    let res = registry
        .register_new_indexing_receipt(indexing_agreement_id, allocation_id, fee)
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

    // Indexing agreement
    let indexer_id = indexer_id!("3c584ee1d89f43c6ccee17e886a001de2bb4d8a9");
    let indexer_url = "http://localhost:8020".parse().expect("Invalid URL");
    let duration = Duration::from_secs(60 * 24 * 60 * 60); // 60 days

    // Indexing receipt
    let allocation_id = allocation_id!("f349a67a71e5ab13e46216cf11494722440e4bd3");
    let fee = 100_i64;

    let registry = PgRegistry::new(db);

    // Register a new indexing request
    let indexing_request_id = registry
        .register_new_indexing_request(requested_by, deployment_id)
        .await
        .expect("Failed to register new indexing request");

    // Register a new indexing agreement
    let indexing_agreement_id = registry
        .register_new_indexing_agreement(indexing_request_id, indexer_id, indexer_url, duration)
        .await
        .expect("Failed to register new indexing agreement");

    //* When
    let res = registry
        .register_new_indexing_receipt(indexing_agreement_id, allocation_id, fee)
        .await;

    //* Then
    let _indexing_receipt_id = res.expect("Failed to register new indexing receipt");

    Ok(())
}

// TODO: Add tests covering "get" and "redeem" methods for indexing receipts
