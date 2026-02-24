//! # A PostgreSQL-based registry for indexing operations.

use sqlx::{
    Acquire,
    migrate::{Migrate, MigrateError},
};

mod indexing_agreement;
mod indexing_receipt;
mod indexing_request;
mod postgres;
mod result;

pub use indexing_agreement::{
    Indexer as IndexingAgreementIndexer, IndexingAgreement, Status as IndexingAgreementStatus,
    Voucher as IndexingAgreementVoucher, VoucherMetadata as IndexingAgreementVoucherMetadata,
    rejection_reason,
};
pub use indexing_receipt::{IndexingReceipt, ReportedWork as IndexingReceiptReportedWork};
pub use indexing_request::{IndexingRequest, Status as IndexingRequestStatus};
pub use postgres::PgRegistry;
pub use result::{Error, Result};

/// Run the DB migrations.
///
/// It is used to ensure that the database is up to date with the latest migrations.
pub async fn run_db_migrations<'a, A>(conn: A) -> Result<(), MigrateError>
where
    A: Acquire<'a>,
    <A::Connection as std::ops::Deref>::Target: Migrate,
{
    sqlx::migrate!("./migrations").run(conn).await?;
    Ok(())
}
