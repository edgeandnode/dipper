//! # A PostgreSQL-based message queue.

use sqlx::{
    Acquire,
    migrate::{Migrate, MigrateError},
};

mod id;
mod job;
mod postgres;
mod queue;

pub use id::JobId;
pub use job::JobGuard;
pub use postgres::PgQueue;
pub use queue::Queue;

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
