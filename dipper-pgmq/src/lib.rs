//! # A PostgreSQL-based message queue.

mod id;
mod job;
mod postgres;
mod queue;

pub use id::JobId;
pub use job::JobGuard;
pub use postgres::{PgQueue, run_db_migrations};
pub use queue::Queue;
