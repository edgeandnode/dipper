//! # A PostgreSQL-based message queue.

pub mod postgres;
mod queue;

pub use queue::{Job, JobId, Queue};
