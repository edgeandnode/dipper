//! The indexer-facing RPC server.
mod context;
mod handlers;
pub mod service;

pub use context::{Ctx, CtxBuilder};
