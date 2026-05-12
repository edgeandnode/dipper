//! Common error handling utilities for RPC handlers.
//!
//! This module provides helper functions to reduce boilerplate when handling
//! registry results in RPC methods.

use jsonrpsee::{core::RpcResult, types::ErrorObject};

/// Handle a registry result that returns `Option<T>`, transforming it into an RPC response.
///
/// Returns:
/// - `Ok(transform(value))` if the result is `Ok(Some(value))`
/// - `Err(404)` if the result is `Ok(None)`
/// - `Err(503)` if the result is `Err(_)`, after logging the error
pub fn handle_optional_result<T, U, E, F>(
    result: Result<Option<T>, E>,
    error_context: &str,
    transform: F,
) -> RpcResult<U>
where
    E: std::fmt::Debug,
    F: FnOnce(T) -> U,
{
    match result {
        Ok(Some(value)) => Ok(transform(value)),
        Ok(None) => Err(ErrorObject::borrowed(404, "Not found", None)),
        Err(err) => {
            tracing::error!(error = ?err, "{}", error_context);
            Err(ErrorObject::borrowed(503, "Service unavailable", None))
        }
    }
}

/// Handle a registry result that returns `Vec<T>`, transforming each item into an RPC response.
///
/// Returns:
/// - `Ok(items.map(transform).collect())` if the result is `Ok(items)`
/// - `Err(503)` if the result is `Err(_)`, after logging the error
pub fn handle_list_result<T, U, E, F>(
    result: Result<Vec<T>, E>,
    error_context: &str,
    transform: F,
) -> RpcResult<Vec<U>>
where
    E: std::fmt::Debug,
    F: FnMut(T) -> U,
{
    match result {
        Ok(items) => Ok(items.into_iter().map(transform).collect()),
        Err(err) => {
            tracing::error!(error = ?err, "{}", error_context);
            Err(ErrorObject::borrowed(503, "Service unavailable", None))
        }
    }
}
