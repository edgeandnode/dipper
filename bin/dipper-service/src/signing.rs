//! Message signing for the DIPs Gateway.
//!
//! This module provides the necessary functionality to sign messages using the
//! EIP-712 standard, as well as the TAP protocol.
//!
//! - See the `eip712` module for the EIP-712 signing functionality used in the DIPs CLI to
//!   Gateway communication, and in the Gateway to Indexer communication.
//! - See the `tap` module for the TAP receipt signing functionality used in the DIPs Gateway
//!   receipt collection process.

pub mod eip712;
pub mod tap;
