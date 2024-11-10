//! EIP712 message signing and verification.
//!
//! This module contains the `SignedMessage` struct which is used to sign and verify messages
//! using the [EIP-712] standard.
//!
//! # API
//!
//! The `signing` submodule provides the following functions:
//!
//! - `sign`: Signs a message using the [EIP-712] standard
//! - `recover_signer_address`: Recovers the signer's address of a signed message
//! - `verify`: Verifies the signer's address of a signed message
//!
//! The `serde` submodule provides a wrapper around the `SignedMessage` struct to allow for
//! serialization and deserialization of signed messages where the signature is serialized as a
//! base64-encoded string.
//!
//! [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
// TODO: Move this to thegraph-core

mod message;
pub mod serde;
pub mod signing;

pub use message::{SignedMessage, ToSolStruct};
