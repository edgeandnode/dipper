//! The API module contains all the public API to be shared among the different application modules.

use thegraph_core::alloy::{
    primitives::{b256, ChainId, B256},
    sol_types::{eip712_domain, Eip712Domain},
};

pub mod indexing_requests;

/// DIPs EIP-712 domain salt
const EIP712_DOMAIN_SALT: B256 =
    b256!("b4632c657c26dce5d4d7da1d65bda185b14ff8f905ddbb03ea0382ed06c5ef28");

/// Create an EIP-712 domain given a chain ID and dispute manager address.
pub fn eip712_domain(chain_id: ChainId) -> Eip712Domain {
    eip712_domain! {
        name: "Graph Protocol",
        version: "0",
        chain_id: chain_id,
        salt: EIP712_DOMAIN_SALT,
    }
}
