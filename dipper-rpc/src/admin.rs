pub mod indexing_agreements;
pub mod indexing_requests;

mod message;

pub use message::SignedMessage;
use thegraph_core::alloy::{
    dyn_abi::Eip712Domain,
    primitives::{B256, ChainId, b256},
    sol_types::eip712_domain,
};

/// The Arbitrum One (mainnet) chain ID (eip155).
pub(crate) const CHAIN_ID_ARBITRUM_ONE: ChainId = 0xa4b1; // 42161

/// DIPs EIP-712 domain salt
pub(crate) const EIP712_DOMAIN_SALT: B256 =
    b256!("b4632c657c26dce5d4d7da1d65bda185b14ff8f905ddbb03ea0382ed06c5ef28");

/// Create an EIP-712 domain given a chain ID and dispute manager address.
pub fn eip712_domain() -> Eip712Domain {
    eip712_domain! {
        name: "Graph Protocol",
        version: "0",
        chain_id: CHAIN_ID_ARBITRUM_ONE,
        salt: EIP712_DOMAIN_SALT,
    }
}
