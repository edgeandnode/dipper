pub use dipper_rpc::eip712_domain;
use thegraph_core::alloy::signers::{k256::SecretKey, local::PrivateKeySigner, SignerSync};

/// Create a new private key signer from a secret key.
pub fn new_private_key_eip712_signer(secret_key: impl AsRef<SecretKey>) -> impl SignerSync {
    PrivateKeySigner::from_signing_key(secret_key.as_ref().into())
}
