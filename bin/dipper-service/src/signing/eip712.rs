//! An EIP-712 signer-recovery helper.
//!
//! Wraps an EIP-712 domain separator plus the signer's address and chain ID,
//! and exposes recovery for signed admin-RPC messages.

use std::marker::PhantomData;

use thegraph_core::{
    alloy::{
        primitives::{Address, ChainId},
        signers::{SignerSync, local::PrivateKeySigner},
        sol_types::{Eip712Domain, SolStruct},
    },
    signed_message::{RecoverSignerError, SignedMessage, ToSolStruct, recover_signer_address},
};

/// An [`Eip712Signer`] backed by a [`PrivateKeySigner`].
pub type PrivateKeyEip712Signer = Eip712Signer<PrivateKeySigner>;

/// Carries the signer's identity and the EIP-712 domain used to verify
/// inbound admin-RPC messages.
///
/// The generic parameter exists for backwards compatibility with callers that
/// type their handles as `Eip712Signer<PrivateKeySigner>`; it is no longer used
/// to sign because dipper only verifies inbound signatures.
pub struct Eip712Signer<S> {
    signer_address: Address,
    signer_chain: ChainId,
    domain: Eip712Domain,
    _phantom: PhantomData<S>,
}

impl<S> Eip712Signer<S>
where
    S: SignerSync,
{
    /// Create a new [`Eip712Signer`] instance.
    pub fn new(signer_address: Address, signer_chain: ChainId, domain: Eip712Domain) -> Self {
        Self {
            signer_address,
            signer_chain,
            domain,
            _phantom: PhantomData,
        }
    }

    /// Get the signer's address
    pub fn address(&self) -> Address {
        self.signer_address
    }

    /// Get the signer's chain ID
    pub fn chain_id(&self) -> ChainId {
        self.signer_chain
    }

    /// Recover the signer's address from an [EIP-712] signed message.
    ///
    /// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
    pub fn recover_signer<M, MSol>(
        &self,
        signed_message: &SignedMessage<M>,
    ) -> Result<Address, RecoverSignerError>
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        recover_signer_address(&self.domain, signed_message)
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::{
        alloy::{
            primitives::{address, b256, keccak256},
            signers::local::PrivateKeySigner,
            sol_types::{Eip712Domain, eip712_domain},
        },
        signed_message::sign,
    };

    use super::Eip712Signer;

    /// Test EIP712 domain separator
    const EIP712_DOMAIN: Eip712Domain = eip712_domain! {
        name: "Test domain",
        version: "1",
        chain_id: 1,
        verifying_contract: address!("a83682bbe91c0d2d48a13fd751b2da8e989fe421"),
        salt: b256!("66eb090e6dbb9668c7d32c0ee7ba5e8f08d84385804485d316dd5f5692273593")
    };

    thegraph_core::alloy::sol! {
        /// Test struct for EIP712 message
        struct Message {
            bytes32 data;
        }
    }

    /// Test utility method generating a random wallet
    fn wallet() -> PrivateKeySigner {
        PrivateKeySigner::random()
    }

    #[test]
    fn signer_sign_and_verify() {
        //* Given
        let signer = wallet();
        let signer_address = signer.address();
        let signer_chain = 42161;
        let domain = EIP712_DOMAIN;

        // Create a message with some data
        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        // Create an Eip712Signer instance for verifying inbound messages
        let eip712_signer: Eip712Signer<PrivateKeySigner> =
            Eip712Signer::new(signer_address, signer_chain, domain.clone());

        // Sign the message with a freestanding signer
        let signed_message = sign(&signer, &domain, message).expect("message signing failed");

        //* When
        // Verify the signed message
        let result = eip712_signer.recover_signer(&signed_message);

        //* Then
        // The signature should be valid
        let recovered_address = result.expect("message verification failed");
        assert_eq!(recovered_address, signer_address);
    }
}
