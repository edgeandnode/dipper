//! An EIP-712 signer implementation
//!
//! The EIP-712 signer is a wrapper around an ECDSA signer and an EIP-712 domain separator.

use thegraph_core::{
    alloy::{
        primitives::{Address, ChainId},
        signers::{SignerSync, local::PrivateKeySigner},
        sol_types::{Eip712Domain, SolStruct},
    },
    signed_message::{
        RecoverSignerError, SignedMessage, SigningError, ToSolStruct, recover_signer_address, sign,
    },
};

/// An [`Eip712Signer`] using a [`PrivateKeySigner`] as the ECDSA signer
pub type PrivateKeyEip712Signer = Eip712Signer<PrivateKeySigner>;

/// An [`Eip712Signer`] wraps a ECDSA signer and an [EIP-712] domain separator.
///
/// It provides a convenient way to sign and verify messages using the [EIP-712] standard.
///
/// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
pub struct Eip712Signer<S> {
    /// The ECDSA signer
    signer: S,
    /// The signer's address
    signer_address: Address,
    /// The signer's chain ID
    signer_chain: ChainId,
    /// The EIP-712 domain separator
    domain: Eip712Domain,
}

impl<S> Eip712Signer<S>
where
    S: SignerSync,
{
    /// Create a new [`Eip712Signer`] instance
    pub fn new(
        signer: S,
        signer_address: Address,
        signer_chain: ChainId,
        domain: Eip712Domain,
    ) -> Self {
        Self {
            signer,
            signer_address,
            signer_chain,
            domain,
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

    /// Sign a message using the [EIP-712] standard
    ///
    /// Returns a [`SignedMessage`] containing the message and the ECDSA signature of the message
    ///
    /// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
    pub fn sign<M, MSol>(&self, message: M) -> Result<SignedMessage<M>, SigningError>
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        sign(&self.signer, &self.domain, message)
    }

    /// Sing a message using the [EIP-712] standard with the given domain
    ///
    /// Returns a [`SignedMessage`] containing the message and the ECDSA signature of the message
    ///
    /// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
    pub fn sign_with_domain<M, MSol>(
        &self,
        domain: &Eip712Domain,
        message: M,
    ) -> Result<SignedMessage<M>, SigningError>
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        sign(&self.signer, domain, message)
    }

    /// Recover the signer's address from an [EIP-712] signed message
    ///
    /// Returns the signer's address
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

    /// Recover the signer's address from an [EIP-712] signed message with the given domain
    ///
    /// Returns the signer's address
    ///
    /// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
    pub fn recover_signer_with_domain<M, MSol>(
        &self,
        domain: &Eip712Domain,
        signed_message: &SignedMessage<M>,
    ) -> Result<Address, RecoverSignerError>
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        recover_signer_address(domain, signed_message)
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::alloy::{
        primitives::{address, b256, keccak256},
        signers::local::PrivateKeySigner,
        sol_types::{Eip712Domain, eip712_domain},
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
    fn signer_sing_and_verify() {
        //* Given
        let signer = wallet();
        let signer_address = signer.address();
        let signer_chain = 42161;
        let domain = EIP712_DOMAIN;

        // Create a message with some data
        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        // Create an Eip712Signer instance
        let eip712_signer = Eip712Signer::new(signer, signer_address, signer_chain, domain);

        //* When
        // Sign the message
        let signed_message = eip712_signer.sign(message).expect("message signing failed");

        // Verify the signed message
        let result = eip712_signer.recover_signer(&signed_message);

        //* Then
        // The signature should be valid
        let recovered_address = result.expect("message verification failed");
        assert_eq!(recovered_address, signer_address);
    }
}
