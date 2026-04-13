//! An EIP-712 signer implementation
//!
//! The EIP-712 signer is a wrapper around an ECDSA signer and an EIP-712 domain separator.

use dipper_rpc::indexer::{gateway_server, indexer_client};
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
    /// The EIP-712 domain separator (admin RPC messages)
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

    /// Sign a DIPs Cancellation message using the [EIP-712] standard.
    ///
    /// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
    pub fn sign_dips_cancellation_msg<M, MSol>(
        &self,
        msg: M,
    ) -> Result<SignedMessage<M>, SigningError>
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        sign(
            &self.signer,
            &indexer_client::dips_cancellation_eip712_domain(self.signer_chain),
            msg,
        )
    }

    /// Recover the signer's address from an [EIP-712] signed DIPs cancellation message.
    ///
    /// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
    pub fn recover_dips_cancellation_msg_signer<M, MSol>(
        &self,
        msg: &SignedMessage<M>,
    ) -> Result<Address, RecoverSignerError>
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        recover_signer_address(
            &gateway_server::dips_cancellation_eip712_domain(self.signer_chain),
            msg,
        )
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

        // Sign the message
        let signed_message = sign(&eip712_signer.signer, &eip712_signer.domain, message)
            .expect("message signing failed");

        //* When
        // Verify the signed message
        let result = eip712_signer.recover_signer(&signed_message);

        //* Then
        // The signature should be valid
        let recovered_address = result.expect("message verification failed");
        assert_eq!(recovered_address, signer_address);
    }
}
