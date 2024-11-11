use thegraph_core::alloy::{
    primitives::{Address, SignatureError},
    signers::{
        k256::ecdsa::Error as EcdsaError, Error as SignerError, SignerSync,
        UnsupportedSignerOperation,
    },
    sol_types::{Eip712Domain, SolStruct},
};

use super::message::{SignedMessage, ToSolStruct};

/// Errors that can occur when signing a message.
#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    /// The signer does not support the operation
    #[error("operation `{0}` is not supported by the signer")]
    UnsupportedOperation(UnsupportedSignerOperation),

    /// The ECDSA signature failed
    #[error(transparent)]
    Ecdsa(#[from] EcdsaError),

    /// Generic error
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

/// Errors that can occur when recovering the signer's address of a message.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct RecoverSignerError(#[from] SignatureError);

/// Errors that can occur when verifying the signer's address of a message.
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// Errors in signature parsing or verification
    #[error(transparent)]
    SignatureError(#[from] SignatureError),

    /// The signer's address does not match the expected address
    #[error("expected signer `{expected}` but received `{received}`")]
    InvalidSigner {
        /// The expected signer's address
        expected: Address,
        /// The received signer's address
        received: Address,
    },
}

/// Signs a message using the [EIP-712] standard
///
/// Returns a [`SignedMessage`] containing the message and the ECDSA signature of the message
///
/// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
pub fn sign<S, M, MSol>(
    signer: &S,
    domain: &Eip712Domain,
    message: M,
) -> Result<SignedMessage<M>, SigningError>
where
    S: SignerSync,
    M: ToSolStruct<MSol>,
    MSol: SolStruct,
{
    let message_sol = message.to_sol_struct();
    let signature = signer
        .sign_typed_data_sync(&message_sol, domain)
        .map_err(|err| match err {
            SignerError::UnsupportedOperation(err) => SigningError::UnsupportedOperation(err),
            SignerError::TransactionChainIdMismatch { .. } => {
                unreachable!("sign_typed_data_sync should not return TransactionChainIdMismatch")
            }
            SignerError::DynAbiError(_) => {
                unreachable!("sign_typed_data_sync should not return DynAbiError")
            }
            SignerError::Ecdsa(err) => SigningError::Ecdsa(err),
            SignerError::HexError(_) => {
                unreachable!("sign_typed_data_sync should not return HexError")
            }
            SignerError::SignatureError(_) => {
                unreachable!("sign_typed_data_sync should not return SignatureError")
            }
            SignerError::Other(err) => SigningError::Other(err),
        })?;
    Ok(SignedMessage { message, signature })
}

/// Recover the signer's address  an [EIP-712] signed message
///
/// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
pub fn recover_signer_address<M, MSol>(
    domain: &Eip712Domain,
    signed_message: &SignedMessage<M>,
) -> Result<Address, RecoverSignerError>
where
    M: ToSolStruct<MSol>,
    MSol: SolStruct,
{
    let message_sol = signed_message.message.to_sol_struct();
    let recovery_message_hash = message_sol.eip712_signing_hash(domain);
    let recovered_address = signed_message
        .signature
        .recover_address_from_prehash(&recovery_message_hash)?;
    Ok(recovered_address)
}

/// Verify the signer's address of an [EIP-712] signed message
///
/// Returns `Ok(())` if the  signer's address retrieved from the signature matches the expected
/// address. Otherwise, returns a [`VerificationError`] with details about the mismatch.
///
/// [EIP-712]: https://eips.ethereum.org/EIPS/eip-712 "EIP-712"
pub fn verify<M, MSol>(
    domain: &Eip712Domain,
    signed_message: &SignedMessage<M>,
    expected_address: &Address,
) -> Result<(), VerificationError>
where
    M: ToSolStruct<MSol>,
    MSol: SolStruct,
{
    let recovered_address =
        recover_signer_address(domain, signed_message).map_err(|RecoverSignerError(err)| err)?;

    if recovered_address != *expected_address {
        Err(VerificationError::InvalidSigner {
            expected: expected_address.to_owned(),
            received: recovered_address,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use thegraph_core::alloy::{
        primitives::{address, b256, keccak256, PrimitiveSignature as Signature},
        signers::local::PrivateKeySigner,
        sol_types::{eip712_domain, Eip712Domain},
    };

    use super::{recover_signer_address, sign, verify, SignedMessage, VerificationError};

    /// Test EIP712 domain separator
    const EIP712_DOMAIN: Eip712Domain = eip712_domain! {
        name: "Test domain",
        version: "1",
        chain_id: 1,
        verifying_contract: address!("a83682bbe91c0d2d48a13fd751b2da8e989fe421"),
        salt: b256!("66eb090e6dbb9668c7d32c0ee7ba5e8f08d84385804485d316dd5f5692273593")
    };

    alloy_sol_types::sol! {
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
    fn sign_message_with_private_key_signer() {
        //* Given
        let signer = wallet();
        let domain = EIP712_DOMAIN;

        // Create a message with some data
        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        //* When
        // Sign the message
        let result = sign(&signer, &domain, message);

        //* Then
        // The message should be signed
        assert!(result.is_ok());
    }

    #[test]
    fn recover_signer_from_signed_message() {
        //* Given
        let signer = wallet();

        let domain = EIP712_DOMAIN;

        // Create a message with some data
        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        // Sign the message
        let signed_message = sign(&signer, &domain, message).unwrap();

        //* When
        // Recover the signer's address
        let result = recover_signer_address(&domain, &signed_message);

        //* Then
        // The address should be recovered
        let signer_address = result.expect("recover_signer failed");

        // The signer should be the wallet's address
        assert_eq!(signer_address, signer_address);
    }

    #[test]
    fn recover_signer_should_fail_with_invalid_signature() {
        //* Given
        let domain = EIP712_DOMAIN;

        // Create a message with some data
        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        // Create a signed message with an invalid signature (random values)
        let invalid_signature_signed_message = SignedMessage {
            message,
            signature: Signature::from_scalars_and_parity(
                b256!("ca457b3f821e5c03545944e0318868a783d0e6b438c85a82537d52a619decfe2"),
                b256!("26a9f36fcf89431476aa556021ee77959dc480fb3458054f26d068b52d525cc4"),
                false,
            ),
        };

        //* When
        // Recover the signer's address
        let result = recover_signer_address(&domain, &invalid_signature_signed_message);

        //* Then
        // The address should not be recovered
        assert!(result.is_err());
    }

    #[test]
    fn verify_signed_message() {
        //* Given
        let signer = wallet();
        let signer_address = signer.address();

        let domain = EIP712_DOMAIN;

        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        // Sign the message
        let signed_message = sign(&signer, &domain, message).unwrap();

        //* When
        // Verify the signed message
        let result = verify(&domain, &signed_message, &signer_address);

        //* Then
        // The signature should be valid
        assert!(result.is_ok());
    }

    #[test]
    fn signed_message_verification_should_fail_with_invalid_signer() {
        //* Given
        let signer = wallet();
        let domain = EIP712_DOMAIN;

        // Create a message with some data
        let message = Message {
            data: keccak256(b"Hello, world!"),
        };

        // Sign the message
        let signed_message = sign(&signer, &domain, message).unwrap();

        // Create a different signer
        let different_signer = wallet();
        let different_signer_address = different_signer.address();

        //* When
        // Verify the signed message
        let result = verify(&domain, &signed_message, &different_signer_address);

        //* Then
        // The signature should be invalid
        let error = result.expect_err("verify_signature should fail");
        if let VerificationError::InvalidSigner { expected, received } = error {
            assert_eq!(expected, different_signer_address);
            assert_eq!(received, signer.address());
        } else {
            panic!("unexpected error: {:?}", error);
        }
    }
}
