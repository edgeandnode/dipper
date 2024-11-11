use thegraph_core::alloy::{primitives::PrimitiveSignature as Signature, sol_types::SolStruct};

/// EIP-712 signed message
///
/// This struct contains a message and the ECDSA signature of the message according to the
/// EIP-712 standard.
///
/// For the message to be signed, it must either:
/// - To be a _Solidity struct_, i.e., Implement the `SolStruct` trait.
/// - To be convertible into a _Solidity struct_, i.e., Implement the `ToSolStruct` trait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedMessage<M> {
    /// Message payload
    pub message: M,
    /// ECDSA message signature
    pub signature: Signature,
}

impl<M> SignedMessage<M> {
    /// Get the EIP-712 signature bytes
    pub fn signature_bytes(&self) -> SignatureBytes {
        SignatureBytes(self.signature.as_bytes())
    }

    /// Get the message hash according to the EIP-712 standard
    pub fn unique_hash<MSol>(&self) -> MessageHash
    where
        M: ToSolStruct<MSol>,
        MSol: SolStruct,
    {
        MessageHash(*self.message.to_sol_struct().eip712_hash_struct())
    }
}

/// EIP-712 signature bytes
///
/// This is a _new-type_ wrapper around the ECDSA signature bytes that can be used as
/// a key in a btree or hashmap.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignatureBytes([u8; 65]);

impl SignatureBytes {
    /// Get the signature bytes
    pub fn as_bytes(&self) -> [u8; 65] {
        self.0
    }
}

impl std::ops::Deref for SignatureBytes {
    type Target = [u8; 65];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Message hash according to the EIP-712 standard
///
/// This is a _new-type_ wrapper around the hash bytes of the `SignedMessage`'s message payload.
///
/// It can be used to deduplicate messages. As the hash does not include the signature,
/// it is unique for a given message payload. This means that two `SignedMessage`s, signed
/// by two different signers, will have the same hash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageHash([u8; 32]);

impl MessageHash {
    /// Get the message hash bytes
    pub fn as_bytes(&self) -> [u8; 32] {
        self.0
    }
}

impl std::ops::Deref for MessageHash {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A conversion trait for converting a type into a solidity struct representation
///
/// This trait is used to convert a Rust type into a struct implementing the `SolStruct` trait
pub trait ToSolStruct<T: SolStruct> {
    /// Convert this type into the solidity struct representation
    fn to_sol_struct(&self) -> T;
}

impl<T> ToSolStruct<T> for T
where
    T: SolStruct + Clone,
{
    fn to_sol_struct(&self) -> T {
        self.clone()
    }
}
