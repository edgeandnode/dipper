use thegraph_core::{
    alloy::primitives::{PrimitiveSignature as Signature, normalize_v},
    signed_message::SignedMessage as InnerSignedMessage,
};

/// New-type wrapper around [`SignedMessage`] implementing `serde::Serialize` and
/// `serde::Deserialize` where the signature is serialized as a base64-encoded string.
///
/// [`SignedMessage`]: crate::signed_message::SignedMessage
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedMessage<M>(InnerSignedMessage<M>);

impl<M> SignedMessage<M> {
    /// Create a new signed message
    pub fn new(message: M, signature: Signature) -> Self {
        SignedMessage(InnerSignedMessage { message, signature })
    }

    /// Get the message
    pub fn message(&self) -> &M {
        &self.0.message
    }

    /// Unwrap the inner [`SignedMessage`] instance
    ///
    /// [`SignedMessage`]: crate::signed_message::SignedMessage
    pub fn into_inner(self) -> InnerSignedMessage<M> {
        self.0
    }

    /// Unwrap the inner message
    pub fn into_message<T>(self) -> T
    where
        M: Into<T>,
    {
        self.0.message.into()
    }
}

impl<M> AsRef<InnerSignedMessage<M>> for SignedMessage<M> {
    fn as_ref(&self) -> &InnerSignedMessage<M> {
        &self.0
    }
}

impl<M> std::ops::Deref for SignedMessage<M> {
    type Target = InnerSignedMessage<M>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M> From<InnerSignedMessage<M>> for SignedMessage<M> {
    fn from(inner: InnerSignedMessage<M>) -> Self {
        SignedMessage(inner)
    }
}

impl<M> From<SignedMessage<M>> for InnerSignedMessage<M> {
    fn from(signed: SignedMessage<M>) -> Self {
        signed.0
    }
}

impl<M> serde::Serialize for SignedMessage<M>
where
    M: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[serde_with::serde_as]
        #[derive(serde::Serialize)]
        struct SignedMessageSer<'a, M> {
            message: &'a M,
            #[serde_as(as = "serde_with::base64::Base64")]
            signature: &'a [u8],
        }

        SignedMessageSer {
            message: &self.0.message,
            signature: &self.0.signature.as_bytes(),
        }
        .serialize(serializer)
    }
}

impl<'de, M> serde::Deserialize<'de> for SignedMessage<M>
where
    M: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Internal struct for deserializing a signed message
        #[serde_with::serde_as]
        #[derive(serde::Deserialize)]
        struct SignedMessageDe<M> {
            message: M,
            #[serde_as(as = "serde_with::base64::Base64")]
            signature: [u8; 65],
        }

        let SignedMessageDe { message, signature } = serde::Deserialize::deserialize(deserializer)?;

        // The signature is a 65-byte array where the last byte is the parity of the `v` value
        let signature = {
            let signature_bytes = &signature[..64];
            let parity = normalize_v(signature[64] as u64)
                .ok_or_else(|| serde::de::Error::custom("invalid signature parity"))?;
            Signature::from_bytes_and_parity(signature_bytes, parity)
        };

        Ok(Self(InnerSignedMessage { message, signature }))
    }
}
