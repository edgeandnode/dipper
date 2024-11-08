use base64::prelude::{Engine as _, BASE64_STANDARD};
use serde::ser::SerializeStruct as _;
use thegraph_core::alloy::primitives::PrimitiveSignature as Signature;

use super::signed_message::SignedMessage as InnerSignedMessage;

/// New-type wrapper around [`SignedMessage`] that serializes the signature as a base64 string
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
        // Encode the signature as a base64 string
        let signature = BASE64_STANDARD.encode(self.0.signature.as_bytes());

        // Serialize the signed message as a struct with two fields
        let mut ser_struct = serializer.serialize_struct("SignedMessage", 2)?;
        ser_struct.serialize_field("message", &self.0.message)?;
        ser_struct.serialize_field("signature", &signature)?;
        ser_struct.end()
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
        struct SignedMessageVisitor<M>(std::marker::PhantomData<M>);

        impl<'de, M> serde::de::Visitor<'de> for SignedMessageVisitor<M>
        where
            M: serde::Deserialize<'de>,
        {
            type Value = SignedMessage<M>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a struct with two fields: message and signature")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut message = None;
                let mut signature = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "message" => {
                            if message.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message = Some(map.next_value()?);
                        }
                        "signature" => {
                            if signature.is_some() {
                                return Err(serde::de::Error::duplicate_field("signature"));
                            }
                            let signature_str: String = map.next_value()?;
                            signature = Some(
                                BASE64_STANDARD
                                    .decode(signature_str.as_bytes())
                                    .map_err(serde::de::Error::custom)?,
                            );
                        }
                        _ => {
                            return Err(serde::de::Error::unknown_field(
                                key,
                                &["message", "signature"],
                            ));
                        }
                    }
                }

                // If the message or signature field is missing, return an error
                let message = message.ok_or_else(|| serde::de::Error::missing_field("message"))?;
                let signature =
                    signature.ok_or_else(|| serde::de::Error::missing_field("signature"))?;

                // Convert the signature into a `Signature` instance
                let signature =
                    Signature::try_from(&signature[..]).map_err(serde::de::Error::custom)?;

                Ok(SignedMessage(InnerSignedMessage { message, signature }))
            }
        }

        deserializer.deserialize_struct(
            "SignedMessage",
            &["message", "signature"],
            SignedMessageVisitor(std::marker::PhantomData),
        )
    }
}
