//! Configuration types and utilities.

use std::str::FromStr;

use thegraph_core::alloy::{
    hex::FromHexError,
    primitives::B256,
    signers::k256::{elliptic_curve::Error as CryptoError, SecretKey},
};

/// A _new-type_ wrapper for a configuration value that should be kept secret.
///
/// It is used to prevent the value from being printed in logs or debug output.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Hidden<T>(pub T);

impl<T> Hidden<T> {
    /// Unwrap the hidden value
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::fmt::Display for Hidden<T>
where
    T: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<T> std::fmt::Debug for Hidden<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

impl<T> std::str::FromStr for Hidden<T>
where
    T: std::str::FromStr,
{
    type Err = T::Err;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        T::from_str(value).map(Self)
    }
}

impl<T> AsRef<T> for Hidden<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> std::borrow::Borrow<T> for Hidden<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::Deref for Hidden<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An error that can occur when parsing a secret key from a hex string.
#[derive(Debug, thiserror::Error)]
pub enum HiddenSecretKeyFromStrError {
    /// An error occurred while parsing the hex string.
    #[error(transparent)]
    FromHexError(#[from] FromHexError),
    /// An error occurred while parsing the secret key bytes.
    #[error(transparent)]
    CryptoError(#[from] CryptoError),
}

/// Parse a secret key from a hex string.
pub fn secret_key_from_str(s: &str) -> Result<Hidden<SecretKey>, HiddenSecretKeyFromStrError> {
    let bytes = B256::from_str(s)?;
    let key = SecretKey::from_slice(bytes.as_slice())?;
    Ok(Hidden(key))
}

/// Serialize (and deserialize) a `SecretKey` as a hex string.
pub struct HiddenSecretKeyAsHexStr;

impl serde_with::SerializeAs<Hidden<SecretKey>> for HiddenSecretKeyAsHexStr {
    fn serialize_as<S>(source: &Hidden<SecretKey>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = B256::from_slice(source.to_bytes().as_slice());
        serializer.serialize_str(&bytes.to_string())
    }
}

impl<'de> serde_with::DeserializeAs<'de, Hidden<SecretKey>> for HiddenSecretKeyAsHexStr {
    fn deserialize_as<D>(deserializer: D) -> Result<Hidden<SecretKey>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: B256 = serde::Deserialize::deserialize(deserializer)?;
        let key = SecretKey::from_slice(bytes.as_slice()).map_err(serde::de::Error::custom)?;
        Ok(Hidden(key))
    }
}
