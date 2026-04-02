//! Common unique identifiers used in the DIPs Gateway (Dipper).
//!
//! [`IndexingAgreementId`] wraps the on-chain `bytes16` agreement ID derived from
//! `keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce))[0..16]`.
//!
//! [`IndexingRequestId`] and [`IndexingReceiptId`] remain UUID v7 *new-type* wrappers.

/// The on-chain agreement ID (`bytes16`).
///
/// Derived from `keccak256(abi.encode(payer, dataService, serviceProvider, deadline, nonce))[0..16]`.
/// Stored as `BYTEA` in Postgres, serialised as `0x`-prefixed hex over JSON/RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingAgreementId([u8; 16]);

impl IndexingAgreementId {
    /// Construct from a raw 16-byte array.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Consume and return the inner 16-byte array.
    pub fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Borrow the inner 16-byte array.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl<'a> From<&'a [u8; 16]> for IndexingAgreementId {
    fn from(bytes: &'a [u8; 16]) -> Self {
        Self(*bytes)
    }
}

impl<'a> TryFrom<&'a [u8]> for IndexingAgreementId {
    type Error = std::array::TryFromSliceError;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        <[u8; 16]>::try_from(bytes).map(Self)
    }
}

impl std::fmt::Display for IndexingAgreementId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for IndexingAgreementId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hex = s.strip_prefix("0x").unwrap_or(s);
        if hex.len() != 32 {
            return Err(format!(
                "expected 32 hex chars (16 bytes), got {} chars",
                hex.len()
            ));
        }
        let mut bytes = [0u8; 16];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_nibble(chunk[0])
                .ok_or_else(|| format!("invalid hex char: {}", chunk[0] as char))?;
            let lo = hex_nibble(chunk[1])
                .ok_or_else(|| format!("invalid hex char: {}", chunk[1] as char))?;
            bytes[i] = (hi << 4) | lo;
        }
        Ok(Self(bytes))
    }
}

/// Convert an ASCII hex character to its 4-bit value.
fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(feature = "serde")]
impl ::serde::Serialize for IndexingAgreementId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        // Serialize as "0x" + lowercase hex
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> ::serde::Deserialize<'de> for IndexingAgreementId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(::serde::de::Error::custom)
    }
}

#[cfg(feature = "sqlx")]
impl ::sqlx::Type<::sqlx::Postgres> for IndexingAgreementId {
    fn type_info() -> <::sqlx::Postgres as ::sqlx::Database>::TypeInfo {
        // BYTEA
        <Vec<u8> as ::sqlx::Type<::sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "sqlx")]
impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for IndexingAgreementId {
    fn encode_by_ref(
        &self,
        buf: &mut <::sqlx::Postgres as ::sqlx::Database>::ArgumentBuffer<'q>,
    ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
        <&[u8] as ::sqlx::Encode<'q, ::sqlx::Postgres>>::encode_by_ref(&&self.0[..], buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for IndexingAgreementId {
    fn decode(
        value: <::sqlx::Postgres as ::sqlx::Database>::ValueRef<'r>,
    ) -> Result<Self, ::sqlx::error::BoxDynError> {
        let bytes: Vec<u8> = <Vec<u8> as ::sqlx::Decode<'r, ::sqlx::Postgres>>::decode(value)?;
        let arr: [u8; 16] = bytes.try_into().map_err(|v: Vec<u8>| {
            format!("expected 16 bytes for IndexingAgreementId, got {}", v.len())
        })?;
        Ok(Self(arr))
    }
}

#[cfg(feature = "fake")]
impl ::fake::Dummy<fake::Faker> for IndexingAgreementId {
    fn dummy_with_rng<R: ::fake::Rng + ?Sized>(_: &fake::Faker, rng: &mut R) -> Self {
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        Self(bytes)
    }
}

/// A unique identifier of an indexing request.
///
/// This is *new-type* wrapper around [`uuid::Uuid`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingRequestId(uuid::Uuid);

/// A unique identifier of an indexing receipt.
///
/// This is a *new-type* wrapper around [`uuid::Uuid`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingReceiptId(uuid::Uuid);

/// Implementations for the new-type wrappers around [`uuid::Uuid`].
macro_rules! uuid_new_type_impls {
    ($name:ident) => {
        impl $name {
            /// Create a new [`$name`].
            ///
            /// The [`$name`] is generated using the [`Uuid::now_v7`] method.
            ///
            /// [`Uuid::now_v7`]: uuid::Uuid::now_v7
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self(::uuid::Uuid::now_v7())
            }

            /// Parse a [`$name`] from bytes.
            pub fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(::uuid::Uuid::from_bytes(bytes))
            }

            /// Unwrap the [`$name`] into the inner [`Uuid`].
            ///
            /// [`Uuid`]: uuid::Uuid
            pub fn into_inner(self) -> ::uuid::Uuid {
                self.0
            }

            /// Get a reference to the inner [`Uuid`].
            ///
            /// [`Uuid`]: uuid::Uuid
            pub fn as_uuid(&self) -> &::uuid::Uuid {
                &self.0
            }
        }

        impl From<::uuid::Uuid> for $name {
            fn from(id: ::uuid::Uuid) -> Self {
                Self(id)
            }
        }

        impl<'a> From<&'a [u8; 16]> for $name {
            fn from(bytes: &'a [u8; 16]) -> Self {
                Self(::uuid::Uuid::from_bytes(*bytes))
            }
        }

        impl<'a> TryFrom<&'a [u8]> for $name {
            type Error = ::uuid::Error;

            fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
                ::uuid::Uuid::from_slice(bytes).map(Self)
            }
        }

        impl std::ops::Deref for $name {
            type Target = ::uuid::Uuid;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        #[cfg(feature = "serde")]
        impl ::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        #[cfg(feature = "serde")]
        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                ::serde::Deserialize::deserialize(deserializer).map(Self)
            }
        }

        #[cfg(feature = "sqlx")]
        impl ::sqlx::Type<::sqlx::Postgres> for $name {
            fn type_info() -> <::sqlx::Postgres as ::sqlx::Database>::TypeInfo {
                ::uuid::Uuid::type_info()
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <::sqlx::Postgres as ::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                self.0.encode_by_ref(buf)
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for $name {
            fn decode(
                value: <::sqlx::Postgres as ::sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, ::sqlx::error::BoxDynError> {
                ::uuid::Uuid::decode(value).map(Self)
            }
        }

        #[cfg(feature = "fake")]
        impl ::fake::Dummy<fake::Faker> for $name {
            fn dummy_with_rng<R: ::fake::Rng + ?Sized>(_: &fake::Faker, rng: &mut R) -> Self {
                use fake::uuid::UUIDv7;

                Self(::fake::Dummy::<UUIDv7>::dummy_with_rng(&UUIDv7, rng))
            }
        }
    };
}

uuid_new_type_impls!(IndexingRequestId);
uuid_new_type_impls!(IndexingReceiptId);
