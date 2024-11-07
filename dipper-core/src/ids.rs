//! Common unique identifiers used in the DIPs Gateway (Dipper).
//!
//! Most of the unique identifiers are *new-type* wrappers around [`Uuid`](uuid::Uuid) v7.

/// The unique identifier of an indexing agreement.
///
/// This is a *new-type* wrapper around [`uuid::Uuid`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexingAgreementId(uuid::Uuid);

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

        impl ::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                let id = ::serde::Deserialize::deserialize(deserializer)?;
                Ok(Self(id))
            }
        }

        impl ::sqlx::Type<::sqlx::Postgres> for $name {
            fn type_info() -> <::sqlx::Postgres as ::sqlx::Database>::TypeInfo {
                ::uuid::Uuid::type_info()
            }
        }

        impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut <::sqlx::Postgres as ::sqlx::Database>::ArgumentBuffer<'q>,
            ) -> Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                self.0.encode_by_ref(buf)
            }
        }

        impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for $name {
            fn decode(
                value: <::sqlx::Postgres as ::sqlx::Database>::ValueRef<'r>,
            ) -> Result<Self, ::sqlx::error::BoxDynError> {
                ::uuid::Uuid::decode(value).map(Self)
            }
        }
    };
}

uuid_new_type_impls!(IndexingAgreementId);
uuid_new_type_impls!(IndexingRequestId);
uuid_new_type_impls!(IndexingReceiptId);
