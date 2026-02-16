use sqlx::Postgres;
use uuid::Uuid;

/// A job ID
///
/// This is a unique identifier for a job in the queue.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Create a new `JobId` from a `Uuid`.
    ///
    /// This is for internal use only.
    pub(crate) fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self(Uuid::nil())
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl sqlx::Type<Postgres> for JobId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("UUID")
    }
}

impl sqlx::Encode<'_, Postgres> for JobId {
    fn encode_by_ref(
        &self,
        buf: &mut <Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        sqlx::Encode::<Postgres>::encode_by_ref(&self.0, buf)
    }
}

impl sqlx::Decode<'_, Postgres> for JobId {
    fn decode(
        value: <Postgres as sqlx::Database>::ValueRef<'_>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: Uuid = sqlx::Decode::<Postgres>::decode(value)?;
        Ok(Self(value))
    }
}
