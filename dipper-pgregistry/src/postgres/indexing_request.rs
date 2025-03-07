//! # Indexing request
//!
//! Indexing Requests are initiated by the customer and are used to request indexing services
//! from indexers. The DIPs Gateway service (Dipper) is responsible for finding appropriate
//! indexers to fulfill the request.

use num_traits::ToPrimitive;
use sqlx::{
    Error, Postgres, Row as _,
    encode::IsNull,
    error::BoxDynError,
    postgres::{PgArgumentBuffer, PgRow, PgTypeInfo, PgValueRef},
};

use super::common::{PgAddress, PgDeploymentId, PgU64};
use crate::indexing_request::{IndexingRequest, Status};

impl sqlx::FromRow<'_, PgRow> for IndexingRequest {
    fn from_row(row: &'_ PgRow) -> Result<Self, Error> {
        let id = row.try_get("id")?;
        let created_at = row.try_get("created_at")?;
        let updated_at = row.try_get("updated_at")?;
        let status = row.try_get("status")?;
        let PgAddress(requested_by) = row.try_get("requested_by")?;
        let PgDeploymentId(deployment_id) = row.try_get("deployment_id")?;
        let PgU64(deployment_chain_id) = row.try_get("deployment_chain_id")?;

        Ok(Self {
            id,
            created_at,
            updated_at,
            status,
            requested_by,
            deployment_id,
            deployment_chain_id,
        })
    }
}

impl sqlx::Type<Postgres> for Status {
    fn type_info() -> PgTypeInfo {
        <i32 as sqlx::Type<Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, Postgres> for Status {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        let value: i32 = self.to_i32().ok_or(Error::Encode(
            "enum variant value out of range for i32".into(),
        ))?;
        <i32 as sqlx::Encode<Postgres>>::encode_by_ref(&value, buf)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for Status {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        let value: i32 = sqlx::Decode::<Postgres>::decode(value)?;
        let value = num_traits::FromPrimitive::from_i32(value).unwrap_or(Status::Unknown);
        Ok(value)
    }
}
