use std::path::Path;

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::models::Key;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("bincode error: {0}")]
    Bincode(bincode::Error),
    #[error("sled error: {0}")]
    Sled(sled::Error),
}

/// Abstract over a database handle. Sled for now.
#[derive(Clone)]
pub struct DbHandle {
    inner: sled::Db,
}

impl DbHandle {
    pub async fn load_at(db_path: &Path) -> Result<Self, DbError> {
        let db = sled::open(db_path).map_err(DbError::Sled)?;
        Ok(Self { inner: db })
    }

    pub fn insert<T>(&self, key: &Key, value: &T) -> Result<(), DbError>
    where
        T: Serialize,
    {
        let value_serialized = bincode::serialize(value).map_err(DbError::Bincode)?;
        self.inner
            .insert(key, value_serialized)
            .map_err(DbError::Sled)?;
        Ok(())
    }

    pub fn get<T: DeserializeOwned>(&self, key: Key) -> Result<Option<T>, DbError> {
        let value = self.inner.get(key).map_err(DbError::Sled)?;
        let value = match value {
            Some(value) => value,
            None => return Ok(None),
        };
        let value: T = bincode::deserialize(&value).map_err(DbError::Bincode)?;
        Ok(Some(value))
    }

    pub async fn flush_async(&self) -> Result<usize, DbError> {
        let result = self.inner.flush_async().await;
        let flushed = result.map_err(DbError::Sled)?;
        Ok(flushed)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[smol_potat::test]
    async fn test_db() {
        let dir = tempdir().unwrap();
        let db = DbHandle::load_at(dir.path()).await.unwrap();
        let key = Key::new("key");
        let value = "value";
        db.insert(&key, &value).unwrap();
        let result: Option<String> = db.get(key.clone()).unwrap();
        assert_eq!(result, Some(value.to_string()));
    }
}
