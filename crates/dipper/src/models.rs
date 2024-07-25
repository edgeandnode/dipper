use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Key(String);

impl Key {
    /// Create a new [`Key`] from a string.
    pub fn new<T: Into<String>>(value: T) -> Self {
        Key(value.into())
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Key(value.to_string())
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Indexer {
    id: String,
    url: String,
}
