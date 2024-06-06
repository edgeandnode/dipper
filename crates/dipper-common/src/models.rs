use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Key(pub String);

impl Into<Key> for &str {
    fn into(self) -> Key {
        Key::from_str(self)
    }
}

impl Key {
    pub fn from_str(key: &str) -> Self {
        Self(key.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Indexer {
    address: String,
}
