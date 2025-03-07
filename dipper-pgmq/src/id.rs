use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A job ID
///
/// This is a unique identifier for a job in the queue.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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

impl AsRef<Uuid> for JobId {
    fn as_ref(&self) -> &Uuid {
        &self.0
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
