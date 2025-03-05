/// Result type for the registry.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can occur when interacting with the [`Registry`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The DB update query failed as no records matching the criteria were found.
    #[error("No records were updated")]
    NoRecordsUpdated,

    /// An error occurred while interacting with the database.
    #[error(transparent)]
    DbError(#[from] sqlx::Error),
}
