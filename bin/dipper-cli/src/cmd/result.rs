/// The result type for CLI commands
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The default error code for CLI commands
const DEFAULT_ERROR_CODE: i32 = 1;

/// The error type for CLI commands
#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct Error {
    /// The error code
    code: i32,
    /// The error message
    #[source]
    source: anyhow::Error,
}

impl Error {
    /// Create a new [`Error`] instance
    ///
    /// The default error code is 1.
    pub fn new(err: impl Into<anyhow::Error> + Send + Sync + 'static) -> Self {
        Self {
            code: DEFAULT_ERROR_CODE,
            source: err.into(),
        }
    }

    /// Set the error code
    ///
    /// # Panic
    ///
    /// Panics if the error code is zero, as zero is reserved for success.
    pub fn with_code(mut self, code: i32) -> Self {
        if code == 0 {
            panic!("Error code must be non-zero");
        }

        self.code = code;
        self
    }

    /// Get the error code
    pub fn code(&self) -> i32 {
        self.code
    }
}

impl From<anyhow::Error> for Error {
    #[inline]
    fn from(value: anyhow::Error) -> Self {
        Self::new(value)
    }
}

impl From<(i32, anyhow::Error)> for Error {
    #[inline]
    fn from((code, err): (i32, anyhow::Error)) -> Self {
        Self::new(err).with_code(code)
    }
}
