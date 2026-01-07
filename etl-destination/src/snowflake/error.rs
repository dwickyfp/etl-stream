//! Snowflake error types.

use etl::error::{ErrorKind, EtlError};
use std::fmt;

/// Snowflake-specific error type.
#[derive(Debug)]
pub struct SnowflakeError {
    pub kind: SnowflakeErrorKind,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Categories of Snowflake errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnowflakeErrorKind {
    /// Python interpreter or PyO3 error
    PythonError,
    /// Connection error
    ConnectionError,
    /// Authentication error
    AuthenticationError,
    /// DDL operation error
    DdlError,
    /// Task management error
    TaskError,
    /// Streaming insert error
    StreamingError,
    /// Schema evolution error
    SchemaEvolutionError,
    /// Configuration error
    ConfigError,
}

impl SnowflakeError {
    /// Creates a new Snowflake error.
    pub fn new(kind: SnowflakeErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    /// Creates an error with a source.
    pub fn with_source(
        kind: SnowflakeErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Creates a Python error.
    pub fn python(message: impl Into<String>) -> Self {
        Self::new(SnowflakeErrorKind::PythonError, message)
    }

    /// Creates a connection error.
    pub fn connection(message: impl Into<String>) -> Self {
        Self::new(SnowflakeErrorKind::ConnectionError, message)
    }

    /// Creates a DDL error.
    pub fn ddl(message: impl Into<String>) -> Self {
        Self::new(SnowflakeErrorKind::DdlError, message)
    }

    /// Creates a streaming error.
    pub fn streaming(message: impl Into<String>) -> Self {
        Self::new(SnowflakeErrorKind::StreamingError, message)
    }
}

impl fmt::Display for SnowflakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{:?}] {}", self.kind, self.message)
    }
}

impl std::error::Error for SnowflakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Converts a Snowflake error to an ETL error.
pub fn snowflake_error_to_etl_error(err: SnowflakeError) -> EtlError {
    let kind = match err.kind {
        SnowflakeErrorKind::PythonError => ErrorKind::Unknown,
        SnowflakeErrorKind::ConnectionError => ErrorKind::Unknown,
        SnowflakeErrorKind::AuthenticationError => ErrorKind::Unknown,
        SnowflakeErrorKind::DdlError => ErrorKind::Unknown,
        SnowflakeErrorKind::TaskError => ErrorKind::Unknown,
        SnowflakeErrorKind::StreamingError => ErrorKind::Unknown,
        SnowflakeErrorKind::SchemaEvolutionError => ErrorKind::Unknown,
        SnowflakeErrorKind::ConfigError => ErrorKind::Unknown,
    };

    etl::etl_error!(kind, "Snowflake error", err.message)
}

#[cfg(feature = "pyo3")]
impl From<pyo3::PyErr> for SnowflakeError {
    fn from(err: pyo3::PyErr) -> Self {
        Self::new(SnowflakeErrorKind::PythonError, err.to_string())
    }
}
