//! Snowflake destination module.
//!
//! Provides Snowflake destination support for ETL pipeline using PyO3 to
//! bridge Rust with Python's Snowpipe Streaming SDK.

mod bridge;
mod config;
mod core;
mod error;

pub use config::SnowflakeConfig;
pub use core::SnowflakeDestination;
pub use error::SnowflakeError;
