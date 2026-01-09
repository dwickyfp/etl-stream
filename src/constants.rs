//! Application-wide constants and configuration values
//!
//! This module centralizes magic numbers, strings, and other constants
//! that were previously scattered throughout the codebase.

/// Snowflake-specific constants
pub mod snowflake {
    /// Default landing schema name for staging tables
    #[allow(dead_code)]
    pub const DEFAULT_LANDING_SCHEMA: &str = "ETL_SCHEMA";
    
    /// Prefix for landing tables
    #[allow(dead_code)]
    pub const LANDING_TABLE_PREFIX: &str = "LANDING_";
    
    /// Suffix for streaming pipe names
    #[allow(dead_code)]
    pub const STREAMING_PIPE_SUFFIX: &str = "-STREAMING";
    
    /// Default task schedule interval in minutes
    #[allow(dead_code)]
    pub const DEFAULT_TASK_SCHEDULE_MINUTES: u64 = 60;
}

/// Redis-specific constants
pub mod redis {
    /// Default Redis connection timeout in seconds
    #[allow(dead_code)]
    pub const DEFAULT_TIMEOUT_SECS: u64 = 5;
    
    /// Key prefix for ETL data
    #[allow(dead_code)]
    pub const DEFAULT_KEY_PREFIX: &str = "etl";
}

/// Database connection constants
pub mod database {
    /// Default PostgreSQL port
    #[allow(dead_code)]
    pub const DEFAULT_PG_PORT: u16 = 5432;
    
    /// Maximum pool size for source database connections
    #[allow(dead_code)]
    pub const SOURCE_POOL_MAX_CONNECTIONS: u32 = 2;
    
    /// Connection acquire timeout in seconds
    #[allow(dead_code)]
    pub const CONNECTION_TIMEOUT_SECS: u64 = 5;
}

/// Alert and monitoring constants
pub mod monitoring {
    /// Maximum retry attempts for webhook alerts
    pub const MAX_ALERT_RETRIES: u32 = 3;
    
    /// Base delay for exponential backoff in seconds
    pub const ALERT_RETRY_BASE_DELAY_SECS: u64 = 1;
    
    /// HTTP timeout for webhook requests in seconds
    pub const ALERT_HTTP_TIMEOUT_SECS: u64 = 10;
}
