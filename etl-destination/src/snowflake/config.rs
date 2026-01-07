//! Snowflake configuration module.

use std::path::PathBuf;

/// Configuration for Snowflake destination.
///
/// Contains all settings required to connect to Snowflake and configure
/// the landing table pattern with merge tasks.
#[derive(Debug, Clone)]
pub struct SnowflakeConfig {
    /// Snowflake account identifier (e.g., "xy12345.us-east-1")
    pub account: String,
    
    /// Snowflake username
    pub user: String,
    
    /// Target database name
    pub database: String,
    
    /// Target schema name for tables
    pub schema: String,
    
    /// Compute warehouse name
    pub warehouse: String,
    
    /// Path to private key file (.p8)
    pub private_key_path: PathBuf,
    
    /// Optional passphrase for encrypted private key
    pub private_key_passphrase: Option<String>,
    
    /// Schema for landing tables (default: "ETL_SCHEMA")
    pub landing_schema: String,
    
    /// Task schedule interval in minutes (default: 60)
    pub task_schedule_minutes: u64,
}

impl SnowflakeConfig {
    /// Creates a new Snowflake configuration with required fields.
    pub fn new(
        account: String,
        user: String,
        database: String,
        schema: String,
        warehouse: String,
        private_key_path: PathBuf,
    ) -> Self {
        Self {
            account,
            user,
            database,
            schema,
            warehouse,
            private_key_path,
            private_key_passphrase: None,
            landing_schema: "ETL_SCHEMA".to_string(),
            task_schedule_minutes: 60,
        }
    }

    /// Sets the private key passphrase.
    pub fn with_passphrase(mut self, passphrase: String) -> Self {
        self.private_key_passphrase = Some(passphrase);
        self
    }

    /// Sets the landing schema name.
    pub fn with_landing_schema(mut self, schema: String) -> Self {
        self.landing_schema = schema;
        self
    }

    /// Sets the task schedule interval in minutes.
    pub fn with_task_schedule(mut self, minutes: u64) -> Self {
        self.task_schedule_minutes = minutes;
        self
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.account.is_empty() {
            return Err("Account is required".to_string());
        }
        if self.user.is_empty() {
            return Err("User is required".to_string());
        }
        if self.database.is_empty() {
            return Err("Database is required".to_string());
        }
        if self.warehouse.is_empty() {
            return Err("Warehouse is required".to_string());
        }
        if !self.private_key_path.exists() {
            return Err(format!(
                "Private key file not found: {}",
                self.private_key_path.display()
            ));
        }
        Ok(())
    }
}

impl Default for SnowflakeConfig {
    fn default() -> Self {
        Self {
            account: String::new(),
            user: String::new(),
            database: String::new(),
            schema: "PUBLIC".to_string(),
            warehouse: String::new(),
            private_key_path: PathBuf::new(),
            private_key_passphrase: None,
            landing_schema: "ETL_SCHEMA".to_string(),
            task_schedule_minutes: 60,
        }
    }
}
