use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;
use std::error::Error;
use tracing::warn;

/// Default configuration values
pub mod defaults {
    pub const CONFIG_DB_HOST: &str = "localhost";
    pub const CONFIG_DB_PORT: u16 = 5432;
    pub const CONFIG_DB_DATABASE: &str = "etl_config";
    pub const CONFIG_DB_USERNAME: &str = "postgres";
    
    pub const PIPELINE_POLL_INTERVAL_SECS: u64 = 5;
    
    pub const WAL_POLL_INTERVAL_SECS: u64 = 60;
    pub const ALERT_WARNING_WAL_MB: u64 = 3000;
    pub const ALERT_DANGER_WAL_MB: u64 = 6000;
    
    pub const ALERT_TIME_CHECK_MINS: u64 = 10;
}

/// Configuration database connection settings
#[derive(Debug, Clone)]
pub struct ConfigDbSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
}

impl ConfigDbSettings {
    /// Load configuration from environment variables with validation
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let port = env::var("CONFIG_DB_PORT")
            .map(|s| s.parse())
            .unwrap_or(Ok(defaults::CONFIG_DB_PORT))?;
        
        // Validate port is not 0 (u16 type already ensures it's <= 65535)
        if port == 0 {
            return Err("Invalid port number: 0. Port must be between 1 and 65535".into());
        }
        
        let host = env::var("CONFIG_DB_HOST")
            .unwrap_or_else(|_| defaults::CONFIG_DB_HOST.to_string());
        
        // Validate host is not empty
        if host.trim().is_empty() {
            return Err("CONFIG_DB_HOST cannot be empty".into());
        }
        
        let database = env::var("CONFIG_DB_DATABASE")
            .unwrap_or_else(|_| defaults::CONFIG_DB_DATABASE.to_string());
        
        // Validate database name
        if database.trim().is_empty() {
            return Err("CONFIG_DB_DATABASE cannot be empty".into());
        }
        
        let username = env::var("CONFIG_DB_USERNAME")
            .unwrap_or_else(|_| defaults::CONFIG_DB_USERNAME.to_string());
        
        // Validate username
        if username.trim().is_empty() {
            return Err("CONFIG_DB_USERNAME cannot be empty".into());
        }
        
        Ok(Self {
            host,
            port,
            database,
            username,
            password: env::var("CONFIG_DB_PASSWORD").ok(),
        })
    }

    /// Build database connection URL
    pub fn database_url(&self) -> String {
        match &self.password {
            Some(password) => format!(
                "postgres://{}:{}@{}:{}/{}",
                self.username, password, self.host, self.port, self.database
            ),
            None => format!(
                "postgres://{}@{}:{}/{}",
                self.username, self.host, self.port, self.database
            ),
        }
    }
}

/// Create a connection pool to the configuration database
pub async fn create_pool(settings: &ConfigDbSettings) -> Result<PgPool, Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database_url())
        .await?;
    Ok(pool)
}

/// Run database migrations to ensure tables exist
pub async fn run_migrations(pool: &PgPool) -> Result<(), Box<dyn Error>> {
    // Create tables if they don't exist
    sqlx::query(
        r#"
        -- Table 1: Sources (PostgreSQL connection configurations)
        CREATE TABLE IF NOT EXISTS sources (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            pg_host VARCHAR(255) NOT NULL,
            pg_port INTEGER NOT NULL DEFAULT 5432,
            pg_database VARCHAR(255) NOT NULL,
            pg_username VARCHAR(255) NOT NULL,
            pg_password VARCHAR(255),
            pg_tls_enabled BOOLEAN NOT NULL DEFAULT false,
            publication_name VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        -- Table 2: Destinations (flexible config with JSONB)
        -- Table 2: Destinations (flexible config with JSONB)
        CREATE TABLE IF NOT EXISTS destinations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            destination_type VARCHAR(50) NOT NULL, -- 'http', 'snowflake'

            -- Snowflake specific configuration
            snowflake_account VARCHAR(255),
            snowflake_user VARCHAR(255),
            snowflake_database VARCHAR(255),
            snowflake_schema VARCHAR(255),
            snowflake_warehouse VARCHAR(255),
            snowflake_role VARCHAR(255),
            snowflake_private_key_path VARCHAR(255),
            snowflake_private_key_passphrase VARCHAR(255),
            snowflake_landing_schema VARCHAR(255) DEFAULT 'ETL_SCHEMA',
            snowflake_task_schedule_minutes INTEGER DEFAULT 60,
            snowflake_host VARCHAR(255),

            -- HTTP specific configuration
            http_url VARCHAR(255),
            http_timeout_ms BIGINT DEFAULT 30000,
            http_retry_attempts INTEGER DEFAULT 3,

            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        -- Table 3: Pipelines (connects source to destination)
        CREATE TABLE IF NOT EXISTS pipelines (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
            destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
            status VARCHAR(20) NOT NULL DEFAULT 'PAUSE',
            batch_max_size INTEGER NOT NULL DEFAULT 1000,
            batch_max_fill_ms BIGINT NOT NULL DEFAULT 5000,
            table_error_retry_delay_ms BIGINT NOT NULL DEFAULT 10000,
            table_error_retry_max_attempts INTEGER NOT NULL DEFAULT 5,
            max_table_sync_workers INTEGER NOT NULL DEFAULT 4,
            id_pipeline BIGINT NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Create indexes if they don't exist
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_pipelines_source_id ON pipelines(source_id)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_pipelines_destination_id ON pipelines(destination_id)")
        .execute(pool)
        .await?;

    Ok(())
}

/// Pipeline manager settings
#[derive(Debug, Clone)]
pub struct PipelineManagerSettings {
    pub poll_interval_secs: u64,
}

impl PipelineManagerSettings {
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let poll_interval = env::var("PIPELINE_POLL_INTERVAL_SECS")
            .map(|s| s.parse())
            .unwrap_or(Ok(defaults::PIPELINE_POLL_INTERVAL_SECS))?;
        
        // Validate poll interval (must be positive and reasonable)
        if poll_interval == 0 {
            return Err("PIPELINE_POLL_INTERVAL_SECS must be greater than 0".into());
        }
        if poll_interval > 3600 {
            return Err("PIPELINE_POLL_INTERVAL_SECS must be 3600 seconds (1 hour) or less".into());
        }
        
        // Warn if poll interval is very aggressive
        if poll_interval < 5 {
            warn!("PIPELINE_POLL_INTERVAL_SECS is set to {} seconds, which may cause high database load", poll_interval);
        }
        
        Ok(Self {
            poll_interval_secs: poll_interval,
        })
    }
}

/// WAL monitor settings for tracking WAL size per source
#[derive(Debug, Clone)]
pub struct WalMonitorSettings {
    /// Poll interval in seconds for WAL size checks
    pub poll_interval_secs: u64,
    /// Warning threshold in MB
    pub warning_wal_mb: u64,
    /// Danger threshold in MB
    pub danger_wal_mb: u64,
}

impl WalMonitorSettings {
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let poll_interval = env::var("WAL_POLL_INTERVAL_SECS")
            .map(|s| s.parse())
            .unwrap_or(Ok(defaults::WAL_POLL_INTERVAL_SECS))?;
        let warning_wal_mb = env::var("WARNING_WAL")
            .map(|s| s.parse())
            .unwrap_or(Ok(defaults::ALERT_WARNING_WAL_MB))?;
        let danger_wal_mb = env::var("DANGER_WAL")
            .map(|s| s.parse())
            .unwrap_or(Ok(defaults::ALERT_DANGER_WAL_MB))?;
        
        // Validate poll interval
        if poll_interval == 0 {
            return Err("WAL_POLL_INTERVAL_SECS must be greater than 0".into());
        }
        if poll_interval > 86400 {
            return Err("WAL_POLL_INTERVAL_SECS must be 86400 seconds (24 hours) or less".into());
        }
        
        // Validate thresholds
        if warning_wal_mb == 0 {
            return Err("WARNING_WAL must be greater than 0 MB".into());
        }
        if danger_wal_mb == 0 {
            return Err("DANGER_WAL must be greater than 0 MB".into());
        }
        if danger_wal_mb <= warning_wal_mb {
            return Err(format!(
                "DANGER_WAL ({} MB) must be greater than WARNING_WAL ({} MB)",
                danger_wal_mb, warning_wal_mb
            ).into());
        }
        
        // Warn if thresholds seem unreasonably low
        if warning_wal_mb < 100 {
            warn!("WARNING_WAL is set to {} MB, which is very low and may cause frequent alerts", warning_wal_mb);
        }
        
        Ok(Self {
            poll_interval_secs: poll_interval,
            warning_wal_mb,
            danger_wal_mb,
        })
    }
}

/// Alert settings for WAL size webhook notifications
#[derive(Debug, Clone)]
pub struct AlertSettings {
    /// Webhook URL to send alerts (feature disabled if None)
    pub alert_wal_url: Option<String>,
    /// Time in minutes a warning/danger status must persist before alerting (default: 10)
    pub time_check_notification_mins: u64,
}

impl AlertSettings {
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        let time_check = env::var("TIME_CHECK_NOTIFICATION")
            .map(|s| s.parse())
            .unwrap_or(Ok(defaults::ALERT_TIME_CHECK_MINS))?;
        
        // Validate time check notification period
        if time_check == 0 {
            return Err("TIME_CHECK_NOTIFICATION must be greater than 0 minutes".into());
        }
        if time_check > 1440 {
            return Err("TIME_CHECK_NOTIFICATION must be 1440 minutes (24 hours) or less".into());
        }
        
        let alert_url = env::var("ALERT_WAL_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());
        
        // Validate URL format if provided
        if let Some(ref url) = alert_url {
            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(format!("ALERT_WAL_URL must be a valid HTTP/HTTPS URL, got: {}", url).into());
            }
        }
        
        Ok(Self {
            alert_wal_url: alert_url,
            time_check_notification_mins: time_check,
        })
    }

    /// Check if alerting is enabled
    pub fn is_enabled(&self) -> bool {
        self.alert_wal_url.is_some()
    }
}
