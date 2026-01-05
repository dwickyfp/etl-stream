use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;
use std::error::Error;

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
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            host: env::var("CONFIG_DB_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("CONFIG_DB_PORT")
                .unwrap_or_else(|_| "5432".to_string())
                .parse()?,
            database: env::var("CONFIG_DB_DATABASE")
                .unwrap_or_else(|_| "etl_config".to_string()),
            username: env::var("CONFIG_DB_USERNAME")
                .unwrap_or_else(|_| "postgres".to_string()),
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
        CREATE TABLE IF NOT EXISTS destinations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            destination_type VARCHAR(50) NOT NULL,
            config JSONB NOT NULL,
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
    pub fn from_env() -> Self {
        Self {
            poll_interval_secs: env::var("PIPELINE_POLL_INTERVAL_SECS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
        }
    }
}
