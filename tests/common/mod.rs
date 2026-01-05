// Common test utilities for ETL Stream E2E tests

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;

/// Test database settings - uses test-specific environment variables or defaults
pub struct TestDbSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
}

impl TestDbSettings {
    pub fn from_env() -> Self {
        Self {
            host: env::var("TEST_DB_HOST")
                .or_else(|_| env::var("CONFIG_DB_HOST"))
                .unwrap_or_else(|_| "localhost".to_string()),
            port: env::var("TEST_DB_PORT")
                .or_else(|_| env::var("CONFIG_DB_PORT"))
                .unwrap_or_else(|_| "5432".to_string())
                .parse()
                .unwrap_or(5432),
            database: env::var("TEST_DB_DATABASE")
                .or_else(|_| env::var("CONFIG_DB_DATABASE"))
                .unwrap_or_else(|_| "etl_config".to_string()),
            username: env::var("TEST_DB_USERNAME")
                .or_else(|_| env::var("CONFIG_DB_USERNAME"))
                .unwrap_or_else(|_| "postgres".to_string()),
            password: env::var("TEST_DB_PASSWORD")
                .or_else(|_| env::var("CONFIG_DB_PASSWORD"))
                .ok(),
        }
    }

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

/// Create a test database connection pool
pub async fn create_test_pool() -> PgPool {
    dotenvy::dotenv().ok();
    let settings = TestDbSettings::from_env();
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&settings.database_url())
        .await
        .expect("Failed to create test database pool")
}

/// Run migrations to ensure test tables exist
pub async fn setup_test_db(pool: &PgPool) {
    // Create tables if they don't exist
    sqlx::query(
        r#"
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
    .await
    .expect("Failed to create sources table");

    sqlx::query(
        r#"
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
    .await
    .expect("Failed to create destinations table");

    sqlx::query(
        r#"
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
    .await
    .expect("Failed to create pipelines table");
}

/// Clean up test data - clears all tables but keeps structure
#[allow(dead_code)]
pub async fn cleanup_test_data(pool: &PgPool) {
    // Delete in order due to foreign keys
    sqlx::query("DELETE FROM pipelines")
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM destinations")
        .execute(pool)
        .await
        .ok();
    sqlx::query("DELETE FROM sources")
        .execute(pool)
        .await
        .ok();
}

/// Generate a unique test name with UUID suffix
pub fn unique_name(prefix: &str) -> String {
    format!("{}_{}", prefix, uuid::Uuid::new_v4().to_string()[..8].to_string())
}

/// Test source data factory
pub struct TestSourceBuilder {
    pub name: String,
    pub pg_host: String,
    pub pg_port: i32,
    pub pg_database: String,
    pub pg_username: String,
    pub pg_password: Option<String>,
    pub pg_tls_enabled: bool,
    pub publication_name: String,
}

impl Default for TestSourceBuilder {
    fn default() -> Self {
        Self {
            name: unique_name("test_source"),
            pg_host: "localhost".to_string(),
            pg_port: 5432,
            pg_database: "test_db".to_string(),
            pg_username: "test_user".to_string(),
            pg_password: Some("test_password".to_string()),
            pg_tls_enabled: false,
            publication_name: "test_publication".to_string(),
        }
    }
}

#[allow(dead_code)]
impl TestSourceBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn with_unique_name(mut self, prefix: &str) -> Self {
        self.name = unique_name(prefix);
        self
    }

    pub async fn insert(self, pool: &PgPool) -> i32 {
        let row: (i32,) = sqlx::query_as(
            r#"
            INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, pg_password, pg_tls_enabled, publication_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
        )
        .bind(&self.name)
        .bind(&self.pg_host)
        .bind(self.pg_port)
        .bind(&self.pg_database)
        .bind(&self.pg_username)
        .bind(&self.pg_password)
        .bind(self.pg_tls_enabled)
        .bind(&self.publication_name)
        .fetch_one(pool)
        .await
        .expect("Failed to insert test source");
        row.0
    }
}

/// Test destination data factory
pub struct TestDestinationBuilder {
    pub name: String,
    pub destination_type: String,
    pub config: serde_json::Value,
}

impl Default for TestDestinationBuilder {
    fn default() -> Self {
        Self {
            name: unique_name("test_destination"),
            destination_type: "http".to_string(),
            config: serde_json::json!({
                "url": "http://localhost:8080/webhook",
                "timeout_ms": 30000,
                "retry_attempts": 3
            }),
        }
    }
}

#[allow(dead_code)]
impl TestDestinationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn with_unique_name(mut self, prefix: &str) -> Self {
        self.name = unique_name(prefix);
        self
    }

    pub fn with_url(mut self, url: &str) -> Self {
        self.config = serde_json::json!({
            "url": url,
            "timeout_ms": 30000,
            "retry_attempts": 3
        });
        self
    }

    pub async fn insert(self, pool: &PgPool) -> i32 {
        let row: (i32,) = sqlx::query_as(
            r#"
            INSERT INTO destinations (name, destination_type, config)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(&self.name)
        .bind(&self.destination_type)
        .bind(&self.config)
        .fetch_one(pool)
        .await
        .expect("Failed to insert test destination");
        row.0
    }
}

/// Test pipeline data factory
pub struct TestPipelineBuilder {
    pub name: String,
    pub source_id: i32,
    pub destination_id: i32,
    pub status: String,
    pub batch_max_size: i32,
    pub batch_max_fill_ms: i64,
    pub id_pipeline: i64,
}

#[allow(dead_code)]
impl TestPipelineBuilder {
    pub fn new(source_id: i32, destination_id: i32) -> Self {
        Self {
            name: unique_name("test_pipeline"),
            source_id,
            destination_id,
            status: "PAUSE".to_string(),
            batch_max_size: 1000,
            batch_max_fill_ms: 5000,
            id_pipeline: 0,
        }
    }

    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    pub fn with_unique_name(mut self, prefix: &str) -> Self {
        self.name = unique_name(prefix);
        self
    }

    pub fn with_status(mut self, status: &str) -> Self {
        self.status = status.to_string();
        self
    }

    pub fn with_id_pipeline(mut self, id_pipeline: i64) -> Self {
        self.id_pipeline = id_pipeline;
        self
    }

    pub async fn insert(self, pool: &PgPool) -> i32 {
        let row: (i32,) = sqlx::query_as(
            r#"
            INSERT INTO pipelines (name, source_id, destination_id, status, batch_max_size, batch_max_fill_ms, id_pipeline)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
        )
        .bind(&self.name)
        .bind(self.source_id)
        .bind(self.destination_id)
        .bind(&self.status)
        .bind(self.batch_max_size)
        .bind(self.batch_max_fill_ms)
        .bind(self.id_pipeline)
        .fetch_one(pool)
        .await
        .expect("Failed to insert test pipeline");
        row.0
    }
}
