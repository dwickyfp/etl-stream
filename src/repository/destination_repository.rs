use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::error::Error;

/// Destination configuration model
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Destination {
    pub id: i32,
    pub name: String,
    pub destination_type: String,
    pub config: JsonValue,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

/// Create a new destination
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CreateDestination {
    pub name: String,
    pub destination_type: String,
    pub config: JsonValue,
}

/// HTTP destination config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpDestinationConfig {
    pub url: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    #[serde(default = "default_retry")]
    pub retry_attempts: u32,
}

fn default_timeout() -> u64 {
    30000
}

fn default_retry() -> u32 {
    3
}

#[allow(dead_code)]
pub struct DestinationRepository;

#[allow(dead_code)]
impl DestinationRepository {
    /// Get all destinations
    pub async fn get_all(pool: &PgPool) -> Result<Vec<Destination>, Box<dyn Error>> {
        let destinations =
            sqlx::query_as::<_, Destination>("SELECT * FROM destinations ORDER BY id")
                .fetch_all(pool)
                .await?;
        Ok(destinations)
    }

    /// Get destination by ID
    pub async fn get_by_id(
        pool: &PgPool,
        id: i32,
    ) -> Result<Option<Destination>, Box<dyn Error>> {
        let destination =
            sqlx::query_as::<_, Destination>("SELECT * FROM destinations WHERE id = $1")
                .bind(id)
                .fetch_optional(pool)
                .await?;
        Ok(destination)
    }

    /// Create a new destination
    pub async fn create(
        pool: &PgPool,
        dest: CreateDestination,
    ) -> Result<Destination, Box<dyn Error>> {
        let created = sqlx::query_as::<_, Destination>(
            r#"
            INSERT INTO destinations (name, destination_type, config)
            VALUES ($1, $2, $3)
            RETURNING *
            "#,
        )
        .bind(&dest.name)
        .bind(&dest.destination_type)
        .bind(&dest.config)
        .fetch_one(pool)
        .await?;
        Ok(created)
    }

    /// Delete a destination by ID
    pub async fn delete(pool: &PgPool, id: i32) -> Result<bool, Box<dyn Error>> {
        let result = sqlx::query("DELETE FROM destinations WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
