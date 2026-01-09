use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use sqlx::PgPool;
use std::error::Error;

/// Destination configuration model
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Destination {
    pub id: i32,
    pub name: String,
    pub destination_type: String,

    // Snowflake config
    pub snowflake_account: Option<String>,
    pub snowflake_user: Option<String>,
    pub snowflake_database: Option<String>,
    pub snowflake_schema: Option<String>,
    pub snowflake_warehouse: Option<String>,
    pub snowflake_role: Option<String>,
    pub snowflake_private_key_path: Option<String>,
    pub snowflake_private_key_passphrase: Option<String>,
    pub snowflake_landing_schema: Option<String>,
    pub snowflake_task_schedule_minutes: Option<i32>,
    pub snowflake_host: Option<String>,

    // HTTP config
    pub http_url: Option<String>,
    pub http_timeout_ms: Option<i64>,
    pub http_retry_attempts: Option<i32>,

    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

/// Create a new destination
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CreateDestination {
    pub name: String,
    pub destination_type: String,

    // Snowflake config
    pub snowflake_account: Option<String>,
    pub snowflake_user: Option<String>,
    pub snowflake_database: Option<String>,
    pub snowflake_schema: Option<String>,
    pub snowflake_warehouse: Option<String>,
    pub snowflake_role: Option<String>,
    pub snowflake_private_key_path: Option<String>,
    pub snowflake_private_key_passphrase: Option<String>,
    pub snowflake_landing_schema: Option<String>,
    pub snowflake_task_schedule_minutes: Option<i32>,
    pub snowflake_host: Option<String>,

    // HTTP config
    pub http_url: Option<String>,
    pub http_timeout_ms: Option<i64>,
    pub http_retry_attempts: Option<i32>,
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
            INSERT INTO destinations (
                name, destination_type,
                snowflake_account, snowflake_user, snowflake_database, snowflake_schema,
                snowflake_warehouse, snowflake_role, snowflake_private_key_path,
                snowflake_private_key_passphrase, snowflake_landing_schema,
                snowflake_task_schedule_minutes, snowflake_host,
                http_url, http_timeout_ms, http_retry_attempts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            RETURNING *
            "#,
        )
        .bind(&dest.name)
        .bind(&dest.destination_type)
        .bind(&dest.snowflake_account)
        .bind(&dest.snowflake_user)
        .bind(&dest.snowflake_database)
        .bind(&dest.snowflake_schema)
        .bind(&dest.snowflake_warehouse)
        .bind(&dest.snowflake_role)
        .bind(&dest.snowflake_private_key_path)
        .bind(&dest.snowflake_private_key_passphrase)
        .bind(&dest.snowflake_landing_schema)
        .bind(&dest.snowflake_task_schedule_minutes)
        .bind(&dest.snowflake_host)
        .bind(&dest.http_url)
        .bind(&dest.http_timeout_ms)
        .bind(&dest.http_retry_attempts)
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
