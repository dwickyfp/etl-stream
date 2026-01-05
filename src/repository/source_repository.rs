use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::error::Error;

/// Source configuration model
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Source {
    pub id: i32,
    pub name: String,
    pub pg_host: String,
    pub pg_port: i32,
    pub pg_database: String,
    pub pg_username: String,
    pub pg_password: Option<String>,
    pub pg_tls_enabled: bool,
    pub publication_name: String,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

/// Create a new source
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CreateSource {
    pub name: String,
    pub pg_host: String,
    pub pg_port: i32,
    pub pg_database: String,
    pub pg_username: String,
    pub pg_password: Option<String>,
    pub pg_tls_enabled: bool,
    pub publication_name: String,
}

#[allow(dead_code)]
pub struct SourceRepository;

#[allow(dead_code)]
impl SourceRepository {
    /// Get all sources
    pub async fn get_all(pool: &PgPool) -> Result<Vec<Source>, Box<dyn Error>> {
        let sources = sqlx::query_as::<_, Source>("SELECT * FROM sources ORDER BY id")
            .fetch_all(pool)
            .await?;
        Ok(sources)
    }

    /// Get source by ID
    pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<Source>, Box<dyn Error>> {
        let source = sqlx::query_as::<_, Source>("SELECT * FROM sources WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(source)
    }

    /// Create a new source
    pub async fn create(pool: &PgPool, source: CreateSource) -> Result<Source, Box<dyn Error>> {
        let created = sqlx::query_as::<_, Source>(
            r#"
            INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, pg_password, pg_tls_enabled, publication_name)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
            "#,
        )
        .bind(&source.name)
        .bind(&source.pg_host)
        .bind(source.pg_port)
        .bind(&source.pg_database)
        .bind(&source.pg_username)
        .bind(&source.pg_password)
        .bind(source.pg_tls_enabled)
        .bind(&source.publication_name)
        .fetch_one(pool)
        .await?;
        Ok(created)
    }

    /// Delete a source by ID
    pub async fn delete(pool: &PgPool, id: i32) -> Result<bool, Box<dyn Error>> {
        let result = sqlx::query("DELETE FROM sources WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
