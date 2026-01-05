use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::error::Error;

use super::destination_repository::Destination;
use super::source_repository::Source;

/// Pipeline status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineStatus {
    #[serde(rename = "START")]
    Start,
    #[serde(rename = "PAUSE")]
    Pause,
}

impl From<String> for PipelineStatus {
    fn from(s: String) -> Self {
        match s.to_uppercase().as_str() {
            "START" => PipelineStatus::Start,
            _ => PipelineStatus::Pause,
        }
    }
}

impl std::fmt::Display for PipelineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineStatus::Start => write!(f, "START"),
            PipelineStatus::Pause => write!(f, "PAUSE"),
        }
    }
}

/// Pipeline configuration model (raw from database)
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PipelineRow {
    pub id: i32,
    pub id_pipeline: i64,
    pub name: String,
    pub source_id: i32,
    pub destination_id: i32,
    pub status: String,
    pub batch_max_size: i32,
    pub batch_max_fill_ms: i64,
    pub table_error_retry_delay_ms: i64,
    pub table_error_retry_max_attempts: i32,
    pub max_table_sync_workers: i32,
    pub created_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
}

/// Pipeline with resolved source and destination
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub id: i32,
    pub name: String,
    pub source: Source,
    pub destination: Destination,
    pub status: PipelineStatus,
    pub batch_max_size: usize,
    pub batch_max_fill_ms: u64,
    pub table_error_retry_delay_ms: u64,
    pub table_error_retry_max_attempts: u32,
    pub max_table_sync_workers: usize,
}

/// Create a new pipeline
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct CreatePipeline {
    pub name: String,
    pub source_id: i32,
    pub destination_id: i32,
    pub status: Option<String>,
    pub batch_max_size: Option<i32>,
    pub batch_max_fill_ms: Option<i64>,
}

#[allow(dead_code)]
pub struct PipelineRepository;

#[allow(dead_code)]
impl PipelineRepository {
    /// Get all pipelines (raw rows)
    pub async fn get_all(pool: &PgPool) -> Result<Vec<PipelineRow>, Box<dyn Error>> {
        let pipelines = sqlx::query_as::<_, PipelineRow>("SELECT * FROM pipelines ORDER BY id")
            .fetch_all(pool)
            .await?;
        Ok(pipelines)
    }

    /// Get all active pipelines (status = 'START')
    pub async fn get_active(pool: &PgPool) -> Result<Vec<PipelineRow>, Box<dyn Error>> {
        let pipelines =
            sqlx::query_as::<_, PipelineRow>("SELECT * FROM pipelines WHERE status = 'START' ORDER BY id")
                .fetch_all(pool)
                .await?;
        Ok(pipelines)
    }

    /// Get pipeline by ID
    pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<PipelineRow>, Box<dyn Error>> {
        let pipeline = sqlx::query_as::<_, PipelineRow>("SELECT * FROM pipelines WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(pipeline)
    }

    /// Create a new pipeline
    pub async fn create(pool: &PgPool, pipeline: CreatePipeline) -> Result<PipelineRow, Box<dyn Error>> {
        let status = pipeline.status.unwrap_or_else(|| "PAUSE".to_string());
        let batch_max_size = pipeline.batch_max_size.unwrap_or(1000);
        let batch_max_fill_ms = pipeline.batch_max_fill_ms.unwrap_or(5000);

        let created = sqlx::query_as::<_, PipelineRow>(
            r#"
            INSERT INTO pipelines (name, source_id, destination_id, status, batch_max_size, batch_max_fill_ms)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            "#,
        )
        .bind(&pipeline.name)
        .bind(pipeline.source_id)
        .bind(pipeline.destination_id)
        .bind(&status)
        .bind(batch_max_size)
        .bind(batch_max_fill_ms)
        .fetch_one(pool)
        .await?;
        Ok(created)
    }

    /// Update pipeline status
    pub async fn update_status(
        pool: &PgPool,
        id: i32,
        status: PipelineStatus,
    ) -> Result<bool, Box<dyn Error>> {
        let result = sqlx::query("UPDATE pipelines SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(status.to_string())
            .bind(id)
            .execute(pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Delete a pipeline by ID
    pub async fn delete(pool: &PgPool, id: i32) -> Result<bool, Box<dyn Error>> {
        let result = sqlx::query("DELETE FROM pipelines WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
