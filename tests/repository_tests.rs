// Repository layer E2E tests
mod common;

use common::*;
use serial_test::serial;

// ============================================================================
// Source Repository Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_create_source() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_name = unique_name("create_source");
    let source_id = TestSourceBuilder::new()
        .with_name(&source_name)
        .insert(&pool)
        .await;

    assert!(source_id > 0);

    // Verify the source was created
    let row: Option<(i32, String)> = sqlx::query_as(
        "SELECT id, name FROM sources WHERE id = $1"
    )
    .bind(source_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_some());
    let (id, name) = row.unwrap();
    assert_eq!(id, source_id);
    assert_eq!(name, source_name);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_get_source_by_id() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("get_source")
        .insert(&pool)
        .await;

    let row: Option<(i32, String, String, i32)> = sqlx::query_as(
        "SELECT id, name, pg_host, pg_port FROM sources WHERE id = $1"
    )
    .bind(source_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_some());
    let (id, _name, host, port) = row.unwrap();
    assert_eq!(id, source_id);
    assert_eq!(host, "localhost");
    assert_eq!(port, 5432);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_get_source_not_found() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let row: Option<(i32,)> = sqlx::query_as(
        "SELECT id FROM sources WHERE id = $1"
    )
    .bind(99999)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_none());
}

#[tokio::test]
#[serial]
async fn test_get_all_sources() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    // Create multiple sources
    TestSourceBuilder::new()
        .with_unique_name("all_source_1")
        .insert(&pool)
        .await;
    TestSourceBuilder::new()
        .with_unique_name("all_source_2")
        .insert(&pool)
        .await;
    TestSourceBuilder::new()
        .with_unique_name("all_source_3")
        .insert(&pool)
        .await;

    let rows: Vec<(i32,)> = sqlx::query_as(
        "SELECT id FROM sources ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .expect("Query failed");

    assert_eq!(rows.len(), 3);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_delete_source() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("delete_source")
        .insert(&pool)
        .await;

    // Delete the source
    let result = sqlx::query("DELETE FROM sources WHERE id = $1")
        .bind(source_id)
        .execute(&pool)
        .await
        .expect("Delete failed");

    assert_eq!(result.rows_affected(), 1);

    // Verify deletion
    let row: Option<(i32,)> = sqlx::query_as(
        "SELECT id FROM sources WHERE id = $1"
    )
    .bind(source_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_none());
}

#[tokio::test]
#[serial]
async fn test_create_duplicate_source_name() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_name = unique_name("duplicate_source");
    
    // Create first source
    TestSourceBuilder::new()
        .with_name(&source_name)
        .insert(&pool)
        .await;

    // Try to create duplicate
    let result = sqlx::query(
        r#"
        INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, publication_name)
        VALUES ($1, 'localhost', 5432, 'test', 'test', 'test_pub')
        "#,
    )
    .bind(&source_name)
    .execute(&pool)
    .await;

    assert!(result.is_err());

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Destination Repository Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_create_destination_http() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let dest_name = unique_name("http_destination");
    let dest_id = TestDestinationBuilder::new()
        .with_name(&dest_name)
        .with_url("http://example.com/webhook")
        .insert(&pool)
        .await;

    assert!(dest_id > 0);

    // Verify the destination was created with correct config
    let row: Option<(i32, String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT id, name, destination_type, config FROM destinations WHERE id = $1"
    )
    .bind(dest_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_some());
    let (id, name, dest_type, config) = row.unwrap();
    assert_eq!(id, dest_id);
    assert_eq!(name, dest_name);
    assert_eq!(dest_type, "http");
    assert_eq!(config["url"], "http://example.com/webhook");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_destination_config_parsing() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("config_parse")
        .insert(&pool)
        .await;

    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT config FROM destinations WHERE id = $1"
    )
    .bind(dest_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_some());
    let (config,) = row.unwrap();
    
    // Verify config structure
    assert!(config.get("url").is_some());
    assert!(config.get("timeout_ms").is_some());
    assert!(config.get("retry_attempts").is_some());

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_destination_default_values() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("default_values")
        .insert(&pool)
        .await;

    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT config FROM destinations WHERE id = $1"
    )
    .bind(dest_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    let (config,) = row.unwrap();
    assert_eq!(config["timeout_ms"], 30000);
    assert_eq!(config["retry_attempts"], 3);

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Pipeline Repository Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_create_pipeline() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    // Create source and destination first
    let source_id = TestSourceBuilder::new()
        .with_unique_name("pipeline_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("pipeline_dest")
        .insert(&pool)
        .await;

    let pipeline_name = unique_name("test_pipeline");
    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_name(&pipeline_name)
        .insert(&pool)
        .await;

    assert!(pipeline_id > 0);

    // Verify the pipeline was created
    let row: Option<(i32, String, i32, i32)> = sqlx::query_as(
        "SELECT id, name, source_id, destination_id FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_some());
    let (id, name, src_id, dst_id) = row.unwrap();
    assert_eq!(id, pipeline_id);
    assert_eq!(name, pipeline_name);
    assert_eq!(src_id, source_id);
    assert_eq!(dst_id, dest_id);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_pipeline_default_status() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("status_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("status_dest")
        .insert(&pool)
        .await;

    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("default_status")
        .insert(&pool)
        .await;

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    let (status,) = row.unwrap();
    assert_eq!(status, "PAUSE");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_pipeline_default_batch_settings() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("batch_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("batch_dest")
        .insert(&pool)
        .await;

    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("batch_settings")
        .insert(&pool)
        .await;

    let row: Option<(i32, i64)> = sqlx::query_as(
        "SELECT batch_max_size, batch_max_fill_ms FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    let (batch_max_size, batch_max_fill_ms) = row.unwrap();
    assert_eq!(batch_max_size, 1000);
    assert_eq!(batch_max_fill_ms, 5000);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_update_pipeline_status() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("update_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("update_dest")
        .insert(&pool)
        .await;

    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("update_status")
        .insert(&pool)
        .await;

    // Update status to START
    sqlx::query("UPDATE pipelines SET status = 'START' WHERE id = $1")
        .bind(pipeline_id)
        .execute(&pool)
        .await
        .expect("Update failed");

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    let (status,) = row.unwrap();
    assert_eq!(status, "START");

    // Update back to PAUSE
    sqlx::query("UPDATE pipelines SET status = 'PAUSE' WHERE id = $1")
        .bind(pipeline_id)
        .execute(&pool)
        .await
        .expect("Update failed");

    let row: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    let (status,) = row.unwrap();
    assert_eq!(status, "PAUSE");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_get_active_pipelines() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("active_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("active_dest")
        .insert(&pool)
        .await;

    // Create 2 active and 1 paused pipeline
    TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("active_1")
        .with_status("START")
        .insert(&pool)
        .await;
    TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("active_2")
        .with_status("START")
        .insert(&pool)
        .await;
    TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("paused_1")
        .with_status("PAUSE")
        .insert(&pool)
        .await;

    let active: Vec<(i32,)> = sqlx::query_as(
        "SELECT id FROM pipelines WHERE status = 'START'"
    )
    .fetch_all(&pool)
    .await
    .expect("Query failed");

    assert_eq!(active.len(), 2);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_pipeline_cascade_delete() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("cascade_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("cascade_dest")
        .insert(&pool)
        .await;

    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("cascade_pipeline")
        .insert(&pool)
        .await;

    // Delete the source - should cascade to pipeline
    sqlx::query("DELETE FROM sources WHERE id = $1")
        .bind(source_id)
        .execute(&pool)
        .await
        .expect("Delete failed");

    // Pipeline should be gone
    let row: Option<(i32,)> = sqlx::query_as(
        "SELECT id FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    assert!(row.is_none());

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_id_pipeline_field() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("id_pipeline_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("id_pipeline_dest")
        .insert(&pool)
        .await;

    let custom_id_pipeline: i64 = 12345678;
    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("id_pipeline_test")
        .with_id_pipeline(custom_id_pipeline)
        .insert(&pool)
        .await;

    let row: Option<(i32, i64)> = sqlx::query_as(
        "SELECT id, id_pipeline FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_optional(&pool)
    .await
    .expect("Query failed");

    let (id, id_pipeline) = row.unwrap();
    assert_ne!(id as i64, id_pipeline); // id and id_pipeline should be different
    assert_eq!(id_pipeline, custom_id_pipeline);

    cleanup_test_data(&pool).await;
}
