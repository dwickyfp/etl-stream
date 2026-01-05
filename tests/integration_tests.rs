// Integration E2E tests
mod common;

use common::*;
use serial_test::serial;

// ============================================================================
// Full Pipeline Lifecycle Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_full_pipeline_lifecycle() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    // 1. Create source
    let source_id = TestSourceBuilder::new()
        .with_unique_name("lifecycle_source")
        .insert(&pool)
        .await;
    assert!(source_id > 0, "Source should be created");

    // 2. Create destination
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("lifecycle_dest")
        .with_url("http://localhost:9999/webhook")
        .insert(&pool)
        .await;
    assert!(dest_id > 0, "Destination should be created");

    // 3. Create pipeline
    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("lifecycle_pipeline")
        .with_id_pipeline(100001)
        .insert(&pool)
        .await;
    assert!(pipeline_id > 0, "Pipeline should be created");

    // 4. Verify initial state is PAUSE
    let row: (String,) = sqlx::query_as("SELECT status FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to query pipeline status");
    assert_eq!(row.0, "PAUSE");

    // 5. Start pipeline
    sqlx::query("UPDATE pipelines SET status = 'START' WHERE id = $1")
        .bind(pipeline_id)
        .execute(&pool)
        .await
        .expect("Failed to start pipeline");

    let row: (String,) = sqlx::query_as("SELECT status FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to query pipeline status");
    assert_eq!(row.0, "START");

    // 6. Stop pipeline
    sqlx::query("UPDATE pipelines SET status = 'PAUSE' WHERE id = $1")
        .bind(pipeline_id)
        .execute(&pool)
        .await
        .expect("Failed to pause pipeline");

    let row: (String,) = sqlx::query_as("SELECT status FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to query pipeline status");
    assert_eq!(row.0, "PAUSE");

    // 7. Delete pipeline
    sqlx::query("DELETE FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .execute(&pool)
        .await
        .expect("Failed to delete pipeline");

    let row: Option<(i32,)> = sqlx::query_as("SELECT id FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .fetch_optional(&pool)
        .await
        .expect("Failed to query pipeline");
    assert!(row.is_none(), "Pipeline should be deleted");

    // 8. Delete destination
    sqlx::query("DELETE FROM destinations WHERE id = $1")
        .bind(dest_id)
        .execute(&pool)
        .await
        .expect("Failed to delete destination");

    // 9. Delete source
    sqlx::query("DELETE FROM sources WHERE id = $1")
        .bind(source_id)
        .execute(&pool)
        .await
        .expect("Failed to delete source");

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Pipeline Status Sync Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_pipeline_status_transitions() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("status_trans_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("status_trans_dest")
        .insert(&pool)
        .await;
    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("status_trans_pipeline")
        .insert(&pool)
        .await;

    // PAUSE -> START -> PAUSE -> START -> PAUSE
    let transitions = vec!["START", "PAUSE", "START", "PAUSE"];

    for status in transitions {
        sqlx::query("UPDATE pipelines SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(status)
            .bind(pipeline_id)
            .execute(&pool)
            .await
            .expect("Failed to update status");

        let row: (String,) = sqlx::query_as("SELECT status FROM pipelines WHERE id = $1")
            .bind(pipeline_id)
            .fetch_one(&pool)
            .await
            .expect("Failed to query status");
        assert_eq!(row.0, status);
    }

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_get_active_pipelines_after_status_change() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("active_check_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("active_check_dest")
        .insert(&pool)
        .await;

    // Create 3 pipelines, all initially paused
    let p1 = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("active_p1")
        .insert(&pool)
        .await;
    let _p2 = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("active_p2")
        .insert(&pool)
        .await;
    let p3 = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("active_p3")
        .insert(&pool)
        .await;

    // Initially all paused
    let active: Vec<(i32,)> = sqlx::query_as("SELECT id FROM pipelines WHERE status = 'START'")
        .fetch_all(&pool)
        .await
        .expect("Query failed");
    assert_eq!(active.len(), 0);

    // Start p1 and p3
    sqlx::query("UPDATE pipelines SET status = 'START' WHERE id IN ($1, $2)")
        .bind(p1)
        .bind(p3)
        .execute(&pool)
        .await
        .expect("Update failed");

    let active: Vec<(i32,)> = sqlx::query_as("SELECT id FROM pipelines WHERE status = 'START' ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("Query failed");
    assert_eq!(active.len(), 2);

    // Stop p1
    sqlx::query("UPDATE pipelines SET status = 'PAUSE' WHERE id = $1")
        .bind(p1)
        .execute(&pool)
        .await
        .expect("Update failed");

    let active: Vec<(i32,)> = sqlx::query_as("SELECT id FROM pipelines WHERE status = 'START'")
        .fetch_all(&pool)
        .await
        .expect("Query failed");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, p3);

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Multiple Pipelines Same Source Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_multiple_pipelines_same_source() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    // One source, multiple destinations
    let source_id = TestSourceBuilder::new()
        .with_unique_name("shared_source")
        .insert(&pool)
        .await;

    let dest1_id = TestDestinationBuilder::new()
        .with_unique_name("multi_dest_1")
        .with_url("http://localhost:8001/webhook")
        .insert(&pool)
        .await;
    let dest2_id = TestDestinationBuilder::new()
        .with_unique_name("multi_dest_2")
        .with_url("http://localhost:8002/webhook")
        .insert(&pool)
        .await;
    let dest3_id = TestDestinationBuilder::new()
        .with_unique_name("multi_dest_3")
        .with_url("http://localhost:8003/webhook")
        .insert(&pool)
        .await;

    // Create pipelines from same source to different destinations
    let _p1 = TestPipelineBuilder::new(source_id, dest1_id)
        .with_unique_name("multi_pipeline_1")
        .with_id_pipeline(200001)
        .insert(&pool)
        .await;
    let _p2 = TestPipelineBuilder::new(source_id, dest2_id)
        .with_unique_name("multi_pipeline_2")
        .with_id_pipeline(200002)
        .insert(&pool)
        .await;
    let _p3 = TestPipelineBuilder::new(source_id, dest3_id)
        .with_unique_name("multi_pipeline_3")
        .with_id_pipeline(200003)
        .insert(&pool)
        .await;

    // Verify all pipelines share the same source
    let pipelines: Vec<(i32, i32, i64)> = sqlx::query_as(
        "SELECT id, source_id, id_pipeline FROM pipelines WHERE source_id = $1 ORDER BY id"
    )
    .bind(source_id)
    .fetch_all(&pool)
    .await
    .expect("Query failed");

    assert_eq!(pipelines.len(), 3);
    assert!(pipelines.iter().all(|(_, src, _)| *src == source_id));
    
    // Verify different id_pipelines
    let id_pipelines: Vec<i64> = pipelines.iter().map(|(_, _, idp)| *idp).collect();
    assert_eq!(id_pipelines, vec![200001, 200002, 200003]);

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Cascade Delete Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_source_delete_cascades_to_pipelines() {
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

    // Create multiple pipelines
    let p1 = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("cascade_p1")
        .insert(&pool)
        .await;
    let p2 = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("cascade_p2")
        .insert(&pool)
        .await;

    // Verify pipelines exist
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pipelines WHERE source_id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .expect("Query failed");
    assert_eq!(count.0, 2);

    // Delete source
    sqlx::query("DELETE FROM sources WHERE id = $1")
        .bind(source_id)
        .execute(&pool)
        .await
        .expect("Delete failed");

    // Pipelines should be gone
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pipelines WHERE id IN ($1, $2)")
        .bind(p1)
        .bind(p2)
        .fetch_one(&pool)
        .await
        .expect("Query failed");
    assert_eq!(count.0, 0);

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_destination_delete_cascades_to_pipelines() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("dest_cascade_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("dest_cascade_dest")
        .insert(&pool)
        .await;

    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("dest_cascade_pipeline")
        .insert(&pool)
        .await;

    // Delete destination
    sqlx::query("DELETE FROM destinations WHERE id = $1")
        .bind(dest_id)
        .execute(&pool)
        .await
        .expect("Delete failed");

    // Pipeline should be gone
    let row: Option<(i32,)> = sqlx::query_as("SELECT id FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .fetch_optional(&pool)
        .await
        .expect("Query failed");
    assert!(row.is_none());

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Data Integrity Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_cannot_create_pipeline_with_invalid_source() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("invalid_source_dest")
        .insert(&pool)
        .await;

    let result = sqlx::query(
        "INSERT INTO pipelines (name, source_id, destination_id) VALUES ($1, $2, $3)"
    )
    .bind("invalid_source_pipeline")
    .bind(99999) // Non-existent source
    .bind(dest_id)
    .execute(&pool)
    .await;

    assert!(result.is_err(), "Should fail with invalid source_id");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_cannot_create_pipeline_with_invalid_destination() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("invalid_dest_source")
        .insert(&pool)
        .await;

    let result = sqlx::query(
        "INSERT INTO pipelines (name, source_id, destination_id) VALUES ($1, $2, $3)"
    )
    .bind("invalid_dest_pipeline")
    .bind(source_id)
    .bind(99999) // Non-existent destination
    .execute(&pool)
    .await;

    assert!(result.is_err(), "Should fail with invalid destination_id");

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_id_pipeline_is_distinct_from_id() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("distinct_id_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("distinct_id_dest")
        .insert(&pool)
        .await;

    // Create pipeline with specific id_pipeline
    let custom_id_pipeline: i64 = 9876543210;
    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("distinct_id_pipeline")
        .with_id_pipeline(custom_id_pipeline)
        .insert(&pool)
        .await;

    let row: (i32, i64) = sqlx::query_as("SELECT id, id_pipeline FROM pipelines WHERE id = $1")
        .bind(pipeline_id)
        .fetch_one(&pool)
        .await
        .expect("Query failed");

    let (id, id_pipeline) = row;
    assert_ne!(id as i64, id_pipeline, "id and id_pipeline should be different");
    assert_eq!(id_pipeline, custom_id_pipeline);

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Batch Settings Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_pipeline_batch_settings() {
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

    // Create with default batch settings
    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("batch_pipeline")
        .insert(&pool)
        .await;

    let row: (i32, i64, i64, i32, i32) = sqlx::query_as(
        "SELECT batch_max_size, batch_max_fill_ms, table_error_retry_delay_ms, table_error_retry_max_attempts, max_table_sync_workers FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(row.0, 1000); // batch_max_size
    assert_eq!(row.1, 5000); // batch_max_fill_ms
    assert_eq!(row.2, 10000); // table_error_retry_delay_ms
    assert_eq!(row.3, 5); // table_error_retry_max_attempts
    assert_eq!(row.4, 4); // max_table_sync_workers

    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_update_pipeline_batch_settings() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("update_batch_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("update_batch_dest")
        .insert(&pool)
        .await;

    let pipeline_id = TestPipelineBuilder::new(source_id, dest_id)
        .with_unique_name("update_batch_pipeline")
        .insert(&pool)
        .await;

    // Update batch settings
    sqlx::query(
        "UPDATE pipelines SET batch_max_size = 500, batch_max_fill_ms = 2000 WHERE id = $1"
    )
    .bind(pipeline_id)
    .execute(&pool)
    .await
    .expect("Update failed");

    let row: (i32, i64) = sqlx::query_as(
        "SELECT batch_max_size, batch_max_fill_ms FROM pipelines WHERE id = $1"
    )
    .bind(pipeline_id)
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(row.0, 500);
    assert_eq!(row.1, 2000);

    cleanup_test_data(&pool).await;
}

// ============================================================================
// Concurrent Operations Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_concurrent_pipeline_creation() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;
    cleanup_test_data(&pool).await;

    let source_id = TestSourceBuilder::new()
        .with_unique_name("concurrent_source")
        .insert(&pool)
        .await;
    let dest_id = TestDestinationBuilder::new()
        .with_unique_name("concurrent_dest")
        .insert(&pool)
        .await;

    // Create multiple pipelines concurrently
    let handles: Vec<_> = (0..5)
        .map(|i| {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                TestPipelineBuilder::new(source_id, dest_id)
                    .with_unique_name(&format!("concurrent_p_{}", i))
                    .with_id_pipeline(i as i64 + 300000)
                    .insert(&pool_clone)
                    .await
            })
        })
        .collect();

    for handle in handles {
        let result = handle.await;
        assert!(result.is_ok());
    }

    // Verify all created
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pipelines WHERE source_id = $1")
        .bind(source_id)
        .fetch_one(&pool)
        .await
        .expect("Query failed");
    assert_eq!(count.0, 5);

    cleanup_test_data(&pool).await;
}
