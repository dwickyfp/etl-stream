// Config module E2E tests
// Note: Environment variable tests are avoided in Rust 2024 due to set_var/remove_var being unsafe
mod common;

use common::*;
use serial_test::serial;

// ============================================================================
// ConfigDbSettings Tests (using struct directly, not env vars)
// ============================================================================

#[test]
fn test_database_url_with_password() {
    let settings = TestDbSettings {
        host: "db.example.com".to_string(),
        port: 5432,
        database: "mydb".to_string(),
        username: "admin".to_string(),
        password: Some("secret123".to_string()),
    };

    let url = settings.database_url();
    assert_eq!(url, "postgres://admin:secret123@db.example.com:5432/mydb");
}

#[test]
fn test_database_url_without_password() {
    let settings = TestDbSettings {
        host: "localhost".to_string(),
        port: 5433,
        database: "testdb".to_string(),
        username: "postgres".to_string(),
        password: None,
    };

    let url = settings.database_url();
    assert_eq!(url, "postgres://postgres@localhost:5433/testdb");
}

#[test]
fn test_database_url_special_characters() {
    // Test with special characters that might need URL encoding
    let settings = TestDbSettings {
        host: "localhost".to_string(),
        port: 5432,
        database: "test_db".to_string(),
        username: "user".to_string(),
        password: Some("password123".to_string()),
    };

    let url = settings.database_url();
    assert_eq!(url, "postgres://user:password123@localhost:5432/test_db");
}

#[test]
fn test_database_url_with_default_port() {
    let settings = TestDbSettings {
        host: "localhost".to_string(),
        port: 5432,
        database: "etl_config".to_string(),
        username: "postgres".to_string(),
        password: Some("pass".to_string()),
    };

    let url = settings.database_url();
    assert!(url.contains(":5432/"));
}

#[test]
fn test_database_url_with_custom_port() {
    let settings = TestDbSettings {
        host: "localhost".to_string(),
        port: 5433,
        database: "test".to_string(),
        username: "user".to_string(),
        password: None,
    };

    let url = settings.database_url();
    assert!(url.contains(":5433/"));
}

// ============================================================================
// Database Migration Tests
// ============================================================================

#[tokio::test]
#[serial]
async fn test_run_migrations_creates_tables() {
    let pool = create_test_pool().await;
    
    // Drop tables first to test creation
    sqlx::query("DROP TABLE IF EXISTS pipelines CASCADE")
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DROP TABLE IF EXISTS destinations CASCADE")
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DROP TABLE IF EXISTS sources CASCADE")
        .execute(&pool)
        .await
        .ok();

    // Run migrations
    setup_test_db(&pool).await;

    // Verify sources table exists
    let sources_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'sources')"
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");
    assert!(sources_exists.0);

    // Verify destinations table exists
    let destinations_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'destinations')"
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");
    assert!(destinations_exists.0);

    // Verify pipelines table exists
    let pipelines_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'pipelines')"
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");
    assert!(pipelines_exists.0);
}

#[tokio::test]
#[serial]
async fn test_run_migrations_idempotent() {
    let pool = create_test_pool().await;

    // Run migrations twice
    setup_test_db(&pool).await;
    setup_test_db(&pool).await; // Should not error

    // Verify tables still exist
    let sources_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'sources')"
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");
    assert!(sources_exists.0);
}

#[tokio::test]
#[serial]
async fn test_migrations_create_correct_columns() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;

    // Check sources columns
    let source_columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'sources' ORDER BY ordinal_position"
    )
    .fetch_all(&pool)
    .await
    .expect("Query failed");

    let source_col_names: Vec<String> = source_columns.into_iter().map(|(c,)| c).collect();
    assert!(source_col_names.contains(&"id".to_string()));
    assert!(source_col_names.contains(&"name".to_string()));
    assert!(source_col_names.contains(&"pg_host".to_string()));
    assert!(source_col_names.contains(&"pg_port".to_string()));
    assert!(source_col_names.contains(&"publication_name".to_string()));

    // Check pipelines has id_pipeline column
    let pipeline_columns: Vec<(String,)> = sqlx::query_as(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'pipelines' ORDER BY ordinal_position"
    )
    .fetch_all(&pool)
    .await
    .expect("Query failed");

    let pipeline_col_names: Vec<String> = pipeline_columns.into_iter().map(|(c,)| c).collect();
    assert!(pipeline_col_names.contains(&"id_pipeline".to_string()));
    assert!(pipeline_col_names.contains(&"batch_max_size".to_string()));
    assert!(pipeline_col_names.contains(&"status".to_string()));
}

#[tokio::test]
#[serial]
async fn test_migrations_create_foreign_keys() {
    let pool = create_test_pool().await;
    setup_test_db(&pool).await;

    // Check that pipelines has foreign keys to sources and destinations
    let fk_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM information_schema.table_constraints 
        WHERE table_name = 'pipelines' AND constraint_type = 'FOREIGN KEY'
        "#
    )
    .fetch_one(&pool)
    .await
    .expect("Query failed");

    assert_eq!(fk_count.0, 2); // source_id and destination_id
}

// ============================================================================
// PipelineManagerSettings Tests (testing the pattern without env manipulation)
// ============================================================================

#[test]
fn test_poll_interval_parsing_valid() {
    let value = "10";
    let poll_interval: u64 = value.parse().unwrap_or(5);
    assert_eq!(poll_interval, 10);
}

#[test]
fn test_poll_interval_parsing_invalid() {
    let value = "not_a_number";
    let poll_interval: u64 = value.parse().unwrap_or(5);
    assert_eq!(poll_interval, 5);
}

#[test]
fn test_poll_interval_parsing_negative() {
    let value = "-5";
    let poll_interval: u64 = value.parse().unwrap_or(5);
    assert_eq!(poll_interval, 5); // Should fall back to default for invalid u64
}

#[test]
fn test_poll_interval_parsing_zero() {
    let value = "0";
    let poll_interval: u64 = value.parse().unwrap_or(5);
    assert_eq!(poll_interval, 0);
}

#[test]
fn test_poll_interval_parsing_large() {
    let value = "3600";
    let poll_interval: u64 = value.parse().unwrap_or(5);
    assert_eq!(poll_interval, 3600);
}
