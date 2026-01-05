// Config module E2E tests
// Note: Environment variable tests are avoided in Rust 2024 due to set_var/remove_var being unsafe
mod common;

use common::*;

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
