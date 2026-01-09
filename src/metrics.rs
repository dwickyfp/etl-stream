//! Centralized metrics module for ETL Stream application.
//!
//! This module defines all Prometheus metrics used throughout the application.
//! Metrics are organized into categories: Pipeline, Event, HTTP, and Redis.

use metrics::{counter, gauge, histogram};
use std::time::Instant;

// =============================================================================
// Pipeline Metrics
// =============================================================================

/// Increment the active pipelines gauge.
pub fn pipeline_active_inc() {
    gauge!("etl_pipeline_active").increment(1.0);
}

/// Decrement the active pipelines gauge.
pub fn pipeline_active_dec() {
    gauge!("etl_pipeline_active").decrement(1.0);
}

/// Record a pipeline start.
pub fn pipeline_started(pipeline_name: &str) {
    counter!("etl_pipeline_starts_total", "pipeline_name" => pipeline_name.to_string()).increment(1);
}

/// Record a pipeline stop.
pub fn pipeline_stopped(pipeline_name: &str) {
    counter!("etl_pipeline_stops_total", "pipeline_name" => pipeline_name.to_string()).increment(1);
}

/// Record a pipeline error.
pub fn pipeline_error(pipeline_name: &str, error_type: &str) {
    counter!(
        "etl_pipeline_errors_total",
        "pipeline_name" => pipeline_name.to_string(),
        "error_type" => error_type.to_string()
    ).increment(1);
}

/// Record categorized error with severity
#[allow(dead_code)]
pub fn categorized_error(component: &str, error_category: &str, severity: &str) {
    counter!(
        "etl_errors_categorized_total",
        "component" => component.to_string(),
        "category" => error_category.to_string(),
        "severity" => severity.to_string()
    ).increment(1);
}

/// Record error rate (errors per second) for alerting
#[allow(dead_code)]
pub fn error_rate(component: &str, rate: f64) {
    gauge!("etl_error_rate", "component" => component.to_string()).set(rate);
}

// =============================================================================
// Event Metrics
// =============================================================================

/// Record events processed by type.
pub fn events_processed(event_type: &str, count: u64) {
    counter!("etl_events_processed_total", "event_type" => event_type.to_string()).increment(count);
}

/// Record events processed size in bytes.
pub fn events_bytes_processed(event_type: &str, bytes: u64) {
    counter!("etl_events_bytes_processed_total", "event_type" => event_type.to_string()).increment(bytes);
}

/// Record event batch size.
pub fn events_batch_size(size: usize) {
    histogram!("etl_events_batch_size").record(size as f64);
}

/// Record event processing duration.
pub fn events_processing_duration(duration_secs: f64) {
    histogram!("etl_events_processing_duration_seconds").record(duration_secs);
}

// =============================================================================
// HTTP Destination Metrics (kept for future use)
// =============================================================================

/// Record HTTP request with status.
#[allow(dead_code)]
pub fn http_request(status: &str) {
    counter!("etl_http_requests_total", "status" => status.to_string()).increment(1);
}

/// Record HTTP request duration.
#[allow(dead_code)]
pub fn http_request_duration(duration_secs: f64) {
    histogram!("etl_http_request_duration_seconds").record(duration_secs);
}

/// Record HTTP retry.
#[allow(dead_code)]
pub fn http_retry() {
    counter!("etl_http_retries_total").increment(1);
}

// =============================================================================
// Snowflake Destination Metrics
// =============================================================================

/// Record Snowflake request with status.
pub fn snowflake_request(status: &str) {
    counter!("etl_snowflake_requests_total", "status" => status.to_string()).increment(1);
}

/// Record Snowflake request duration.
pub fn snowflake_request_duration(duration_secs: f64) {
    histogram!("etl_snowflake_request_duration_seconds").record(duration_secs);
}

/// Record Snowflake rows inserted.
pub fn snowflake_rows_inserted(table: &str, count: u64) {
    counter!("etl_snowflake_rows_inserted_total", "table" => table.to_string()).increment(count);
}

/// Record Snowflake bytes processed.
pub fn snowflake_bytes_processed(bytes: u64) {
    counter!("etl_snowflake_bytes_processed_total").increment(bytes);
}

/// Record Snowflake table initialization.
pub fn snowflake_table_initialized(table: &str) {
    counter!("etl_snowflake_tables_initialized_total", "table" => table.to_string()).increment(1);
}

/// Record Snowflake error.
pub fn snowflake_error(operation: &str) {
    counter!("etl_snowflake_errors_total", "operation" => operation.to_string()).increment(1);
}

// =============================================================================
// Redis Store Metrics
// =============================================================================

/// Record a Redis operation.
pub fn redis_operation(operation: &str) {
    counter!("etl_redis_operations_total", "operation" => operation.to_string()).increment(1);
}

/// Record Redis operation duration.
pub fn redis_operation_duration(operation: &str, duration_secs: f64) {
    histogram!("etl_redis_operation_duration_seconds", "operation" => operation.to_string()).record(duration_secs);
}

/// Record a Redis error.
pub fn redis_error(operation: &str) {
    counter!("etl_redis_errors_total", "operation" => operation.to_string()).increment(1);
}

// =============================================================================
// Source Metrics
// =============================================================================

/// Record Postgres source connection status.
pub fn pg_source_status(source_name: &str, connected: bool) {
    gauge!("etl_pg_source_status", "source_name" => source_name.to_string()).set(if connected { 1.0 } else { 0.0 });
}

/// Record Postgres source WAL size in MB.
pub fn pg_source_wal_size_mb(source_name: &str, size_mb: f64) {
    gauge!("etl_pg_source_wal_size_mb", "source_name" => source_name.to_string()).set(size_mb);
}

/// Record connection pool size
pub fn connection_pool_size(pool_name: &str, size: usize) {
    gauge!("etl_connection_pool_size", "pool" => pool_name.to_string()).set(size as f64);
}

/// Record connection pool cleanup
pub fn connection_pool_cleanup(pool_name: &str, cleaned: usize) {
    counter!("etl_connection_pool_cleanup_total", "pool" => pool_name.to_string()).increment(cleaned as u64);
}

// =============================================================================
// Timing Helpers
// =============================================================================

/// A simple timer for measuring operation durations.
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Start a new timer.
    pub fn start() -> Self {
        Self { start: Instant::now() }
    }

    /// Get elapsed time in seconds.
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

// =============================================================================
// Schema Cache Metrics  
// =============================================================================

/// Record schema cache hit
#[allow(dead_code)]
pub fn schema_cache_hit(cache_type: &str) {
    counter!("etl_schema_cache_hits_total", "type" => cache_type.to_string()).increment(1);
}

/// Record schema cache miss
#[allow(dead_code)]
pub fn schema_cache_miss(cache_type: &str) {
    counter!("etl_schema_cache_misses_total", "type" => cache_type.to_string()).increment(1);
}

/// Record cache cleanup
#[allow(dead_code)]
pub fn schema_cache_cleanup(expired_count: usize) {
    counter!("etl_schema_cache_cleanup_total").increment(expired_count as u64);
}

// =============================================================================
// Distributed Tracing Support
// =============================================================================

/// Record a trace span start
#[allow(dead_code)]
pub fn trace_span_start(operation: &str, trace_id: &str) {
    counter!("etl_trace_spans_total", "operation" => operation.to_string(), "trace_id" => trace_id.to_string()).increment(1);
}

/// Record trace span duration
#[allow(dead_code)]
pub fn trace_span_duration(operation: &str, duration_secs: f64) {
    histogram!("etl_trace_span_duration_seconds", "operation" => operation.to_string()).record(duration_secs);
}
