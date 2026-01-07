---
trigger: always_on
---

# ETL Stream Code Style Guide

This document defines the coding conventions and style rules for the `etl-stream` project.

---

## 1. Project Structure

### 1.1 Module Organization

```
src/
├── main.rs              # Entry point, minimal logic
├── config.rs            # All configuration structs and env loading
├── metrics.rs           # Centralized metrics definitions
├── destination/         # Destination implementations
│   ├── mod.rs
│   └── http_destination.rs
├── repository/          # Database repository layer
│   ├── mod.rs
│   └── *_repository.rs
├── store/               # State storage implementations
│   ├── mod.rs
│   └── redis_store.rs
└── <feature_name>.rs    # Feature-specific modules
```

### 1.2 Module Declaration

- Declare all modules in `main.rs` at the top:

```rust
mod alert_manager;
mod config;
mod destination;
mod metrics;
mod pipeline_manager;
mod repository;
mod schema_cache;
mod store;
mod wal_monitor;
```

- Use `mod.rs` for re-exporting from subdirectories:

```rust
// src/repository/mod.rs
pub mod destination_repository;
pub mod pipeline_repository;
pub mod source_repository;
```

---

## 2. Naming Conventions

### 2.1 Files

| Type | Convention | Example |
|------|------------|---------|
| Module files | `snake_case.rs` | `pipeline_manager.rs` |
| Repository files | `<entity>_repository.rs` | `source_repository.rs` |
| Store files | `<backend>_store.rs` | `redis_store.rs` |
| Destination files | `<type>_destination.rs` | `http_destination.rs` |

### 2.2 Types

| Type | Convention | Example |
|------|------------|---------|
| Structs | `PascalCase` | `PipelineManager`, `AlertSettings` |
| Enums | `PascalCase` | `WalStatus`, `PipelineStatus` |
| Enum Variants | `PascalCase` | `WalStatus::Danger` |
| Traits | `PascalCase` | `Destination`, `StateStore` |
| Type Aliases | `PascalCase` | `EtlResult<T>` |

### 2.3 Functions & Variables

| Type | Convention | Example |
|------|------------|---------|
| Functions | `snake_case` | `from_env()`, `update_status()` |
| Methods | `snake_case` | `self.persist_mapping_to_redis()` |
| Variables | `snake_case` | `poll_interval_secs`, `warning_wal_mb` |
| Constants | `SCREAMING_SNAKE_CASE` | `DEFAULT_TIMEOUT` |

### 2.4 Environment Variables

- Use `SCREAMING_SNAKE_CASE` with descriptive prefixes:

```rust
env::var("CONFIG_DB_HOST")
env::var("PIPELINE_POLL_INTERVAL_SECS")
env::var("WARNING_WAL")
env::var("ALERT_WAL_URL")
```

---

## 3. Struct Patterns

### 3.1 Configuration Structs

```rust
/// Description of the config purpose
#[derive(Debug, Clone)]
pub struct ConfigName {
    /// Field documentation
    pub field_name: Type,
}

impl ConfigName {
    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            field_name: env::var("ENV_VAR_NAME")
                .unwrap_or_else(|_| "default".to_string()),
        })
    }
}
```

### 3.2 State/Manager Structs

```rust
/// Brief description
pub struct ManagerName {
    pool: PgPool,
    settings: Settings,
    state: Arc<RwLock<HashMap<K, V>>>,
}

impl ManagerName {
    pub fn new(pool: PgPool, settings: Settings) -> Self {
        Self {
            pool,
            settings,
            state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        // Implementation
    }

    pub async fn shutdown(&self) {
        // Cleanup logic
    }
}
```

### 3.3 Repository Structs

```rust
/// Row model from database
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct EntityRow {
    pub id: i32,
    pub name: String,
    pub created_at: Option<NaiveDateTime>,
}

pub struct EntityRepository;

impl EntityRepository {
    pub async fn get_all(pool: &PgPool) -> Result<Vec<EntityRow>, Box<dyn Error>> {
        let rows = sqlx::query_as::<_, EntityRow>("SELECT * FROM entities ORDER BY id")
            .fetch_all(pool)
            .await?;
        Ok(rows)
    }

    pub async fn get_by_id(pool: &PgPool, id: i32) -> Result<Option<EntityRow>, Box<dyn Error>> {
        let row = sqlx::query_as::<_, EntityRow>("SELECT * FROM entities WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(row)
    }
}
```

---

## 4. Async Patterns

### 4.1 Tokio Runtime

- Use `#[tokio::main]` for async main:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // ...
}
```

### 4.2 Background Tasks

- Spawn background tasks with `tokio::spawn`:

```rust
let manager_clone = manager.clone();
tokio::spawn(async move {
    let mut interval = interval(Duration::from_secs(poll_interval_secs));
    loop {
        interval.tick().await;
        if let Err(e) = manager_clone.sync().await {
            error!("Sync failed: {}", e);
        }
    }
});
```

### 4.3 Lock Patterns

- Use `Arc<RwLock<T>>` for shared mutable state:

```rust
let states = self.states.write().await;
// Modify state
drop(states); // Release lock before async operations
```

- Always drop locks before async operations to avoid deadlocks.

---

## 5. Error Handling

### 5.1 Result Types

- Use `Result<T, Box<dyn Error>>` for most functions:

```rust
pub async fn process(&self) -> Result<(), Box<dyn Error>> {
    // ...
}
```

- Use domain-specific error types for library code:

```rust
use etl::error::{ErrorKind, EtlResult};
use etl::{bail, etl_error};

pub fn new() -> EtlResult<Self> {
    let client = Client::builder()
        .build()
        .map_err(|e| etl_error!(ErrorKind::Unknown, "HTTP client error", source: e))?;
    Ok(Self { client })
}
```

### 5.2 Error Logging

- Log errors at call site:

```rust
if let Err(e) = self.process().await {
    error!("Processing failed: {}", e);
}
```

---

## 6. Documentation

### 6.1 Module Documentation

```rust
//! Module name and brief description.
//!
//! Extended explanation of what this module does
//! and how it fits into the system.
```

### 6.2 Struct/Function Documentation

```rust
/// Brief description in imperative mood.
///
/// Extended explanation if needed.
///
/// # Arguments
/// * `param` - Description of parameter
///
/// # Returns
/// Description of return value
pub fn function_name(param: Type) -> ReturnType {
    // ...
}
```

### 6.3 Inline Comments

- Use for non-obvious logic:

```rust
// Drop lock before async operation to prevent deadlock
drop(states);

// Reconstruct the integer value from base-10000 digits
// Each digit represents a value 0-9999 in base 10000
for digit in &digits {
    int_value = int_value * 10000 + (*digit as u128);
}
```

---

## 7. Logging & Metrics

### 7.1 Tracing Usage

```rust
use tracing::{debug, error, info, warn};

info!("Pipeline manager started. Polling every {} seconds.", interval);
warn!("Attempt {}/3: status {}", attempt, status);
error!("Failed to send alert: {}", e);
debug!("Source '{}' returned to normal", source_name);
```

### 7.2 Metrics Pattern

- Define metrics in `src/metrics.rs`:

```rust
/// Record a specific operation
pub fn operation_name(label: &str) {
    counter!("etl_operation_total", "label" => label.to_string()).increment(1);
}

/// Record operation duration
pub fn operation_duration(duration_secs: f64) {
    histogram!("etl_operation_duration_seconds").record(duration_secs);
}
```

- Use `Timer` helper for measuring durations:

```rust
let timer = metrics::Timer::start();
// ... operation
metrics::http_request_duration(timer.elapsed_secs());
```

---

## 8. Testing

### 8.1 Unit Tests

- Place tests at bottom of each module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn test_helper() -> TestType {
        // Setup test data
    }

    #[test]
    fn test_feature_behavior() {
        let input = test_helper();
        assert_eq!(function(input), expected);
    }

    #[tokio::test]
    async fn test_async_feature() {
        // Async test
    }

    #[tokio::test]
    #[ignore = "requires external service"]
    async fn test_integration() {
        // Integration test requiring external dependencies
    }
}
```

### 8.2 Test File Organization

- Place integration tests in `tests/` directory:

```
tests/
├── common/
│   └── mod.rs           # Shared test utilities
├── config_tests.rs
├── http_destination_tests.rs
└── schema_cache_tests.rs
```

### 8.3 Test Naming

- Use descriptive names: `test_<function>_<scenario>`:

```rust
#[test]
fn test_wal_status_from_size_normal() { }

#[test]
fn test_wal_status_from_size_warning() { }

#[test]
fn test_alert_manager_disabled_without_url() { }
```

### 8.4 Test Sections

- Organize tests with comment headers:

```rust
// ============================================================================
// Datum Parsing Tests
// ============================================================================

#[test]
fn test_parse_null() { }

// ============================================================================
// HTTP Client Tests
// ============================================================================

#[tokio::test]
async fn test_http_post_success() { }
```

---

## 9. Dependencies

### 9.1 Import Order

1. Standard library
2. External crates
3. Internal crates (`etl`, `etl-telemetry`)
4. Local modules (`crate::`)

```rust
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};

use crate::config::AlertSettings;
use crate::metrics;
```

### 9.2 Feature Flags

- Specify minimal features in `Cargo.toml`:

```toml
tokio = { version = "1.0", features = ["full"] }
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "json", "chrono"] }
```

---

## 10. Concurrency

### 10.1 Shared State

```rust
// Use Arc<RwLock<T>> for read-heavy shared state
state: Arc<RwLock<HashMap<K, V>>>

// Use Arc<Mutex<T>> for write-heavy shared state
tables: Arc<Mutex<HashMap<TableId, TableEntry>>>
```

### 10.2 Clone for Async Move

```rust
let pool = self.pool.clone();
let settings = self.settings.clone();

tokio::spawn(async move {
    // Use pool and settings
});
```

---

## 11. Enums

### 11.1 Status Enums

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatusType {
    Normal,
    Warning,
    Danger,
}

impl StatusType {
    /// Factory method from value
    pub fn from_value(value: T, thresholds: &Thresholds) -> Self {
        // ...
    }

    /// Check condition
    pub fn requires_action(&self) -> bool {
        matches!(self, StatusType::Warning | StatusType::Danger)
    }
}

impl std::fmt::Display for StatusType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatusType::Normal => write!(f, "normal"),
            StatusType::Warning => write!(f, "warning"),
            StatusType::Danger => write!(f, "danger"),
        }
    }
}
```
