# ETL Stream AI Coding Instructions

## Project Overview

ETL Stream is a Rust-based CDC (Change Data Capture) pipeline that replicates PostgreSQL data to Snowflake using logical replication. Built on Supabase's [etl](https://github.com/supabase/etl) library, it uses a hybrid Rust/Python architecture via PyO3 for Snowflake integration.

## Architecture Components

### Core Pipeline Flow

1. **Configuration DB** (PostgreSQL) stores source/destination/pipeline configs via `migrations/001_create_tables.sql`
2. **Pipeline Manager** (`src/pipeline_manager.rs`) polls config DB, spawns/manages ETL pipelines
3. **ETL Library** (external `etl` crate) handles PostgreSQL logical replication
4. **Redis Store** (`src/store/`) provides state persistence with hybrid in-memory/Redis approach
5. **Destination Handler** (`src/destination/`) dispatches to Snowflake (extensible enum pattern)
6. **Snowflake Bridge** (`etl-snowflake-py/`) Python SDK wrapper called via PyO3/Arrow IPC

### Key Data Flows

- **State Management**: `RedisStore` uses in-memory storage for non-serializable types (`TableSchema`, `TableReplicationPhase`) while persisting mappings to Redis
- **Zero-Copy Bridging**: Rust batches CDC events into Arrow `RecordBatch`, transfers to Python via Arrow C Data Interface for Snowpipe Streaming
- **Schema Evolution**: `SchemaCache` per source_id, shared across pipelines from the same source

## Critical Developer Workflows

### Running the Application

```bash
# Start dependencies (PostgreSQL on 5433, Redis on 6377)
docker compose up -d

# Run with Python interpreter for PyO3 (macOS example)
PYO3_PYTHON=/Library/Frameworks/Python.framework/Versions/3.12/bin/python3 cargo run

# Install Python bridge (required for Snowflake destination)
pip install -e etl-snowflake-py/
```

### Testing

```bash
# Run all tests (uses tokio-test, wiremock)
cargo test

# Integration tests use serial_test for isolation
cargo test --test schema_cache_tests -- --test-threads=1
```

### Configuration

- Main config: `.env` file (see `.env.example`)
- Database migrations auto-run on startup via `sqlx::migrate!`
- Rust edition: **2024** (note unusual edition)

## Project-Specific Patterns

### Destination Pattern

Uses enum dispatch instead of trait objects for performance:

```rust
// src/destination/mod.rs
pub enum DestinationHandler {
    Snowflake(SnowflakeDestination),
    // Future: BigQuery, Iceberg (see etl-destination/ crate)
}
```

### Metrics & Observability

- Centralized metrics in `src/metrics.rs` using `metrics` crate
- Prometheus endpoint auto-starts on `[::]:9000/metrics` via `etl-telemetry`
- Custom metrics: `metrics::snowflake_rows_inserted()`, `metrics::redis_operation_duration()`, etc.
- Structured logging via `tracing` with `etl-telemetry::tracing::init_tracing()`

### Error Handling

```rust
// Use etl crate's error macros
use etl::{etl_error, error::{EtlResult, ErrorKind}};

etl_error!(ErrorKind::Unknown, "Description", details)
```

### Repository Pattern

Database access via repositories (`src/repository/`):

- `PipelineRepository::load_active_pipelines()` for poll-based management
- `SourceRepository::find_by_id()` for connection pooling
- Status enum: `PipelineStatus::Start` | `PipelineStatus::Pause`

## Integration Points

### External Dependencies

- **etl crate**: Core replication engine (GitHub dependency)
- **Snowflake SDK**: Python `snowflake-connector-python` + Snowpipe Streaming (via `etl-snowflake-py/`)
- **Redis**: State persistence at `REDIS_URL` (default: `127.0.0.1:6379`)
- **PostgreSQL**: Config DB (see `CONFIG_DB_*` env vars)

### PyO3 Bridging

- Initialize client: `etl_snowflake.client.SnowflakeClient(config)` from Rust
- Data transfer uses `arrow::pyarrow::ToPyArrow` for zero-copy batches
- Python dependencies in `etl-snowflake-py/requirements.txt`

### Cross-Component Communication

- **Schema Cache**: Shared via `Arc<RwLock<HashMap<i32, SchemaCache>>>` in PipelineManager
- **Running Pipelines**: Tracked with `JoinHandle` in `HashMap<i32, RunningPipeline>`
- **WAL Monitoring**: Separate `WalMonitor` task with alert webhook support (`src/wal_monitor.rs`)

## Common Gotchas

1. **Python Environment**: PyO3 requires explicit Python path (use `PYO3_PYTHON` env var)
2. **Port Conflicts**: Docker PostgreSQL uses 5433 (not 5432) to avoid conflicts
3. **Redis Keys**: Prefix `etl:` for all keys (see `RedisStore::with_prefix`)
4. **Table Names**: Snowflake landing tables prefixed with `landing_` in separate schema
5. **Snowflake Auth**: Requires RSA key-pair authentication (store in `private_hub/`, gitignored)

## File References

- Entry point: [src/main.rs](src/main.rs)
- Pipeline orchestration: [src/pipeline_manager.rs](src/pipeline_manager.rs)
- Snowflake implementation: [src/destination/snowflake_destination.rs](src/destination/snowflake_destination.rs)
- Python bridge: [etl-snowflake-py/etl_snowflake/client.py](etl-snowflake-py/etl_snowflake/client.py)
- State management: [src/store/redis_store.rs](src/store/redis_store.rs)
- Config schema: [migrations/001_create_tables.sql](migrations/001_create_tables.sql)
