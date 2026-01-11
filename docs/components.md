# Component Reference

## 1. PipelineManager (`pipeline_manager.rs`)
**Role**: Orchestrator of the entire ETL process.
*   **Responsibilities**:
    *   Reads pipeline configurations from the `pipeline` table.
    *   spawns/stops async tasks for each active pipeline.
    *   Detects schema changes (new tables) and restarts pipelines if necessary.
    *   Manages the `SchemaCache` to minimize database lookup overhead.

## 2. WalMonitor (`wal_monitor.rs`)
**Role**: Guardian of Source Database Health.
*   **Responsibilities**:
    *   Periodically checks the size of the WAL directory on source databases.
    *   **Throttling**: Uses a token bucket / semaphore approach to avoid overwhelming the source DB with monitoring queries (PERF-01/02 fixes).
    *   **Alerting**: Sends webhooks (via `AlertManager`) if WAL size exceeds `warning` or `danger` thresholds. This is critical to prevent the source DB from crashing due to disk full events.

## 3. SnowflakeDestination (`destination/snowflake_destination.rs`)
**Role**: The "loader" component.
*   **Architecture**:
    *   Implemented as a Rust wrapper around the Python Snowpipe Streaming SDK.
    *   **Actor Model**: Uses an `ActorPool` where multiple independent Actors (each with its own Python interpreter instance thread-local data) run in parallel to handle high-throughput concurrent writes.
    *   **Circuit Breaker**: Implements the Circuit Breaker pattern to fail fast when Snowflake is down.
    *   **Zero-Copy**: Bridges Rust `Arrow` arrays to Python `PyArrow` arrays.

## 4. RedisStore (`store/redis_store.rs`)
**Role**: Distributed State Management.
*   **Responsibilities**:
    *   Stores the replication state (LSN, offsets) for each pipeline.
    *   Allows the ETL process to resume from exactly where it left off after a restart.

## 5. Telemetry & Metrics
**Role**: Observability.
*   **Metrics**: Prometheus endpoint (default port 9000).
    *   `pg_source_wal_size_mb`: WAL size.
    *   `pipeline_active`: Number of running pipelines.
    *   `etl_throughput_rows`: Rows processed per second.
*   **Tracing**: Key events are logged with structured context (JSON logs possible).
