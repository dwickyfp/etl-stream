# Maintenance & Operations

## Running the Application

### Prerequisites
*   Rust (latest stable)
*   Python 3.10+ (and `pip`)
*   PostgreSQL (Source)
*   Snowflake Account (Destination)
*   Redis (State Store)

### Environment Setup
Create a `.env` file in the root directory:
```bash
DATABASE_URL=postgres://user:pass@localhost:5432/config_db
REDIS_URL=redis://localhost:6379
SNOWFLAKE_ACTOR_POOL_SIZE=4
```

### Build & Run
```bash
# Debug run
cargo run

# Production build
cargo build --release
./target/release/main
```

## Configuration Management

Configuration is primarily driven by the **PostgreSQL Config Database**.
*   **`source` table**: Defines PostgreSQL source connections.
*   **`destination` table**: Defines Snowflake connection details.
*   **`pipeline` table**: Links a source to a destination and defines batching policies.

To add a new pipeline, you simply insert a row into the `pipeline` table. The `PipelineManager` will detect it on the next poll cycle and start it automatically.

## Troubleshooting

### Common Issues

1.  **"Actor Closed" / Panic in Logs**
    *   **Cause**: Usually an unhandled exception in the Python layer (Snowflake SDK).
    *   **Fix**: Check the "Panic: ..." message in the logs. It often relates to authentication (invalid key) or network timeouts.

2.  **High WAL Usage Alerts**
    *   **Cause**: The pipeline is too slow or down, causing the replication slot to retain WAL segments.
    *   **Action**:
        *   Check if `etl-stream` is running.
        *   Check `etl_throughput_rows` metric.
        *   Verify Snowflake is accepting data.

3.  **Missing Tables in Snowflake**
    *   **Cause**: Self-healing might have failed or permissions issue.
    *   **Fix**: Check logs for `EnsureTableInitialized` errors. Verify the Snowflake role has `CREATE TABLE` permissions in the target schema.

## Development & Testing

### Running Tests
```bash
# Run unit tests
cargo test

# Run Rust-Python integration tests (requires valid env)
cargo test --features integration
```

### Adding Dependencies
*   **Rust**: Add to `Cargo.toml`
*   **Python**: Update `requirements.txt` or `setup.py` in `etl-snowflake-py`.
