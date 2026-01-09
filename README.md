# ETL Stream

A Rust-based ETL (Extract, Transform, Load) streaming application that replicates data from PostgreSQL sources to Snowflake using logical replication.

## Prerequisites

- **Rust** (Edition 2024) - [Install Rust](https://rustup.rs/)
- **Docker & Docker Compose** - For running PostgreSQL
- **PostgreSQL client** (optional) - For manual database operations

## Quick Start

### 1. Start PostgreSQL with Docker

```bash
docker compose up -d
```

This starts PostgreSQL on port **5433** with logical replication enabled.

### 2. Configure Environment

Copy the example environment file and adjust if needed:

```bash
cp .env.example .env
```

Default configuration:
```env
CONFIG_DB_HOST=localhost
CONFIG_DB_PORT=5433
CONFIG_DB_DATABASE=postgres
CONFIG_DB_USERNAME=postgres
CONFIG_DB_PASSWORD=postgres
PIPELINE_POLL_INTERVAL_SECS=5
```

### 3. Run the Application

```bash
cargo run
```

The application will:
1. Connect to the configuration database
2. Run migrations to create `sources`, `destinations`, and `pipelines` tables
3. Start polling for active pipelines

### 4. Create a Pipeline

Connect to PostgreSQL and create your pipeline configuration:

```bash
# Connect to the config database
psql -h localhost -p 5433 -U postgres -d postgres
```

```sql
-- 1. Create a source (your PostgreSQL database to replicate FROM)
INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, pg_password, publication_name)
VALUES ('my_source', 'localhost', 5433, 'postgres', 'postgres', 'postgres', 'my_publication');

## Snowflake Destination

The Snowflake destination uses a hybrid Rust/Python bridge via **PyO3** to leverage the official Snowflake Python SDK.

### 1. Python Prerequisites
Ensure you have Python 3.9+ and the required packages:

```bash
# Install the bridge package in editable mode
pip install -e etl-snowflake-py/
```

### 2. Security Setup (Key-Pair Auth)
Snowflake requires key-pair authentication. 

1. Generate your private key:
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out /private_hub/rsa_key.p8
   ```
2. For encrypted keys, set a passphrase during generation.
3. **Note**: The `/private_hub/` directory is gitignored to prevent accidental key exposure.

### 3. Database Configuration
Insert a Snowflake destination record:

```sql
INSERT INTO destinations (name, destination_type, config)
VALUES ('Snowflake Prod', 'snowflake', '{
    "account": "xy12345.us-east-1",
    "user": "ETL_USER",
    "database": "DEVELOPMENT",
    "schema": "PUBLIC",
    "warehouse": "COMPUTE_WH",
    "private_key_path": "/Users/.../private_hub/rsa_key.p8",
    "private_key_passphrase": "your_passphrase",
    "landing_schema": "ETL_SCHEMA",
    "task_schedule_minutes": 60
}');
```

| Field | Description |
|-------|-------------|
| `landing_schema` | Schema where CDC data is first streamed (prefixed with `landing_`) |
| `task_schedule_minutes` | How often to run the MERGE task from landing to target table |
| `private_key_passphrase` | (Optional) If your key is password-protected |

### 4. How it Works
- **Auto-Initialization**: The system automatically creates landing and target tables if they don't exist.
- **Schema Evolution**: Detected schema changes in PostgreSQL are automatically applied to Snowflake.
- **Merge Engine**: Data is streamed to `landing_<table_name>`, and a Snowflake Task automatically merges it into the target table based on the primary key.

### 5. Performance Optimization: Arrow IPC
To achieve high throughput and low memory usage, data transfer from Rust to Python uses **Arrow IPC** with a zero-copy architecture:

1. **Rust**: Logical replication events are batched into Apache Arrow `RecordBatch` structures using the `arrow` crate.
2. **Zero-Copy Transfer**: The Arrow batch is passed directly to Python using the Arrow C Data Interface (via `pyo3` and `arrow::pyarrow`).
3. **Python**: The batch is received as a `pyarrow.RecordBatch` and efficiently converted for the Snowpipe Streaming SDK.

This architecture eliminates the double-allocation overhead associated with standard row-by-row JSON conversion.

## Project Structure

```
etl-stream/
├── src/
│   ├── main.rs              # Application entry point
│   ├── config.rs            # Configuration and migrations
│   ├── pipeline_manager.rs  # Pipeline lifecycle management
│   ├── repository/          # Database repositories
│   └── destination/         # Destination handlers
│       └── snowflake_destination.rs # Rust wrapper for Snowflake
├── etl-snowflake-py/        # Python Snowflake SDK bridge (Snowpipe Streaming)
│   ├── etl_snowflake/
│   │   ├── client.py        # Main streaming client
│   │   ├── ddl.py           # Table and Schema operations
│   │   └── task.py          # MERGE task management
├── tests/                   # E2E tests
├── docker-compose.yml       # PostgreSQL setup
└── .env.example             # Environment template
```
## Run

```bash
# Start the application
PYO3_PYTHON=/Library/Frameworks/Python.framework/Versions/3.12/bin/python3 cargo run
```

## Pipeline Status

- `START` - Pipeline is active and replicating
- `PAUSE` - Pipeline is stopped

Update status to control pipelines:

```sql
-- Start a pipeline
UPDATE pipelines SET status = 'START' WHERE id = 1;

-- Pause a pipeline
UPDATE pipelines SET status = 'PAUSE' WHERE id = 1;
```

## Troubleshooting

### Connection Refused
Ensure PostgreSQL is running:
```bash
docker compose ps
docker compose logs postgres
```

### Permission Denied for Replication
Your PostgreSQL user needs replication privileges:
```sql
ALTER ROLE postgres WITH REPLICATION;
```

### Publication Not Found
Create the publication on your source database before starting the pipeline.

## Shutdown

Press `Ctrl+C` in the terminal running `cargo run`, or:

```bash
docker compose down
```