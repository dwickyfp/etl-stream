# ETL Stream

A Rust-based ETL (Extract, Transform, Load) streaming application that replicates data from PostgreSQL sources to HTTP destinations using logical replication.

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

-- 2. Create a destination (HTTP webhook endpoint)
INSERT INTO destinations (name, destination_type, config)
VALUES ('my_webhook', 'http', '{"url": "http://localhost:5001/events"}');

-- 3. Create a pipeline connecting source to destination
INSERT INTO pipelines (name, source_id, destination_id, status, id_pipeline)
VALUES ('my_pipeline', 1, 1, 'START', 1001);
```

### 5. Set Up Replication on Source Database

On your source PostgreSQL database, create a publication:

```sql
-- Create a publication for the tables you want to replicate
CREATE PUBLICATION my_publication FOR ALL TABLES;

-- Or for specific tables:
CREATE PUBLICATION my_publication FOR TABLE users, orders;
```

## Running Tests

### Run All E2E Tests

Make sure PostgreSQL is running, then:

```bash
# Run all tests (sequentially due to shared database)
cargo test -- --test-threads=1
```

### Run Specific Test Suites

```bash
# Repository tests
cargo test repository_tests -- --test-threads=1

# Config tests
cargo test config_tests -- --test-threads=1

# Schema cache tests
cargo test schema_cache_tests

# Custom store tests
cargo test custom_store_tests

# HTTP destination tests
cargo test http_destination_tests

# Integration tests
cargo test integration_tests -- --test-threads=1
```

### Test with Verbose Output

```bash
cargo test -- --test-threads=1 --nocapture
```

## Project Structure

```
etl-stream/
├── src/
│   ├── main.rs              # Application entry point
│   ├── config.rs            # Configuration and migrations
│   ├── pipeline_manager.rs  # Pipeline lifecycle management
│   ├── schema_cache.rs      # Shared schema caching
│   ├── repository/          # Database repositories
│   │   ├── source_repository.rs
│   │   ├── destination_repository.rs
│   │   └── pipeline_repository.rs
│   ├── destination/         # Destination handlers
│   │   └── http_destination.rs
│   └── store/               # State storage
│       └── custom_store.rs
├── tests/                   # E2E tests
│   ├── common/mod.rs        # Test utilities
│   ├── repository_tests.rs
│   ├── config_tests.rs
│   ├── schema_cache_tests.rs
│   ├── custom_store_tests.rs
│   ├── http_destination_tests.rs
│   └── integration_tests.rs
├── webhook/                 # Sample webhook receiver
├── docker-compose.yml       # PostgreSQL setup
└── .env.example             # Environment template
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