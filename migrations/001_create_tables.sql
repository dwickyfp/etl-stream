-- ETL Stream Configuration Database Schema

-- Table 1: Sources (PostgreSQL connection configurations)
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    pg_host VARCHAR(255) NOT NULL,
    pg_port INTEGER NOT NULL DEFAULT 5432,
    pg_database VARCHAR(255) NOT NULL,
    pg_username VARCHAR(255) NOT NULL,
    pg_password VARCHAR(255),
    pg_tls_enabled BOOLEAN NOT NULL DEFAULT false,
    publication_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table 2: Destinations (flexible config with JSONB)
CREATE TABLE IF NOT EXISTS destinations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    destination_type VARCHAR(50) NOT NULL, -- 'http', 'snowflake'

    -- Snowflake specific configuration
    snowflake_account VARCHAR(255),
    snowflake_user VARCHAR(255),
    snowflake_database VARCHAR(255),
    snowflake_schema VARCHAR(255),
    snowflake_warehouse VARCHAR(255),
    snowflake_role VARCHAR(255),
    snowflake_private_key_path VARCHAR(255),
    snowflake_private_key_passphrase VARCHAR(255),
    snowflake_landing_schema VARCHAR(255) DEFAULT 'ETL_SCHEMA',
    snowflake_task_schedule_minutes INTEGER DEFAULT 60,
    snowflake_host VARCHAR(255),

    -- HTTP specific configuration
    http_url VARCHAR(255),
    http_timeout_ms BIGINT DEFAULT 30000,
    http_retry_attempts INTEGER DEFAULT 3,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table 3: Pipelines (connects source to destination)
CREATE TABLE IF NOT EXISTS pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'PAUSE', -- 'START' or 'PAUSE'
    batch_max_size INTEGER NOT NULL DEFAULT 1000,
    batch_max_fill_ms BIGINT NOT NULL DEFAULT 5000,
    table_error_retry_delay_ms BIGINT NOT NULL DEFAULT 10000,
    table_error_retry_max_attempts INTEGER NOT NULL DEFAULT 5,
    max_table_sync_workers INTEGER NOT NULL DEFAULT 4,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_source_id ON pipelines(source_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_id ON pipelines(destination_id);

-- Example data (optional - comment out in production)
-- INSERT INTO sources (name, pg_host, pg_port, pg_database, pg_username, pg_password, publication_name)
-- VALUES ('test_source', '172.16.122.180', 5433, 'postgres', 'postgres', 'postgres', 'my_publication');

-- INSERT INTO destinations (name, destination_type, http_url, http_timeout_ms, http_retry_attempts)
-- VALUES ('test_http', 'http', 'http://172.16.62.226:5000', 30000, 3);

-- INSERT INTO pipelines (name, source_id, destination_id, status)
-- VALUES ('test_pipeline', 1, 1, 'START');
