# ETL Snowflake Module

Python module for Snowflake operations in the ETL pipeline.

## Features

- Snowpipe Streaming for low-latency data ingestion
- DDL operations (CREATE, ALTER tables)
- Task management (MERGE tasks)
- PostgreSQL to Snowflake type mapping

## Installation

```bash
pip install -e .
```

## Requirements

- Python 3.9+
- Snowflake account with key-pair authentication
- Snowpipe Streaming enabled

## Usage

```python
from etl_snowflake import SnowflakeConfig, SnowflakeClient

config = SnowflakeConfig(
    account="xy12345.us-east-1",
    user="ETL_USER",
    database="MY_DB",
    schema="PUBLIC",
    warehouse="COMPUTE_WH",
    private_key_path="/path/to/key.p8",
)

client = SnowflakeClient(config)
client.insert_rows("my_table", rows)
```
