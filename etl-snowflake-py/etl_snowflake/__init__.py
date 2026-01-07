"""ETL Snowflake Module.

Provides Snowflake destination support for ETL pipeline using Snowpipe Streaming.
"""

from etl_snowflake.config import SnowflakeConfig
from etl_snowflake.client import SnowflakeClient
from etl_snowflake.ddl import SnowflakeDDL
from etl_snowflake.task import SnowflakeTaskManager
from etl_snowflake.type_mapping import postgres_to_snowflake_type

__all__ = [
    "SnowflakeConfig",
    "SnowflakeClient",
    "SnowflakeDDL",
    "SnowflakeTaskManager",
    "postgres_to_snowflake_type",
]

__version__ = "0.1.0"
