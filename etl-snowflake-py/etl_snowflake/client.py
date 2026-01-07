"""Snowflake Streaming Client module.

Provides Snowpipe Streaming integration for low-latency data ingestion.
"""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from etl_snowflake.config import SnowflakeConfig
from etl_snowflake.ddl import SnowflakeDDL
from etl_snowflake.task import SnowflakeTaskManager

logger = logging.getLogger(__name__)


@dataclass
class Channel:
    """Represents a Snowpipe Streaming channel.
    
    Attributes:
        name: Channel name
        table_name: Target table name
        is_open: Whether the channel is currently open
    """
    name: str
    table_name: str
    is_open: bool = True
    _client: Any = field(default=None, repr=False)


class SnowflakeClient:
    """Snowflake client for streaming data ingestion.
    
    Provides high-level interface for:
    - Opening Snowpipe Streaming channels
    - Inserting rows with ETL metadata
    - Managing table lifecycle (create, alter)
    - Task management for MERGE operations
    """
    
    def __init__(self, config: SnowflakeConfig):
        """Initialize the Snowflake client.
        
        Args:
            config: Snowflake configuration
        """
        self.config = config
        self.ddl = SnowflakeDDL(config)
        self.task_manager = SnowflakeTaskManager(config, self.ddl)
        self._channels: Dict[str, Channel] = {}
        self._initialized_tables: set = set()
        self._streaming_client = None
    
    def _ensure_streaming_client(self) -> Any:
        """Lazily initialize the Snowpipe Streaming client.
        
        Returns:
            Snowpipe Streaming client instance
        """
        if self._streaming_client is not None:
            return self._streaming_client
        
        try:
            from snowpipe_streaming import SnowpipeStreamingClient
            
            # Load private key
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend
            
            with open(self.config.private_key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=self.config.private_key_passphrase.encode()
                        if self.config.private_key_passphrase else None,
                    backend=default_backend()
                )
            
            self._streaming_client = SnowpipeStreamingClient(
                account=self.config.account,
                user=self.config.user,
                private_key=private_key,
                database=self.config.database,
                schema=self.config.landing_schema,
            )
            
            logger.info("Snowpipe Streaming client initialized")
            return self._streaming_client
            
        except ImportError:
            logger.warning(
                "snowpipe-streaming package not available, "
                "falling back to batch insert mode"
            )
            return None
    
    def initialize(self) -> None:
        """Initialize the client and ensure landing schema exists."""
        self.ddl.ensure_schema_exists(self.config.landing_schema)
        logger.info(f"Snowflake client initialized with landing schema: {self.config.landing_schema}")
    
    def close(self) -> None:
        """Close all resources."""
        # Close all open channels
        for table_name in list(self._channels.keys()):
            self.close_channel(table_name)
        
        # Close DDL connection
        self.ddl.close()
        
        logger.info("Snowflake client closed")
    
    def ensure_table_initialized(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        primary_key_columns: List[str]
    ) -> None:
        """Ensure landing table, target table, and task are created.
        
        Args:
            table_name: Base table name
            columns: Column definitions from source
            primary_key_columns: Primary key column names
        """
        if table_name in self._initialized_tables:
            return
        
        # Ensure landing schema exists
        self.ddl.ensure_schema_exists(self.config.landing_schema)
        
        # Create landing table
        self.ddl.create_landing_table(table_name, columns, primary_key_columns)
        
        # Create target table if it doesn't exist
        if not self.ddl.table_exists(table_name, self.config.schema):
            self.ddl.create_target_table(table_name, columns, primary_key_columns)
        
        # Create and resume merge task
        column_names = [col["name"] for col in columns]
        if not self.task_manager.task_exists(table_name):
            self.task_manager.create_merge_task(table_name, column_names, primary_key_columns)
            self.task_manager.resume_task(table_name)
        
        self._initialized_tables.add(table_name)
        logger.info(f"Table initialized: {table_name}")
    
    def open_channel(self, table_name: str) -> Channel:
        """Open a Snowpipe Streaming channel for a table.
        
        Args:
            table_name: Target landing table name (without 'landing_' prefix)
            
        Returns:
            Channel object for inserting rows
        """
        if table_name in self._channels and self._channels[table_name].is_open:
            return self._channels[table_name]
        
        landing_table = f"landing_{table_name}"
        channel_name = f"etl_channel_{table_name}"
        
        streaming_client = self._ensure_streaming_client()
        
        if streaming_client is not None:
            try:
                client = streaming_client.open_channel(
                    channel_name=channel_name,
                    table_name=landing_table,
                )
                channel = Channel(
                    name=channel_name,
                    table_name=landing_table,
                    is_open=True,
                    _client=client
                )
            except Exception as e:
                logger.warning(f"Failed to open streaming channel: {e}, using batch mode")
                channel = Channel(
                    name=channel_name,
                    table_name=landing_table,
                    is_open=True,
                    _client=None
                )
        else:
            channel = Channel(
                name=channel_name,
                table_name=landing_table,
                is_open=True,
                _client=None
            )
        
        self._channels[table_name] = channel
        logger.debug(f"Opened channel for table: {table_name}")
        return channel
    
    def insert_rows(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        operation: str = "INSERT"
    ) -> str:
        """Insert rows into a landing table.
        
        Args:
            table_name: Base table name (without 'landing_' prefix)
            rows: List of row dictionaries
            operation: ETL operation type (INSERT, UPDATE, DELETE)
            
        Returns:
            Offset token for tracking
        """
        if not rows:
            return ""
        
        # Auto-create table if it doesn't exist (infer schema from first row)
        landing_table = f"landing_{table_name}"
        if table_name not in self._initialized_tables:
            if not self.ddl.table_exists(landing_table, self.config.landing_schema):
                logger.info(f"Landing table {landing_table} does not exist, creating from row schema...")
                # Infer columns from first row
                columns = []
                for key, value in rows[0].items():
                    if key.startswith("_etl_"):
                        continue  # Skip ETL metadata columns, they'll be added by create_landing_table
                    col_type = self._infer_snowflake_type(value)
                    columns.append({
                        "name": key,
                        "type_oid": 0,
                        "type_name": col_type,
                        "modifier": -1,
                        "nullable": True,
                    })
                
                # Ensure schema exists
                self.ddl.ensure_schema_exists(self.config.landing_schema)
                
                # Create landing table
                self.ddl.create_landing_table(table_name, columns, [])
                logger.info(f"Created landing table: {landing_table}")
            
            self._initialized_tables.add(table_name)
        
        channel = self.open_channel(table_name)
        
        # Add ETL metadata to each row
        import time
        sequence_base = int(time.time() * 1000000)
        
        enriched_rows = []
        for i, row in enumerate(rows):
            enriched_row = dict(row)
            enriched_row["_etl_op"] = operation
            enriched_row["_etl_sequence"] = f"{sequence_base}_{i:08d}"
            enriched_rows.append(enriched_row)
        
        # Try streaming insert first
        if channel._client is not None:
            try:
                result = channel._client.insert_rows(enriched_rows)
                logger.debug(f"Streamed {len(rows)} rows to {table_name}")
                return str(result.get("offset", ""))
            except Exception as e:
                logger.warning(f"Streaming insert failed: {e}, falling back to batch")
        
        # Fallback to batch insert via SQL
        self._batch_insert(table_name, enriched_rows)
        return f"{sequence_base}_{len(rows)-1:08d}"
    
    def _infer_snowflake_type(self, value: Any) -> str:
        """Infer Snowflake type from Python value.
        
        Args:
            value: Python value
            
        Returns:
            Snowflake type name
        """
        if value is None:
            return "VARCHAR"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "NUMBER"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            return "VARCHAR"
        elif isinstance(value, (dict, list)):
            return "VARIANT"
        else:
            return "VARCHAR"
    
    def _batch_insert(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        """Fallback batch insert using SQL.
        
        Args:
            table_name: Base table name
            rows: List of row dictionaries with ETL metadata
        """
        if not rows:
            return
        
        landing_table = f'"{self.config.database}"."{self.config.landing_schema}"."landing_{table_name}"'
        
        # Get column names from first row
        columns = list(rows[0].keys())
        columns_sql = ", ".join(f'"{col}"' for col in columns)
        
        # Build VALUES clause
        values_list = []
        for row in rows:
            values = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    values.append("NULL")
                elif isinstance(val, str):
                    # Escape single quotes
                    escaped = val.replace("'", "''")
                    values.append(f"'{escaped}'")
                elif isinstance(val, bool):
                    values.append("TRUE" if val else "FALSE")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, (dict, list)):
                    import json
                    escaped = json.dumps(val).replace("'", "''")
                    values.append(f"PARSE_JSON('{escaped}')")
                else:
                    escaped = str(val).replace("'", "''")
                    values.append(f"'{escaped}'")
            values_list.append(f"({', '.join(values)})")
        
        # Split into batches of 1000 for large inserts
        batch_size = 1000
        for i in range(0, len(values_list), batch_size):
            batch = values_list[i:i + batch_size]
            sql = f"INSERT INTO {landing_table} ({columns_sql}) VALUES {', '.join(batch)}"
            self.ddl.execute(sql)
        
        logger.debug(f"Batch inserted {len(rows)} rows to {table_name}")
    
    def close_channel(self, table_name: str) -> None:
        """Close a Snowpipe Streaming channel.
        
        Args:
            table_name: Base table name
        """
        if table_name not in self._channels:
            return
        
        channel = self._channels[table_name]
        if channel._client is not None:
            try:
                channel._client.close()
            except Exception as e:
                logger.warning(f"Error closing channel: {e}")
        
        channel.is_open = False
        del self._channels[table_name]
        logger.debug(f"Closed channel for table: {table_name}")
    
    def handle_schema_evolution(
        self,
        table_name: str,
        new_columns: List[Dict[str, Any]],
        all_columns: List[Dict[str, Any]],
        primary_key_columns: List[str]
    ) -> None:
        """Handle schema evolution by adding new columns.
        
        Args:
            table_name: Base table name
            new_columns: List of new column definitions to add
            all_columns: Complete list of all columns (for task recreation)
            primary_key_columns: Primary key column names
        """
        if not new_columns:
            return
        
        logger.info(f"Schema evolution detected for {table_name}: adding {len(new_columns)} columns")
        
        # Add columns to landing table
        landing_table = f"landing_{table_name}"
        self.ddl.alter_add_columns(landing_table, self.config.landing_schema, new_columns)
        
        # Add columns to target table
        self.ddl.alter_add_columns(table_name, self.config.schema, new_columns)
        
        # Recreate merge task with new columns
        column_names = [col["name"] for col in all_columns]
        self.task_manager.recreate_task(table_name, column_names, primary_key_columns)
        
        logger.info(f"Schema evolution completed for {table_name}")
    
    def truncate_table(self, table_name: str) -> None:
        """Truncate landing and target tables, recreate task.
        
        Args:
            table_name: Base table name
        """
        landing_table = f"landing_{table_name}"
        
        # Drop and mark for recreation
        self.ddl.drop_table(landing_table, self.config.landing_schema)
        self.ddl.drop_table(table_name, self.config.schema)
        
        # Remove from initialized set to force recreation
        self._initialized_tables.discard(table_name)
        
        # Close channel if open
        if table_name in self._channels:
            self.close_channel(table_name)
        
        logger.info(f"Truncated table: {table_name}")
