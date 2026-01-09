"""Snowflake Streaming Client module.

Provides Snowpipe Streaming integration for low-latency data ingestion.
"""

import logging
import json
import os
import uuid
import logging
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field

try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pa = None  # type: ignore

from etl_snowflake.config import SnowflakeConfig
from etl_snowflake.ddl import SnowflakeDDL
from etl_snowflake.task import SnowflakeTaskManager
from etl_snowflake.cleanup import ResourceCleaner

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
        self._streaming_clients: Dict[str, Any] = {}  # table_name -> client
        self._resource_cleaner = ResourceCleaner()  # Resource cleanup manager

    def _load_private_key_pem(self) -> str:
        """Load and decode private key to PEM format."""
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            with open(self.config.private_key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=(
                        self.config.private_key_passphrase.encode()
                        if self.config.private_key_passphrase
                        else None
                    ),
                    backend=default_backend(),
                )

            return private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            ).decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to load private key: {e}")
            raise

    def _ensure_streaming_client(self, table_name: str) -> Any:
        """Lazily initialize the Snowpipe Streaming client for a specific table.

        Args:
            table_name: The table to stream to.

        Returns:
            Snowpipe Streaming client instance bound to the table.
        """
        if table_name in self._streaming_clients:
            return self._streaming_clients[table_name]

        try:
            from snowflake.ingest.streaming import StreamingIngestClient

            # Helper to get path (it should already be created by ensure_table_initialized, but strictly ensure here too)
            private_key_pem = self._load_private_key_pem()
            profile_path = self._create_profile_json(table_name, private_key_pem)

            # The SDK will auto-create the "default" streaming pipe on the first call.
            # We provide a pipe_name that follows the pattern LANDING_<table>.
            # (Do NOT manually CREATE PIPE via SQL - that creates a standard pipe and causes ERR_PIPE_KIND_NOT_SUPPORTED)
            landing_table = f"LANDING_{table_name.upper()}"
            pipe_name = f"{landing_table}-STREAMING"

            client = StreamingIngestClient(
                client_name=f"ETL_CLIENT_{table_name}_{uuid.uuid4()}",
                db_name=self.config.database,
                schema_name=self.config.landing_schema,
                pipe_name=pipe_name,  # Required: SDK auto-creates streaming pipe on first use
                profile_json=profile_path,
            )

            self._streaming_clients[table_name] = client
            logger.info(f"Snowpipe Streaming client initialized for table {table_name}")
            return client

        except ImportError:
            # If snowflake-ingest is missing, we cannot proceed as we are strictly streaming now.
            logger.error("snowflake-ingest package not available. Critical error.")
            raise ImportError(
                "snowflake-ingest package is required for Snowpipe Streaming."
            )

    def _create_profile_json(self, table_name: str, private_key_pem: str) -> str:
        """Create a profile.json file for the Snowpipe Streaming client.

        Args:
            table_name: The name of the table this client is for.
            private_key_pem: The unencrypted PEM string of the private key.

        Returns:
            Absolute path to the generated profile.json file.
        """
        # Ensure profile directory exists in current working directory
        profile_dir = os.path.join(os.getcwd(), "profile_json")
        os.makedirs(profile_dir, exist_ok=True)

        filename = f"profile_{table_name}.json"
        filepath = os.path.join(profile_dir, filename)

        # Helper function to construct URL
        # Scheme + Host + Port
        # Expected format: https://<account>.<locator>.snowflakecomputing.com:443
        host = self.config.host
        if not host:
            host = f"{self.config.account}.snowflakecomputing.com"

        # Ensure scheme is not duplicated if already in host
        if host.startswith("http"):
            url = f"{host}:443"
        else:
            url = f"https://{host}:443"

        # Build profile dictionary matching profile.json.example structure
        profile = {
            "url": url,
            "account": self.config.account,
            "user": self.config.user,
            "private_key": private_key_pem,
            "warehouse": self.config.warehouse,
            "database": self.config.database,
            "schema": self.config.landing_schema,
        }

        if self.config.role:
            profile["role"] = self.config.role

        # Write profile with secure permissions (owner read/write only)
        # Set umask to restrict file permissions before writing
        old_umask = os.umask(0o077)  # Temporary umask for secure file creation
        try:
            with open(filepath, "w") as f:
                json.dump(profile, f, indent=2)

            # Explicitly set file permissions to 0o600 (owner read/write only)
            # This is critical as profile contains private key
            os.chmod(filepath, 0o600)
            logger.info(
                f"Generated secure profile.json for table {table_name} at {filepath} (mode: 0600)"
            )
        finally:
            os.umask(old_umask)  # Restore original umask

        return filepath

    def initialize(self) -> None:
        """Initialize the client and ensure landing schema exists.

        Raises:
            Exception: If initialization fails (connection, permissions, etc.)
        """
        logger.info("=" * 60)
        logger.info("SNOWFLAKE CLIENT INITIALIZATION STARTING")
        logger.info(f"Account: {self.config.account}")
        logger.info(f"User: {self.config.user}")
        logger.info(f"Database: {self.config.database}")
        logger.info(f"Target Schema: {self.config.schema}")
        logger.info(f"Landing Schema: {self.config.landing_schema}")
        logger.info(f"Warehouse: {self.config.warehouse}")
        if self.config.role:
            logger.info(f"Role: {self.config.role}")
        logger.info("=" * 60)

        try:
            # Test connection
            logger.info("Testing Snowflake connection...")
            conn = self.ddl.connect()
            logger.info("Connection successful!")

            # Ensure landing schema exists
            logger.info(
                f"Ensuring landing schema '{self.config.landing_schema}' exists..."
            )
            self.ddl.ensure_schema_exists(self.config.landing_schema)
            logger.info(f"Landing schema ready: {self.config.landing_schema}")

            logger.info("=" * 60)
            logger.info("SNOWFLAKE CLIENT INITIALIZATION COMPLETE")
            logger.info("=" * 60)
        except Exception as e:
            logger.error("=" * 60)
            logger.error("SNOWFLAKE CLIENT INITIALIZATION FAILED")
            logger.error(f"Error: {e}")
            logger.error("=" * 60)
            raise

    def close(self) -> None:
        """Close all resources and cleanup."""
        # Close all open channels
        for table_name in list(self._channels.keys()):
            self.close_channel(table_name)

        # Cleanup all resources (profiles, channels)
        cleanup_stats = self._resource_cleaner.cleanup_all(self._streaming_clients)
        logger.info(f"Resource cleanup completed: {cleanup_stats}")

        # Close DDL connection
        self.ddl.close()

        logger.info("Snowflake client closed")

    def ensure_table_initialized(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        primary_key_columns: List[str],
    ) -> None:
        """Ensure landing table, target table, and task are created.

        Args:
            table_name: Base table name
            columns: Column definitions from source
            primary_key_columns: Primary key column names

        Raises:
            Exception: If table creation fails, with detailed error
        """
        if table_name in self._initialized_tables:
            logger.debug(f"Table {table_name} already initialized in memory, skipping")
            return

        logger.info(f"=== Initializing table: {table_name} ===")
        logger.info(f"Columns: {len(columns)}, Primary keys: {primary_key_columns}")

        # Track what was created for potential rollback
        created_landing = False
        created_target = False
        created_task = False

        try:
            # Step 1: Ensure landing schema exists
            try:
                logger.info(
                    f"Step 1: Ensuring landing schema '{self.config.landing_schema}' exists..."
                )
                self.ddl.ensure_schema_exists(self.config.landing_schema)
                logger.info(f"Step 1: Landing schema OK")
            except Exception as e:
                logger.error(
                    f"FAILED to create landing schema '{self.config.landing_schema}': {e}"
                )
                raise Exception(f"Failed to create landing schema: {e}") from e

            # Step 2: Create landing table if it doesn't exist
            landing_table = f"LANDING_{table_name.upper()}"
            try:
                logger.info(
                    f"Step 2: Checking/creating landing table '{landing_table}'..."
                )
                if not self.ddl.table_exists(landing_table, self.config.landing_schema):
                    self.ddl.create_landing_table(
                        table_name, columns, primary_key_columns
                    )
                    created_landing = True
                    logger.info(f"Step 2: Landing table created")
                else:
                    logger.info(
                        f"Step 2: Landing table already exists, skipping creation"
                    )
            except Exception as e:
                logger.error(f"FAILED to create landing table '{landing_table}': {e}")
                raise Exception(f"Failed to create landing table: {e}") from e

            # Step 3: Create target table if it doesn't exist
            try:
                target_table = table_name.upper()
                logger.info(
                    f"Step 3: Checking/creating target table '{target_table}' in schema '{self.config.schema}'..."
                )
                if not self.ddl.table_exists(target_table, self.config.schema):
                    self.ddl.create_target_table(
                        table_name, columns, primary_key_columns
                    )
                    created_target = True
                    logger.info(f"Step 3: Target table created")
                else:
                    logger.info(
                        f"Step 3: Target table already exists, skipping creation"
                    )
            except Exception as e:
                logger.error(f"FAILED to create target table '{table_name}': {e}")
                # Clean up landing table if we created it
                if created_landing:
                    try:
                        logger.warning(
                            f"Rolling back: dropping landing table {landing_table}"
                        )
                        self.ddl.drop_table(landing_table, self.config.landing_schema)
                    except Exception as cleanup_error:
                        logger.error(
                            f"Failed to cleanup landing table during rollback: {cleanup_error}"
                        )
                raise Exception(f"Failed to create target table: {e}") from e

            # Step 4: Create and resume merge task (only if we have primary keys)
            if primary_key_columns:
                try:
                    logger.info(f"Step 4: Creating merge task for '{table_name}'...")
                    column_names = [col["name"] for col in columns]
                    if not self.task_manager.task_exists(table_name):
                        self.task_manager.create_merge_task(
                            table_name, column_names, primary_key_columns
                        )
                        created_task = True
                        self.task_manager.resume_task(table_name)
                        logger.info(f"Step 4: Merge task created and resumed")
                    else:
                        logger.info(f"Step 4: Merge task already exists")
                except Exception as e:
                    logger.error(f"FAILED to create merge task for '{table_name}': {e}")
                    # Don't fail table initialization if task creation fails
                    # Tables can still function without automatic merge
                    logger.warning(
                        f"WARNING: Table {table_name} created but merge task failed. Data will accumulate in landing table."
                    )
                    logger.warning(
                        f"Manual intervention may be required to merge data or recreate task."
                    )
            else:
                logger.warning(
                    f"Step 4: Skipping merge task - no primary keys defined for table '{table_name}'"
                )

            # Step 5: Eagerly generate profile.json
            # This ensures the authentication profile is ready immediately after initialization
            try:
                logger.info(f"Step 5: Generating profile.json for '{table_name}'...")
                private_key_pem = self._load_private_key_pem()
                self._create_profile_json(table_name, private_key_pem)
                logger.info(f"Step 5: Profile generated")
            except Exception as e:
                logger.error(f"FAILED to generate profile.json for '{table_name}': {e}")
                logger.warning(
                    f"Profile generation failed - streaming inserts will not work until this is resolved"
                )
                # Non-critical for table creation, but log prominently

            # Mark as initialized only if we got this far
            self._initialized_tables.add(table_name)
            logger.info(f"=== Table {table_name} initialized successfully ===")

        except Exception as e:
            # Log comprehensive error for debugging
            logger.error(f"=== Table {table_name} initialization FAILED ===")
            logger.error(f"Error: {e}")
            logger.error(
                f"Created resources: landing={created_landing}, target={created_target}, task={created_task}"
            )
            raise

    def ensure_tables_from_publication(self, tables: List[Dict[str, Any]]) -> List[str]:
        """Ensure all tables from a publication exist in Snowflake.

        This method is called during pipeline startup to pre-create all tables
        defined in the source publication. It checks each table and creates
        the landing table, target table, and merge task if not already present.

        Args:
            tables: List of table definitions with keys:
                - name: Table name (without schema prefix)
                - columns: List of column definitions
                - primary_key_columns: Optional list of primary key column names

        Returns:
            List of table names that were newly created
        """
        logger.info("=" * 60)
        logger.info(f"PUBLICATION TABLE SYNC: Processing {len(tables)} tables")
        logger.info("=" * 60)

        if not tables:
            logger.warning("No tables provided for publication sync")
            return []

        created_tables = []
        skipped_tables = []
        failed_tables = []

        for i, table_def in enumerate(tables, 1):
            table_name = table_def.get("name", "")
            if not table_name:
                logger.warning(f"[{i}/{len(tables)}] Skipping table with empty name")
                continue

            columns = table_def.get("columns", [])
            pk_columns = table_def.get("primary_key_columns", [])

            logger.info(
                f"[{i}/{len(tables)}] Processing table: {table_name} ({len(columns)} columns, PK: {pk_columns})"
            )

            # Check if landing table already exists
            landing_table = f"LANDING_{table_name.upper()}"
            try:
                target_table = table_name.upper()
                if self.ddl.table_exists(landing_table, self.config.landing_schema):
                    logger.info(
                        f"[{i}/{len(tables)}] Table {table_name} already exists, skipping"
                    )
                    self._initialized_tables.add(table_name)
                    skipped_tables.append(table_name)
                    continue
            except Exception as e:
                logger.error(
                    f"[{i}/{len(tables)}] Error checking table existence for {table_name}: {e}"
                )
                failed_tables.append(table_name)
                continue

            # Create the table
            try:
                self.ensure_table_initialized(table_name, columns, pk_columns)
                created_tables.append(table_name)
                logger.info(
                    f"[{i}/{len(tables)}] Successfully created table: {table_name}"
                )
            except Exception as e:
                logger.error(
                    f"[{i}/{len(tables)}] FAILED to create table {table_name}: {e}"
                )
                failed_tables.append(table_name)

        logger.info("=" * 60)
        logger.info(f"PUBLICATION SYNC COMPLETE")
        logger.info(f"  Created: {len(created_tables)} tables")
        logger.info(f"  Skipped (already exist): {len(skipped_tables)} tables")
        logger.info(f"  Failed: {len(failed_tables)} tables")
        if failed_tables:
            logger.error(f"  Failed tables: {failed_tables}")
        logger.info("=" * 60)

        return created_tables

    def open_channel(self, table_name: str) -> Channel:
        """Open a Snowpipe Streaming channel for a table.

        Args:
            table_name: Target landing table name (without 'landing_' prefix)

        Returns:
            Channel object for inserting rows
        """
        if table_name in self._channels and self._channels[table_name].is_open:
            return self._channels[table_name]

        landing_table = f"LANDING_{table_name.upper()}"
        channel_name = f"etl_channel_{table_name}"

        # Get table-specific client
        # IMPORTANT: Pass base table_name, NOT landing_table, because
        # _ensure_streaming_client adds the LANDING_ prefix itself.
        streaming_client = self._ensure_streaming_client(table_name)

        if streaming_client is not None:
            try:
                # Open the channel using the new SDK
                # Returns (channel, status) tuple, so we assume index 0 is channel
                client_tuple = streaming_client.open_channel(
                    channel_name=channel_name,
                )
                client = client_tuple[0]  # Get the channel object

                channel = Channel(
                    name=channel_name,
                    table_name=landing_table,
                    is_open=True,
                    _client=client,
                )
            except Exception as e:
                logger.error(f"Failed to open streaming channel: {e}")
                raise
        else:
            # Should technically be unreachable due to ensure_streaming_client raising ImportError
            raise RuntimeError("Streaming client could not be initialized")

        self._channels[table_name] = channel
        logger.debug(f"Opened channel for table: {table_name}")
        return channel

    def insert_arrow_batch(
        self, table_name: str, batch: "pa.RecordBatch", operation: str = "INSERT"
    ) -> str:
        """Insert Arrow RecordBatch into landing table (zero-copy from Rust).

        This method accepts a PyArrow RecordBatch from the Rust side,
        adds ETL metadata columns, and passes to the Snowpipe Streaming SDK.

        Args:
            table_name: Base table name (without 'landing_' prefix)
            batch: PyArrow RecordBatch from Rust (via pyo3-arrow)
            operation: ETL operation type (INSERT, UPDATE, DELETE)

        Returns:
            Offset token for tracking
        """
        if not HAS_PYARROW:
            raise RuntimeError("pyarrow is required for insert_arrow_batch")

        if batch is None or batch.num_rows == 0:
            return ""

        import time

        sequence_base = int(time.time() * 1000000)
        num_rows = batch.num_rows

        # Add ETL metadata columns efficiently using Arrow
        ops = [operation] * num_rows
        seqs = [f"{sequence_base}_{i:08d}" for i in range(num_rows)]

        batch_with_meta = batch.append_column("OPERATION", pa.array(ops))
        batch_with_meta = batch_with_meta.append_column("SEQUENCE", pa.array(seqs))

        # Convert to list of dicts for Snowpipe Streaming SDK
        rows = batch_with_meta.to_pylist()

        logger.debug(f"Arrow batch of {num_rows} rows converted for {table_name}")

        channel = self.open_channel(table_name)

        # Strictly use streaming insert
        if channel._client is not None:
            try:
                # Use append_rows instead of insert_rows
                # We will use the last seq as the offset token
                last_seq = seqs[-1]

                # append_rows(rows, start_offset_token, end_offset_token)
                # It returns None, so we return the token we sent
                channel._client.append_rows(rows, end_offset_token=last_seq)
                logger.debug(f"Streamed {len(rows)} rows to {table_name} (via Arrow)")
                return last_seq

            except Exception as e:
                logger.error(f"Streaming insert failed for {table_name}: {e}")
                raise
        else:
            raise RuntimeError(
                f"Channel for {table_name} is not connected to streaming client."
            )

    def insert_rows(
        self, table_name: str, rows: List[Dict[str, Any]], operation: str = "INSERT"
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

        # Auto-create tables if either landing or target doesn't exist (infer schema from first row)
        landing_table = f"LANDING_{table_name.upper()}"
        if table_name not in self._initialized_tables:
            # Check if EITHER landing OR target table is missing
            target_table = table_name.upper()
            landing_exists = self.ddl.table_exists(
                landing_table, self.config.landing_schema
            )
            target_exists = self.ddl.table_exists(target_table, self.config.schema)

            if landing_exists and target_exists:
                # Both tables exist, just mark as initialized and continue
                logger.info(f"Tables for {table_name} already exist, skipping creation")
                self._initialized_tables.add(table_name)
            elif not landing_exists or not target_exists:
                logger.info(
                    f"Auto-creating tables for {table_name} (landing_exists={landing_exists}, target_exists={target_exists})"
                )

                # Detect primary keys from first row
                primary_key_columns = self._detect_primary_keys(rows[0])

                # Infer columns from first row
                columns = []
                for key, value in rows[0].items():
                    if key in ("OPERATION", "SEQUENCE", "TIMESTAMP"):
                        continue  # Skip ETL metadata columns, they'll be added by create_landing_table
                    col_type = self._infer_snowflake_type(value)
                    columns.append(
                        {
                            "name": key,
                            "type_oid": 0,
                            "type_name": col_type,
                            "modifier": -1,
                            "nullable": True,
                        }
                    )

                # Ensure landing schema exists
                self.ddl.ensure_schema_exists(self.config.landing_schema)

                # Create landing table if not exists
                if not landing_exists:
                    self.ddl.create_landing_table(
                        table_name, columns, primary_key_columns
                    )
                    logger.info(f"Created landing table: {landing_table}")

                # Ensure target schema exists
                self.ddl.ensure_schema_exists(self.config.schema)

                # Create target table if not exists
                if not target_exists:
                    self.ddl.create_target_table(
                        table_name, columns, primary_key_columns
                    )
                    logger.info(f"Created target table: {table_name}")

                # Create and start merge task if primary keys were detected
                if primary_key_columns:
                    try:
                        column_names = [col["name"] for col in columns]
                        if not self.task_manager.task_exists(table_name):
                            self.task_manager.create_merge_task(
                                table_name, column_names, primary_key_columns
                            )
                            self.task_manager.resume_task(table_name)
                            logger.info(
                                f"Created and started merge task for: {table_name}"
                            )
                        else:
                            logger.info(f"Merge task already exists for: {table_name}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to create merge task for {table_name}: {e}"
                        )

                self._initialized_tables.add(table_name)

        channel = self.open_channel(table_name)

        # Add ETL metadata to each row
        import time

        sequence_base = int(time.time() * 1000000)

        enriched_rows = []
        for i, row in enumerate(rows):
            enriched_row = dict(row)
            enriched_row["OPERATION"] = operation
            enriched_row["SEQUENCE"] = f"{sequence_base}_{i:08d}"
            enriched_rows.append(enriched_row)

        # Strictly use streaming insert
        if channel._client is not None:
            try:
                # Use the last sequence number as the offset token
                last_seq = enriched_rows[-1]["SEQUENCE"]

                # append_rows
                channel._client.append_rows(enriched_rows, end_offset_token=last_seq)
                logger.debug(f"Streamed {len(rows)} rows to {table_name}")
                return last_seq

            except Exception as e:
                logger.error(f"Streaming insert failed for {table_name}: {e}")
                raise
        else:
            raise RuntimeError(
                f"Channel for {table_name} is not connected to streaming client."
            )

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
            return "DOUBLE"
        elif isinstance(value, list):
            return "ARRAY"
        elif isinstance(value, dict):
            return "VARIANT"
        elif isinstance(value, str):
            # Try to detect if it's a numeric string (often used for high-precision NUMERIC)
            # Must contain only digits, optional minus sign, and optional single dot
            import re

            if re.match(r"^-?\d+(\.\d+)?$", value):
                if "." in value:
                    return "NUMBER(38, 10)"  # Reasonable default for decimal strings
                else:
                    return "BIGINT"
            return "VARCHAR"
        else:
            return "VARCHAR"

    def _detect_primary_keys(self, row: Dict[str, Any]) -> List[str]:
        """Detect potential primary key columns from row data.

        Uses common naming conventions to identify primary key candidates:
        1. Column named exactly 'id'
        2. Columns ending with '_id' (but prioritize 'id' alone)

        Args:
            row: Sample row dictionary

        Returns:
            List of detected primary key column names
        """
        column_names = [
            key
            for key in row.keys()
            if key not in ("OPERATION", "SEQUENCE", "TIMESTAMP")
        ]

        # Priority 1: Check for exact 'id' column (case-insensitive)
        for col in column_names:
            if col.lower() == "id":
                logger.info(f"Detected primary key: {col} (exact match)")
                return [col]

        # Priority 2: Check for columns ending with '_id' that look like primary keys
        # Prefer table-specific ids like 'user_id', 'order_id' over foreign key patterns
        id_columns = [col for col in column_names if col.lower().endswith("_id")]

        if id_columns:
            # If there's only one _id column, use it
            if len(id_columns) == 1:
                logger.info(
                    f"Detected primary key: {id_columns[0]} (single _id column)"
                )
                return id_columns

            # If multiple _id columns, look for common primary key patterns
            pk_patterns = ["pk_id", "primary_id", "row_id", "record_id"]
            for col in id_columns:
                if col.lower() in pk_patterns:
                    logger.info(f"Detected primary key: {col} (pattern match)")
                    return [col]

            # Fall back to first _id column if no clear winner
            logger.info(f"Detected primary key: {id_columns[0]} (first _id column)")
            return [id_columns[0]]

        # Priority 3: Check for uuid columns
        for col in column_names:
            if "uuid" in col.lower():
                logger.info(f"Detected primary key: {col} (uuid column)")
                return [col]

        logger.warning("No primary key detected, merge task will not be created")
        return []

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
        primary_key_columns: List[str],
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

        logger.info(
            f"Schema evolution detected for {table_name}: adding {len(new_columns)} columns"
        )

        # Add columns to landing table
        landing_table = f"LANDING_{table_name.upper()}"
        target_table = table_name.upper()
        self.ddl.alter_add_columns(
            landing_table, self.config.landing_schema, new_columns
        )

        # Add columns to target table
        self.ddl.alter_add_columns(target_table, self.config.schema, new_columns)

        # Recreate merge task with new columns
        column_names = [col["name"] for col in all_columns]
        self.task_manager.recreate_task(table_name, column_names, primary_key_columns)

        logger.info(f"Schema evolution completed for {table_name}")

    def truncate_table(self, table_name: str) -> None:
        """Truncate landing table only. Target table should be preserved.

        Uses TRUNCATE instead of DROP to preserve schema and prevent data loss
        vulnerabilities on startup.

        Args:
            table_name: Base table name
        """
        landing_table = f"LANDING_{table_name.upper()}"

        # Only truncate landing table, don't drop target table
        # Using IF EXISTS for safety
        sql = f'TRUNCATE TABLE IF EXISTS "{self.config.database}"."{self.config.landing_schema}"."{landing_table}"'
        self.ddl.execute(sql)

        # Remove from initialized set to force re-check (but don't drop)
        self._initialized_tables.discard(table_name)

        # Close channel if open to ensure clean state
        if table_name in self._channels:
            self.close_channel(table_name)

        logger.info(f"Truncated landing table for: {table_name}")
