"""Snowflake task management module."""

import logging
from typing import Dict, List, Optional

from etl_snowflake.config import SnowflakeConfig
from etl_snowflake.ddl import SnowflakeDDL

logger = logging.getLogger(__name__)


# Snowflake types that require conversion from VARCHAR in landing tables
ARRAY_VARIANT_TYPES = {"ARRAY", "VARIANT"}


class SnowflakeTaskManager:
    """Manages Snowflake tasks for MERGE operations.

    Creates and manages scheduled tasks that merge data from landing tables
    into target tables.
    """

    def __init__(self, config: SnowflakeConfig, ddl: Optional[SnowflakeDDL] = None):
        """Initialize task manager.

        Args:
            config: Snowflake configuration
            ddl: Optional DDL handler (creates new if not provided)
        """
        self.config = config
        self.ddl = ddl or SnowflakeDDL(config)

    def _get_task_name(self, table_name: str) -> str:
        """Generate task name for a table.

        Args:
            table_name: Base table name

        Returns:
            Full task name
        """
        return f"ETL_MERGE_{table_name.upper()}_TASK"

    def _get_full_task_name(self, table_name: str) -> str:
        """Get fully qualified task name.

        Args:
            table_name: Base table name

        Returns:
            Fully qualified task name with database and schema
        """
        task_name = self._get_task_name(table_name)
        return f'"{self.config.database}"."{self.config.landing_schema}"."{task_name}"'

    def _get_column_expression(
        self, column_name: str, column_type: str, prefix: str = "source"
    ) -> str:
        """Generate SQL expression for a column with proper type conversion.

        For ARRAY and VARIANT columns, wraps the column reference with
        TRY_PARSE_JSON to convert string representations from landing table
        to proper Snowflake structured types.

        Args:
            column_name: Column name
            column_type: Snowflake type (e.g., 'ARRAY', 'VARIANT', 'VARCHAR')
            prefix: Table alias prefix (default: 'source')

        Returns:
            SQL expression for the column
        """
        # Normalize type (remove precision/scale for comparison)
        base_type = column_type.split("(")[0].upper().strip()

        if base_type == "ARRAY":
            return f'TRY_PARSE_JSON({prefix}."{column_name}")::ARRAY'
        elif base_type == "VARIANT":
            return f'TRY_PARSE_JSON({prefix}."{column_name}")::VARIANT'
        else:
            return f'{prefix}."{column_name}"'

    def create_merge_task(
        self,
        table_name: str,
        column_names: List[str],
        primary_key_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
    ) -> None:
        """Create a MERGE task for a table.

        Creates a scheduled task that merges data from the landing table
        into the target table, handling INSERT, UPDATE, and DELETE operations.

        For ARRAY and VARIANT columns, the task applies TRY_PARSE_JSON to convert
        string representations from the landing table to proper Snowflake types.

        Args:
            table_name: Base table name
            column_names: List of all column names (excluding ETL metadata)
            primary_key_columns: List of primary key column names
            column_types: Optional dict mapping column names to Snowflake types.
                          Used for ARRAY/VARIANT type conversion in MERGE.

        Raises:
            ValueError: If identifiers are invalid or dangerous
        """
        if not primary_key_columns:
            raise ValueError(f"Primary key columns required for table {table_name}")

        # Import validation function
        from etl_snowflake.ddl import validate_identifier

        # Validate all identifiers to prevent SQL injection
        validate_identifier(table_name, "table_name")
        for col in column_names:
            validate_identifier(col, "column_name")
        for col in primary_key_columns:
            validate_identifier(col, "primary_key_column")

        full_task_name = self._get_full_task_name(table_name)
        landing_table_name = f"LANDING_{table_name.upper()}"
        target_table_name = table_name.upper()
        landing_table = f'"{self.config.database}"."{self.config.landing_schema}"."{landing_table_name}"'
        target_table = (
            f'"{self.config.database}"."{self.config.schema}"."{target_table_name}"'
        )

        # Build primary key match condition
        pk_columns_quoted = [f'"{col}"' for col in primary_key_columns]
        pk_match = " AND ".join(
            f'target."{col}" = source."{col}"' for col in primary_key_columns
        )

        # Build column lists with type conversion for ARRAY/VARIANT
        column_types = column_types or {}
        all_columns_quoted = [f'"{col}"' for col in column_names]

        # Generate column expressions with proper type conversion
        update_set_parts = []
        insert_value_parts = []
        for col in column_names:
            col_type = column_types.get(col, "VARCHAR")
            col_expr = self._get_column_expression(col, col_type)
            update_set_parts.append(f'target."{col}" = {col_expr}')
            insert_value_parts.append(col_expr)

        update_set = ", ".join(update_set_parts)
        insert_columns = ", ".join(all_columns_quoted)
        insert_values = ", ".join(insert_value_parts)

        schedule_minutes = self.config.task_schedule_minutes

        sql = f"""CREATE OR REPLACE TASK {full_task_name}
WAREHOUSE = {self.config.warehouse}
SCHEDULE = '{schedule_minutes} MINUTES'
AS
BEGIN
    LET max_seq VARCHAR;
    
    SELECT COALESCE(MAX("SEQUENCE"), '0') INTO :max_seq
    FROM {landing_table};
    
    -- Only proceed if there's data to merge
    IF (max_seq != '0') THEN
        MERGE INTO {target_table} AS target
        USING (
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {", ".join(pk_columns_quoted)}
                    ORDER BY "SEQUENCE" DESC
                ) AS _dedupe_id
                FROM {landing_table}
                WHERE "SEQUENCE" <= :max_seq
            ) AS subquery
            WHERE _dedupe_id = 1
        ) AS source
        ON {pk_match}
        WHEN MATCHED AND source."OPERATION" = 'DELETE' THEN DELETE
        WHEN MATCHED AND source."OPERATION" IN ('INSERT', 'UPDATE') THEN 
            UPDATE SET {update_set}
        WHEN NOT MATCHED AND source."OPERATION" IN ('INSERT', 'UPDATE') THEN 
            INSERT ({insert_columns}) VALUES ({insert_values});
        
        -- Clean up processed rows
        DELETE FROM {landing_table}
        WHERE "SEQUENCE" <= :max_seq;
    END IF;
END;"""

        self.ddl.execute(sql)
        logger.info(f"Created merge task: {self._get_task_name(table_name)}")

    def resume_task(self, table_name: str) -> None:
        """Resume (enable) a task.

        Args:
            table_name: Base table name
        """
        full_task_name = self._get_full_task_name(table_name)
        sql = f"ALTER TASK {full_task_name} RESUME"
        self.ddl.execute(sql)
        logger.info(f"Resumed task for table: {table_name}")

    def suspend_task(self, table_name: str) -> None:
        """Suspend (disable) a task.

        Args:
            table_name: Base table name
        """
        full_task_name = self._get_full_task_name(table_name)
        sql = f"ALTER TASK {full_task_name} SUSPEND"
        self.ddl.execute(sql)
        logger.info(f"Suspended task for table: {table_name}")

    def drop_task(self, table_name: str) -> None:
        """Drop a task.

        Args:
            table_name: Base table name
        """
        full_task_name = self._get_full_task_name(table_name)
        sql = f"DROP TASK IF EXISTS {full_task_name}"
        self.ddl.execute(sql)
        logger.info(f"Dropped task for table: {table_name}")

    def task_exists(self, table_name: str) -> bool:
        """Check if a task exists.

        Args:
            table_name: Base table name

        Returns:
            True if task exists, False otherwise
        """
        task_name = self._get_task_name(table_name)

        # Use SHOW TASKS LIKE for checking task existence
        sql = f'SHOW TASKS LIKE \'{task_name}\' IN SCHEMA "{self.config.database}"."{self.config.landing_schema}"'

        conn = self.ddl.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            return len(results) > 0
        finally:
            cursor.close()

    def recreate_task(
        self,
        table_name: str,
        column_names: List[str],
        primary_key_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
    ) -> None:
        """Drop and recreate a task (used for schema evolution).

        Args:
            table_name: Base table name
            column_names: Updated list of column names
            primary_key_columns: List of primary key column names
            column_types: Optional dict mapping column names to Snowflake types
        """
        # Suspend first to avoid conflicts
        if self.task_exists(table_name):
            try:
                self.suspend_task(table_name)
            except Exception:
                pass  # Task may already be suspended or not exist
            self.drop_task(table_name)

        self.create_merge_task(table_name, column_names, primary_key_columns, column_types)
        self.resume_task(table_name)
        logger.info(f"Recreated task for table: {table_name}")
