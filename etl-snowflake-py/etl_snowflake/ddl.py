"""Snowflake DDL operations module."""

import logging
from typing import List, Dict, Any, Optional

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from etl_snowflake.config import SnowflakeConfig
from etl_snowflake.type_mapping import get_snowflake_column_def

logger = logging.getLogger(__name__)


class SnowflakeDDL:
    """Handles Snowflake DDL operations.
    
    Responsible for creating schemas, tables, and executing ALTER commands.
    Uses key-pair authentication for secure connection.
    """
    
    def __init__(self, config: SnowflakeConfig):
        """Initialize DDL handler with configuration.
        
        Args:
            config: Snowflake configuration
        """
        self.config = config
        self._conn: Optional[SnowflakeConnection] = None
    
    def _get_private_key(self) -> bytes:
        """Load private key from file.
        
        Returns:
            Private key bytes for authentication
        """
        with open(self.config.private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=self.config.private_key_passphrase.encode() 
                    if self.config.private_key_passphrase else None,
                backend=default_backend()
            )
        
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    
    def connect(self) -> SnowflakeConnection:
        """Establish connection to Snowflake.
        
        Returns:
            Active Snowflake connection
        """
        if self._conn is not None and not self._conn.is_closed():
            return self._conn
        
        private_key = self._get_private_key()
        
        self._conn = snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            database=self.config.database,
            schema=self.config.schema,
            warehouse=self.config.warehouse,
            private_key=private_key,
        )
        
        logger.info(f"Connected to Snowflake: {self.config.account}")
        return self._conn
    
    def close(self) -> None:
        """Close the Snowflake connection."""
        if self._conn is not None and not self._conn.is_closed():
            self._conn.close()
            self._conn = None
            logger.info("Snowflake connection closed")
    
    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """Execute a SQL statement.
        
        Args:
            sql: SQL statement to execute
            params: Optional parameters for parameterized query
        """
        conn = self.connect()
        cursor = conn.cursor()
        try:
            logger.debug(f"Executing SQL: {sql[:200]}...")
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            logger.debug("SQL executed successfully")
        finally:
            cursor.close()
    
    def execute_batch(self, statements: List[str]) -> None:
        """Execute multiple SQL statements in a transaction.
        
        Args:
            statements: List of SQL statements
        """
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN")
            for sql in statements:
                logger.debug(f"Executing: {sql[:100]}...")
                cursor.execute(sql)
            cursor.execute("COMMIT")
            logger.info(f"Executed {len(statements)} statements successfully")
        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.error(f"Batch execution failed, rolled back: {e}")
            raise
        finally:
            cursor.close()
    
    def ensure_schema_exists(self, schema_name: str) -> None:
        """Ensure a schema exists, creating if necessary.
        
        Args:
            schema_name: Name of the schema to create
        """
        sql = f'CREATE SCHEMA IF NOT EXISTS "{self.config.database}"."{schema_name}"'
        self.execute(sql)
        logger.info(f"Schema ensured: {schema_name}")
    
    def create_landing_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        primary_key_columns: Optional[List[str]] = None
    ) -> None:
        """Create a landing table in the ETL schema.
        
        Landing tables include ETL metadata columns for tracking operations.
        
        Args:
            table_name: Base table name (will be prefixed with 'landing_')
            columns: List of column definitions from source
            primary_key_columns: Optional list of primary key column names
        """
        landing_table_name = f"landing_{table_name}"
        full_table_name = f'"{self.config.database}"."{self.config.landing_schema}"."{landing_table_name}"'
        
        # Build column definitions
        col_defs = []
        for col in columns:
            col_def = get_snowflake_column_def(
                column_name=col["name"],
                type_oid=col.get("type_oid", 0),
                type_name=col.get("type_name", ""),
                modifier=col.get("modifier", -1),
                nullable=col.get("nullable", True),
                is_primary_key=col["name"] in (primary_key_columns or [])
            )
            col_defs.append(col_def)
        
        # Add ETL metadata columns
        col_defs.extend([
            '"_etl_op" VARCHAR(6) NOT NULL',
            '"_etl_sequence" VARCHAR(64) NOT NULL',
            '"_etl_timestamp" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()'
        ])
        
        columns_sql = ",\n    ".join(col_defs)
        
        sql = f'''CREATE TABLE IF NOT EXISTS {full_table_name} (
    {columns_sql}
)'''
        
        self.execute(sql)
        logger.info(f"Landing table created: {landing_table_name}")
    
    def create_target_table(
        self,
        table_name: str,
        columns: List[Dict[str, Any]],
        primary_key_columns: Optional[List[str]] = None
    ) -> None:
        """Create a target table in the configured schema.
        
        Args:
            table_name: Table name
            columns: List of column definitions from source
            primary_key_columns: Optional list of primary key column names
        """
        full_table_name = f'"{self.config.database}"."{self.config.schema}"."{table_name}"'
        
        # Build column definitions
        col_defs = []
        for col in columns:
            col_def = get_snowflake_column_def(
                column_name=col["name"],
                type_oid=col.get("type_oid", 0),
                type_name=col.get("type_name", ""),
                modifier=col.get("modifier", -1),
                nullable=col.get("nullable", True),
                is_primary_key=col["name"] in (primary_key_columns or [])
            )
            col_defs.append(col_def)
        
        columns_sql = ",\n    ".join(col_defs)
        
        sql = f'''CREATE TABLE IF NOT EXISTS {full_table_name} (
    {columns_sql}
)'''
        
        self.execute(sql)
        logger.info(f"Target table created: {table_name}")
    
    def alter_add_columns(
        self,
        table_name: str,
        schema_name: str,
        new_columns: List[Dict[str, Any]]
    ) -> None:
        """Add new columns to an existing table.
        
        Args:
            table_name: Table name to alter
            schema_name: Schema containing the table
            new_columns: List of new column definitions
        """
        if not new_columns:
            return
        
        full_table_name = f'"{self.config.database}"."{schema_name}"."{table_name}"'
        
        # Build ADD COLUMN clauses
        add_clauses = []
        for col in new_columns:
            col_def = get_snowflake_column_def(
                column_name=col["name"],
                type_oid=col.get("type_oid", 0),
                type_name=col.get("type_name", ""),
                modifier=col.get("modifier", -1),
                nullable=True,  # New columns must be nullable
            )
            add_clauses.append(f"ADD COLUMN {col_def}")
        
        sql = f"ALTER TABLE {full_table_name} {', '.join(add_clauses)}"
        self.execute(sql)
        logger.info(f"Added {len(new_columns)} columns to {table_name}")
    
    def drop_table(self, table_name: str, schema_name: str) -> None:
        """Drop a table.
        
        Args:
            table_name: Table name to drop
            schema_name: Schema containing the table
        """
        full_table_name = f'"{self.config.database}"."{schema_name}"."{table_name}"'
        sql = f"DROP TABLE IF EXISTS {full_table_name}"
        self.execute(sql)
        logger.info(f"Dropped table: {table_name}")
    
    def table_exists(self, table_name: str, schema_name: str) -> bool:
        """Check if a table exists.
        
        Args:
            table_name: Table name to check
            schema_name: Schema containing the table
            
        Returns:
            True if table exists, False otherwise
        """
        sql = f'''
            SELECT COUNT(*) 
            FROM "{self.config.database}".INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        '''
        
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (schema_name.upper(), table_name.upper()))
            result = cursor.fetchone()
            return result[0] > 0 if result else False
        finally:
            cursor.close()
    
    def get_table_columns(self, table_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table.
        
        Args:
            table_name: Table name
            schema_name: Schema name
            
        Returns:
            List of column information dictionaries
        """
        sql = f'''
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                ORDINAL_POSITION
            FROM "{self.config.database}".INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        '''
        
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (schema_name.upper(), table_name.upper()))
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "position": row[3]
                })
            return columns
        finally:
            cursor.close()
