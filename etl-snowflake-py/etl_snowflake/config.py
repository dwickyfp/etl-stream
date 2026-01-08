"""Snowflake configuration module."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class SnowflakeConfig:
    """Configuration for Snowflake connection.
    
    Attributes:
        account: Snowflake account identifier (e.g., "xy12345.us-east-1")
        user: Snowflake username
        database: Target database name
        schema: Target schema name for tables
        warehouse: Compute warehouse name
        private_key_path: Path to private key file (.p8)
        private_key_passphrase: Optional passphrase for encrypted private key
        role: Optional Snowflake role to use
        landing_schema: Schema for landing tables (default: "ETL_SCHEMA")
        task_schedule_minutes: Task schedule interval in minutes (default: 60)
    """
    account: str
    user: str
    database: str
    schema: str
    warehouse: str
    private_key_path: Path
    private_key_passphrase: Optional[str] = None
    role: Optional[str] = None
    landing_schema: str = "ETL_SCHEMA"
    task_schedule_minutes: int = 60
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if isinstance(self.private_key_path, str):
            self.private_key_path = Path(self.private_key_path)
        
        if not self.private_key_path.exists():
            raise ValueError(f"Private key file not found: {self.private_key_path}")
        
        if not self.account:
            raise ValueError("Account is required")
        if not self.user:
            raise ValueError("User is required")
        if not self.database:
            raise ValueError("Database is required")
        if not self.warehouse:
            raise ValueError("Warehouse is required")
    
    def get_connection_params(self) -> dict:
        """Get connection parameters for Snowflake connector."""
        params = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "private_key_path": str(self.private_key_path),
            "private_key_passphrase": self.private_key_passphrase,
        }
        if self.role:
            params["role"] = self.role
        return params
