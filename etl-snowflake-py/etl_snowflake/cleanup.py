"""Resource cleanup utilities for Snowflake client.

Provides cleanup mechanisms for:
- Snowpipe Streaming channels
- Profile JSON files
- Connection resources
"""

import logging
import os
import time
from pathlib import Path
from typing import Set, Dict, Any

logger = logging.getLogger(__name__)


class ResourceCleaner:
    """Manages cleanup of Snowflake client resources."""

    def __init__(self, profile_dir: str = "profile_json"):
        """Initialize resource cleaner.

        Args:
            profile_dir: Directory containing profile JSON files
        """
        self.profile_dir = Path(profile_dir)
        self.active_tables: Set[str] = set()
        self.channel_last_used: Dict[str, float] = {}
        self.channel_ttl_seconds = 3600  # 1 hour

    def register_table(self, table_name: str) -> None:
        """Register a table as actively used.

        Args:
            table_name: Name of the table
        """
        self.active_tables.add(table_name)
        self.channel_last_used[table_name] = time.time()

    def unregister_table(self, table_name: str) -> None:
        """Unregister a table (e.g., when dropped).

        Args:
            table_name: Name of the table
        """
        self.active_tables.discard(table_name)
        self.channel_last_used.pop(table_name, None)

    def cleanup_profile_files(self, keep_active: bool = True) -> int:
        """Clean up profile JSON files for inactive tables.

        Args:
            keep_active: If True, only remove profiles for inactive tables

        Returns:
            Number of files cleaned up
        """
        if not self.profile_dir.exists():
            return 0

        cleaned = 0
        for profile_file in self.profile_dir.glob("profile_*.json"):
            try:
                # Extract table name from filename (profile_<table_name>.json)
                table_name = profile_file.stem.replace("profile_", "")

                # Skip if table is active and we want to keep active files
                if keep_active and table_name in self.active_tables:
                    continue

                # Remove the file
                profile_file.unlink()
                logger.info(f"Cleaned up profile file: {profile_file}")
                cleaned += 1

            except Exception as e:
                logger.error(f"Failed to cleanup profile file {profile_file}: {e}")

        return cleaned

    def cleanup_stale_channels(self, streaming_clients: Dict[str, Any]) -> int:
        """Clean up stale streaming client channels.

        Args:
            streaming_clients: Dictionary of table_name -> streaming client

        Returns:
            Number of channels cleaned up
        """
        cleaned = 0
        current_time = time.time()
        stale_tables = []

        for table_name, last_used in self.channel_last_used.items():
            if current_time - last_used > self.channel_ttl_seconds:
                stale_tables.append(table_name)

        for table_name in stale_tables:
            if table_name in streaming_clients:
                try:
                    client = streaming_clients[table_name]
                    # Close the client if it has a close method
                    if hasattr(client, "close"):
                        client.close()
                    del streaming_clients[table_name]
                    self.channel_last_used.pop(table_name, None)
                    logger.info(
                        f"Cleaned up stale streaming client for table: {table_name}"
                    )
                    cleaned += 1
                except Exception as e:
                    logger.error(
                        f"Failed to cleanup streaming client for {table_name}: {e}"
                    )

        return cleaned

    def cleanup_all(self, streaming_clients: Dict[str, Any]) -> Dict[str, int]:
        """Perform full cleanup of all resources.

        Args:
            streaming_clients: Dictionary of table_name -> streaming client

        Returns:
            Dictionary with cleanup counts
        """
        return {
            "profile_files": self.cleanup_profile_files(keep_active=False),
            "stale_channels": self.cleanup_stale_channels(streaming_clients),
        }

    def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get current cleanup statistics.

        Returns:
            Dictionary with statistics
        """
        current_time = time.time()
        stale_count = sum(
            1
            for last_used in self.channel_last_used.values()
            if current_time - last_used > self.channel_ttl_seconds
        )

        return {
            "active_tables": len(self.active_tables),
            "tracked_channels": len(self.channel_last_used),
            "stale_channels": stale_count,
            "profile_dir": str(self.profile_dir),
        }
