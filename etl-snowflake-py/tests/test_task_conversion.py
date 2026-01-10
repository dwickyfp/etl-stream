"""
Unit tests for MERGE task type conversion (ARRAY/VARIANT columns).

Tests that the task manager correctly generates SQL expressions to convert
string representations from landing tables to proper Snowflake types.
"""

import pytest
from unittest.mock import MagicMock, patch

from etl_snowflake.task import SnowflakeTaskManager
from etl_snowflake.ddl import get_column_types_map


class TestColumnExpression:
    """Test _get_column_expression helper method."""

    @pytest.fixture
    def task_manager(self):
        """Create a task manager with mocked config and DDL."""
        # Use MagicMock to avoid SnowflakeConfig validation (private key check)
        mock_config = MagicMock()
        mock_config.database = "TEST_DB"
        mock_config.schema = "TEST_SCHEMA"
        mock_config.warehouse = "TEST_WH"
        mock_config.landing_schema = "TEST_LANDING"
        mock_config.task_schedule_minutes = 1

        mock_ddl = MagicMock()
        return SnowflakeTaskManager(mock_config, mock_ddl)

    def test_array_column_expression(self, task_manager):
        """Test ARRAY column generates TRY_PARSE_JSON cast."""
        result = task_manager._get_column_expression("tags", "ARRAY")
        assert result == 'TRY_PARSE_JSON(source."tags")::ARRAY'

    def test_variant_column_expression(self, task_manager):
        """Test VARIANT column generates TRY_PARSE_JSON cast."""
        result = task_manager._get_column_expression("metadata", "VARIANT")
        assert result == 'TRY_PARSE_JSON(source."metadata")::VARIANT'

    def test_varchar_column_expression(self, task_manager):
        """Test VARCHAR column returns plain column reference."""
        result = task_manager._get_column_expression("name", "VARCHAR")
        assert result == 'source."name"'

    def test_number_column_expression(self, task_manager):
        """Test NUMBER column returns plain column reference."""
        result = task_manager._get_column_expression("amount", "NUMBER(12,2)")
        assert result == 'source."amount"'

    def test_integer_column_expression(self, task_manager):
        """Test INTEGER column returns plain column reference."""
        result = task_manager._get_column_expression("id", "INTEGER")
        assert result == 'source."id"'

    def test_custom_prefix(self, task_manager):
        """Test custom table alias prefix."""
        result = task_manager._get_column_expression("tags", "ARRAY", prefix="landing")
        assert result == 'TRY_PARSE_JSON(landing."tags")::ARRAY'

    def test_case_insensitive_type(self, task_manager):
        """Test type matching is case-insensitive."""
        result = task_manager._get_column_expression("data", "variant")
        assert result == 'TRY_PARSE_JSON(source."data")::VARIANT'


class TestColumnTypesMap:
    """Test get_column_types_map helper function."""

    def test_basic_column_types(self):
        """Test extraction of Snowflake types from column definitions."""
        columns = [
            {"name": "id", "type_oid": 23, "type_name": "int4", "modifier": -1},
            {"name": "name", "type_oid": 25, "type_name": "text", "modifier": -1},
        ]
        result = get_column_types_map(columns)
        assert result["id"] == "INTEGER"
        assert result["name"] == "VARCHAR"

    def test_array_type(self):
        """Test TEXT[] is mapped to ARRAY."""
        columns = [
            {"name": "tags", "type_oid": 1009, "type_name": "text[]", "modifier": -1},
        ]
        result = get_column_types_map(columns)
        assert result["tags"] == "ARRAY"

    def test_jsonb_type(self):
        """Test JSONB is mapped to VARIANT."""
        columns = [
            {"name": "metadata", "type_oid": 3802, "type_name": "jsonb", "modifier": -1},
        ]
        result = get_column_types_map(columns)
        assert result["metadata"] == "VARIANT"

    def test_mixed_columns(self):
        """Test mixed column types including ARRAY and VARIANT."""
        columns = [
            {"name": "sale_id", "type_oid": 20, "type_name": "int8", "modifier": -1},
            {"name": "tags", "type_oid": 1009, "type_name": "text[]", "modifier": -1},
            {"name": "metadata", "type_oid": 3802, "type_name": "jsonb", "modifier": -1},
            {"name": "amount", "type_oid": 1700, "type_name": "numeric", "modifier": -1},
        ]
        result = get_column_types_map(columns)
        assert result["sale_id"] == "BIGINT"
        assert result["tags"] == "ARRAY"
        assert result["metadata"] == "VARIANT"
        assert "NUMBER" in result["amount"]


class TestMergeTaskSQL:
    """Test that MERGE task SQL contains proper type conversions."""

    @pytest.fixture
    def mock_config(self):
        """Create mock Snowflake config."""
        # Use MagicMock to avoid SnowflakeConfig validation (private key check)
        mock_config = MagicMock()
        mock_config.database = "TEST_DB"
        mock_config.schema = "TEST_SCHEMA"
        mock_config.warehouse = "TEST_WH"
        mock_config.landing_schema = "TEST_LANDING"
        mock_config.task_schedule_minutes = 1
        return mock_config

    def test_merge_task_with_array_column(self, mock_config):
        """Test that MERGE task contains ARRAY conversion."""
        with patch("etl_snowflake.task.SnowflakeDDL") as MockDDL:
            mock_ddl = MagicMock()
            MockDDL.return_value = mock_ddl

            manager = SnowflakeTaskManager(mock_config, mock_ddl)

            column_names = ["id", "name", "tags"]
            column_types = {"id": "BIGINT", "name": "VARCHAR", "tags": "ARRAY"}

            manager.create_merge_task(
                "test_table", column_names, ["id"], column_types
            )

            # Get the SQL that was passed to execute()
            call_args = mock_ddl.execute.call_args[0][0]
            sql = call_args

            # Verify ARRAY conversion in SQL
            assert 'TRY_PARSE_JSON(source."tags")::ARRAY' in sql

    def test_merge_task_with_variant_column(self, mock_config):
        """Test that MERGE task contains VARIANT conversion."""
        with patch("etl_snowflake.task.SnowflakeDDL") as MockDDL:
            mock_ddl = MagicMock()
            MockDDL.return_value = mock_ddl

            manager = SnowflakeTaskManager(mock_config, mock_ddl)

            column_names = ["id", "metadata"]
            column_types = {"id": "BIGINT", "metadata": "VARIANT"}

            manager.create_merge_task(
                "test_table", column_names, ["id"], column_types
            )

            sql = mock_ddl.execute.call_args[0][0]

            # Verify VARIANT conversion in SQL
            assert 'TRY_PARSE_JSON(source."metadata")::VARIANT' in sql

    def test_merge_task_no_conversion_for_regular_columns(self, mock_config):
        """Test that regular columns have no type conversion."""
        with patch("etl_snowflake.task.SnowflakeDDL") as MockDDL:
            mock_ddl = MagicMock()
            MockDDL.return_value = mock_ddl

            manager = SnowflakeTaskManager(mock_config, mock_ddl)

            column_names = ["id", "name", "amount"]
            column_types = {"id": "BIGINT", "name": "VARCHAR", "amount": "NUMBER(12,2)"}

            manager.create_merge_task(
                "test_table", column_names, ["id"], column_types
            )

            sql = mock_ddl.execute.call_args[0][0]

            # Verify plain column references for regular types
            assert 'source."name"' in sql
            assert 'source."amount"' in sql
            # No TRY_PARSE_JSON for these columns
            assert 'TRY_PARSE_JSON(source."name")' not in sql
            assert 'TRY_PARSE_JSON(source."amount")' not in sql

    def test_merge_task_backward_compatible_without_column_types(self, mock_config):
        """Test that merge task works without column_types (backward compatible)."""
        with patch("etl_snowflake.task.SnowflakeDDL") as MockDDL:
            mock_ddl = MagicMock()
            MockDDL.return_value = mock_ddl

            manager = SnowflakeTaskManager(mock_config, mock_ddl)

            column_names = ["id", "name"]
            # No column_types passed - should default to plain column references

            manager.create_merge_task("test_table", column_names, ["id"])

            sql = mock_ddl.execute.call_args[0][0]

            # Verify SQL was generated successfully
            assert "MERGE INTO" in sql
            assert 'source."id"' in sql
            assert 'source."name"' in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
