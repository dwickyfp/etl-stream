import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import json

# Add module to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl_snowflake.client import SnowflakeClient
from etl_snowflake.config import SnowflakeConfig

class TestSnowflakeFlow(unittest.TestCase):
    def setUp(self):
        self.config = SnowflakeConfig(
            account="test_account",
            user="test_user",
            database="TEST_DB",
            schema="TEST_SCHEMA",
            warehouse="TEST_WH",
            private_key_path="test_key.p8",
            landing_schema="TEST_LANDING"
        )
        
        # Mock file operations for private key
        patcher = patch('builtins.open', unittest.mock.mock_open(read_data=b"test_key"))
        self.addCleanup(patcher.stop)
        patcher.start()
        
        # Mock Path.exists
        patcher_path = patch('pathlib.Path.exists', return_value=True)
        self.addCleanup(patcher_path.stop)
        patcher_path.start()

    @patch('etl_snowflake.ddl.SnowflakeDDL.connect')
    @patch('etl_snowflake.ddl.SnowflakeDDL.execute')
    def test_create_landing_table_sql(self, mock_execute, mock_connect):
        client = SnowflakeClient(self.config)
        columns = [{"name": "col1", "type_name": "VARCHAR"}]
        
        client.ddl.create_landing_table("test_table", columns)
        
        # Get the executed SQL
        call_args = mock_execute.call_args_list[0]
        sql = call_args[0][0]
        
        print("\n=== Landing Table SQL ===")
        print(sql)
        
        # Verify schema evolution enabled
        self.assertIn("ENABLE_SCHEMA_EVOLUTION = TRUE", sql)
        
        # Verify new column names
        self.assertIn('"OPERATION" VARCHAR(6)', sql)
        self.assertIn('"SEQUENCE" VARCHAR(64)', sql)
        self.assertIn('"TIMESTAMP" TIMESTAMP_NTZ', sql)
        
        # Verify correct schema usage
        self.assertIn('"TEST_DB"."TEST_LANDING"."LANDING_TEST_TABLE"', sql)

    @patch('etl_snowflake.ddl.SnowflakeDDL.connect')
    @patch('etl_snowflake.ddl.SnowflakeDDL.execute')
    def test_merge_task_sql(self, mock_execute, mock_connect):
        client = SnowflakeClient(self.config)
        
        # Mock task existence check to return False so it tries to create
        with patch.object(client.task_manager, 'task_exists', return_value=False):
             client.task_manager.create_merge_task("test_table", ["col1"], ["id"])
        
        # Get execution calls
        # We look for the CREATE TASK call
        create_task_call = None
        for call in mock_execute.call_args_list:
            if "CREATE OR REPLACE TASK" in call[0][0]:
                create_task_call = call
                break
        
        self.assertIsNotNone(create_task_call)
        sql = create_task_call[0][0]
        
        print("\n=== Merge Task SQL ===")
        print(sql)
        
        # Verify new column usage in MERGE logic
        self.assertIn('MAX("SEQUENCE")', sql)
        self.assertIn('ORDER BY "SEQUENCE" DESC', sql)
        self.assertIn('source."OPERATION" = \'DELETE\'', sql)

    @patch('etl_snowflake.client.SnowflakeClient._ensure_streaming_client')
    def test_insert_rows_columns(self, mock_get_client):
        # Mock the streaming client
        mock_streaming_client = MagicMock()
        mock_get_client.return_value = mock_streaming_client
        
        # Mock open_channel to return a channel with our mock client
        client = SnowflakeClient(self.config)
        
        # Force table initialization set to skip DB calls
        client._initialized_tables.add("test_table")
        
        rows = [{"id": 1, "data": "test"}]
        client.insert_rows("test_table", rows, operation="INSERT")
        
        # Verify append_rows call
        # It's called on the channel's client
        # channel = client.open_channel returns object that has _client which is mock_streaming_client.open_channel()[0]
        # But wait, open_channel calls streaming_client.open_channel
        
        # Let's mock open_channel directly for easier verification of logic BEFORE the SDK
        with patch.object(client, 'open_channel') as mock_open_channel:
            mock_channel = MagicMock()
            mock_channel._client = MagicMock()
            mock_open_channel.return_value = mock_channel
            
            client.insert_rows("test_table", rows, operation="INSERT")
            
            # Check what was passed to append_rows
            args, _ = mock_channel._client.append_rows.call_args
            inserted_rows = args[0]
            
            print("\n=== Inserted Row Sample ===")
            print(inserted_rows[0])
            
            self.assertIn("OPERATION", inserted_rows[0])
            self.assertIn("SEQUENCE", inserted_rows[0])
            self.assertEqual(inserted_rows[0]["OPERATION"], "INSERT")

if __name__ == '__main__':
    unittest.main()
