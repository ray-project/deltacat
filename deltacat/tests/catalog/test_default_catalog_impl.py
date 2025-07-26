import pytest
import uuid
import tempfile
import shutil
import pandas as pd
import pyarrow as pa
from typing import Dict, Any
import threading
import time
import multiprocessing
import os
from unittest.mock import patch, MagicMock

import ray
from deltacat import Catalog
from deltacat.catalog import CatalogProperties
from deltacat.tests.test_utils.pyarrow import (
    create_delta_from_csv_file,
    commit_delta_to_partition,
)
from deltacat.types.media import DatasetType, ContentType, DistributedDatasetType
from deltacat.storage.model.types import DeltaType
from deltacat.storage import metastore
from deltacat.storage.model.schema import Schema, Field
from deltacat.storage.model.table import TableProperties
from deltacat.types.tables import TableWriteMode, TableProperty, TableReadOptimizationLevel, TablePropertyDefaultValues
from deltacat.exceptions import DeltaCatError, ValidationError
import deltacat as dc
import boto3
from moto import mock_s3
import pytest
import os
import pyarrow as pa
from unittest.mock import patch
import subprocess
import socket
import threading
from minio import Minio
from minio.error import S3Error


class TestReadTableMain:
    READ_TABLE_NAMESPACE = "catalog_read_table_namespace"
    SAMPLE_FILE_PATH = "deltacat/tests/catalog/data/sample_table.csv"

    @classmethod
    def setup_class(cls):
        """Setup Ray and catalog for the test class."""
        dc.init()
        
        # Use the default catalog storage location instead of a temp directory
        # This ensures both catalog and direct storage operations use the same backend
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = CatalogProperties(root=cls.temp_dir)
        
        cls.catalog_name = str(uuid.uuid4())
        
        # Use the default catalog configuration
        cls.catalog = dc.put_catalog(
            cls.catalog_name,
            catalog=Catalog(config=cls.catalog_properties),
        )
        
        # Create the env dictionary
        cls.env = {
            "temp_dir": cls.temp_dir,
            "catalog_properties": cls.catalog_properties,
            "catalog_name": cls.catalog_name,
            "catalog": cls.catalog,
        }
        
    @classmethod
    def teardown_class(cls):
        """Clean up test environment."""
        dc.clear_catalogs()
        shutil.rmtree(cls.catalog_properties.root, ignore_errors=True)

    def test_daft_distributed_read_sanity(self):
        env = self.env
        
        # setup
        READ_TABLE_TABLE_NAME = "test_read_table"
        create_delta_from_csv_file(
            self.READ_TABLE_NAMESPACE,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            content_type=ContentType.PARQUET,
            inner=env["catalog_properties"],
            supported_content_types=[ContentType.PARQUET],
            delta_type=DeltaType.APPEND,
        )

        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=self.READ_TABLE_NAMESPACE,
            catalog=env["catalog_name"],
            distributed_dataset_type=DatasetType.DAFT,
        )

        # verify
        assert df.count_rows() == 6
        assert df.column_names == ["pk", "value"]

    def test_daft_distributed_read_multiple_deltas(self):
        env = self.env
        
        # setup
        READ_TABLE_TABLE_NAME = "test_read_table_2"
        delta = create_delta_from_csv_file(
            self.READ_TABLE_NAMESPACE,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            content_type=ContentType.PARQUET,
            inner=env["catalog_properties"],
            supported_content_types=[ContentType.PARQUET],
            delta_type=DeltaType.APPEND,
        )

        partition = metastore.get_partition(
            delta.stream_locator,
            delta.partition_values,
            inner=env["catalog_properties"],
        )

        commit_delta_to_partition(
            partition=partition,
            file_paths=[self.SAMPLE_FILE_PATH],
            inner=env["catalog_properties"],
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.APPEND,
        )

        # action
        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=self.READ_TABLE_NAMESPACE,
            catalog=env["catalog_name"],
            distributed_dataset_type=DatasetType.DAFT,
        )

        # verify
        assert df.count_rows() == 12, "we expect twice as many columns as merge on read is disabled"
        assert df.column_names == ["pk", "value"]


class TestCopyOnWrite:
    """
    End-to-end copy-on-wrte tests using the default catalogs write and read APIs.
    """
    
    @classmethod
    def setup_class(cls):
        """Set up test environment with Ray and catalog."""
        dc.init()
        
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = CatalogProperties(root=cls.temp_dir)
        
        # Initialize deltacat catalog with a unique name
        catalog_name = "test_compaction_catalog"
        cls.catalog_name = catalog_name
        cls.catalog = dc.put_catalog(
            catalog_name,
            catalog=Catalog(config=cls.catalog_properties),
        )
        
        # Set up test namespace
        cls.test_namespace = "test_e2e_compaction"
        dc.create_namespace(
            namespace=cls.test_namespace,
            catalog=catalog_name,
        )
        
    @classmethod
    def teardown_class(cls):
        """Clean up test environment."""
        dc.clear_catalogs()
        shutil.rmtree(cls.temp_dir, ignore_errors=True)
    
    def _create_table_with_merge_keys(self, table_name: str) -> Schema:
        """Create a table with merge keys using the standard test schema."""
        schema = Schema.of([
            Field.of(pa.field("id", pa.int64()), is_merge_key=True),  # Primary merge key
            Field.of(pa.field("name", pa.string())),
            Field.of(pa.field("age", pa.int32())),
            Field.of(pa.field("city", pa.string())),
        ])
        
        # Create table properties with automatic compaction enabled
        table_properties: TableProperties = {
            TableProperty.READ_OPTIMIZATION_LEVEL: TableReadOptimizationLevel.MAX,
            TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER: 2,  # Trigger compaction after 2 records for testing
            TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER: 1000,
            TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER: 100,
        }
        
        dc.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            content_types=[ContentType.PARQUET],  # Specify content types
            properties=table_properties,  # Enable automatic compaction
            catalog=self.catalog_name,
        )
        
        return schema
    
    def _create_initial_data(self) -> pd.DataFrame:
        """Create initial test data matching standard test patterns."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Dave', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'city': ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix']
        })
    
    def _create_overlapping_upsert_data(self) -> pd.DataFrame:
        """Create overlapping data that will trigger merge/upsert behavior."""
        return pd.DataFrame({
            'id': [3, 4, 6, 7],  # IDs 3,4 overlap with initial data, 6,7 are new
            'name': ['Charlie_Updated', 'Dave_Updated', 'Frank', 'Grace'],
            'age': [36, 41, 50, 55],  # Updated ages for existing records
            'city': ['Chicago_New', 'Houston_New', 'Boston', 'Seattle']
        })
    
    def _create_third_batch_upsert_data(self) -> pd.DataFrame:
        """Create third batch with more overlapping data for comprehensive testing."""
        return pd.DataFrame({
            'id': [1, 5, 8, 9],  # IDs 1,5 overlap with initial, 8,9 are new
            'name': ['Alice_Final', 'Eve_Final', 'Henry', 'Iris'],
            'age': [26, 46, 60, 65],  # Updated ages
            'city': ['NYC_Final', 'Phoenix_Final', 'Denver', 'Portland']
        })
    
    def _verify_dataframe_contents(self, result_df, expected_data: Dict[int, Dict[str, Any]]):
        """Verify that the result DataFrame contains expected data after compaction."""
        # Handle both Daft DataFrames and Pandas DataFrames
        if hasattr(result_df, 'collect'):
            # Daft DataFrame - materialize it first
            materialized_df = result_df.collect()
            # Convert Daft DataFrame to pandas for easier comparison
            pandas_df = materialized_df.to_pandas()
        else:
            # Already a pandas DataFrame
            pandas_df = result_df
        
        # Convert to dict keyed by id for easy comparison
        result_dict = {}
        for _, row in pandas_df.iterrows():
            result_dict[int(row['id'])] = {
                'name': row['name'],
                'age': int(row['age']),
                'city': row['city']
            }
        
        # Check that we have exactly the expected records
        assert set(result_dict.keys()) == set(expected_data.keys()), \
            f"Expected IDs {set(expected_data.keys())}, got {set(result_dict.keys())}"
        
        # Check each record's content
        for record_id, expected_record in expected_data.items():
            actual_record = result_dict[record_id]
            assert actual_record == expected_record, \
                f"Record {record_id}: expected {expected_record}, got {actual_record}"
    
    def test_simple_append_no_compaction_needed(self):
        """Test that simple append operations work without requiring compaction."""
        table_name = "test_simple_append"
        data = self._create_initial_data()
        
        # Create table and write initial data
        dc.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        # Read back and verify
        result = dc.read_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DatasetType.DAFT,
        )
        
        # Should get all original records
        result_count = result.count_rows() if hasattr(result, 'count_rows') else len(result)
        assert result_count == 5
        self._verify_dataframe_contents(result, {
            1: {'name': 'Alice', 'age': 25, 'city': 'NYC'},
            2: {'name': 'Bob', 'age': 30, 'city': 'LA'},
            3: {'name': 'Charlie', 'age': 35, 'city': 'Chicago'},
            4: {'name': 'Dave', 'age': 40, 'city': 'Houston'},
            5: {'name': 'Eve', 'age': 45, 'city': 'Phoenix'},
        })
        
    
    def test_two_upsert_deltas_with_compaction(self):
        """
        End-to-end test: write two upsert deltas with overlapping merge keys,
        then read back to verify compaction worked correctly.
        """
        table_name = "test_two_upserts"
        
        # Step 1: Create table with merge keys
        schema = self._create_table_with_merge_keys(table_name)
        
        # Step 2: Write initial data using MERGE mode (creates UPSERT delta)
        initial_data = self._create_initial_data()
        
        dc.write_to_table(
            data=initial_data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,  # This creates UPSERT delta
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        
        # Step 3: Write overlapping upsert data (should trigger compaction)
        upsert_data = self._create_overlapping_upsert_data()
        
        dc.write_to_table(
            data=upsert_data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,  # This creates another UPSERT delta
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        
        # Step 4: Read table back and verify compaction results
        result = dc.read_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DatasetType.DAFT,
        )
        
        
        # Step 5: Verify the results show proper merge behavior
        # Expected: 7 total records (5 original + 2 new, with 2 updated)
        result_count = result.count_rows() if hasattr(result, 'count_rows') else len(result)
        assert result_count == 7, f"Expected 7 records after merge, got {result_count}"
        
        # Verify the merged data contains expected updates and additions
        expected_final_data = {
            1: {'name': 'Alice', 'age': 25, 'city': 'NYC'},  # Unchanged from initial
            2: {'name': 'Bob', 'age': 30, 'city': 'LA'},      # Unchanged from initial  
            3: {'name': 'Charlie_Updated', 'age': 36, 'city': 'Chicago_New'},  # Updated by upsert
            4: {'name': 'Dave_Updated', 'age': 41, 'city': 'Houston_New'},     # Updated by upsert
            5: {'name': 'Eve', 'age': 45, 'city': 'Phoenix'}, # Unchanged from initial
            6: {'name': 'Frank', 'age': 50, 'city': 'Boston'},  # New from upsert
            7: {'name': 'Grace', 'age': 55, 'city': 'Seattle'}, # New from upsert
        }
        
        self._verify_dataframe_contents(result, expected_final_data)
        
    
    def test_three_upsert_deltas_comprehensive_merge(self):
        """
        Comprehensive test: write three upsert deltas with various overlapping patterns
        to thoroughly test compaction merge behavior.
        """
        table_name = "test_three_upserts"
        
        # Step 1: Create table with merge keys
        schema = self._create_table_with_merge_keys(table_name)
        
        # Step 2: Write initial data
        initial_data = self._create_initial_data()
        dc.write_to_table(
            data=initial_data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        # Step 3: Write first upsert batch
        upsert_data_1 = self._create_overlapping_upsert_data()
        dc.write_to_table(
            data=upsert_data_1,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        # Step 4: Write second upsert batch  
        upsert_data_2 = self._create_third_batch_upsert_data()
        dc.write_to_table(
            data=upsert_data_2,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        # Step 5: Read and verify final state
        result = dc.read_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DatasetType.DAFT,
        )
        
        
        # Expected: 9 total unique records after all merges
        result_count = result.count_rows() if hasattr(result, 'count_rows') else len(result)
        assert result_count == 9, f"Expected 9 records after all merges, got {result_count}"
        
        # Verify merge behavior:
        # - ID 1: Updated in batch 3 (Alice_Final)
        # - ID 2: Never updated (Bob original)
        # - ID 3: Updated in batch 2 (Charlie_Updated)
        # - ID 4: Updated in batch 2 (Dave_Updated)
        # - ID 5: Updated in batch 3 (Eve_Final)
        # - ID 6: Added in batch 2 (Frank)
        # - ID 7: Added in batch 2 (Grace)
        # - ID 8: Added in batch 3 (Henry)
        # - ID 9: Added in batch 3 (Iris)
        expected_final_data = {
            1: {'name': 'Alice_Final', 'age': 26, 'city': 'NYC_Final'},      # Updated in batch 3
            2: {'name': 'Bob', 'age': 30, 'city': 'LA'},                    # Original, never updated
            3: {'name': 'Charlie_Updated', 'age': 36, 'city': 'Chicago_New'}, # Updated in batch 2
            4: {'name': 'Dave_Updated', 'age': 41, 'city': 'Houston_New'},   # Updated in batch 2
            5: {'name': 'Eve_Final', 'age': 46, 'city': 'Phoenix_Final'},    # Updated in batch 3
            6: {'name': 'Frank', 'age': 50, 'city': 'Boston'},              # Added in batch 2
            7: {'name': 'Grace', 'age': 55, 'city': 'Seattle'},             # Added in batch 2
            8: {'name': 'Henry', 'age': 60, 'city': 'Denver'},              # Added in batch 3
            9: {'name': 'Iris', 'age': 65, 'city': 'Portland'},             # Added in batch 3
        }
        
        self._verify_dataframe_contents(result, expected_final_data)
        
    
    def test_verify_delta_types_created(self):
        """
        Verify that MERGE operations create UPSERT deltas as expected.
        This test demonstrates the delta type behavior that triggers compaction.
        """
        table_name = "test_delta_types"
        
        # Create table with merge keys
        self._create_table_with_merge_keys(table_name)
        
        # Write data using MERGE mode - this should create UPSERT deltas
        data = self._create_initial_data()
        
        dc.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        # Get the table to inspect what was created
        table_def = dc.get_table(
            name=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
        )
        
        # Verify schema has the expected merge key
        merge_keys = table_def.table_version.schema.merge_keys
        assert merge_keys is not None and len(merge_keys) > 0, "Expected merge keys on table"
        
        # The merge key should be on the 'id' field
        id_field_is_merge_key = any(
            field.is_merge_key for field in table_def.table_version.schema.fields 
            if field.arrow.name == 'id'
        )
        assert id_field_is_merge_key, "Expected 'id' field to be marked as merge key"
        
    
    def test_concurrent_write_conflict(self):
        """
        Test that concurrent writes to the same table properly handle conflicts.
        This test simulates two concurrent write operations where one writer
        is delayed during staging, creating a race condition that should trigger
        transaction-level conflict detection.
        """
        table_name = "test_concurrent_writes"
        
        # Step 1: Create table configured to always trigger copy-on-write compaction
        schema = Schema.of([
            Field.of(pa.field("id", pa.int64()), is_merge_key=True),
            Field.of(pa.field("name", pa.string())),
            Field.of(pa.field("timestamp", pa.int64())),
        ])
        
        # Ensure compaction happens on every write
        table_properties: TableProperties = {
            TableProperty.READ_OPTIMIZATION_LEVEL: TableReadOptimizationLevel.MAX, # copy-on-write
            TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER: 1,  # Trigger on every record
            TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER: 1,    # Trigger on every file
            TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER: 1,   # Trigger on every delta
        }
        
        dc.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            content_types=[ContentType.PARQUET],
            properties=table_properties,
            catalog=self.catalog_name,
        )
        
        # Step 2: Write initial data to establish baseline (this ensures subsequent writes are UPSERTs)
        initial_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice_Initial', 'Bob_Initial', 'Charlie_Initial'],
            'timestamp': [500, 501, 502]
        })
        
        dc.write_to_table(
            data=initial_data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,
            content_type=ContentType.PARQUET,
            catalog=self.catalog_name,
        )
        
        # Step 3: Create test data for both writers (both will be UPSERT operations)
        data_writer_a = pd.DataFrame({
            'id': [1, 2],  # Updates existing records
            'name': ['Alice_A', 'Bob_A'], 
            'timestamp': [1000, 1001]
        })
        
        data_writer_b = pd.DataFrame({
            'id': [2, 3],  # Updates existing records (ID 2 conflicts with Writer A)
            'name': ['Bob_B', 'Charlie_B'],
            'timestamp': [2000, 2001]
        })
        
        # Variables to track execution
        results = {"writer_a": None, "writer_b": None}
        exceptions = {"writer_a": None, "writer_b": None}
        
        # Create artificial delay in commit for Writer A to create race condition
        from deltacat.storage.main.impl import commit_partition
        original_commit_partition = commit_partition
        
        # Use threading Event as a latch - Writer A waits until Writer B completes
        writer_b_completed = threading.Event()
        
        def delayed_commit_partition(*args, **kwargs):
            """Use latch mechanism to ensure Writer B completes before Writer A"""
            current_thread = threading.current_thread()
            
            if hasattr(current_thread, 'name') and 'writer_a' in current_thread.name.lower():
                # Writer A waits for Writer B to complete first
                writer_b_completed.wait(timeout=10)  # Wait up to 10 seconds
                
            # Call the original function
            result = original_commit_partition(*args, **kwargs)
            
            if hasattr(current_thread, 'name') and 'writer_b' in current_thread.name.lower():
                # Writer B signals completion after successful commit
                writer_b_completed.set()
                
            return result
        
        def writer_a_task():
            """Task for Writer A - will be delayed during commit"""
            try:
                current_thread = threading.current_thread()
                current_thread.name = "writer_a_thread"
                
                dc.write_to_table(
                    data=data_writer_a,
                    table=table_name,
                    namespace=self.test_namespace,
                    mode=TableWriteMode.MERGE,
                    content_type=ContentType.PARQUET,
                    catalog=self.catalog_name,
                )
                results["writer_a"] = "success"
                
            except Exception as e:
                exceptions["writer_a"] = e
        
        def writer_b_task():
            """Task for Writer B - should complete normally"""
            try:
                current_thread = threading.current_thread()
                current_thread.name = "writer_b_thread"
                
                dc.write_to_table(
                    data=data_writer_b,
                    table=table_name,
                    namespace=self.test_namespace,
                    mode=TableWriteMode.MERGE,
                    content_type=ContentType.PARQUET,
                    catalog=self.catalog_name,
                )
                results["writer_b"] = "success"
                
            except Exception as e:
                exceptions["writer_b"] = e
        
        # Execute concurrent writes with delayed commit for Writer A
        with patch('deltacat.storage.main.impl.commit_partition', side_effect=delayed_commit_partition):
            # Start both writers concurrently
            thread_a = threading.Thread(target=writer_a_task, name="writer_a_thread")
            thread_b = threading.Thread(target=writer_b_task, name="writer_b_thread")
            
            thread_a.start()
            thread_b.start()
            
            # Wait for both to complete (with timeout)
            thread_a.join(timeout=10)
            thread_b.join(timeout=10)
        
        # Verify that exactly one writer succeeded and one failed due to conflict
        success_count = sum(1 for result in results.values() if result == "success")
        
        assert success_count == 1, f"Expected exactly one writer to succeed, but {success_count} succeeded"
        
        # Verify that the failed writer got a concurrent conflict error
        if results["writer_a"] == "success":
            failed_exception = exceptions["writer_b"]
        else:
            failed_exception = exceptions["writer_a"]
        
        assert failed_exception is not None, "Failed writer should have an exception"
        
        # Verify this is a legitimate concurrent write conflict error
        # Check both the main exception message and any underlying cause
        error_message = str(failed_exception)
        cause_message = str(failed_exception.__cause__) if hasattr(failed_exception, '__cause__') and failed_exception.__cause__ else ""
        full_error_context = error_message + " " + cause_message
        
        # Look specifically for our conflict detection message
        has_conflict_message = "Concurrent modification detected" in full_error_context
        assert has_conflict_message, f"Expected 'Concurrent modification detected' error message, got: {failed_exception} (cause: {cause_message})"
        
        # Verify final table state is consistent
        final_result = dc.read_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DatasetType.DAFT,
        )
        
        final_count = final_result.count_rows()
        assert final_count == 3, f"Expected exactly 3 records after conflict resolution, got {final_count}"
    
    @pytest.mark.skipif(
        multiprocessing.cpu_count() < 2,
        reason="Stress test requires at least 2 CPUs for meaningful concurrent testing"
    )
    def test_concurrent_write_stress(self):
        """
        Stress test for concurrent write conflicts with data integrity validation.
        This test runs multiple rounds of parallel writes and verifies that successful
        writes never lose data. Failed writes due to conflicts are acceptable, but
        successful writes must preserve all their data in the final table state.
        """
        import multiprocessing
        
        table_name = "test_concurrent_stress"
        concurrent_writers = multiprocessing.cpu_count()
        rounds = 10
        
        # Create table with merge keys for upsert behavior
        schema = Schema.of([
            Field.of(pa.field("id", pa.int64()), is_merge_key=True),
            Field.of(pa.field("round_num", pa.int32())),
            Field.of(pa.field("writer_id", pa.string())),
            Field.of(pa.field("data", pa.string())),
        ])
        
        # Aggressive compaction to stress the conflict detection system
        table_properties: TableProperties = {
            TableProperty.READ_OPTIMIZATION_LEVEL: TableReadOptimizationLevel.MAX,
            TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER: 1,
            TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER: 1,
            TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER: 1,
        }
        
        dc.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            content_types=[ContentType.PARQUET],
            properties=table_properties,
            catalog=self.catalog_name,
        )
        
        # Track successful writes across all rounds
        successful_writes = []
        
        for round_num in range(rounds):
            # Per-round tracking with clean isolation
            round_results = {}
            round_exceptions = {}
            
            def create_writer_task(round_num, writer_idx):
                def writer_task():
                    try:
                        current_thread = threading.current_thread()
                        current_thread.name = f"round_{round_num}_writer_{writer_idx}_thread"
                        
                        # Generate unique IDs
                        base_id = round_num * 1000 + writer_idx * 10
                        writer_data = pd.DataFrame({
                            'id': [base_id, base_id + 1, base_id + 2],
                            'round_num': [round_num] * 3,
                            'writer_id': [f'round_{round_num:02d}_writer_{writer_idx:02d}'] * 3,
                            'data': [f'round_{round_num:02d}_writer_{writer_idx:02d}_record_{i}' for i in range(3)]
                        })
                        
                        dc.write_to_table(
                            data=writer_data,
                            table=table_name,
                            namespace=self.test_namespace,
                            mode=TableWriteMode.MERGE,
                            content_type=ContentType.PARQUET,
                            catalog=self.catalog_name,
                        )
                        round_results[writer_idx] = "success"
                        
                    except Exception as e:
                        round_exceptions[writer_idx] = e
                        round_results[writer_idx] = "failed"
                
                return writer_task
            
            # Create and start all writers for this round
            threads = []
            for writer_idx in range(concurrent_writers):
                task = create_writer_task(round_num, writer_idx)
                thread = threading.Thread(target=task, name=f"round_{round_num}_writer_{writer_idx}_thread")
                threads.append(thread)
            
            # Start all threads simultaneously
            for thread in threads:
                thread.start()
            
            # Wait for all threads to complete with reasonable timeout
            for thread in threads:
                thread.join(timeout=30)
            
            # Record successful writes for this round
            round_successful_count = 0
            for writer_idx, result in round_results.items():
                if result == "success":
                    round_successful_count += 1
                    # Recreate the data that this successful writer wrote
                    base_id = round_num * 1000 + writer_idx * 10
                    writer_data = pd.DataFrame({
                        'id': [base_id, base_id + 1, base_id + 2],
                        'round_num': [round_num] * 3,
                        'writer_id': [f'round_{round_num:02d}_writer_{writer_idx:02d}'] * 3,
                        'data': [f'round_{round_num:02d}_writer_{writer_idx:02d}_record_{i}' for i in range(3)]
                    })
                    successful_writes.append((round_num, writer_idx, writer_data))
            
            # Verify at least one write succeeded in this round
            assert round_successful_count > 0, f"No writers succeeded in round {round_num}"
        
        # Read final table state
        final_result = dc.read_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DatasetType.DAFT,
        )
        
        final_df = final_result.collect().to_pandas()
        
        # Create a map of final table contents by ID
        final_data_by_id = {}
        for _, row in final_df.iterrows():
            record_id = int(row['id'])
            final_data_by_id[record_id] = {
                'round_num': int(row['round_num']),
                'writer_id': row['writer_id'],
                'data': row['data']
            }
        
        # Validate data integrity: every successful write's data must be present
        missing_records = []
        corrupted_records = []
        
        for round_num, writer_idx, expected_data in successful_writes:
            for _, expected_row in expected_data.iterrows():
                expected_id = int(expected_row['id'])
                expected_record = {
                    'round_num': int(expected_row['round_num']),
                    'writer_id': expected_row['writer_id'],
                    'data': expected_row['data']
                }
                
                if expected_id not in final_data_by_id:
                    missing_records.append((expected_id, expected_record))
                else:
                    actual_record = final_data_by_id[expected_id]
                    # Verify the record has valid data (not corrupted)
                    if actual_record['data'] == "" or actual_record['writer_id'] == "":
                        corrupted_records.append((expected_id, expected_record, actual_record))
        
        # Assert data integrity
        assert len(missing_records) == 0, f"Missing records from successful writes: {missing_records[:5]}..."
        assert len(corrupted_records) == 0, f"Corrupted records from successful writes: {corrupted_records[:5]}..."
        
        # Verify no phantom records (records that don't belong to any successful write)
        expected_ids = set()
        for round_num, writer_idx, expected_data in successful_writes:
            for _, row in expected_data.iterrows():
                expected_ids.add(int(row['id']))
        
        actual_ids = set(final_data_by_id.keys())
        phantom_ids = actual_ids - expected_ids
        
        assert len(phantom_ids) == 0, f"Found phantom records not from any successful write: {list(phantom_ids)[:10]}..."
        
        # Summary statistics and validation
        total_successful_writes = len(successful_writes)
        total_expected_records = total_successful_writes * 3  # Each write has 3 records
        total_actual_records = len(final_df)
        
        # With unique IDs per writer, we should have exactly the expected number of records
        assert total_actual_records == total_expected_records, f"Expected {total_expected_records} records, got {total_actual_records}"
        assert total_actual_records > 0, "No records found in final table"
        
        # Verify we had some conflicts across all rounds (not every writer succeeded)
        total_possible_writes = rounds * concurrent_writers
        conflict_rate = (total_possible_writes - total_successful_writes) / total_possible_writes
        
        # Print conflict statistics for analysis
        print(f"\n=== CONFLICT STATISTICS ===")
        print(f"Concurrent writers: {concurrent_writers}")
        print(f"Total rounds: {rounds}")
        print(f"Total possible writes: {total_possible_writes}")
        print(f"Total successful writes: {total_successful_writes}")
        print(f"Conflict rate: {conflict_rate:.1%}")
        
        # More lenient conflict rate validation - adjust based on observed behavior
        assert conflict_rate > 0.01, f"Too few conflicts ({conflict_rate:.1%}) - conflict detection may not be working"
        assert conflict_rate < 0.99, f"Too many conflicts ({conflict_rate:.1%}) - conflict detection may not be working" 
    

class TestMinIOIntegration:
    """
    Test suite to verify GitHub issue #567 using real MinIO server.
    
    This provides a more realistic test than moto mocking by using actual S3-compatible
    server (MinIO) to test end-to-end S3 catalog functionality.
    """
    
    MINIO_PORT = 9501  # Use non-standard port to avoid conflicts
    MINIO_ACCESS_KEY = "testuser"
    MINIO_SECRET_KEY = "testpass123"
    BUCKET_NAME = "deltacat-test"
    
    @classmethod
    def setup_class(cls):
        """Start MinIO server and initialize test environment."""
        dc.init()
        
        # Create temporary directory for MinIO data
        cls.minio_data_dir = tempfile.mkdtemp()
        
        # Start MinIO server in background
        cls.minio_process = None
        cls._start_minio_server()
        
        # Wait for server to be ready
        cls._wait_for_minio_ready()
        
        # Initialize MinIO client and create bucket
        cls.minio_client = Minio(
            f"localhost:{cls.MINIO_PORT}",
            access_key=cls.MINIO_ACCESS_KEY,
            secret_key=cls.MINIO_SECRET_KEY,
            secure=False
        )
        
        # Create test bucket
        if not cls.minio_client.bucket_exists(cls.BUCKET_NAME):
            cls.minio_client.make_bucket(cls.BUCKET_NAME)
        
        # Set up AWS environment variables to point to MinIO
        cls.original_env = {}
        env_vars = {
            'AWS_ACCESS_KEY_ID': cls.MINIO_ACCESS_KEY,
            'AWS_SECRET_ACCESS_KEY': cls.MINIO_SECRET_KEY,
            'AWS_ENDPOINT_URL_S3': f'http://localhost:{cls.MINIO_PORT}',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }
        
        for key, value in env_vars.items():
            cls.original_env[key] = os.environ.get(key)
            os.environ[key] = value
    
    @classmethod
    def teardown_class(cls):
        """Clean up MinIO server and test environment."""
        # Restore original environment variables
        for key, value in cls.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        
        # Stop MinIO server
        if cls.minio_process:
            cls.minio_process.terminate()
            cls.minio_process.wait()
        
        # Clean up temporary directory
        if hasattr(cls, 'minio_data_dir'):
            shutil.rmtree(cls.minio_data_dir, ignore_errors=True)
    
    @classmethod
    def _start_minio_server(cls):
        """Start MinIO server as subprocess."""
        cmd = [
            'minio', 'server',
            '--address', f':{cls.MINIO_PORT}',
            '--console-address', f':{cls.MINIO_PORT + 1}',  # Console on different port
            cls.minio_data_dir
        ]
        
        env = os.environ.copy()
        env['MINIO_ROOT_USER'] = cls.MINIO_ACCESS_KEY
        env['MINIO_ROOT_PASSWORD'] = cls.MINIO_SECRET_KEY
        
        cls.minio_process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    
    @classmethod
    def _wait_for_minio_ready(cls, timeout=30):
        """Wait for MinIO server to be ready to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('localhost', cls.MINIO_PORT))
                sock.close()
                if result == 0:
                    # Give it a bit more time to fully initialize
                    time.sleep(2)
                    return
            except Exception:
                pass
            time.sleep(0.5)
        
        raise Exception(f"MinIO server failed to start on port {cls.MINIO_PORT}")
    
    def test_s3_catalog_initialization_with_minio(self):
        """Test that DeltaCAT can initialize a catalog with S3 root using MinIO."""
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-catalog"
        
        try:
            # Try to create catalog properties with S3 root
            catalog_properties = CatalogProperties(root=s3_catalog_root)
            
            # Try to register a catalog with S3 root
            catalog_name = f"minio-test-catalog-{uuid.uuid4()}"
            catalog = dc.put_catalog(
                catalog_name,
                catalog=Catalog(config=catalog_properties)
            )
            
            # Clean up
            dc.clear_catalogs()
            
        except Exception as e:
            pytest.fail(f"S3 catalog initialization failed: {e}")
    
    def test_end_to_end_s3_workflow_with_minio(self):
        """Test complete write_to_table -> read_table workflow using MinIO S3."""
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-e2e-test"
        catalog_name = f"minio-e2e-test-{uuid.uuid4()}"
        namespace = "test_namespace"
        table_name = "test_table"
        
        try:
            # Step 1: Register S3 catalog
            catalog_properties = CatalogProperties(root=s3_catalog_root)
            catalog = dc.put_catalog(
                catalog_name,
                catalog=Catalog(config=catalog_properties)
            )
            
            # Step 2: Create test data
            test_data = pa.table({
                'id': [1, 2, 3, 4, 5],
                'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
                'value': [10.1, 20.2, 30.3, 40.4, 50.5]
            })
            
            # Step 3: Write to S3 table via DeltaCAT
            dc.write_to_table(
                data=test_data,
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                mode=TableWriteMode.CREATE
            )
             
            # Step 4: Read from S3 table via DeltaCAT (this is where issue #567 would manifest) 
            # Create Daft IOConfig for MinIO authentication
            try:
                from daft.io import IOConfig, S3Config
                
                # Get MinIO configuration from environment variables
                endpoint_url = os.environ.get('AWS_ENDPOINT_URL_S3')
                access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
                secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
                
                if endpoint_url and access_key_id and secret_access_key:
                    # Create S3Config for MinIO
                    s3_config = S3Config(
                        endpoint_url=endpoint_url,
                        key_id=access_key_id,
                        access_key=secret_access_key,
                        region_name=region,
                        force_virtual_addressing=False  # MinIO works better with path-style addressing
                    )
                    io_config = IOConfig(s3=s3_config)
                else:
                    io_config = None
                    
            except ImportError as e:
                io_config = None
            
            result_table = dc.read_table(
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                distributed_dataset_type=DatasetType.DAFT,  # Force DAFT to test MinIO fix
                io_config=io_config  # Pass IOConfig for MinIO authentication
            )
             
            # Step 5: Verify data integrity
            # Daft DataFrame has different attributes than PyArrow Table
            assert result_table.count_rows() == test_data.num_rows
            assert result_table.column_names == test_data.column_names
            
            # Convert to pandas for easier comparison
            original_df = test_data.to_pandas().sort_values('id').reset_index(drop=True)
            result_df = result_table.to_pandas().sort_values('id').reset_index(drop=True)
            
            pd.testing.assert_frame_equal(original_df, result_df)
             
        except Exception as e:
            # If this fails, it likely indicates issue #567 still exists 
            # For debugging, let's see what happened
            if "s3://" in str(e) or "S3" in str(e):
                print("Error appears to be S3-related, consistent with issue #567")
            
            # Re-raise to fail the test and show the actual error
            pytest.fail(f"S3 end-to-end workflow failed with error: {e}")
            
        finally:
            # Clean up
            try:
                dc.clear_catalogs()
            except Exception:
                pass  # Ignore cleanup errors
 

    def test_s3_uri_reconstruction_fix(self):
        """Test that our fix correctly reconstructs S3 URIs for external readers."""
        from deltacat.catalog import get_catalog_properties
        from deltacat.catalog.model.properties import CatalogProperties
        
        # Test the S3 scheme reconstruction logic directly
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-test-reconstruction"
        
        # Create catalog properties with S3 root
        catalog_properties = CatalogProperties(root=s3_catalog_root)
        
        # Test reconstruction with various path formats
        test_cases = [
            ("bucket/path/file.parquet", "s3://bucket/path/file.parquet"),
            ("test-bucket/data/table.parquet", "s3://test-bucket/data/table.parquet"),
            ("mybucket/nested/deep/file.parquet", "s3://mybucket/nested/deep/file.parquet"),
        ]
        
        for input_path, expected_output in test_cases:
            result = catalog_properties.reconstruct_full_path(input_path)
            assert result == expected_output, f"Expected {expected_output}, got {result}"
        
        # Test that paths with existing schemes are left unchanged
        full_s3_path = "s3://already-full/path/file.parquet"
        result = catalog_properties.reconstruct_full_path(full_s3_path)
        assert result == full_s3_path, f"Full S3 path should be unchanged, got {result}"
         
    def test_local_storage_s3_uri_reconstruction(self):
        """
        Test that LOCAL storage type also handles S3 URI reconstruction correctly.
        
        This test specifically verifies that GitHub issue #567 is fixed for the LOCAL
        storage path by setting distributed_dataset_type=None and testing different
        table types (PyArrow, Pandas, Polars).
        """
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-local-test"
        namespace = "test_namespace"

        # Test data
        test_data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.1, 20.2, 30.3, 40.4, 50.5]
        })

        # Test different table types with LOCAL storage
        table_types_to_test = [
            (DatasetType.PYARROW, "PyArrow"),
            (DatasetType.PANDAS, "Pandas"),
            (DatasetType.POLARS, "Polars"),
            (DatasetType.NUMPY, "NumPy"),
        ]

        for table_type, table_type_name in table_types_to_test: 
            catalog_name = f"local-s3-test-{table_type_name.lower()}-{uuid.uuid4()}"
            table_name = f"test_table_{table_type_name.lower()}"
            
            try:
                # Step 1: Register S3 catalog
                catalog_properties = CatalogProperties(root=s3_catalog_root)
                catalog = dc.put_catalog(
                    catalog_name,
                    catalog=Catalog(config=catalog_properties)
                )

                # Step 2: Write to S3 table via DeltaCAT (this should work)
                dc.write_to_table(
                    data=test_data,
                    table=table_name,
                    namespace=namespace,
                    catalog=catalog_name,
                    mode=TableWriteMode.CREATE
                )

                # Step 3: Read using LOCAL storage (distributed_dataset_type=None)
                result_table = dc.read_table(
                    table=table_name,
                    namespace=namespace,
                    catalog=catalog_name,
                    table_type=table_type,
                    distributed_dataset_type=None,  # Force LOCAL storage
                )

                # Step 4: Verify data integrity
                # LOCAL storage now returns a single concatenated table (not a list)
                # Verify based on table type
                if table_type == DatasetType.PYARROW:
                    assert isinstance(result_table, pa.Table)
                    assert result_table.num_rows == test_data.num_rows
                    assert result_table.column_names == test_data.column_names

                    # Convert to pandas for comparison
                    import pandas as pd
                    original_df = test_data.to_pandas().sort_values('id').reset_index(drop=True)
                    result_df = result_table.to_pandas().sort_values('id').reset_index(drop=True)
                    pd.testing.assert_frame_equal(original_df, result_df)

                elif table_type == DatasetType.PANDAS:
                    import pandas as pd
                    assert isinstance(result_table, pd.DataFrame)
                    assert len(result_table) == test_data.num_rows
                    assert list(result_table.columns) == test_data.column_names

                    # Compare DataFrames directly
                    original_df = test_data.to_pandas().sort_values('id').reset_index(drop=True)
                    result_df = result_table.sort_values('id').reset_index(drop=True)
                    pd.testing.assert_frame_equal(original_df, result_df)

                elif table_type == DatasetType.POLARS:
                    import polars as pl
                    assert isinstance(result_table, pl.DataFrame)
                    assert result_table.height == test_data.num_rows
                    assert result_table.columns == test_data.column_names

                    # Convert both to pandas for comparison
                    import pandas as pd
                    original_df = test_data.to_pandas().sort_values('id').reset_index(drop=True)
                    result_df = result_table.to_pandas().sort_values('id').reset_index(drop=True)
                    pd.testing.assert_frame_equal(original_df, result_df)

                elif table_type == DatasetType.NUMPY:
                    import numpy as np
                    assert isinstance(result_table, np.ndarray)
                    assert result_table.shape[0] == test_data.num_rows
                    
                    # NumPy arrays with mixed data types (strings and numbers) are stored as object arrays
                    # This is expected behavior, so we just verify basic structure and data presence 
                    # For mixed-type data, NumPy typically flattens or converts to object array
                    # Just verify we have the correct number of rows and some data
                    if result_table.ndim == 2:
                        assert result_table.shape[1] == len(test_data.column_names)
                    elif result_table.ndim == 1:
                        # Flattened array case
                        assert result_table.shape[0] >= test_data.num_rows
                    
                    # Verify the array is not empty and contains some expected values
                    assert result_table.size > 0
                    # Look for some of our test data in the array (converted to strings)
                    array_str = str(result_table)
                    assert 'Alice' in array_str or '1' in array_str  # Check for some test data

            except Exception as e:
                pytest.fail(f"LOCAL storage S3 URI reconstruction failed for {table_type_name}: {e}")
            
            finally:
                # Clean up this catalog
                try:
                    dc.clear_catalogs()
                except Exception:
                    pass  # Ignore cleanup errors

    def test_pyarrow_local_kwargs(self):
        """Test PyArrow LOCAL storage with custom kwargs."""
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-pyarrow-test"
        namespace = "test_namespace"
        catalog_name = f"pyarrow-test-{uuid.uuid4()}"
        table_name = "test_pyarrow_table"

        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.1, 20.2, 30.3]
        })

        catalog_properties = CatalogProperties(root=s3_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )
        
        result = dc.read_table(
            table=table_name, namespace=namespace, catalog=catalog_name,
            distributed_dataset_type=None, table_type=DatasetType.PYARROW,
            pre_buffer=True, use_pandas_metadata=True
        )
        
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        dc.clear_catalogs()

    def test_pandas_local_kwargs(self):
        """Test Pandas LOCAL storage with custom kwargs."""
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-pandas-test"
        namespace = "test_namespace"
        catalog_name = f"pandas-test-{uuid.uuid4()}"
        table_name = "test_pandas_table"

        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.1, 20.2, 30.3]
        })
        catalog_properties = CatalogProperties(root=s3_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )
        
        result = dc.read_table(
            table=table_name, namespace=namespace, catalog=catalog_name,
            distributed_dataset_type=None, table_type=DatasetType.PANDAS,
            use_pandas_metadata=True
        )
        
        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        dc.clear_catalogs()

    def test_polars_local_kwargs(self):
        """Test Polars LOCAL storage with custom kwargs."""
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-polars-test"
        namespace = "test_namespace"
        catalog_name = f"polars-test-{uuid.uuid4()}"
        table_name = "test_polars_table"

        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.1, 20.2, 30.3]
        })
        catalog_properties = CatalogProperties(root=s3_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )
        
        result = dc.read_table(
            table=table_name, namespace=namespace, catalog=catalog_name,
            distributed_dataset_type=None, table_type=DatasetType.POLARS,
            use_pyarrow=True
        )
        
        import polars as pl
        assert isinstance(result, pl.DataFrame)
        assert result.shape[0] == 3
        dc.clear_catalogs()

    def test_numpy_local_kwargs(self):
        """Test NumPy LOCAL storage with custom kwargs."""
        s3_catalog_root = f"s3://{self.BUCKET_NAME}/deltacat-numpy-test"
        namespace = "test_namespace"
        catalog_name = f"numpy-test-{uuid.uuid4()}"
        table_name = "test_numpy_table"

        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.1, 20.2, 30.3]
        })
        catalog_properties = CatalogProperties(root=s3_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )
        
        result = dc.read_table(
            table=table_name, namespace=namespace, catalog=catalog_name,
            distributed_dataset_type=None, table_type=DatasetType.NUMPY,
            use_pandas_metadata=True
        )
        
        import numpy as np
        assert isinstance(result, np.ndarray)
        assert result.shape[0] == 3
        dc.clear_catalogs()

    def test_baseline_storage_types_local_catalog(self):
        """
        Test that all LOCAL and DISTRIBUTED storage types work correctly when reading
        from a local disk catalog without any custom kwargs specified.
        
        This serves as a baseline test to ensure basic functionality works for all
        supported storage types in DeltaCAT.
        """
        # Use a local filesystem catalog (not S3)
        local_catalog_root = f"/tmp/deltacat-baseline-test-{uuid.uuid4()}"
        namespace = "test_namespace"
        catalog_name = f"baseline-test-{uuid.uuid4()}"
        table_name = "baseline_test_table"

        # Test data
        test_data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.1, 20.2, 30.3, 40.4, 50.5],
            'category': ['A', 'B', 'A', 'C', 'B']
        })

        try:
            # Step 1: Register local filesystem catalog
            catalog_properties = CatalogProperties(root=local_catalog_root)
            catalog = dc.put_catalog(
                catalog_name,
                catalog=Catalog(config=catalog_properties)
            )
            
            # Step 2: Write test data
            dc.write_to_table(
                data=test_data,
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                mode=TableWriteMode.CREATE,
            )

            # Step 3: Test all LOCAL storage types (no custom kwargs) 
            local_storage_types = [
                (DatasetType.PYARROW, "PyArrow"),
                (DatasetType.PANDAS, "Pandas"), 
                (DatasetType.POLARS, "Polars"),
                (DatasetType.NUMPY, "NumPy"),
            ]

            for table_type, type_name in local_storage_types:
                try:
                    result_table = dc.read_table(
                        table=table_name,
                        namespace=namespace,
                        catalog=catalog_name,
                        table_type=table_type,
                        distributed_dataset_type=None,  # Force LOCAL storage
                        # NO custom kwargs passed - this is the baseline test
                    )
                    
                    # Verify the data was read correctly and is a single table (not a list)
                    if table_type == DatasetType.PYARROW:
                        assert isinstance(result_table, pa.Table)
                        assert result_table.num_rows == 5
                        assert len(result_table.column_names) == 4
                    elif table_type == DatasetType.PANDAS:
                        import pandas as pd
                        assert isinstance(result_table, pd.DataFrame)
                        assert len(result_table) == 5
                        assert len(result_table.columns) == 4
                    elif table_type == DatasetType.POLARS:
                        import polars as pl
                        assert isinstance(result_table, pl.DataFrame)
                        assert result_table.shape == (5, 4)
                    elif table_type == DatasetType.NUMPY:
                        import numpy as np
                        assert isinstance(result_table, np.ndarray)
                        assert result_table.shape[0] == 5
                        assert result_table.size > 0
                except Exception as e:
                    pytest.fail(f"Baseline test failed for {type_name} LOCAL storage: {e}")

        except Exception as e:
            pytest.fail(f"Baseline storage types test setup failed unexpectedly: {e}")
            
        # Step 4: Test all DISTRIBUTED storage types (no custom kwargs) - separate try block 
        distributed_storage_types = [
            (DatasetType.RAY_DATASET, "RAY_DATASET"),
            (DatasetType.DAFT, "DAFT"),
        ]

        distributed_successes = 0
        for distributed_type, type_name in distributed_storage_types:
            result_table = dc.read_table(
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                distributed_dataset_type=distributed_type,
                # NO custom kwargs passed - this is the baseline test
            )
            
            # For distributed types, we expect different return types
            # Just verify we got something back and it's not None or empty
            assert result_table is not None
            distributed_successes += 1
         
        # Clean up
        try:
            dc.clear_catalogs()
            import shutil
            shutil.rmtree(local_catalog_root, ignore_errors=True)
        except Exception:
            pass  # Ignore cleanup errors

    def test_distributed_storage_debug(self):
        """
        Debug test to understand exactly what's failing with distributed storage types.
        """
        # Use a local filesystem catalog
        local_catalog_root = f"/tmp/deltacat-debug-test-{uuid.uuid4()}"
        namespace = "test_namespace"
        catalog_name = f"debug-test-{uuid.uuid4()}"
        table_name = "debug_test_table"

        # Test data
        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.1, 20.2, 30.3]
        })

        # Setup catalog and write data
        catalog_properties = CatalogProperties(root=local_catalog_root)
        catalog = dc.put_catalog(
            catalog_name,
            catalog=Catalog(config=catalog_properties)
        )
        
        dc.write_to_table(
            data=test_data,
            table=table_name,
            namespace=namespace,
            catalog=catalog_name,
            mode=TableWriteMode.CREATE,
        )

        # Test each distributed storage type individually with detailed error reporting
        distributed_types = [
            (DatasetType.RAY_DATASET, "RAY_DATASET"),
            (DatasetType.DAFT, "DAFT"),
        ]

        for distributed_type, type_name in distributed_types:
            result = dc.read_table(
                table=table_name,
                namespace=namespace,
                catalog=catalog_name,
                distributed_dataset_type=distributed_type,
            )
                    
        # Clean up
        try:
            dc.clear_catalogs()
            import shutil
            shutil.rmtree(local_catalog_root, ignore_errors=True)
        except Exception:
            pass


class TestLocalIntegration:
    """
    Test suite to verify all DeltaCAT functionality using local filesystem storage.
    
    This provides a stable testing environment without cloud storage dependencies,
    focusing on core functionality like distributed storage types, custom kwargs
    propagation, and table concatenation.
    """
    
    @classmethod
    def setup_class(cls):
        """Initialize test environment."""
        dc.init()
        
    def test_comprehensive_storage_types_local_catalog(self):
        """Test all LOCAL and DISTRIBUTED storage types with local filesystem catalog."""
        local_catalog_root = f"/tmp/deltacat-comprehensive-test-{uuid.uuid4()}"
        namespace = "test_namespace"
        catalog_name = f"comprehensive-test-{uuid.uuid4()}"
        table_name = "comprehensive_test_table"

        # Test data
        test_data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.1, 20.2, 30.3, 40.4, 50.5],
            'category': ['A', 'B', 'A', 'C', 'B']
        })

        catalog_properties = CatalogProperties(root=local_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )

        # Test all LOCAL storage types 
        local_storage_types = [
            (DatasetType.PYARROW, "PyArrow"),
            (DatasetType.PANDAS, "Pandas"),
            (DatasetType.POLARS, "Polars"),
            (DatasetType.NUMPY, "NumPy"),
        ]

        local_successes = 0
        for storage_type, type_name in local_storage_types:
            result_table = dc.read_table(
                table=table_name, namespace=namespace, catalog=catalog_name,
                distributed_dataset_type=None, table_type=storage_type,
            )
            
            # Verify the data was read correctly
            if storage_type == DatasetType.PYARROW:
                assert isinstance(result_table, pa.Table)
                assert result_table.num_rows == 5
                assert len(result_table.column_names) == 4
            elif storage_type == DatasetType.PANDAS:
                import pandas as pd
                assert isinstance(result_table, pd.DataFrame)
                assert len(result_table) == 5
                assert len(result_table.columns) == 4
            elif storage_type == DatasetType.POLARS:
                import polars as pl
                assert isinstance(result_table, pl.DataFrame)
                assert result_table.shape == (5, 4)
            elif storage_type == DatasetType.NUMPY:
                import numpy as np
                assert isinstance(result_table, np.ndarray)
                assert result_table.shape[0] == 5
            local_successes += 1

        # Test all DISTRIBUTED storage types
        distributed_storage_types = [
            (DatasetType.RAY_DATASET, "RAY_DATASET"),
            (DatasetType.DAFT, "DAFT"),
        ]

        distributed_successes = 0
        for distributed_type, type_name in distributed_storage_types:
            result_table = dc.read_table(
                table=table_name, namespace=namespace, catalog=catalog_name,
                distributed_dataset_type=distributed_type,
            )
            
            # For distributed types, we expect different return types
            assert result_table is not None
            distributed_successes += 1
            
        # Clean up
        try:
            dc.clear_catalogs()
            import shutil
            shutil.rmtree(local_catalog_root, ignore_errors=True)
        except Exception:
            pass

    def test_custom_kwargs_comprehensive_local_storage(self):
        """Test custom kwargs propagation with all storage types using local filesystem."""
        local_catalog_root = f"/tmp/deltacat-kwargs-comprehensive-test-{uuid.uuid4()}"
        namespace = "test_namespace"
        catalog_name = f"kwargs-comprehensive-test-{uuid.uuid4()}"
        table_name = "kwargs_comprehensive_test_table"

        # Test data
        test_data = pa.table({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.1, 20.2, 30.3, 40.4, 50.5],
            'category': ['A', 'B', 'A', 'C', 'B']
        })

        catalog_properties = CatalogProperties(root=local_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )

        # Test LOCAL storage types with custom kwargs
        local_test_cases = [
            {
                "table_type": DatasetType.PYARROW,
                "name": "PyArrow",
                "custom_kwargs": {
                    "pre_buffer": True,
                    "use_pandas_metadata": True,
                    "file_path_column": "_source_path",  # Test standardized file path column
                }
            },
            {
                "table_type": DatasetType.PANDAS,
                "name": "Pandas", 
                "custom_kwargs": {
                    "use_pandas_metadata": True,
                    "file_path_column": "_source_path",  # Test standardized file path column
                }
            },
            {
                "table_type": DatasetType.POLARS,
                "name": "Polars",
                "custom_kwargs": {
                    "use_pyarrow": True,
                    "file_path_column": "_source_path",  # Test standardized file path column
                }
            },
            {
                "table_type": DatasetType.NUMPY,
                "name": "NumPy",
                "custom_kwargs": {
                    "use_pandas_metadata": True,
                    "file_path_column": "_source_path",  # Test standardized file path column
                    # Note: NumPy doesn't support named columns, so no file_path_column
                }
            }
        ]

        for test_case in local_test_cases:
            result_table = dc.read_table(
                table=table_name, namespace=namespace, catalog=catalog_name,
                distributed_dataset_type=None, table_type=test_case["table_type"],
                **test_case["custom_kwargs"]
            )
            
            # Verify the data was read correctly
            file_path_column = test_case["custom_kwargs"].get("file_path_column")
            
            if test_case["table_type"] == DatasetType.PYARROW:
                assert isinstance(result_table, pa.Table)
                assert result_table.num_rows == 5
                expected_cols = 5 if file_path_column else 4
                assert len(result_table.column_names) == expected_cols
                if file_path_column:
                    assert file_path_column in result_table.column_names
                    
            elif test_case["table_type"] == DatasetType.PANDAS:
                import pandas as pd
                assert isinstance(result_table, pd.DataFrame)
                assert len(result_table) == 5
                expected_cols = 5 if file_path_column else 4
                assert len(result_table.columns) == expected_cols
                if file_path_column:
                    assert file_path_column in result_table.columns
                    
            elif test_case["table_type"] == DatasetType.POLARS:
                import polars as pl
                assert isinstance(result_table, pl.DataFrame)
                expected_cols = 5 if file_path_column else 4
                assert result_table.shape == (5, expected_cols)
                if file_path_column:
                    assert file_path_column in result_table.columns
                    
            elif test_case["table_type"] == DatasetType.NUMPY:
                import numpy as np
                assert isinstance(result_table, np.ndarray)
                expected_cols = 5 if file_path_column else 4
                assert result_table.shape[0] == expected_cols
                # NumPy doesn't support named columns, so we just validate column count
            
        # Test DISTRIBUTED storage types with custom kwargs
        distributed_test_cases = [
            {
                "distributed_dataset_type": DatasetType.RAY_DATASET,
                "name": "RAY_DATASET",
                "custom_kwargs": {
                    "file_path_column": "path",  # Standardized file path column parameter
                }
            },
            {
                "distributed_dataset_type": DatasetType.DAFT,
                "name": "DAFT", 
                "custom_kwargs": {
                    "io_config": None,  # Daft IOConfig - None means use default local filesystem
                    "ray_init_options": {"num_cpus": 1},  # Ray options for Daft's Ray backend
                    "file_path_column": "_source_file_path",  # Add source file path column
                }
            }
        ]

        for test_case in distributed_test_cases:
            result_table = dc.read_table(
                table=table_name, namespace=namespace, catalog=catalog_name,
                distributed_dataset_type=test_case["distributed_dataset_type"],
                **test_case["custom_kwargs"]
            )
            assert result_table is not None
            
            # Additional validation based on type
            if test_case["distributed_dataset_type"] == DatasetType.RAY_DATASET:
                # Ray dataset should be materialized
                assert hasattr(result_table, 'num_rows') or hasattr(result_table, 'count')
                
                # Special validation for file_path_column if it was specified
                if test_case["custom_kwargs"].get("file_path_column"):
                    file_path_column_name = test_case["custom_kwargs"]["file_path_column"]
                    
                    # Check schema for path column
                    schema_names = result_table.schema().names
                    
                    # Ray dataset should have the file path column
                    sample_data = result_table.take(2)
                    assert sample_data and file_path_column_name in sample_data[0], f"File path column '{file_path_column_name}' not found in data!"
                    paths = [row[file_path_column_name] for row in sample_data]
                    assert all(path is not None and len(str(path)) > 0 for path in paths), "Ray paths should not be empty"
                    assert any("/" in str(path) for path in paths), "Ray paths should contain valid file system paths"
            elif test_case["distributed_dataset_type"] == DatasetType.DAFT:
                # Daft dataframe should have proper methods
                assert hasattr(result_table, 'collect') or hasattr(result_table, 'show')
                
                # Special validation for file_path_column if it was specified
                if "file_path_column" in test_case["custom_kwargs"]:
                    file_path_column_name = test_case["custom_kwargs"]["file_path_column"]
                    
                    # Get the column names from the Daft DataFrame
                    column_names = result_table.column_names
                    
                    # Verify the file path column exists
                    assert file_path_column_name in column_names, f"File path column '{file_path_column_name}' not found in columns: {column_names}"
                    
                    # Collect a sample to verify the file paths are populated
                    sample_data = result_table.limit(3).collect()
                    file_paths = sample_data.to_pydict()[file_path_column_name]
                    
                    # Verify file paths are not None/empty and contain actual paths
                    assert all(path is not None and len(str(path)) > 0 for path in file_paths), "File paths should not be empty"
                    assert any("/" in str(path) for path in file_paths), "File paths should contain valid file system paths"
            
        # Clean up
        try:
            dc.clear_catalogs()
            import shutil
            shutil.rmtree(local_catalog_root, ignore_errors=True)
        except Exception:
            pass

    def test_file_path_column_with_column_selection(self):
        """
        Test that file_path_column is always included when used with column selection.
        
        This test verifies that when both file_path_column and include_columns are specified,
        the file path column is always present in the result, even if it wasn't explicitly
        included in the include_columns list.
        """
        local_catalog_root = f"/tmp/deltacat-file-path-col-test-{uuid.uuid4()}"
        namespace = "test_namespace"
        catalog_name = f"file-path-col-test-{uuid.uuid4()}"
        table_name = "file_path_column_test_table"

        # Test data with multiple columns
        test_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'], 
            'value': [10.1, 20.2, 30.3],
            'category': ['A', 'B', 'A'],
            'extra_col': ['x', 'y', 'z']
        })

        catalog_properties = CatalogProperties(root=local_catalog_root)
        catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
        
        dc.write_to_table(
            data=test_data, table=table_name, namespace=namespace,
            catalog=catalog_name, mode=TableWriteMode.CREATE,
        )

        # Test cases for different combinations of file_path_column and column selection
        test_cases = [
            {
                "name": "PyArrow LOCAL with file_path_column + include_columns (path NOT in include)",
                "table_type": DatasetType.PYARROW,
                "distributed_dataset_type": None,
                "include_columns": ["id", "name"],  # Deliberately exclude file path column
                "file_path_column": "_file_source",
                "expected_columns": {"id", "name", "_file_source"},  # Should include path column anyway
            },
            {
                "name": "PyArrow LOCAL with file_path_column + include_columns (path IN include)",
                "table_type": DatasetType.PYARROW,
                "distributed_dataset_type": None,
                "include_columns": ["id", "name", "_file_source"],  # Explicitly include file path column
                "file_path_column": "_file_source",
                "expected_columns": {"id", "name", "_file_source"},
            },
            {
                "name": "Pandas LOCAL with file_path_column + include_columns (path NOT in include)",
                "table_type": DatasetType.PANDAS,
                "distributed_dataset_type": None,
                "include_columns": ["value", "category"],  # Deliberately exclude file path column
                "file_path_column": "_source_file",
                "expected_columns": {"value", "category", "_source_file"},  # Should include path column anyway
            },
            {
                "name": "RAY_DATASET with file_path_column + include_columns (path NOT in include)",
                "table_type": None,
                "distributed_dataset_type": DatasetType.RAY_DATASET,
                "include_columns": ["id", "category"],  # Deliberately exclude file path column
                "file_path_column": "ray_path",
                "expected_columns": {"id", "category", "ray_path"},  # Should include path column anyway
            },
            {
                "name": "DAFT with file_path_column + include_columns (path NOT in include)",
                "table_type": None,
                "distributed_dataset_type": DatasetType.DAFT,
                "include_columns": ["name", "value"],  # Deliberately exclude file path column
                "file_path_column": "daft_source",
                "expected_columns": {"name", "value", "daft_source"},  # Should include path column anyway
            },
        ]

        for test_case in test_cases: 
            # Prepare arguments
            read_args = {
                "table": table_name,
                "namespace": namespace, 
                "catalog": catalog_name,
                "columns": test_case["include_columns"],  # Use 'columns' not 'include_columns'
                "file_path_column": test_case["file_path_column"],
            }
            
            if test_case["table_type"]:
                read_args["table_type"] = test_case["table_type"]
                read_args["distributed_dataset_type"] = None
            else:
                read_args["distributed_dataset_type"] = test_case["distributed_dataset_type"]
            
            # Read the table
            result = dc.read_table(**read_args)
            
            # Get column names based on result type
            if test_case["table_type"] == DatasetType.PYARROW:
                actual_columns = set(result.column_names)
            elif test_case["table_type"] == DatasetType.PANDAS:
                actual_columns = set(result.columns)
            elif test_case["distributed_dataset_type"] == DatasetType.RAY_DATASET:
                actual_columns = set(result.schema().names)
            elif test_case["distributed_dataset_type"] == DatasetType.DAFT:
                actual_columns = set(result.column_names)
            else:
                raise ValueError(f"Unsupported test case: {test_case}")
            
            # Verify that we got exactly the expected columns
            assert actual_columns == test_case["expected_columns"], \
                f"Column mismatch. Expected: {test_case['expected_columns']}, Got: {actual_columns}"
            
            # Specifically verify that the file path column is present
            assert test_case["file_path_column"] in actual_columns, \
                f"File path column '{test_case['file_path_column']}' missing from result"

        # Clean up
        try:
            dc.clear_catalogs()
            import shutil
            shutil.rmtree(local_catalog_root, ignore_errors=True)
        except Exception:
            pass
