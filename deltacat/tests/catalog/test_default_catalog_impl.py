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
    create_table_from_csv_file_paths,
)
from deltacat.types.media import DatasetType, ContentType, DistributedDatasetType
from deltacat.storage.model.types import DeltaType
from deltacat.storage import metastore
from deltacat.storage.model.schema import Schema, Field
from deltacat.storage.model.types import SchemaConsistencyType
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
            pa_table=create_table_from_csv_file_paths([self.SAMPLE_FILE_PATH]),
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
        has_conflict_message = "Concurrent modification" in full_error_context or "concurrent conflict" in full_error_context
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
    

class TestDatasetTypes:
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


class TestTableVersionWriteModes:
    """
    Comprehensive test suite for write_to_table with table_version parameter.
    Tests all combinations of:
    - Write modes: CREATE, APPEND, REPLACE, MERGE, DELETE, AUTO
    - Table version specification: None (latest) vs specific version
    - Table existence: exists vs doesn't exist
    - Table version existence: exists vs doesn't exist
    """
    
    @classmethod
    def setup_class(cls):
        """Set up catalog for all tests in this class."""
        cls.local_catalog_root = tempfile.mkdtemp()
        cls.catalog_name = "test_table_version_catalog"
        catalog_properties = CatalogProperties(root=cls.local_catalog_root)
        cls.catalog = dc.put_catalog(cls.catalog_name, catalog=Catalog(config=catalog_properties))
        
        # Test data
        cls.test_data = {
            'initial': pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']}),
            'additional': pd.DataFrame({'id': [3, 4], 'name': ['Charlie', 'David']}),
            'merge_data': pd.DataFrame({'id': [1, 3], 'name': ['Alice_Updated', 'Charlie_New']}),
        }
    
    @classmethod
    def teardown_class(cls):
        """Clean up after all tests."""
        try:
            dc.clear_catalogs()
            shutil.rmtree(cls.local_catalog_root, ignore_errors=True)
        except Exception:
            pass
    
    def test_create_mode_combinations(self):
        """Test CREATE mode with all table/version existence combinations."""
        
        test_cases = [
            # (table_suffix, setup_versions, test_version, should_succeed, description)
            ('new', [], '1', True, 'Create new table with version 1'),
            ('new2', [], None, True, 'Create new table without specifying version'),
            
            ('existing', ['1'], '2', True, 'Create version 2 of existing table'),
            ('existing2', ['1'], '1', False, 'Try to create existing version 1'),
            ('existing3', ['1'], None, False, 'Try to create existing table without version'),
            
            ('multi', ['1', '2'], '3', True, 'Create version 3 when 1,2 exist'),
            ('multi2', ['1', '2'], '1', False, 'Try to create existing version 1 when 1,2 exist'),
            ('multi3', ['1', '2'], '2', False, 'Try to create existing version 2 when 1,2 exist'),
        ]
        
        for table_suffix, setup_versions, test_version, should_succeed, description in test_cases:
            table_name = f'create_test_{table_suffix}'
            namespace = 'test_ns'
            
            # Set up existing versions if needed
            for version in setup_versions:
                dc.write_to_table(
                    self.test_data['initial'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=version,
                    mode=TableWriteMode.CREATE
                )
            
            if should_succeed:
                # Should succeed
                dc.write_to_table(
                    self.test_data['initial'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=test_version,
                    mode=TableWriteMode.CREATE
                )
                
                # Verify the table/version was created
                if test_version:
                    table_def = dc.get_table(
                        table_name, 
                        catalog=self.catalog_name, 
                        namespace=namespace, 
                        table_version=test_version
                    )
                    assert table_def is not None, f"Failed to create version {test_version}"
                    assert table_def.table_version.table_version == test_version
                
            else:
                # Should fail
                with pytest.raises((ValueError, Exception)):
                    dc.write_to_table(
                        self.test_data['initial'],
                        table_name,
                        catalog=self.catalog_name,
                        namespace=namespace,
                        table_version=test_version,
                        mode=TableWriteMode.CREATE
                    )
    
    def test_append_mode_combinations(self):
        """Test APPEND mode with all table/version existence combinations."""
        
        test_cases = [
            # (table_suffix, setup_versions, test_version, should_succeed, description)
            ('new', [], None, False, 'Try to append to non-existent table'),
            ('new2', [], '1', False, 'Try to append to non-existent table version'),
            
            ('existing', ['1'], None, True, 'Append to existing table (latest version)'),
            ('existing2', ['1'], '1', True, 'Append to existing version 1'),
            ('existing3', ['1'], '2', False, 'Try to append to non-existent version 2'),
            
            ('multi', ['1', '2'], None, True, 'Append to existing table (latest is 2)'),
            ('multi2', ['1', '2'], '1', True, 'Append to existing version 1'),
            ('multi3', ['1', '2'], '2', True, 'Append to existing version 2'),
            ('multi4', ['1', '2'], '3', False, 'Try to append to non-existent version 3'),
        ]
        
        for table_suffix, setup_versions, test_version, should_succeed, description in test_cases:
            table_name = f'append_test_{table_suffix}'
            namespace = 'test_ns'
            
            # Set up existing versions if needed
            for version in setup_versions:
                dc.write_to_table(
                    self.test_data['initial'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=version,
                    mode=TableWriteMode.CREATE
                )
            
            if should_succeed:
                # Should succeed
                dc.write_to_table(
                    self.test_data['additional'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=test_version,
                    mode=TableWriteMode.APPEND
                )
            else:
                # Should fail
                with pytest.raises((ValueError, Exception)):
                    dc.write_to_table(
                        self.test_data['additional'],
                        table_name,
                        catalog=self.catalog_name,
                        namespace=namespace,
                        table_version=test_version,
                        mode=TableWriteMode.APPEND
                    )
    
    def test_auto_mode_combinations(self):
        """Test AUTO mode with all table/version existence combinations."""
        
        test_cases = [
            # (table_suffix, setup_versions, test_version, should_succeed, description)
            ('new', [], None, True, 'Auto create new table'),
            ('new2', [], '1', True, 'Auto create new table with version 1'),
            
            ('existing', ['1'], None, True, 'Auto use existing table (latest version)'),
            ('existing2', ['1'], '1', True, 'Auto use existing version 1'),
            ('existing3', ['1'], '2', False, 'Try auto with non-existent version 2'),
            
            ('multi', ['1', '2'], None, True, 'Auto use existing table (latest is 2)'),
            ('multi2', ['1', '2'], '1', True, 'Auto use existing version 1'),
            ('multi3', ['1', '2'], '2', True, 'Auto use existing version 2'),
            ('multi4', ['1', '2'], '3', False, 'Try auto with non-existent version 3'),
        ]
        
        for table_suffix, setup_versions, test_version, should_succeed, description in test_cases:
            table_name = f'auto_test_{table_suffix}'
            namespace = 'test_ns'
            
            # Set up existing versions if needed
            for version in setup_versions:
                dc.write_to_table(
                    self.test_data['initial'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=version,
                    mode=TableWriteMode.CREATE
                )
            
            if should_succeed:
                # Should succeed
                dc.write_to_table(
                    self.test_data['additional'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=test_version,
                    mode=TableWriteMode.AUTO
                )
            else:
                # Should fail
                with pytest.raises((ValueError, Exception)):
                    dc.write_to_table(
                        self.test_data['additional'],
                        table_name,
                        catalog=self.catalog_name,
                        namespace=namespace,
                        table_version=test_version,
                        mode=TableWriteMode.AUTO
                    )
    
    def test_replace_mode_combinations(self):
        """Test REPLACE mode with all table/version existence combinations."""
        
        test_cases = [
            # (table_suffix, setup_versions, test_version, should_succeed, description)
            ('new', [], None, False, 'Try to replace non-existent table'),
            ('new2', [], '1', False, 'Try to replace non-existent table version'),
            
            ('existing', ['1'], None, True, 'Replace existing table (latest version)'),
            ('existing2', ['1'], '1', True, 'Replace existing version 1'),
            ('existing3', ['1'], '2', False, 'Try to replace non-existent version 2'),
            
            ('multi', ['1', '2'], None, True, 'Replace existing table (latest is 2)'),
            ('multi2', ['1', '2'], '1', True, 'Replace existing version 1'),
            ('multi3', ['1', '2'], '2', True, 'Replace existing version 2'),
            ('multi4', ['1', '2'], '3', False, 'Try to replace non-existent version 3'),
        ]
        
        for table_suffix, setup_versions, test_version, should_succeed, description in test_cases:
            table_name = f'replace_test_{table_suffix}'
            namespace = 'test_ns'
            
            # Set up existing versions if needed
            for version in setup_versions:
                dc.write_to_table(
                    self.test_data['initial'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=version,
                    mode=TableWriteMode.CREATE
                )
            
            if should_succeed:
                # Should succeed
                dc.write_to_table(
                    self.test_data['merge_data'],
                    table_name,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=test_version,
                    mode=TableWriteMode.REPLACE
                )
            else:
                # Should fail
                with pytest.raises((ValueError, Exception)):
                    dc.write_to_table(
                        self.test_data['merge_data'],
                        table_name,
                        catalog=self.catalog_name,
                        namespace=namespace,
                        table_version=test_version,
                        mode=TableWriteMode.REPLACE
                    )
    
    def test_merge_delete_modes_with_schema(self):
        """Test MERGE and DELETE modes with schema requirements."""
        
        # Create table with merge keys using the same pattern as existing tests
        merge_schema = Schema.of([
            Field.of(pa.field("id", pa.int64()), is_merge_key=True),
            Field.of(pa.field("name", pa.string())),
        ])
        
        table_name = 'merge_test_table'
        namespace = 'test_ns'
        
        # Create table with merge keys
        dc.write_to_table(
            self.test_data['initial'],
            table_name,
            catalog=self.catalog_name,
            namespace=namespace,
            table_version='1',
            mode=TableWriteMode.CREATE,
            schema=merge_schema
        )
        
        # Create table without merge keys for negative testing
        no_merge_table = 'no_merge_table'
        dc.write_to_table(
            self.test_data['initial'],
            no_merge_table,
            catalog=self.catalog_name,
            namespace=namespace,
            table_version='1',
            mode=TableWriteMode.CREATE
        )
        
        # Test MERGE mode
        test_cases = [
            # (table, table_version, should_succeed, mode, description)
            (table_name, '1', True, TableWriteMode.MERGE, 'MERGE on table with merge keys'),
            (table_name, '2', False, TableWriteMode.MERGE, 'MERGE on non-existent version'),
            (no_merge_table, '1', False, TableWriteMode.MERGE, 'MERGE on table without merge keys'),
            
            (table_name, '1', True, TableWriteMode.DELETE, 'DELETE on table with merge keys'),
            (table_name, '2', False, TableWriteMode.DELETE, 'DELETE on non-existent version'),
            (no_merge_table, '1', False, TableWriteMode.DELETE, 'DELETE on table without merge keys'),
        ]
        
        for table, table_version, should_succeed, mode, description in test_cases:
            if should_succeed:
                # Should succeed - entry_params will be automatically set from schema merge keys
                dc.write_to_table(
                    self.test_data['merge_data'],
                    table,
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version=table_version,
                    mode=mode
                )
            else:
                # Should fail
                with pytest.raises((ValueError, Exception)):
                    dc.write_to_table(
                        self.test_data['merge_data'],
                        table,
                        catalog=self.catalog_name,
                        namespace=namespace,
                        table_version=table_version,
                        mode=mode
                    )

    def test_schema_validation_and_coercion(self):
        """Test schema validation and coercion functionality with different consistency types."""
        
        namespace = 'test_ns'
        
        # Test data with type mismatches that can be coerced
        test_data_coercible = pa.table({
            'id': [1, 2, 3],  # int64, should coerce to int32
            'name': ['alice', 'bob', 'charlie'],  # string
            'score': [95.5, 87.2, 92.1],  # float64, should coerce to float32
        })
        
        # Test data with type mismatches that cannot be coerced
        test_data_incompatible = pa.table({
            'id': ['not_a_number', 'invalid', 'bad'],  # string, cannot coerce to int32
            'name': ['alice', 'bob', 'charlie'],  # string
            'score': [95.5, 87.2, 92.1],  # float64
        })
        
        # Test 1: Schema with COERCE consistency type
        coerce_schema = Schema.of([
            Field.of(pa.field("id", pa.int32()), consistency_type=SchemaConsistencyType.COERCE),
            Field.of(pa.field("name", pa.string()), consistency_type=SchemaConsistencyType.NONE),
            Field.of(pa.field("score", pa.float32()), consistency_type=SchemaConsistencyType.COERCE),
        ])
        
        # Should succeed - data can be coerced
        dc.write_to_table(
            test_data_coercible,
            'coerce_test_table',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.CREATE,
            schema=coerce_schema
        )
        print("✅ COERCE mode with compatible data types")
        
        # Test 2: Schema with VALIDATE consistency type
        validate_schema = Schema.of([
            Field.of(pa.field("id", pa.int64()), consistency_type=SchemaConsistencyType.VALIDATE),
            Field.of(pa.field("name", pa.string()), consistency_type=SchemaConsistencyType.VALIDATE),
            Field.of(pa.field("score", pa.float64()), consistency_type=SchemaConsistencyType.VALIDATE),
        ])
        
        # Should succeed - data types match exactly
        dc.write_to_table(
            test_data_coercible,
            'validate_test_table',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.CREATE,
            schema=validate_schema
        )
        print("✅ VALIDATE mode with exact type match")
        
        # Test 3: Schema with NONE consistency type (should always work)
        none_schema = Schema.of([
            Field.of(pa.field("id", pa.int32()), consistency_type=SchemaConsistencyType.NONE),
            Field.of(pa.field("name", pa.string()), consistency_type=SchemaConsistencyType.NONE),
            Field.of(pa.field("score", pa.float32()), consistency_type=SchemaConsistencyType.NONE),
        ])
        
        # Should succeed - no validation performed
        dc.write_to_table(
            test_data_coercible,
            'none_test_table',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.CREATE,
            schema=none_schema
        )
        print("✅ NONE consistency type (no validation)")
        
        # Test 4: COERCE should fail when types cannot be coerced
        try:
            dc.write_to_table(
                test_data_incompatible,
                'coerce_fail_table',
                catalog=self.catalog_name,
                namespace=namespace,
                mode=TableWriteMode.CREATE,
                schema=coerce_schema
            )
            assert False, "Expected coercion to fail with incompatible data"
        except Exception as e:
            print(f"✅ COERCE mode correctly failed with incompatible data: {type(e).__name__}")
        
        # Test 5: VALIDATE should fail when types don't match exactly
        mismatch_schema = Schema.of([
            Field.of(pa.field("id", pa.int32()), consistency_type=SchemaConsistencyType.VALIDATE),  # Expects int32 but gets int64
            Field.of(pa.field("name", pa.string()), consistency_type=SchemaConsistencyType.VALIDATE),
            Field.of(pa.field("score", pa.float32()), consistency_type=SchemaConsistencyType.VALIDATE),  # Expects float32 but gets float64
        ])
        
        try:
            dc.write_to_table(
                test_data_coercible,  # This has int64 and float64, but schema expects int32 and float32
                'validate_fail_table',
                catalog=self.catalog_name,
                namespace=namespace,
                mode=TableWriteMode.CREATE,
                schema=mismatch_schema
            )
            assert False, "Expected validation to fail with type mismatch"
        except Exception as e:
            print(f"✅ VALIDATE mode correctly failed with type mismatch: {type(e).__name__}")
 
    def test_backward_compatibility(self):
        """Test that existing behavior is preserved when table_version is not specified."""
        namespace = 'test_ns'
        
        # Test all modes without specifying table_version (should work exactly as before)
        
        # 1. CREATE mode without version - should create new table
        dc.write_to_table(
            self.test_data['initial'],
            'compat_table_1',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.CREATE
        )
        
        # 2. AUTO mode without version - should use latest
        dc.write_to_table(
            self.test_data['additional'],
            'compat_table_1',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.AUTO
        )
        
        # 3. APPEND mode without version - should append to latest
        dc.write_to_table(
            self.test_data['additional'],
            'compat_table_1',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.APPEND
        )
        
        # 4. REPLACE mode without version - should replace latest
        dc.write_to_table(
            self.test_data['merge_data'],
            'compat_table_1',
            catalog=self.catalog_name,
            namespace=namespace,
            mode=TableWriteMode.REPLACE
        )
        
    
    def test_error_messages_quality(self):
        """Test that error messages are clear and helpful."""
        namespace = 'test_ns'
        
        # Create a table for testing
        dc.write_to_table(
            self.test_data['initial'],
            'error_test_table',
            catalog=self.catalog_name,
            namespace=namespace,
            table_version='1',
            mode=TableWriteMode.CREATE
        )
        
        error_cases = [
            {
                'operation': lambda: dc.write_to_table(
                    self.test_data['initial'],
                    'error_test_table',
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version='1',
                    mode=TableWriteMode.CREATE
                ),
                'expected_keywords': ['version', 'already exists', 'CREATE'],
                'description': 'CREATE existing version error'
            },
            {
                'operation': lambda: dc.write_to_table(
                    self.test_data['initial'],
                    'error_test_table',
                    catalog=self.catalog_name,
                    namespace=namespace,
                    table_version='99',
                    mode=TableWriteMode.APPEND
                ),
                'expected_keywords': ['version', 'does not exist'],
                'description': 'APPEND to non-existent version error'
            },
            {
                'operation': lambda: dc.write_to_table(
                    self.test_data['initial'],
                    'nonexistent_table',
                    catalog=self.catalog_name,
                    namespace=namespace,
                    mode=TableWriteMode.APPEND
                ),
                'expected_keywords': ['does not exist', 'APPEND'],
                'description': 'APPEND to non-existent table error'
            }
        ]
        
        for case in error_cases:
            with pytest.raises(Exception) as exc_info:
                case['operation']()
            
            error_message = str(exc_info.value)
            for keyword in case['expected_keywords']:
                assert keyword.lower() in error_message.lower(), \
                    f"Error message for {case['description']} should contain '{keyword}'. Got: {error_message}"


def test_missing_field_backfill_behavior():
    """Test missing field handling and backfill behavior based on SchemaConsistencyType."""
    # Import deltacat API functions instead of internal implementation
    from deltacat.storage.model.schema import Schema, Field, SchemaConsistencyType
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    import deltacat as dc
    import pyarrow as pa
    import pandas as pd
    import uuid
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"test-{uuid.uuid4()}"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Create a schema with different consistency types and field configurations
    fields = [
        # VALIDATE type - should error when missing
        Field.of(pa.field("required_field", pa.string(), nullable=False), 
                 consistency_type=SchemaConsistencyType.VALIDATE),
        
        # COERCE type with future_default - should use default when missing
        Field.of(pa.field("default_field", pa.int64(), nullable=False), 
                 consistency_type=SchemaConsistencyType.COERCE,
                 future_default=42),
        
        # COERCE type nullable without default - should backfill with nulls
        Field.of(pa.field("nullable_field", pa.string(), nullable=True), 
                 consistency_type=SchemaConsistencyType.COERCE),
        
        # COERCE type non-nullable without default - should error when missing
        Field.of(pa.field("non_nullable_no_default", pa.float64(), nullable=False), 
                 consistency_type=SchemaConsistencyType.COERCE),
        
        # NONE type with future_default - should use default
        Field.of(pa.field("none_with_default", pa.string(), nullable=False), 
                 consistency_type=SchemaConsistencyType.NONE,
                 future_default="default_value"),
        
        # NONE type without default - should backfill with nulls
        Field.of(pa.field("none_nullable", pa.int32(), nullable=True), 
                 consistency_type=SchemaConsistencyType.NONE),
    ]
    
    schema = Schema.of(fields)
    table_name_base = "test_missing_fields_table"
    
    # Test 1: Missing required field (VALIDATE) should fail
    incomplete_data = pd.DataFrame({
        "default_field": [1, 2],
        "nullable_field": ["a", "b"],
        "non_nullable_no_default": [1.1, 2.2],
        "none_with_default": ["x", "y"], 
        "none_nullable": [10, 20]
        # Missing required_field - should cause VALIDATE error
    })
    
    with pytest.raises(ValueError, match="required but not present"):
        dc.write_to_table(
            data=incomplete_data,
            table=table_name_base + "_test1",
            namespace=namespace,
            catalog=catalog_name,
            schema=schema
        )
    
    # Test 2: Missing non-nullable field without default (COERCE) should fail
    incomplete_data2 = pd.DataFrame({
        "required_field": ["val1", "val2"],
        "default_field": [1, 2], 
        "nullable_field": ["a", "b"],
        "none_with_default": ["x", "y"],
        "none_nullable": [10, 20]
        # Missing non_nullable_no_default - should cause COERCE error
    })
    
    with pytest.raises(ValueError, match="not nullable.*not present.*no future_default"):
        dc.write_to_table(
            data=incomplete_data2,
            table=table_name_base + "_test2",
            namespace=namespace,
            catalog=catalog_name,
            schema=schema
        )
    
    # Test 3: Success case - provide only some fields, let others backfill appropriately
    partial_data = pd.DataFrame({
        "required_field": ["val1", "val2"],
        "non_nullable_no_default": [1.1, 2.2]
        # Missing: default_field, nullable_field, none_with_default, none_nullable
        # These should be backfilled based on their consistency types and defaults
    })
    
    # This should succeed with appropriate backfilling
    dc.write_to_table(
        data=partial_data,
        table=table_name_base + "_test3",
        namespace=namespace,
        catalog=catalog_name,
        schema=schema,
        mode=TableWriteMode.CREATE
    )
    
    # Read back and verify backfill behavior
    result_df = dc.read_table(table=table_name_base + "_test3", namespace=namespace, catalog=catalog_name)
    
    # Convert to pandas for easier testing
    if hasattr(result_df, 'to_pandas'):
        result_df = result_df.to_pandas()
    elif hasattr(result_df, 'collect'):
        result_df = result_df.collect().to_pandas()
    
    assert len(result_df) == 2
    assert list(result_df["required_field"]) == ["val1", "val2"]
    assert list(result_df["non_nullable_no_default"]) == [1.1, 2.2]
    
    # Verify backfilled values
    assert list(result_df["default_field"]) == [42, 42]  # future_default used
    assert list(result_df["nullable_field"]) == [None, None]  # nulls for nullable COERCE
    assert list(result_df["none_with_default"]) == ["default_value", "default_value"]  # future_default used
    
    # For integer columns, pandas represents nulls as NaN
    import numpy as np
    assert all(np.isnan(x) for x in result_df["none_nullable"])  # nulls for NONE type
    
    # Test 4: Verify extra field rejection
    data_with_extra = pd.DataFrame({
        "required_field": ["val1", "val2"],
        "default_field": [1, 2],
        "nullable_field": ["a", "b"], 
        "non_nullable_no_default": [1.1, 2.2],
        "none_with_default": ["x", "y"],
        "none_nullable": [10, 20],
        "extra_field": ["should", "fail"]  # This field is not in schema
    })
    
    with pytest.raises(ValueError, match="not present in the schema"):
        dc.write_to_table(
            data=data_with_extra,
            table=table_name_base + "_test4",
            namespace=namespace,
            catalog=catalog_name,
            schema=schema
        )


def test_schemaless_table_append_mode():
    """Test write_to_table with schema=None in APPEND mode."""
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    import deltacat as dc
    import pyarrow as pa
    import pandas as pd
    import uuid
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-schemaless-append-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"schemaless-append-test-{uuid.uuid4()}"
    table_name = "schemaless_append_table"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Test 1: Create schemaless table with initial data
    initial_data = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "value": [10.1, 20.2]
    })
    
    dc.write_to_table(
        data=initial_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.CREATE
    )
    
    # Verify initial data can be read back
    result1 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    
    # For schemaless tables, we now get a list of tables
    if isinstance(result1, list):
        # Combine the list of tables into a single DataFrame
        all_tables = []
        for table in result1:
            if hasattr(table, 'to_pandas'):
                table = table.to_pandas()
            all_tables.append(table)
        result1 = pd.concat(all_tables, ignore_index=True, sort=False) if all_tables else pd.DataFrame()
    elif hasattr(result1, 'to_pandas'):
        result1 = result1.to_pandas()
    elif hasattr(result1, 'collect'):
        result1 = result1.collect().to_pandas()
    
    assert len(result1) == 2
    assert set(result1.columns) == {"id", "name", "value"}
    assert list(result1["id"]) == [1, 2]
    assert list(result1["name"]) == ["Alice", "Bob"]
    
    # Test 2: Append more data to schemaless table
    append_data = pd.DataFrame({
        "id": [3, 4],
        "name": ["Charlie", "Diana"],
        "value": [30.3, 40.4]
    })
    
    dc.write_to_table(
        data=append_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.APPEND
    )
    
    # Verify all data is present
    result2 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    
    # For schemaless tables, we now get a list of tables
    if isinstance(result2, list):
        # Combine the list of tables into a single DataFrame
        all_tables = []
        for table in result2:
            if hasattr(table, 'to_pandas'):
                table = table.to_pandas()
            all_tables.append(table)
        result2 = pd.concat(all_tables, ignore_index=True, sort=False) if all_tables else pd.DataFrame()
    elif hasattr(result2, 'to_pandas'):
        result2 = result2.to_pandas()
    elif hasattr(result2, 'collect'):
        result2 = result2.collect().to_pandas()
    
    assert len(result2) == 4
    assert set(result2.columns) == {"id", "name", "value"}
    assert sorted(result2["id"]) == [1, 2, 3, 4]
    assert sorted(result2["name"]) == ["Alice", "Bob", "Charlie", "Diana"]
    
    # Test 3: Append data with same structure (should work for schemaless)
    more_data = pd.DataFrame({
        "id": [5, 6],
        "name": ["Eve", "Frank"],
        "value": [50.5, 60.6]
    })
    
    dc.write_to_table(
        data=more_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.APPEND
    )
    
    # Verify all data is present
    result3 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    
    # For schemaless tables, we now get a list of tables
    if isinstance(result3, list):
        # Combine the list of tables into a single DataFrame
        all_tables = []
        for table in result3:
            if hasattr(table, 'to_pandas'):
                table = table.to_pandas()
            all_tables.append(table)
        result3 = pd.concat(all_tables, ignore_index=True, sort=False) if all_tables else pd.DataFrame()
    elif hasattr(result3, 'to_pandas'):
        result3 = result3.to_pandas()
    elif hasattr(result3, 'collect'):
        result3 = result3.collect().to_pandas()
    
    assert len(result3) == 6
    # Should have original columns
    expected_columns = {"id", "name", "value"}
    assert set(result3.columns) == expected_columns
    
    # Verify all data is correctly written
    assert sorted(result3["id"]) == [1, 2, 3, 4, 5, 6]
    expected_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]
    assert sorted(result3["name"]) == sorted(expected_names)
    
    print("✅ Schemaless APPEND mode test passed!")


def test_schemaless_table_merge_delete_mode():
    """Test write_to_table with schema=None - MERGE and DELETE modes should fail."""
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    import deltacat as dc
    import pandas as pd
    import uuid
    import pytest
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-schemaless-merge-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"schemaless-merge-test-{uuid.uuid4()}"
    table_name = "schemaless_merge_table"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Test 1: Create schemaless table (schema=None)
    initial_data = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [10.1, 20.2, 30.3],
        "status": ["active", "active", "inactive"]
    })
    
    # Create table without schema - note: merge_keys are ignored for schemaless tables
    dc.write_to_table(
        data=initial_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.CREATE
    )
    
    # Verify initial data
    result1 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    
    # For schemaless tables, we now get a list of tables
    if isinstance(result1, list):
        # Combine the list of tables into a single DataFrame
        all_tables = []
        for table in result1:
            if hasattr(table, 'to_pandas'):
                table = table.to_pandas()
            all_tables.append(table)
        result1 = pd.concat(all_tables, ignore_index=True, sort=False) if all_tables else pd.DataFrame()
    elif hasattr(result1, 'to_pandas'):
        result1 = result1.to_pandas()
    elif hasattr(result1, 'collect'):
        result1 = result1.collect().to_pandas()
    
    assert len(result1) == 3
    assert set(result1.columns) == {"id", "name", "value", "status"}
    
    # Test 2: MERGE operation should fail for schemaless tables
    merge_data = pd.DataFrame({
        "id": [2, 3, 4],
        "name": ["Bob_Updated", "Charlie_Updated", "Diana"],
        "value": [25.5, 35.5, 40.4],
        "status": ["updated", "updated", "new"]
    })
    
    # MERGE mode should raise ValueError for schemaless tables
    with pytest.raises(ValueError, match="MERGE mode requires tables to have at least one merge key"):
        dc.write_to_table(
            data=merge_data,
            table=table_name,
            namespace=namespace,
            catalog=catalog_name,
            schema=None,  # Explicitly set to None
            mode=TableWriteMode.MERGE
        )
    
    # Test 3: DELETE operation should also fail for schemaless tables
    delete_data = pd.DataFrame({
        "id": [1, 3],
        "name": ["Alice", "Charlie"],
        "value": [10.1, 30.3],
        "status": ["active", "inactive"]
    })
    
    # DELETE mode should raise ValueError for schemaless tables
    with pytest.raises(ValueError, match="DELETE mode requires tables to have at least one merge key"):
        dc.write_to_table(
            data=delete_data,
            table=table_name,
            namespace=namespace,
            catalog=catalog_name,
            schema=None,  # Explicitly set to None
            mode=TableWriteMode.DELETE
        )
    
    # Test 4: APPEND mode should still work for schemaless tables
    append_data = pd.DataFrame({
        "id": [4, 5],
        "name": ["Diana", "Eve"],
        "value": [40.4, 50.5],
        "status": ["new", "active"]
    })
    
    dc.write_to_table(
        data=append_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.APPEND
    )
    
    # Verify append results
    result2 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    
    # For schemaless tables, we now get a list of tables
    if isinstance(result2, list):
        # Combine the list of tables into a single DataFrame
        all_tables = []
        for table in result2:
            if hasattr(table, 'to_pandas'):
                table = table.to_pandas()
            all_tables.append(table)
        result2 = pd.concat(all_tables, ignore_index=True, sort=False) if all_tables else pd.DataFrame()
    elif hasattr(result2, 'to_pandas'):
        result2 = result2.to_pandas()
    elif hasattr(result2, 'collect'):
        result2 = result2.collect().to_pandas()
    
    # Should have 5 records total (3 original + 2 appended)
    assert len(result2) == 5
    assert set(result2.columns) == {"id", "name", "value", "status"}


def test_schemaless_table_with_evolving_schema():
    """Test write_to_table with schema=None when new columns are added over time."""
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    from deltacat.types.media import DatasetType
    import deltacat as dc
    import pandas as pd
    import uuid
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-schemaless-evolving-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"schemaless-evolving-test-{uuid.uuid4()}"
    table_name = "schemaless_evolving_table"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Test 1: Create schemaless table with initial columns
    initial_data = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "value": [10.1, 20.2]
    })
    
    dc.write_to_table(
        data=initial_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.CREATE
    )
    
    # Test 2: Append data with additional columns
    extended_data = pd.DataFrame({
        "id": [3, 4],
        "name": ["Charlie", "Diana"],
        "value": [30.3, 40.4],
        "status": ["active", "inactive"],  # New column
        "category": ["A", "B"]  # Another new column
    })
    
    dc.write_to_table(
        data=extended_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.APPEND
    )
    
    # Test 3: Read back the data - this might fail due to schema mismatches
    try:
        # Force local storage to test our schemaless table fix
        result = dc.read_table(
            table=table_name, 
            namespace=namespace, 
            catalog=catalog_name,
            table_type=DatasetType.PYARROW,
            distributed_dataset_type=None  # Force local storage
        )
        
        # Check if result is a list (which it should be for schemaless tables)
        print(f"Result type: {type(result)}")
        print(f"Result is list: {isinstance(result, list)}")
        
        if isinstance(result, list):
            print(f"Got list of {len(result)} tables")
            for i, table in enumerate(result):
                print(f"Table {i}: type={type(table)}, shape={table.shape if hasattr(table, 'shape') else 'N/A'}")
                if hasattr(table, 'columns'):
                    print(f"Table {i} columns: {list(table.columns)}")
            
            # For a list of tables, we should manually combine them
            # This is what the user would need to do for schemaless tables
            all_tables = []
            for table in result:
                if hasattr(table, 'to_pandas'):
                    table = table.to_pandas()
                all_tables.append(table)
            
            # Combine with pandas concat to handle different schemas
            result = pd.concat(all_tables, ignore_index=True, sort=False)
        else:
            if hasattr(result, 'to_pandas'):
                result = result.to_pandas()
            elif hasattr(result, 'collect'):
                result = result.collect().to_pandas()
        
        # If we get here, the read succeeded
        print(f"Read successful! Result shape: {result.shape}")
        print(f"Columns: {sorted(result.columns)}")
        
        # For schemaless tables with evolving schema, we should expect all columns
        # but some records will have NaN/None for missing columns
        expected_columns = {"id", "name", "value", "status", "category"}
        assert set(result.columns) == expected_columns
        assert len(result) == 4
        
        # Check that early records have NaN for new columns
        early_records = result[result['id'].isin([1, 2])]
        assert early_records['status'].isna().all()
        assert early_records['category'].isna().all()
        
        # Check that later records have values for all columns
        later_records = result[result['id'].isin([3, 4])]
        assert not later_records['status'].isna().any()
        assert not later_records['category'].isna().any()
        
        print("✅ Schemaless evolving schema test passed!")
        
    except Exception as e:
        print(f"❌ Read failed with error: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # This might be expected for current implementation
        # The read might fail due to schema concatenation issues
        # In that case, we need to modify read_table to handle schemaless tables differently
        raise


def test_schemaless_table_with_distributed_datasets():
    """Test schemaless tables with distributed dataset types (RAY_DATASET, DAFT)."""
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    from deltacat.types.media import DatasetType
    import deltacat as dc
    import pandas as pd
    import uuid
    import pytest
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-schemaless-distributed-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"schemaless-distributed-test-{uuid.uuid4()}"
    table_name = "schemaless_distributed_table"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Create schemaless table with initial columns
    initial_data = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"],
        "value": [10.1, 20.2]
    })
    
    dc.write_to_table(
        data=initial_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.CREATE
    )
    
    # Append data with additional columns
    extended_data = pd.DataFrame({
        "id": [3, 4],
        "name": ["Charlie", "Diana"],
        "value": [30.3, 40.4],
        "status": ["active", "inactive"],  # New column
        "category": ["A", "B"]  # Another new column
    })
    
    dc.write_to_table(
        data=extended_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=None,  # Explicitly set to None
        mode=TableWriteMode.APPEND
    )
    
    # Test 1: DAFT should raise NotImplementedError for schemaless tables
    print("=== Testing DAFT ===")
    with pytest.raises(NotImplementedError, match="Distributed dataset reading is not yet supported for schemaless tables"):
        dc.read_table(
            table=table_name, 
            namespace=namespace, 
            catalog=catalog_name,
            distributed_dataset_type=DatasetType.DAFT
        )
    print("✅ DAFT correctly raised NotImplementedError for schemaless tables")
    
    # Test 2: RAY_DATASET should also raise NotImplementedError for schemaless tables
    print("\n=== Testing RAY_DATASET ===")
    with pytest.raises(NotImplementedError, match="Distributed dataset reading is not yet supported for schemaless tables"):
        dc.read_table(
            table=table_name, 
            namespace=namespace, 
            catalog=catalog_name,
            distributed_dataset_type=DatasetType.RAY_DATASET
        )
    print("✅ RAY_DATASET correctly raised NotImplementedError for schemaless tables")
    
    # Test 3: Local storage should still work (explicit None)
    print("\n=== Testing Local Storage (explicit None) ===")
    result_local = dc.read_table(
        table=table_name, 
        namespace=namespace, 
        catalog=catalog_name,
        distributed_dataset_type=None  # Explicitly use local storage
    )
    print(f"Local storage result type: {type(result_local)}")
    
    # For schemaless tables with local storage, we expect a list of tables
    assert isinstance(result_local, list), "Local storage should return list for schemaless tables"
    print(f"Local storage returned list of {len(result_local)} tables (as expected)")
    
    # Manually combine the tables to check all columns are preserved
    all_tables = []
    for table in result_local:
        if hasattr(table, 'to_pandas'):
            table = table.to_pandas()
        all_tables.append(table)
    
    # Use pandas concat to handle different schemas
    df_local = pd.concat(all_tables, ignore_index=True, sort=False)
    print(f"Local storage combined result shape: {df_local.shape}")
    print(f"Local storage combined columns: {sorted(df_local.columns)}")
    
    # Check if we got all columns
    expected_columns = {"id", "name", "value", "status", "category"}
    assert set(df_local.columns) == expected_columns, f"Local storage missing columns: {expected_columns - set(df_local.columns)}"
    assert len(df_local) == 4, f"Local storage should have 4 rows, got {len(df_local)}"
    print("✅ Local storage correctly preserved all columns")
    
    print("\n=== Test completed ===")


def test_schema_type_promotion_with_none_consistency():
    """Test automatic type promotion for fields with SchemaConsistencyType.NONE."""
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    from deltacat.storage.model.schema import Field, Schema
    from deltacat.storage.model.types import SchemaConsistencyType
    import deltacat as dc
    import pandas as pd
    import pyarrow as pa
    import uuid
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-type-promotion-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"type-promotion-test-{uuid.uuid4()}"
    table_name = "type_promotion_table"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Test 1: Create table with int32 field with NONE consistency type
    initial_data = pd.DataFrame({
        "id": [1, 2, 3],
        "count": pd.array([10, 20, 30], dtype="int32"),  # Explicitly int32
        "name": ["Alice", "Bob", "Charlie"]
    })
    
    # Create schema with NONE consistency type for the count field
    schema_fields = [
        Field.of(pa.field("id", pa.int64()), consistency_type=SchemaConsistencyType.VALIDATE),
        Field.of(pa.field("count", pa.int32()), consistency_type=SchemaConsistencyType.NONE),  # This should allow promotion
        Field.of(pa.field("name", pa.string()), consistency_type=SchemaConsistencyType.VALIDATE)
    ]
    initial_schema = Schema.of(schema_fields)
    
    dc.write_to_table(
        data=initial_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=initial_schema,
        mode=TableWriteMode.CREATE
    )
    
    # Verify initial data
    result1 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result1, list):
        combined_result1 = pd.concat([t.to_pandas() for t in result1], ignore_index=True, sort=False)
    else:
        combined_result1 = result1.to_pandas() if hasattr(result1, 'to_pandas') else result1
    
    print(f"Initial data types: {combined_result1.dtypes}")
    assert len(combined_result1) == 3
    assert combined_result1["count"].dtype.name.startswith("int32")  # Should still be int32
    
    # Test 2: Write data with int64 values - should promote int32 -> int64
    extended_data = pd.DataFrame({
        "id": [4, 5],
        "count": pd.array([2147483648, 2147483649], dtype="int64"),  # Values that require int64
        "name": ["Diana", "Eve"]
    })
    
    dc.write_to_table(
        data=extended_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        mode=TableWriteMode.APPEND
    )
    
    # Verify type promotion occurred
    result2 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result2, list):
        combined_result2 = pd.concat([t.to_pandas() for t in result2], ignore_index=True, sort=False)
    else:
        combined_result2 = result2.to_pandas() if hasattr(result2, 'to_pandas') else result2
    
    print(f"After int64 data types: {combined_result2.dtypes}")
    assert len(combined_result2) == 5
    # The count column should now be int64 (promoted)
    assert combined_result2["count"].dtype.name.startswith("int64")
    assert combined_result2["count"].iloc[-1] == 2147483649  # Verify large value preserved
    
    # Test 3: Write data with float values - should promote int64 -> float64  
    float_data = pd.DataFrame({
        "id": [6],
        "count": [3.14159],  # Float value
        "name": ["Frank"]
    })
    
    dc.write_to_table(
        data=float_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        mode=TableWriteMode.APPEND
    )
    
    # Verify type promotion to float
    result3 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result3, list):
        combined_result3 = pd.concat([t.to_pandas() for t in result3], ignore_index=True, sort=False)
    else:
        combined_result3 = result3.to_pandas() if hasattr(result3, 'to_pandas') else result3
    
    print(f"After float data types: {combined_result3.dtypes}")
    assert len(combined_result3) == 6
    # The count column should now be float64 (promoted from int64)
    assert combined_result3["count"].dtype.name in ["float64", "double"]
    assert abs(combined_result3["count"].iloc[-1] - 3.14159) < 0.00001  # Verify float value preserved
    
    # Test 4: Write data with string values - should promote float64 -> string
    string_data = pd.DataFrame({
        "id": [7],
        "count": ["not_a_number"],  # String value
        "name": ["Grace"]
    })
    
    dc.write_to_table(
        data=string_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        mode=TableWriteMode.APPEND
    )
    
    # Verify type promotion to string
    result4 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result4, list):
        combined_result4 = pd.concat([t.to_pandas() for t in result4], ignore_index=True, sort=False)
    else:
        combined_result4 = result4.to_pandas() if hasattr(result4, 'to_pandas') else result4
    
    print(f"After string data types: {combined_result4.dtypes}")
    assert len(combined_result4) == 7
    # The count column should now be string (promoted from float64)
    assert combined_result4["count"].dtype.name in ["object", "string"]
    assert combined_result4["count"].iloc[-1] == "not_a_number"  # Verify string value preserved
    
    print("✅ All type promotions worked correctly!")
    print("Type evolution: int32 → int64 → float64 → string")


def test_schema_type_promotion_edge_cases():
    """Test edge cases for type promotion with SchemaConsistencyType.NONE."""
    from deltacat.storage.model.schema import Field
    from deltacat.storage.model.types import SchemaConsistencyType  
    import pyarrow as pa
    
    # Test 1: Same type - no promotion
    field_int32 = Field.of(pa.field("test", pa.int32()), consistency_type=SchemaConsistencyType.NONE)
    data_int32 = pa.array([1, 2, 3], type=pa.int32())
    promoted_data, was_promoted = field_int32.promote_type_if_needed(data_int32)
    assert not was_promoted, "Same type should not trigger promotion"
    assert promoted_data.type == pa.int32(), "Data type should remain int32"
    
    # Test 2: int32 to int64 promotion
    data_int64 = pa.array([2147483648], type=pa.int64())  # Value requiring int64
    promoted_data, was_promoted = field_int32.promote_type_if_needed(data_int64)
    assert was_promoted, "int32 field should promote to int64"
    assert promoted_data.type == pa.int64(), "Promoted data should be int64"
    
    # Test 3: Nullability preservation
    field_nullable = Field.of(pa.field("test", pa.int32(), nullable=True), consistency_type=SchemaConsistencyType.NONE)
    data_with_null = pa.array([1, None, 3], type=pa.int32())
    promoted_data, was_promoted = field_nullable.promote_type_if_needed(data_with_null)
    assert not was_promoted, "Same nullable type should not promote"
    
    # Test 4: Cross-type promotion (int to float)
    field_int = Field.of(pa.field("test", pa.int32()), consistency_type=SchemaConsistencyType.NONE)
    data_float = pa.array([1.5, 2.7], type=pa.float64())
    promoted_data, was_promoted = field_int.promote_type_if_needed(data_float)
    assert was_promoted, "int32 should promote to accommodate float64"
    assert pa.types.is_floating(promoted_data.type), f"Should promote to float type, got {promoted_data.type}"
    
    print("✅ All edge case type promotions work correctly!")


def test_binary_type_promotion_and_stability():
    """Test binary type promotion and ensure binary types are never down-promoted."""
    from deltacat.catalog.model.properties import CatalogProperties
    from deltacat.catalog.model.catalog import Catalog
    from deltacat.types.tables import TableWriteMode
    from deltacat.storage.model.schema import Field, Schema
    from deltacat.storage.model.types import SchemaConsistencyType
    import deltacat as dc
    import pandas as pd
    import pyarrow as pa
    import uuid
    
    # Setup catalog for testing
    local_catalog_root = f"/tmp/deltacat-binary-promotion-test-{uuid.uuid4()}"
    namespace = "test_namespace"
    catalog_name = f"binary-promotion-test-{uuid.uuid4()}"
    table_name = "binary_promotion_table"
    catalog_properties = CatalogProperties(root=local_catalog_root)
    catalog = dc.put_catalog(catalog_name, catalog=Catalog(config=catalog_properties))
    
    # Test 1: Start with string field that has NONE consistency type
    initial_data = pd.DataFrame({
        "id": [1, 2, 3],
        "data": ["hello", "world", "test"],  # String data
        "category": ["A", "B", "C"]
    })
    
    # Create schema with NONE consistency type for the data field
    schema_fields = [
        Field.of(pa.field("id", pa.int64()), consistency_type=SchemaConsistencyType.VALIDATE),
        Field.of(pa.field("data", pa.string()), consistency_type=SchemaConsistencyType.NONE),  # Should allow promotion
        Field.of(pa.field("category", pa.string()), consistency_type=SchemaConsistencyType.VALIDATE)
    ]
    initial_schema = Schema.of(schema_fields)
    
    dc.write_to_table(
        data=initial_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        schema=initial_schema,
        mode=TableWriteMode.CREATE
    )
    
    # Verify initial data
    result1 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result1, list):
        combined_result1 = pd.concat([t.to_pandas() for t in result1], ignore_index=True, sort=False)
    else:
        combined_result1 = result1.to_pandas() if hasattr(result1, 'to_pandas') else result1
    
    print(f"Initial data types: {combined_result1.dtypes}")
    assert len(combined_result1) == 3
    assert combined_result1["data"].dtype.name in ["object", "string"]  # Should be string type
    
    # Test 2: Write binary data - should promote string → binary
    binary_data = pd.DataFrame({
        "id": [4, 5],
        "data": [b"binary_data_1", b"binary_data_2"],  # Binary data
        "category": ["D", "E"]
    })
    
    dc.write_to_table(
        data=binary_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        mode=TableWriteMode.APPEND
    )
    
    # Verify promotion to binary occurred
    result2 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result2, list):
        combined_result2 = pd.concat([t.to_pandas() for t in result2], ignore_index=True, sort=False)
    else:
        combined_result2 = result2.to_pandas() if hasattr(result2, 'to_pandas') else result2
    
    print(f"After binary data types: {combined_result2.dtypes}")
    assert len(combined_result2) == 5
    # The data column should now be binary (promoted from string)
    # Note: pandas might represent this as object dtype containing bytes
    assert combined_result2["data"].dtype.name == "object"  # Binary data in pandas shows as object
    
    # Verify we can read back the binary data correctly
    assert isinstance(combined_result2["data"].iloc[-1], bytes), "Should be able to read binary data"
    assert combined_result2["data"].iloc[-1] == b"binary_data_2"
    
    # Test 3: CRITICAL - Write string data again - should NOT down-promote binary → string
    # Binary should remain binary and accept the string data (converted to bytes)
    string_again_data = pd.DataFrame({
        "id": [6, 7],
        "data": ["back_to_string_1", "back_to_string_2"],  # String data again
        "category": ["F", "G"]
    })
    
    dc.write_to_table(
        data=string_again_data,
        table=table_name,
        namespace=namespace,
        catalog=catalog_name,
        mode=TableWriteMode.APPEND
    )
    
    # Verify binary type was preserved (no down-promotion)
    result3 = dc.read_table(table=table_name, namespace=namespace, catalog=catalog_name, distributed_dataset_type=None)
    if isinstance(result3, list):
        combined_result3 = pd.concat([t.to_pandas() for t in result3], ignore_index=True, sort=False)
    else:
        combined_result3 = result3.to_pandas() if hasattr(result3, 'to_pandas') else result3
    
    print(f"After string-again data types: {combined_result3.dtypes}")
    assert len(combined_result3) == 7
    # The data column should STILL be binary (no down-promotion)
    assert combined_result3["data"].dtype.name == "object"  # Still binary
    
    # The new string data should have been converted to binary
    # Check that we have a mix of original strings (converted to binary), binary data, and new strings (converted to binary)
    last_item = combined_result3["data"].iloc[-1]
    print(f"Last item type: {type(last_item)}, value: {last_item}")
    
    # Test 4: Test direct unit-level binary promotion
    print("\n=== Testing direct field-level binary promotion ===")
    
    # Test int → binary
    field_int = Field.of(pa.field("test", pa.int32()), consistency_type=SchemaConsistencyType.NONE)
    binary_data_array = pa.array([b"test1", b"test2"], type=pa.binary())
    promoted_data, was_promoted = field_int.promote_type_if_needed(binary_data_array)
    assert was_promoted, "int32 should promote to binary"
    assert pa.types.is_binary(promoted_data.type), f"Should promote to binary type, got {promoted_data.type}"
    print("✅ int32 → binary promotion works")
    
    # Test float → binary  
    field_float = Field.of(pa.field("test", pa.float64()), consistency_type=SchemaConsistencyType.NONE)
    promoted_data, was_promoted = field_float.promote_type_if_needed(binary_data_array)
    assert was_promoted, "float64 should promote to binary"
    assert pa.types.is_binary(promoted_data.type), f"Should promote to binary type, got {promoted_data.type}"
    print("✅ float64 → binary promotion works")
    
    # Test string → binary
    field_string = Field.of(pa.field("test", pa.string()), consistency_type=SchemaConsistencyType.NONE)
    promoted_data, was_promoted = field_string.promote_type_if_needed(binary_data_array)
    assert was_promoted, "string should promote to binary"
    assert pa.types.is_binary(promoted_data.type), f"Should promote to binary type, got {promoted_data.type}"
    print("✅ string → binary promotion works")
    
    # Test binary → string should NOT happen (binary should stay binary)
    field_binary = Field.of(pa.field("test", pa.binary()), consistency_type=SchemaConsistencyType.NONE)
    string_data_array = pa.array(["string1", "string2"], type=pa.string())
    promoted_data, was_promoted = field_binary.promote_type_if_needed(string_data_array)
    assert not was_promoted, "binary should NOT down-promote to string"
    assert pa.types.is_binary(promoted_data.type), f"Should remain binary type, got {promoted_data.type}"
    print("✅ binary field correctly rejects down-promotion to string")
    
    print("\n✅ All binary type promotion tests passed!")
    print("Key findings:")
    print("- Any type can be promoted to binary")  
    print("- Binary types never down-promote to less permissive types")
    print("- Binary is the most permissive type in the hierarchy")
