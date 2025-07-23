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
from unittest.mock import patch, MagicMock

import ray
from deltacat import Catalog
from deltacat.catalog import CatalogProperties
from deltacat.tests.test_utils.pyarrow import (
    create_delta_from_csv_file,
    commit_delta_to_partition,
)
from deltacat.types.media import DistributedDatasetType, ContentType
from deltacat.storage.model.types import DeltaType
from deltacat.storage import metastore
from deltacat.storage.model.schema import Schema, Field
from deltacat.storage.model.table import TableProperties
from deltacat.types.tables import TableWriteMode, TableProperty, TableReadOptimizationLevel, TablePropertyDefaultValues
from deltacat.exceptions import DeltaCatError, ValidationError
import deltacat as dc


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
            distributed_dataset_type=DistributedDatasetType.DAFT,
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
            distributed_dataset_type=DistributedDatasetType.DAFT,
            merge_on_read=False,
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
            distributed_dataset_type=DistributedDatasetType.DAFT,
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
            distributed_dataset_type=DistributedDatasetType.DAFT,
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
            distributed_dataset_type=DistributedDatasetType.DAFT,
        )
        
        
        # Expected: 9 total unique records after all merges
        result_count = result.count_rows() if hasattr(result, 'count_rows') else len(result)
        assert result_count == 9, f"Expected 9 records after all merges, got {result_count}"
        
        # Verify complex merge behavior:
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
        
        # Very aggressive triggers to ensure compaction happens on every write
        table_properties: TableProperties = {
            TableProperty.READ_OPTIMIZATION_LEVEL: TableReadOptimizationLevel.MAX,
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
        has_conflict_message = "Previous partition ID mismatch" in full_error_context
        assert has_conflict_message, f"Expected 'Previous partition ID mismatch' error message, got: {failed_exception} (cause: {cause_message})"
        
        # Verify final table state is consistent
        final_result = dc.read_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
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
            distributed_dataset_type=DistributedDatasetType.DAFT,
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
        assert conflict_rate < 0.99, f"Too many conflicts ({conflict_rate:.1%}) - system may be broken"
