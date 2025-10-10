import pytest
import time
import os
import threading
from typing import Any, Dict
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType
from pyiceberg.io.pyarrow import (
    schema_to_pyarrow,
    data_file_statistics_from_parquet_metadata,
    compute_statistics_plan,
    parquet_path_to_id_mapping,
    _check_pyarrow_schema_compatible,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from deltacat.tests.test_utils.filesystem import temp_dir_autocleanup
from deltacat.experimental.converter_agent.table_monitor import (
    CallbackContext,
    CallbackResult,
    CallbackStage,
    _resolve_callback,
    _invoke_callback,
    monitor_table,
    FilesystemType,
)
from deltacat.constants import NANOS_PER_SEC


# Helper callback functions for use in tests
def _simple_test_callback(context: CallbackContext) -> Dict[str, Any]:
    """Simple test callback that returns status."""
    return {"status": "success", "callback": "simple", "stage": context.stage}


def _failing_test_callback(context: CallbackContext) -> Dict[str, Any]:
    """Test callback that raises an exception."""
    raise ValueError("Intentional test failure")


class TestCallbackResolution:
    """Test class for callback resolution functionality."""

    def test_resolve_direct_callable(self):
        """Test resolving a direct callable function."""
        resolved = _resolve_callback(_simple_test_callback)
        assert resolved is _simple_test_callback
        assert callable(resolved)

    def test_resolve_none(self):
        """Test resolving None returns None."""
        resolved = _resolve_callback(None)
        assert resolved is None

    def test_resolve_string_spec_valid(self):
        """Test resolving a valid string specification."""
        callback_spec = (
            "deltacat.tests.experimental.converter_agent.test_table_monitor:"
            "_simple_test_callback"
        )
        resolved = _resolve_callback(callback_spec)
        assert callable(resolved)
        assert resolved.__name__ == "_simple_test_callback"

    def test_resolve_string_spec_invalid_format(self):
        """Test that invalid string format raises ValueError."""
        with pytest.raises(ValueError, match="must be in 'module:function' format"):
            _resolve_callback("invalid_format_no_colon")

    def test_resolve_string_spec_nonexistent_module(self):
        """Test that non-existent module raises ImportError."""
        with pytest.raises(ImportError, match="Failed to import module"):
            _resolve_callback("nonexistent_module_xyz:some_function")

    def test_resolve_string_spec_nonexistent_function(self):
        """Test that non-existent function raises AttributeError."""
        with pytest.raises(AttributeError, match="not found in module"):
            _resolve_callback(
                "deltacat.tests.experimental.converter_agent.test_table_monitor:"
                "nonexistent_function_xyz"
            )

    def test_resolve_invalid_type(self):
        """Test that invalid callback type raises TypeError."""
        with pytest.raises(TypeError, match="Invalid callback type"):
            _resolve_callback(12345)  # Invalid type


class TestCallbackInvocation:
    """Test class for callback invocation functionality."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.test_context_pre = CallbackContext.of(
            {
                "last_write_time": time.time_ns(),
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_namespace",
                "table_name": "test_table",
                "merge_keys": ["id", "timestamp"],
                "max_converter_parallelism": 4,
                "stage": CallbackStage.PRE.value,
            }
        )

    def test_invoke_callback_success(self):
        """Test successful callback invocation."""
        result_holder = []

        def capture_callback(context: CallbackContext) -> Dict[str, Any]:
            result_holder.append(context.stage)
            return {"status": "captured"}

        _invoke_callback(capture_callback, self.test_context_pre)
        assert len(result_holder) == 1
        assert result_holder[0] == CallbackStage.PRE.value

    def test_invoke_callback_with_none(self):
        """Test that invoking None callback does nothing."""
        # Should not raise any exception
        _invoke_callback(None, self.test_context_pre)

    def test_invoke_callback_handles_exception(self):
        """Test that callback exceptions are handled gracefully."""
        # Should not raise exception even though callback fails
        _invoke_callback(_failing_test_callback, self.test_context_pre)


class TestCallbackContext:
    """Test class for CallbackContext functionality."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.base_time_ns = 1696800000 * int(1e9)
        self.context = CallbackContext.of(
            {
                "last_write_time": self.base_time_ns,
                "conversion_start_time": self.base_time_ns,
                "conversion_end_time": self.base_time_ns + int(10.5 * 1e9),
                "last_snapshot_id": "snap-001",
                "snapshot_id": "snap-002",
                "catalog_uri": "http://localhost:8181",
                "namespace": "test_ns",
                "table_name": "test_tbl",
                "merge_keys": ["id", "ts"],
                "max_converter_parallelism": 4,
                "stage": CallbackStage.POST.value,
            }
        )

    def test_context_is_dict(self):
        """Test that CallbackContext behaves as a dictionary."""
        assert isinstance(self.context, dict)
        assert "table_name" in self.context
        assert "stage" in self.context
        assert len(self.context) == 11

    def test_context_dictionary_access(self):
        """Test dictionary-style access to context fields."""
        assert self.context["table_name"] == "test_tbl"
        assert self.context["namespace"] == "test_ns"
        assert self.context["merge_keys"] == ["id", "ts"]
        assert self.context["stage"] == CallbackStage.POST.value

    def test_context_property_access(self):
        """Test property-style access to context fields."""
        assert self.context.table_name == "test_tbl"
        assert self.context.namespace == "test_ns"
        assert self.context.merge_keys == ["id", "ts"]
        assert self.context.max_converter_parallelism == 4
        assert self.context.catalog_uri == "http://localhost:8181"
        assert self.context.snapshot_id == "snap-002"
        assert self.context.last_snapshot_id == "snap-001"
        assert self.context.stage == CallbackStage.POST.value

    def test_context_timestamps(self):
        """Test timestamp properties return correct values."""
        assert self.context.last_write_time == self.base_time_ns
        assert self.context.conversion_start_time == self.base_time_ns
        assert self.context.conversion_end_time == self.base_time_ns + int(10.5 * 1e9)

    def test_context_manual_duration_calculation(self):
        """Test manual duration calculation from nanosecond timestamps."""
        start = self.context.conversion_start_time
        end = self.context.conversion_end_time
        duration = (end - start) / NANOS_PER_SEC if start and end else None
        assert duration is not None
        assert abs(duration - 10.5) < 0.001  # Should be 10.5 seconds

    def test_context_optional_fields(self):
        """Test that optional fields return None when not present."""
        context_no_times = CallbackContext.of(
            {
                "last_write_time": self.base_time_ns,
                "snapshot_id": "snap-002",
                "namespace": "test_ns",
                "table_name": "test_tbl",
                "merge_keys": ["id"],
                "max_converter_parallelism": 2,
                "stage": CallbackStage.PRE.value,
            }
        )
        assert context_no_times.conversion_start_time is None
        assert context_no_times.conversion_end_time is None
        assert context_no_times.last_snapshot_id is None
        assert context_no_times.catalog_uri is None

    def test_context_of_factory_method(self):
        """Test CallbackContext.of() factory method."""
        data = {
            "namespace": "ns",
            "table_name": "table",
            "snapshot_id": "snap",
            "merge_keys": ["key"],
            "max_converter_parallelism": 1,
            "last_write_time": 12345,
            "stage": "pre",
        }
        context = CallbackContext.of(data)
        assert isinstance(context, CallbackContext)
        assert context.namespace == "ns"
        assert context.table_name == "table"


class TestCallbackResult:
    """Test class for CallbackResult functionality."""

    def test_callback_result_basic(self):
        """Test basic CallbackResult creation and access."""
        from deltacat.experimental.converter_agent.table_monitor import CallbackResult

        result = CallbackResult.of({"status": "success", "messageId": "test-123"})

        assert isinstance(result, CallbackResult)
        assert result.status == "success"
        assert result["messageId"] == "test-123"

    def test_callback_result_with_reason(self):
        """Test CallbackResult with optional reason field."""
        from deltacat.experimental.converter_agent.table_monitor import CallbackResult

        result = CallbackResult.of({"status": "error", "reason": "Connection timeout"})

        assert result.status == "error"
        assert result.reason == "Connection timeout"

    def test_callback_result_properties_empty(self):
        """Test that properties returns empty dict when not set."""
        from deltacat.experimental.converter_agent.table_monitor import CallbackResult

        result = CallbackResult.of({"status": "success"})
        assert result.properties == {}
        assert isinstance(result.properties, dict)

    def test_callback_result_properties_access(self):
        """Test accessing and setting properties."""
        from deltacat.experimental.converter_agent.table_monitor import CallbackResult

        result = CallbackResult.of(
            {
                "status": "success",
                "properties": {
                    "custom_field": "custom_value",
                    "metadata": {"key": "value"},
                },
            }
        )

        assert result.properties["custom_field"] == "custom_value"
        assert result.properties["metadata"]["key"] == "value"

    def test_callback_result_properties_mutation(self):
        """Test that properties can be mutated."""
        from deltacat.experimental.converter_agent.table_monitor import CallbackResult

        result = CallbackResult.of({"status": "success"})

        # Add properties dynamically
        result.properties["user_data"] = "test"
        result.properties["count"] = 42

        assert result.properties["user_data"] == "test"
        assert result.properties["count"] == 42
        assert result["properties"]["user_data"] == "test"


class TestCallbackStage:
    """Test class for CallbackStage enum."""

    def test_stage_values(self):
        """Test that CallbackStage has correct values."""
        assert CallbackStage.PRE.value == "pre"
        assert CallbackStage.POST.value == "post"

    def test_stage_is_string(self):
        """Test that CallbackStage values are strings."""
        assert isinstance(CallbackStage.PRE.value, str)
        assert isinstance(CallbackStage.POST.value, str)

    def test_stage_in_context(self):
        """Test using CallbackStage in context."""
        context = CallbackContext.of(
            {
                "namespace": "ns",
                "table_name": "table",
                "snapshot_id": "snap",
                "merge_keys": [],
                "max_converter_parallelism": 1,
                "last_write_time": 0,
                "stage": CallbackStage.PRE.value,
            }
        )
        assert context.stage == "pre"
        assert context["stage"] == CallbackStage.PRE.value


class TestTableMonitorEndToEnd:
    """End-to-end integration test for table monitor."""

    def test_table_monitor_with_shared_catalog(self, setup_ray_cluster):
        """
        Test that table monitor automatically detects and converts data using a shared catalog.

        This test resolves the catalog sharing issue by passing the catalog instance
        directly to the monitor, ensuring both test and monitor see the same metadata.
        """
        # Track callback invocations
        callback_results = []
        conversion_complete = threading.Event()

        # Mock callback that tracks invocations
        def mock_completion_callback(context: CallbackContext) -> CallbackResult:
            """Mock callback that tracks invocations and signals completion."""
            result = CallbackResult.of(
                {
                    "status": "success",
                    "stage": context.stage,
                    "snapshot_id": context.snapshot_id,
                }
            )
            callback_results.append(result)

            # Signal completion on POST conversion
            if context.stage == CallbackStage.POST.value:
                conversion_complete.set()

            return result

        with temp_dir_autocleanup() as temp_catalog_dir:
            # Create warehouse directory
            warehouse_path = os.path.join(temp_catalog_dir, "iceberg_warehouse")
            os.makedirs(warehouse_path, exist_ok=True)

            # Set up local SQL catalog with file-based database
            # File-based database needed for catalog state persistence
            catalog_db_path = os.path.join(temp_catalog_dir, "catalog.db")
            catalog_uri = f"sqlite:///{catalog_db_path}"

            local_catalog = load_catalog(
                "local_sql_catalog",
                **{
                    "type": "sql",
                    "uri": catalog_uri,
                    "warehouse": warehouse_path,
                },
            )

            # Define schema
            schema = Schema(
                NestedField(
                    field_id=1, name="id", field_type=LongType(), required=True
                ),
                NestedField(
                    field_id=2, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="value", field_type=LongType(), required=False
                ),
                NestedField(
                    field_id=4, name="version", field_type=LongType(), required=False
                ),
                schema_id=0,
            )

            # Create table properties
            properties = {
                "write.format.default": "parquet",
                "write.delete.mode": "merge-on-read",
                "write.update.mode": "merge-on-read",
                "write.merge.mode": "merge-on-read",
                "format-version": "2",
            }

            # Create the table
            table_identifier = "default.test_monitor_shared"
            try:
                local_catalog.create_namespace("default")
            except NamespaceAlreadyExistsError:
                pass
            try:
                local_catalog.drop_table(table_identifier)
            except NoSuchTableError:
                pass

            local_catalog.create_table(
                table_identifier,
                schema=schema,
                properties=properties,
            )
            tbl = local_catalog.load_table(table_identifier)

            # Set the name mapping property
            with tbl.transaction() as tx:
                tx.set_properties(
                    **{
                        "schema.name-mapping.default": schema.name_mapping.model_dump_json()
                    }
                )

            tbl.refresh()

            # Start the table monitor with SHARED catalog instance
            monitor_interval = 0.5
            # Use very high timeout - we'll control lifecycle explicitly via Ray shutdown
            ray_timeout = 3600  # 1 hour - effectively disabled

            def monitor_wrapper():
                """Wrapper to catch exceptions in monitor thread."""
                try:
                    monitor_table(
                        catalog_type="sql",  # Ignored when catalog is provided
                        warehouse_path=warehouse_path,
                        catalog_uri=catalog_uri,  # Ignored when catalog is provided
                        namespace="default",
                        table_name="test_monitor_shared",
                        merge_keys=["id"],
                        filesystem_type=FilesystemType.LOCAL,
                        monitor_interval=monitor_interval,
                        max_converter_parallelism=1,
                        ray_inactivity_timeout=ray_timeout,
                        pre_conversion_callback=mock_completion_callback,
                        post_conversion_callback=mock_completion_callback,
                        catalog=local_catalog,  # SHARE the catalog!
                    )
                except Exception:
                    import traceback

                    traceback.print_exc()

            # Use a daemon thread - it won't block test exit
            # We'll explicitly shutdown Ray in finally block to stop the monitor
            monitor_thread = threading.Thread(target=monitor_wrapper, daemon=True)
            monitor_thread.start()

            try:
                # Give monitor time to start
                time.sleep(monitor_interval + 0.2)

                # Create PyArrow schema
                arrow_schema = schema_to_pyarrow(schema)

                # Write initial data
                initial_data = pa.table(
                    {
                        "id": [1, 2, 3, 4],
                        "name": ["Alice", "Bob", "Charlie", "David"],
                        "value": [100, 200, 300, 400],
                        "version": [1, 1, 1, 1],
                    },
                    schema=arrow_schema,
                )

                # Write updates (creates duplicates)
                updated_data = pa.table(
                    {
                        "id": [2, 3, 5],
                        "name": ["Robert", "Charles", "Eve"],
                        "value": [201, 301, 500],
                        "version": [2, 2, 1],
                    },
                    schema=arrow_schema,
                )

                # Write and commit data files
                data_files_to_commit = []
                for i, data in enumerate([initial_data, updated_data]):
                    data_file_path = os.path.join(warehouse_path, f"data_{i}.parquet")
                    pq.write_table(data, data_file_path)

                    parquet_metadata = pq.read_metadata(data_file_path)
                    file_size = os.path.getsize(data_file_path)

                    _check_pyarrow_schema_compatible(
                        schema, parquet_metadata.schema.to_arrow_schema()
                    )

                    statistics = data_file_statistics_from_parquet_metadata(
                        parquet_metadata=parquet_metadata,
                        stats_columns=compute_statistics_plan(
                            schema, tbl.metadata.properties
                        ),
                        parquet_column_mapping=parquet_path_to_id_mapping(schema),
                    )

                    data_file = DataFile(
                        content=DataFileContent.DATA,
                        file_path=data_file_path,
                        file_format=FileFormat.PARQUET,
                        partition={},
                        file_size_in_bytes=file_size,
                        sort_order_id=None,
                        spec_id=tbl.metadata.default_spec_id,
                        key_metadata=None,
                        equality_ids=None,
                        **statistics.to_serialized_dict(),
                    )
                    data_files_to_commit.append(data_file)

                # Commit data to trigger monitor detection
                with tbl.transaction() as tx:
                    with tx.update_snapshot().fast_append() as update_snapshot:
                        for data_file in data_files_to_commit:
                            update_snapshot.append_data_file(data_file)

                tbl.refresh()

                # Verify duplicates exist
                initial_scan = tbl.scan().to_arrow().to_pydict()
                expected_duplicate_ids = [1, 2, 2, 3, 3, 4, 5]
                assert sorted(initial_scan["id"]) == expected_duplicate_ids

                # Wait for monitor to detect and convert
                timeout = 15.0
                success = conversion_complete.wait(timeout=timeout)

                assert success, (
                    f"Monitor did not complete conversion within {timeout}s. "
                    f"Callbacks: {len(callback_results)}, Stages: {[r['stage'] for r in callback_results]}"
                )

                # Verify callbacks
                assert len(callback_results) >= 2
                stages = [r["stage"] for r in callback_results]
                assert CallbackStage.PRE.value in stages
                assert CallbackStage.POST.value in stages

                # Verify conversion results
                tbl.refresh()
                final_scan = tbl.scan().to_arrow().to_pydict()
                expected_unique_ids = [1, 2, 3, 4, 5]
                actual_ids = sorted(final_scan["id"])

                assert (
                    actual_ids == expected_unique_ids
                ), f"Expected {expected_unique_ids}, got {actual_ids}"

                # Verify position deletes were created
                latest_snapshot = tbl.metadata.current_snapshot()
                assert latest_snapshot is not None

                manifests = latest_snapshot.manifests(tbl.io)
                position_delete_files = []
                for manifest in manifests:
                    entries = manifest.fetch_manifest_entry(tbl.io)
                    for entry in entries:
                        if entry.data_file.content == DataFileContent.POSITION_DELETES:
                            position_delete_files.append(entry.data_file.file_path)

                assert len(position_delete_files) > 0, "No position delete files created"

                # Verify updated values
                final_data_by_id = {}
                for i, id_val in enumerate(final_scan["id"]):
                    final_data_by_id[id_val] = {
                        "name": final_scan["name"][i],
                        "value": final_scan["value"][i],
                        "version": final_scan["version"][i],
                    }

                assert final_data_by_id[2]["name"] == "Robert"
                assert final_data_by_id[2]["value"] == 201
                assert final_data_by_id[3]["name"] == "Charles"
                assert final_data_by_id[3]["value"] == 301

            finally:
                # Explicitly shutdown Ray to stop the monitor thread and ensure clean state
                # The monitor loop checks ray.is_initialized(), so shutting down Ray will
                # cause the monitor thread to exit gracefully
                import ray
                if ray.is_initialized():
                    ray.shutdown()

                # Give the daemon thread a moment to exit after Ray shutdown
                monitor_thread.join(timeout=2)
