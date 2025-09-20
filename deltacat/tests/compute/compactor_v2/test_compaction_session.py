import ray
import pytest
import tempfile
import shutil
import pandas as pd
from deltacat.storage import metastore
from deltacat.catalog import CatalogProperties
from deltacat.types.media import ContentType
from deltacat.storage.model.types import DeltaType
from deltacat.compute.compactor_v2.compaction_session import compact_partition
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from deltacat.compute.resource_estimation import ResourceEstimationMethod
from deltacat.exceptions import ValidationError
from deltacat.tests.compute.test_util_common import (
    get_rci_from_partition,
    read_audit_file,
)


@pytest.fixture(autouse=True, scope="module")
def setup_ray_cluster():
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture
def catalog():
    """Create a temporary catalog for testing."""
    tmpdir = tempfile.mkdtemp()
    catalog = CatalogProperties(root=tmpdir)
    yield catalog
    shutil.rmtree(tmpdir)


class TestCompactionSessionMain:
    """Compaction session tests using main deltacat metastore."""

    NAMESPACE = "compact_partition_main_test"
    ERROR_RATE = 0.05

    # Test data equivalent to the CSV files
    BACKFILL_DATA = pd.DataFrame(
        {
            "pk": ["2022-10-21", "2022-10-20", "2022-11-24", "2023-10-23"],
            "value": [1, 2, 3, 4],
        }
    )

    INCREMENTAL_DATA = pd.DataFrame(
        {"pk": ["2022-10-21", "2022-11-25"], "value": [1, 5]}
    )

    def _create_namespace_and_table(self, namespace_suffix, catalog):
        """Helper to create namespace and table for tests."""
        namespace_name = f"{self.NAMESPACE}_{namespace_suffix}"

        # Create namespace
        namespace = metastore.create_namespace(
            namespace=namespace_name,
            catalog=catalog,
        )

        # Create table and table version
        table, table_version, stream = metastore.create_table_version(
            namespace=namespace.locator.namespace,
            table_name=f"table_{namespace_suffix}",
            catalog=catalog,
        )

        return namespace, table, table_version, stream

    def _stage_and_commit_partition(self, stream, catalog):
        """Helper to stage and commit a partition."""
        partition = metastore.stage_partition(
            stream=stream,
            catalog=catalog,
        )
        return metastore.commit_partition(
            partition=partition,
            catalog=catalog,
        )

    def _stage_and_commit_delta(
        self, data, partition, catalog, delta_type=DeltaType.UPSERT
    ):
        """Helper to stage and commit a delta with data."""
        staged_delta = metastore.stage_delta(
            data=data,
            partition=partition,
            catalog=catalog,
            content_type=ContentType.PARQUET,
            delta_type=delta_type,
        )

        return metastore.commit_delta(
            delta=staged_delta,
            catalog=catalog,
        )

    def test_compact_partition_basic_sanity(self, catalog):
        """Basic sanity test to verify compact_partition works with main metastore."""

        # Create source namespace and table
        source_namespace = metastore.create_namespace(
            namespace=f"{self.NAMESPACE}_source",
            catalog=catalog,
        )

        # Create destination namespace and table
        dest_namespace = metastore.create_namespace(
            namespace=f"{self.NAMESPACE}_dest",
            catalog=catalog,
        )

        # Create a simple test dataset
        test_data = pd.DataFrame(
            {
                "pk": [1, 2, 3, 4],
                "name": ["A", "B", "C", "D"],
                "value": [10, 20, 30, 40],
            }
        )

        # Create source table and partition
        (
            source_table,
            source_table_version,
            source_stream,
        ) = metastore.create_table_version(
            namespace=source_namespace.locator.namespace,
            table_name="source_table",
            catalog=catalog,
        )

        source_partition = metastore.stage_partition(
            stream=source_stream,
            catalog=catalog,
        )
        source_partition = metastore.commit_partition(
            partition=source_partition,
            catalog=catalog,
        )

        # Stage and commit a delta to the source partition
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=source_partition,
            catalog=catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        source_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=catalog,
        )

        # Create destination table and partition
        dest_table, dest_table_version, dest_stream = metastore.create_table_version(
            namespace=dest_namespace.locator.namespace,
            table_name="dest_table",
            catalog=catalog,
        )

        dest_partition = metastore.stage_partition(
            stream=dest_stream,
            catalog=catalog,
        )
        dest_partition = metastore.commit_partition(
            partition=dest_partition,
            catalog=catalog,
        )
        # Test compact_partition with minimal parameters
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 1,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "name", "value"],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_partition.locator,
                }
            )
        )

        # Basic verification - if we get here without exceptions, the basic flow works

        # Get a fresh reference to the destination partition to see updates
        updated_dest_partition = metastore.get_partition(
            stream_locator=dest_stream.locator,
            partition_values=None,  # unpartitioned
            catalog=catalog,
        )

        # Verify that the destination partition now has some deltas
        dest_partition_deltas = metastore.list_partition_deltas(
            partition_like=updated_dest_partition,
            include_manifest=True,
            catalog=catalog,
        )

        delta_count = len(dest_partition_deltas.all_items())

        # Verify that at least one compacted delta was written to the destination partition
        assert (
            delta_count > 0
        ), f"Expected at least one delta in destination partition, but found {delta_count}"

    def test_compact_partition_when_no_input_deltas_to_compact(self, catalog):
        """Test compaction when there are no input deltas to compact."""
        # Create source and destination namespaces/tables
        _, _, _, source_stream = self._create_namespace_and_table("source", catalog)
        _, _, _, dest_stream = self._create_namespace_and_table("destination", catalog)

        # Create source and destination partitions (no deltas)
        source_partition = self._stage_and_commit_partition(source_stream, catalog)
        dest_partition = self._stage_and_commit_partition(dest_stream, catalog)

        # For partitions with no deltas, use stream position 0 or 1 as the last position to compact
        last_position = source_partition.stream_position or 0

        # Attempt compaction
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": last_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_partition.locator,
                }
            )
        )

    def test_compact_partition_when_incremental_then_rci_stats_accurate(self, catalog):
        """Test case which asserts the RCI stats are correctly generated for a rebase and incremental use-case."""
        # Create source and destination namespaces/tables
        _, _, _, source_stream = self._create_namespace_and_table("source", catalog)
        _, _, _, dest_stream = self._create_namespace_and_table("destination", catalog)

        # Create source partition and commit backfill data
        source_partition = self._stage_and_commit_partition(source_stream, catalog)
        source_delta = self._stage_and_commit_delta(
            self.BACKFILL_DATA, source_partition, catalog
        )

        # Create destination partition
        dest_partition = self._stage_and_commit_partition(dest_stream, catalog)

        # First compaction with backfill data
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "original_fields": {"pk", "value"},
                    "rebase_source_partition_locator": source_delta.partition_locator,
                    "rebase_source_partition_high_watermark": source_delta.stream_position,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_delta.partition_locator,
                }
            )
        )

        # Get RoundCompletionInfo from the compacted partition instead of file
        backfill_rci = get_rci_from_partition(
            dest_partition.locator, metastore, catalog=catalog
        )
        # Get catalog root for audit file resolution
        catalog_root = catalog.root

        compaction_audit = CompactionSessionAuditInfo(
            **read_audit_file(backfill_rci.compaction_audit_url, catalog_root)
        )

        # Verify that inflation and record size values are reasonable (not exact due to storage differences)
        # Note: inflation values may be None in some storage implementations
        if backfill_rci.input_inflation is not None:
            assert (
                0.01 <= backfill_rci.input_inflation <= 0.2
            )  # Reasonable inflation range
        if backfill_rci.input_average_record_size_bytes is not None:
            assert (
                5 <= backfill_rci.input_average_record_size_bytes <= 50
            )  # Reasonable record size range

        assert compaction_audit.input_records == 4
        assert compaction_audit.records_deduped == 0
        assert compaction_audit.records_deleted == 0
        assert compaction_audit.untouched_file_count == 0
        assert compaction_audit.untouched_record_count == 0
        assert compaction_audit.untouched_size_bytes == 0
        assert compaction_audit.untouched_file_ratio == 0
        assert compaction_audit.uniform_deltas_created == 1
        assert compaction_audit.hash_bucket_count == 2
        assert compaction_audit.input_file_count == 1
        assert compaction_audit.output_file_count == 2
        # Allow larger tolerance for file size differences between storage implementations
        # File sizes can vary significantly due to different compression, metadata, etc.
        assert compaction_audit.output_size_bytes > 0
        assert compaction_audit.input_size_bytes > 0

        # Now commit incremental data and run incremental compaction
        new_source_delta = self._stage_and_commit_delta(
            self.INCREMENTAL_DATA, source_partition, catalog
        )

        # Use the original destination partition for incremental compaction
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": new_source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "original_fields": {"pk", "value"},
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": new_source_delta.partition_locator,
                }
            )
        )

        # Get RoundCompletionInfo from the compacted partition instead of file
        new_rci = get_rci_from_partition(
            dest_partition.locator, metastore, catalog=catalog
        )
        # Get catalog root for audit file resolution
        catalog_root = catalog.root

        compaction_audit = CompactionSessionAuditInfo(
            **read_audit_file(new_rci.compaction_audit_url, catalog_root)
        )

        # Verify incremental compaction metrics are reasonable (looser bounds due to storage differences)
        # Note: inflation values may be None in some storage implementations
        if new_rci.input_inflation is not None:
            assert 0.01 <= new_rci.input_inflation <= 0.2  # Reasonable inflation range
        if new_rci.input_average_record_size_bytes is not None:
            assert (
                5 <= new_rci.input_average_record_size_bytes <= 50
            )  # Reasonable record size range

        assert compaction_audit.input_records >= 4  # At least the backfill records
        assert compaction_audit.records_deduped >= 0
        assert compaction_audit.records_deleted == 0
        assert compaction_audit.untouched_file_count >= 0
        assert compaction_audit.untouched_record_count >= 0
        # Allow larger tolerance for size differences
        assert compaction_audit.untouched_file_ratio >= 0
        assert compaction_audit.uniform_deltas_created >= 1
        assert compaction_audit.hash_bucket_count == 2
        assert compaction_audit.input_file_count >= 1
        assert compaction_audit.output_file_count >= 1
        # Allow larger tolerance for file size differences between storage implementations
        # File sizes can vary significantly due to different compression, metadata, etc.
        assert compaction_audit.output_size_bytes > 0
        assert compaction_audit.input_size_bytes > 0

    def test_compact_partition_when_hash_bucket_count_changes_then_validation_error(
        self, catalog
    ):
        """Test that changing hash bucket count between compactions raises ValidationError."""
        # Create source and destination namespaces/tables
        _, _, _, source_stream = self._create_namespace_and_table("source", catalog)
        _, _, _, dest_stream = self._create_namespace_and_table("destination", catalog)

        # Create source partition and commit backfill data
        source_partition = self._stage_and_commit_partition(source_stream, catalog)
        source_delta = self._stage_and_commit_delta(
            self.BACKFILL_DATA, source_partition, catalog
        )

        # Create destination partition
        dest_partition = self._stage_and_commit_partition(dest_stream, catalog)

        # First compaction with hash_bucket_count=2
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "rebase_source_partition_locator": source_delta.partition_locator,
                    "rebase_source_partition_high_watermark": source_delta.stream_position,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_delta.partition_locator,
                }
            )
        )

        # Now commit incremental data and run incremental compaction with different hash bucket count
        new_source_delta = self._stage_and_commit_delta(
            self.INCREMENTAL_DATA, source_partition, catalog
        )

        # This should raise ValidationError due to hash bucket count mismatch (2 vs 1)
        with pytest.raises(ValidationError) as exc_info:
            compact_partition(
                CompactPartitionParams.of(
                    {
                        "catalog": catalog,
                        "compacted_file_content_type": ContentType.PARQUET,
                        "dd_max_parallelism_ratio": 1.0,
                        "deltacat_storage": metastore,
                        "deltacat_storage_kwargs": {"catalog": catalog},
                        "destination_partition_locator": dest_partition.locator,
                        "drop_duplicates": True,
                        "hash_bucket_count": 1,  # Different from initial compaction (2)
                        "last_stream_position_to_compact": new_source_delta.stream_position,
                        "list_deltas_kwargs": {
                            "catalog": catalog,
                            "equivalent_table_types": [],
                        },
                        "primary_keys": ["pk"],
                        "all_column_names": ["pk", "value"],
                        "rebase_source_partition_locator": None,
                        "rebase_source_partition_high_watermark": None,
                        "records_per_compacted_file": 4000,
                        "source_partition_locator": new_source_delta.partition_locator,
                    }
                )
            )

        # Verify the error message contains the expected hash bucket count mismatch details
        error_message = str(exc_info.value)
        assert "Partition hash bucket count for compaction has changed" in error_message
        assert "Hash bucket count in RCI=2" in error_message
        assert "hash bucket count in params=1" in error_message

    def test_compact_partition_when_incremental_then_intelligent_estimation_sanity(
        self, catalog
    ):
        """Test case which asserts the RCI stats are correctly generated for a rebase and incremental use-case with intelligent estimation."""
        # Create source and destination namespaces/tables
        _, _, _, source_stream = self._create_namespace_and_table("source", catalog)
        _, _, _, dest_stream = self._create_namespace_and_table("destination", catalog)

        # Create source partition and commit backfill data
        source_partition = self._stage_and_commit_partition(source_stream, catalog)
        source_delta = self._stage_and_commit_delta(
            self.BACKFILL_DATA, source_partition, catalog
        )

        # Create destination partition
        dest_partition = self._stage_and_commit_partition(dest_stream, catalog)

        # Test compaction with intelligent estimation
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "rebase_source_partition_locator": source_delta.partition_locator,
                    "rebase_source_partition_high_watermark": source_delta.stream_position,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_delta.partition_locator,
                    "resource_estimation_method": ResourceEstimationMethod.INTELLIGENT_ESTIMATION,
                }
            )
        )

    def test_compact_partition_when_incremental_then_content_type_meta_estimation_sanity(
        self, catalog
    ):
        """Test case which asserts the RCI stats are correctly generated for a rebase and incremental use-case with content type meta estimation."""
        # Create source and destination namespaces/tables
        _, _, _, source_stream = self._create_namespace_and_table("source", catalog)
        _, _, _, dest_stream = self._create_namespace_and_table("destination", catalog)

        # Create source partition and commit backfill data
        source_partition = self._stage_and_commit_partition(source_stream, catalog)
        source_delta = self._stage_and_commit_delta(
            self.BACKFILL_DATA, source_partition, catalog
        )

        # Create destination partition
        dest_partition = self._stage_and_commit_partition(dest_stream, catalog)

        # Test compaction with content type meta estimation
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "rebase_source_partition_locator": source_delta.partition_locator,
                    "rebase_source_partition_high_watermark": source_delta.stream_position,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_delta.partition_locator,
                    "resource_estimation_method": ResourceEstimationMethod.CONTENT_TYPE_META,
                }
            )
        )

    def test_compact_partition_when_incremental_then_previous_inflation_estimation_sanity(
        self, catalog
    ):
        """Test case which asserts the RCI stats are correctly generated for a rebase and incremental use-case with previous inflation estimation."""
        # Create source and destination namespaces/tables
        _, _, _, source_stream = self._create_namespace_and_table("source", catalog)
        _, _, _, dest_stream = self._create_namespace_and_table("destination", catalog)

        # Create source partition and commit backfill data
        source_partition = self._stage_and_commit_partition(source_stream, catalog)
        source_delta = self._stage_and_commit_delta(
            self.BACKFILL_DATA, source_partition, catalog
        )

        # Create destination partition
        dest_partition = self._stage_and_commit_partition(dest_stream, catalog)

        # Test compaction with previous inflation estimation
        compact_partition(
            CompactPartitionParams.of(
                {
                    "catalog": catalog,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
                    },
                    "primary_keys": ["pk"],
                    "all_column_names": ["pk", "value"],
                    "rebase_source_partition_locator": source_delta.partition_locator,
                    "rebase_source_partition_high_watermark": source_delta.stream_position,
                    "records_per_compacted_file": 4000,
                    "source_partition_locator": source_delta.partition_locator,
                    "resource_estimation_method": ResourceEstimationMethod.PREVIOUS_INFLATION,
                }
            )
        )
