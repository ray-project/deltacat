"""
Tests to verify that download_delta and merge_deltas preserve ordering
of delta manifest entries.
"""
import shutil
import tempfile
import pyarrow as pa
import pandas as pd
import ray

from deltacat.storage import (
    metastore,
    LifecycleState,
    Schema,
)
from deltacat.types.media import (
    ContentType,
    DatasetType,
    StorageType,
)
from deltacat.storage.model.delta import Delta
from deltacat.storage.model.manifest import ManifestAuthor
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
)
from deltacat.catalog import CatalogProperties
from deltacat.types.tables import table_to_pandas


class TestDeltaOrdering:
    """Tests to ensure download_delta and merge_deltas preserve manifest entry order."""

    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)

        # Create and commit namespace
        cls.namespace = create_test_namespace()
        metastore.create_namespace(
            namespace=cls.namespace.locator.namespace,
            catalog=cls.catalog,
        )

        # Create a schema for the table version
        cls.arrow_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("value", pa.int64()),
                ("order_marker", pa.string()),  # Unique marker to verify order
            ]
        )
        schema = Schema.of(
            schema=cls.arrow_schema,
            schema_id=1,
        )

        # Create and commit table version with schema
        cls.table, cls.table_version, cls.stream = metastore.create_table_version(
            namespace=cls.namespace.locator.namespace,
            table_name="test_ordering_table",
            table_version="v.1",
            schema=schema,
            catalog=cls.catalog,
        )

        # Make the table version active
        metastore.update_table_version(
            namespace=cls.namespace.locator.namespace,
            table_name="test_ordering_table",
            table_version="v.1",
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=cls.catalog,
        )

        # Stage and commit partition
        cls.partition = metastore.stage_partition(
            stream=cls.stream,
            catalog=cls.catalog,
        )
        cls.partition = metastore.commit_partition(
            partition=cls.partition,
            catalog=cls.catalog,
        )
        # Get the committed partition to ensure we have the latest state
        cls.partition = metastore.get_partition_by_id(
            stream_locator=cls.partition.stream_locator,
            partition_id=cls.partition.partition_id,
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_method(cls):
        if ray.is_initialized():
            ray.shutdown()
        shutil.rmtree(cls.tmpdir)

    def _create_delta_with_multiple_entries(
        self, num_entries: int, max_records_per_entry: int
    ):
        """
        Create a delta with multiple manifest entries.
        Each entry has distinct order markers to verify ordering.
        """
        # Create separate dataframes for each entry
        all_data = []
        for entry_idx in range(num_entries):
            start_id = entry_idx * max_records_per_entry
            df = pd.DataFrame(
                {
                    "id": list(range(start_id, start_id + max_records_per_entry)),
                    "value": [
                        entry_idx * 100 + i for i in range(max_records_per_entry)
                    ],
                    "order_marker": [
                        f"entry_{entry_idx:03d}_row_{i:03d}"
                        for i in range(max_records_per_entry)
                    ],
                }
            )
            all_data.append(df)

        # Concatenate all data
        combined_df = pd.concat(all_data, ignore_index=True)

        # Stage delta with max_records_per_entry to create multiple manifest entries
        delta = metastore.stage_delta(
            data=combined_df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            max_records_per_entry=max_records_per_entry,
        )

        # Verify we created the expected number of entries
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == num_entries, (
            f"Expected {num_entries} manifest entries, "
            f"got {len(delta.manifest.entries)}"
        )

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=delta,
            catalog=self.catalog,
        )

        return committed_delta

    def _verify_download_ordering(
        self,
        downloaded_data,
        expected_num_entries: int,
        dataset_type: DatasetType,
        storage_type: StorageType,
    ):
        """
        Verify that downloaded data preserves the order of manifest entries.
        """
        if storage_type == StorageType.LOCAL:
            # Local storage returns a list of tables
            assert isinstance(
                downloaded_data, list
            ), f"Expected list for LOCAL storage, got {type(downloaded_data)}"
            assert (
                len(downloaded_data) == expected_num_entries
            ), f"Expected {expected_num_entries} entries, got {len(downloaded_data)}"

            for entry_idx, table in enumerate(downloaded_data):
                # Convert to pandas for easier validation
                df = table_to_pandas(table, schema=self.arrow_schema)

                # Verify all rows have the correct entry marker
                assert len(df) > 0, f"Entry {entry_idx} is empty"
                for _, row in df.iterrows():
                    order_marker = row["order_marker"]
                    assert order_marker.startswith(
                        f"entry_{entry_idx:03d}_"
                    ), f"Entry {entry_idx} has incorrect order_marker: {order_marker}"

        else:  # DISTRIBUTED storage
            # Distributed storage returns a Ray Dataset or Daft DataFrame
            if dataset_type == DatasetType.DAFT or hasattr(
                downloaded_data, "to_pandas"
            ):
                combined_df = table_to_pandas(downloaded_data)

                # Verify order by checking that order_markers are monotonically increasing
                order_markers = combined_df["order_marker"].tolist()
                assert len(order_markers) > 0, "No data in downloaded dataset"

                # Verify markers follow the pattern entry_000_*, entry_001_*, etc.
                current_entry_idx = 0
                for marker in order_markers:
                    marker_entry_idx = int(marker.split("_")[1])
                    assert marker_entry_idx >= current_entry_idx, (
                        f"Order violation: found entry {marker_entry_idx} "
                        f"after entry {current_entry_idx}"
                    )
                    current_entry_idx = marker_entry_idx

    def test_download_delta_sequential_preserves_order_pyarrow(self):
        """Test that download_delta with max_parallelism=1 preserves order."""
        num_entries = 5
        records_per_entry = 10

        delta = self._create_delta_with_multiple_entries(num_entries, records_per_entry)

        # Download with sequential processing
        downloaded = metastore.download_delta(
            delta_like=delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            max_parallelism=1,
            catalog=self.catalog,
        )

        self._verify_download_ordering(
            downloaded,
            num_entries,
            DatasetType.PYARROW,
            StorageType.LOCAL,
        )

    def test_download_delta_parallel_preserves_order_pyarrow(self):
        """Test that download_delta with max_parallelism>1 preserves order."""
        num_entries = 10
        records_per_entry = 5

        delta = self._create_delta_with_multiple_entries(num_entries, records_per_entry)

        # Download with parallel processing
        downloaded = metastore.download_delta(
            delta_like=delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            max_parallelism=4,
            catalog=self.catalog,
        )

        self._verify_download_ordering(
            downloaded,
            num_entries,
            DatasetType.PYARROW,
            StorageType.LOCAL,
        )

    def test_download_delta_pandas_preserves_order(self):
        """Test that download_delta with PANDAS dataset type preserves order."""
        num_entries = 5
        records_per_entry = 10

        delta = self._create_delta_with_multiple_entries(num_entries, records_per_entry)

        # Download as PANDAS with parallel processing
        downloaded = metastore.download_delta(
            delta_like=delta,
            table_type=DatasetType.PANDAS,
            storage_type=StorageType.LOCAL,
            max_parallelism=4,
            catalog=self.catalog,
        )

        self._verify_download_ordering(
            downloaded,
            num_entries,
            DatasetType.PANDAS,
            StorageType.LOCAL,
        )

    def test_download_delta_numpy_preserves_order(self):
        """Test that download_delta with NUMPY dataset type preserves manifest entry boundaries."""
        num_entries = 5
        records_per_entry = 10

        delta = self._create_delta_with_multiple_entries(num_entries, records_per_entry)

        # Download as NUMPY with parallel processing
        downloaded = metastore.download_delta(
            delta_like=delta,
            table_type=DatasetType.NUMPY,
            storage_type=StorageType.LOCAL,
            max_parallelism=4,
            catalog=self.catalog,
        )

        self._verify_download_ordering(
            downloaded,
            num_entries,
            DatasetType.NUMPY,
            StorageType.LOCAL,
        )

    def test_download_delta_distributed_ray_dataset_preserves_order(self):
        """Test that download_delta with RAY_DATASET preserves order."""
        if not ray.is_initialized():
            ray.init()

        num_entries = 5
        records_per_entry = 10

        delta = self._create_delta_with_multiple_entries(num_entries, records_per_entry)

        # Download as distributed Ray Dataset
        downloaded = metastore.download_delta(
            delta_like=delta,
            table_type=DatasetType.RAY_DATASET,
            storage_type=StorageType.DISTRIBUTED,
            max_parallelism=4,
            catalog=self.catalog,
        )

        self._verify_download_ordering(
            downloaded,
            num_entries,
            DatasetType.RAY_DATASET,
            StorageType.DISTRIBUTED,
        )

    def test_merge_deltas_preserves_delta_order(self):
        """Test that merge_deltas preserves the order of deltas."""
        # Create 3 deltas with different data
        deltas = []
        for delta_idx in range(3):
            df = pd.DataFrame(
                {
                    "id": [delta_idx * 10 + i for i in range(5)],
                    "value": [delta_idx * 100 + i for i in range(5)],
                    "order_marker": [f"delta_{delta_idx}_row_{i}" for i in range(5)],
                }
            )

            delta = metastore.stage_delta(
                data=df,
                partition=self.partition,
                catalog=self.catalog,
                content_type=ContentType.PARQUET,
            )

            deltas.append(delta)

        # Merge the deltas
        merged_delta = Delta.merge_deltas(
            deltas=deltas,
            manifest_author=ManifestAuthor.of("test_author", "1.0"),
            stream_position=1,
        )

        # Verify merged manifest has entries in order
        assert merged_delta.manifest is not None
        assert len(merged_delta.manifest.entries) == 3

        # Download and verify order
        downloaded = metastore.download_delta(
            delta_like=merged_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            max_parallelism=1,
            catalog=self.catalog,
        )

        # Verify each entry corresponds to the correct delta
        assert len(downloaded) == 3
        for delta_idx, table in enumerate(downloaded):
            df = table_to_pandas(table)
            for _, row in df.iterrows():
                order_marker = row["order_marker"]
                assert order_marker.startswith(
                    f"delta_{delta_idx}_"
                ), f"Expected delta_{delta_idx}, got {order_marker}"

    def test_merge_deltas_preserves_manifest_entry_order(self):
        """Test that merge_deltas preserves order of manifest entries within each delta."""
        # Create 2 deltas, each with multiple manifest entries
        deltas = []
        for delta_idx in range(2):
            # Create 3 entries per delta
            df = pd.DataFrame(
                {
                    "id": [delta_idx * 30 + i for i in range(30)],
                    "value": [delta_idx * 1000 + i for i in range(30)],
                    "order_marker": [
                        f"delta_{delta_idx}_row_{i:03d}" for i in range(30)
                    ],
                }
            )

            delta = metastore.stage_delta(
                data=df,
                partition=self.partition,
                catalog=self.catalog,
                content_type=ContentType.PARQUET,
                max_records_per_entry=10,  # Create 3 entries per delta
            )

            assert len(delta.manifest.entries) == 3
            deltas.append(delta)

        # Merge the deltas
        merged_delta = Delta.merge_deltas(
            deltas=deltas,
            manifest_author=ManifestAuthor.of("test_author", "1.0"),
            stream_position=1,
        )

        # Verify merged manifest has 6 entries (3 from each delta) in order
        assert merged_delta.manifest is not None
        assert len(merged_delta.manifest.entries) == 6

        # Download and verify order
        downloaded = metastore.download_delta(
            delta_like=merged_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            max_parallelism=1,
            catalog=self.catalog,
        )

        # Verify entries are in order: delta_0 (3 entries), then delta_1 (3 entries)
        assert len(downloaded) == 6

        # First 3 entries should be from delta_0
        for entry_idx in range(3):
            df = table_to_pandas(downloaded[entry_idx])
            for _, row in df.iterrows():
                order_marker = row["order_marker"]
                assert order_marker.startswith(
                    "delta_0_"
                ), f"Entry {entry_idx} should be from delta_0, got {order_marker}"

        # Last 3 entries should be from delta_1
        for entry_idx in range(3, 6):
            df = table_to_pandas(downloaded[entry_idx])
            for _, row in df.iterrows():
                order_marker = row["order_marker"]
                assert order_marker.startswith(
                    "delta_1_"
                ), f"Entry {entry_idx} should be from delta_1, got {order_marker}"

    def test_download_delta_with_columns_preserves_order(self):
        """Test that download_delta with column selection still preserves order."""
        num_entries = 5
        records_per_entry = 10

        delta = self._create_delta_with_multiple_entries(num_entries, records_per_entry)

        # Download with column selection
        downloaded = metastore.download_delta(
            delta_like=delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            max_parallelism=4,
            columns=["order_marker", "value"],  # Select subset of columns
            catalog=self.catalog,
        )

        # Verify order is still preserved
        self._verify_download_ordering(
            downloaded,
            num_entries,
            DatasetType.PYARROW,
            StorageType.LOCAL,
        )
