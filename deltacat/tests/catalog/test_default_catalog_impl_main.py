import unittest
import uuid
import tempfile

import ray
import os
from deltacat import Catalog
from deltacat.catalog import CatalogProperties
from deltacat.utils.common import current_time_ms
from deltacat.tests.test_utils.pyarrow_main import (
    create_delta_from_csv_file,
    commit_delta_to_partition,
)
from deltacat.types.media import DistributedDatasetType, ContentType
from deltacat.storage import metastore
import deltacat as dc


class TestReadTableMain(unittest.TestCase):
    READ_TABLE_NAMESPACE = f"catalog_read_table_namespace_main_{current_time_ms()}"
    SAMPLE_FILE_PATH = "deltacat/tests/catalog/data/sample_table.csv"

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)

        # Use the default catalog storage location instead of a temp directory
        # This ensures both catalog and direct storage operations use the same backend
        cls.catalog_properties = CatalogProperties()  # Use default location
        
        # For catalog read operations - no catalog property since it's passed separately
        cls.kwargs = {
            "supported_content_types": [ContentType.PARQUET],
        }
        
        # For storage operations that expect ds_mock_kwargs - use the same catalog_properties
        cls.ds_mock_kwargs = {"catalog": cls.catalog_properties}

        cls.catalog_name = str(uuid.uuid4())
        # Use the default catalog configuration
        catalog_config = CatalogProperties(storage=metastore)
        dc.put_catalog(
            cls.catalog_name,
            catalog=Catalog(catalog_config),
        )
        super().setUpClass()

    @classmethod
    def doClassCleanups(cls) -> None:
        # Clean up the default catalog location if needed
        import shutil
        try:
            shutil.rmtree(cls.catalog_properties.root, ignore_errors=True)
        except:
            pass
        ray.shutdown()
        super().tearDownClass()

    def test_daft_distributed_read_sanity(self):
        # setup
        READ_TABLE_TABLE_NAME = "test_read_table_main"
        test_namespace = f"{self.READ_TABLE_NAMESPACE}_sanity"
        
        create_delta_from_csv_file(
            test_namespace,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            ds_mock_kwargs=self.ds_mock_kwargs,
            **{"supported_content_types": [ContentType.PARQUET]},
        )

        # For read_table, we need to pass storage config without the catalog key to avoid conflicts
        read_kwargs = {k: v for k, v in self.ds_mock_kwargs.items() if k != "catalog"}
        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            **read_kwargs,
        )

        # verify
        self.assertEqual(df.count_rows(), 6)
        self.assertEqual(df.column_names, ["pk", "value"])

    def test_daft_distributed_read_multiple_deltas(self):
        # setup
        READ_TABLE_TABLE_NAME = "test_read_table_2_main"
        test_namespace = f"{self.READ_TABLE_NAMESPACE}_multiple"
        
        delta = create_delta_from_csv_file(
            test_namespace,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            ds_mock_kwargs=self.ds_mock_kwargs,
            **{"supported_content_types": [ContentType.PARQUET]},
        )

        partition = metastore.get_partition(
            delta.stream_locator, delta.partition_values, **self.ds_mock_kwargs
        )

        commit_delta_to_partition(
            partition=partition, 
            file_paths=[self.SAMPLE_FILE_PATH], 
            ds_mock_kwargs=self.ds_mock_kwargs,
            **{"supported_content_types": [ContentType.PARQUET]},
        )

        # For read_table, we need to pass storage config without the catalog key to avoid conflicts
        read_kwargs = {k: v for k, v in self.ds_mock_kwargs.items() if k != "catalog"}
        # action
        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=test_namespace,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            merge_on_read=False,
            **read_kwargs,
        )

        # verify
        self.assertEqual(
            df.count_rows(),
            12,
            "we expect twice as many columns as merge on read is disabled",
        )
        self.assertEqual(df.column_names, ["pk", "value"])


if __name__ == '__main__':
    unittest.main() 