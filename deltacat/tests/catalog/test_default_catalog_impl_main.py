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
    READ_TABLE_NAMESPACE = "catalog_read_table_namespace"
    SAMPLE_FILE_PATH = "deltacat/tests/catalog/data/sample_table.csv"

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)

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
        READ_TABLE_TABLE_NAME = "test_read_table"
        create_delta_from_csv_file(
            self.READ_TABLE_NAMESPACE,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            content_type=ContentType.PARQUET,
            inner=self.catalog_properties,
            supported_content_types=[ContentType.PARQUET],
        )

        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=self.READ_TABLE_NAMESPACE,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
        )

        # verify
        self.assertEqual(df.count_rows(), 6)
        self.assertEqual(df.column_names, ["pk", "value"])

    def test_daft_distributed_read_multiple_deltas(self):
        # setup
        READ_TABLE_TABLE_NAME = "test_read_table_2"
        delta = create_delta_from_csv_file(
            self.READ_TABLE_NAMESPACE,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            content_type=ContentType.PARQUET,
            inner=self.catalog_properties,
            supported_content_types=[ContentType.PARQUET],
        )

        partition = metastore.get_partition(
            delta.stream_locator, 
            delta.partition_values, 
            inner=self.catalog_properties,
        )

        commit_delta_to_partition(
            partition=partition, 
            file_paths=[self.SAMPLE_FILE_PATH], 
            inner=self.catalog_properties,
            content_type=ContentType.PARQUET,
        )

        # action
        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=self.READ_TABLE_NAMESPACE,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            merge_on_read=False,
        )

        # verify
        self.assertEqual(
            df.count_rows(),
            12,
            "we expect twice as many" " columns as merge on read is disabled",
        )
        self.assertEqual(df.column_names, ["pk", "value"])
