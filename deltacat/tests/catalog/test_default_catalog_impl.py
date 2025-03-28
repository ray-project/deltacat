import unittest
import sqlite3
import uuid

import ray
import os
import deltacat.tests.local_deltacat_storage as ds
from deltacat import Catalog
from deltacat.catalog import CatalogProperties
from deltacat.utils.common import current_time_ms
from deltacat.tests.test_utils.pyarrow import (
    create_delta_from_csv_file,
    commit_delta_to_partition,
)
from deltacat.types.media import DistributedDatasetType, ContentType
import deltacat as dc


class TestReadTable(unittest.TestCase):
    READ_TABLE_NAMESPACE = "catalog_read_table_namespace"
    DB_FILE_PATH = f"{current_time_ms()}.db"
    SAMPLE_FILE_PATH = "deltacat/tests/catalog/data/sample_table.csv"

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)

        con = sqlite3.connect(cls.DB_FILE_PATH)
        cur = con.cursor()
        cls.kwargs = {
            ds.SQLITE_CON_ARG: con,
            ds.SQLITE_CUR_ARG: cur,
            "supported_content_types": [ContentType.CSV],
        }
        cls.deltacat_storage_kwargs = {ds.DB_FILE_PATH_ARG: cls.DB_FILE_PATH}

        cls.catalog_name = str(uuid.uuid4())
        catalog_config = CatalogProperties(storage=ds)
        dc.put_catalog(
            cls.catalog_name,
            catalog=Catalog.default(config=catalog_config),
            ray_init_args={"ignore_reinit_error": True},
        )
        super().setUpClass()

    @classmethod
    def doClassCleanups(cls) -> None:
        os.remove(cls.DB_FILE_PATH)
        ray.shutdown()
        super().tearDownClass()

    def test_daft_distributed_read_sanity(self):
        # setup
        READ_TABLE_TABLE_NAME = "test_read_table"
        create_delta_from_csv_file(
            self.READ_TABLE_NAMESPACE,
            [self.SAMPLE_FILE_PATH],
            table_name=READ_TABLE_TABLE_NAME,
            **self.kwargs,
        )

        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=self.READ_TABLE_NAMESPACE,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            **self.kwargs,
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
            **self.kwargs,
        )

        partition = ds.get_partition(
            delta.stream_locator, delta.partition_values, **self.kwargs
        )

        commit_delta_to_partition(
            partition=partition, file_paths=[self.SAMPLE_FILE_PATH], **self.kwargs
        )

        # action
        df = dc.read_table(
            table=READ_TABLE_TABLE_NAME,
            namespace=self.READ_TABLE_NAMESPACE,
            catalog=self.catalog_name,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            merge_on_read=False,
            **self.kwargs,
        )

        # verify
        self.assertEqual(
            df.count_rows(),
            12,
            "we expect twice as many" " columns as merge on read is disabled",
        )
        self.assertEqual(df.column_names, ["pk", "value"])
