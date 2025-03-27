import os
import tempfile
import shutil
import uuid

import deltacat
import pytest
from deltacat import Field, Schema
from pyiceberg.catalog import CatalogType

import pyarrow as pa

from deltacat.catalog.iceberg.iceberg_catalog_config import IcebergCatalogConfig

@pytest.fixture
def schema_a():
    return Schema.of(
        [
            Field.of(
                field=pa.field("col1", pa.int32(), nullable=False),
                field_id=1,
                is_merge_key=True,
            )
        ]
    )

class TestIcebergCatalogInitialization:
    temp_dir = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def test_iceberg_catalog_initialize(self, schema_a):

        catalog_name = str(uuid.uuid4())

        config = IcebergCatalogConfig(type=CatalogType.IN_MEMORY,
                                      properties={"warehouse": self.temp_dir})
        # Initialize with the PyIceberg catalog
        deltacat.put_catalog(
            catalog_name, deltacat.IcebergCatalog,
            **{"config": config})

        iceberg_catalog = deltacat.get_catalog(catalog_name)
        deltacat.create_table("test_table", catalog=catalog_name, schema=schema_a)