import tempfile
import shutil
import uuid
import ray
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

    def test_iceberg_catalog_and_table_create(self, schema_a):

        # Note - we're using the global Catalog context here (shared across tests), so generating a random catalog name
        catalog_name = str(uuid.uuid4())

        config = IcebergCatalogConfig(
            type=CatalogType.SQL,
            properties={
                "warehouse": self.temp_dir,
                "uri": f"sqlite:////{self.temp_dir}/sql-catalog.db",
            },
        )

        # Initialize with the PyIceberg catalog
        deltacat.put_catalog(
            catalog_name,
            impl=deltacat.IcebergCatalog,
            **{"config": config},
            ray_init_args={"ignore_reinit_error": True}
        )

        table_def = deltacat.create_table(
            "test_table", catalog=catalog_name, schema=schema_a
        )

        # Fetch table we just created
        fetched_table_def = deltacat.get_table("test_table", catalog=catalog_name)
        assert table_def.table_version == fetched_table_def.table_version

        # For now, just check that we created a table version with an equivalent schema
        assert table_def.table_version.schema.equivalent_to(schema_a)

        # Sanity check that list namespaces works
        namespaces = deltacat.list_namespaces(catalog=catalog_name).all_items()
        assert table_def.table.namespace in [n.namespace for n in namespaces]
