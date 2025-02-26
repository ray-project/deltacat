import math
import shutil
import tempfile
from typing import Dict, List, Optional

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from deltacat.catalog.main.impl import PropertyCatalog, initialize
from deltacat.catalog.main.impl import (
    create_table,
    write_to_table,
    read_table,
    get_table,
    table_exists,
    create_namespace,
    namespace_exists,
)
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.sort_key import SortScheme, SortKey
from deltacat.storage.model.types import LocalTable, LifecycleState
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode
from deltacat.storage.rivulet.reader.query_expression import QueryExpression


class TestCatalogBasicEndToEnd:
    temp_dir = None
    catalog = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog = initialize(root=cls.temp_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def create_test_table(self, table_name: str, namespace: Optional[str] = None) -> pa.Table:
        """Helper to create a test table with sample data"""
        # Create sample data
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45]
        }
        table = pa.Table.from_pydict(data)
        
        # Create schema
        schema = Schema.of(table.schema)
        
        # Create the table
        create_table(
            table=table_name,
            namespace=namespace,
            schema=schema,
            catalog=self.catalog
        )
        
        return table

    def test_create_and_get_table(self):
        """Test creating a table and retrieving its metadata"""
        table_name = "test_create_get"
        
        # Create the table
        pa_table = self.create_test_table(table_name)
        
        # Verify table exists
        assert table_exists(table_name, catalog=self.catalog)
        
        # Get table definition
        table_def = get_table(table_name, catalog=self.catalog)
        
        # Verify table definition
        assert table_def is not None
        assert table_def.name == table_name
        assert table_def.schema is not None
        assert table_def.sort_keys is not None
        assert len(table_def.sort_keys.keys) == 1
        assert table_def.sort_keys.keys[0].name == "id"

    def test_write_and_read_table(self):
        """Test writing data to a table and reading it back"""
        table_name = "test_write_read"
        
        # Create the table
        pa_table = self.create_test_table(table_name)
        
        # Write data to the table
        write_to_table(
            data=pa_table,
            table=table_name,
            mode=TableWriteMode.APPEND,
            content_type=ContentType.PARQUET,
            catalog=self.catalog
        )
        
        # Read data from the table
        result = read_table(
            table=table_name,
            catalog=self.catalog
        )
        
        # Convert to pandas for comparison
        original_df = pa_table.to_pandas()
        result_df = result.to_pandas()
        
        # Verify data
        assert len(result_df) == len(original_df)
        assert set(result_df["id"].tolist()) == set(original_df["id"].tolist())
        assert set(result_df["name"].tolist()) == set(original_df["name"].tolist())
        assert set(result_df["age"].tolist()) == set(original_df["age"].tolist())

    def test_write_with_auto_create(self):
        """Test writing data with auto-create mode"""
        table_name = "test_auto_create"
        
        # Create sample data
        data = {
            "id": [10, 20, 30],
            "name": ["Frank", "Grace", "Heidi"],
            "age": [50, 55, 60]
        }
        pa_table = pa.Table.from_pydict(data)
        
        # Write data with AUTO mode (should create the table)
        write_to_table(
            data=pa_table,
            table=table_name,
            mode=TableWriteMode.AUTO,
            content_type=ContentType.PARQUET,
            catalog=self.catalog,
            schema=Schema.of_arrow(pa_table.schema),
            sort_keys=SortScheme([SortKey("id")])
        )
        
        # Verify table exists
        assert table_exists(table_name, catalog=self.catalog)
        
        # Read data back
        result = read_table(
            table=table_name,
            catalog=self.catalog
        )
        
        # Convert to pandas for comparison
        original_df = pa_table.to_pandas()
        result_df = result.to_pandas()
        
        # Verify data
        assert len(result_df) == len(original_df)
        assert set(result_df["id"].tolist()) == set(original_df["id"].tolist())

    def test_write_multiple_batches(self):
        """Test writing multiple batches to a table"""
        table_name = "test_multiple_batches"
        
        # Create the table
        pa_table = self.create_test_table(table_name)
        
        # Write first batch
        write_to_table(
            data=pa_table,
            table=table_name,
            mode=TableWriteMode.APPEND,
            content_type=ContentType.PARQUET,
            catalog=self.catalog
        )
        
        # Create second batch with different data
        data2 = {
            "id": [6, 7, 8],
            "name": ["Frank", "Grace", "Heidi"],
            "age": [50, 55, 60]
        }
        pa_table2 = pa.Table.from_pydict(data2)
        
        # Write second batch
        write_to_table(
            data=LocalTable(pa_table2),
            table=table_name,
            mode=TableWriteMode.APPEND,
            content_type=ContentType.PARQUET,
            catalog=self.catalog
        )
        
        # Read data back
        result = read_table(
            table=table_name,
            catalog=self.catalog
        )
        
        # Verify data
        result_df = result.to_pandas()
        assert len(result_df) == 8  # 5 from first batch + 3 from second batch
        assert set(result_df["id"].tolist()) == {1, 2, 3, 4, 5, 6, 7, 8}

    def test_read_with_query(self):
        """Test reading data with a query expression"""
        table_name = "test_query"
        
        # Create the table with sample data
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "name": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"],
            "age": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        }
        pa_table = pa.Table.from_pydict(data)
        
        # Create schema and sort keys
        schema = Schema.of(pa_table.schema)
        sort_keys = None
        
        # Create the table
        create_table(
            table=table_name,
            schema=schema,
            sort_keys=sort_keys,
            catalog=self.catalog
        )
        
        # Write data
        write_to_table(
            data=pa_table,
            table=table_name,
            mode=TableWriteMode.APPEND,
            catalog=self.catalog
        )
        
        # Read with query for id range 3-7
        query = QueryExpression().with_range(3, 7)
        result = read_table(
            table=table_name,
            catalog=self.catalog,
            query=query
        )
        
        # Verify filtered data
        result_df = result.to_pandas()
        assert len(result_df) == 5  # ids 3, 4, 5, 6, 7
        assert set(result_df["id"].tolist()) == {3, 4, 5, 6, 7}

    def test_namespace_operations(self):
        """Test namespace operations"""
        namespace = "test_namespace"
        
        # Create namespace
        create_namespace(
            namespace=namespace,
            properties={},
            catalog=self.catalog
        )
        
        # Verify namespace exists
        assert namespace_exists(namespace, catalog=self.catalog)
        
        # Create table in namespace
        table_name = "namespaced_table"
        pa_table = self.create_test_table(table_name, namespace)
        
        # Verify table exists in namespace
        assert table_exists(table_name, namespace, catalog=self.catalog)
        
        # Write and read data in namespaced table
        write_to_table(
            data=pa_table,
            table=table_name,
            namespace=namespace,
            mode=TableWriteMode.APPEND,
            catalog=self.catalog
        )
        
        result = read_table(
            table=table_name,
            namespace=namespace,
            catalog=self.catalog
        )
        
        # Verify data
        original_df = pa_table.to_pandas()
        result_df = result.to_pandas()
        assert len(result_df) == len(original_df)


class TestCatalogErrorHandling:
    temp_dir = None
    catalog = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog = initialize(root=cls.temp_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def test_create_table_already_exists(self):
        """Test error when creating a table that already exists"""
        table_name = "existing_table"
        
        # Create the table first time
        data = {"id": [1, 2, 3]}
        pa_table = pa.Table.from_pydict(data)
        schema = Schema.of(pa_table.schema)
        
        create_table(
            table=table_name,
            schema=schema,
            catalog=self.catalog
        )
        
        # Try to create again with fail_if_exists=True
        with pytest.raises(ValueError, match=f"Table .+{table_name} already exists"):
            create_table(
                table=table_name,
                schema=schema,
                catalog=self.catalog,
                fail_if_exists=True
            )
        
        # Should not raise with fail_if_exists=False
        result = create_table(
            table=table_name,
            schema=schema,
            catalog=self.catalog,
            fail_if_exists=False
        )
        assert result is not None

    def test_write_to_nonexistent_table(self):
        """Test error when writing to a table that doesn't exist"""
        table_name = "nonexistent_table"
        
        data = {"id": [1, 2, 3]}
        pa_table = pa.Table.from_pydict(data)
        
        # Try to append to non-existent table
        with pytest.raises(ValueError, match=f"Table .+{table_name} does not exist"):
            write_to_table(
                data=pa_table,
                table=table_name,
                mode=TableWriteMode.APPEND,
                catalog=self.catalog
            )
        
        # Should work with AUTO mode
        write_to_table(
            data=pa_table,
            table=table_name,
            mode=TableWriteMode.AUTO,
            catalog=self.catalog,
            schema=Schema.of(pa_table.schema)
        )
        
        # Verify table was created
        assert table_exists(table_name, catalog=self.catalog)

    def test_read_nonexistent_table(self):
        """Test error when reading a table that doesn't exist"""
        table_name = "another_nonexistent_table"
        
        # Try to read non-existent table
        with pytest.raises(ValueError, match=f"Table .+{table_name} does not exist"):
            read_table(
                table=table_name,
                catalog=self.catalog
            ) 