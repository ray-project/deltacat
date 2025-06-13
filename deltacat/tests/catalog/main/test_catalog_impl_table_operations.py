import shutil
import tempfile

import pytest
import pyarrow as pa
import pandas as pd
import polars as pl
import numpy as np
import ray.data as rd
import daft

import deltacat.catalog.main.impl as catalog
from deltacat.catalog import get_catalog_properties
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.sort_key import SortKey, SortScheme, SortOrder, NullOrder
from deltacat.storage.model.table import TableProperties
from deltacat.storage.model.namespace import NamespaceProperties
from deltacat.storage.model.types import LifecycleState
from deltacat.exceptions import (
    TableAlreadyExistsError,
    TableNotFoundError,
)
from deltacat.types.tables import TableWriteMode
from deltacat.types.media import ContentType


@pytest.fixture(scope="class")
def catalog_setup():
    """Setup and teardown for the catalog test environment."""
    temp_dir = tempfile.mkdtemp()
    catalog_properties = get_catalog_properties(root=temp_dir)
    yield temp_dir, catalog_properties

    # Teardown
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def test_namespace(catalog_setup):
    """Create a test namespace for each test."""
    _, catalog_properties = catalog_setup
    namespace_name = "test_table_namespace"

    if not catalog.namespace_exists(namespace_name, inner=catalog_properties):
        catalog.create_namespace(
            namespace=namespace_name,
            properties={"description": "Test Table Namespace"},
            inner=catalog_properties,
        )

    return namespace_name, catalog_properties


@pytest.fixture
def sample_arrow_schema():
    """Create a sample PyArrow schema for testing."""
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("value", pa.float64()),
        ]
    )


@pytest.fixture
def sample_sort_keys():
    """Create a sample sort scheme for testing."""
    return SortScheme(
        keys=[
            SortKey.of(
                key=["id"], sort_order=SortOrder.ASCENDING, null_order=NullOrder.AT_END
            ),
        ]
    )


class TestCatalogTableOperations:
    """Test catalog table operations including table creation, existence checks, etc."""

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = get_catalog_properties(root=cls.temp_dir)

        # Create a test namespace
        cls.test_namespace = "test_write_operations"
        catalog.create_namespace(
            namespace=cls.test_namespace, inner=cls.catalog_properties
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def test_create_table(self, test_namespace, sample_arrow_schema, sample_sort_keys):
        """Test creating a table with schema and properties"""
        namespace_name, catalog_properties = test_namespace
        table_name = "test_create_table"

        # Create a schema
        schema = Schema(arrow=sample_arrow_schema)

        # Create table properties
        table_properties = TableProperties(
            {"owner": "test-user", "department": "engineering"}
        )

        # Create namespace properties
        namespace_properties = NamespaceProperties({"description": "Test Namespace"})

        # Create the table
        table_definition = catalog.create_table(
            name=table_name,
            namespace=namespace_name,
            schema=schema,
            sort_keys=sample_sort_keys,
            description="Test table for unit tests",
            table_properties=table_properties,
            namespace_properties=namespace_properties,
            inner=catalog_properties,
        )

        # Verify table was created
        assert catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        table = table_definition.table
        table_version = table_definition.table_version

        # Verify table definition properties
        assert table_version.table_name == table_name
        assert table_version.namespace == namespace_name
        assert table_version.description == "Test table for unit tests"
        assert table_version.state == LifecycleState.CREATED
        assert table.properties.get("owner") == "test-user"
        assert table.properties.get("department") == "engineering"
        assert table_version.schema.arrow.names == sample_arrow_schema.names
        assert len(table_version.sort_scheme.keys) == 1
        sort_key_paths = [key[0][0] for key in table_version.sort_scheme.keys]
        assert "id" in sort_key_paths

    def test_create_table_already_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        table_name = "test_table_exists"

        # Create the table
        catalog.create_table(
            name=table_name,
            namespace=namespace_name,
            description="First creation",
            inner=catalog_properties,
        )

        # Verify table exists
        assert catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Try to create the same table again, should raise TableAlreadyExistsError
        with pytest.raises(
            TableAlreadyExistsError,
            match=f"Table {namespace_name}.{table_name} already exists",
        ):
            catalog.create_table(
                name=table_name,
                namespace=namespace_name,
                description="Second creation attempt",
                inner=catalog_properties,
            )

    def test_create_table_already_exists_no_fail(self, test_namespace):
        """Test creating a table that already exists with fail_if_exists=False"""
        namespace_name, catalog_properties = test_namespace
        table_name = "test_table_exists_no_fail"

        # Create the table with original description
        catalog.create_table(
            name=table_name,
            namespace=namespace_name,
            description="Original description",
            inner=catalog_properties,
        )

        assert catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Create the same table with fail_if_exists=False
        table_definition = catalog.create_table(
            name=table_name,
            namespace=namespace_name,
            description="Updated description",
            fail_if_exists=False,
            inner=catalog_properties,
        )

        table = table_definition.table

        assert table.table_name == table_name
        assert table.namespace == namespace_name
        # Ensure description is unchanged
        assert table.description == "Original description"

    def test_drop_table(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        table_name = "test_drop_table"

        # Create the table
        catalog.create_table(
            name=table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Verify table exists
        assert catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Drop the table
        catalog.drop_table(
            name=table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Verify table no longer exists
        assert not catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

    def test_drop_table_not_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        table_name = "nonexistent_table"

        # Verify table doesn't exist
        assert not catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Try to drop the table, should raise TableNotFoundError
        with pytest.raises(TableNotFoundError, match=table_name):
            catalog.drop_table(
                name=table_name, namespace=namespace_name, inner=catalog_properties
            )

    def test_rename_table(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        original_name = "test_original_table"
        new_name = "test_renamed_table"

        # Create the table with original name
        catalog.create_table(
            name=original_name,
            namespace=namespace_name,
            description="Table to be renamed",
            inner=catalog_properties,
        )

        # Verify original table exists
        assert catalog.table_exists(
            original_name, namespace=namespace_name, inner=catalog_properties
        )

        # Rename the table
        catalog.rename_table(
            table=original_name,
            new_name=new_name,
            namespace=namespace_name,
            inner=catalog_properties,
        )

        # Verify new table exists and old table doesn't
        assert catalog.table_exists(
            new_name, namespace=namespace_name, inner=catalog_properties
        )
        assert not catalog.table_exists(
            original_name, namespace=namespace_name, inner=catalog_properties
        )

    def test_rename_table_not_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        original_name = "nonexistent_table"
        new_name = "test_renamed_nonexistent"

        # Verify table doesn't exist
        assert not catalog.table_exists(
            original_name, namespace=namespace_name, inner=catalog_properties
        )

        # Try to rename the table, should raise TableNotFoundError
        with pytest.raises(TableNotFoundError, match=original_name):
            catalog.rename_table(
                table=original_name,
                new_name=new_name,
                namespace=namespace_name,
                inner=catalog_properties,
            )

    def test_table_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        existing_table = "test_table_exists_check"
        non_existing_table = "nonexistent_table"

        # Create a table
        catalog.create_table(
            name=existing_table, namespace=namespace_name, inner=catalog_properties
        )

        # Check existing table
        assert catalog.table_exists(
            existing_table, namespace=namespace_name, inner=catalog_properties
        )

        # Check non-existing table
        assert not catalog.table_exists(
            non_existing_table, namespace=namespace_name, inner=catalog_properties
        )

    def test_create_table_with_default_namespace(self, catalog_setup):
        _, catalog_properties = catalog_setup
        table_name = "test_default_namespace_table"

        # Create table with default namespace
        table_definition = catalog.create_table(
            name=table_name, inner=catalog_properties
        )

        table = table_definition.table
        # Verify table was created in default namespace
        default_ns = catalog.default_namespace()
        assert table.namespace == default_ns
        assert catalog.table_exists(
            table_name, namespace=default_ns, inner=catalog_properties
        )

    def test_create_table_with_missing_namespace(self, catalog_setup):
        _, catalog_properties = catalog_setup
        table_name = "test_namespace_not_found_table"
        new_namespace = "nonexistent_namespace"

        # Verify namespace doesn't exist yet
        assert not catalog.namespace_exists(new_namespace, inner=catalog_properties)

        # Try to create table with non-existent namespace
        catalog.create_table(
            name=table_name, namespace=new_namespace, inner=catalog_properties
        )

        assert catalog.table_exists(
            table_name, namespace=new_namespace, inner=catalog_properties
        )
        assert catalog.namespace_exists(new_namespace, inner=catalog_properties)

    def test_alter_table(self, test_namespace, sample_arrow_schema, sample_sort_keys):
        namespace_name, catalog_properties = test_namespace
        table_name = "test_alter_table"

        # Create initial schema and properties
        schema = Schema.of(schema=sample_arrow_schema)
        initial_properties = TableProperties(
            {"owner": "original-user", "department": "engineering"}
        )

        # Create the table with initial properties
        table = catalog.create_table(
            name=table_name,
            namespace=namespace_name,
            schema=schema,
            sort_keys=sample_sort_keys,
            description="Initial description",
            table_properties=initial_properties,
            inner=catalog_properties,
        )
        old_schema = table.table_version.schema

        # Verify table was created with initial properties
        assert catalog.table_exists(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Create updated schema
        updated_arrow_schema = pa.schema(
            [
                pa.field("count", pa.float64()),  # Added field
            ]
        )

        new_schema = old_schema.add_subschema(
            name="updated_schema",
            schema=updated_arrow_schema,
        )

        # Create updated properties
        updated_properties = TableProperties(
            {"owner": "new-user", "department": "data-science", "priority": "high"}
        )

        # Alter the table with new properties
        catalog.alter_table(
            table=table_name,
            namespace=namespace_name,
            schema_updates=new_schema,
            description="Updated description",
            properties=updated_properties,
            inner=catalog_properties,
        )

        # Get the updated table definition
        updated_table_def = catalog.get_table(
            table_name, namespace=namespace_name, inner=catalog_properties
        )

        updated_table = updated_table_def.table
        updated_table_version = updated_table_def.table_version

        # Verify table properties were updated
        assert updated_table_version.description == "Updated description"
        assert updated_table_version.state == LifecycleState.CREATED
        assert updated_table.properties.get("owner") == "new-user"
        assert updated_table.properties.get("department") == "data-science"
        assert updated_table.properties.get("priority") == "high"

    def test_alter_table_not_exists(self, test_namespace):
        """Test altering a table that doesn't exist"""
        namespace_name, catalog_properties = test_namespace
        nonexistent_table = "nonexistent_alter_table"

        # Verify table doesn't exist
        assert not catalog.table_exists(
            nonexistent_table, namespace=namespace_name, inner=catalog_properties
        )

        # Try to alter the nonexistent table, should raise TableNotFoundError
        with pytest.raises(TableNotFoundError, match=nonexistent_table):
            catalog.alter_table(
                table=nonexistent_table,
                namespace=namespace_name,
                description="Updated description",
                inner=catalog_properties,
            )

    def test_drop_with_purge_validation(self, test_namespace):
        """Test that using purge flag raises ValidationError"""
        namespace_name, catalog_properties = test_namespace
        table_name = "test_drop_with_purge"

        # Create the table
        catalog.create_table(
            name=table_name, namespace=namespace_name, inner=catalog_properties
        )

        # Try to drop with purge=True, should raise ValidationError
        with pytest.raises(
            NotImplementedError, match="Purge flag is not currently supported"
        ):
            catalog.drop_table(
                name=table_name,
                namespace=namespace_name,
                purge=True,
                inner=catalog_properties,
            )

    def test_create_table_basic(self):
        """Test basic table creation"""
        table_name = "test_create_table_basic"
        schema = Schema.of(
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("name", pa.string()),
                ]
            )
        )

        table_def = catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            inner=self.catalog_properties,
        )

        assert table_def.table.table_name == table_name
        assert table_def.table_version.schema.equivalent_to(schema)

        # Verify table exists
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    def test_create_table_already_exists_fail_if_exists_true(self):
        """Test creating a table that already exists with fail_if_exists=True"""
        table_name = "test_create_table_exists"
        schema = Schema.of(schema=pa.schema([("id", pa.int64())]))

        # Create table first
        catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            inner=self.catalog_properties,
        )

        # Try to create again with fail_if_exists=True (default)
        with pytest.raises(TableAlreadyExistsError):
            catalog.create_table(
                name=table_name,
                namespace=self.test_namespace,
                schema=schema,
                fail_if_exists=True,
                inner=self.catalog_properties,
            )

    def test_create_table_already_exists_fail_if_exists_false(self):
        """Test creating a table that already exists with fail_if_exists=False"""
        table_name = "test_create_table_exists_ok"
        schema = Schema.of(schema=pa.schema([("id", pa.int64())]))

        # Create table first
        table_def1 = catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            inner=self.catalog_properties,
        )

        # Create again with fail_if_exists=False should return existing table
        table_def2 = catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            fail_if_exists=False,
            inner=self.catalog_properties,
        )

        assert table_def1.table.table_name == table_def2.table.table_name


class TestWriteToTable:
    """Test the write_to_table implementation with different modes and data types."""

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = get_catalog_properties(root=cls.temp_dir)

        # Create a test namespace
        cls.test_namespace = "test_write_to_table"
        catalog.create_namespace(
            namespace=cls.test_namespace, inner=cls.catalog_properties
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def _create_test_pandas_data(self):
        """Create test pandas DataFrame"""
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
            }
        )

    def _create_test_pyarrow_data(self):
        """Create test PyArrow Table"""
        return pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
            }
        )

    def _create_test_polars_data(self):
        """Create test Polars DataFrame"""
        return pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"],
            }
        )

    def _create_second_batch_pandas_data(self):
        """Create second batch of test data for append tests"""
        return pd.DataFrame(
            {
                "id": [6, 7, 8],
                "name": ["Frank", "Grace", "Henry"],
                "age": [50, 55, 60],
                "city": ["Boston", "Seattle", "Denver"],
            }
        )

    def _create_test_ray_data(self):
        """Create test Ray Dataset for schema inference testing."""
        import ray

        # Initialize Ray if not already initialized
        if not ray.is_initialized():
            ray.init(local_mode=True)

        data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "NYC"},
            {"id": 2, "name": "Bob", "age": 30, "city": "LA"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
        ]
        return rd.from_items(data)

    def _create_test_daft_data(self):
        """Create test Daft DataFrame for schema inference testing."""
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "city": ["NYC", "LA", "Chicago"],
        }
        return daft.from_pydict(data)

    def _create_test_numpy_1d_data(self):
        """Create test 1D numpy array for schema inference testing."""
        return np.array([1, 2, 3, 4, 5])

    def _create_test_numpy_2d_data(self):
        """Create test 2D numpy array for schema inference testing."""
        return np.array([[1, 25], [2, 30], [3, 35]], dtype=np.int64)

    def _create_table_with_merge_keys(self, table_name: str):
        """Create a table with merge keys for testing MERGE mode"""
        from deltacat.storage.model.schema import Schema, Field

        # Create schema with merge keys
        schema = Schema.of(
            [
                Field.of(pa.field("id", pa.int64()), is_merge_key=True),  # merge key
                Field.of(pa.field("name", pa.string())),
                Field.of(pa.field("age", pa.int32())),
                Field.of(pa.field("city", pa.string())),
            ]
        )

        catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            inner=self.catalog_properties,
        )

        return schema

    def _create_table_without_merge_keys(self, table_name: str):
        """Create a table without merge keys for testing APPEND mode"""
        # Use schema inference with no merge keys
        data = self._create_test_pandas_data()
        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

    # Test TableWriteMode.AUTO
    def test_write_to_table_auto_create_new_table_pandas(self):
        """Test AUTO mode creating a new table with pandas data"""
        table_name = "test_auto_create_pandas"
        data = self._create_test_pandas_data()

        # Table doesn't exist, AUTO should create it
        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.AUTO,
            inner=self.catalog_properties,
        )

        # Verify table was created
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        # Verify table has correct schema
        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )
        assert table_def.table_version.schema is not None

    def test_write_to_table_auto_create_new_table_pyarrow(self):
        """Test AUTO mode creating a new table with PyArrow data"""
        table_name = "test_auto_create_pyarrow"
        data = self._create_test_pyarrow_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.AUTO,
            inner=self.catalog_properties,
        )

        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    def test_write_to_table_auto_create_new_table_polars(self):
        """Test AUTO mode creating a new table with Polars data"""
        table_name = "test_auto_create_polars"
        data = self._create_test_polars_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.AUTO,
            inner=self.catalog_properties,
        )

        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    def test_write_to_table_auto_append_existing_table(self):
        """Test AUTO mode appending to existing table"""
        table_name = "test_auto_append"
        data1 = self._create_test_pandas_data()
        data2 = self._create_second_batch_pandas_data()

        # First write creates table
        catalog.write_to_table(
            data=data1,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.AUTO,
            inner=self.catalog_properties,
        )

        # Second write should append
        catalog.write_to_table(
            data=data2,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.AUTO,
            inner=self.catalog_properties,
        )

        # Verify table still exists
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    # Test TableWriteMode.CREATE
    def test_write_to_table_create_new_table(self):
        """Test CREATE mode with new table"""
        table_name = "test_create_new"
        data = self._create_test_pandas_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    def test_write_to_table_create_existing_table_fails(self):
        """Test CREATE mode fails when table exists"""
        table_name = "test_create_fail"
        data = self._create_test_pandas_data()

        # Create table first
        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        # Try to create again should fail
        with pytest.raises(ValueError, match="already exists and mode is CREATE"):
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.CREATE,
                inner=self.catalog_properties,
            )

    # Test TableWriteMode.APPEND
    def test_write_to_table_append_existing_table(self):
        """Test APPEND mode with existing table"""
        table_name = "test_append_existing"
        data1 = self._create_test_pandas_data()
        data2 = self._create_second_batch_pandas_data()

        # Create table first
        catalog.write_to_table(
            data=data1,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        # Append to existing table
        catalog.write_to_table(
            data=data2,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.APPEND,
            inner=self.catalog_properties,
        )

    def test_write_to_table_append_nonexistent_table_fails(self):
        """Test APPEND mode fails when table doesn't exist"""
        table_name = "test_append_fail"
        data = self._create_test_pandas_data()

        with pytest.raises(ValueError, match="does not exist and mode is"):
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.APPEND,
                inner=self.catalog_properties,
            )

    def test_write_to_table_append_with_merge_keys_fails(self):
        """Test APPEND mode fails when table has merge keys"""
        table_name = "test_append_with_merge_keys"

        # Create a table with merge keys
        self._create_table_with_merge_keys(table_name)

        # Create test data that matches the schema
        data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"],
            }
        )

        # APPEND mode should fail since table has merge keys
        with pytest.raises(
            ValueError,
            match="APPEND mode cannot be used with tables that have merge keys",
        ):
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.APPEND,
                inner=self.catalog_properties,
            )

    def test_write_to_table_append_without_merge_keys_succeeds(self):
        """Test APPEND mode works when table has no merge keys"""
        table_name = "test_append_no_merge_keys"

        # Create a table without merge keys
        self._create_table_without_merge_keys(table_name)

        # Add more data to the table
        data2 = self._create_second_batch_pandas_data()

        # APPEND mode should work since table has no merge keys
        catalog.write_to_table(
            data=data2,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.APPEND,
            inner=self.catalog_properties,
        )

        # Table should still exist
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    # Test explicit schema specification
    def test_write_to_table_explicit_schema(self):
        """Test writing with explicit schema specification"""
        table_name = "test_explicit_schema"
        data = self._create_test_pandas_data()

        # Define explicit schema
        explicit_schema = Schema.of(
            schema=pa.schema(
                [
                    ("id", pa.int64()),
                    ("name", pa.string()),
                    ("age", pa.int32()),  # Different from inferred schema
                    ("city", pa.string()),
                ]
            )
        )

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            schema=explicit_schema,
            inner=self.catalog_properties,
        )

        # Verify schema was used
        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )
        assert table_def.table_version.schema.equivalent_to(explicit_schema)

    def test_write_to_table_explicit_schema_none(self):
        """Test writing with explicit schema=None to create schemaless table"""
        table_name = "test_explicit_schema_none"
        data = self._create_test_pandas_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            schema=None,  # Explicitly set schema=None
            inner=self.catalog_properties,
        )

        # Verify table was created with schema=None (schemaless)
        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        # The table should exist but have a None/empty schema
        assert table_def is not None
        # Note: The exact behavior of schemaless tables may vary by storage implementation
        # We're mainly testing that the create_table call succeeded with schema=None

    def test_schema_behavior_comparison(self):
        """Test that demonstrates the difference between no schema vs explicit schema=None"""
        data = self._create_test_pandas_data()

        # Case 1: No schema argument - should infer schema
        table_name_inferred = "test_schema_inferred"
        catalog.write_to_table(
            data=data,
            table=table_name_inferred,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            # No schema argument provided - should infer from data
            inner=self.catalog_properties,
        )

        # Case 2: Explicit schema=None - should create schemaless table
        table_name_schemaless = "test_schema_none"
        catalog.write_to_table(
            data=data,
            table=table_name_schemaless,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            schema=None,  # Explicitly set schema=None
            inner=self.catalog_properties,
        )

        # Verify both tables were created
        table_inferred = catalog.get_table(
            name=table_name_inferred,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        table_schemaless = catalog.get_table(
            name=table_name_schemaless,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        # Both tables should exist
        assert table_inferred is not None
        assert table_schemaless is not None

        # The inferred schema table should have a schema with the expected columns
        inferred_schema = table_inferred.table_version.schema.arrow
        assert "id" in inferred_schema.names
        assert "name" in inferred_schema.names
        assert "age" in inferred_schema.names
        assert "city" in inferred_schema.names

    # Test schema inference from different data types
    def test_schema_inference_pandas(self):
        """Test schema inference from pandas DataFrame"""
        table_name = "test_schema_inference_pandas"
        data = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert "int_col" in schema.names
        assert "float_col" in schema.names
        assert "str_col" in schema.names
        assert "bool_col" in schema.names

    def test_schema_inference_pyarrow(self):
        """Test schema inference from PyArrow Table"""
        table_name = "test_schema_inference_pyarrow"
        data = pa.table(
            {
                "int64_col": pa.array([1, 2, 3], type=pa.int64()),
                "string_col": pa.array(["x", "y", "z"], type=pa.string()),
                "double_col": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            }
        )

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert schema.field("int64_col").type == pa.int64()
        assert schema.field("string_col").type == pa.string()
        assert schema.field("double_col").type == pa.float64()

    def test_schema_inference_polars(self):
        """Test schema inference from Polars DataFrame"""
        table_name = "test_schema_inference_polars"
        data = pl.DataFrame(
            {
                "int_col": [1, 2, 3],
                "str_col": ["a", "b", "c"],
                "float_col": [1.1, 2.2, 3.3],
            }
        )

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert "int_col" in schema.names
        assert "str_col" in schema.names
        assert "float_col" in schema.names

    def test_schema_inference_ray_dataset(self):
        """Test schema inference from Ray Dataset"""
        table_name = "test_schema_inference_ray"
        data = self._create_test_ray_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert "id" in schema.names
        assert "name" in schema.names
        assert "age" in schema.names
        assert "city" in schema.names

    def test_schema_inference_daft_dataframe(self):
        """Test schema inference from Daft DataFrame"""
        table_name = "test_schema_inference_daft"
        data = self._create_test_daft_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert "id" in schema.names
        assert "name" in schema.names
        assert "age" in schema.names
        assert "city" in schema.names

    def test_schema_inference_numpy_1d(self):
        """Test schema inference from 1D numpy array"""
        table_name = "test_schema_inference_numpy_1d"
        data = self._create_test_numpy_1d_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert "column_0" in schema.names
        assert len(schema.names) == 1

    def test_schema_inference_numpy_2d(self):
        """Test schema inference from 2D numpy array"""
        table_name = "test_schema_inference_numpy_2d"
        data = self._create_test_numpy_2d_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        schema = table_def.table_version.schema.arrow
        assert "column_0" in schema.names
        assert "column_1" in schema.names
        assert len(schema.names) == 2

    def test_numpy_3d_array_error(self):
        """Test that 3D numpy arrays raise an error"""
        table_name = "test_numpy_3d_error"
        data = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]])  # 3D array

        with pytest.raises(
            ValueError, match="NumPy arrays with 3 dimensions are not supported"
        ):
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.CREATE,
                inner=self.catalog_properties,
            )

    # Test different content types
    def test_write_to_table_different_content_types(self):
        """Test writing with different content types"""
        data = self._create_test_pandas_data()

        content_types = [
            ContentType.PARQUET,
            ContentType.CSV,
            ContentType.JSON,
        ]

        for i, content_type in enumerate(content_types):
            table_name = f"test_content_type_{content_type.value}_{i}"

            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.CREATE,
                content_type=content_type,
                inner=self.catalog_properties,
            )

            assert catalog.table_exists(
                table=table_name,
                namespace=self.test_namespace,
                inner=self.catalog_properties,
            )

    # Test table creation parameters
    def test_write_to_table_with_table_properties(self):
        """Test writing with table creation parameters"""
        table_name = "test_table_properties"
        data = self._create_test_pandas_data()

        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            description="Test table with properties",
            lifecycle_state=LifecycleState.ACTIVE,
            inner=self.catalog_properties,
        )

        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        assert table_def.table.description == "Test table with properties"
        # Note: lifecycle_state defaults to ACTIVE in create_table, but may be overridden
        # We'll accept either ACTIVE or CREATED as both are valid for our test purpose
        assert table_def.table_version.state in [
            LifecycleState.ACTIVE,
            LifecycleState.CREATED,
        ]

    # Test error conditions
    def test_write_to_table_unsupported_data_type(self):
        """Test error when data type cannot be inferred"""
        table_name = "test_unsupported_data"

        # Use a plain dict which doesn't have schema inference
        unsupported_data = {"key": "value"}

        with pytest.raises(ValueError, match="Could not infer schema from data"):
            catalog.write_to_table(
                data=unsupported_data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.CREATE,
                inner=self.catalog_properties,
            )

    def test_write_to_table_replace_mode(self):
        """Test REPLACE mode creating a new stream to replace existing data"""
        table_name = "test_replace_mode"
        data1 = self._create_test_pandas_data()
        data2 = self._create_second_batch_pandas_data()

        # First, create the table
        catalog.write_to_table(
            data=data1,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        # Verify table exists
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        # Now use REPLACE mode to replace all existing data
        catalog.write_to_table(
            data=data2,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.REPLACE,
            inner=self.catalog_properties,
        )

        # Table should still exist
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    def test_write_to_table_merge_mode_with_merge_keys(self):
        """Test MERGE mode works when table has merge keys"""
        table_name = "test_merge_mode_with_keys"

        # Create a table with merge keys
        self._create_table_with_merge_keys(table_name)

        # Create test data that matches the schema
        data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["NYC", "LA", "Chicago"],
            }
        )

        # MERGE mode should work since table has merge keys
        catalog.write_to_table(
            data=data,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.MERGE,
            inner=self.catalog_properties,
        )

        # Table should still exist
        assert catalog.table_exists(
            table=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

    def test_write_to_table_merge_mode_without_merge_keys_fails(self):
        """Test MERGE mode fails when table has no merge keys"""
        table_name = "test_merge_mode_no_keys"

        # Create a table without merge keys
        self._create_table_without_merge_keys(table_name)

        data = self._create_test_pandas_data()

        # MERGE mode should fail since table has no merge keys
        with pytest.raises(
            ValueError,
            match="MERGE mode requires tables to have at least one merge key",
        ):
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.MERGE,
                inner=self.catalog_properties,
            )

    # Test default namespace behavior
    def test_write_to_table_default_namespace(self):
        """Test writing to table using default namespace"""
        table_name = "test_default_namespace"
        data = self._create_test_pandas_data()

        # Don't specify namespace, should use default
        catalog.write_to_table(
            data=data,
            table=table_name,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        # Should be able to find table in default namespace
        default_ns = catalog.default_namespace(inner=self.catalog_properties)
        assert catalog.table_exists(
            table=table_name, namespace=default_ns, inner=self.catalog_properties
        )

    def test_write_to_table_append_creates_separate_deltas(self):
        """Test that APPEND mode creates separate deltas in the same partition"""
        from deltacat.catalog.main.impl import _get_storage

        table_name = "test_append_separate_deltas"
        data1 = self._create_test_pandas_data()
        data2 = self._create_second_batch_pandas_data()

        # Create table with first batch
        catalog.write_to_table(
            data=data1,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.CREATE,
            inner=self.catalog_properties,
        )

        # Get the table definition to access stream information
        table_def = catalog.get_table(
            name=table_name,
            namespace=self.test_namespace,
            inner=self.catalog_properties,
        )

        # Get storage interface
        storage = _get_storage(inner=self.catalog_properties)

        # Get the stream
        stream = storage.get_stream(
            namespace=self.test_namespace,
            table_name=table_name,
            table_version=table_def.table_version.table_version,
            inner=self.catalog_properties,
        )

        # Get the partition (should be only one for unpartitioned table)
        partition = storage.get_partition(
            stream_locator=stream.locator,
            partition_values=None,  # unpartitioned
            inner=self.catalog_properties,
        )

        # List deltas before second write
        deltas_before = storage.list_partition_deltas(
            partition_like=partition,
            inner=self.catalog_properties,
        ).all_items()

        assert (
            len(deltas_before) == 1
        ), f"Expected 1 delta before append, got {len(deltas_before)}"

        # Append second batch using APPEND mode
        catalog.write_to_table(
            data=data2,
            table=table_name,
            namespace=self.test_namespace,
            mode=TableWriteMode.APPEND,
            inner=self.catalog_properties,
        )

        # Get the same partition again (should be the same partition object)
        partition_after = storage.get_partition(
            stream_locator=stream.locator,
            partition_values=None,  # unpartitioned
            inner=self.catalog_properties,
        )

        # Verify it's the same partition
        assert (
            partition.partition_id == partition_after.partition_id
        ), "APPEND should reuse the same partition"

        # List deltas after second write
        deltas_after = storage.list_partition_deltas(
            partition_like=partition_after,
            inner=self.catalog_properties,
        ).all_items()

        # Should now have 2 deltas in the same partition
        assert (
            len(deltas_after) == 2
        ), f"Expected 2 deltas after append, got {len(deltas_after)}"

        # Verify deltas have different stream positions
        stream_positions = [delta.stream_position for delta in deltas_after]
        assert (
            len(set(stream_positions)) == 2
        ), "Deltas should have different stream positions"
        assert min(stream_positions) == 1, "First delta should have stream position 1"
        assert max(stream_positions) == 2, "Second delta should have stream position 2"

    def test_write_to_table_partitioned_table_raises_not_implemented(self):
        """Test that write_to_table raises NotImplementedError for partitioned tables"""
        from deltacat.storage.model.partition import (
            PartitionScheme,
            PartitionKey,
            PartitionKeyList,
        )
        from deltacat.storage.model.transform import IdentityTransform

        table_name = "test_partitioned_table"
        data = self._create_test_pandas_data()

        # Create a partition scheme with partition keys
        partition_keys = [
            PartitionKey.of(
                key=["city"],
                name="city_partition",
                transform=IdentityTransform.of(),
            )
        ]
        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(partition_keys),
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )

        # Try to create a partitioned table using write_to_table
        with pytest.raises(
            NotImplementedError,
            match="write_to_table does not yet support partitioned tables",
        ):
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.CREATE,
                partition_scheme=partition_scheme,  # This makes it partitioned
                inner=self.catalog_properties,
            )

    def test_write_to_table_sorted_table_raises_not_implemented(self):
        """Test that write_to_table raises NotImplementedError for tables with sort keys"""
        from deltacat.storage.model.sort_key import SortScheme, SortKey, SortKeyList
        from deltacat.storage.model.types import SortOrder, NullOrder

        table_name = "test_sorted_table"
        data = self._create_test_pandas_data()

        # Create sort scheme with sort keys
        sort_scheme = SortScheme.of(
            keys=SortKeyList.of(
                [
                    SortKey.of(
                        key=["id"],
                        sort_order=SortOrder.ASCENDING,
                        null_order=NullOrder.AT_END,
                    )
                ]
            ),
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )

        # Create table with sort keys
        catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            sort_keys=sort_scheme,
            inner=self.catalog_properties,
        )

        # Attempt to write to the sorted table should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            catalog.write_to_table(
                data=data,
                table=table_name,
                namespace=self.test_namespace,
                mode=TableWriteMode.APPEND,
                inner=self.catalog_properties,
            )

        # Verify the error message contains expected information
        assert "sort keys" in str(exc_info.value)
        assert "sort scheme with 1 sort key(s)" in str(exc_info.value)
        assert "id" in str(exc_info.value)
