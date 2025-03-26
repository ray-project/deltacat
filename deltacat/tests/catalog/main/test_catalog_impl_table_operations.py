import shutil
import tempfile

import pytest
import pyarrow as pa

import deltacat.catalog.main as catalog
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

    if not catalog.namespace_exists(namespace_name, catalog=catalog_properties):
        catalog.create_namespace(
            namespace=namespace_name,
            properties={"description": "Test Table Namespace"},
            catalog=catalog_properties,
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
            catalog=catalog_properties,
        )

        # Verify table was created
        assert catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
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
            catalog=catalog_properties,
        )

        # Verify table exists
        assert catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
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
                catalog=catalog_properties,
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
            catalog=catalog_properties,
        )

        assert catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Create the same table with fail_if_exists=False
        table_definition = catalog.create_table(
            name=table_name,
            namespace=namespace_name,
            description="Updated description",
            fail_if_exists=False,
            catalog=catalog_properties,
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
            name=table_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Verify table exists
        assert catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Drop the table
        catalog.drop_table(
            name=table_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Verify table no longer exists
        assert not catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
        )

    def test_drop_table_not_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        table_name = "nonexistent_table"

        # Verify table doesn't exist
        assert not catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Try to drop the table, should raise TableNotFoundError
        with pytest.raises(TableNotFoundError, match=table_name):
            catalog.drop_table(
                name=table_name, namespace=namespace_name, catalog=catalog_properties
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
            catalog=catalog_properties,
        )

        # Verify original table exists
        assert catalog.table_exists(
            original_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Rename the table
        catalog.rename_table(
            table=original_name,
            new_name=new_name,
            namespace=namespace_name,
            catalog=catalog_properties,
        )

        # Verify new table exists and old table doesn't
        assert catalog.table_exists(
            new_name, namespace=namespace_name, catalog=catalog_properties
        )
        assert not catalog.table_exists(
            original_name, namespace=namespace_name, catalog=catalog_properties
        )

    def test_rename_table_not_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        original_name = "nonexistent_table"
        new_name = "test_renamed_nonexistent"

        # Verify table doesn't exist
        assert not catalog.table_exists(
            original_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Try to rename the table, should raise TableNotFoundError
        with pytest.raises(TableNotFoundError, match=original_name):
            catalog.rename_table(
                table=original_name,
                new_name=new_name,
                namespace=namespace_name,
                catalog=catalog_properties,
            )

    def test_table_exists(self, test_namespace):
        namespace_name, catalog_properties = test_namespace
        existing_table = "test_table_exists_check"
        non_existing_table = "nonexistent_table"

        # Create a table
        catalog.create_table(
            name=existing_table, namespace=namespace_name, catalog=catalog_properties
        )

        # Check existing table
        assert catalog.table_exists(
            existing_table, namespace=namespace_name, catalog=catalog_properties
        )

        # Check non-existing table
        assert not catalog.table_exists(
            non_existing_table, namespace=namespace_name, catalog=catalog_properties
        )

    def test_create_table_with_default_namespace(self, catalog_setup):
        _, catalog_properties = catalog_setup
        table_name = "test_default_namespace_table"

        # Create table with default namespace
        table_definition = catalog.create_table(
            name=table_name, catalog=catalog_properties
        )

        table = table_definition.table
        # Verify table was created in default namespace
        default_ns = catalog.default_namespace()
        assert table.namespace == default_ns
        assert catalog.table_exists(
            table_name, namespace=default_ns, catalog=catalog_properties
        )

    def test_create_table_with_missing_namespace(self, catalog_setup):
        _, catalog_properties = catalog_setup
        table_name = "test_namespace_not_found_table"
        new_namespace = "nonexistent_namespace"

        # Verify namespace doesn't exist yet
        assert not catalog.namespace_exists(new_namespace, catalog=catalog_properties)

        # Try to create table with non-existent namespace
        catalog.create_table(
            name=table_name, namespace=new_namespace, catalog=catalog_properties
        )

        assert catalog.table_exists(
            table_name, namespace=new_namespace, catalog=catalog_properties
        )
        assert catalog.namespace_exists(new_namespace, catalog=catalog_properties)

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
            catalog=catalog_properties,
        )
        old_schema = table.table_version.schema

        # Verify table was created with initial properties
        assert catalog.table_exists(
            table_name, namespace=namespace_name, catalog=catalog_properties
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
            catalog=catalog_properties,
        )

        # Get the updated table definition
        updated_table_def = catalog.get_table(
            table_name, namespace=namespace_name, catalog=catalog_properties
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
            nonexistent_table, namespace=namespace_name, catalog=catalog_properties
        )

        # Try to alter the nonexistent table, should raise TableNotFoundError
        with pytest.raises(TableNotFoundError, match=nonexistent_table):
            catalog.alter_table(
                table=nonexistent_table,
                namespace=namespace_name,
                description="Updated description",
                catalog=catalog_properties,
            )

    def test_drop_with_purge_validation(self, test_namespace):
        """Test that using purge flag raises ValidationError"""
        namespace_name, catalog_properties = test_namespace
        table_name = "test_drop_with_purge"

        # Create the table
        catalog.create_table(
            name=table_name, namespace=namespace_name, catalog=catalog_properties
        )

        # Try to drop with purge=True, should raise ValidationError
        with pytest.raises(
            NotImplementedError, match="Purge flag is not currently supported"
        ):
            catalog.drop_table(
                name=table_name,
                namespace=namespace_name,
                purge=True,
                catalog=catalog_properties,
            )
