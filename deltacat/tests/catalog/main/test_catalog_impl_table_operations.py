import shutil
import tempfile
import pytest
import pyarrow as pa

import deltacat.catalog.v2.catalog_impl as catalog
from deltacat.catalog.catalog_properties import initialize_properties
from deltacat.storage.model.schema import Schema
from deltacat.storage.model.sort_key import SortKey, SortScheme
from deltacat.storage.model.table import TableProperties
from deltacat.storage.model.namespace import NamespaceProperties
from deltacat.storage.model.types import LifecycleState


class TestCatalogTableOperations:
    temp_dir = None
    catalog_properties = None

    @classmethod
    def setup_class(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.catalog_properties = initialize_properties(root=cls.temp_dir)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.temp_dir)

    def setup_method(self):
        """Setup for each test - create a test namespace"""
        self.test_namespace = "test_table_namespace"
        if not catalog.namespace_exists(self.test_namespace, catalog=self.catalog_properties):
            catalog.create_namespace(
                namespace=self.test_namespace, 
                properties={"description": "Test Table Namespace"}, 
                catalog=self.catalog_properties
            )

    def create_sample_arrow_schema(self):
        """Create a sample PyArrow schema for testing"""
        return pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
            pa.field('value', pa.float64()),
        ])

    def create_sample_sort_keys(self):
        """Create a sample sort scheme for testing"""
        from deltacat.storage.model.sort_key import SortOrder, NullOrder
        
        return SortScheme(keys=[
            SortKey.of(
                key=["id"],
                sort_order=SortOrder.ASCENDING,
                null_order=NullOrder.AT_END
            ),
        ])

    def test_create_table(self):
        """Test creating a table with schema and properties"""
        table_name = "test_create_table"
        
        # Create a schema
        arrow_schema = self.create_sample_arrow_schema()
        schema = Schema(arrow=arrow_schema)
        
        # Create sort keys
        sort_keys = self.create_sample_sort_keys()
        
        # Create table properties
        table_properties = TableProperties({"owner": "test-user", "department": "engineering"})
        
        # Create namespace properties
        namespace_properties = NamespaceProperties({"description": "Test Namespace"})
        
        # Create the table
        table_definition = catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            sort_keys=sort_keys,
            description="Test table for unit tests",
            table_properties=table_properties,
            namespace_properties=namespace_properties,
            catalog=self.catalog_properties
        )
        
        # Verify table was created
        assert catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)

        table = table_definition.table
        table_version = table_definition.table_version

        # Verify table definition properties
        assert table_version.table_name == table_name
        assert table_version.namespace == self.test_namespace
        assert table_version.description == "Test table for unit tests"
        assert table_version.state == LifecycleState.UNRELEASED
        assert table.properties.get("owner") == "test-user"
        assert table.properties.get("department") == "engineering"
        assert table_version.schema.arrow.names == arrow_schema.names
        assert len(table_version.sort_scheme.keys) == 1
        sort_key_paths = [key[0][0] for key in table_version.sort_scheme.keys]
        assert "id" in sort_key_paths

    def test_create_table_already_exists(self):
        table_name = "test_table_exists"
        
        # Create the table
        catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            description="First creation",
            catalog=self.catalog_properties
        )
        
        # Verify table exists
        assert catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Try to create the same table again, should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            catalog.create_table(
                name=table_name,
                namespace=self.test_namespace,
                description="Second creation attempt",
                catalog=self.catalog_properties
            )
        
        # Verify the error message
        assert f"Table {self.test_namespace}.{table_name} already exists" in str(excinfo.value)

    def test_create_table_already_exists_no_fail(self):
        """Test creating a table that already exists with fail_if_exists=False"""
        table_name = "test_table_exists_no_fail"
        
        # Create the table with original description
        catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            description="Original description",
            catalog=self.catalog_properties
        )

        assert catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Create the same table with fail_if_exists=False
        table_definition = catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            description="Updated description",
            fail_if_exists=False,
            catalog=self.catalog_properties
        )
        
        table = table_definition.table

        assert table.table_name == table_name
        assert table.namespace == self.test_namespace
        # Ensure desccription is unchanged
        assert table.description == "Original description"

    def test_drop_table(self):
        table_name = "test_drop_table"
        
        # Create the table
        catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_properties
        )
        
        # Verify table exists
        assert catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Drop the table
        catalog.drop_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_properties
        )
        
        # Verify table no longer exists
        assert not catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)

    def test_drop_table_not_exists(self):
        table_name = "nonexistent_table"
        
        # Verify table doesn't exist
        assert not catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Try to drop the table, should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            catalog.drop_table(
                table=table_name,
                namespace=self.test_namespace,
                catalog=self.catalog_properties
            )
        
        # Verify the error message
        assert f"Table {self.test_namespace}.{table_name} does not exist" in str(excinfo.value)

    def test_rename_table(self):
        original_name = "test_original_table"
        new_name = "test_renamed_table"
        
        # Create the table with original name
        catalog.create_table(
            name=original_name,
            namespace=self.test_namespace,
            description="Table to be renamed",
            catalog=self.catalog_properties
        )
        
        # Verify original table exists
        assert catalog.table_exists(original_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Rename the table
        catalog.rename_table(
            table=original_name,
            new_name=new_name,
            namespace=self.test_namespace,
            catalog=self.catalog_properties
        )
        
        # Verify new table exists and old table doesn't
        assert catalog.table_exists(new_name, self.test_namespace, catalog=self.catalog_properties)
        assert not catalog.table_exists(original_name, self.test_namespace, catalog=self.catalog_properties)

    def test_rename_table_not_exists(self):
        original_name = "nonexistent_table"
        new_name = "test_renamed_nonexistent"
        
        # Verify table doesn't exist
        assert not catalog.table_exists(original_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Try to rename the table, should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            catalog.rename_table(
                table=original_name,
                new_name=new_name,
                namespace=self.test_namespace,
                catalog=self.catalog_properties
            )
        
        # Verify the error message
        assert f"Table {self.test_namespace}.{original_name} does not exist" in str(excinfo.value)

    def test_table_exists(self):
        existing_table = "test_table_exists_check"
        non_existing_table = "nonexistent_table"
        
        # Create a table
        catalog.create_table(
            name=existing_table,
            namespace=self.test_namespace,
            catalog=self.catalog_properties
        )
        
        # Check existing table
        assert catalog.table_exists(existing_table, self.test_namespace, catalog=self.catalog_properties)
        
        # Check non-existing table
        assert not catalog.table_exists(non_existing_table, self.test_namespace, catalog=self.catalog_properties)

    def test_create_table_with_default_namespace(self):
        table_name = "test_default_namespace_table"
        
        # Create table with default namespace
        table_definition = catalog.create_table(
            name=table_name,
            catalog=self.catalog_properties
        )
        
        table = table_definition.table
        # Verify table was created in default namespace
        default_ns = catalog.default_namespace()
        assert table.namespace == default_ns
        assert catalog.table_exists(table_name, default_ns, catalog=self.catalog_properties)

    def test_create_table_with_missing_namespace(self):
        table_name = "test_auto_create_namespace_table"
        new_namespace = "auto_created_namespace"
        namespace_properties = NamespaceProperties({"auto_created": True})
        
        # Verify namespace doesn't exist yet
        assert not catalog.namespace_exists(new_namespace, catalog=self.catalog_properties)
        
        # Create table with new namespace and namespace properties
        catalog.create_table(
            name=table_name,
            namespace=new_namespace,
            namespace_properties=namespace_properties,
            catalog=self.catalog_properties
        )
        
        # Verify both namespace and table were created
        assert catalog.namespace_exists(new_namespace, catalog=self.catalog_properties)
        assert catalog.table_exists(table_name, new_namespace, catalog=self.catalog_properties)
        
        # Verify namespace properties were set
        namespace = catalog.get_namespace(new_namespace, catalog=self.catalog_properties)
        assert namespace.properties["auto_created"] == True

    def test_alter_table(self):
        table_name = "test_alter_table"
        
        # Create initial schema and properties
        arrow_schema = self.create_sample_arrow_schema()
        schema = Schema(arrow=arrow_schema)
        initial_sort_keys = self.create_sample_sort_keys()
        initial_properties = TableProperties({"owner": "original-user", "department": "engineering"})
        
        # Create the table with initial properties
        table_definition = catalog.create_table(
            name=table_name,
            namespace=self.test_namespace,
            schema=schema,
            sort_keys=initial_sort_keys,
            description="Initial description",
            table_properties=initial_properties,
            catalog=self.catalog_properties
        )
        
        # Verify table was created with initial properties
        assert catalog.table_exists(table_name, self.test_namespace, catalog=self.catalog_properties)
        
        # Create updated schema
        updated_arrow_schema = pa.schema([
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
            pa.field('value', pa.float64()),
            pa.field('timestamp', pa.timestamp('us'))  # Added field
        ])
        
        # Create updated sort keys
        from deltacat.storage.model.sort_key import SortOrder, NullOrder
        updated_sort_keys = SortScheme(keys=[
            SortKey.of(
                key=["timestamp"],
                sort_order=SortOrder.DESCENDING,
                null_order=NullOrder.AT_START
            ),
            SortKey.of(
                key=["id"],
                sort_order=SortOrder.ASCENDING,
                null_order=NullOrder.AT_END
            )
        ])
        
        # Create updated properties
        updated_properties = TableProperties({
            "owner": "new-user", 
            "department": "data-science",
            "priority": "high"
        })
        
        # Alter the table with new properties
        catalog.alter_table(
            table=table_name,
            namespace=self.test_namespace,
            schema_updates={"schema": Schema(arrow=updated_arrow_schema)},
            sort_keys=updated_sort_keys,
            description="Updated description",
            properties=updated_properties,
            catalog=self.catalog_properties
        )
        
        # Get the updated table definition
        updated_table_def = catalog.get_table(
            table=table_name,
            namespace=self.test_namespace,
            catalog=self.catalog_properties
        )
        
        updated_table = updated_table_def.table
        updated_table_version = updated_table_def.table_version
        
        # Verify table properties were updated
        assert updated_table_version.description == "Updated description"
        assert updated_table_version.state == LifecycleState.ACTIVE
        assert updated_table.properties.get("owner") == "new-user"
        assert updated_table.properties.get("department") == "data-science"
        assert updated_table.properties.get("priority") == "high"
        
        # Verify schema has been updated with the new field
        assert "timestamp" in updated_table_version.schema.arrow.names
        
        # Verify sort keys were updated
        assert len(updated_table_version.sort_scheme.keys) == 2
        sort_key_paths = [k.key[0] for k in updated_table_version.sort_scheme.keys]
        assert "timestamp" in sort_key_paths
        assert "id" in sort_key_paths
        
        # Verify the first sort key is now timestamp
        assert updated_table_version.sort_scheme.keys[0].key[0] == "timestamp"
        assert updated_table_version.sort_scheme.keys[0].sort_order == SortOrder.DESCENDING

    def test_alter_table_not_exists(self):
        """Test altering a table that doesn't exist"""
        nonexistent_table = "nonexistent_alter_table"
        
        # Verify table doesn't exist
        assert not catalog.table_exists(
            nonexistent_table, 
            self.test_namespace, 
            catalog=self.catalog_properties
        )
        
        # Try to alter the nonexistent table, should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            catalog.alter_table(
                table=nonexistent_table,
                namespace=self.test_namespace,
                description="Updated description",
                catalog=self.catalog_properties
            )
        
        # Verify the error message
        assert f"Table {self.test_namespace}.{nonexistent_table} does not exist" in str(excinfo.value)
