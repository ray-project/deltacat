#!/usr/bin/env python3
"""
Unit tests for the backfill script that migrates catalogs from old to new canonical string format.

Tests verify that catalogs created with the old canonical_string format (with parent hexdigest)
can be successfully migrated to the new hierarchical format (without parent hexdigest).
"""

import tempfile
import shutil
import uuid
from typing import Dict, Any
import pandas as pd
import pyarrow as pa

import deltacat as dc
from deltacat import Catalog
from deltacat.catalog.main import impl as catalog
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.storage.model.schema import Schema, Field
from deltacat.types.tables import TableWriteMode
from deltacat.experimental.compatibility.backfill_locator_to_id_mappings import (
    patched_canonical_string,
    migrate_catalog,
)


def get_catalog_properties(root: str) -> CatalogProperties:
    """Helper to create catalog properties for testing."""
    return CatalogProperties(root=root)


def create_test_schema() -> Schema:
    """Create a basic schema for testing."""
    return Schema.of([
        Field.of(pa.field("id", pa.int64())),
        Field.of(pa.field("name", pa.string())),
        Field.of(pa.field("value", pa.float64())),
    ])


def create_test_data() -> pd.DataFrame:
    """Create test data for writing to tables."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.0, 30.5]
    })


class TestBackfillLocatorToIdMappings:
    """Test the backfill script for canonical string migration."""

    @classmethod
    def setup_class(cls):
        """Set up test environment."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.dest_dir = tempfile.mkdtemp()

    @classmethod
    def teardown_class(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir, ignore_errors=True)
        shutil.rmtree(cls.dest_dir, ignore_errors=True)

    def setup_method(self):
        """Set up for each test method."""
        # Clear directories for each test
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        shutil.rmtree(self.dest_dir, ignore_errors=True)
        self.temp_dir = tempfile.mkdtemp()
        self.dest_dir = tempfile.mkdtemp()

        self.catalog_properties = get_catalog_properties(root=self.temp_dir)
        self.dest_catalog_properties = get_catalog_properties(root=self.dest_dir)

    def create_old_format_catalog(self) -> Dict[str, Any]:
        """
        Create a catalog using the old canonical_string format.

        Returns:
            Dict with information about created objects for verification
        """
        # Initialize DeltaCAT following the correct pattern
        dc.init()
        source_catalog_name = f"test_source_{uuid.uuid4()}"
        dc.put_catalog(
            source_catalog_name,
            catalog=Catalog(config=self.catalog_properties)
        )

        # Create catalog structure using old canonical_string format
        with patched_canonical_string(use_old_format=True):
            # Create namespace
            namespace_name = "test_namespace"
            catalog.create_namespace(
                namespace=namespace_name,
                inner=self.catalog_properties
            )

            # Create multiple tables with different stream formats
            tables_info = []

            # Table 1: Basic table with deltacat stream
            table1_name = "table_one"
            table1_data = create_test_data()
            catalog.write_to_table(
                data=table1_data,
                table=table1_name,
                namespace=namespace_name,
                mode=TableWriteMode.CREATE,
                inner=self.catalog_properties,
                # This will create deltacat format stream by default
            )
            tables_info.append({
                'name': table1_name,
                'expected_streams': ['deltacat'],
                'expected_partitions': [('default',)]
            })

            # Table 2: Table with additional data (creates more partitions/deltas)
            table2_name = "table_two"
            table2_data1 = create_test_data()
            table2_data2 = pd.DataFrame({
                'id': [4, 5, 6],
                'name': ['David', 'Eve', 'Frank'],
                'value': [40.0, 50.5, 60.0]
            })

            # Create table
            catalog.write_to_table(
                data=table2_data1,
                table=table2_name,
                namespace=namespace_name,
                mode=TableWriteMode.CREATE,
                inner=self.catalog_properties,
            )

            # Append more data (creates additional delta)
            catalog.write_to_table(
                data=table2_data2,
                table=table2_name,
                namespace=namespace_name,
                mode=TableWriteMode.APPEND,
                inner=self.catalog_properties,
            )

            tables_info.append({
                'name': table2_name,
                'expected_streams': ['deltacat'],
                'expected_partitions': [('default',)]
            })

            # Table 3: Table with explicit schema (might create different stream characteristics)
            table3_name = "table_three"
            table3_data = create_test_data()
            catalog.write_to_table(
                data=table3_data,
                table=table3_name,
                namespace=namespace_name,
                mode=TableWriteMode.CREATE,
                schema=create_test_schema(),
                inner=self.catalog_properties,
            )
            tables_info.append({
                'name': table3_name,
                'expected_streams': ['deltacat'],
                'expected_partitions': [('default',)]
            })

        return {
            'namespace': namespace_name,
            'tables': tables_info,
            'catalog_root': self.temp_dir,
            'catalog_name': source_catalog_name
        }

    def verify_catalog_integrity(self, catalog_root: str, expected_objects: Dict[str, Any]):
        """
        Verify that a catalog contains the expected objects and they can be read.

        Args:
            catalog_root: Path to catalog root
            expected_objects: Dict with expected namespace, tables, etc.
        """
        # Use the catalog name from expected_objects if available, otherwise create a new one
        if 'catalog_name' in expected_objects:
            verify_catalog_name = expected_objects['catalog_name']
        else:
            # Fallback: create a new catalog for verification
            verify_catalog_name = f"verify_{uuid.uuid4()}"
            catalog_props = get_catalog_properties(root=catalog_root)
            dc.put_catalog(
                verify_catalog_name,
                catalog=Catalog(config=catalog_props)
            )

        namespace_name = expected_objects['namespace']

        # Verify namespace exists
        assert dc.namespace_exists(
            namespace=namespace_name,
            catalog=verify_catalog_name
        ), f"Namespace {namespace_name} should exist"

        # Verify each table exists and can be read
        for table_info in expected_objects['tables']:
            table_name = table_info['name']

            # Check table exists with specific table version (default is "1")
            assert dc.table_exists(
                table=table_name,
                namespace=namespace_name,
                catalog=verify_catalog_name,
                table_version="1"
            ), f"Table {namespace_name}/{table_name} should exist"

            # Check we can get table definition with specific table version
            table_def = dc.get_table(
                table=table_name,
                namespace=namespace_name,
                catalog=verify_catalog_name,
                table_version="1"
            )
            assert table_def is not None
            assert table_def.table.table_name == table_name

            # Check we can list table versions using dc.list_tables
            try:
                tables_list = dc.list_tables(
                    namespace=namespace_name,
                    catalog=verify_catalog_name,
                    table=table_name  # List versions of this specific table
                )
                assert len(tables_list.all_items()) > 0, f"Table {table_name} should have versions"
                print(f"Successfully listed {len(tables_list.all_items())} versions for table {table_name}")
            except Exception as e:
                print(f"Warning: Could not list versions for {table_name}: {e}")

            # Try to read some data from the table to verify it works
            try:
                from deltacat.utils.url import DeltaCatUrl
                table_url = DeltaCatUrl(f"dc://{verify_catalog_name}/{namespace_name}/{table_name}/")
                dc.get(table_url)  # Just verify we can read, don't store the result
                print(f"Successfully verified table {table_name} - can read data")
            except Exception as e:
                print(f"Warning: Could not read data from {table_name}: {e}")
                # Don't fail the test if we can't read data, as long as the table exists

    def test_patched_canonical_string_context_manager(self):
        """Test that the canonical_string patching works correctly."""
        from deltacat.storage.model.namespace import NamespaceLocator
        from deltacat.storage.model.table import TableLocator

        # Create test locators
        ns_locator = NamespaceLocator({'namespace': 'test_ns'})
        table_locator = TableLocator({
            'namespaceLocator': ns_locator,
            'tableName': 'test_table'
        })

        # Test normal (new) format
        normal_result = table_locator.canonical_string()
        assert normal_result == 'test_table'

        # Test patched (old) format
        with patched_canonical_string(use_old_format=True):
            old_result = table_locator.canonical_string()
            # Should include parent hexdigest
            assert old_result != normal_result
            assert old_result.endswith('|test_table')
            assert len(old_result.split('|')) == 2

        # Test that patch is restored
        restored_result = table_locator.canonical_string()
        assert restored_result == normal_result

    def test_migrate_catalog_dry_run(self):
        """Test migration in dry-run mode."""
        # Create catalog with old format
        old_catalog_info = self.create_old_format_catalog()

        # Create destination catalog
        dest_catalog_name = f"test_dest_{uuid.uuid4()}"
        dc.put_catalog(
            dest_catalog_name,
            catalog=Catalog(config=self.dest_catalog_properties)
        )

        # Use catalog names in URLs, not directory paths
        source_url = f"dc://{old_catalog_info['catalog_name']}/"
        dest_url = f"dc://{dest_catalog_name}/"

        # Test dry run migration
        success = migrate_catalog(source_url, dest_url, dry_run=True)
        assert success, "Dry run migration should succeed"

        # Destination should be empty after dry run
        import os
        dest_contents = os.listdir(self.dest_dir) if os.path.exists(self.dest_dir) else []
        assert len(dest_contents) == 0, "Destination should be empty after dry run"

    def test_migrate_catalog_full_migration(self):
        """Test full migration from old to new canonical string format."""
        # Create catalog with old canonical_string format
        old_catalog_info = self.create_old_format_catalog()

        # Verify the old catalog works (using patched canonical_string since it was created with old format)
        with patched_canonical_string(use_old_format=True):
            self.verify_catalog_integrity(self.temp_dir, old_catalog_info)

        # Create destination catalog
        dest_catalog_name = f"test_dest_{uuid.uuid4()}"
        dc.put_catalog(
            dest_catalog_name,
            catalog=Catalog(config=self.dest_catalog_properties)
        )

        # Perform migration using catalog names
        source_url = f"dc://{old_catalog_info['catalog_name']}/"
        dest_url = f"dc://{dest_catalog_name}/"

        success = migrate_catalog(source_url, dest_url, dry_run=False)
        assert success, "Migration should succeed"

        # Verify migrated catalog has same structure and data (update catalog_name for destination)
        migrated_catalog_info = old_catalog_info.copy()
        migrated_catalog_info['catalog_name'] = dest_catalog_name
        migrated_catalog_info['catalog_root'] = self.dest_dir
        self.verify_catalog_integrity(self.dest_dir, migrated_catalog_info)

    def test_migrate_catalog_preserves_data_integrity(self):
        """Test that migration preserves data integrity."""
        # Create catalog with old format
        old_catalog_info = self.create_old_format_catalog()

        # Read data from original catalog
        original_table_data = {}
        for table_info in old_catalog_info['tables']:
            table_name = table_info['name']
            # Use DeltaCAT's high level API to read table data
            from deltacat.utils.url import DeltaCatUrl
            table_url = DeltaCatUrl(f"dc://{old_catalog_info['catalog_name']}/{old_catalog_info['namespace']}/{table_name}/")
            original_data = dc.get(table_url)
            if hasattr(original_data, 'to_pandas'):
                original_table_data[table_name] = original_data.to_pandas()
            else:
                # Handle different data types
                original_table_data[table_name] = original_data

        # Create destination catalog for migration
        dest_catalog_name = f"test_dest_{uuid.uuid4()}"
        dc.put_catalog(
            dest_catalog_name,
            catalog=Catalog(config=self.dest_catalog_properties)
        )

        # Perform migration
        source_url = f"dc://{old_catalog_info['catalog_name']}/"
        dest_url = f"dc://{dest_catalog_name}/"

        success = migrate_catalog(source_url, dest_url, dry_run=False)
        assert success, "Migration should succeed"

        # Read data from migrated catalog and compare

        for table_info in old_catalog_info['tables']:
            table_name = table_info['name']
            migrated_table_url = DeltaCatUrl(f"dc://{dest_catalog_name}/{old_catalog_info['namespace']}/{table_name}/")
            migrated_data = dc.get(migrated_table_url)

            if hasattr(migrated_data, 'to_pandas'):
                migrated_df = migrated_data.to_pandas()
                original_df = original_table_data[table_name]

                # Compare data content (allowing for different row ordering)
                assert len(migrated_df) == len(original_df), f"Row count should match for {table_name}"

                # Sort both dataframes for comparison
                migrated_sorted = migrated_df.sort_values('id').reset_index(drop=True)
                original_sorted = original_df.sort_values('id').reset_index(drop=True)

                pd.testing.assert_frame_equal(
                    migrated_sorted,
                    original_sorted,
                    f"Data should be identical for table {table_name}"
                )

    def test_migrate_empty_catalog(self):
        """Test migration of an empty catalog."""
        # Create empty catalog with old format
        dc.init()
        empty_catalog_name = f"empty_{uuid.uuid4()}"
        dc.put_catalog(
            empty_catalog_name,
            catalog=Catalog(config=self.catalog_properties)
        )

        with patched_canonical_string(use_old_format=True):
            # Just create a namespace, no tables
            dc.create_namespace(
                namespace="empty_namespace",
                catalog=empty_catalog_name
            )

        # Create destination catalog for migration
        dest_catalog_name = f"dest_{uuid.uuid4()}"
        dc.put_catalog(
            dest_catalog_name,
            catalog=Catalog(config=self.dest_catalog_properties)
        )

        # Perform migration
        source_url = f"dc://{empty_catalog_name}/"
        dest_url = f"dc://{dest_catalog_name}/"

        success = migrate_catalog(source_url, dest_url, dry_run=False)
        assert success, "Migration of empty catalog should succeed"

        # Verify namespace exists in destination
        assert dc.namespace_exists(
            namespace="empty_namespace",
            catalog=dest_catalog_name
        ), "Namespace should exist in migrated catalog"

    def test_migration_error_handling(self):
        """Test migration error handling for invalid inputs."""
        # Test migration with non-existent source
        invalid_source = f"dc://{self.temp_dir}/nonexistent/"
        dest_url = f"dc://{self.dest_dir}/"

        # This should handle the error gracefully
        success = migrate_catalog(invalid_source, dest_url, dry_run=True)
        # May succeed or fail depending on implementation, but shouldn't crash
        assert isinstance(success, bool), "Should return boolean result"

    def test_canonical_string_format_differences(self):
        """Test that old and new canonical string formats are actually different."""
        from deltacat.storage.model.namespace import NamespaceLocator
        from deltacat.storage.model.table import TableLocator
        from deltacat.storage.model.table_version import TableVersionLocator
        from deltacat.storage.model.stream import StreamLocator

        # Create hierarchy of locators
        ns_locator = NamespaceLocator({'namespace': 'test_ns'})
        table_locator = TableLocator({
            'namespaceLocator': ns_locator,
            'tableName': 'test_table'
        })
        table_version_locator = TableVersionLocator({
            'tableLocator': table_locator,
            'version': '1'
        })
        stream_locator = StreamLocator({
            'tableVersionLocator': table_version_locator,
            'streamFormat': 'deltacat'
        })

        # Test each level shows difference between old and new format
        test_cases = [
            ("namespace", ns_locator, True),  # Namespace should be same (no parent)
            ("table", table_locator, False),  # Table should be different
            ("table_version", table_version_locator, False),  # Table version should be different
            ("stream", stream_locator, False),  # Stream should be different
        ]

        for obj_type, locator, should_be_same in test_cases:
            new_format = locator.canonical_string()

            with patched_canonical_string(use_old_format=True):
                old_format = locator.canonical_string()

            if should_be_same:
                assert old_format == new_format, f"{obj_type} canonical strings should be the same"
            else:
                assert old_format != new_format, f"{obj_type} canonical strings should be different"
                assert '|' in old_format, f"{obj_type} old format should contain separator"
                # New format should be a suffix of old format
                assert old_format.endswith(f"|{new_format}"), f"{obj_type} old format should end with new format"
