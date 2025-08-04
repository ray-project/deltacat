import shutil
import tempfile

import deltacat as dc
from deltacat.constants import METAFILE_FORMAT_MSGPACK
from deltacat import Namespace, DeltaCatUrl, DatasetType
from deltacat.storage import Metafile
from deltacat.catalog import write_to_table
from deltacat.types.media import ContentType
from deltacat.types.tables import TableWriteMode
import pandas as pd

from deltacat.io import (
    METAFILE_TYPE_COLUMN_NAME,
    METAFILE_DATA_COLUMN_NAME,
)


class TestDeltaCAT:
    @classmethod
    def setup_method(cls):
        cls.temp_dir_1 = tempfile.mkdtemp()
        cls.temp_dir_2 = tempfile.mkdtemp()
        # Initialize DeltaCAT with two local catalogs.
        dc.init()
        dc.put(DeltaCatUrl("dc://test_catalog_1"), root=cls.temp_dir_1)
        dc.put(DeltaCatUrl("dc://test_catalog_2"), root=cls.temp_dir_2)

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.temp_dir_1)
        shutil.rmtree(cls.temp_dir_2)

    def test_cross_catalog_namespace_copy(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is copied across catalogs.
        namespace_src = dc.put(DeltaCatUrl("dc://test_catalog_1/test_namespace"))
        namespace_dst = dc.copy(
            DeltaCatUrl("dc://test_catalog_1/test_namespace"),
            DeltaCatUrl("dc://test_catalog_2/test_namespace"),
        )
        # Expect the catalog namespace created in each catalog
        # method to be equivalent but not equal to the source namespace
        # (due to different metafile IDs).
        assert namespace_src.equivalent_to(namespace_dst)
        assert not namespace_src == namespace_dst

        # When each catalog namespace is fetched explicitly
        # Expect them to be equivalent but not equal
        # (due to different metafile IDs).
        actual_namespace_src = dc.get(DeltaCatUrl("dc://test_catalog_1/test_namespace"))
        actual_namespace_dst = dc.get(DeltaCatUrl("dc://test_catalog_2/test_namespace"))
        assert actual_namespace_src.equivalent_to(actual_namespace_dst)
        assert actual_namespace_src == namespace_src
        assert not actual_namespace_src == actual_namespace_dst
        assert namespace_dst == actual_namespace_dst

    def test_catalog_listing_shallow_local_metafiles(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is put in the catalog.
        namespace_src: Namespace = dc.put(
            DeltaCatUrl("dc://test_catalog_1/test_namespace")
        )
        # Expect the namespace to be listed.
        assert any(
            namespace_src.equivalent_to(other)
            for other in dc.list(DeltaCatUrl("dc://test_catalog_1"))
        )

    def test_catalog_listing_shallow_ray_dataset(self):
        # Given two empty DeltaCAT catalogs.
        # When a namespace is put in the catalog.
        namespace_src: Namespace = dc.put(
            DeltaCatUrl("dc://test_catalog_1/test_namespace")
        )
        # Expect the namespace to be listed.
        dataset = dc.list(
            DeltaCatUrl("dc://test_catalog_1"),
            dataset_type=DatasetType.RAY_DATASET,
        )
        actual_namespace = Metafile.deserialize(
            serialized=dataset.take(1)[0][METAFILE_DATA_COLUMN_NAME],
            meta_format=METAFILE_FORMAT_MSGPACK,
        )
        assert actual_namespace.equivalent_to(namespace_src)
        namespace_type = dataset.take(1)[0][METAFILE_TYPE_COLUMN_NAME]
        assert namespace_type == "Namespace"

    def test_recursive_listing_multiple_namespaces_with_tables(self):
        """
        Test that recursive listing correctly processes ALL namespaces, not just the first one.
        This guards against the bug where metafiles variable was overwritten in the recursive loop.
        """
        # Create multiple namespaces with tables and data
        dc.put(DeltaCatUrl("dc://test_catalog_1/namespace_alpha"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/namespace_beta"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/namespace_gamma"))

        # Create tables with data in each namespace
        test_data = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        # Create tables in each namespace
        write_to_table(
            data=test_data,
            table="table1",
            namespace="namespace_alpha",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        write_to_table(
            data=test_data,
            table="table2",
            namespace="namespace_beta",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        write_to_table(
            data=test_data,
            table="table3",
            namespace="namespace_gamma",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        # Test recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Verify we found objects from ALL namespaces
        namespace_names = set()
        table_names = set()
        delta_count = 0

        for obj in all_objects:
            if hasattr(obj, "namespace") and obj.namespace:
                namespace_names.add(obj.namespace)
            if hasattr(obj, "table_name") and obj.table_name:
                table_names.add(obj.table_name)
            if hasattr(obj, "type") and obj.type and "delta" in str(obj.type).lower():
                delta_count += 1

        # Assert we found all namespaces (guards against the bug)
        assert (
            "namespace_alpha" in namespace_names
        ), f"Missing namespace_alpha. Found: {namespace_names}"
        assert (
            "namespace_beta" in namespace_names
        ), f"Missing namespace_beta. Found: {namespace_names}"
        assert (
            "namespace_gamma" in namespace_names
        ), f"Missing namespace_gamma. Found: {namespace_names}"

        # Assert we found all tables (guards against incomplete traversal)
        assert "table1" in table_names, f"Missing table1. Found: {table_names}"
        assert "table2" in table_names, f"Missing table2. Found: {table_names}"
        assert "table3" in table_names, f"Missing table3. Found: {table_names}"

        # Assert we found deltas (indicates deep traversal worked)
        assert delta_count >= 3, f"Expected at least 3 deltas, found {delta_count}"

    def test_recursive_listing_multiple_tables_per_namespace(self):
        """
        Test that recursive listing finds all tables within a namespace, not just the first one.
        """
        # Create one namespace with multiple tables
        dc.put(DeltaCatUrl("dc://test_catalog_1/multi_table_namespace"))

        test_data = pd.DataFrame({"id": [1, 2], "value": ["x", "y"]})

        # Create multiple tables in the same namespace
        table_names = ["events", "users", "products", "orders"]
        for table_name in table_names:
            write_to_table(
                data=test_data,
                table=table_name,
                namespace="multi_table_namespace",
                mode=TableWriteMode.CREATE,
                content_type=ContentType.PARQUET,
                catalog="test_catalog_1",
            )

        # Test recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Extract table names from results
        found_table_names = set()
        for obj in all_objects:
            if hasattr(obj, "table_name") and obj.table_name:
                found_table_names.add(obj.table_name)

        # Verify all tables were found
        for expected_table in table_names:
            assert (
                expected_table in found_table_names
            ), f"Missing table {expected_table}. Found: {found_table_names}"

    def test_recursive_listing_multiple_deltas_per_table(self):
        """
        Test that recursive listing finds all deltas within a table, ensuring complete traversal.
        This is crucial for compaction candidate detection.
        """
        # Create namespace and table
        dc.put(DeltaCatUrl("dc://test_catalog_1/delta_test_namespace"))

        # Create table with multiple deltas
        batch1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})

        batch2 = pd.DataFrame({"id": [4, 5, 6], "value": ["d", "e", "f"]})

        # Write first batch (CREATE)
        write_to_table(
            data=batch1,
            table="multi_delta_table",
            namespace="delta_test_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        # Write second batch (APPEND - creates second delta)
        write_to_table(
            data=batch2,
            table="multi_delta_table",
            namespace="delta_test_namespace",
            mode=TableWriteMode.APPEND,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        # Test recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Count deltas for our specific table
        delta_count = 0
        for obj in all_objects:
            if (
                hasattr(obj, "type")
                and obj.type
                and "delta" in str(obj.type).lower()
                and hasattr(obj, "table_name")
                and obj.table_name == "multi_delta_table"
            ):
                delta_count += 1

        # Should find exactly 2 deltas
        assert (
            delta_count == 2
        ), f"Expected 2 deltas for multi_delta_table, found {delta_count}"

    def test_recursive_listing_empty_namespaces_mixed_with_populated(self):
        """
        Test that recursive listing handles a mix of empty and populated namespaces correctly.
        """
        # Create mix of empty and populated namespaces
        dc.put(DeltaCatUrl("dc://test_catalog_1/empty_namespace_1"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/empty_namespace_2"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/populated_namespace"))

        # Add data only to the populated namespace
        test_data = pd.DataFrame({"id": [1, 2], "data": ["test1", "test2"]})

        write_to_table(
            data=test_data,
            table="test_table",
            namespace="populated_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        # Test recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Verify we found all namespaces
        namespace_names = set()
        for obj in all_objects:
            if hasattr(obj, "namespace") and obj.namespace:
                namespace_names.add(obj.namespace)

        assert "empty_namespace_1" in namespace_names
        assert "empty_namespace_2" in namespace_names
        assert "populated_namespace" in namespace_names

        # Verify we found the table in the populated namespace
        table_names = set()
        for obj in all_objects:
            if hasattr(obj, "table_name") and obj.table_name:
                table_names.add(obj.table_name)

        assert "test_table" in table_names

    def test_non_recursive_listing_vs_recursive_listing(self):
        """
        Test that non-recursive listing only returns top-level objects while recursive returns all.
        """
        # Create nested structure
        dc.put(DeltaCatUrl("dc://test_catalog_1/namespace_one"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/namespace_two"))

        test_data = pd.DataFrame({"id": [1], "value": ["test"]})

        write_to_table(
            data=test_data,
            table="table_in_ns1",
            namespace="namespace_one",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        # Non-recursive listing (should only get namespaces)
        shallow_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=False)

        # Recursive listing (should get everything)
        deep_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Shallow should have fewer objects than deep
        assert len(shallow_objects) < len(deep_objects)

        # Shallow should only contain namespaces
        shallow_types = set()
        for obj in shallow_objects:
            if hasattr(obj, "type") and obj.type:
                shallow_types.add(str(obj.type))

        # Deep should contain multiple types (namespaces, tables, streams, partitions, deltas)
        deep_types = set()
        for obj in deep_objects:
            if hasattr(obj, "type") and obj.type:
                deep_types.add(str(obj.type))

        assert len(deep_types) > len(
            shallow_types
        ), f"Deep types: {deep_types}, Shallow types: {shallow_types}"

    def test_recursive_listing_all_children_processed(self):
        """
        Ensure that all children are processed at each level of recursive listings.
        """
        # Create 3 namespaces
        dc.put(DeltaCatUrl("dc://test_catalog_1/alpha_namespace"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/beta_namespace"))
        dc.put(DeltaCatUrl("dc://test_catalog_1/gamma_namespace"))

        # Create test data
        test_data = pd.DataFrame(
            {"id": [1, 2], "name": ["test1", "test2"], "value": [100, 200]}
        )

        # Create tables in EACH namespace
        write_to_table(
            data=test_data,
            table="alpha_table",
            namespace="alpha_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        write_to_table(
            data=test_data,
            table="beta_table",
            namespace="beta_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        write_to_table(
            data=test_data,
            table="gamma_table",
            namespace="gamma_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
        )

        # Perform recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Extract all table names found
        found_tables = set()
        found_namespaces = set()

        for obj in all_objects:
            if hasattr(obj, "table_name") and obj.table_name:
                found_tables.add(obj.table_name)
            if hasattr(obj, "namespace") and obj.namespace:
                found_namespaces.add(obj.namespace)

        # All namespaces should be found
        assert (
            "alpha_namespace" in found_namespaces
        ), "alpha_namespace missing - indicates recursive traversal bug"
        assert (
            "beta_namespace" in found_namespaces
        ), "beta_namespace missing - indicates recursive traversal bug"
        assert (
            "gamma_namespace" in found_namespaces
        ), "gamma_namespace missing - indicates recursive traversal bug"

        # ALL tables should be found
        assert (
            "alpha_table" in found_tables
        ), "alpha_table missing - BUG: only processing last child in recursion"
        assert (
            "beta_table" in found_tables
        ), "beta_table missing - BUG: only processing last child in recursion"
        assert (
            "gamma_table" in found_tables
        ), "gamma_table missing - BUG: only processing last child in recursion"

        # Verify we have the expected number of tables (3)
        table_count = len(found_tables)
        assert (
            table_count >= 3
        ), f"Expected at least 3 tables, found {table_count}: {found_tables}. This indicates the recursive traversal bug."

        # Ensure we found objects from all levels of hierarchy
        total_objects = len(all_objects)
        assert (
            total_objects > 10
        ), f"Expected many objects from deep traversal, found only {total_objects}. This indicates incomplete recursive traversal."
