import shutil
import tempfile
from collections import defaultdict

import deltacat as dc
from deltacat.constants import METAFILE_FORMAT_MSGPACK
from deltacat import (
    ContentType,
    DeltaCatUrl,
    DatasetType,
    Namespace,
    TableProperties,
    TableWriteMode,
    TableProperty,
    TableReadOptimizationLevel,
)
from deltacat.storage import (
    Metafile,
    Table,
    TableVersion,
    Stream,
    Partition,
    Delta,
)
from deltacat.storage.model.partition import UNPARTITIONED_SCHEME_ID
from deltacat.catalog import write_to_table
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
        # Take all namespaces and find the one we created
        all_rows = dataset.take_all()
        test_namespace_row = None
        for row in all_rows:
            namespace_obj = Metafile.deserialize(
                serialized=row[METAFILE_DATA_COLUMN_NAME],
                meta_format=METAFILE_FORMAT_MSGPACK,
            )
            if (
                hasattr(namespace_obj, "namespace")
                and namespace_obj.namespace == "test_namespace"
            ):
                test_namespace_row = row
                break

        assert test_namespace_row is not None, "test_namespace should be in the listing"
        actual_namespace = Metafile.deserialize(
            serialized=test_namespace_row[METAFILE_DATA_COLUMN_NAME],
            meta_format=METAFILE_FORMAT_MSGPACK,
        )
        assert actual_namespace.equivalent_to(namespace_src)
        namespace_type = test_namespace_row[METAFILE_TYPE_COLUMN_NAME]
        assert namespace_type == "Namespace"

    def test_recursive_listing_multiple_namespaces_with_tables(self):
        """
        Test that recursive listing correctly processes namespaces, tables, and deltas.
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
            auto_create_namespace=True,
        )

        write_to_table(
            data=test_data,
            table="table2",
            namespace="namespace_beta",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        write_to_table(
            data=test_data,
            table="table3",
            namespace="namespace_gamma",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Test recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Verify we found objects from ALL namespaces
        object_types_to_names = defaultdict(list)

        # Verify we found all namespaces, tables, and deltas
        for obj in all_objects:
            obj_type = Metafile.get_class(obj)
            object_types_to_names[obj_type].append(obj.name)

        # Assert we found all namespaces (including auto-created default)
        expected_namespaces = {
            "namespace_alpha",
            "namespace_beta",
            "namespace_gamma",
            "default",
        }
        assert (
            len(object_types_to_names[Namespace]) == 4
        ), f"Expected 4 namespaces (including default), found {len(object_types_to_names[Namespace])}"
        assert (
            set(object_types_to_names[Namespace]) == expected_namespaces
        ), f"Expected namespaces: {expected_namespaces}, found: {object_types_to_names[Namespace]}"

        # Assert we found all tables
        expected_tables = {"table1", "table2", "table3"}
        assert (
            len(object_types_to_names[Table]) == 3
        ), f"Expected 3 tables, found {len(object_types_to_names[Table])}"
        assert (
            set(object_types_to_names[Table]) == expected_tables
        ), f"Expected tables: {expected_tables}, found: {object_types_to_names[Table]}"

        # Assert we found all deltas
        assert (
            len(object_types_to_names[Delta]) == 3
        ), f"Expected 3 deltas, found {len(object_types_to_names[Delta])}"
        expected_deltas = {
            "1"
        }  # all 3 deltas should have the same stream position in their respective partitions
        assert (
            set(object_types_to_names[Delta]) == expected_deltas
        ), f"Expected deltas: {expected_deltas}, found: {object_types_to_names[Delta]}"

    def test_recursive_listing_multiple_tables_per_namespace(self):
        """
        Test that recursive listing finds all tables within a namespace.
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
        object_types_to_names = defaultdict(list)
        for obj in all_objects:
            obj_type = Metafile.get_class(obj)
            object_types_to_names[obj_type].append(obj.name)

        # Assert we found all tables
        assert len(object_types_to_names[Table]) == len(
            table_names
        ), f"Expected {len(table_names)} tables, found {len(object_types_to_names[Table])}"
        assert set(object_types_to_names[Table]) == set(
            table_names
        ), f"Expected tables: {table_names}, found: {object_types_to_names[Table]}"

    def test_recursive_listing_multiple_deltas_per_table(self):
        """
        Test that recursive listing finds all deltas within a table.
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

        # Extract table names from results
        object_types_to_names = defaultdict(list)
        for obj in all_objects:
            obj_type = Metafile.get_class(obj)
            object_types_to_names[obj_type].append(obj.name)

        # Assert we found all deltas
        expected_deltas = {
            "1",
            "2",
        }  # all deltas should have the same stream position in their respective partitions
        assert (
            len(object_types_to_names[Delta]) == 2
        ), f"Expected 2 deltas, found {len(object_types_to_names[Delta])}"
        assert (
            set(object_types_to_names[Delta]) == expected_deltas
        ), f"Expected deltas: {expected_deltas}, found: {object_types_to_names[Delta]}"

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
            auto_create_namespace=True,
        )

        # Test recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        object_types_to_names = defaultdict(list)
        for obj in all_objects:
            obj_type = Metafile.get_class(obj)
            object_types_to_names[obj_type].append(obj.name)

        # Verify we found all namespaces (including auto-created default)
        expected_namespaces = {
            "empty_namespace_1",
            "empty_namespace_2",
            "populated_namespace",
            "default",
        }
        assert (
            len(object_types_to_names[Namespace]) == 4
        ), f"Expected 4 namespaces (including default), found {len(object_types_to_names[Namespace])}"
        assert (
            set(object_types_to_names[Namespace]) == expected_namespaces
        ), f"Expected namespaces: {expected_namespaces}, found: {object_types_to_names[Namespace]}"

        # Verify we found the table in the populated namespace
        expected_tables = {"test_table"}
        assert (
            len(object_types_to_names[Table]) == 1
        ), f"Expected 1 table, found {len(object_types_to_names[Table])}"
        assert (
            set(object_types_to_names[Table]) == expected_tables
        ), f"Expected tables: {expected_tables}, found: {object_types_to_names[Table]}"

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
            auto_create_namespace=True,
        )

        # Non-recursive listing (should only get namespaces)
        shallow_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=False)

        # Recursive listing (should get everything)
        deep_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Shallow should have fewer objects than deep
        assert len(shallow_objects) < len(deep_objects)

        # Shallow should only contain namespaces
        shallow_object_types_to_names = defaultdict(list)
        for obj in shallow_objects:
            obj_type = Metafile.get_class(obj)
            shallow_object_types_to_names[obj_type].append(obj.name)

        # Assert we found all namespaces (including auto-created default)
        expected_namespaces = {"namespace_one", "namespace_two", "default"}
        assert (
            len(shallow_object_types_to_names[Namespace]) == 3
        ), f"Expected 3 namespaces (including default), found {len(shallow_object_types_to_names[Namespace])}"
        assert (
            set(shallow_object_types_to_names[Namespace]) == expected_namespaces
        ), f"Expected namespaces: {expected_namespaces}, found: {shallow_object_types_to_names[Namespace]}"
        assert (
            len(shallow_object_types_to_names) == 1
        ), f"Expected 1 object type, found {len(shallow_object_types_to_names)}"
        assert set(shallow_object_types_to_names.keys()) == {
            Namespace
        }, f"Expected only Namespace object type, found: {shallow_object_types_to_names.keys()}"

        # Deep should contain multiple types (namespaces, tables, streams, partitions, deltas)
        deep_object_types_to_names = defaultdict(list)
        for obj in deep_objects:
            deep_object_types_to_names[Metafile.get_class(obj)].append(obj.name)

        expected_namespaces = {"namespace_one", "namespace_two", "default"}
        assert (
            len(deep_object_types_to_names[Namespace]) == 3
        ), f"Expected 3 namespaces (including default), found {len(deep_object_types_to_names[Namespace])}"
        assert (
            set(deep_object_types_to_names[Namespace]) == expected_namespaces
        ), f"Expected namespaces: {expected_namespaces}, found: {deep_object_types_to_names[Namespace]}"

        expected_tables = {"table_in_ns1"}
        assert (
            len(deep_object_types_to_names[Table]) == 1
        ), f"Expected 1 table, found {len(deep_object_types_to_names[Table])}"
        assert (
            set(deep_object_types_to_names[Table]) == expected_tables
        ), f"Expected tables: {expected_tables}, found: {deep_object_types_to_names[Table]}"

        expected_table_versions = {"1"}
        assert (
            len(deep_object_types_to_names[TableVersion]) == 1
        ), f"Expected 1 table version, found {len(deep_object_types_to_names[TableVersion])}"
        assert (
            set(deep_object_types_to_names[TableVersion]) == expected_table_versions
        ), f"Expected table versions: {expected_table_versions}, found: {deep_object_types_to_names[TableVersion]}"

        expected_streams = {"deltacat"}
        assert (
            len(deep_object_types_to_names[Stream]) == 1
        ), f"Expected 1 stream, found {len(deep_object_types_to_names[Stream])}"
        assert (
            set(deep_object_types_to_names[Stream]) == expected_streams
        ), f"Expected streams: {expected_streams}, found: {deep_object_types_to_names[Stream]}"

        expected_partitions = {f"None|{UNPARTITIONED_SCHEME_ID}"}
        assert (
            len(deep_object_types_to_names[Partition]) == 1
        ), f"Expected 1 partition, found {len(deep_object_types_to_names[Partition])}"
        assert (
            set(deep_object_types_to_names[Partition]) == expected_partitions
        ), f"Expected partitions: {expected_partitions}, found: {deep_object_types_to_names[Partition]}"

        expected_deltas = {"1"}
        assert (
            len(deep_object_types_to_names[Delta]) == 1
        ), f"Expected 1 delta, found {len(deep_object_types_to_names[Delta])}"
        assert (
            set(deep_object_types_to_names[Delta]) == expected_deltas
        ), f"Expected deltas: {expected_deltas}, found: {deep_object_types_to_names[Delta]}"

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
            auto_create_namespace=True,
        )

        write_to_table(
            data=test_data,
            table="beta_table",
            namespace="beta_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        write_to_table(
            data=test_data,
            table="gamma_table",
            namespace="gamma_namespace",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Perform recursive listing
        all_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)

        # Extract all objects found
        object_types_to_names = defaultdict(list)
        for obj in all_objects:
            obj_type = Metafile.get_class(obj)
            object_types_to_names[obj_type].append(obj.name)

        # All namespaces should be found (including auto-created default)
        expected_namespaces = {
            "alpha_namespace",
            "beta_namespace",
            "gamma_namespace",
            "default",
        }
        assert (
            len(object_types_to_names[Namespace]) == 4
        ), f"Expected 4 namespaces (including default), found {len(object_types_to_names[Namespace])}"
        assert (
            set(object_types_to_names[Namespace]) == expected_namespaces
        ), f"Expected namespaces: {expected_namespaces}, found: {object_types_to_names[Namespace]}"

        # All tables should be found
        expected_tables = {"alpha_table", "beta_table", "gamma_table"}
        assert (
            len(object_types_to_names[Table]) == 3
        ), f"Expected 3 tables, found {len(object_types_to_names[Table])}"
        assert (
            set(object_types_to_names[Table]) == expected_tables
        ), f"Expected tables: {expected_tables}, found: {object_types_to_names[Table]}"

        # All table versions should be found
        expected_table_versions = {"1"}
        assert (
            len(object_types_to_names[TableVersion]) == 3
        ), f"Expected 3 table versions, found {len(object_types_to_names[TableVersion])}"
        assert (
            set(object_types_to_names[TableVersion]) == expected_table_versions
        ), f"Expected table versions: {expected_table_versions}, found: {object_types_to_names[TableVersion]}"

        # All streams should be found
        expected_streams = {"deltacat"}
        assert (
            len(object_types_to_names[Stream]) == 3
        ), f"Expected 1 stream, found {len(object_types_to_names[Stream])}"
        assert (
            set(object_types_to_names[Stream]) == expected_streams
        ), f"Expected streams: {expected_streams}, found: {object_types_to_names[Stream]}"

        # All partitions should be found
        expected_partitions = {f"None|{UNPARTITIONED_SCHEME_ID}"}
        assert (
            len(object_types_to_names[Partition]) == 3
        ), f"Expected 1 partition, found {len(object_types_to_names[Partition])}"
        assert (
            set(object_types_to_names[Partition]) == expected_partitions
        ), f"Expected partitions: {expected_partitions}, found: {object_types_to_names[Partition]}"

        # All deltas should be found
        expected_deltas = {"1"}
        assert (
            len(object_types_to_names[Delta]) == 3
        ), f"Expected 3 deltas, found {len(object_types_to_names[Delta])}"
        assert (
            set(object_types_to_names[Delta]) == expected_deltas
        ), f"Expected deltas: {expected_deltas}, found: {object_types_to_names[Delta]}"

        # Ensure we found the expected objects across all levels of hierarchy
        total_objects = len(all_objects)
        assert (
            total_objects == 19
        ), f"Expected 19 objects from deep traversal (including default namespace), found only {total_objects}."

    def test_recursive_cross_catalog_copy(self):
        """
        Test comprehensive cross-catalog copy using dc.copy with ** pattern.
        This test validates complete catalog copying with all metadata types:
        namespaces, tables, table versions, streams, partitions, and deltas.
        """
        # Create multiple namespaces, multiple tables, versions, streams, partitions, and deltas

        # Namespace 1: Analytics data with multiple table versions
        dc.put(DeltaCatUrl("dc://test_catalog_1/analytics"))

        # Create table with multiple versions
        events_data_v1 = pd.DataFrame(
            {
                "event_id": [1, 2, 3],
                "user_id": ["user_1", "user_2", "user_3"],
                "event_type": ["click", "view", "purchase"],
                "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "value": [10.5, 20.0, 150.75],
            }
        )

        table_properties: TableProperties = {
            TableProperty.READ_OPTIMIZATION_LEVEL: TableReadOptimizationLevel.MAX,
            TableProperty.APPENDED_RECORD_COUNT_COMPACTION_TRIGGER: 1,
            TableProperty.APPENDED_FILE_COUNT_COMPACTION_TRIGGER: 1,
            TableProperty.APPENDED_DELTA_COUNT_COMPACTION_TRIGGER: 1,
        }

        write_to_table(
            data=events_data_v1,
            table="events",
            namespace="analytics",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            table_properties=table_properties,
            auto_create_namespace=True,
        )

        # Add more data to create additional deltas
        events_data_v2 = pd.DataFrame(
            {
                "event_id": [4, 5, 6, 7],
                "user_id": ["user_4", "user_1", "user_5", "user_2"],
                "event_type": ["view", "click", "purchase", "refund"],
                "timestamp": pd.to_datetime(
                    ["2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07"]
                ),
                "value": [0.0, 5.25, 299.99, -150.75],
            }
        )

        write_to_table(
            data=events_data_v2,
            table="events",
            namespace="analytics",
            mode=TableWriteMode.APPEND,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Create second table in analytics namespace
        users_data = pd.DataFrame(
            {
                "user_id": ["user_1", "user_2", "user_3", "user_4", "user_5"],
                "username": ["alice", "bob", "charlie", "diana", "eve"],
                "email": [
                    "alice@test.com",
                    "bob@test.com",
                    "charlie@test.com",
                    "diana@test.com",
                    "eve@test.com",
                ],
                "created_at": pd.to_datetime(
                    [
                        "2022-12-01",
                        "2022-12-15",
                        "2022-12-20",
                        "2023-01-01",
                        "2023-01-03",
                    ]
                ),
                "is_active": [True, True, False, True, True],
            }
        )

        write_to_table(
            data=users_data,
            table="users",
            namespace="analytics",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Create version 2 of the events table to test table version ordering in recursive copy
        events_data_v3 = pd.DataFrame(
            {
                "event_id": [8, 9, 10],
                "user_id": ["user_3", "user_4", "user_5"],
                "event_type": ["signup", "login", "logout"],
                "timestamp": pd.to_datetime(["2023-01-08", "2023-01-09", "2023-01-10"]),
                "value": [0.0, 0.0, 0.0],
            }
        )

        write_to_table(
            data=events_data_v3,
            table="events",
            namespace="analytics",
            table_version="2",  # Explicitly create version 2
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Namespace 2: Product data with different schema
        dc.put(DeltaCatUrl("dc://test_catalog_1/products"))

        products_data = pd.DataFrame(
            {
                "product_id": ["prod_1", "prod_2", "prod_3"],
                "name": ["Widget A", "Widget B", "Super Widget"],
                "category": ["widgets", "widgets", "premium"],
                "price": [19.99, 29.99, 149.99],
                "in_stock": [True, False, True],
                "metadata": [
                    {"color": "red"},
                    {"color": "blue", "size": "large"},
                    {"color": "gold", "premium": True},
                ],
            }
        )

        write_to_table(
            data=products_data,
            table="inventory",
            namespace="products",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Create product categories table
        categories_data = pd.DataFrame(
            {
                "category_id": ["widgets", "premium", "accessories"],
                "display_name": ["Standard Widgets", "Premium Products", "Accessories"],
                "description": [
                    "Basic widget products",
                    "High-end premium items",
                    "Additional accessories",
                ],
                "active": [True, True, False],
            }
        )

        write_to_table(
            data=categories_data,
            table="categories",
            namespace="products",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Namespace 3: Empty namespace (edge case testing)
        dc.put(DeltaCatUrl("dc://test_catalog_1/empty_data"))

        # Namespace 4: Orders with complex nested data
        dc.put(DeltaCatUrl("dc://test_catalog_1/orders"))

        orders_data = pd.DataFrame(
            {
                "order_id": ["order_1", "order_2", "order_3"],
                "user_id": ["user_1", "user_2", "user_1"],
                "product_ids": [["prod_1"], ["prod_2", "prod_3"], ["prod_1", "prod_2"]],
                "order_date": pd.to_datetime(
                    ["2023-01-05", "2023-01-06", "2023-01-07"]
                ),
                "total_amount": [19.99, 179.98, 49.98],
                "status": ["completed", "pending", "completed"],
            }
        )

        write_to_table(
            data=orders_data,
            table="transactions",
            namespace="orders",
            mode=TableWriteMode.CREATE,
            content_type=ContentType.PARQUET,
            catalog="test_catalog_1",
            auto_create_namespace=True,
        )

        # Verify source catalog structure before copy
        source_objects = dc.list(DeltaCatUrl("dc://test_catalog_1"), recursive=True)
        source_urls_by_type = defaultdict(list)
        source_by_type = defaultdict(list)

        for obj in source_objects:
            obj_class = Metafile.get_class(obj.to_serializable())
            source_urls_by_type[obj_class].append(obj.url())
            source_by_type[obj_class].append(obj)

        assert (
            len(source_urls_by_type[Namespace]) == 5
        ), f"Expected 5 namespaces (including default), got {len(source_urls_by_type[Namespace])}"
        assert (
            len(source_urls_by_type[Table]) == 5
        ), f"Expected 5 tables, got {len(source_urls_by_type[Table])}"
        assert (
            len(source_urls_by_type[TableVersion]) == 6
        ), f"Expected 6 table versions, got {len(source_urls_by_type[TableVersion])}"
        assert (
            len(source_urls_by_type[Stream]) == 6
        ), f"Expected 6 streams, got {len(source_urls_by_type[Stream])}"
        assert (
            len(source_urls_by_type[Partition]) == 6
        ), f"Expected 6 partitions, got {len(source_urls_by_type[Partition])}"
        assert (
            len(source_urls_by_type[Delta]) == 6
        ), f"Expected 6 deltas, got {len(source_urls_by_type[Delta])}"

        # Test the /** recursive copy pattern.
        dc.copy(
            DeltaCatUrl("dc://test_catalog_1/**"),  # ** means recursive copy all
            DeltaCatUrl("dc://test_catalog_2/"),
        )

        # Verify destination catalog has same structure
        dest_objects = dc.list(DeltaCatUrl("dc://test_catalog_2"), recursive=True)
        dest_urls_by_type = defaultdict(list)
        dest_by_type = defaultdict(list)
        assert len(dest_objects) == len(
            source_objects
        ), f"Expected {len(source_objects)} objects, got {len(dest_objects)}"

        for obj in dest_objects:
            obj_class = Metafile.get_class(obj.to_serializable())
            dest_urls_by_type[obj_class].append(obj.url())
            dest_by_type[obj_class].append(obj)

        assert sorted(dest_urls_by_type[Namespace]) == sorted(
            source_urls_by_type[Namespace]
        ), f"Namespace mismatch: {dest_urls_by_type[Namespace]} vs {source_urls_by_type[Namespace]}"
        assert sorted(dest_urls_by_type[Table]) == sorted(
            source_urls_by_type[Table]
        ), f"Table mismatch: {dest_urls_by_type[Table]} vs {source_urls_by_type[Table]}"
        assert sorted(dest_urls_by_type[TableVersion]) == sorted(
            source_urls_by_type[TableVersion]
        ), f"Table version mismatch: {dest_urls_by_type[TableVersion]} vs {source_urls_by_type[TableVersion]}"
        assert sorted(dest_urls_by_type[Stream]) == sorted(
            source_urls_by_type[Stream]
        ), f"Stream mismatch: {dest_urls_by_type[Stream]} vs {source_urls_by_type[Stream]}"
        assert sorted(dest_urls_by_type[Partition]) == sorted(
            source_urls_by_type[Partition]
        ), f"Partition mismatch: {dest_urls_by_type[Partition]} vs {source_urls_by_type[Partition]}"
        assert sorted(dest_urls_by_type[Delta]) == sorted(
            source_urls_by_type[Delta]
        ), f"Delta mismatch: {dest_urls_by_type[Delta]} vs {source_urls_by_type[Delta]}"

        # Validate each hierarchy level
        for obj_type in source_by_type.keys():
            source_count = len(source_by_type.get(obj_type))
            dest_count = len(dest_by_type.get(obj_type, []))
            assert (
                dest_count == source_count
            ), f"{obj_type} count mismatch: {dest_count} vs {source_count}"

            # Spot check equivalence of each type
            if obj_type == Namespace and source_count > 0:
                # Check namespace properties are preserved
                source_ns = source_by_type[obj_type][0]  # NamespaceModel
                dest_ns = next(
                    (
                        ns
                        for ns in dest_by_type[obj_type]
                        if ns.namespace == source_ns.namespace
                    ),
                    None,
                )
                assert (
                    dest_ns is not None
                ), f"Namespace {source_ns.namespace} not found in destination"
                assert source_ns.equivalent_to(
                    dest_ns
                ), f"Namespace {source_ns.namespace} not equivalent to {dest_ns.namespace}"
            elif obj_type == Table:
                source_table = source_by_type[obj_type][0]  # TableModel
                dest_table = next(
                    (
                        t
                        for t in dest_by_type[obj_type]
                        if t.namespace == source_table.namespace
                        and t.table_name == source_table.table_name
                    ),
                    None,
                )
                assert (
                    dest_table is not None
                ), f"Table {source_table.namespace}.{source_table.table_name} not found in destination"
                assert source_table.equivalent_to(
                    dest_table
                ), f"Table {source_table.namespace}.{source_table.table_name} not equivalent to {dest_table.namespace}.{dest_table.table_name}"
            elif obj_type == TableVersion and source_count > 0:
                # Check table version properties are preserved
                source_tv = source_by_type[obj_type][0]  # TableVersionModel
                dest_tv = next(
                    (
                        tv
                        for tv in dest_by_type[obj_type]
                        if tv.namespace == source_tv.namespace
                        and tv.table_name == source_tv.table_name
                        and tv.table_version == source_tv.table_version
                    ),
                    None,
                )
                assert (
                    dest_tv is not None
                ), f"TableVersion {source_tv.namespace}.{source_tv.table_name}.{source_tv.table_version} not found in destination"
                assert dest_tv.equivalent_to(
                    source_tv
                ), f"TableVersion {source_tv.namespace}.{source_tv.table_name}.{source_tv.table_version} not equivalent to {dest_tv.namespace}.{dest_tv.table_name}.{dest_tv.table_version}"

                # Special validation for table version ordering - check that analytics.events has versions 1 and 2
                analytics_events_versions = [
                    tv
                    for tv in dest_by_type[obj_type]
                    if tv.namespace == "analytics" and tv.table_name == "events"
                ]
                if analytics_events_versions:
                    versions = sorted(
                        [tv.table_version for tv in analytics_events_versions]
                    )
                    assert versions == [
                        "1",
                        "2",
                    ], f"Expected analytics.events versions ['1', '2'], got {versions}"
            elif obj_type == Stream and source_count > 0:
                # Check stream properties are preserved
                source_stream = source_by_type[obj_type][0]  # StreamModel
                dest_stream = next(
                    (
                        s
                        for s in dest_by_type[obj_type]
                        if s.namespace == source_stream.namespace
                        and s.table_name == source_stream.table_name
                        and s.stream_format == source_stream.stream_format
                    ),
                    None,
                )
                assert (
                    dest_stream is not None
                ), f"Stream {source_stream.namespace}.{source_stream.table_name}.{source_stream.stream_format} not found in destination"
                assert dest_stream.equivalent_to(
                    source_stream
                ), f"Stream {source_stream.namespace}.{source_stream.table_name}.{source_stream.stream_format} not equivalent to {dest_stream.namespace}.{dest_stream.table_name}.{dest_stream.stream_format}"
            elif obj_type == Partition and source_count > 0:
                # Check partition properties are preserved (with new partition IDs)
                source_partition = source_by_type[obj_type][0]  # PartitionModel
                dest_partition = next(
                    (
                        p
                        for p in dest_by_type[obj_type]
                        if p.namespace == source_partition.namespace
                        and p.table_name == source_partition.table_name
                    ),
                    None,
                )
                assert (
                    dest_partition is not None
                ), f"Partition for {source_partition.namespace}.{source_partition.table_name} not found in destination"
                assert dest_partition.equivalent_to(
                    source_partition
                ), f"Partition {source_partition.namespace}.{source_partition.table_name} not equivalent to {dest_partition.namespace}.{dest_partition.table_name}"
            elif obj_type == Delta and source_count > 0:
                # Check delta properties are preserved (with same stream positions)
                source_delta = source_by_type[obj_type][0]  # DeltaModel
                dest_delta = next(
                    (
                        d
                        for d in dest_by_type[obj_type]
                        if d.namespace == source_delta.namespace
                        and d.table_name == source_delta.table_name
                        and d.stream_position == source_delta.stream_position
                    ),
                    None,
                )
                assert (
                    dest_delta is not None
                ), f"Delta for {source_delta.namespace}.{source_delta.table_name} at position {source_delta.stream_position} not found in destination"
                assert dest_delta.equivalent_to(
                    source_delta
                ), f"Delta {source_delta.namespace}.{source_delta.table_name} at position {source_delta.stream_position} not equivalent to {dest_delta.namespace}.{dest_delta.table_name} at position {dest_delta.stream_position}"

        # Validate each table's data integrity
        test_cases = [
            ("analytics", "events"),
            ("analytics", "users"),
            ("products", "inventory"),
            ("products", "categories"),
            ("orders", "transactions"),
        ]

        for namespace, table in test_cases:
            # Check table exists in destination
            assert dc.table_exists(
                table=table,
                namespace=namespace,
                catalog="test_catalog_2",
            ), f"Table {namespace}/{table} should exist in destination catalog"

            # Verify table data equivalence using read_table
            source_df = dc.read_table(
                table=table,
                namespace=namespace,
                catalog="test_catalog_1",
                read_as=DatasetType.PANDAS,
            )

            dest_df = dc.read_table(
                table=table,
                namespace=namespace,
                catalog="test_catalog_2",
                read_as=DatasetType.PANDAS,
            )

            # Verify both datasets are valid pandas DataFrames
            assert (
                source_df is not None
            ), f"Source data should not be None for {namespace}.{table}"
            assert (
                dest_df is not None
            ), f"Destination data should not be None for {namespace}.{table}"

            # Compare DataFrame properties
            assert len(source_df) == len(
                dest_df
            ), f"Row count mismatch for {namespace}.{table}: {len(source_df)} vs {len(dest_df)}"
            assert list(source_df.columns) == list(
                dest_df.columns
            ), f"Column mismatch for {namespace}.{table}"

            # Sort both dataframes by first column for comparison (to handle potential row ordering differences)
            _assert_data_equivalence(source_df, dest_df)

            # Verify that writing to the source table doesn't affect the destination table
            dc.write_to_table(
                data=source_df,
                table=table,
                namespace=namespace,
                catalog="test_catalog_1",
                mode=TableWriteMode.APPEND,
                auto_create_namespace=True,
            )

            # Verify that the destination table's data hasn't changed
            dest_df = dc.read_table(
                table=table,
                namespace=namespace,
                catalog="test_catalog_2",
                read_as=DatasetType.PANDAS,
            )
            _assert_data_equivalence(source_df, dest_df)

            # Verify that the source table has source_df repeated twice
            source_df_repeated = dc.read_table(
                table=table,
                namespace=namespace,
                catalog="test_catalog_1",
                read_as=DatasetType.PANDAS,
            )
            assert (
                len(source_df_repeated) == len(source_df) * 2
            ), f"Source table {namespace}.{table} should have {len(source_df) * 2} rows"

            # Verify that writing to the destination table doesn't affect the source table
            dc.write_to_table(
                data=dest_df,
                table=table,
                namespace=namespace,
                catalog="test_catalog_2",
                mode=TableWriteMode.APPEND,
                auto_create_namespace=True,
            )

            # Verify that the source table's data hasn't changed
            source_df_unchanged = dc.read_table(
                table=table,
                namespace=namespace,
                catalog="test_catalog_1",
                read_as=DatasetType.PANDAS,
            )
            _assert_data_equivalence(source_df_repeated, source_df_unchanged)

            # Verify that the destination table's data has dest_df repeated twice
            dest_df_repeated = dc.read_table(
                table=table,
                namespace=namespace,
                catalog="test_catalog_2",
                read_as=DatasetType.PANDAS,
            )
            assert (
                len(dest_df_repeated) == len(dest_df) * 2
            ), f"Destination table {namespace}.{table} should have {len(dest_df) * 2} rows"

        # Verify empty namespace was copied correctly
        assert dc.namespace_exists(
            namespace="empty_data",
            catalog="test_catalog_2",
        ), "Empty namespace should exist in destination catalog"

    def test_cross_catalog_table_copy_without_rename(self):
        """Test copying a table between catalogs without renaming."""
        # Create test data in source catalog
        test_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["alice", "bob", "charlie"],
                "value": [1.1, 2.2, 3.3],
            }
        )

        dc.write_to_table(
            data=test_df,
            table="users",
            namespace="default",
            catalog="test_catalog_1",
            mode=TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

        # Copy table without renaming: dc://src/ns/table/ -> dc://dest/ns/table/
        dc.copy(
            "dc://test_catalog_1/default/users/", "dc://test_catalog_2/default/users/"
        )

        # Verify table was copied correctly
        dest_df = dc.read_table(
            table="users",
            namespace="default",
            catalog="test_catalog_2",
            read_as=DatasetType.PANDAS,
        )

        _assert_data_equivalence(test_df, dest_df)

    def test_cross_catalog_table_copy_with_table_rename(self):
        """Test copying a table between catalogs with table renaming."""
        # Create test data in source catalog
        test_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["alice", "bob", "charlie"],
                "value": [1.1, 2.2, 3.3],
            }
        )

        dc.write_to_table(
            data=test_df,
            table="users",
            namespace="default",
            catalog="test_catalog_1",
            mode=TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

        # Copy table with table rename: dc://src/ns/table/ -> dc://dest/ns/new_table/
        dc.copy(
            "dc://test_catalog_1/default/users/",
            "dc://test_catalog_2/default/employees/",
        )

        # Verify table was copied with new name
        dest_df = dc.read_table(
            table="employees",
            namespace="default",
            catalog="test_catalog_2",
            read_as=DatasetType.PANDAS,
        )

        _assert_data_equivalence(test_df, dest_df)

        # Verify original table still exists in source
        source_df = dc.read_table(
            table="users",
            namespace="default",
            catalog="test_catalog_1",
            read_as=DatasetType.PANDAS,
        )

        _assert_data_equivalence(test_df, source_df)

    def test_cross_catalog_namespace_recursive_copy_with_tables(self):
        """Test copying a namespace with all its tables using the /** recursive pattern."""
        # Create test data for multiple tables in the same namespace
        users_df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["alice", "bob", "charlie"],
                "role": ["admin", "user", "user"],
            }
        )

        products_df = pd.DataFrame(
            {
                "product_id": [101, 102, 103],
                "name": ["laptop", "mouse", "keyboard"],
                "price": [999.99, 29.99, 79.99],
            }
        )

        # Write tables to the same namespace
        dc.write_to_table(
            data=users_df,
            table="users",
            namespace="analytics",
            catalog="test_catalog_1",
            mode=TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

        dc.write_to_table(
            data=products_df,
            table="products",
            namespace="analytics",
            catalog="test_catalog_1",
            mode=TableWriteMode.CREATE,
        )

        # Copy the namespace from source to dest first
        dc.copy("dc://test_catalog_1/analytics/", "dc://test_catalog_2/analytics/")

        # Copy all namespace tables from source to dest using /**
        dc.copy("dc://test_catalog_1/analytics/**", "dc://test_catalog_2/analytics/")

        # Verify namespace exists in destination
        assert dc.namespace_exists(
            namespace="analytics", catalog="test_catalog_2"
        ), "Analytics namespace should exist in destination catalog"

        # Verify both tables were copied and data matches
        dest_users_df = dc.read_table(
            table="users",
            namespace="analytics",
            catalog="test_catalog_2",
            read_as=DatasetType.PANDAS,
        )

        dest_products_df = dc.read_table(
            table="products",
            namespace="analytics",
            catalog="test_catalog_2",
            read_as=DatasetType.PANDAS,
        )

        # Verify data integrity
        _assert_data_equivalence(users_df, dest_users_df)
        _assert_data_equivalence(products_df, dest_products_df)

        # Verify original tables still exist in source
        source_users_df = dc.read_table(
            table="users",
            namespace="analytics",
            catalog="test_catalog_1",
            read_as=DatasetType.PANDAS,
        )

        source_products_df = dc.read_table(
            table="products",
            namespace="analytics",
            catalog="test_catalog_1",
            read_as=DatasetType.PANDAS,
        )

        _assert_data_equivalence(users_df, source_users_df)
        _assert_data_equivalence(products_df, source_products_df)

    def test_cross_catalog_namespace_copy_without_rename(self):
        """Test copying a namespace metafile between catalogs without renaming."""
        # Create namespace in source catalog (lightweight)
        dc.create_namespace(namespace="production", catalog="test_catalog_1")

        # Copy namespace metafile without rename: dc://src/ns/ -> dc://dest/ns/
        dc.copy("dc://test_catalog_1/production/", "dc://test_catalog_2/production/")

        # Verify namespace exists in destination catalog
        assert dc.namespace_exists(
            namespace="production", catalog="test_catalog_2"
        ), "Namespace should exist in destination catalog"

        # Verify namespace can be listed in destination
        dest_namespaces = dc.list("dc://test_catalog_2/")
        dest_namespace_names = [ns.name for ns in dest_namespaces]
        assert (
            "production" in dest_namespace_names
        ), f"production namespace should be in {dest_namespace_names}"

        # Verify original namespace still exists in source
        assert dc.namespace_exists(
            namespace="production", catalog="test_catalog_1"
        ), "Original namespace should still exist in source catalog"

    def test_cross_catalog_namespace_copy_with_rename(self):
        """Test copying a namespace metafile between catalogs with renaming."""
        # Create namespace in source catalog (lightweight)
        dc.create_namespace(namespace="staging", catalog="test_catalog_1")

        # Copy namespace metafile with rename: dc://src/ns/ -> dc://dest/new_ns/
        dc.copy("dc://test_catalog_1/staging/", "dc://test_catalog_2/development/")

        # Verify namespace was copied with new name
        assert dc.namespace_exists(
            namespace="development", catalog="test_catalog_2"
        ), "Renamed namespace should exist in destination catalog"

        # Verify namespace can be listed in destination with new name
        dest_namespaces = dc.list("dc://test_catalog_2/")
        dest_namespace_names = [ns.name for ns in dest_namespaces]
        assert (
            "development" in dest_namespace_names
        ), f"development namespace should be in {dest_namespace_names}"
        assert (
            "staging" not in dest_namespace_names
        ), f"staging namespace should NOT be in {dest_namespace_names}"

        # Verify original namespace still exists in source with original name
        assert dc.namespace_exists(
            namespace="staging", catalog="test_catalog_1"
        ), "Original namespace should still exist in source catalog"

    def test_cross_catalog_table_copy_multiple_deltas(self):
        """Test copying a table with multiple deltas between catalogs."""
        # Create initial table with first delta
        initial_df = pd.DataFrame(
            {"id": [1, 2], "name": ["alice", "bob"], "value": [1.1, 2.2]}
        )

        dc.write_to_table(
            data=initial_df,
            table="multi_delta_table",
            namespace="default",
            catalog="test_catalog_1",
            mode=TableWriteMode.CREATE,
            auto_create_namespace=True,
        )

        # Add second delta via APPEND
        additional_df = pd.DataFrame(
            {"id": [3, 4], "name": ["charlie", "david"], "value": [3.3, 4.4]}
        )

        dc.write_to_table(
            data=additional_df,
            table="multi_delta_table",
            namespace="default",
            catalog="test_catalog_1",
            mode=TableWriteMode.APPEND,
        )

        # Copy table with multiple deltas
        dc.copy(
            "dc://test_catalog_1/default/multi_delta_table/",
            "dc://test_catalog_2/default/multi_delta_table_copy/",
        )

        # Verify both deltas were copied and combined data is accessible
        dest_df = dc.read_table(
            table="multi_delta_table_copy",
            namespace="default",
            catalog="test_catalog_2",
            read_as=DatasetType.PANDAS,
        )

        # Combined expected data from both deltas
        expected_df = pd.concat([initial_df, additional_df], ignore_index=True)
        _assert_data_equivalence(expected_df, dest_df)


def _assert_data_equivalence(source_df: pd.DataFrame, dest_df: pd.DataFrame):
    # Sort both dataframes by first column for comparison (to handle potential row ordering differences)
    if len(source_df) > 0:
        first_col = source_df.columns[0]
        # Handle sorting with potential complex data types
        source_sorted = source_df.sort_values(first_col).reset_index(drop=True)
        dest_sorted = dest_df.sort_values(first_col).reset_index(drop=True)

        # Compare data values using pandas testing
        pd.testing.assert_frame_equal(
            source_sorted,
            dest_sorted,
        )
