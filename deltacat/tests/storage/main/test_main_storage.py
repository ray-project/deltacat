import shutil
import tempfile
import uuid

import pytest
import copy
import pyarrow as pa

from deltacat import PartitionKey, PartitionScheme
from deltacat.exceptions import TableNotFoundError
from deltacat.storage import (
    metastore,
    CommitState,
    IdentityTransform,
    LifecycleState,
    Metafile,
    Namespace,
    NamespaceLocator,
    Partition,
    PartitionLocator,
    TableVersion,
    TableVersionLocator,
    Schema,
    SortKey,
    SortScheme,
    StreamFormat,
    StreamLocator,
    SortOrder,
    NullOrder,
    BucketTransform,
    BucketTransformParameters,
    BucketingStrategy,
    YearTransform,
    MonthTransform,
    DayTransform,
    HourTransform,
    TruncateTransform,
    TruncateTransformParameters,
    VoidTransform,
)
from deltacat.storage.model.partition import (
    UNPARTITIONED_SCHEME,
    UNPARTITIONED_SCHEME_ID,
    UNPARTITIONED_SCHEME_NAME,
    PartitionKeyList,
)
from deltacat.storage.model.sort_key import (
    UNSORTED_SCHEME,
    UNSORTED_SCHEME_ID,
    UNSORTED_SCHEME_NAME,
    SortKeyList,
)
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
    create_test_table,
    create_test_table_version,
)
from deltacat.catalog import CatalogProperties

from deltacat.storage.main.impl import DEFAULT_TABLE_VERSION


class TestNamespace:
    @classmethod
    def setup_method(cls):

        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)
        cls.namespace1 = metastore.create_namespace(
            namespace="namespace1",
            catalog=cls.catalog,
        )
        cls.namespace2 = metastore.create_namespace(
            namespace="namespace2",
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_namespaces(self):
        # expect the namespace returned to match the input namespace to create
        namespace_locator = NamespaceLocator.of(namespace="namespace1")
        expected_namespace = Namespace.of(locator=namespace_locator)
        assert expected_namespace.equivalent_to(self.namespace1)

        # expect the namespace to exist
        assert metastore.namespace_exists(
            namespace="namespace1",
            catalog=self.catalog,
        )

        # expect the namespace to also be returned when listing namespaces
        list_result = metastore.list_namespaces(catalog=self.catalog)
        namespaces_by_name = {n.locator.namespace: n for n in list_result.all_items()}
        assert len(namespaces_by_name.items()) == 2
        assert namespaces_by_name["namespace1"].equivalent_to(self.namespace1)
        assert namespaces_by_name["namespace2"].equivalent_to(self.namespace2)

    def test_get_namespace(self):
        # expect the namespace to also be returned when explicitly retrieved
        read_namespace = metastore.get_namespace(
            namespace="namespace1",
            catalog=self.catalog,
        )
        assert read_namespace and read_namespace.equivalent_to(self.namespace1)

    def test_namespace_exists_existing(self):
        assert metastore.namespace_exists(
            "namespace1",
            catalog=self.catalog,
        )

    def test_namespace_not_exists(self):
        assert not metastore.namespace_exists(
            "foobar",
            catalog=self.catalog,
        )

    def test_delete_namespace(self):
        # Given a namespace that exists
        assert metastore.namespace_exists(
            namespace="namespace1",
            catalog=self.catalog,
        )

        # When we delete the namespace
        metastore.delete_namespace(
            namespace="namespace1",
            catalog=self.catalog,
        )

        # Then the namespace should not exist anymore
        assert not metastore.namespace_exists(
            namespace="namespace1",
            catalog=self.catalog,
        )

        # And it should not be listed in namespaces
        list_result = metastore.list_namespaces(catalog=self.catalog)
        namespaces = list_result.all_items()
        assert len(namespaces) == 1  # Only namespace2 should remain
        assert namespaces[0].equivalent_to(self.namespace2)

    def test_delete_namespace_not_exists(self):
        # When we try to delete a non-existent namespace
        # Then we should get a ValueError
        with pytest.raises(ValueError):
            metastore.delete_namespace(
                namespace="non_existent_namespace",
                catalog=self.catalog,
            )

    def test_delete_namespace_with_purge(self):
        # Given a namespace that exists
        assert metastore.namespace_exists(
            namespace="namespace1",
            catalog=self.catalog,
        )

        # When we delete the namespace with purge=True
        metastore.delete_namespace(
            namespace="namespace1",
            purge=True,
            catalog=self.catalog,
        )

        # Then the namespace should not exist anymore
        assert not metastore.namespace_exists(
            namespace="namespace1",
            catalog=self.catalog,
        )

        # And it should not be listed in namespaces
        list_result = metastore.list_namespaces(catalog=self.catalog)
        namespaces = list_result.all_items()
        assert len(namespaces) == 1  # Only namespace2 should remain
        assert namespaces[0].equivalent_to(self.namespace2)

    def test_delete_namespace_operations_after_delete(self):
        # Given a namespace that exists
        namespace = "namespace1"
        assert metastore.namespace_exists(
            namespace=namespace,
            catalog=self.catalog,
        )

        # When we delete the namespace
        metastore.delete_namespace(
            namespace=namespace,
            catalog=self.catalog,
        )

        # Then trying to create a table in the deleted namespace should fail
        with pytest.raises(ValueError):
            metastore.create_table_version(
                namespace=namespace,
                table_name="test_table",
                table_version="v.1",
                catalog=self.catalog,
            )

        # And trying to list tables in the deleted namespace should fail
        with pytest.raises(ValueError):
            metastore.list_tables(
                namespace=namespace,
                catalog=self.catalog,
            )

        # And trying to delete the namespace again should fail
        with pytest.raises(ValueError):
            metastore.delete_namespace(
                namespace=namespace,
                catalog=self.catalog,
            )

        # But we should be able to recreate the namespace
        recreated_namespace = metastore.create_namespace(
            namespace=namespace,
            catalog=self.catalog,
        )
        assert metastore.namespace_exists(
            namespace=namespace,
            catalog=self.catalog,
        )
        # And it should be listed in namespaces
        list_result = metastore.list_namespaces(catalog=self.catalog)
        namespaces = list_result.all_items()
        assert (
            len(namespaces) == 2
        )  # Both namespace2 and recreated namespace1 should exist
        namespaces_by_name = {n.locator.namespace: n for n in namespaces}
        assert namespace in namespaces_by_name
        assert namespaces_by_name[namespace].equivalent_to(recreated_namespace)

    def test_update_namespace_properties(self):
        # Given a namespace with no properties
        namespace = "namespace1"
        assert metastore.namespace_exists(
            namespace=namespace,
            catalog=self.catalog,
        )
        original_namespace = metastore.get_namespace(
            namespace=namespace,
            catalog=self.catalog,
        )
        assert original_namespace.properties is None

        # When we update the namespace properties
        new_properties = {"description": "Test Namespace", "owner": "test-user"}
        metastore.update_namespace(
            namespace=namespace,
            properties=new_properties,
            catalog=self.catalog,
        )

        # Then the namespace should have the new properties
        updated_namespace = metastore.get_namespace(
            namespace=namespace,
            catalog=self.catalog,
        )
        assert updated_namespace.properties == new_properties
        # And the namespace name should remain unchanged
        assert updated_namespace.namespace == namespace

    def test_update_namespace_rename(self):
        # Given a namespace that exists
        old_name = "namespace1"
        new_name = "renamed_namespace"
        assert metastore.namespace_exists(
            namespace=old_name,
            catalog=self.catalog,
        )

        # When we rename the namespace
        metastore.update_namespace(
            namespace=old_name,
            new_namespace=new_name,
            catalog=self.catalog,
        )

        # Then the namespace should exist with the new name
        assert metastore.namespace_exists(
            namespace=new_name,
            catalog=self.catalog,
        )
        # And should not exist with the old name
        assert not metastore.namespace_exists(
            namespace=old_name,
            catalog=self.catalog,
        )

        # And we should be able to get the namespace with the new name
        renamed_namespace = metastore.get_namespace(
            namespace=new_name,
            catalog=self.catalog,
        )
        assert renamed_namespace.namespace == new_name

    def test_update_namespace_not_exists(self):
        # When we try to update a non-existent namespace
        with pytest.raises(ValueError):
            metastore.update_namespace(
                namespace="non_existent_namespace",
                properties={"description": "Test"},
                catalog=self.catalog,
            )

    def test_update_namespace_rename_to_existing(self):
        # Given two existing namespaces
        namespace1 = "namespace1"
        namespace2 = "namespace2"
        assert metastore.namespace_exists(
            namespace=namespace1,
            catalog=self.catalog,
        )
        assert metastore.namespace_exists(
            namespace=namespace2,
            catalog=self.catalog,
        )

        # When we try to rename namespace1 to namespace2
        # Then it should fail since namespace2 already exists
        with pytest.raises(ValueError):
            metastore.update_namespace(
                namespace=namespace1,
                new_namespace=namespace2,
                catalog=self.catalog,
            )


class TestTable:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)
        # Create a namespace to hold our tables
        cls.test_namespace = create_test_namespace()
        cls.namespace_obj = metastore.create_namespace(
            namespace=cls.test_namespace.namespace,
            catalog=cls.catalog,
        )
        cls.test_table1 = create_test_table()
        cls.test_table1.latest_table_version = "v.1"
        cls.test_table2 = create_test_table()
        cls.test_table2.locator.table_name = "table2"
        cls.test_table2.latest_table_version = "v.1"
        # Create two table versions (their parent tables will be auto-created)
        cls.table1, cls.tv1, cls.stream1 = metastore.create_table_version(
            namespace=cls.test_table1.namespace,
            table_name=cls.test_table1.table_name,
            table_version=cls.test_table1.latest_table_version,
            table_description=cls.test_table1.description,
            table_properties=cls.test_table1.properties,
            catalog=cls.catalog,
        )
        cls.table2, cls.tv2, cls.stream2 = metastore.create_table_version(
            namespace=cls.test_table2.namespace,
            table_name=cls.test_table2.table_name,
            table_version=cls.test_table2.latest_table_version,
            table_description=cls.test_table2.description,
            table_properties=cls.test_table2.properties,
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_tables(self):
        # list the tables under our namespace
        list_result = metastore.list_tables(
            namespace=self.test_namespace.namespace,
            catalog=self.catalog,
        )
        all_tables = list_result.all_items()

        # we expect 2 distinct tables
        for table in all_tables:
            if table.table_name == self.test_table1.table_name:
                assert table.equivalent_to(self.test_table1)
            else:
                assert table.equivalent_to(self.test_table2)

    def test_get_table(self):
        # test we can retrieve table1 by name
        tbl = metastore.get_table(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        assert tbl is not None
        assert tbl.equivalent_to(self.test_table1)

    def test_table_exists_existing(self):
        # table1 should exist
        assert metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )

    def test_table_not_exists(self):
        assert not metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name="no_such_table",
            catalog=self.catalog,
        )

    def test_delete_table(self):
        # Given a table with table versions
        assert metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        # When we delete the table
        metastore.delete_table(
            namespace=self.test_namespace.namespace,
            name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        # Then the table should not exist anymore
        assert not metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        # And it should not be listed in tables
        list_result = metastore.list_tables(
            namespace=self.test_namespace.namespace,
            catalog=self.catalog,
        )
        tables = list_result.all_items()
        assert len(tables) == 1  # Only table2 should remain
        assert tables[0].equivalent_to(self.test_table2)

    def test_delete_table_with_purge(self):
        # Given a table with table versions
        assert metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        # When we delete the table with purge=True
        metastore.delete_table(
            namespace=self.test_namespace.namespace,
            name=self.test_table1.table_name,
            purge=True,
            catalog=self.catalog,
        )
        # Then the table should not exist anymore
        assert not metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        # And it should not be listed in tables
        list_result = metastore.list_tables(
            namespace=self.test_namespace.namespace,
            catalog=self.catalog,
        )
        tables = list_result.all_items()
        assert len(tables) == 1  # Only table2 should remain
        assert tables[0].equivalent_to(self.test_table2)

    def test_delete_table_not_exists(self):
        # When we try to delete a non-existent table
        # Then we should get a TableNotFoundError
        with pytest.raises(TableNotFoundError):
            metastore.delete_table(
                namespace=self.test_namespace.namespace,
                name="non_existent_table",
                catalog=self.catalog,
            )

    def test_delete_table_bad_namespace(self):
        # When we try to delete a table with a non-existent namespace
        # Then we should get a TableNotFoundError
        with pytest.raises(TableNotFoundError):
            metastore.delete_table(
                namespace="non_existent_namespace",
                name=self.test_table1.table_name,
                catalog=self.catalog,
            )

    def test_delete_table_operations_after_delete(self):
        # Given a table that we delete
        table_name = self.test_table1.table_name
        table_version = self.tv1.table_version
        metastore.delete_table(
            namespace=self.test_namespace.namespace,
            name=table_name,
            catalog=self.catalog,
        )

        # When we update the table version's description
        # This should fail with ValueError since the table no longer exists
        with pytest.raises(ValueError):
            metastore.update_table_version(
                namespace=self.test_namespace.namespace,
                table_name=table_name,
                table_version=table_version,
                description="new description",
                catalog=self.catalog,
            )

        # When we create a stream under the deleted table version
        # This should fail with ValueError since the table no longer exists
        with pytest.raises(ValueError):
            metastore.stage_stream(
                namespace=self.test_namespace.namespace,
                table_name=table_name,
                table_version=table_version,
                catalog=self.catalog,
            )

        # When we create a stream under the deleted table, but omit the table
        # version to force resolution of the latest active table version.
        # This should fail with ValueError since the table no longer exists.
        with pytest.raises(ValueError):
            metastore.stage_stream(
                namespace=self.test_namespace.namespace,
                table_name=table_name,
                catalog=self.catalog,
            )

        # When we stage a partition to the deleted table version's stream
        # This should fail with ValueError since the table no longer exists
        with pytest.raises(ValueError):
            metastore.stage_partition(
                stream=self.stream1,
                partition_values=[123, "abc"],
                partition_scheme_id=self.tv1.partition_scheme.id,
                catalog=self.catalog,
            )

        # When we try to create a new table with the same name
        table, tv, stream = metastore.create_table_version(
            namespace=self.test_namespace.namespace,
            table_name=table_name,
            table_version=table_version,
            catalog=self.catalog,
        )
        # Then the new table should be created successfully
        assert table is not None
        assert tv is not None
        assert stream is not None
        assert metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=table_name,
            catalog=self.catalog,
        )
        # And we should be able to get the table
        retrieved_table = metastore.get_table(
            namespace=self.test_namespace.namespace,
            table_name=table_name,
            catalog=self.catalog,
        )
        assert retrieved_table is not None
        assert retrieved_table.equivalent_to(table)

    def test_update_table_rename_to_existing(self):
        # Given two existing tables
        assert metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table1.table_name,
            catalog=self.catalog,
        )
        assert metastore.table_exists(
            namespace=self.test_namespace.namespace,
            table_name=self.test_table2.table_name,
            catalog=self.catalog,
        )

        # When we try to rename table1 to table2's name
        # Then it should fail since table2 already exists
        with pytest.raises(ValueError):
            metastore.update_table(
                namespace=self.test_namespace.namespace,
                table_name=self.test_table1.table_name,
                new_table_name=self.test_table2.table_name,
                catalog=self.catalog,
            )


class TestTableVersion:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)

        # create the namespace that we'll attach the base table to
        cls.namespace = create_test_namespace()
        # create the base table that we'll attach table versions to
        cls.table = create_test_table()
        # create the first table version to attach to the base table
        cls.table_version = create_test_table_version()
        # create the second table version to attach to the base table
        cls.table_version2 = create_test_table_version()
        cls.table_version2.previous_table_version = cls.table_version.table_version
        cls.table_version2.locator.table_version = "v.2"

        # create a namespace and single table
        cls.namespace_obj = metastore.create_namespace(
            namespace=cls.namespace.namespace,
            catalog=cls.catalog,
        )

        # create a "base" table with single table version attached
        cls.table1, cls.tv1, cls.stream1 = metastore.create_table_version(
            namespace=cls.table.namespace,
            table_name=cls.table.table_name,
            table_version=cls.table_version.table_version,
            schema=cls.table_version.schema,
            partition_scheme=cls.table_version.partition_scheme,
            sort_keys=cls.table_version.sort_scheme,
            table_version_description=cls.table_version.description,
            table_version_properties=cls.table_version.properties,
            table_description=cls.table.description,
            table_properties=cls.table.properties,
            supported_content_types=cls.table_version.content_types,
            catalog=cls.catalog,
        )
        # now attach a second table version to the same base table
        cls.table2, cls.tv2, cls.stream2 = metastore.create_table_version(
            namespace=cls.table.namespace,
            table_name=cls.table.table_name,
            table_version=cls.table_version2.table_version,
            schema=cls.table_version2.schema,
            partition_scheme=cls.table_version2.partition_scheme,
            sort_keys=cls.table_version2.sort_scheme,
            table_version_description=cls.table_version2.description,
            table_version_properties=cls.table_version2.properties,
            table_description=cls.table.description,
            table_properties=cls.table.properties,
            supported_content_types=cls.table_version2.content_types,
            catalog=cls.catalog,
        )
        cls.table.latest_table_version = cls.table_version2.table_version

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_create_bad_next_table_version(self):
        # given that the latest ordinal table version is 2
        table_version = create_test_table_version()
        # when we try to create ordinal table version 1 again
        # expect an error to be raised (ordinal version 3 expected)
        with pytest.raises(ValueError):
            metastore.create_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=table_version.table_version,
                schema=self.table_version.schema,
                partition_scheme=table_version.partition_scheme,
                sort_keys=self.table_version.sort_scheme,
                table_version_description=table_version.description,
                table_version_properties=table_version.properties,
                table_description=self.table.description,
                table_properties=self.table.properties,
                supported_content_types=table_version.content_types,
                catalog=self.catalog,
            )

    def test_create_next_table_version(self):
        # given that our test table's latest ordinal table version is 2
        table_version = create_test_table_version()
        table_version.locator.table_version = TableVersion.next_version(
            self.table_version2.table_version
        )
        # when we try to create the next ordinal table version (3)
        metastore.create_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=table_version.table_version,
            schema=self.table_version.schema,
            partition_scheme=table_version.partition_scheme,
            sort_keys=self.table_version.sort_scheme,
            table_version_description=table_version.description,
            table_version_properties=table_version.properties,
            table_description=self.table.description,
            table_properties=self.table.properties,
            supported_content_types=table_version.content_types,
            catalog=self.catalog,
        )
        # expect ordinal table version 3 to be successfully created
        table_version3 = metastore.get_latest_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        table_version.previous_table_version = self.table_version2.table_version
        assert table_version3.equivalent_to(table_version)

    def test_create_first_table_version_default_id_assignment(self):
        # given a new first table version created without a table version ID
        metastore.create_table_version(
            namespace=self.table.namespace,
            table_name="test_table_2",
            schema=self.table_version.schema,
            partition_scheme=self.table_version.partition_scheme,
            sort_keys=self.table_version.sort_scheme,
            table_version_description=self.table_version.description,
            table_version_properties=self.table_version.properties,
            table_description=self.table.description,
            table_properties=self.table.properties,
            supported_content_types=self.table_version.content_types,
            catalog=self.catalog,
        )
        # when we retrieve this table version
        table_version = metastore.get_latest_table_version(
            namespace=self.table.namespace,
            table_name="test_table_2",
            catalog=self.catalog,
        )
        # expect it to have the correct default table version ID assigned
        table_version.previous_table_version = self.table_version2.table_version
        table_version_default_id = TableVersion(Metafile.update_for(self.table_version))
        table_version_default_id.locator.table_version = DEFAULT_TABLE_VERSION
        assert table_version.equivalent_to(table_version)

    def test_list_table_versions(self):
        # given 2 previously created table versions in the same table
        list_result = metastore.list_table_versions(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        # when we list all table versions
        # expect the table versions fetched to be equivalent to those created
        tvs = list_result.all_items()
        for tv in tvs:
            if tv.id == self.table_version.id:
                assert tv.equivalent_to(self.table_version)
            elif tv.id == self.table_version2.id:
                assert tv.equivalent_to(self.table_version2)

    def test_list_table_versions_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # given a bad table version parent locator
            kwargs_copy[key] = "i_dont_exist"
            # when we list table versions
            # expect an error to be raised
            with pytest.raises(ValueError):
                metastore.list_table_versions(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_get_latest_table_version(self):
        # given two previously created table versions in the same table
        # when we get the latest table version
        tv = metastore.get_latest_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        # expect it to be equivalent ot the last created table version
        assert tv.equivalent_to(self.table_version2)

    def test_get_latest_table_version_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # given a bad table version parent locator
            kwargs_copy[key] = "i_dont_exist"
            # when we get the latest table version
            # expect an error to be raised
            with pytest.raises(ValueError):
                metastore.get_latest_table_version(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_update_table_version_schema_add_named_subschema(self):
        # given an update to the schema of table version 1
        old_schema = self.table_version.schema
        new_pyarrow_schema = pa.schema(
            [
                ("col_1", pa.int64()),
                ("col_2", pa.float64()),
                ("col_3", pa.string()),
            ]
        )
        new_schema = old_schema.add_subschema(
            name="test",
            schema=new_pyarrow_schema,
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            schema=new_schema,
            catalog=self.catalog,
        )
        # when we get the new schema of table version 1
        actual_schema = metastore.get_table_version_schema(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the expected schema
        assert actual_schema.equivalent_to(new_schema)
        assert not actual_schema.equivalent_to(old_schema)
        # expect the table version to have two schemas in its evolution history
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        assert len(tv.schemas) == 2
        assert tv.schemas[0].equivalent_to(old_schema)
        assert tv.schemas[1].equivalent_to(new_schema)
        # expect ONLY the schema to be updated
        expected_tv = Metafile.update_for(self.table_version)
        expected_tv.schema = tv.schema
        expected_tv.schemas = [old_schema, tv.schema]
        assert tv.equivalent_to(expected_tv)

    def test_update_table_version_schema_same_schema_id_fails(self):
        # given an update to the schema of table version 1 w/ the same schema ID
        old_schema = self.table_version.schema
        new_schema = Schema.of(
            schema=pa.schema(
                [
                    ("col_1", pa.int64()),
                    ("col_2", pa.float64()),
                    ("col_3", pa.string()),
                ]
            ),
            schema_id=old_schema.id,
        )
        # when we try to update the schema
        # expect an error to be raised
        with pytest.raises(ValueError):
            metastore.update_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=self.table_version.table_version,
                schema=new_schema,
                catalog=self.catalog,
            )

    def test_update_table_version_schema_equivalent_schema_noop(self):
        # given a noop update to the schema of table version 1
        old_schema = self.table_version.schema
        new_schema = Schema.of(
            schema=old_schema.arrow,
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            schema=new_schema,
            catalog=self.catalog,
        )
        # when we get the new schema of table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the old schema (including metadata)
        assert tv.schema.equivalent_to(old_schema, True)
        # expect it to only have one schema in its evolution history
        assert len(tv.schemas) == 1
        assert tv.schemas[0].equivalent_to(old_schema, True)
        # expect the full table version to also be unchanged
        assert tv.equivalent_to(self.table_version)

    def test_update_table_version_schema_equivalent_schema_new_id(self):
        # given an update to only the schema ID of table version 1
        old_schema = self.table_version.schema
        new_schema = Schema.of(
            schema=old_schema.arrow,
            schema_id=old_schema.id + 1,
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            schema=new_schema,
            catalog=self.catalog,
        )
        # when we get the new schema of table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the old schema (ignoring metadata)
        assert tv.schema.equivalent_to(old_schema)
        assert not tv.schema.equivalent_to(old_schema, True)
        assert tv.schema.id == new_schema.id != old_schema.id
        # expect it to have two schema in its evolution history
        assert len(tv.schemas) == 2
        assert tv.schemas[0].equivalent_to(old_schema, True)
        assert tv.schemas[0].id == old_schema.id
        assert tv.schemas[1].equivalent_to(old_schema)
        assert not tv.schemas[1].equivalent_to(old_schema, True)
        assert tv.schemas[1].id == new_schema.id != old_schema.id

    def test_update_table_version_partition_scheme(self):
        # given an update to the partition scheme of table version 1
        identity_transform = IdentityTransform.of()
        partition_keys = [
            PartitionKey.of(
                key=["some_string"],
                name="test_partition_key_string",
                field_id=1,
                transform=identity_transform,
            ),
            PartitionKey.of(
                key=["some_int32"],
                name="test_partition_key_int32",
                field_id=1,
                transform=identity_transform,
            ),
        ]
        new_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(partition_keys),
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id_2",
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            partition_scheme=new_scheme,
            catalog=self.catalog,
        )
        # when we get the new partition scheme of table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the expected scheme
        assert tv.partition_scheme.equivalent_to(new_scheme, True)
        assert tv.partition_scheme == new_scheme
        # expect the table version to have two schemes in its evolution history
        assert len(tv.partition_schemes) == 2
        old_scheme = self.table_version.partition_scheme
        assert tv.partition_schemes[0].equivalent_to(old_scheme, True)
        assert tv.partition_schemes[0] == old_scheme
        assert tv.partition_schemes[1].equivalent_to(new_scheme, True)
        assert tv.partition_schemes[1] == new_scheme
        # expect ONLY the partition scheme to be updated
        expected_tv = Metafile.update_for(self.table_version)
        expected_tv.partition_scheme = new_scheme
        expected_tv.partition_schemes = [old_scheme, new_scheme]
        assert tv.equivalent_to(expected_tv)

    def test_update_table_version_partition_scheme_same_id_fails(self):
        # given an update to table version 1 partition scheme using the same ID
        identity_transform = IdentityTransform.of()
        partition_keys = [
            PartitionKey.of(
                key=["some_string"],
                name="test_partition_key_string",
                field_id=1,
                transform=identity_transform,
            ),
            PartitionKey.of(
                key=["some_int32"],
                name="test_partition_key_int32",
                field_id=1,
                transform=identity_transform,
            ),
        ]
        new_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(partition_keys),
            name="new_partition_scheme_name",
            scheme_id="test_partition_scheme_id",
        )
        # when we try to update the partition scheme
        # expect an error to be raised
        with pytest.raises(ValueError):
            metastore.update_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=self.table_version.table_version,
                partition_scheme=new_scheme,
                catalog=self.catalog,
            )

    def test_update_table_version_partition_scheme_equivalent_scheme_noop(self):
        # given a noop update to the partition scheme of table version 1
        old_scheme = self.table_version.partition_scheme
        new_scheme = copy.deepcopy(old_scheme)
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            partition_scheme=new_scheme,
            catalog=self.catalog,
        )
        # when we get the new partition scheme of table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equal the old scheme (including identifiers)
        assert tv.partition_scheme.equivalent_to(old_scheme, True)
        assert tv.partition_scheme == old_scheme
        # expect it to only have one scheme in its evolution history
        assert len(tv.partition_schemes) == 1
        assert tv.partition_schemes[0].equivalent_to(old_scheme, True)
        assert tv.partition_schemes[0] == old_scheme
        # expect the full table version to also be unchanged
        assert tv.equivalent_to(self.table_version)

    def test_update_table_version_partition_scheme_equivalent_scheme_new_id(self):
        # given an update to only the partition scheme ID of table version 1
        old_scheme = self.table_version.partition_scheme
        new_scheme = PartitionScheme.of(
            keys=copy.deepcopy(old_scheme.keys),
            name=old_scheme.name,
            scheme_id=old_scheme.id + "_2",
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            partition_scheme=new_scheme,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the old scheme (ignoring identifiers)
        assert tv.partition_scheme.equivalent_to(old_scheme, False)
        assert not tv.partition_scheme.equivalent_to(old_scheme, True)
        # expect it to have two schemes in its evolution history
        assert len(tv.partition_schemes) == 2
        assert tv.partition_schemes[0].equivalent_to(old_scheme, True)
        assert tv.partition_schemes[0].id == old_scheme.id != new_scheme.id
        assert tv.partition_schemes[1].equivalent_to(old_scheme)
        assert not tv.partition_schemes[1].equivalent_to(old_scheme, True)
        assert tv.partition_schemes[1].id == new_scheme.id != old_scheme.id

    def test_update_table_version_partition_scheme_equivalent_scheme_new_name(self):
        # given an update to the partition scheme name & ID of table version 1
        old_scheme = self.table_version.partition_scheme
        new_scheme = PartitionScheme.of(
            keys=copy.deepcopy(old_scheme.keys),
            name=old_scheme.name + "_2",
            scheme_id=old_scheme.id + "_2",
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            partition_scheme=new_scheme,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the old scheme (ignoring identifiers)
        assert tv.partition_scheme.equivalent_to(old_scheme, False)
        assert tv.partition_scheme.id == new_scheme.id != old_scheme.id
        assert tv.partition_scheme.name == new_scheme.name != old_scheme.name
        assert not tv.partition_scheme.equivalent_to(old_scheme, True)
        # expect it to have two schemes in its evolution history
        assert len(tv.partition_schemes) == 2
        assert tv.partition_schemes[0].equivalent_to(old_scheme, True)
        assert tv.partition_schemes[0].id == old_scheme.id
        assert tv.partition_schemes[1].equivalent_to(old_scheme)
        assert not tv.partition_schemes[1].equivalent_to(old_scheme, True)
        assert tv.partition_schemes[1].id == new_scheme.id != old_scheme.id
        assert tv.partition_schemes[1].name == new_scheme.name != old_scheme.name

    def test_update_table_version_sort_scheme(self):
        # given an update to the sort scheme of table version 1
        sort_keys = [
            SortKey.of(
                key=["some_int32"],
                transform=IdentityTransform.of(),
            )
        ]
        new_scheme = SortScheme.of(
            keys=SortKeyList.of(sort_keys),
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id_2",
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            sort_keys=new_scheme,
            catalog=self.catalog,
        )
        # when we get the new sort scheme of table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the expected scheme
        assert tv.sort_scheme.equivalent_to(new_scheme, True)
        assert tv.sort_scheme == new_scheme
        # expect the table version to have two schemes in its evolution history
        assert len(tv.sort_schemes) == 2
        old_scheme = self.table_version.sort_scheme
        assert tv.sort_schemes[0].equivalent_to(old_scheme, True)
        assert tv.sort_schemes[0] == old_scheme
        assert tv.sort_schemes[1].equivalent_to(new_scheme, True)
        assert tv.sort_schemes[1] == new_scheme
        # expect ONLY the sort scheme to be updated
        expected_tv = Metafile.update_for(self.table_version)
        expected_tv.sort_scheme = new_scheme
        expected_tv.sort_schemes = [old_scheme, new_scheme]
        assert tv.equivalent_to(expected_tv)

    def test_update_table_version_sort_scheme_same_id_fails(self):
        # given an update to table version 1 sort scheme using the same ID
        sort_keys = [
            SortKey.of(
                key=["some_int32"],
                transform=IdentityTransform.of(),
            )
        ]
        new_scheme = SortScheme.of(
            keys=SortKeyList.of(sort_keys),
            name="new_sort_scheme_name",
            scheme_id="test_sort_scheme_id",
        )
        # when we try to update the sort scheme
        # expect an error to be raised
        with pytest.raises(ValueError):
            metastore.update_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=self.table_version.table_version,
                sort_keys=new_scheme,
                catalog=self.catalog,
            )

    def test_update_table_version_sort_scheme_equivalent_scheme_noop(self):
        # given a noop update to the sort scheme of table version 1
        old_scheme = self.table_version.sort_scheme
        new_scheme = copy.deepcopy(old_scheme)
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            sort_keys=new_scheme,
            catalog=self.catalog,
        )
        # when we get the new sort scheme of table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equal the old scheme (including identifiers)
        assert tv.sort_scheme.equivalent_to(old_scheme, True)
        assert tv.sort_scheme == old_scheme
        # expect it to only have one scheme in its evolution history
        assert len(tv.sort_schemes) == 1
        assert tv.sort_schemes[0].equivalent_to(old_scheme, True)
        assert tv.sort_schemes[0] == old_scheme
        # expect the full table version to also be unchanged
        assert tv.equivalent_to(self.table_version)

    def test_update_table_version_sort_scheme_equivalent_scheme_new_id(self):
        # given an update to only the sort scheme ID of table version 1
        old_scheme = self.table_version.sort_scheme
        new_scheme = SortScheme.of(
            keys=copy.deepcopy(old_scheme.keys),
            name=old_scheme.name,
            scheme_id=old_scheme.id + "_2",
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            sort_keys=new_scheme,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the old scheme (ignoring identifiers)
        assert tv.sort_scheme.equivalent_to(old_scheme, False)
        assert not tv.sort_scheme.equivalent_to(old_scheme, True)
        # expect it to have two schemes in its evolution history
        assert len(tv.sort_schemes) == 2
        assert tv.sort_schemes[0].equivalent_to(old_scheme, True)
        assert tv.sort_schemes[0].id == old_scheme.id != new_scheme.id
        assert tv.sort_schemes[1].equivalent_to(old_scheme)
        assert not tv.sort_schemes[1].equivalent_to(old_scheme, True)
        assert tv.sort_schemes[1].id == new_scheme.id != old_scheme.id

    def test_update_table_version_sort_scheme_equivalent_scheme_new_name(self):
        # given an update to the sort scheme name & ID of table version 1
        old_scheme = self.table_version.sort_scheme
        new_scheme = SortScheme.of(
            keys=copy.deepcopy(old_scheme.keys),
            name=old_scheme.name + "_2",
            scheme_id=old_scheme.id + "_2",
        )
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            sort_keys=new_scheme,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to be equivalent to the old scheme (ignoring identifiers)
        assert tv.sort_scheme.equivalent_to(old_scheme, False)
        assert tv.sort_scheme.id == new_scheme.id != old_scheme.id
        assert tv.sort_scheme.name == new_scheme.name != old_scheme.name
        assert not tv.sort_scheme.equivalent_to(old_scheme, True)
        # expect it to have two schemes in its evolution history
        assert len(tv.sort_schemes) == 2
        assert tv.sort_schemes[0].equivalent_to(old_scheme, True)
        assert tv.sort_schemes[0].id == old_scheme.id
        assert tv.sort_schemes[1].equivalent_to(old_scheme)
        assert not tv.sort_schemes[1].equivalent_to(old_scheme, True)
        assert tv.sort_schemes[1].id == new_scheme.id != old_scheme.id
        assert tv.sort_schemes[1].name == new_scheme.name != old_scheme.name

    def test_update_table_version_description(self):
        # given an update to the description of table version 1
        new_description = "new description"
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            description=new_description,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to contain the new description
        assert tv.description == new_description != self.table_version.description
        # expect ONLY the description to be updated
        expected_tv = Metafile.update_for(self.table_version)
        expected_tv.description = new_description
        assert tv.equivalent_to(expected_tv)

    def test_update_table_version_description_empty(self):
        # given an update to create an empty description of table version 1
        new_description = ""
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            description=new_description,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to contain the new description
        assert tv.description == new_description != self.table_version.description

    def test_update_table_version_description_noop(self):
        # given an attempt to set the description of table version 1
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            description=None,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to contain the old description (None == noop)
        assert tv.description == self.table_version.description
        # expect the full table version to also be unchanged
        assert tv.equivalent_to(self.table_version)

    def test_update_table_version_properties(self):
        # given an update to the properties of table version 1
        new_properties = {"new_property_key": "new_property_value"}
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            properties=new_properties,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to contain the new properties
        assert tv.properties == new_properties != self.table_version.properties
        # expect ONLY the properties to be updated
        expected_tv = Metafile.update_for(self.table_version)
        expected_tv.properties = new_properties
        assert tv.equivalent_to(expected_tv)

    def test_update_table_version_properties_empty(self):
        # given an update to leave table version 1 properties empty
        new_properties = {}
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            properties=new_properties,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to contain the new properties
        assert tv.properties == new_properties != self.table_version.properties

    def test_update_table_version_properties_noop(self):
        # given an attempt to set the properties of table version 1
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            properties=None,
            catalog=self.catalog,
        )
        # when we get table version 1
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect it to contain the old properties (None == noop)
        assert tv.properties == self.table_version.properties
        # expect the full table version to also be unchanged
        assert tv.equivalent_to(self.table_version)

    def test_get_latest_active_table_version(self):
        # given two table versions but no active table version
        # when we get the latest active table version
        tv = metastore.get_latest_active_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        # expect it to be undefined
        assert tv is None
        # when we get the parent table
        table = metastore.get_table(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        # expect its latest table version to be table version 2
        assert table.latest_table_version == self.table_version2.table_version
        # expect its latest active table version to be None
        assert table.latest_active_table_version is None
        # expect table attributes to be equal to the original parent table
        assert table.equivalent_to(self.table)

        # given an update to make table version 1 active
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )
        # when we get the latest active table version
        tv = metastore.get_latest_active_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        # expect it to be table version 1
        active_table_version = TableVersion(
            Metafile.update_for(self.table_version),
        )
        active_table_version.state = LifecycleState.ACTIVE
        assert tv.equivalent_to(active_table_version)
        # given an update to make table version 2 active
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version2.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )
        # when we get the latest active table version
        tv = metastore.get_latest_active_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        # expect it to be table version 2
        active_table_version2 = TableVersion(
            Metafile.update_for(self.table_version2),
        )
        active_table_version2.state = LifecycleState.ACTIVE
        assert tv.equivalent_to(active_table_version2)

    def test_get_latest_active_table_version_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # given a bad table version parent locator
            kwargs_copy[key] = "i_dont_exist"
            # when we get the latest active table version
            # expect an error to be raised
            with pytest.raises(ValueError):
                metastore.get_latest_active_table_version(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_get_table_version(self):
        # given a previously created table version
        # when we explicitly get that table version by ID
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        # expect the table version returned to be equivalent to the one created
        assert tv.equivalent_to(self.table_version)

    def test_get_table_version_not_exists(self):
        # given a previously created table
        # when we explicitly try to get table version whose ID doesn't exist
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version="v.999",
            catalog=self.catalog,
        )
        # expect nothing to be returned
        assert tv is None

    def test_get_table_version_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # given a bad table version parent locator
            kwargs_copy[key] = "i_dont_exist"
            # when we try to explicitly get a table version by ID
            # expect result to be None
            assert (
                metastore.get_table_version(
                    table_version=self.table_version.table_version,
                    catalog=self.catalog,
                    **kwargs_copy,
                )
                is None
            )

    def test_table_version_exists(self):
        # given a previously created table version
        # when we check if that table version exists by ID
        # expect the check to pass
        assert metastore.table_version_exists(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )

    def test_table_version_not_exists(self):
        # given a previously created table
        # when we check if a non-existent table version ID exists
        # expect the check to fail
        assert not metastore.table_version_exists(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version="v.999",
            catalog=self.catalog,
        )

    def test_table_version_exists_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # given a bad table version parent locator
            kwargs_copy[key] = "i_dont_exist"
            # when we try to explicitly check if a table version exists by ID
            # expect empty results
            assert not metastore.table_version_exists(
                table_version=self.table_version.table_version,
                catalog=self.catalog,
                **kwargs_copy,
            )

    def test_creation_fails_if_already_exists(self):
        # given an existing table version
        # when we try to create a table version with the same ID
        # expect an error to be raised
        with pytest.raises(ValueError):
            metastore.create_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=self.table_version.table_version,
                catalog=self.catalog,
            )

    def test_update_table_version_to_unpartitioned(self):
        # Given a table version with a partition scheme
        assert self.tv1.partition_scheme is not None
        assert not self.tv1.partition_scheme.equivalent_to(UNPARTITIONED_SCHEME)

        # When we update it to use UNPARTITIONED_SCHEME
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            partition_scheme=UNPARTITIONED_SCHEME,
            catalog=self.catalog,
        )

        # Then the table version should be updated to use UNPARTITIONED_SCHEME
        updated_tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        assert updated_tv.partition_scheme is not None
        assert updated_tv.partition_scheme.equivalent_to(UNPARTITIONED_SCHEME)
        assert updated_tv.partition_scheme.id == UNPARTITIONED_SCHEME_ID
        assert updated_tv.partition_scheme.name == UNPARTITIONED_SCHEME_NAME
        assert updated_tv.partition_scheme.keys is None

        # Verify the partition scheme history is updated correctly
        assert len(updated_tv.partition_schemes) == 2
        assert updated_tv.partition_schemes[0].equivalent_to(self.tv1.partition_scheme)
        assert updated_tv.partition_schemes[1].equivalent_to(UNPARTITIONED_SCHEME)

    def test_update_table_version_to_unsorted(self):
        # Given a table version with a sort scheme
        assert self.tv1.sort_scheme is not None

        # When we update it to use UNSORTED_SCHEME
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            sort_keys=UNSORTED_SCHEME,
            catalog=self.catalog,
        )

        # Then the table version should be updated to use UNSORTED_SCHEME
        updated_tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        assert updated_tv.sort_scheme is not None
        assert updated_tv.sort_scheme.equivalent_to(UNSORTED_SCHEME)
        assert updated_tv.sort_scheme.id == UNSORTED_SCHEME_ID
        assert updated_tv.sort_scheme.name == UNSORTED_SCHEME_NAME
        assert updated_tv.sort_scheme.keys is None

        # Verify the sort scheme history is updated correctly
        assert len(updated_tv.sort_schemes) == 2
        assert updated_tv.sort_schemes[0].equivalent_to(self.tv1.sort_scheme)
        assert updated_tv.sort_schemes[1].equivalent_to(UNSORTED_SCHEME)

    def test_create_table_version_validates_partition_scheme(self):
        # Test that creating a table version validates partition scheme fields exist in schema
        schema = Schema.of(
            schema=pa.schema(
                [
                    ("col1", pa.int32()),
                    ("col2", pa.string()),
                ]
            ),
        )
        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(
                [
                    PartitionKey.of(
                        key=[
                            "non_existent_field"
                        ],  # Field that doesn't exist in schema
                        transform=IdentityTransform.of(),
                    )
                ]
            ),
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )

        # Expect error when field doesn't exist in schema
        with pytest.raises(
            ValueError,
            match="Partition key field 'non_existent_field' not found in schema",
        ):
            metastore.create_table_version(
                namespace=self.table.namespace,
                table_name="validation_test_table",
                table_version="v.1",
                schema=schema,
                partition_scheme=partition_scheme,
                catalog=self.catalog,
            )

    def test_create_table_version_validates_sort_scheme(self):
        # Test that creating a table version validates sort scheme fields exist in schema
        schema = Schema.of(
            schema=pa.schema(
                [
                    ("col1", pa.int32()),
                    ("col2", pa.string()),
                ]
            ),
        )
        sort_scheme = SortScheme.of(
            keys=SortKeyList.of(
                [
                    SortKey.of(
                        key=[
                            "non_existent_field"
                        ],  # Field that doesn't exist in schema
                        sort_order=SortOrder.ASCENDING,
                        null_order=NullOrder.AT_END,
                    )
                ]
            ),
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )

        # Expect error when field doesn't exist in schema
        with pytest.raises(
            ValueError, match="Sort key field 'non_existent_field' not found in schema"
        ):
            metastore.create_table_version(
                namespace=self.table.namespace,
                table_name="validation_test_table",
                table_version="v.1",
                schema=schema,
                sort_keys=sort_scheme,
                catalog=self.catalog,
            )

    def test_update_table_version_validates_partition_scheme(self):
        # Test that updating a table version validates partition scheme fields exist in schema
        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(
                [
                    PartitionKey.of(
                        key=[
                            "non_existent_field"
                        ],  # Field that doesn't exist in schema
                        transform=IdentityTransform.of(),
                    )
                ]
            ),
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id_2",
        )

        # Expect error when field doesn't exist in schema
        with pytest.raises(
            ValueError,
            match="Partition key field 'non_existent_field' not found in schema",
        ):
            metastore.update_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=self.table_version.table_version,
                partition_scheme=partition_scheme,
                catalog=self.catalog,
            )

    def test_update_table_version_validates_sort_scheme(self):
        # Test that updating a table version validates sort scheme fields exist in schema
        sort_scheme = SortScheme.of(
            keys=SortKeyList.of(
                [
                    SortKey.of(
                        key=[
                            "non_existent_field"
                        ],  # Field that doesn't exist in schema
                        sort_order=SortOrder.ASCENDING,
                        null_order=NullOrder.AT_END,
                    )
                ]
            ),
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id_2",
        )

        # Expect error when field doesn't exist in schema
        with pytest.raises(
            ValueError, match="Sort key field 'non_existent_field' not found in schema"
        ):
            metastore.update_table_version(
                namespace=self.table.namespace,
                table_name=self.table.table_name,
                table_version=self.table_version.table_version,
                sort_keys=sort_scheme,
                catalog=self.catalog,
            )

    def test_validation_skipped_if_schema_none(self):
        # Test that validation is skipped if schema is None
        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(
                [
                    PartitionKey.of(
                        key=["any_field"],  # Can be any field since schema is None
                        transform=IdentityTransform.of(),
                    )
                ]
            ),
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )
        sort_scheme = SortScheme.of(
            keys=SortKeyList.of(
                [
                    SortKey.of(
                        key=["any_field"],  # Can be any field since schema is None
                        sort_order=SortOrder.ASCENDING,
                        null_order=NullOrder.AT_END,
                    )
                ]
            ),
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )

        # Should not raise error even with non-existent fields
        metastore.create_table_version(
            namespace=self.table.namespace,
            table_name="validation_test_table",
            table_version="v.1",
            schema=None,
            partition_scheme=partition_scheme,
            sort_keys=sort_scheme,
            catalog=self.catalog,
        )


class TestStream:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)
        metastore.create_namespace(
            "test_stream_ns",
            catalog=cls.catalog,
        )
        # Create a table version.
        # This call should automatically create a default DeltaCAT stream.
        cls.table, cls.tv, cls.stream = metastore.create_table_version(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=cls.catalog,
        )
        # Retrieve the auto-created default stream.
        cls.default_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=cls.catalog,
        )
        # Ensure that the default stream was auto-created.
        assert cls.default_stream is not None, "Default stream not found."
        assert cls.default_stream.equivalent_to(cls.stream)

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_streams(self):
        list_result = metastore.list_streams(
            "test_stream_ns",
            "mystreamtable",
            "v.1",
            catalog=self.catalog,
        )
        streams = list_result.all_items()
        # We expect exactly one stream (the default "deltacat" stream).
        assert len(streams) == 1
        assert streams[0].equivalent_to(self.default_stream)

    def test_stream_exists(self):
        exists = metastore.stream_exists(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        assert exists

    def test_stream_not_exists(self):
        exists = metastore.stream_exists(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            stream_format=StreamFormat.ICEBERG,
            catalog=self.catalog,
        )
        assert not exists

    def test_stream_exists_bad_parent_locator(self):
        kwargs = {
            "namespace": "test_stream_ns",
            "table_name": "mystreamtable",
            "table_version": "v.1",
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # table version format must be v.N to not raise a ValueError
            kwargs_copy[key] = "i_dont_exist" if key != "table_version" else "v.1000"
            assert not metastore.stream_exists(
                catalog=self.catalog,
                **kwargs_copy,
            )

    def test_list_streams_bad_parent_locator(self):
        kwargs = {
            "namespace": "test_stream_ns",
            "table_name": "mystreamtable",
            "table_version": "v.1",
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.list_streams(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_get_stream(self):
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        assert stream.equivalent_to(self.default_stream)

    def test_get_stream_bad_parent_locator(self):
        kwargs = {
            "namespace": "test_stream_ns",
            "table_name": "mystreamtable",
            "table_version": "v.1",
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            # table version format must be v.N to not raise a ValueError
            kwargs_copy[key] = "i_dont_exist" if key != "table_version" else "v.1000"
            assert (
                metastore.get_stream(
                    catalog=self.catalog,
                    **kwargs_copy,
                )
                is None
            )

    def test_get_missing_stream(self):
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            stream_format=StreamFormat.ICEBERG,
            catalog=self.catalog,
        )
        assert stream is None

    def test_list_stream_partitions_empty(self):
        # Given a stream with no partitions
        # When we list the partitions
        list_result = metastore.list_stream_partitions(
            stream=self.stream,
            catalog=self.catalog,
        )
        # Then we should get an empty list
        all_streams = list_result.all_items()
        assert len(all_streams) == 0

    def test_delete_stream(self):
        # Given a directive to delete the default stream
        metastore.delete_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        # When we try to get the last committed stream
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        # Expect nothing to be returned
        assert stream is None

        # Even when we try to get the last committed stream by ID
        stream = metastore.get_stream_by_id(
            table_version_locator=TableVersionLocator.at(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v.1",
            ),
            stream_id=self.default_stream.id,
            catalog=self.catalog,
        )
        # Expect nothing to be returned
        assert stream is None
        # TODO(pdames): Add new getter method for deleted but not GC'd streams?

    def test_delete_stream_latest_table_version(self):
        # Given an update to make table version 1 active
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.tv.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )
        # Ensure that a stream under the latest active table version exists
        latest_active_stream_exists = metastore.stream_exists(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        assert latest_active_stream_exists

        # Ensure that we can get the stream under the latest active table version
        latest_active_stream = metastore.get_stream(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        assert latest_active_stream is not None
        assert latest_active_stream.equivalent_to(self.default_stream)

        # Given a directive to delete the default stream from the latest
        # active table version.
        metastore.delete_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            catalog=self.catalog,
        )
        # When we try to get the last committed stream
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        # Expect nothing to be returned
        assert stream is None

        # Even when we try to get the last committed stream by ID
        stream = metastore.get_stream_by_id(
            table_version_locator=TableVersionLocator.at(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v.1",
            ),
            stream_id=self.default_stream.id,
            catalog=self.catalog,
        )
        # Expect nothing to be returned
        assert stream is None
        # TODO(pdames): Add new getter method for deleted but not GC'd streams?

    def test_delete_missing_stream(self):
        with pytest.raises(ValueError):
            metastore.delete_stream(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v.1",
                stream_format=StreamFormat.ICEBERG,
                catalog=self.catalog,
            )

    def test_delete_stream_bad_parent_locator(self):
        kwargs = {
            "namespace": "test_stream_ns",
            "table_name": "mystreamtable",
            "table_version": "v.1",
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.delete_stream(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_stage_and_commit_stream_replacement(self):
        # Given a staged stream that overwrites the default stream
        staged_stream = metastore.stage_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        # When that staged stream is retrieved by ID
        fetched_stream = metastore.get_stream_by_id(
            table_version_locator=TableVersionLocator.at(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v.1",
            ),
            stream_id=staged_stream.stream_id,
            catalog=self.catalog,
        )
        # Ensure that it is equivalent to the stream we staged
        assert fetched_stream.id == staged_stream.id == fetched_stream.stream_id
        assert fetched_stream.equivalent_to(staged_stream)
        # Also ensure that the last committed deltacat stream returned is
        # NOT the staged stream, but the committed default stream.
        fetched_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        assert fetched_stream.id == self.default_stream.id == fetched_stream.stream_id
        assert fetched_stream.equivalent_to(self.default_stream)
        # Given a committed stream that replaces the default stream
        committed_stream = metastore.commit_stream(
            stream=staged_stream,
            catalog=self.catalog,
        )
        # When the last committed stream is retrieved
        fetched_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v.1",
            catalog=self.catalog,
        )
        # Ensure that it is equivalent to the stream we committed
        assert fetched_stream.id == committed_stream.id == fetched_stream.stream_id
        assert fetched_stream.equivalent_to(committed_stream)
        list_result = metastore.list_streams(
            "test_stream_ns",
            "mystreamtable",
            "v.1",
            catalog=self.catalog,
        )
        streams = list_result.all_items()
        # Only the newly committed stream should be listed
        assert len(streams) == 1
        assert streams[0].equivalent_to(committed_stream)


class TestPartition:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)
        # Create a namespace and table version with a stream to attach partitions to
        cls.namespace = metastore.create_namespace(
            namespace="test_partition_ns",
            catalog=cls.catalog,
        )
        # Create a partition scheme
        cls.partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(
                [
                    PartitionKey.of(
                        key=["col1"],
                        transform=IdentityTransform.of(),
                    ),
                    PartitionKey.of(
                        key=["col2"],
                        transform=IdentityTransform.of(),
                    ),
                ]
            ),
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )
        # Create a table version with a schema and partition scheme
        cls.table, cls.tv, cls.stream = metastore.create_table_version(
            namespace="test_partition_ns",
            table_name="mypartitiontable",
            table_version="v.1",
            schema=Schema.of(
                schema=pa.schema(
                    [
                        ("col1", pa.int32()),
                        ("col2", pa.string()),
                    ]
                ),
            ),
            partition_scheme=cls.partition_scheme,
            catalog=cls.catalog,
        )
        # Verify that the partition scheme was properly set up
        assert cls.tv.partition_scheme is not None
        assert cls.tv.partition_scheme.equivalent_to(cls.partition_scheme)
        assert cls.stream.partition_scheme is not None
        assert cls.stream.partition_scheme.equivalent_to(cls.partition_scheme)
        # Create an unpartitioned table version
        (
            cls.unpartitioned_table,
            cls.unpartitioned_tv,
            cls.unpartitioned_stream,
        ) = metastore.create_table_version(
            namespace="test_partition_ns",
            table_name="myunpartitionedtable",
            table_version="v.1",
            schema=Schema.of(
                schema=pa.schema(
                    [
                        ("col1", pa.int32()),
                        ("col2", pa.string()),
                    ]
                ),
            ),
            catalog=cls.catalog,
        )
        # Verify that the unpartitioned table version has UNPARTITIONED_SCHEME
        assert cls.unpartitioned_tv.partition_scheme is not None
        assert cls.unpartitioned_tv.partition_scheme.equivalent_to(UNPARTITIONED_SCHEME)
        assert cls.unpartitioned_tv.partition_scheme.id == UNPARTITIONED_SCHEME_ID
        assert cls.unpartitioned_tv.partition_scheme.name == UNPARTITIONED_SCHEME_NAME

    @classmethod
    def teardown_method(cls):
        pass
        # shutil.rmtree(cls.tmpdir)

    def test_stage_partition(self):
        # Given a partition scheme and values
        partition_values = [123, "abc"]
        # When we stage a partition
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        # Then the partition should be staged correctly
        assert staged_partition is not None
        assert staged_partition.partition_values == partition_values
        assert staged_partition.partition_scheme_id == self.tv.partition_scheme.id

    def test_commit_partition(self):
        # Given a staged partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        # When we commit the partition
        committed_partition = metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )
        # Then the partition should be committed correctly
        assert committed_partition is not None
        assert committed_partition.partition_values == partition_values
        assert committed_partition.partition_scheme_id == self.tv.partition_scheme.id

    def test_get_partition(self):
        # Given a committed partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition = metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )

        # Validate the committed partition
        assert committed_partition is not None
        assert committed_partition.state == CommitState.COMMITTED
        assert committed_partition.partition_values == partition_values
        assert committed_partition.partition_scheme_id == self.tv.partition_scheme.id
        assert committed_partition.previous_partition_id is None
        assert committed_partition.locator is not None
        assert committed_partition.locator.stream_locator == self.stream.locator

        # When we get the partition
        retrieved_partition = metastore.get_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # Then we should get the correct partition
        assert retrieved_partition is not None
        assert retrieved_partition == committed_partition

    def test_get_partition_by_id(self):
        # Given a committed partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition = metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )

        # When we get the partition by ID
        retrieved_partition = metastore.get_partition_by_id(
            stream_locator=self.stream.locator,
            partition_id=committed_partition.partition_id,
            catalog=self.catalog,
        )

        # Then we should get the correct partition
        assert retrieved_partition is not None
        assert retrieved_partition == committed_partition
        assert retrieved_partition.partition_id == committed_partition.partition_id
        assert retrieved_partition.state == CommitState.COMMITTED
        assert retrieved_partition.partition_values == partition_values
        assert retrieved_partition.partition_scheme_id == self.tv.partition_scheme.id

    def test_get_partition_by_id_not_exists(self):
        # When we try to get a partition with a non-existent ID
        non_existent_id = "non_existent_partition_id"
        retrieved_partition = metastore.get_partition_by_id(
            stream_locator=self.stream.locator,
            partition_id=non_existent_id,
            catalog=self.catalog,
        )

        # Then we should get None
        assert retrieved_partition is None

    def test_get_partition_by_id_staged(self):
        # Given a staged partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # When we get the partition by ID
        retrieved_partition = metastore.get_partition_by_id(
            stream_locator=self.stream.locator,
            partition_id=staged_partition.partition_id,
            catalog=self.catalog,
        )

        # Then we should get the staged partition
        assert retrieved_partition is not None
        assert retrieved_partition.equivalent_to(staged_partition)
        assert retrieved_partition.partition_id == staged_partition.partition_id
        assert retrieved_partition.state == CommitState.STAGED
        assert retrieved_partition.partition_values == partition_values
        assert retrieved_partition.partition_scheme_id == self.tv.partition_scheme.id

    def test_get_partition_by_id_bad_stream_locator(self):
        # Given a committed partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition = metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )

        # Create a bad stream locator
        bad_stream_locator = StreamLocator.of(
            table_version_locator=TableVersionLocator.at(
                namespace="non_existent_namespace",
                table_name="non_existent_table",
                table_version="v.1",
            ),
            stream_id="non_existent_stream",
            stream_format=StreamFormat.DELTACAT,
        )

        # When we try to get the partition using a bad stream locator
        # Then we should get None
        retrieved_partition = metastore.get_partition_by_id(
            stream_locator=bad_stream_locator,
            partition_id=committed_partition.partition_id,
            catalog=self.catalog,
        )
        assert retrieved_partition is None

    def test_get_partition_not_exists(self):
        # Given a partition scheme and values that don't exist
        partition_values = [456, "def"]
        # When we try to get a non-existent partition
        # Then we should get None
        assert (
            metastore.get_partition(
                stream_locator=self.stream.locator,
                partition_values=partition_values,
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )
            is None
        )

    def test_list_stream_partitions(self):
        # Given two committed partitions
        partition_values1 = [123, "abc"]
        partition_values2 = [456, "def"]
        staged_partition1 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values1,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition1 = metastore.commit_partition(
            partition=staged_partition1,
            catalog=self.catalog,
        )
        staged_partition2 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values2,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition2 = metastore.commit_partition(
            partition=staged_partition2,
            catalog=self.catalog,
        )
        # When we list the partitions
        list_result = metastore.list_stream_partitions(
            stream=self.stream,
            catalog=self.catalog,
        )
        # Then we should get both partitions in a proper ListResult
        partitions_list = list_result.all_items()
        assert len(partitions_list) == 2

        # Verify partition values are correct
        partition_values = [p.partition_values for p in partitions_list]
        assert partition_values1 in partition_values
        assert partition_values2 in partition_values

        # Verify all returned partitions are committed and equivalent to the original committed partitions
        for p in partitions_list:
            assert p.state == CommitState.COMMITTED
            assert p.partition_scheme_id == self.tv.partition_scheme.id
            # Find the corresponding original committed partition
            if p.partition_values == partition_values1:
                assert p.equivalent_to(committed_partition1)
            else:
                assert p.equivalent_to(committed_partition2)

    def test_partition_replacement(self):
        # Given an initial committed partition
        partition_values = [123, "abc"]
        staged_partition1 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition1 = metastore.commit_partition(
            partition=staged_partition1,
            catalog=self.catalog,
        )
        # Validate the first committed partition
        assert committed_partition1.state == CommitState.COMMITTED
        assert committed_partition1.partition_values == partition_values
        assert committed_partition1.partition_scheme_id == self.tv.partition_scheme.id
        assert committed_partition1.previous_partition_id is None
        # Verify it can be retrieved directly
        retrieved_partition1 = metastore.get_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        assert retrieved_partition1.equivalent_to(committed_partition1)
        # When we stage and commit a new partition with the same values
        staged_partition2 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition2 = metastore.commit_partition(
            partition=staged_partition2,
            catalog=self.catalog,
        )
        # Then the new partition should replace the old one
        assert committed_partition2.previous_partition_id == committed_partition1.id
        retrieved_partition = metastore.get_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        assert retrieved_partition.id == committed_partition2.id

    def test_delete_partition(self):
        # Given a committed partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        committed_partition = metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )

        # When we delete the partition
        metastore.delete_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # Then the partition should not be retrievable
        retrieved_partition = metastore.get_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        assert retrieved_partition is None

        # And it should not be listed in stream partitions
        list_result = metastore.list_stream_partitions(
            stream=self.stream,
            catalog=self.catalog,
        )
        assert committed_partition not in list_result.all_items()

    def test_delete_partition_not_exists(self):
        # When we try to delete a non-existent partition
        partition_values = [456, "def"]
        # Then we should get a ValueError
        with pytest.raises(ValueError):
            metastore.delete_partition(
                stream_locator=self.stream.locator,
                partition_values=partition_values,
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

    def test_delete_partition_staged(self):
        # Given a staged partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # When we try to delete the staged partition
        # Then we should get a ValueError since only committed partitions
        # can be deleted
        with pytest.raises(ValueError):
            metastore.delete_partition(
                stream_locator=self.stream.locator,
                partition_values=partition_values,
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

    def test_delete_partition_operations_after_delete(self):
        # Given a committed partition
        partition_values = [123, "abc"]
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )

        # When we delete the partition
        metastore.delete_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # Then staging a new partition with the same values should work
        new_staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        assert new_staged_partition is not None
        assert new_staged_partition.partition_values == partition_values

        # And committing the new partition should work
        new_committed_partition = metastore.commit_partition(
            partition=new_staged_partition,
            catalog=self.catalog,
        )
        assert new_committed_partition is not None
        assert new_committed_partition.state == CommitState.COMMITTED
        assert new_committed_partition.partition_values == partition_values

        # And the new partition should be retrievable
        retrieved_partition = metastore.get_partition(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        assert retrieved_partition is not None
        assert retrieved_partition.equivalent_to(new_committed_partition)

    def test_stage_partition_bad_scheme_id(self):
        # Given a partition scheme ID that doesn't exist
        partition_values = [123, "abc"]
        bad_scheme_id = "nonexistent_scheme_id"
        # When we try to stage a partition with a bad scheme ID
        # Then we should get a ValueError
        with pytest.raises(ValueError):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=partition_values,
                partition_scheme_id=bad_scheme_id,
                catalog=self.catalog,
            )

    def test_commit_partition_without_staging(self):
        # Given a partition that hasn't been staged
        partition_values = [123, "abc"]
        partition_locator = PartitionLocator.of(
            stream_locator=self.stream.locator,
            partition_values=partition_values,
            partition_id=str(uuid.uuid4()),
        )
        partition = Partition.of(
            locator=partition_locator,
            schema=None,
            content_types=None,
            state=CommitState.STAGED,
            partition_scheme_id=self.tv.partition_scheme.id,
        )
        # When we try to commit the partition without staging
        # Then we should get a ValueError
        with pytest.raises(ValueError):
            metastore.commit_partition(
                partition=partition,
                catalog=self.catalog,
            )

    def test_stage_partition_none_values(self):
        # Given None partition values
        staged_partition = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )
        # Then the partition should be staged with None values and
        # UNPARTITIONED_SCHEME_ID
        assert staged_partition is not None
        assert staged_partition.partition_values is None
        assert staged_partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID

    def test_stage_partition_empty_values(self):
        # Given empty partition values list
        staged_partition = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=[],
            catalog=self.catalog,
        )
        # Then the partition should be staged with empty values and
        # UNPARTITIONED_SCHEME_ID
        assert staged_partition is not None
        assert staged_partition.partition_values == []
        assert staged_partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID

    def test_stage_partition_type_validation(self):
        """Test that partition values must match the schema types of partition keys."""
        # Given valid partition values matching schema types (col1: int32, col2: string)
        valid_values = [42, "test"]

        # When staging with valid values
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=valid_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # Then the partition should be staged successfully
        assert staged_partition is not None
        assert staged_partition.partition_values == valid_values

        # When trying to stage with invalid type for col1 (string instead of int32)
        with pytest.raises(
            ValueError, match="incompatible with partition transform return type int32"
        ):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=["not_an_int", "test"],
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

        # When trying to stage with invalid type for col2 (int instead of string)
        with pytest.raises(
            ValueError, match="incompatible with partition transform return type string"
        ):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=[42, 123],
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

    def test_stage_partition_wrong_number_of_values(self):
        """Test that the number of partition values must match the number of partition keys."""
        # When trying to stage with too few values
        with pytest.raises(ValueError, match="does not match number of partition keys"):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=[42],  # Missing second value
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

        # When trying to stage with too many values
        with pytest.raises(ValueError, match="does not match number of partition keys"):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=[42, "test", "extra"],
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

    def test_stage_partition_null_values(self):
        """Test that None values are allowed for partition key fields."""
        # When staging with None values
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=[None, "test"],
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # Then the partition should be staged successfully
        assert staged_partition is not None
        assert staged_partition.partition_values == [None, "test"]

        # Also verify None values are allowed for string fields
        staged_partition = metastore.stage_partition(
            stream=self.stream,
            partition_values=[42, None],
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # Then the partition should be staged successfully
        assert staged_partition is not None
        assert staged_partition.partition_values == [42, None]

    def test_stage_partition_multiple_transform_types(self):
        """Test partition scheme with multiple transform types and their type validation."""
        # Create a schema with fields for different transform types
        schema = Schema.of(
            pa.schema(
                [
                    ("user_id", pa.string()),
                    ("product_id", pa.int32()),
                    ("timestamp", pa.timestamp("ns")),
                    ("amount", pa.decimal128(38, 2)),
                    ("category", pa.string()),
                ]
            )
        )

        # Create partition keys with different transform types
        partition_keys = [
            # Multi-field bucket transform on user_id and product_id
            PartitionKey.of(
                key=["user_id", "product_id"],
                transform=BucketTransform.of(
                    BucketTransformParameters.of(
                        num_buckets=100,
                        bucketing_strategy=BucketingStrategy.DEFAULT,
                    )
                ),
            ),
            # Year transform on timestamp
            PartitionKey.of(
                key=["timestamp"],
                transform=YearTransform.of(),
            ),
            # Month transform on timestamp
            PartitionKey.of(
                key=["timestamp"],
                transform=MonthTransform.of(),
            ),
            # Day transform on timestamp
            PartitionKey.of(
                key=["timestamp"],
                transform=DayTransform.of(),
            ),
            # Hour transform on timestamp
            PartitionKey.of(
                key=["timestamp"],
                transform=HourTransform.of(),
            ),
            # Truncate transform on category
            PartitionKey.of(
                key=["category"],
                transform=TruncateTransform.of(TruncateTransformParameters.of(width=3)),
            ),
            # Void transform (always returns None)
            PartitionKey.of(
                key=["amount"],
                transform=VoidTransform.of(),
            ),
        ]

        partition_scheme = PartitionScheme.of(
            keys=PartitionKeyList.of(partition_keys),
            name="multi_transform_scheme",
            scheme_id="multi_transform_scheme_id",
        )

        # Create a new table version with this schema and partition scheme
        _, tv, stream = metastore.create_table_version(
            namespace=self.namespace.namespace,
            table_name="multi_transform_table",
            table_version="v.1",
            schema=schema,
            partition_scheme=partition_scheme,
            catalog=self.catalog,
        )

        # Test valid partition values
        valid_values = [
            5,  # Bucket transform result for user_id + product_id
            2024,  # Year
            3,  # Month
            14,  # Day
            15,  # Hour
            "foo",  # Truncated category
            None,  # Void transform result
        ]

        # When staging with valid values
        staged_partition = metastore.stage_partition(
            stream=stream,
            partition_values=valid_values,
            partition_scheme_id=partition_scheme.id,
            catalog=self.catalog,
        )

        # Then the partition should be staged successfully
        assert staged_partition is not None
        assert staged_partition.partition_values == valid_values

        # Test invalid bucket transform value type
        invalid_bucket_values = [
            "not_an_int",  # Should be decimal128(38, 0) from bucket transform
            2024,
            3,
            14,
            15,
            "foo",
            None,
        ]
        with pytest.raises(
            ValueError,
            match="incompatible with partition transform return type decimal128\\(38, 0\\)",
        ):
            metastore.stage_partition(
                stream=stream,
                partition_values=invalid_bucket_values,
                partition_scheme_id=partition_scheme.id,
                catalog=self.catalog,
            )

        # Test invalid year transform value type
        invalid_year_values = [
            5,  # Valid bucket transform value
            "not_an_int",  # Should be int64 from year transform
            3,
            14,
            15,
            "foo",
            None,
        ]
        with pytest.raises(
            ValueError, match="incompatible with partition transform return type int64"
        ):
            metastore.stage_partition(
                stream=stream,
                partition_values=invalid_year_values,
                partition_scheme_id=partition_scheme.id,
                catalog=self.catalog,
            )

        # Test invalid month transform value type
        invalid_month_values = [
            5,
            2024,
            "not_an_int",  # Should be int64 from month transform
            14,
            15,
            "foo",
            None,
        ]
        with pytest.raises(
            ValueError, match="incompatible with partition transform return type int64"
        ):
            metastore.stage_partition(
                stream=stream,
                partition_values=invalid_month_values,
                partition_scheme_id=partition_scheme.id,
                catalog=self.catalog,
            )
