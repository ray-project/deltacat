import shutil
import tempfile
import uuid

import pytest
import copy
import pyarrow as pa
import pandas as pd
import polars as pl
import numpy as np
import ray
import ray.data

from deltacat import PartitionKey, PartitionScheme
from deltacat.exceptions import (
    SchemaValidationError,
    TableNotFoundError,
    UnclassifiedDeltaCatError,
    NamespaceNotFoundError,
    NamespaceAlreadyExistsError,
    TableAlreadyExistsError,
    TableVersionNotFoundError,
    TableVersionAlreadyExistsError,
    TableValidationError,
    StreamNotFoundError,
    PartitionNotFoundError,
)
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
    DeltaType,
)
from deltacat.types.media import (
    ContentType,
    ContentEncoding,
    DatasetType,
    StorageType,
    DistributedDatasetType,
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
from deltacat.storage.model.manifest import ManifestAuthor
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
    create_test_table,
    create_test_table_version,
)
from deltacat.catalog import CatalogProperties

from deltacat.storage.main.impl import DEFAULT_TABLE_VERSION

# Add imports for type checking near the top of the file (after the existing imports)
from ray.data.dataset import Dataset as RayDataset
from daft import DataFrame as DaftDataFrame


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
        assert (
            len(namespaces_by_name.items()) == 3
        )  # namespace1, namespace2, and auto-created default
        assert namespaces_by_name["namespace1"].equivalent_to(self.namespace1)
        assert namespaces_by_name["namespace2"].equivalent_to(self.namespace2)
        assert (
            "default" in namespaces_by_name
        )  # auto-created default namespace should exist

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
        assert len(namespaces) == 2  # namespace2 and auto-created default should remain
        namespaces_by_name = {n.locator.namespace: n for n in namespaces}
        assert "namespace2" in namespaces_by_name
        assert "default" in namespaces_by_name
        assert namespaces_by_name["namespace2"].equivalent_to(self.namespace2)

    def test_delete_namespace_not_exists(self):
        # When we try to delete a non-existent namespace
        # Then we should get a NamespaceNotFoundError
        with pytest.raises(NamespaceNotFoundError):
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
        # Then we should get a ValueError due to purge not being supported
        with pytest.raises(NotImplementedError):
            metastore.delete_namespace(
                namespace="namespace1",
                purge=True,
                catalog=self.catalog,
            )

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
        with pytest.raises(NamespaceNotFoundError):
            metastore.create_table_version(
                namespace=namespace,
                table_name="test_table",
                table_version="v.1",
                catalog=self.catalog,
            )

        # And trying to list tables in the deleted namespace should fail
        with pytest.raises(NamespaceNotFoundError):
            metastore.list_tables(
                namespace=namespace,
                catalog=self.catalog,
            )

        # And trying to delete the namespace again should fail
        with pytest.raises(NamespaceNotFoundError):
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
            len(namespaces) == 3
        )  # namespace2, recreated namespace1, and auto-created default should exist
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
        with pytest.raises(NamespaceNotFoundError):
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
        with pytest.raises(NamespaceAlreadyExistsError):
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
            table_name=self.test_table1.table_name,
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
        # Then we should get a NotImplementedError due to purge not being supported
        with pytest.raises(NotImplementedError):
            metastore.delete_table(
                namespace=self.test_namespace.namespace,
                table_name=self.test_table1.table_name,
                purge=True,
                catalog=self.catalog,
            )

    def test_delete_table_not_exists(self):
        # When we try to delete a non-existent table
        # Then we should get a TableNotFoundError
        with pytest.raises(TableNotFoundError):
            metastore.delete_table(
                namespace=self.test_namespace.namespace,
                table_name="non_existent_table",
                catalog=self.catalog,
            )

    def test_delete_table_bad_namespace(self):
        # When we try to delete a table with a non-existent namespace
        # Then we should get a TableNotFoundError
        with pytest.raises(TableNotFoundError):
            metastore.delete_table(
                namespace="non_existent_namespace",
                table_name=self.test_table1.table_name,
                catalog=self.catalog,
            )

    def test_delete_table_operations_after_delete(self):
        # Given a table that we delete
        table_name = self.test_table1.table_name
        table_version = self.tv1.table_version
        metastore.delete_table(
            namespace=self.test_namespace.namespace,
            table_name=table_name,
            catalog=self.catalog,
        )

        # When we update the table version's description
        # This should fail with ValueError since the table no longer exists
        with pytest.raises(TableVersionNotFoundError):
            metastore.update_table_version(
                namespace=self.test_namespace.namespace,
                table_name=table_name,
                table_version=table_version,
                description="new description",
                catalog=self.catalog,
            )

        # When we create a stream under the deleted table version
        # This should fail with ValueError since the table no longer exists
        with pytest.raises(TableVersionNotFoundError):
            metastore.stage_stream(
                namespace=self.test_namespace.namespace,
                table_name=table_name,
                table_version=table_version,
                catalog=self.catalog,
            )

        # When we create a stream under the deleted table, but omit the table
        # version to force resolution of the latest active table version.
        # This should fail with ValueError since the table no longer exists.
        with pytest.raises(TableNotFoundError):
            metastore.stage_stream(
                namespace=self.test_namespace.namespace,
                table_name=table_name,
                catalog=self.catalog,
            )

        # When we stage a partition to the deleted table version's stream
        # This should fail with ValueError since the table no longer exists
        with pytest.raises(TableVersionNotFoundError):
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
        with pytest.raises(TableAlreadyExistsError):
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
        with pytest.raises(TableVersionAlreadyExistsError):
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
            with pytest.raises(TableNotFoundError):
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
            with pytest.raises(TableNotFoundError):
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
        with pytest.raises(TableValidationError):
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
        with pytest.raises(TableValidationError):
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
        with pytest.raises(TableValidationError):
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
            with pytest.raises(TableNotFoundError):
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
        with pytest.raises(TableVersionAlreadyExistsError):
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
            SchemaValidationError,
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
            SchemaValidationError,
            match="Sort key field 'non_existent_field' not found in schema",
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
            SchemaValidationError,
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
            SchemaValidationError,
            match="Sort key field 'non_existent_field' not found in schema",
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
            kwargs_copy[key] = "i_dont_exist.1"
            with pytest.raises(TableVersionNotFoundError):
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
        with pytest.raises(StreamNotFoundError):
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
            kwargs_copy[key] = "i_dont_exist.1"
            with pytest.raises(StreamNotFoundError):
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
        with pytest.raises(PartitionNotFoundError):
            metastore.delete_partition(
                stream_locator=self.stream.locator,
                partition_values=partition_values,
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

    def test_delete_partition_staged(self):
        # Given a staged partition
        partition_values = [123, "abc"]
        metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )

        # When we try to delete the staged partition
        # Then we should get a ValueError since only committed partitions
        # can be deleted
        with pytest.raises(PartitionNotFoundError):
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
        with pytest.raises(TableValidationError):
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
            content_types=None,
            state=CommitState.STAGED,
            partition_scheme_id=self.tv.partition_scheme.id,
        )
        # When we try to commit the partition without staging
        # Then we should get a ValueError
        with pytest.raises(PartitionNotFoundError):
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
        assert staged_partition.partition_values is None
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
            TableValidationError,
            match="incompatible with partition transform return type int32",
        ):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=["not_an_int", "test"],
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

        # When trying to stage with invalid type for col2 (int instead of string)
        with pytest.raises(
            TableValidationError,
            match="incompatible with partition transform return type string",
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
        with pytest.raises(
            TableValidationError, match="does not match number of partition keys"
        ):
            metastore.stage_partition(
                stream=self.stream,
                partition_values=[42],  # Missing second value
                partition_scheme_id=self.tv.partition_scheme.id,
                catalog=self.catalog,
            )

        # When trying to stage with too many values
        with pytest.raises(
            TableValidationError, match="does not match number of partition keys"
        ):
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
            "not_an_int",  # Should be int32() from bucket transform
            2024,
            3,
            14,
            15,
            "foo",
            None,
        ]
        with pytest.raises(
            TableValidationError,
            match="incompatible with partition transform return type int32()",
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
            "not_an_int",  # Should be int32 from year transform
            3,
            14,
            15,
            "foo",
            None,
        ]
        with pytest.raises(
            TableValidationError,
            match="incompatible with partition transform return type int32",
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
            "not_an_int",  # Should be int32 from month transform
            14,
            15,
            "foo",
            None,
        ]
        with pytest.raises(
            TableValidationError,
            match="incompatible with partition transform return type int32",
        ):
            metastore.stage_partition(
                stream=stream,
                partition_values=invalid_month_values,
                partition_scheme_id=partition_scheme.id,
                catalog=self.catalog,
            )

    def test_get_partition_unpartitioned_stream_none_values(self):
        """Test retrieving a staged and committed partition with partition_values=None for an unpartitioned stream."""
        # Given a staged partition with None partition values on an unpartitioned stream
        staged_partition = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )

        # Verify the partition was staged correctly with None values and UNPARTITIONED_SCHEME_ID
        assert staged_partition is not None
        assert staged_partition.partition_values is None
        assert staged_partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID

        # When we commit the partition
        committed_partition = metastore.commit_partition(
            partition=staged_partition,
            catalog=self.catalog,
        )

        # Then the partition should be committed correctly
        assert committed_partition is not None
        assert committed_partition.state == CommitState.COMMITTED
        assert committed_partition.partition_values is None
        assert committed_partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID
        assert committed_partition.previous_partition_id is None
        assert committed_partition.locator is not None
        assert (
            committed_partition.locator.stream_locator
            == self.unpartitioned_stream.locator
        )

        # When we retrieve the partition using get_partition with partition_values=None and
        # partition_scheme_id=UNPARTITIONED_SCHEME_ID
        retrieved_partition = metastore.get_partition(
            stream_locator=self.unpartitioned_stream.locator,
            partition_values=None,
            # partition_scheme_id=UNPARTITIONED_SCHEME_ID,
            catalog=self.catalog,
        )

        # Then we should get the correct committed partition
        assert retrieved_partition is not None
        assert retrieved_partition.equivalent_to(committed_partition)
        assert retrieved_partition.partition_id == committed_partition.partition_id
        assert retrieved_partition.state == CommitState.COMMITTED
        assert retrieved_partition.partition_values is None
        assert retrieved_partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID
        assert (
            retrieved_partition.locator.stream_locator
            == self.unpartitioned_stream.locator
        )

        # When we retrieve the partition using get_partition with partition_values=None and
        # partition_scheme_id=UNPARTITIONED_SCHEME_ID
        retrieved_partition = metastore.get_partition(
            stream_locator=self.unpartitioned_stream.locator,
            partition_values=None,
            partition_scheme_id=UNPARTITIONED_SCHEME_ID,
            catalog=self.catalog,
        )

        # Should get the same partition
        assert retrieved_partition is not None
        assert retrieved_partition.equivalent_to(committed_partition)
        assert retrieved_partition.partition_values is None
        assert retrieved_partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID

        # Verify we can also retrieve it by partition ID
        retrieved_by_id = metastore.get_partition_by_id(
            stream_locator=self.unpartitioned_stream.locator,
            partition_id=committed_partition.partition_id,
            catalog=self.catalog,
        )

        # Should get the same partition
        assert retrieved_by_id is not None
        assert retrieved_by_id.equivalent_to(committed_partition)
        assert retrieved_by_id.partition_values is None
        assert retrieved_by_id.partition_scheme_id == UNPARTITIONED_SCHEME_ID

    def test_list_stream_partitions_unpartitioned(self):
        """Test listing partitions for an unpartitioned stream with None partition values."""
        # Given a committed partition with None partition values on an unpartitioned stream
        partition_1 = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )
        committed_partition_1 = metastore.commit_partition(
            partition=partition_1,
            catalog=self.catalog,
        )

        # When we stage and commit another partition with None values, it should replace the first one
        partition_2 = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )
        committed_partition_2 = metastore.commit_partition(
            partition=partition_2,
            catalog=self.catalog,
        )

        # Verify the second partition replaced the first (same partition values = replacement)
        assert committed_partition_2.previous_partition_id == committed_partition_1.id

        # When we list the partitions for the unpartitioned stream
        list_result = metastore.list_stream_partitions(
            stream=self.unpartitioned_stream,
            catalog=self.catalog,
        )

        # Then we should get only the latest partition (the replacement)
        partitions_list = list_result.all_items()
        assert len(partitions_list) == 1

        # Verify the returned partition has None values and correct scheme ID
        partition = partitions_list[0]
        assert partition.state == CommitState.COMMITTED
        assert partition.partition_values is None
        assert partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID
        assert partition.locator.stream_locator == self.unpartitioned_stream.locator

        # Verify we got the latest committed partition (the replacement)
        assert partition.partition_id == committed_partition_2.partition_id

    def test_list_stream_partitions_partitioned_and_unpartitioned(self):
        """Test listing partitions for both partitioned and unpartitioned streams to compare behavior."""
        # Given committed partitions on a partitioned stream with actual values
        partition_values1 = [123, "abc"]
        partition_values2 = [456, "def"]

        staged_partition1 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values1,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=staged_partition1,
            catalog=self.catalog,
        )

        staged_partition2 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values2,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=staged_partition2,
            catalog=self.catalog,
        )

        # And a committed partition on an unpartitioned stream with None values
        unpartitioned_partition1 = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=unpartitioned_partition1,
            catalog=self.catalog,
        )

        # When we list partitions for the partitioned stream
        partitioned_list_result = metastore.list_stream_partitions(
            stream=self.stream,
            catalog=self.catalog,
        )
        partitioned_partitions = partitioned_list_result.all_items()

        # Then we should get the partitioned stream's partitions with actual values
        assert len(partitioned_partitions) == 2

        partition_values_list = [p.partition_values for p in partitioned_partitions]
        assert partition_values1 in partition_values_list
        assert partition_values2 in partition_values_list

        for p in partitioned_partitions:
            assert p.state == CommitState.COMMITTED
            assert p.partition_scheme_id == self.tv.partition_scheme.id
            assert p.partition_values is not None
            assert p.locator.stream_locator == self.stream.locator

        # When we list partitions for the unpartitioned stream
        unpartitioned_list_result = metastore.list_stream_partitions(
            stream=self.unpartitioned_stream,
            catalog=self.catalog,
        )
        unpartitioned_partitions = unpartitioned_list_result.all_items()

        # Then we should get the unpartitioned stream's partition with None values
        assert len(unpartitioned_partitions) == 1

        partition = unpartitioned_partitions[0]
        assert partition.state == CommitState.COMMITTED
        assert partition.partition_values is None
        assert partition.partition_scheme_id == UNPARTITIONED_SCHEME_ID
        assert partition.locator.stream_locator == self.unpartitioned_stream.locator

        # Verify partition IDs are distinct between streams
        partitioned_ids = {p.partition_id for p in partitioned_partitions}
        unpartitioned_ids = {p.partition_id for p in unpartitioned_partitions}
        assert len(partitioned_ids.intersection(unpartitioned_ids)) == 0  # No overlap

    def test_list_stream_partitions_empty_values_vs_none_values(self):
        """Test listing partitions with empty list values vs None values for unpartitioned streams."""
        # Given a partition with empty list partition values
        empty_list_partition = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=[],
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=empty_list_partition,
            catalog=self.catalog,
        )

        # And a partition with None partition values
        none_values_partition = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=none_values_partition,
            catalog=self.catalog,
        )

        # When we list the partitions
        list_result = metastore.list_stream_partitions(
            stream=self.unpartitioned_stream,
            catalog=self.catalog,
        )
        partitions_list = list_result.all_items()

        # Then we should get only one committed unpartitioned partition
        assert len(partitions_list) == 1

        # Verify the committed unpartitioned partition is committed and has the right scheme ID
        assert partitions_list[0].state == CommitState.COMMITTED
        assert partitions_list[0].partition_scheme_id == UNPARTITIONED_SCHEME_ID
        assert (
            partitions_list[0].locator.stream_locator
            == self.unpartitioned_stream.locator
        )

        # Verify we have one with empty list and one with None
        assert partitions_list[0].partition_values is None

        # Verify we can retrieve the partition either by empty list or none partition values
        retrieved_empty_list = metastore.get_partition(
            stream_locator=self.unpartitioned_stream.locator,
            partition_values=[],
            partition_scheme_id=UNPARTITIONED_SCHEME_ID,
            catalog=self.catalog,
        )
        assert retrieved_empty_list is not None
        assert retrieved_empty_list.partition_values is None

        retrieved_none_values = metastore.get_partition(
            stream_locator=self.unpartitioned_stream.locator,
            partition_values=None,
            partition_scheme_id=UNPARTITIONED_SCHEME_ID,
            catalog=self.catalog,
        )
        assert retrieved_none_values is not None
        assert retrieved_none_values.partition_values is None

    def test_list_partitions_basic_functionality(self):
        """Test basic list_partitions functionality with committed partitions."""
        # Given two committed partitions in a partitioned table
        partition_values1 = [123, "abc"]
        partition_values2 = [456, "def"]

        staged_partition1 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values1,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=staged_partition1,
            catalog=self.catalog,
        )

        staged_partition2 = metastore.stage_partition(
            stream=self.stream,
            partition_values=partition_values2,
            partition_scheme_id=self.tv.partition_scheme.id,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=staged_partition2,
            catalog=self.catalog,
        )

        # When we list partitions using list_partitions
        list_result = metastore.list_partitions(
            namespace="test_partition_ns",
            table_name="mypartitiontable",
            table_version="v.1",
            catalog=self.catalog,
        )

        # Then we should get both partitions
        partitions_list = list_result.all_items()
        assert len(partitions_list) >= 2

        # Verify partition values are correct
        partition_values = [p.partition_values for p in partitions_list]
        assert partition_values1 in partition_values
        assert partition_values2 in partition_values

        # Verify all returned partitions are committed and have correct properties
        for p in partitions_list:
            assert p.state == CommitState.COMMITTED
            assert p.partition_scheme_id == self.tv.partition_scheme.id
            assert p.locator.stream_locator == self.stream.locator

    def test_list_partitions_without_table_version(self):
        """Test list_partitions when table_version is not specified (should resolve to latest active).

        This test verifies that list_partitions correctly resolves the latest active table version
        when table_version=None, and creates proper partition locators using the resolved stream's
        locator rather than attempting to construct locators with None table_version values.
        """
        # First make the table version active so it can be resolved automatically
        metastore.update_table_version(
            namespace="test_partition_ns",
            table_name="mypartitiontable",
            table_version="v.1",
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )

        # Given a committed partition
        partition_values = [789, "ghi"]

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

        # When we list partitions without specifying table_version (should default to latest active)
        list_result = metastore.list_partitions(
            namespace="test_partition_ns",
            table_name="mypartitiontable",
            catalog=self.catalog,
        )

        # Then we should get the partition
        partitions_list = list_result.all_items()
        assert len(partitions_list) >= 1

        # Find our partition in the list
        found_partition = None
        for p in partitions_list:
            if p.partition_values == partition_values:
                found_partition = p
                break

        assert found_partition is not None
        assert found_partition.state == CommitState.COMMITTED
        assert found_partition.partition_scheme_id == self.tv.partition_scheme.id

    def test_list_partitions_unpartitioned_table(self):
        """Test list_partitions with an unpartitioned table."""
        # Given a committed partition on an unpartitioned table
        unpartitioned_partition = metastore.stage_partition(
            stream=self.unpartitioned_stream,
            partition_values=None,
            catalog=self.catalog,
        )
        metastore.commit_partition(
            partition=unpartitioned_partition,
            catalog=self.catalog,
        )

        # When we list partitions for the unpartitioned table
        list_result = metastore.list_partitions(
            namespace="test_partition_ns",
            table_name="myunpartitionedtable",
            table_version="v.1",
            catalog=self.catalog,
        )

        # Then we should get the unpartitioned partition
        partitions_list = list_result.all_items()
        assert len(partitions_list) >= 1

        # Find the unpartitioned partition
        unpartitioned_found = None
        for p in partitions_list:
            if p.partition_values is None:
                unpartitioned_found = p
                break

        assert unpartitioned_found is not None
        assert unpartitioned_found.state == CommitState.COMMITTED
        assert unpartitioned_found.partition_scheme_id == UNPARTITIONED_SCHEME_ID
        assert (
            unpartitioned_found.locator.stream_locator
            == self.unpartitioned_stream.locator
        )

    def test_list_partitions_error_invalid_namespace(self):
        """Test list_partitions error handling with invalid namespace."""
        # When we try to list partitions with an invalid namespace
        # Then we should get a ValueError
        with pytest.raises(StreamNotFoundError, match="Default stream.*not found"):
            metastore.list_partitions(
                namespace="non_existent_namespace",
                table_name="mypartitiontable",
                table_version="v.1",
                catalog=self.catalog,
            )

    def test_list_partitions_error_invalid_table_name(self):
        """Test list_partitions error handling with invalid table name."""
        # When we try to list partitions with an invalid table name
        # Then we should get a ValueError
        with pytest.raises(StreamNotFoundError, match="Default stream.*not found"):
            metastore.list_partitions(
                namespace="test_partition_ns",
                table_name="non_existent_table",
                table_version="v.1",
                catalog=self.catalog,
            )

    def test_list_partitions_error_invalid_table_version(self):
        """Test list_partitions error handling with invalid table version."""
        # When we try to list partitions with an invalid table version
        # Then we should get a ValueError
        with pytest.raises(StreamNotFoundError, match="Default stream.*not found"):
            metastore.list_partitions(
                namespace="test_partition_ns",
                table_name="mypartitiontable",
                table_version="v.99",
                catalog=self.catalog,
            )

    def test_list_partitions_error_empty_namespace(self):
        """Test list_partitions error handling with empty namespace."""
        # When we try to list partitions with an empty namespace
        # Then we should get a ValueError
        with pytest.raises(ValueError, match="Namespace cannot be empty"):
            metastore.list_partitions(
                namespace="",
                table_name="mypartitiontable",
                table_version="v.1",
                catalog=self.catalog,
            )

    def test_list_partitions_error_empty_table_name(self):
        """Test list_partitions error handling with empty table name."""
        # When we try to list partitions with an empty table name
        # Then we should get a ValueError
        with pytest.raises(ValueError, match="Table name cannot be empty"):
            metastore.list_partitions(
                namespace="test_partition_ns",
                table_name="",
                table_version="v.1",
                catalog=self.catalog,
            )

    def test_list_partitions_consistency_with_list_stream_partitions(self):
        """Test that list_partitions and list_stream_partitions return consistent results."""
        # Given committed partitions
        partition_values1 = [333, "consistency"]
        partition_values2 = [444, "test"]

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

        # When we list partitions using list_partitions
        list_partitions_result = metastore.list_partitions(
            namespace="test_partition_ns",
            table_name="mypartitiontable",
            table_version="v.1",
            catalog=self.catalog,
        )

        # And when we list partitions using list_stream_partitions
        list_stream_partitions_result = metastore.list_stream_partitions(
            stream=self.stream,
            catalog=self.catalog,
        )

        # Then both should return the same partitions
        partitions_by_list_partitions = list_partitions_result.all_items()
        partitions_by_list_stream_partitions = list_stream_partitions_result.all_items()

        # Convert to sets for comparison (ignoring order)
        def partition_to_key(p):
            return (
                tuple(p.partition_values) if p.partition_values else None,
                p.partition_id,
            )

        list_partitions_keys = {
            partition_to_key(p) for p in partitions_by_list_partitions
        }
        list_stream_partitions_keys = {
            partition_to_key(p) for p in partitions_by_list_stream_partitions
        }

        assert list_partitions_keys == list_stream_partitions_keys

        # Both should contain our test partitions
        expected_values = {
            (tuple(partition_values1), committed_partition1.partition_id),
            (tuple(partition_values2), committed_partition2.partition_id),
        }
        assert expected_values.issubset(list_partitions_keys)
        assert expected_values.issubset(list_stream_partitions_keys)


class TestDelta:
    @classmethod
    def setup_method(cls):

        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(root=cls.tmpdir)

        # Create and commit namespace
        cls.namespace = create_test_namespace()
        metastore.create_namespace(
            namespace=cls.namespace.locator.namespace,
            catalog=cls.catalog,
        )

        # Create a schema for the table version
        arrow_schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("age", pa.int64()),
                ("city", pa.string()),
            ]
        )
        schema = Schema.of(
            schema=arrow_schema,
            schema_id=1,
        )

        # Create and commit table version with schema
        cls.table, cls.table_version, cls.stream = metastore.create_table_version(
            namespace=cls.namespace.locator.namespace,
            table_name="test_table",
            table_version="v.1",
            schema=schema,
            catalog=cls.catalog,
        )

        # Make the table version active
        metastore.update_table_version(
            namespace=cls.namespace.locator.namespace,
            table_name="test_table",
            table_version="v.1",
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=cls.catalog,
        )

        # Stage and commit partition
        cls.partition = metastore.stage_partition(
            stream=cls.stream,
            catalog=cls.catalog,
        )
        cls.partition = metastore.commit_partition(
            partition=cls.partition,
            catalog=cls.catalog,
        )
        # Get the committed partition to ensure we have the latest state
        cls.partition = metastore.get_partition_by_id(
            stream_locator=cls.partition.stream_locator,
            partition_id=cls.partition.partition_id,
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_stage_delta_with_polars(self):
        # Create a sample Polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the Polars DataFrame
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.PARQUET.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_polars_csv(self):
        # Create a sample Polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the Polars DataFrame as CSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.CSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.CSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_polars_json(self):
        # Create a sample Polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the Polars DataFrame as JSON
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.JSON,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.JSON.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_polars_avro(self):
        # Create a sample Polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the Polars DataFrame as AVRO
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.AVRO,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.AVRO.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_pandas(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.PARQUET.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_pandas_csv(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as CSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.CSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.CSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_pandas_json(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as JSON
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.JSON,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.JSON.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_pandas_avro(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as AVRO
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.AVRO,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.AVRO.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_pandas_feather(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as Feather
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.FEATHER,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.FEATHER.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pandas_tsv(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as TSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pandas_psv(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as PSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.PSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pandas_unescaped_tsv(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as UNESCAPED_TSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.UNESCAPED_TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.UNESCAPED_TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_polars_tsv(self):
        # Create a sample polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the polars DataFrame as TSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_polars_psv(self):
        # Create a sample polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the polars DataFrame as PSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.PSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_polars_unescaped_tsv(self):
        # Create a sample polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the polars DataFrame as UNESCAPED_TSV
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.UNESCAPED_TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.UNESCAPED_TSV.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_tsv(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as TSV
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_psv(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as PSV
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.PSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_unescaped_tsv(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as UNESCAPED_TSV
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.UNESCAPED_TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.UNESCAPED_TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_csv(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as CSV
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.CSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.CSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_orc(self):
        # Create a sample PyArrow Table

        table = pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as ORC
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            content_type=ContentType.ORC,
        )

        # Verify the delta was staged correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.ORC.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_pyarrow_parquet(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as Parquet
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.PARQUET.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_feather(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as Feather
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.FEATHER,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.FEATHER.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value
        assert entry.meta.record_count == 3

    def test_stage_delta_with_pyarrow_json(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as JSON
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.JSON,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.JSON.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_pyarrow_avro(self):
        # Create a sample PyArrow Table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the PyArrow Table as AVRO
        delta = metastore.stage_delta(
            data=table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.AVRO,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.AVRO.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_pandas_orc(self):
        # Create a sample pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame as ORC
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.ORC,
        )

        # Verify the delta was staged correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1

        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.ORC.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_polars_orc(self):
        # Create a sample polars DataFrame

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the polars DataFrame as ORC
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            content_type=ContentType.ORC,
        )

        # Verify the delta was staged correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.ORC.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_stage_delta_with_ray_dataset_parquet(self):
        """Test staging a delta using a Ray Dataset with Parquet format."""
        # Create a sample Ray Dataset

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        ds = ray.data.from_pandas(df)

        # Stage the delta using the Ray Dataset as Parquet
        delta = metastore.stage_delta(
            data=ds,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Verify the delta was staged correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.PARQUET
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY
        assert entry.meta.record_count == 3

    def test_stage_delta_with_ray_dataset_csv(self):
        # Create a sample Ray Dataset

        data = [{"col1": "a", "col2": i} for i in range(5)]
        dataset = ray.data.from_items(data)

        # Stage the delta
        delta = metastore.stage_delta(
            partition=self.partition,
            data=dataset,
            catalog=self.catalog,
            content_type=ContentType.CSV,
        )

        # Verify the delta was staged
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator
        assert len(delta.manifest.entries) > 0
        entry = delta.manifest.entries[0]
        assert entry.meta.content_type == ContentType.CSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_ray_dataset_tsv(self):
        # Create a sample Ray Dataset

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        dataset = ray.data.from_pandas(df)

        # Stage the delta using the Ray Dataset as TSV
        delta = metastore.stage_delta(
            data=dataset,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_ray_dataset_psv(self):
        # Create a sample Ray Dataset

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        dataset = ray.data.from_pandas(df)

        # Stage the delta using the Ray Dataset as PSV
        delta = metastore.stage_delta(
            data=dataset,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.PSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_ray_dataset_unescaped_tsv(self):
        # Create a sample Ray Dataset

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        dataset = ray.data.from_pandas(df)

        # Stage the delta using the Ray Dataset as UNESCAPED_TSV
        delta = metastore.stage_delta(
            data=dataset,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.UNESCAPED_TSV,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) == 1
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.UNESCAPED_TSV.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    def test_stage_delta_with_ray_dataset_unescaped_tsv_fails_with_delimiters(self):
        # Create a sample Ray Dataset with data that would need escaping

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a\tb,c", "d\te,f", "g\th,i"],
                "age": [25, 30, 35],
            }
        )
        dataset = ray.data.from_pandas(df)

        # Attempt to stage the delta with data containing delimiters
        with pytest.raises(Exception) as exc_info:
            metastore.stage_delta(
                data=dataset,
                partition=self.partition,
                catalog=self.catalog,
                content_type=ContentType.UNESCAPED_TSV,
            )
        assert (
            "CSV values may not contain structural characters if quoting style is"
            in str(exc_info.value)
        )

    def test_stage_delta_with_ray_dataset_json(self):
        # Create a sample Ray Dataset

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )
        dataset = ray.data.from_pandas(df)

        # Stage the delta using the Ray Dataset as JSON
        delta = metastore.stage_delta(
            data=dataset,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.JSON,
        )

        # Verify the delta was created correctly
        assert delta is not None
        assert delta.manifest is not None
        assert len(delta.manifest.entries) > 0
        assert delta.locator.stream_locator == self.stream.locator
        assert delta.locator.partition_locator == self.partition.locator

        # Verify the manifest entry metadata
        entry = delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.JSON.value
        assert entry.meta.content_encoding == ContentEncoding.GZIP.value

    # ========== COMMIT DELTA TESTS ==========

    def test_commit_delta_basic_upsert(self):
        # Given a staged delta with pandas DataFrame

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage the delta using the pandas DataFrame
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        # Verify the delta is staged initially
        assert staged_delta is not None
        assert staged_delta.type == DeltaType.UPSERT
        assert staged_delta.manifest is not None
        assert len(staged_delta.manifest.entries) > 0

        # Commit the staged delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.type == DeltaType.UPSERT
        assert committed_delta.manifest is not None
        assert len(committed_delta.manifest.entries) > 0
        assert committed_delta.locator.stream_locator == self.stream.locator
        assert committed_delta.locator.partition_locator == self.partition.locator

        # Verify the committed delta has stream position assigned
        assert committed_delta.locator.stream_position is not None
        assert committed_delta.locator.stream_position >= 1

        # Verify manifest entry metadata
        entry = committed_delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.PARQUET.value
        assert entry.meta.content_encoding == ContentEncoding.IDENTITY.value

    def test_commit_delta_basic_append(self):
        # Given a staged delta with append type

        df = pl.DataFrame(
            {
                "id": [4, 5, 6],
                "name": ["David", "Eva", "Frank"],
                "age": [40, 45, 50],
            }
        )

        # Stage the delta as APPEND type
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.APPEND,
        )

        # Verify staging
        assert staged_delta.type == DeltaType.APPEND

        # Commit the staged delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.type == DeltaType.APPEND
        assert committed_delta.locator.stream_position is not None
        assert committed_delta.locator.stream_position >= 1

        # Verify manifest entry metadata
        entry = committed_delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_commit_delta_basic_delete(self):
        # Given a staged delete delta

        # Create a delete delta with records to delete
        delete_table = pa.Table.from_pylist(
            [
                {"id": 1},  # Delete record with id=1
                {"id": 2},  # Delete record with id=2
            ]
        )

        # Stage the delta as DELETE type
        staged_delta = metastore.stage_delta(
            data=delete_table,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.DELETE,
        )

        # Verify staging
        assert staged_delta.type == DeltaType.DELETE

        # Commit the delete delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.type == DeltaType.DELETE
        assert committed_delta.locator.stream_position is not None
        assert committed_delta.locator.stream_position >= 1

        # Verify manifest entry metadata
        entry = committed_delta.manifest.entries[0]
        assert entry.meta.record_count == 2
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_commit_delta_with_different_content_types(self):
        # Test committing deltas with different content types

        test_data = pd.DataFrame(
            {
                "id": [1, 2],
                "value": ["test1", "test2"],
            }
        )

        content_types_to_test = [
            ContentType.PARQUET,
            ContentType.CSV,
            ContentType.TSV,
            ContentType.JSON,
            ContentType.FEATHER,
            ContentType.AVRO,
        ]

        committed_deltas = []

        for content_type in content_types_to_test:
            # Stage delta with specific content type
            staged_delta = metastore.stage_delta(
                data=test_data,
                partition=self.partition,
                catalog=self.catalog,
                content_type=content_type,
                delta_type=DeltaType.UPSERT,
            )

            # Commit the delta
            committed_delta = metastore.commit_delta(
                delta=staged_delta,
                catalog=self.catalog,
            )

            # Verify the delta was committed correctly
            assert committed_delta is not None
            assert committed_delta.type == DeltaType.UPSERT
            assert committed_delta.locator.stream_position is not None

            # Verify content type in manifest entry
            entry = committed_delta.manifest.entries[0]
            assert entry.meta.content_type == content_type.value
            assert entry.meta.record_count == 2

            committed_deltas.append(committed_delta)

        # Verify all deltas have unique stream positions
        stream_positions = [d.locator.stream_position for d in committed_deltas]
        assert len(set(stream_positions)) == len(
            stream_positions
        ), "Stream positions should be unique"

    def test_commit_delta_sequential_stream_positions(self):
        # Test that sequential delta commits get incrementing stream positions

        base_data = pd.DataFrame(
            {
                "id": [1, 2],
                "value": ["base1", "base2"],
            }
        )

        committed_deltas = []

        # Commit multiple deltas in sequence
        for i in range(5):
            test_data = base_data.copy()
            test_data["batch"] = i

            # Stage delta
            staged_delta = metastore.stage_delta(
                data=test_data,
                partition=self.partition,
                catalog=self.catalog,
                content_type=ContentType.PARQUET,
                delta_type=DeltaType.UPSERT,
            )

            # Commit the delta
            committed_delta = metastore.commit_delta(
                delta=staged_delta,
                catalog=self.catalog,
            )

            # Verify the delta was committed correctly
            assert committed_delta is not None
            assert committed_delta.type == DeltaType.UPSERT
            assert committed_delta.locator.stream_position is not None

            committed_deltas.append(committed_delta)

        # Verify stream positions are increasing
        stream_positions = [d.locator.stream_position for d in committed_deltas]
        for i in range(1, len(stream_positions)):
            assert (
                stream_positions[i] > stream_positions[i - 1]
            ), "Stream positions should be increasing"

        # Verify all positions are unique
        assert len(set(stream_positions)) == len(
            stream_positions
        ), "Stream positions should be unique"

    def test_commit_delta_with_properties(self):
        # Test committing delta with custom properties

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "data": ["a", "b", "c"],
            }
        )

        # Stage delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Define custom properties
        custom_properties = {
            "batch_id": "batch_001",
            "source_system": "test_system",
            "processing_timestamp": "2024-01-01T00:00:00Z",
        }

        # Commit delta with properties
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            properties=custom_properties,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.properties == custom_properties
        assert committed_delta.locator.stream_position is not None

    def test_commit_delta_with_author_metadata(self):
        # Test committing delta with author metadata

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "author_test": ["data1", "data2"],
            }
        )

        # Create manifest author
        author = ManifestAuthor.of(name="test_author", version="1.0")

        # Stage delta with author
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            author=author,
        )

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.manifest is not None
        # Author information should be preserved in the manifest
        assert committed_delta.manifest.author is not None
        assert committed_delta.manifest.author.name == "test_author"
        assert committed_delta.manifest.author.version == "1.0"

    def test_commit_delta_error_handling_invalid_partition(self):
        # Test error handling when committing delta with invalid partition

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "data": ["test1", "test2"],
            }
        )

        # Stage delta normally
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Modify the delta to have an invalid partition ID by updating the locator
        staged_delta.locator.partition_locator.partition_id = "invalid_partition_id"

        # Attempt to commit should raise an error
        with pytest.raises(PartitionNotFoundError) as exc_info:
            metastore.commit_delta(
                delta=staged_delta,
                catalog=self.catalog,
            )

        assert "Partition not found" in str(exc_info.value)

    def test_commit_delta_default_type_assignment(self):
        # Test that delta type defaults to UPSERT when not specified

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "default_type": ["a", "b", "c"],
            }
        )

        # Stage delta without specifying type (should default to UPSERT)
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Verify staging defaults to UPSERT
        assert staged_delta.type == DeltaType.UPSERT

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly with UPSERT type
        assert committed_delta is not None
        assert committed_delta.type == DeltaType.UPSERT
        assert committed_delta.locator.stream_position is not None

    def test_commit_delta_large_dataset(self):
        # Test committing a larger dataset to verify performance and handling

        # Create a larger dataset
        large_data = pd.DataFrame(
            {
                "id": range(1000),
                "name": [f"user_{i}" for i in range(1000)],
                "value": [i * 1.5 for i in range(1000)],
                "category": [f"cat_{i % 10}" for i in range(1000)],
            }
        )

        # Stage the large delta
        staged_delta = metastore.stage_delta(
            data=large_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.APPEND,
        )

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.type == DeltaType.APPEND
        assert committed_delta.locator.stream_position is not None

        # Verify manifest entry metadata for large dataset
        entry = committed_delta.manifest.entries[0]
        assert entry.meta.record_count == 1000
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_commit_delta_with_ray_dataset(self):
        # Test committing delta created from Ray Dataset

        # Create Ray Dataset
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "ray_data": ["a", "b", "c", "d", "e"],
            }
        )
        ray_dataset = ray.data.from_pandas(df)

        # Stage delta with Ray Dataset
        staged_delta = metastore.stage_delta(
            data=ray_dataset,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the delta was committed correctly
        assert committed_delta is not None
        assert committed_delta.type == DeltaType.UPSERT
        assert committed_delta.locator.stream_position is not None

        # Verify manifest entry metadata
        entry = committed_delta.manifest.entries[0]
        assert entry.meta.record_count == 5
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_commit_delta_stream_position_consistency(self):
        # Test that stream positions are managed correctly across multiple commits

        # Get initial partition state
        initial_partition = metastore.get_partition_by_id(
            stream_locator=self.partition.stream_locator,
            partition_id=self.partition.partition_id,
            catalog=self.catalog,
        )
        initial_stream_position = initial_partition.stream_position or 0

        test_data = pd.DataFrame(
            {
                "id": [1, 2],
                "stream_test": ["data1", "data2"],
            }
        )

        # Stage and commit first delta
        staged_delta_1 = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        committed_delta_1 = metastore.commit_delta(
            delta=staged_delta_1,
            catalog=self.catalog,
        )

        # Verify first delta stream position
        assert committed_delta_1.locator.stream_position == initial_stream_position + 1
        assert committed_delta_1.previous_stream_position == initial_stream_position

        # Stage and commit second delta
        staged_delta_2 = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        committed_delta_2 = metastore.commit_delta(
            delta=staged_delta_2,
            catalog=self.catalog,
        )

        # Verify second delta stream position
        assert committed_delta_2.locator.stream_position == initial_stream_position + 2
        assert committed_delta_2.previous_stream_position == initial_stream_position + 1

        # Verify that partition's stream position is updated
        updated_partition = metastore.get_partition_by_id(
            stream_locator=self.partition.stream_locator,
            partition_id=self.partition.partition_id,
            catalog=self.catalog,
        )
        assert updated_partition.stream_position == initial_stream_position + 2

    # ========== GET DELTA TESTS ==========

    def test_get_delta_basic_retrieval(self):
        # Test basic delta retrieval after commit

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
            }
        )

        # Stage and commit delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Retrieve the committed delta
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.namespace,
            table_name=self.table.table_name,
            stream_position=committed_delta.stream_position,
            table_version=self.table_version.table_version,  # Explicitly specify table version
            include_manifest=True,  # Explicitly request manifest
            catalog=self.catalog,
        )

        # Verify the retrieved delta matches the committed one
        assert (
            retrieved_delta is not None
        ), f"Failed to retrieve delta at stream position {committed_delta.locator.stream_position}"
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )
        assert retrieved_delta.type == committed_delta.type
        assert retrieved_delta.type == DeltaType.UPSERT

        # Verify manifest integrity
        assert retrieved_delta.manifest is not None
        assert len(retrieved_delta.manifest.entries) == len(
            committed_delta.manifest.entries
        )

    def test_get_delta_different_types(self):
        # Test retrieving deltas with different delta types

        test_data = pd.DataFrame(
            {
                "id": [1, 2],
                "value": ["test1", "test2"],
            }
        )

        delta_types_to_test = [
            DeltaType.UPSERT,
            DeltaType.APPEND,
            DeltaType.DELETE,
        ]

        for delta_type in delta_types_to_test:
            # Stage and commit delta with specific type
            staged_delta = metastore.stage_delta(
                data=test_data,
                partition=self.partition,
                catalog=self.catalog,
                content_type=ContentType.PARQUET,
                delta_type=delta_type,
            )

            committed_delta = metastore.commit_delta(
                delta=staged_delta,
                catalog=self.catalog,
            )

            # Retrieve the committed delta
            retrieved_delta = metastore.get_delta(
                namespace=self.namespace.locator.namespace,
                table_name="test_table",
                stream_position=committed_delta.locator.stream_position,
                partition_values=None,  # Using None for unpartitioned table
                table_version="v.1",
                include_manifest=True,
                partition_scheme_id=self.partition.partition_scheme_id,
                catalog=self.catalog,
            )

            # Verify the retrieved delta matches the committed one
            assert (
                retrieved_delta is not None
            ), f"Failed to retrieve {delta_type} delta at stream position {committed_delta.locator.stream_position}"
            assert retrieved_delta.type == delta_type
            assert (
                retrieved_delta.locator.stream_position
                == committed_delta.locator.stream_position
            )

    def test_get_delta_with_properties(self):
        # Test retrieving delta with custom properties

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "prop_test": ["a", "b", "c"],
            }
        )

        # Stage delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
        )

        # Define custom properties
        custom_properties = {
            "batch_id": "batch_retrieve_001",
            "source_system": "test_retrieval_system",
            "processing_timestamp": "2024-01-01T12:00:00Z",
        }

        # Commit delta with properties
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            properties=custom_properties,
            catalog=self.catalog,
        )

        # Retrieve the committed delta
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta.locator.stream_position,
            partition_values=None,  # Using None for unpartitioned table
            table_version="v.1",
            include_manifest=True,
            partition_scheme_id=self.partition.partition_scheme_id,
            catalog=self.catalog,
        )

        # Verify the retrieved delta preserves custom properties
        assert (
            retrieved_delta is not None
        ), f"Failed to retrieve delta with properties at stream position {committed_delta.locator.stream_position}"
        assert retrieved_delta.properties == custom_properties
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )

    def test_get_delta_without_manifest(self):
        # Test retrieving delta without manifest for performance

        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "perf_test": ["x", "y", "z"],
            }
        )

        # Stage and commit delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Retrieve the committed delta WITHOUT manifest
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta.locator.stream_position,
            partition_values=None,  # Using None for unpartitioned table
            table_version="v.1",
            include_manifest=False,  # Do not include manifest
            partition_scheme_id=self.partition.partition_scheme_id,
            catalog=self.catalog,
        )

        # Verify the retrieved delta but without manifest
        assert (
            retrieved_delta is not None
        ), f"Failed to retrieve delta without manifest at stream position {committed_delta.locator.stream_position}"
        assert retrieved_delta.type == DeltaType.UPSERT
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )
        assert (
            retrieved_delta.manifest is None
        )  # Should be None when include_manifest=False

    def test_get_delta_not_exists(self):
        # Test error handling when trying to retrieve non-existent delta
        # Attempt to retrieve non-existent delta
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=999999,  # Non-existent stream position
            partition_values=None,  # Using None for unpartitioned table
            table_version="v.1",
            include_manifest=True,
            partition_scheme_id=self.partition.partition_scheme_id,
            catalog=self.catalog,
        )

        # Should return None for non-existent delta
        assert retrieved_delta is None

    def test_get_delta_invalid_namespace(self):
        # Test error handling when trying to retrieve delta with invalid namespace
        # Attempt to retrieve delta with invalid namespace
        with pytest.raises(StreamNotFoundError):
            metastore.get_delta(
                namespace="non_existent_namespace",
                table_name="test_table",
                stream_position=1,
                partition_values=None,  # Using None for unpartitioned table
                table_version="v.1",
                include_manifest=True,
                partition_scheme_id=self.partition.partition_scheme_id,
                catalog=self.catalog,
            )

    def test_get_delta_large_dataset_retrieval(self):
        # Test retrieving a delta with a large dataset

        # Create a larger dataset
        large_data = pd.DataFrame(
            {
                "id": range(500),
                "name": [f"user_{i}" for i in range(500)],
                "value": [i * 2.5 for i in range(500)],
                "category": [f"cat_{i % 5}" for i in range(500)],
            }
        )

        # Stage the large delta
        staged_delta = metastore.stage_delta(
            data=large_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.APPEND,
        )

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Retrieve the committed delta
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta.locator.stream_position,
            partition_values=None,  # Using None for unpartitioned table
            table_version="v.1",
            include_manifest=True,
            partition_scheme_id=self.partition.partition_scheme_id,
            catalog=self.catalog,
        )

        # Verify the retrieved delta for large dataset
        assert retrieved_delta is not None
        assert retrieved_delta.type == DeltaType.APPEND
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )
        assert retrieved_delta.manifest is not None

        # Verify manifest entry metadata for large dataset
        entry = retrieved_delta.manifest.entries[0]
        assert entry.meta.record_count == 500
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_get_delta_with_ray_dataset_retrieval(self):
        # Test retrieving delta originally created from Ray Dataset

        # Create Ray Dataset
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "ray_retrieve": ["a", "b", "c", "d", "e"],
            }
        )
        ray_dataset = ray.data.from_pandas(df)

        # Stage delta with Ray Dataset
        staged_delta = metastore.stage_delta(
            data=ray_dataset,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        # Commit the delta
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Retrieve the committed delta
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta.locator.stream_position,
            partition_values=None,  # Using None for unpartitioned table
            table_version="v.1",
            include_manifest=True,
            partition_scheme_id=self.partition.partition_scheme_id,
            catalog=self.catalog,
        )

        # Verify the retrieved delta
        assert retrieved_delta is not None
        assert retrieved_delta.type == DeltaType.UPSERT
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )
        assert retrieved_delta.manifest is not None

        # Verify manifest entry metadata
        entry = retrieved_delta.manifest.entries[0]
        assert entry.meta.record_count == 5
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_get_delta_manifest_consistency(self):
        # Test that retrieved delta manifest is consistent with committed delta

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "consistency_test": ["w", "x", "y", "z"],
                "metadata": [10, 20, 30, 40],
            }
        )

        # Stage and commit delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.CSV,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Retrieve the committed delta
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta.locator.stream_position,
            partition_values=None,  # Using None for unpartitioned table
            table_version="v.1",
            include_manifest=True,
            partition_scheme_id=self.partition.partition_scheme_id,
            catalog=self.catalog,
        )

        # Verify complete manifest consistency
        assert retrieved_delta is not None
        assert retrieved_delta.manifest is not None
        assert committed_delta.manifest is not None

        # Check manifest metadata
        retrieved_meta = retrieved_delta.manifest.meta
        committed_meta = committed_delta.manifest.meta
        assert retrieved_meta.record_count == committed_meta.record_count
        assert retrieved_meta.content_type == committed_meta.content_type
        assert retrieved_meta.content_encoding == committed_meta.content_encoding

        # Check manifest entries
        assert len(retrieved_delta.manifest.entries) == len(
            committed_delta.manifest.entries
        )

        for i, (retrieved_entry, committed_entry) in enumerate(
            zip(retrieved_delta.manifest.entries, committed_delta.manifest.entries)
        ):
            assert retrieved_entry.uri == committed_entry.uri
            assert (
                retrieved_entry.meta.record_count == committed_entry.meta.record_count
            )
            assert (
                retrieved_entry.meta.content_type == committed_entry.meta.content_type
            )
            assert (
                retrieved_entry.meta.content_length
                == committed_entry.meta.content_length
            )

        # Verify stream position and locator consistency
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )
        assert (
            retrieved_delta.locator.partition_locator.partition_id
            == committed_delta.locator.partition_locator.partition_id
        )
        assert (
            retrieved_delta.previous_stream_position
            == committed_delta.previous_stream_position
        )

    def test_get_delta_with_different_table_versions(self):
        # Test retrieving deltas from different table versions

        # Create data for first table version
        df1 = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Stage and commit delta for first table version (v.1)
        staged_delta_1 = metastore.stage_delta(
            data=df1,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta_1 = metastore.commit_delta(
            delta=staged_delta_1,
            catalog=self.catalog,
        )

        # Create data for second table version
        df2 = pd.DataFrame(
            {
                "id": [4, 5, 6],
                "name": ["David", "Eve", "Frank"],
            }
        )

        # Create a second table version
        _, table_version_2, stream_2 = metastore.create_table_version(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            table_version="v.2",
            catalog=self.catalog,
        )

        # Stage and commit partition for second table version
        partition_2 = metastore.stage_partition(
            stream=stream_2,
            catalog=self.catalog,
        )
        partition_2 = metastore.commit_partition(
            partition=partition_2,
            catalog=self.catalog,
        )

        # Stage and commit delta for second table version
        staged_delta_2 = metastore.stage_delta(
            data=df2,
            partition=partition_2,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta_2 = metastore.commit_delta(
            delta=staged_delta_2,
            catalog=self.catalog,
        )

        # Retrieve delta from first table version
        retrieved_delta_1 = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta_1.locator.stream_position,
            table_version="v.1",
            include_manifest=True,
            catalog=self.catalog,
        )

        # Retrieve delta from second table version
        retrieved_delta_2 = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta_2.locator.stream_position,
            table_version="v.2",
            include_manifest=True,
            catalog=self.catalog,
        )

        # Verify both deltas were retrieved correctly
        assert retrieved_delta_1 is not None
        assert retrieved_delta_2 is not None
        assert (
            retrieved_delta_1.locator.partition_locator.stream_locator.table_version_locator.table_version
            == "v.1"
        )
        assert (
            retrieved_delta_2.locator.partition_locator.stream_locator.table_version_locator.table_version
            == "v.2"
        )

    def test_get_delta_infers_latest_active_table_version(self):
        """Test that get_delta can infer the latest active table version when not explicitly specified."""

        # Create data for the test
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Set the current table version to active explicitly
        metastore.update_table_version(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            table_version=self.table_version.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )

        # Stage and commit a delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Retrieve the delta WITHOUT specifying table_version (should infer latest active)
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=committed_delta.locator.stream_position,
            # table_version=None,  # Not specified - should infer latest active
            include_manifest=True,
            catalog=self.catalog,
        )

        # Verify the delta was retrieved correctly
        assert (
            retrieved_delta is not None
        ), "Should be able to retrieve delta with inferred table version"
        assert retrieved_delta.type == DeltaType.UPSERT
        assert (
            retrieved_delta.locator.stream_position
            == committed_delta.locator.stream_position
        )
        assert retrieved_delta.manifest is not None
        assert (
            retrieved_delta.locator.partition_locator.stream_locator.table_version_locator.table_version
            == self.table_version.table_version
        )

        # Verify manifest entry metadata
        entry = retrieved_delta.manifest.entries[0]
        assert entry.meta.record_count == 3
        assert entry.meta.content_type == ContentType.PARQUET.value

    def test_list_deltas_infers_latest_active_table_version(self):
        """Test that list_deltas can infer the latest active table version when not explicitly specified."""

        # Set the current table version to active explicitly
        metastore.update_table_version(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            table_version=self.table_version.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )

        # Create and commit multiple deltas
        df1 = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
            }
        )

        df2 = pd.DataFrame(
            {
                "id": [3, 4],
                "name": ["Charlie", "David"],
            }
        )

        # Stage and commit first delta
        staged_delta_1 = metastore.stage_delta(
            data=df1,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta_1 = metastore.commit_delta(
            delta=staged_delta_1,
            catalog=self.catalog,
        )

        # Stage and commit second delta
        staged_delta_2 = metastore.stage_delta(
            data=df2,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.APPEND,
        )

        committed_delta_2 = metastore.commit_delta(
            delta=staged_delta_2,
            catalog=self.catalog,
        )

        # List deltas WITHOUT specifying table_version (should infer latest active)
        deltas = metastore.list_deltas(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            # table_version=None,  # Not specified - should infer latest active
            catalog=self.catalog,
        ).all_items()

        # Verify deltas were listed correctly
        assert len(deltas) == 2, f"Expected 2 deltas, got {len(deltas)}"

        # Convert to list to access by index
        delta_list = list(deltas)

        # Verify both deltas are from the correct table version
        for delta in delta_list:
            assert (
                delta.locator.partition_locator.stream_locator.table_version_locator.table_version
                == self.table_version.table_version
            )

        # Verify the deltas are the ones we committed (by stream position)
        stream_positions = [delta.locator.stream_position for delta in delta_list]
        expected_positions = [
            committed_delta_1.locator.stream_position,
            committed_delta_2.locator.stream_position,
        ]

        assert set(stream_positions) == set(
            expected_positions
        ), f"Expected stream positions {expected_positions}, got {stream_positions}"

        # Verify delta types
        delta_types = [delta.type for delta in delta_list]
        assert DeltaType.UPSERT in delta_types
        assert DeltaType.APPEND in delta_types

    def test_commit_delta_with_user_specified_stream_position(self):
        """Test that user-specified stream positions are preserved during delta commit instead of auto-assigning."""

        # Get initial partition state to understand current stream position
        initial_partition = metastore.get_partition_by_id(
            stream_locator=self.partition.stream_locator,
            partition_id=self.partition.partition_id,
            catalog=self.catalog,
        )
        initial_stream_position = initial_partition.stream_position or 0

        # Create test data
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "custom_stream_position": ["test1", "test2", "test3"],
            }
        )

        # Stage a delta normally (stream position will be None initially)
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        # Verify the staged delta initially has no stream position set
        assert (
            staged_delta.locator.stream_position is None
        ), "Staged delta should initially have no stream position"

        # Set a custom stream position that's greater than the current stream position
        # We'll use a large gap to make it obvious this is user-specified
        user_specified_stream_position = initial_stream_position + 100

        # Set the stream position on the delta's locator before committing
        staged_delta.locator.stream_position = user_specified_stream_position

        # Verify the stream position was set
        assert (
            staged_delta.locator.stream_position == user_specified_stream_position
        ), "Stream position should be set to user-specified value"

        # Commit the delta with the user-specified stream position
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the committed delta preserves the user-specified stream position
        assert (
            committed_delta.locator.stream_position == user_specified_stream_position
        ), f"Committed delta should preserve user-specified stream position {user_specified_stream_position}, but got {committed_delta.locator.stream_position}"

        # Verify the previous stream position is correctly set
        assert (
            committed_delta.previous_stream_position == initial_stream_position
        ), f"Previous stream position should be {initial_stream_position}, but got {committed_delta.previous_stream_position}"

        # Verify the partition's stream position is updated to the user-specified position
        updated_partition = metastore.get_partition_by_id(
            stream_locator=self.partition.stream_locator,
            partition_id=self.partition.partition_id,
            catalog=self.catalog,
        )
        assert (
            updated_partition.stream_position == user_specified_stream_position
        ), f"Partition stream position should be updated to {user_specified_stream_position}, but got {updated_partition.stream_position}"

        # Test that we can retrieve the delta using the user-specified stream position
        retrieved_delta = metastore.get_delta(
            namespace=self.namespace.locator.namespace,
            table_name="test_table",
            stream_position=user_specified_stream_position,
            table_version=self.table_version.table_version,
            include_manifest=True,
            catalog=self.catalog,
        )

        # Verify the retrieved delta matches the committed one
        assert (
            retrieved_delta is not None
        ), f"Should be able to retrieve delta at user-specified stream position {user_specified_stream_position}"
        assert retrieved_delta.locator.stream_position == user_specified_stream_position
        assert retrieved_delta.type == DeltaType.UPSERT
        assert retrieved_delta.manifest is not None

        # Test committing another delta after the custom stream position
        df2 = pd.DataFrame(
            {
                "id": [4, 5],
                "name": ["David", "Eve"],
                "follow_up": ["test4", "test5"],
            }
        )

        # Stage and commit a second delta (should get auto-assigned position)
        staged_delta_2 = metastore.stage_delta(
            data=df2,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.APPEND,
        )

        # Don't set a custom stream position for the second delta
        committed_delta_2 = metastore.commit_delta(
            delta=staged_delta_2,
            catalog=self.catalog,
        )

        # Verify the second delta gets auto-assigned position (user_specified_position + 1)
        expected_auto_assigned_position = user_specified_stream_position + 1
        assert (
            committed_delta_2.locator.stream_position == expected_auto_assigned_position
        ), f"Second delta should get auto-assigned position {expected_auto_assigned_position}, but got {committed_delta_2.locator.stream_position}"
        assert (
            committed_delta_2.previous_stream_position == user_specified_stream_position
        ), f"Second delta's previous stream position should be {user_specified_stream_position}, but got {committed_delta_2.previous_stream_position}"

    def test_commit_delta_user_stream_position_validation(self):
        """Test that user-specified stream positions are validated to be greater than previous stream position."""

        # Get initial partition state
        initial_partition = metastore.get_partition_by_id(
            stream_locator=self.partition.stream_locator,
            partition_id=self.partition.partition_id,
            catalog=self.catalog,
        )
        initial_stream_position = initial_partition.stream_position or 0

        # Create test data
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "validation_test": ["data1", "data2"],
            }
        )

        # Stage a delta
        staged_delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        # Test 1: Try to set stream position equal to previous stream position (should fail)
        staged_delta.locator.stream_position = initial_stream_position

        with pytest.raises(
            TableValidationError,
            match="Delta stream position .* must be greater than previous stream position",
        ):
            metastore.commit_delta(
                delta=staged_delta,
                catalog=self.catalog,
            )

        # Test 2: Try to set stream position less than previous stream position (should fail)
        if initial_stream_position > 0:
            staged_delta.locator.stream_position = initial_stream_position - 1

            with pytest.raises(
                TableValidationError,
                match="Delta stream position .* must be greater than previous stream position",
            ):
                metastore.commit_delta(
                    delta=staged_delta,
                    catalog=self.catalog,
                )

        # Test 3: Set valid stream position greater than previous (should succeed)
        valid_stream_position = initial_stream_position + 50
        staged_delta.locator.stream_position = valid_stream_position

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify the valid stream position was preserved
        assert (
            committed_delta.locator.stream_position == valid_stream_position
        ), f"Valid stream position {valid_stream_position} should be preserved, but got {committed_delta.locator.stream_position}"

    def test_download_delta_local_pyarrow(self):
        """Test downloading delta with local storage and PyArrow table type."""

        # Create very simple test data that matches the schema exactly
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["New York", "Boston", "Chicago"],
            }
        )

        # Stage and commit the delta
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test downloading with specific columns
        selected_columns = ["id", "name", "city"]
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            columns=selected_columns,
            catalog=self.catalog,
        )

        # Verify only selected columns are present
        downloaded_table = (
            downloaded_tables[0]
            if len(downloaded_tables) == 1
            else pa.concat_tables(downloaded_tables)
        )
        downloaded_df = downloaded_table.to_pandas()

        assert (
            list(downloaded_df.columns) == selected_columns
        ), f"Expected columns {selected_columns}, got {list(downloaded_df.columns)}"

        # Verify data integrity for selected columns
        expected_df = test_data[selected_columns]
        downloaded_df_sorted = downloaded_df.sort_values("id").reset_index(drop=True)
        expected_df_sorted = expected_df.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(downloaded_df_sorted, expected_df_sorted)

    def test_download_delta_with_delta_locator(self):
        """Test downloading delta using DeltaLocator instead of Delta object."""

        # Create and commit test data
        test_data = pd.DataFrame(
            {
                "id": [1001, 1002, 1003],
                "name": ["Customer A", "Customer B", "Customer C"],
                "age": [30, 35, 40],
                "city": ["Seattle", "Portland", "San Francisco"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test download using DeltaLocator instead of Delta object
        delta_locator = committed_delta.locator
        downloaded_tables = metastore.download_delta(
            delta_like=delta_locator,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Verify the result
        assert isinstance(downloaded_tables, list), "Expected list for LOCAL storage"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        downloaded_table = (
            downloaded_tables[0]
            if len(downloaded_tables) == 1
            else pa.concat_tables(downloaded_tables)
        )
        downloaded_df = (
            downloaded_table.to_pandas().sort_values("id").reset_index(drop=True)
        )
        expected_df = test_data.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_download_delta_invalid_columns(self):
        """Test error handling when requesting non-existent columns."""

        # Create and commit test data
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["New York", "Boston", "Chicago"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test with invalid column names (one valid, one invalid)
        invalid_columns = ["id", "non_existent_column"]
        with pytest.raises(
            SchemaValidationError,
            match="One or more columns .* are not present in table version columns",
        ):
            metastore.download_delta(
                delta_like=committed_delta,
                table_type=DatasetType.PYARROW,
                storage_type=StorageType.LOCAL,
                columns=invalid_columns,
                catalog=self.catalog,
            )

    def test_download_delta_manifest_entry_basic(self):
        """Test downloading a specific manifest entry by index."""

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["New York", "Boston", "Chicago"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert (
            len(committed_delta.manifest.entries) > 0
        ), "Should have at least one manifest entry"

        # Test downloading the first manifest entry
        entry_index = 0
        downloaded_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=entry_index,
            table_type=DatasetType.PYARROW,
            catalog=self.catalog,
        )

        # Verify the result is a PyArrow table
        assert isinstance(
            downloaded_table, pa.Table
        ), f"Expected PyArrow Table, got {type(downloaded_table)}"
        assert len(downloaded_table) > 0, "Downloaded table should not be empty"
        assert len(downloaded_table) <= len(
            test_data
        ), "Downloaded table should not have more rows than test data"

        # Verify columns match expected schema
        expected_columns = ["id", "name", "age", "city"]
        assert (
            list(downloaded_table.column_names) == expected_columns
        ), f"Expected columns {expected_columns}, got {list(downloaded_table.column_names)}"

    def test_download_delta_content_types_with_pandas(self):
        """Test downloading delta with different content types as pandas DataFrames."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "David"],
                "age": [25, 30, 35, 40],
                "city": ["New York", "London", "Paris", "Tokyo"],
            }
        )

        # Test with PARQUET content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download as pandas DataFrame
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PANDAS,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_tables, list), "Expected list for LOCAL storage"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        # Concatenate all tables and verify data
        if len(downloaded_tables) == 1:
            result_df = downloaded_tables[0]
        else:
            result_df = pd.concat(downloaded_tables, ignore_index=True)

        assert isinstance(result_df, pd.DataFrame), "Expected pandas DataFrame"
        assert len(result_df) == len(test_data), "Row count mismatch"
        assert set(result_df.columns) == set(test_data.columns), "Column mismatch"

    def test_download_delta_content_types_with_pyarrow(self):
        """Test downloading delta with different content types as PyArrow Tables."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [10, 20, 30],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [25, 30, 35],
                "city": ["Seattle", "Boston", "Chicago"],
            }
        )

        # Test with CSV content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.CSV,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download as PyArrow Table
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_tables, list), "Expected list for LOCAL storage"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        # Concatenate all tables and verify data
        if len(downloaded_tables) == 1:
            result_table = downloaded_tables[0]
        else:
            result_table = pa.concat_tables(downloaded_tables)

        assert isinstance(result_table, pa.Table), "Expected PyArrow Table"
        assert len(result_table) == len(test_data), "Row count mismatch"
        assert set(result_table.column_names) == set(
            test_data.columns
        ), "Column mismatch"

    def test_download_delta_content_types_with_polars(self):
        """Test downloading delta with different content types as Polars DataFrames."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [101, 102, 103, 104, 105],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "age": [15, 23, 8, 45, 12],
                "city": ["City A", "City B", "City C", "City D", "City E"],
            }
        )

        # Test with PARQUET content type (avoiding JSON due to PanicException)
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download as Polars DataFrame
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.POLARS,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_tables, list), "Expected list for LOCAL storage"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        # Concatenate all tables and verify data
        if len(downloaded_tables) == 1:
            result_df = downloaded_tables[0]
        else:
            result_df = pl.concat(downloaded_tables)

        assert isinstance(result_df, pl.DataFrame), "Expected Polars DataFrame"
        assert len(result_df) == len(test_data), "Row count mismatch"
        assert set(result_df.columns) == set(test_data.columns), "Column mismatch"

    def test_download_delta_content_types_with_numpy(self):
        """Test downloading delta with different content types as NumPy arrays."""

        # Test data - simpler data types that work well with numpy
        test_data = pd.DataFrame(
            {"id": [1, 2, 3], "value": [10.5, 20.7, 30.9], "count": [5, 10, 15]}
        )

        # Test with FEATHER content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.FEATHER,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download as NumPy array
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.NUMPY,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_tables, list), "Expected list for LOCAL storage"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        # Concatenate all arrays and verify data
        if len(downloaded_tables) == 1:
            result_array = downloaded_tables[0]
        else:
            result_array = np.concatenate(downloaded_tables, axis=0)

        assert isinstance(result_array, np.ndarray), "Expected NumPy array"
        assert len(result_array) == len(test_data), "Row count mismatch"
        # NumPy arrays don't have column names, but should have the right shape
        assert result_array.shape[1] == len(test_data.columns), "Column count mismatch"

    def test_download_manifest_entry_content_types_with_pandas(self):
        """Test downloading manifest entries with different content types as pandas DataFrames."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [1001, 1002, 1003],
                "name": ["Customer A", "Customer B", "Customer C"],
                "age": [30, 45, 28],
                "city": ["Houston", "Phoenix", "Philadelphia"],
            }
        )

        # Test with TSV content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.TSV,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest with entries
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert len(committed_delta.manifest.entries) > 0, "Should have manifest entries"

        # Download first manifest entry as pandas DataFrame
        downloaded_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=0,
            table_type=DatasetType.PANDAS,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_table, pd.DataFrame), "Expected pandas DataFrame"
        assert len(downloaded_table) > 0, "Downloaded table should not be empty"
        assert len(downloaded_table) <= len(
            test_data
        ), "Downloaded table should not exceed test data size"
        assert set(downloaded_table.columns) == set(
            test_data.columns
        ), "Column mismatch"

    def test_download_manifest_entry_content_types_with_pyarrow(self):
        """Test downloading manifest entries with different content types as PyArrow Tables."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [2001, 2002, 2003, 2004],
                "name": ["Account A", "Account B", "Account C", "Account D"],
                "age": [32, 28, 45, 39],
                "city": ["San Antonio", "San Diego", "Dallas", "San Jose"],
            }
        )

        # Test with PSV content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PSV,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest with entries
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert len(committed_delta.manifest.entries) > 0, "Should have manifest entries"

        # Download first manifest entry as PyArrow Table
        downloaded_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=0,
            table_type=DatasetType.PYARROW,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_table, pa.Table), "Expected PyArrow Table"
        assert len(downloaded_table) > 0, "Downloaded table should not be empty"
        assert len(downloaded_table) <= len(
            test_data
        ), "Downloaded table should not exceed test data size"
        assert set(downloaded_table.column_names) == set(
            test_data.columns
        ), "Column mismatch"

    def test_download_delta_cross_table_type_consistency(self):
        """Test that different table types return consistent data for the same delta."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alpha", "Beta", "Gamma"],
                "age": [25, 30, 35],
                "city": ["City A", "City B", "City C"],
            }
        )

        # Stage and commit delta
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,  # Use Parquet for best type preservation
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download as different table types
        pandas_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PANDAS,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        pyarrow_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        polars_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.POLARS,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        numpy_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.NUMPY,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Verify all return types and basic properties
        assert all(
            isinstance(t, pd.DataFrame) for t in pandas_tables
        ), "All pandas tables should be DataFrames"
        assert all(
            isinstance(t, pa.Table) for t in pyarrow_tables
        ), "All PyArrow tables should be Tables"
        assert all(
            isinstance(t, pl.DataFrame) for t in polars_tables
        ), "All Polars tables should be DataFrames"
        assert all(
            isinstance(t, np.ndarray) for t in numpy_tables
        ), "All NumPy tables should be arrays"

        # Verify row counts are consistent
        pandas_total_rows = sum(len(t) for t in pandas_tables)
        pyarrow_total_rows = sum(len(t) for t in pyarrow_tables)
        polars_total_rows = sum(len(t) for t in polars_tables)
        numpy_total_rows = sum(len(t) for t in numpy_tables)

        assert pandas_total_rows == len(
            test_data
        ), "Pandas row count should match test data"
        assert pyarrow_total_rows == len(
            test_data
        ), "PyArrow row count should match test data"
        assert polars_total_rows == len(
            test_data
        ), "Polars row count should match test data"
        assert numpy_total_rows == len(
            test_data
        ), "NumPy row count should match test data"

    def test_download_manifest_entry_cross_table_type_consistency(self):
        """Test that different table types return consistent data for the same manifest entry."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [401, 402, 403],
                "name": ["Metric A", "Metric B", "Metric C"],
                "age": [5, 10, 15],  # metric age in years
                "city": ["Server A", "Server B", "Server C"],
            }
        )

        # Stage and commit delta
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,  # Use Parquet for best type preservation
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest with entries
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert len(committed_delta.manifest.entries) > 0, "Should have manifest entries"

        # Download same manifest entry as different table types
        entry_index = 0

        pandas_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=entry_index,
            table_type=DatasetType.PANDAS,
            catalog=self.catalog,
        )

        pyarrow_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=entry_index,
            table_type=DatasetType.PYARROW,
            catalog=self.catalog,
        )

        polars_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=entry_index,
            table_type=DatasetType.POLARS,
            catalog=self.catalog,
        )

        numpy_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=entry_index,
            table_type=DatasetType.NUMPY,
            catalog=self.catalog,
        )

        # Verify return types
        assert isinstance(
            pandas_table, pd.DataFrame
        ), "Pandas download should return DataFrame"
        assert isinstance(
            pyarrow_table, pa.Table
        ), "PyArrow download should return Table"
        assert isinstance(
            polars_table, pl.DataFrame
        ), "Polars download should return DataFrame"
        assert isinstance(
            numpy_table, np.ndarray
        ), "NumPy download should return ndarray"

        # Verify basic consistency (same number of rows, same columns where applicable)
        assert len(pandas_table) == len(
            pyarrow_table
        ), "Pandas and PyArrow should have same row count"
        assert len(pyarrow_table) == len(
            polars_table
        ), "PyArrow and Polars should have same row count"
        assert len(polars_table) == len(
            numpy_table
        ), "Polars and NumPy should have same row count"

        # Verify column counts where applicable
        assert len(pandas_table.columns) == len(
            pyarrow_table.column_names
        ), "Column count should match between pandas and PyArrow"
        assert len(pyarrow_table.column_names) == len(
            polars_table.columns
        ), "Column count should match between PyArrow and Polars"
        assert (
            len(polars_table.columns) == numpy_table.shape[1]
        ), "Column count should match between Polars and NumPy"

    def test_download_delta_with_column_selection_all_table_types(self):
        """Test downloading delta with column selection across all table types."""

        # Test data with correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": [
                    "Product A",
                    "Product B",
                    "Product C",
                    "Product D",
                    "Product E",
                ],
                "age": [1, 2, 3, 4, 5],  # product age in years
                "city": [
                    "Factory A",
                    "Factory B",
                    "Factory C",
                    "Factory D",
                    "Factory E",
                ],
            }
        )

        # Stage and commit delta
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test column selection - using actual schema columns
        selected_columns = ["id", "name", "age"]

        # Test with pandas
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PANDAS,
            storage_type=StorageType.LOCAL,
            columns=selected_columns,
            catalog=self.catalog,
        )

        assert isinstance(
            downloaded_tables, list
        ), "Expected list for LOCAL storage with pandas"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        if len(downloaded_tables) == 1:
            result = downloaded_tables[0]
        else:
            result = pd.concat(downloaded_tables, ignore_index=True)
        assert (
            list(result.columns) == selected_columns
        ), "Column selection failed for pandas"

        # Test with PyArrow
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            columns=selected_columns,
            catalog=self.catalog,
        )

        assert isinstance(
            downloaded_tables, list
        ), "Expected list for LOCAL storage with PyArrow"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        if len(downloaded_tables) == 1:
            result = downloaded_tables[0]
        else:
            result = pa.concat_tables(downloaded_tables)
        assert (
            list(result.column_names) == selected_columns
        ), "Column selection failed for PyArrow"

        # Test with Polars
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.POLARS,
            storage_type=StorageType.LOCAL,
            columns=selected_columns,
            catalog=self.catalog,
        )

        assert isinstance(
            downloaded_tables, list
        ), "Expected list for LOCAL storage with Polars"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        if len(downloaded_tables) == 1:
            result = downloaded_tables[0]
        else:
            result = pl.concat(downloaded_tables)
        assert (
            list(result.columns) == selected_columns
        ), "Column selection failed for Polars"

        # Test with NumPy
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.NUMPY,
            storage_type=StorageType.LOCAL,
            columns=selected_columns,
            catalog=self.catalog,
        )

        assert isinstance(
            downloaded_tables, list
        ), "Expected list for LOCAL storage with NumPy"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"

        if len(downloaded_tables) == 1:
            result = downloaded_tables[0]
        else:
            result = np.concatenate(downloaded_tables, axis=0)
        # NumPy arrays don't have column names, but should have the right number of columns
        assert result.shape[1] == len(
            selected_columns
        ), "Column count mismatch for NumPy"

    def test_download_manifest_entry_content_types_with_polars(self):
        """Test downloading manifest entries with different content types as Polars DataFrames."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [301, 302, 303, 304, 305],
                "name": ["Sensor A", "Sensor B", "Sensor C", "Sensor D", "Sensor E"],
                "age": [1, 2, 3, 4, 5],  # sensor age in years
                "city": [
                    "Building A",
                    "Building B",
                    "Building C",
                    "Building D",
                    "Building E",
                ],
            }
        )

        # Test with AVRO content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.AVRO,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest with entries
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert len(committed_delta.manifest.entries) > 0, "Should have manifest entries"

        # Download first manifest entry as Polars DataFrame
        downloaded_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=0,
            table_type=DatasetType.POLARS,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_table, pl.DataFrame), "Expected Polars DataFrame"
        assert len(downloaded_table) > 0, "Downloaded table should not be empty"
        assert len(downloaded_table) <= len(
            test_data
        ), "Downloaded table should not exceed test data size"
        assert set(downloaded_table.columns) == set(
            test_data.columns
        ), "Column mismatch"

    def test_download_manifest_entry_content_types_with_numpy(self):
        """Test downloading manifest entries with different content types as NumPy arrays."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Point A", "Point B", "Point C", "Point D"],
                "age": [10, 20, 30, 40],  # point age
                "city": ["Grid 1", "Grid 2", "Grid 3", "Grid 4"],
            }
        )

        # Test with ORC content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.ORC,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest with entries
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert len(committed_delta.manifest.entries) > 0, "Should have manifest entries"

        # Download first manifest entry as NumPy array
        downloaded_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=0,
            table_type=DatasetType.NUMPY,
            catalog=self.catalog,
        )

        # Verify result
        assert isinstance(downloaded_table, np.ndarray), "Expected NumPy array"
        assert len(downloaded_table) > 0, "Downloaded table should not be empty"
        assert len(downloaded_table) <= len(
            test_data
        ), "Downloaded table should not exceed test data size"
        # NumPy arrays don't have column names, but should have the right shape
        assert downloaded_table.shape[1] == len(
            test_data.columns
        ), "Column count mismatch"

    def test_download_delta_content_types_with_polars_json(self):
        """Test downloading delta with JSON content type as Polars DataFrames."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [201, 202, 203, 204, 205],
                "name": ["UserA", "UserB", "UserC", "UserD", "UserE"],
                "age": [25, 30, 35, 40, 45],
                "city": ["CityA", "CityB", "CityC", "CityD", "CityE"],
            }
        )

        # Test with JSON content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.JSON,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Try to download as Polars DataFrame - this may fail due to known Polars+JSON issues
        downloaded_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.POLARS,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # If we get here, the download succeeded
        assert isinstance(
            downloaded_tables, list
        ), "Expected list for LOCAL storage with Polars"
        assert len(downloaded_tables) > 0, "Downloaded tables should not be empty"
        assert all(
            isinstance(t, pl.DataFrame) for t in downloaded_tables
        ), "All tables should be Polars DataFrames"

        # Verify data integrity
        if len(downloaded_tables) == 1:
            result = downloaded_tables[0]
        else:
            result = pl.concat(downloaded_tables)

        assert len(result) == len(test_data), "Row count should match"
        assert set(result.columns) == set(
            test_data.columns
        ), "Column names should match"

    def test_download_manifest_entry_content_types_with_polars_json(self):
        """Test downloading manifest entries with JSON content type as Polars DataFrames."""

        # Test data - using correct schema: id, name, age, city
        test_data = pd.DataFrame(
            {
                "id": [401, 402, 403, 404],
                "name": ["ItemA", "ItemB", "ItemC", "ItemD"],
                "age": [1, 2, 3, 4],  # item age
                "city": ["StoreA", "StoreB", "StoreC", "StoreD"],
            }
        )

        # Test with JSON content type
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.JSON,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Verify we have a manifest with entries
        assert committed_delta.manifest is not None, "Delta should have a manifest"
        assert len(committed_delta.manifest.entries) > 0, "Should have manifest entries"

        # Try to download manifest entry as Polars DataFrame
        downloaded_table = metastore.download_delta_manifest_entry(
            delta_like=committed_delta,
            entry_index=0,
            table_type=DatasetType.POLARS,
            catalog=self.catalog,
        )

        # If we get here, the download succeeded
        assert isinstance(downloaded_table, pl.DataFrame), "Expected Polars DataFrame"
        assert len(downloaded_table) > 0, "Downloaded table should not be empty"
        assert len(downloaded_table) <= len(
            test_data
        ), "Downloaded table should not exceed test data size"
        assert set(downloaded_table.columns) == set(
            test_data.columns
        ), "Column mismatch"

    def test_download_delta_distributed_basic_pyarrow(self):
        """Test basic distributed download with Ray Data using PyArrow table type."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create larger test data to benefit from distributed processing
        test_data = pd.DataFrame(
            {
                "id": list(range(1000, 1100)),
                "name": [f"User_{i}" for i in range(1000, 1100)],
                "age": [25 + (i % 50) for i in range(100)],
                "city": [f"City_{i % 10}" for i in range(100)],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test distributed download with Ray Dataset
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            max_parallelism=4,
            catalog=self.catalog,
        )

        # Verify the result is a Ray Dataset
        assert isinstance(distributed_dataset, RayDataset), "Expected Ray Dataset"

        # Verify row count without materializing the entire dataset
        row_count = distributed_dataset.count()
        assert row_count == len(test_data), "Row count should match"

        # Convert to pandas to verify data integrity
        downloaded_df = (
            distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        )
        expected_df = test_data.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_download_delta_distributed_with_delta_locator(self):
        """Test distributed download using DeltaLocator instead of Delta object."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [2001, 2002, 2003, 2004, 2005],
                "name": ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"],
                "age": [30, 35, 40, 45, 50],
                "city": [
                    "Seattle",
                    "Portland",
                    "San Francisco",
                    "Los Angeles",
                    "San Diego",
                ],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test download using DeltaLocator with distributed storage
        delta_locator = committed_delta.locator
        distributed_dataset = metastore.download_delta(
            delta_like=delta_locator,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            catalog=self.catalog,
        )

        # Verify the result is a Ray Dataset
        assert isinstance(distributed_dataset, RayDataset), "Expected Ray Dataset"

        # Convert to pandas and verify data integrity
        downloaded_df = (
            distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        )
        expected_df = test_data.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_download_delta_distributed_different_table_types(self):
        """Test distributed download with different table types."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [3001, 3002, 3003],
                "name": ["ServiceA", "ServiceB", "ServiceC"],
                "age": [1, 2, 3],  # service years
                "city": ["DataCenter1", "DataCenter2", "DataCenter3"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test different table types with distributed download
        table_types_to_test = [
            DatasetType.PYARROW,
            DatasetType.PANDAS,
            DatasetType.POLARS,  # Now supported in distributed Ray datasets
        ]

        for table_type in table_types_to_test:
            distributed_dataset = metastore.download_delta(
                delta_like=committed_delta,
                table_type=table_type,
                storage_type=StorageType.DISTRIBUTED,
                distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
                catalog=self.catalog,
            )

            # Verify the result is a Ray Dataset
            assert isinstance(
                distributed_dataset, RayDataset
            ), f"Expected Ray Dataset for {table_type}"

            # Convert to pandas and verify basic properties
            downloaded_df = distributed_dataset.to_pandas()
            assert len(downloaded_df) == len(
                test_data
            ), f"Row count mismatch for {table_type}"
            assert set(downloaded_df.columns) == set(
                test_data.columns
            ), f"Column mismatch for {table_type}"

    def test_download_delta_distributed_different_content_types(self):
        """Test distributed download with different content types."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Test data
        test_data = pd.DataFrame(
            {
                "id": [4001, 4002, 4003, 4004],
                "name": ["Product A", "Product B", "Product C", "Product D"],
                "age": [1, 2, 3, 4],  # product years
                "city": ["Warehouse A", "Warehouse B", "Warehouse C", "Warehouse D"],
            }
        )

        # Test different content types
        content_types_to_test = [
            ContentType.PARQUET,
            ContentType.CSV,
            ContentType.JSON,
            ContentType.AVRO,
        ]

        for content_type in content_types_to_test:
            staged_delta = metastore.stage_delta(
                data=test_data,
                partition=self.partition,
                catalog=self.catalog,
                content_type=content_type,
                delta_type=DeltaType.UPSERT,
            )

            committed_delta = metastore.commit_delta(
                delta=staged_delta,
                catalog=self.catalog,
            )

            # Test distributed download
            distributed_dataset = metastore.download_delta(
                delta_like=committed_delta,
                table_type=DatasetType.PYARROW,
                storage_type=StorageType.DISTRIBUTED,
                distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
                catalog=self.catalog,
            )

            # Verify the result is a Ray Dataset
            assert isinstance(
                distributed_dataset, RayDataset
            ), f"Expected Ray Dataset for {content_type}"

            # Convert to pandas and verify basic properties
            downloaded_df = distributed_dataset.to_pandas()
            assert len(downloaded_df) == len(
                test_data
            ), f"Row count mismatch for {content_type}"
            assert set(downloaded_df.columns) == set(
                test_data.columns
            ), f"Column mismatch for {content_type}"

    def test_download_delta_distributed_with_column_selection(self):
        """Test distributed download with column selection."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create test data with multiple columns
        test_data = pd.DataFrame(
            {
                "id": [5001, 5002, 5003, 5004, 5005],
                "name": ["Task A", "Task B", "Task C", "Task D", "Task E"],
                "age": [10, 20, 30, 40, 50],  # task priority
                "city": ["Queue 1", "Queue 2", "Queue 3", "Queue 4", "Queue 5"],
                "extra_column": ["data1", "data2", "data3", "data4", "data5"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test download with column selection
        selected_columns = ["id", "name", "age"]
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            columns=selected_columns,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            catalog=self.catalog,
        )

        # Verify the result is a Ray Dataset
        assert isinstance(distributed_dataset, RayDataset), "Expected Ray Dataset"

        # Convert to pandas and verify column selection
        downloaded_df = distributed_dataset.to_pandas()
        assert set(downloaded_df.columns) == set(
            selected_columns
        ), "Column selection mismatch"
        assert len(downloaded_df) == len(test_data), "Row count should match"

        # Verify data integrity for selected columns
        expected_df = test_data[selected_columns]
        downloaded_df_sorted = downloaded_df.sort_values("id").reset_index(drop=True)
        expected_df_sorted = expected_df.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(downloaded_df_sorted, expected_df_sorted)

    def test_download_delta_distributed_with_max_parallelism(self):
        """Test distributed download with different parallelism settings."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create larger test data to see parallelism effects
        test_data = pd.DataFrame(
            {
                "id": list(range(6000, 6050)),
                "name": [f"Record_{i}" for i in range(6000, 6050)],
                "age": [25 + (i % 40) for i in range(50)],
                "city": [f"Location_{i % 5}" for i in range(50)],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test different parallelism settings
        parallelism_settings = [1, 2, 4, 8]

        for max_parallelism in parallelism_settings:
            distributed_dataset = metastore.download_delta(
                delta_like=committed_delta,
                table_type=DatasetType.PYARROW,
                storage_type=StorageType.DISTRIBUTED,
                max_parallelism=max_parallelism,
                distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
                catalog=self.catalog,
            )

            # Verify the result is a Ray Dataset
            assert isinstance(
                distributed_dataset, RayDataset
            ), f"Expected Ray Dataset for parallelism {max_parallelism}"

            # Convert to pandas and verify data integrity
            downloaded_df = distributed_dataset.to_pandas()
            assert len(downloaded_df) == len(
                test_data
            ), f"Row count mismatch for parallelism {max_parallelism}"
            assert set(downloaded_df.columns) == set(
                test_data.columns
            ), f"Column mismatch for parallelism {max_parallelism}"

    def test_download_delta_distributed_large_dataset(self):
        """Test distributed download with a larger dataset to verify performance benefits."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create a larger dataset
        size = 1000
        test_data = pd.DataFrame(
            {
                "id": list(range(7000, 7000 + size)),
                "name": [f"Entity_{i}" for i in range(size)],
                "age": [20 + (i % 60) for i in range(size)],
                "city": [f"Region_{i % 20}" for i in range(size)],
                "category": [f"Cat_{i % 10}" for i in range(size)],
                "value": [float(i * 1.5) for i in range(size)],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test distributed download with high parallelism
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            max_parallelism=8,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            catalog=self.catalog,
        )

        # Verify the result is a Ray Dataset
        assert isinstance(distributed_dataset, RayDataset), "Expected Ray Dataset"
        # isinstance check already verifies Ray Dataset type which includes count method

        # Verify row count without materializing the entire dataset
        row_count = distributed_dataset.count()
        assert row_count == len(test_data), "Row count should match"

        # Sample a few rows to verify data integrity
        sample_df = distributed_dataset.limit(10).to_pandas()
        assert len(sample_df) == 10, "Sample should have 10 rows"
        assert set(sample_df.columns) == set(
            test_data.columns
        ), "Column names should match"

    def test_download_delta_distributed_cross_table_type_consistency(self):
        """Test that distributed downloads return consistent data across table types."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Test data
        test_data = pd.DataFrame(
            {
                "id": [8001, 8002, 8003],
                "name": ["ItemX", "ItemY", "ItemZ"],
                "age": [5, 10, 15],
                "city": ["StoreX", "StoreY", "StoreZ"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,  # Use Parquet for best type preservation
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download as different table types with distributed storage
        table_types_to_test = [
            DatasetType.PYARROW,
            DatasetType.PANDAS,
            DatasetType.POLARS,  # Now supported in distributed Ray datasets
        ]

        downloaded_dfs = {}
        for table_type in table_types_to_test:
            distributed_dataset = metastore.download_delta(
                delta_like=committed_delta,
                table_type=table_type,
                storage_type=StorageType.DISTRIBUTED,
                distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
                catalog=self.catalog,
            )

            # Convert to pandas for comparison
            downloaded_df = (
                distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
            )
            downloaded_dfs[table_type] = downloaded_df

        # Verify consistency across table types
        base_df = downloaded_dfs[DatasetType.PYARROW]
        for table_type, df in downloaded_dfs.items():
            assert len(df) == len(base_df), f"Row count mismatch for {table_type}"
            assert set(df.columns) == set(
                base_df.columns
            ), f"Column names mismatch for {table_type}"

            # Compare data values (allowing for type differences)
            for col in base_df.columns:
                assert list(df[col]) == list(
                    base_df[col]
                ), f"Data mismatch in column {col} for {table_type}"

    def test_download_delta_distributed_ray_options(self):
        """Test distributed download with custom Ray options."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [9001, 9002, 9003, 9004],
                "name": ["Resource A", "Resource B", "Resource C", "Resource D"],
                "age": [1, 2, 3, 4],
                "city": ["Node A", "Node B", "Node C", "Node D"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Define custom Ray options provider
        def custom_ray_options_provider(parallelism, context):
            return {
                "num_cpus": 0.5,  # Use half a CPU per task
                "memory": 100 * 1024 * 1024,  # 100MB memory limit
            }

        # Test download with custom Ray options
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            ray_options_provider=custom_ray_options_provider,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            catalog=self.catalog,
        )

        # Verify the result is a Ray Dataset
        assert isinstance(distributed_dataset, RayDataset), "Expected Ray Dataset"

        # Convert to pandas and verify data integrity
        downloaded_df = (
            distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        )
        expected_df = test_data.sort_values("id").reset_index(drop=True)

        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_download_delta_distributed_vs_local_consistency(self):
        """Test that distributed and local downloads return the same data."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [10001, 10002, 10003, 10004, 10005],
                "name": ["CompareA", "CompareB", "CompareC", "CompareD", "CompareE"],
                "age": [30, 35, 40, 45, 50],
                "city": [
                    "TestCity1",
                    "TestCity2",
                    "TestCity3",
                    "TestCity4",
                    "TestCity5",
                ],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download using local storage
        local_tables = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.LOCAL,
            catalog=self.catalog,
        )

        # Convert local result to pandas
        if len(local_tables) == 1:
            local_df = local_tables[0].to_pandas()
        else:
            local_df = pa.concat_tables(local_tables).to_pandas()
        local_df = local_df.sort_values("id").reset_index(drop=True)

        # Download using distributed storage
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            catalog=self.catalog,
        )

        # Convert distributed result to pandas
        distributed_df = (
            distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        )

        # Verify they are identical
        pd.testing.assert_frame_equal(local_df, distributed_df)

    def test_download_delta_distributed_error_handling(self):
        """Test error handling in distributed downloads."""

        # Ensure Ray is initialized
        if not ray.is_initialized():
            ray.init()

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [11001, 11002, 11003],
                "name": ["ErrorTestA", "ErrorTestB", "ErrorTestC"],
                "age": [25, 30, 35],
                "city": ["ErrorCity1", "ErrorCity2", "ErrorCity3"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test with invalid column names
        with pytest.raises(
            SchemaValidationError,
            match="One or more columns .* are not present in table version columns",
        ):
            metastore.download_delta(
                delta_like=committed_delta,
                table_type=DatasetType.PYARROW,
                storage_type=StorageType.DISTRIBUTED,
                columns=["id", "non_existent_column"],
                distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
                catalog=self.catalog,
            )

    # ========== DAFT DISTRIBUTED TESTS ==========

    def test_download_delta_distributed_daft_basic(self):
        """Test basic distributed download with DAFT dataset type."""

        # Create test data
        test_data = pd.DataFrame(
            {
                "id": [12001, 12002, 12003, 12004, 12005],
                "name": ["DAFT_A", "DAFT_B", "DAFT_C", "DAFT_D", "DAFT_E"],
                "age": [21, 22, 23, 24, 25],
                "city": [
                    "DaftCity1",
                    "DaftCity2",
                    "DaftCity3",
                    "DaftCity4",
                    "DaftCity5",
                ],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )

        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download using DAFT distributed dataset type
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            catalog=self.catalog,
        )

        # Verify the result is a DAFT DataFrame
        assert isinstance(distributed_dataset, DaftDataFrame), "Expected DAFT DataFrame"

        # Convert to pandas and verify data integrity
        downloaded_df = (
            distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        )
        expected_df = test_data.sort_values("id").reset_index(drop=True)

        assert len(downloaded_df) == len(expected_df), "Row count mismatch"
        assert set(downloaded_df.columns) == set(
            expected_df.columns
        ), "Column names mismatch"
        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_download_delta_distributed_daft_with_delta_locator(self):
        """Test DAFT distributed download using DeltaLocator instead of Delta object."""

        test_data = pd.DataFrame(
            {
                "id": [12101, 12102, 12103],
                "name": ["DAFT_Locator_A", "DAFT_Locator_B", "DAFT_Locator_C"],
                "age": [30, 31, 32],
                "city": ["DaftLocCity1", "DaftLocCity2", "DaftLocCity3"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
            catalog=self.catalog,
        )
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download using DeltaLocator with DAFT
        distributed_dataset = metastore.download_delta(
            delta_like=committed_delta.locator,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            catalog=self.catalog,
        )

        downloaded_df = (
            distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        )
        expected_df = test_data.sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_download_delta_distributed_daft_vs_ray_consistency(self):
        """Test that DAFT and Ray distributed downloads return the same data."""

        test_data = pd.DataFrame(
            {
                "id": [12501, 12502, 12503, 12504],
                "name": [
                    "Consistency_A",
                    "Consistency_B",
                    "Consistency_C",
                    "Consistency_D",
                ],
                "age": [60, 61, 62, 63],
                "city": [
                    "ConsistencyCity1",
                    "ConsistencyCity2",
                    "ConsistencyCity3",
                    "ConsistencyCity4",
                ],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Download using DAFT
        daft_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.DAFT,
            catalog=self.catalog,
        )

        # Download using Ray
        ray_dataset = metastore.download_delta(
            delta_like=committed_delta,
            table_type=DatasetType.PYARROW,
            storage_type=StorageType.DISTRIBUTED,
            distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
            catalog=self.catalog,
        )

        # Compare results
        daft_df = daft_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        ray_df = ray_dataset.to_pandas().sort_values("id").reset_index(drop=True)
        pd.testing.assert_frame_equal(daft_df, ray_df)

    def test_download_delta_distributed_daft_error_handling(self):
        """Test error handling in DAFT distributed downloads."""

        test_data = pd.DataFrame(
            {
                "id": [12601, 12602, 12603],
                "name": ["DaftError_A", "DaftError_B", "DaftError_C"],
                "age": [70, 71, 72],
                "city": ["DaftErrorCity1", "DaftErrorCity2", "DaftErrorCity3"],
            }
        )

        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=self.partition,
            catalog=self.catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )
        committed_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=self.catalog,
        )

        # Test with invalid column names using DAFT
        with pytest.raises(
            SchemaValidationError,
            match="One or more columns .* are not present in table version columns",
        ):
            metastore.download_delta(
                delta_like=committed_delta,
                table_type=DatasetType.PYARROW,
                storage_type=StorageType.DISTRIBUTED,
                columns=["id", "invalid_column"],
                distributed_dataset_type=DistributedDatasetType.DAFT,
                catalog=self.catalog,
            )

    def test_list_partition_deltas_empty_partition(self):
        """Test listing deltas from a partition with no committed deltas."""
        result = metastore.list_partition_deltas(
            partition_like=self.partition.locator,
            catalog=self.catalog,
        )

        # Should return empty list for partition with no deltas
        deltas = result.all_items()
        assert isinstance(deltas, list)
        assert len(deltas) == 0

    def test_list_partition_deltas_with_partition_object(self):
        """Test listing deltas using a partition object."""
        # Create test data
        df = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
        )

        # Stage and commit multiple deltas
        delta1 = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
        )
        committed_delta1 = metastore.commit_delta(delta1, catalog=self.catalog)

        delta2 = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
            delta_type=DeltaType.APPEND,
        )
        committed_delta2 = metastore.commit_delta(delta2, catalog=self.catalog)

        # Test listing with partition object
        result = metastore.list_partition_deltas(
            partition_like=self.partition,
            catalog=self.catalog,
        )

        # Should return both deltas in descending order by default
        deltas = result.all_items()
        assert isinstance(deltas, list)
        deltas = result.all_items()
        assert len(deltas) == 2
        assert deltas[0].stream_position == committed_delta2.stream_position
        assert deltas[1].stream_position == committed_delta1.stream_position
        assert deltas[0].type == DeltaType.APPEND
        assert deltas[1].type == DeltaType.UPSERT

    def test_list_partition_deltas_with_partition_locator(self):
        """Test listing deltas using a partition locator."""
        # Create test data
        df = pd.DataFrame(
            {"id": [4, 5, 6], "name": ["David", "Eve", "Frank"], "age": [40, 45, 50]}
        )

        # Stage and commit a delta
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
        )
        committed_delta = metastore.commit_delta(delta, catalog=self.catalog)

        # Test listing with partition locator
        result = metastore.list_partition_deltas(
            partition_like=self.partition.locator,
            catalog=self.catalog,
        )

        # Should return the delta
        deltas = result.all_items()
        assert isinstance(deltas, list)
        deltas = result.all_items()
        assert len(deltas) >= 1  # At least the delta we just added
        found_delta = next(
            (d for d in deltas if d.stream_position == committed_delta.stream_position),
            None,
        )
        assert found_delta is not None
        assert found_delta.type == DeltaType.UPSERT

    def test_list_partition_deltas_with_stream_position_filter(self):
        """Test filtering deltas by stream position."""
        # Create test data
        df = pd.DataFrame(
            {"id": [7, 8, 9], "name": ["Grace", "Henry", "Irene"], "age": [55, 60, 65]}
        )

        # Stage and commit multiple deltas
        committed_deltas = []
        for i in range(3):
            delta = metastore.stage_delta(
                data=df,
                partition=self.partition,
                catalog=self.catalog,
            )
            committed_delta = metastore.commit_delta(delta, catalog=self.catalog)
            committed_deltas.append(committed_delta)

        # Test filtering by first_stream_position
        result = metastore.list_partition_deltas(
            partition_like=self.partition,
            first_stream_position=committed_deltas[1].stream_position,
            catalog=self.catalog,
        )

        # Should only return deltas from position 2 onwards
        deltas = result.all_items()
        assert isinstance(deltas, list)
        deltas = result.all_items()
        assert len(deltas) >= 2
        deltas = result.all_items()
        for delta in deltas:
            assert delta.stream_position >= committed_deltas[1].stream_position

        # Test filtering by last_stream_position
        result = metastore.list_partition_deltas(
            partition_like=self.partition,
            last_stream_position=committed_deltas[1].stream_position,
            catalog=self.catalog,
        )

        # Should only return deltas up to position 2
        deltas = result.all_items()
        assert isinstance(deltas, list)
        deltas = result.all_items()
        for delta in deltas:
            assert delta.stream_position <= committed_deltas[1].stream_position

        # Test filtering by both first and last
        result = metastore.list_partition_deltas(
            partition_like=self.partition,
            first_stream_position=committed_deltas[0].stream_position,
            last_stream_position=committed_deltas[1].stream_position,
            catalog=self.catalog,
        )

        # Should return deltas in the specified range
        deltas = result.all_items()
        assert isinstance(deltas, list)
        deltas = result.all_items()
        for delta in deltas:
            assert (
                committed_deltas[0].stream_position
                <= delta.stream_position
                <= committed_deltas[1].stream_position
            )

    def test_list_partition_deltas_ascending_order(self):
        """Test listing deltas in ascending order."""
        # Create test data
        df = pd.DataFrame(
            {"id": [10, 11, 12], "name": ["Jack", "Kate", "Luke"], "age": [70, 75, 80]}
        )

        # Stage and commit multiple deltas
        committed_deltas = []
        for i in range(3):
            delta = metastore.stage_delta(
                data=df,
                partition=self.partition,
                catalog=self.catalog,
            )
            committed_delta = metastore.commit_delta(delta, catalog=self.catalog)
            committed_deltas.append(committed_delta)

        # Test ascending order (which actually reverses the default order)
        result = metastore.list_partition_deltas(
            partition_like=self.partition,
            ascending_order=True,
            catalog=self.catalog,
        )

        # Should return deltas in ascending stream position order (reversed from default)
        deltas = result.all_items()
        assert isinstance(deltas, list)
        assert len(deltas) >= 3

        # Check that the first 3 deltas are in ascending order
        first_three = deltas[:3]
        for i in range(len(first_three) - 1):
            assert first_three[i].stream_position < first_three[i + 1].stream_position

    def test_list_partition_deltas_with_manifest(self):
        """Test listing deltas with and without manifests."""
        # Create test data
        df = pd.DataFrame(
            {"id": [13, 14, 15], "name": ["Mary", "Nick", "Olive"], "age": [85, 90, 95]}
        )

        # Stage and commit a delta
        delta = metastore.stage_delta(
            data=df,
            partition=self.partition,
            catalog=self.catalog,
        )
        committed_delta = metastore.commit_delta(delta, catalog=self.catalog)

        # Test listing with manifest included
        result = metastore.list_partition_deltas(
            partition_like=self.partition,
            include_manifest=True,
            catalog=self.catalog,
        )

        deltas = result.all_items()
        # Find our delta and verify it has a manifest
        found_delta = next(
            (d for d in deltas if d.stream_position == committed_delta.stream_position),
            None,
        )
        assert found_delta is not None
        assert found_delta.manifest is not None
        assert len(found_delta.manifest.entries) > 0

        # Test listing without manifest (default)
        result_no_manifest = metastore.list_partition_deltas(
            partition_like=self.partition,
            include_manifest=False,
            catalog=self.catalog,
        )

        # Find our delta and verify it doesn't have a manifest by default
        deltas_no_manifest = result_no_manifest.all_items()
        found_delta_no_manifest = next(
            (
                d
                for d in deltas_no_manifest
                if d.stream_position == committed_delta.stream_position
            ),
            None,
        )
        assert found_delta_no_manifest is not None
        # Note: The manifest might still be present since it was loaded during commit


class TestErrorCategorization:
    """Test cases for metastore error categorization functions."""

    def test_can_categorize_deltacat_error(self):
        """Test that can_categorize returns True for DeltaCatError instances."""
        from deltacat.exceptions import DeltaCatError

        # Create a DeltaCatError instance
        error = DeltaCatError("Test DeltaCat error")

        # Test that it can be categorized
        result = metastore.can_categorize(error)
        assert result is True, "DeltaCatError should be categorizable"

    def test_can_categorize_table_not_found_error(self):
        """Test that can_categorize returns True for TableNotFoundError (subclass of DeltaCatError)."""
        # Create a TableNotFoundError instance (which inherits from DeltaCatError)
        error = TableNotFoundError("Test table not found")

        # Test that it can be categorized
        result = metastore.can_categorize(error)
        assert result is True, "TableNotFoundError should be categorizable"

    def test_can_categorize_standard_exception(self):
        """Test that can_categorize returns False for standard Python exceptions."""
        # Create a standard Python exception
        error = ValueError("Test value error")

        # Test that it cannot be categorized
        result = metastore.can_categorize(error)
        assert result is False, "Standard exceptions should not be categorizable"

    def test_can_categorize_runtime_error(self):
        """Test that can_categorize returns False for RuntimeError."""
        # Create a RuntimeError
        error = RuntimeError("Test runtime error")

        # Test that it cannot be categorized
        result = metastore.can_categorize(error)
        assert result is False, "RuntimeError should not be categorizable"

    def test_can_categorize_type_error(self):
        """Test that can_categorize returns False for TypeError."""
        # Create a TypeError
        error = TypeError("Test type error")

        # Test that it cannot be categorized
        result = metastore.can_categorize(error)
        assert result is False, "TypeError should not be categorizable"

    def test_raise_categorized_error_with_deltacat_error(self):
        """Test that raise_categorized_error re-raises DeltaCatError instances."""
        from deltacat.exceptions import DeltaCatError

        # Create a DeltaCatError instance
        original_error = DeltaCatError("Test DeltaCat error")

        # Test that it raises UnclassifiedDeltaCatError (since categorized is always None)
        with pytest.raises(UnclassifiedDeltaCatError) as exc_info:
            metastore.raise_categorized_error(original_error)

        # Verify the error message
        assert "Failed to classify error DeltaCatError" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    def test_raise_categorized_error_with_standard_exception(self):
        """Test that raise_categorized_error raises UnclassifiedDeltaCatError for standard exceptions."""
        from deltacat.exceptions import UnclassifiedDeltaCatError

        # Create a standard Python exception
        original_error = ValueError("Test value error")

        # Test that it raises UnclassifiedDeltaCatError
        with pytest.raises(UnclassifiedDeltaCatError) as exc_info:
            metastore.raise_categorized_error(original_error)

        # Verify the error message contains the original error type and message
        assert "Failed to classify error ValueError" in str(exc_info.value)
        assert "Test value error" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    def test_raise_categorized_error_with_table_not_found_error(self):
        """Test that raise_categorized_error handles TableNotFoundError properly."""
        from deltacat.exceptions import UnclassifiedDeltaCatError

        # Create a TableNotFoundError
        original_error = TableNotFoundError("Table 'test_table' not found")

        # Test that it raises UnclassifiedDeltaCatError
        with pytest.raises(UnclassifiedDeltaCatError) as exc_info:
            metastore.raise_categorized_error(original_error)

        # Verify the error message and chaining
        assert "Failed to classify error TableNotFoundError" in str(exc_info.value)
        assert "Table 'test_table' not found" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    def test_raise_categorized_error_with_runtime_error(self):
        """Test that raise_categorized_error handles RuntimeError properly."""
        from deltacat.exceptions import UnclassifiedDeltaCatError

        # Create a RuntimeError
        original_error = RuntimeError("Something went wrong at runtime")

        # Test that it raises UnclassifiedDeltaCatError
        with pytest.raises(UnclassifiedDeltaCatError) as exc_info:
            metastore.raise_categorized_error(original_error)

        # Verify the error message and chaining
        assert "Failed to classify error RuntimeError" in str(exc_info.value)
        assert "Something went wrong at runtime" in str(exc_info.value)
        assert exc_info.value.__cause__ is original_error

    def test_raise_categorized_error_preserves_exception_chain(self):
        """Test that raise_categorized_error properly preserves the exception chain."""
        from deltacat.exceptions import UnclassifiedDeltaCatError

        # Create a chain of exceptions - use manual chaining instead of 'from'
        root_cause = ValueError("Root cause")
        intermediate_error = RuntimeError("Intermediate error")
        intermediate_error.__cause__ = root_cause

        # Test that raise_categorized_error preserves the chain
        with pytest.raises(UnclassifiedDeltaCatError) as exc_info:
            metastore.raise_categorized_error(intermediate_error)

        # Verify the exception chain is preserved
        assert exc_info.value.__cause__ is intermediate_error
        assert intermediate_error.__cause__ is root_cause

    def test_can_categorize_with_kwargs(self):
        """Test that can_categorize accepts and ignores additional kwargs."""
        from deltacat.exceptions import DeltaCatError

        # Create a DeltaCatError instance
        error = DeltaCatError("Test error")

        # Test with additional kwargs
        result = metastore.can_categorize(error, some_arg="test", another_kwarg=123)
        assert result is True, "can_categorize should handle additional kwargs"

    def test_raise_categorized_error_with_kwargs(self):
        """Test that raise_categorized_error accepts and ignores additional kwargs."""
        from deltacat.exceptions import UnclassifiedDeltaCatError

        # Create a standard exception
        original_error = ValueError("Test error")

        # Test with additional kwargs
        with pytest.raises(UnclassifiedDeltaCatError):
            metastore.raise_categorized_error(
                original_error, some_arg="test", another_kwarg=123
            )
