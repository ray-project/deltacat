import shutil
import tempfile

import pytest
import copy

from deltacat.storage import (
    metastore,
    CommitState,
    LifecycleState,
    Metafile,
    Namespace,
    NamespaceLocator,
    TableVersion,
    TableVersionLocator,
    Stream,
    StreamFormat,
)
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
    create_test_table,
    create_test_table_version,
)
from deltacat.catalog.main.impl import PropertyCatalog
from deltacat.storage.main.impl import DEFAULT_TABLE_VERSION


class TestNamespace:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
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


class TestTable:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
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


class TestTableVersion:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)

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
        table_version_default_id: TableVersion = Metafile.update_for(self.table_version)
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
        active_table_version: TableVersion = Metafile.update_for(self.table_version)
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
        active_table_version2: TableVersion = Metafile.update_for(self.table_version2)
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
            # expect an error to be raised
            with pytest.raises(ValueError):
                metastore.get_table_version(
                    table_version=self.table_version.table_version,
                    catalog=self.catalog,
                    **kwargs_copy,
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
            # expect an error to be raised
            with pytest.raises(ValueError):
                metastore.table_version_exists(
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


class TestStream:
    @classmethod
    def setup_method(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
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
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.stream_exists(
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
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.get_stream(
                    catalog=self.catalog,
                    **kwargs_copy,
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
        # no partitions yet
        list_result = metastore.list_stream_partitions(
            self.default_stream,
            catalog=self.catalog,
        )
        partitions = list_result.all_items()
        assert len(partitions) == 0

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
        # This will list the default stream and the newly committed stream
        for stream in streams:
            if stream.id == committed_stream.id:
                assert stream.equivalent_to(committed_stream)
            else:
                deprecated_default_stream: Stream = Metafile.update_for(
                    self.default_stream
                )
                deprecated_default_stream.state = CommitState.DEPRECATED
                assert stream.equivalent_to(deprecated_default_stream)
        assert len(streams) == 2
