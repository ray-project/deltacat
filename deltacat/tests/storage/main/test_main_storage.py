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
        cls.stream1 = metastore.create_table_version(
            namespace=cls.test_table1.namespace,
            table_name=cls.test_table1.table_name,
            table_version=cls.test_table1.latest_table_version,
            table_description=cls.test_table1.description,
            table_properties=cls.test_table1.properties,
            catalog=cls.catalog,
        )
        cls.stream2 = metastore.create_table_version(
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

        cls.namespace = create_test_namespace()
        cls.table = create_test_table()
        cls.table_version = create_test_table_version()
        cls.table_version2 = create_test_table_version()
        cls.table_version2.previous_table_version = cls.table_version.table_version
        cls.table_version2.locator.table_version = "v.2"

        # Create a namespace and single table
        cls.namespace_obj = metastore.create_namespace(
            namespace=cls.namespace.namespace,
            catalog=cls.catalog,
        )

        # Create a "base" table to attach versions to
        cls.stream1 = metastore.create_table_version(
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
        # Now create an additional version
        cls.stream2 = metastore.create_table_version(
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

    @classmethod
    def teardown_method(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_table_versions(self):
        list_result = metastore.list_table_versions(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
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
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.list_table_versions(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_get_latest_table_version(self):
        tv = metastore.get_latest_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        assert tv.equivalent_to(self.table_version2)

    def test_get_latest_table_version_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.get_latest_table_version(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_get_latest_active_table_version(self):
        tv = metastore.get_latest_active_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        assert tv is None
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )
        tv = metastore.get_latest_active_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
        active_table_version: TableVersion = Metafile.update_for(self.table_version)
        active_table_version.state = LifecycleState.ACTIVE
        assert tv.equivalent_to(active_table_version)
        metastore.update_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version2.table_version,
            lifecycle_state=LifecycleState.ACTIVE,
            catalog=self.catalog,
        )
        tv = metastore.get_latest_active_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            catalog=self.catalog,
        )
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
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.get_latest_active_table_version(
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_get_table_version(self):
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )
        assert tv.equivalent_to(self.table_version)

    def test_get_table_version_not_exists(self):
        tv = metastore.get_table_version(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version="v.999",
            catalog=self.catalog,
        )
        assert tv is None

    def test_get_table_version_bad_parent_locator(self):
        kwargs = {
            "namespace": self.table.namespace,
            "table_name": self.table.table_name,
        }
        for key in kwargs.keys():
            kwargs_copy = copy.copy(kwargs)
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.get_table_version(
                    table_version=self.table_version.table_version,
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_table_version_exists(self):
        # v.2 should exist
        assert metastore.table_version_exists(
            namespace=self.table.namespace,
            table_name=self.table.table_name,
            table_version=self.table_version.table_version,
            catalog=self.catalog,
        )

    def test_table_version_not_exists(self):
        # "v.999" should not exist
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
            kwargs_copy[key] = "i_dont_exist"
            with pytest.raises(ValueError):
                metastore.table_version_exists(
                    table_version=self.table_version.table_version,
                    catalog=self.catalog,
                    **kwargs_copy,
                )

    def test_creation_fails_if_already_exists(self):
        # Assert that creating the same table version again raises a ValueError
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
        metastore.create_table_version(
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
