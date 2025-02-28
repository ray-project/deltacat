import shutil
import tempfile

import pytest

from deltacat import Schema, Field
from deltacat.storage import (
    metastore,
    Namespace,
    NamespaceLocator,
    Table,
    TableVersion,
    TableVersionLocator,
    StreamFormat,
)
from deltacat.catalog import CatalogProperties
import pyarrow as pa


@pytest.fixture
def schema():
    return Schema.of(
        [
            Field.of(
                field=pa.field("some_string", pa.string(), nullable=False),
                field_id=1,
                is_merge_key=True,
            ),
            Field.of(
                field=pa.field("some_int32", pa.int32(), nullable=False),
                field_id=2,
                is_merge_key=True,
            ),
            Field.of(
                field=pa.field("some_float64", pa.float64()),
                field_id=3,
                is_merge_key=False,
            ),
        ]
    )


class TestNamespace:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(cls.tmpdir)
        cls.namespace1 = metastore.create_namespace(
            namespace="namespace1",
            catalog_properties=cls.catalog,
        )
        cls.namespace2 = metastore.create_namespace(
            namespace="namespace2",
            catalog_properties=cls.catalog,
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_namespaces(self):
        # expect the namespace returned to match the input namespace to create
        namespace_locator = NamespaceLocator.of(namespace="namespace1")
        expected_namespace = Namespace.of(locator=namespace_locator)
        assert expected_namespace.equivalent_to(self.namespace1)

        # expect the namespace to exist
        assert metastore.namespace_exists(
            namespace="namespace1",
            catalog_properties=self.catalog,
        )

        # expect the namespace to also be returned when listing namespaces
        list_result = metastore.list_namespaces(catalog_properties=self.catalog)
        namespaces_by_name = {n.locator.namespace: n for n in list_result.all_items()}
        assert len(namespaces_by_name.items()) == 2
        assert namespaces_by_name["namespace1"].equivalent_to(self.namespace1)
        assert namespaces_by_name["namespace2"].equivalent_to(self.namespace2)

    def test_get_namespace(self):
        # expect the namespace to also be returned when explicitly retrieved
        read_namespace = metastore.get_namespace(
            namespace="namespace1",
            catalog_properties=self.catalog,
        )
        assert read_namespace and read_namespace.equivalent_to(self.namespace1)

    def test_namespace_exists_existing(self):
        assert metastore.namespace_exists(
            "namespace1",
            catalog_properties=self.catalog,
        )

    def test_namespace_exists_nonexisting(self):
        assert not metastore.namespace_exists(
            "foobar",
            catalog_properties=self.catalog,
        )


class TestTable:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(cls.tmpdir)
        # Create a namespace to hold our tables
        cls.namespace_obj = metastore.create_namespace(
            namespace="test_table_ns",
            catalog_properties=cls.catalog,
        )
        # Create two tables
        cls.stream1 = metastore.create_table_version(
            namespace="test_table_ns",
            table_name="table1",
            table_version="v1",
            catalog_properties=cls.catalog,
        )
        cls.stream2 = metastore.create_table_version(
            namespace="test_table_ns",
            table_name="table2",
            table_version="v1",
            catalog_properties=cls.catalog,
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_tables(self):
        # list the tables under our namespace
        list_result = metastore.list_tables(
            "test_table_ns",
            catalog_properties=self.catalog,
        )
        all_tables = list_result.all_items()

        # we expect 2 distinct tables
        table_names = {t.table_name for t in all_tables if isinstance(t, Table)}
        assert "table1" in table_names
        assert "table2" in table_names

    def test_get_table(self):
        # test we can retrieve table1 by name
        tbl = metastore.get_table(
            "test_table_ns",
            "table1",
            catalog_properties=self.catalog,
        )
        assert tbl is not None
        # TODO(pdames): replace with tbl.equivalent_to(expected)
        assert tbl.table_name == "table1"

    def test_table_exists_existing(self):
        # table1 should exist
        assert metastore.table_exists(
            "test_table_ns",
            "table1",
            catalog_properties=self.catalog,
        )

    def test_table_exists_nonexisting(self):
        assert not metastore.table_exists(
            "test_table_ns",
            "no_such_table",
            catalog_properties=self.catalog,
        )


class TestTableVersion:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(cls.tmpdir)
        # Create a namespace and single table
        cls.namespace_obj = metastore.create_namespace(
            namespace="test_tv_ns",
            catalog_properties=cls.catalog,
        )
        # Create a "base" table to attach versions to
        metastore.create_table_version(
            namespace="test_tv_ns",
            table_name="mytable",
            table_version="v1",
            catalog_properties=cls.catalog,
        )
        # Now create an additional version
        cls.stream2 = metastore.create_table_version(
            namespace="test_tv_ns",
            table_name="mytable",
            table_version="v2",
            catalog_properties=cls.catalog,
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_table_versions(self):
        list_result = metastore.list_table_versions(
            "test_tv_ns",
            "mytable",
            catalog_properties=self.catalog,
        )
        tvs = list_result.all_items()
        # we expect v1 and v2
        version_ids = []
        for tv in tvs:
            if isinstance(tv, TableVersion):
                version_ids.append(tv.table_version)
        assert set(version_ids) == {"v1", "v2"}

    def test_get_table_version(self):
        tv = metastore.get_table_version(
            "test_tv_ns",
            "mytable",
            "v1",
            catalog_properties=self.catalog,
        )
        assert tv is not None
        assert tv.table_version == "v1"

    def test_table_version_exists(self):
        # v2 should exist
        assert metastore.table_version_exists(
            "test_tv_ns",
            "mytable",
            "v2",
            catalog_properties=self.catalog,
        )

    def test_table_version_exists_nonexisting(self):
        # "v999" should not exist
        assert not metastore.table_version_exists(
            "test_tv_ns",
            "mytable",
            "v999",
            catalog_properties=self.catalog,
        )

    def test_creation_fails_if_already_exists(self):
        # Assert that creating the same table version again raises a ValueError
        with pytest.raises(ValueError):
            metastore.create_table_version(
                namespace="test_tv_ns",
                table_name="mytable",
                table_version="v1",
                catalog_properties=self.catalog,
            )


class TestStream:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = CatalogProperties(cls.tmpdir)
        metastore.create_namespace(
            "test_stream_ns",
            catalog_properties=cls.catalog,
        )
        # Create a table version.
        # This call should automatically create a default DeltaCAT stream.
        metastore.create_table_version(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog_properties=cls.catalog,
        )
        # Retrieve the auto-created default stream.
        cls.default_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog_properties=cls.catalog,
        )
        # Ensure that the default stream was auto-created.
        assert cls.default_stream is not None, "Default stream not found."

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_streams(self):
        list_result = metastore.list_streams(
            "test_stream_ns", "mystreamtable", "v1", catalog_properties=self.catalog
        )
        streams = list_result.all_items()
        # We expect exactly one stream (the default "deltacat" stream).
        assert len(streams) == 1

    def test_get_stream(self):
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog_properties=self.catalog,
        )
        assert stream is not None
        # The stream's format should be the default "deltacat"
        assert stream.stream_format.lower() == StreamFormat.DELTACAT.value.lower()

    def test_list_stream_partitions_empty(self):
        # no partitions yet
        list_result = metastore.list_stream_partitions(
            self.default_stream,
            catalog_properties=self.catalog,
        )
        partitions = list_result.all_items()
        assert len(partitions) == 0

    def test_delete_stream(self):
        # We can delete the stream
        metastore.delete_stream(
            "test_stream_ns",
            "mystreamtable",
            "v1",
            catalog_properties=self.catalog,
        )
        # Now get_stream should return None
        stream = metastore.get_stream(
            "test_stream_ns",
            "mystreamtable",
            "v1",
            catalog_properties=self.catalog,
        )
        assert stream is None

    def test_stage_and_commit_stream_replacement(self):
        # Stage & commit a stream
        stream = metastore.stage_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog_properties=self.catalog,
        )
        fetched_stream = metastore.get_staged_stream(
            table_version_locator=TableVersionLocator.at(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v1",
            ),
            stream_id=stream.stream_id,
            catalog_properties=self.catalog,
        )
        assert fetched_stream.equivalent_to(stream)
        committed_stream = metastore.commit_stream(
            stream=stream,
            catalog_properties=self.catalog,
        )
        fetched_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog_properties=self.catalog,
        )
        assert fetched_stream.equivalent_to(committed_stream)
        list_result = metastore.list_streams(
            "test_stream_ns",
            "mystreamtable",
            "v1",
            catalog_properties=self.catalog,
        )
        streams = list_result.all_items()
        # This will list the staged stream and the committed stream
        assert len(streams) == 2
