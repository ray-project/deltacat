import shutil
import tempfile

import pytest

from deltacat import Schema, Field
from deltacat.storage import (
    metastore,
    Namespace,
    NamespaceLocator,
    TableVersion,
    StreamFormat,
)
from deltacat.storage.model.table import Table
from deltacat.catalog.main.impl import PropertyCatalog
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
        assert metastore.namespace_exists("namespace1", catalog=self.catalog)

    def test_namespace_exists_nonexisting(self):
        assert not metastore.namespace_exists("foobar", catalog=self.catalog)


class TestTable:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
        # Create a namespace to hold our tables
        cls.namespace_obj = metastore.create_namespace(
            namespace="test_table_ns",
            catalog=cls.catalog,
        )
        # Create two tables
        cls.table1 = metastore.create_table_version(
            namespace="test_table_ns",
            table_name="table1",
            table_version="v1",
            catalog=cls.catalog,
        )
        cls.table2 = metastore.create_table_version(
            namespace="test_table_ns",
            table_name="table2",
            table_version="v1",
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_tables(self):
        # list the tables under our namespace
        list_result = metastore.list_tables("test_table_ns", catalog=self.catalog)
        all_tables = list_result.all_items()

        # we expect 2 distinct tables
        table_names = {t.table_name for t in all_tables if isinstance(t, Table)}
        assert "table1" in table_names
        assert "table2" in table_names

    def test_get_table(self):
        # test we can retrieve table1 by name
        tbl = metastore.get_table("test_table_ns", "table1", catalog=self.catalog)
        assert tbl is not None
        # In your architecture, you might check equivalence:
        # e.g. tbl.equivalent_to(...) if you have that method:
        assert tbl.table_name == "table1"

    def test_table_exists_existing(self):
        # table1 should exist
        assert metastore.table_exists("test_table_ns", "table1", catalog=self.catalog)

    def test_table_exists_nonexisting(self):
        assert not metastore.table_exists(
            "test_table_ns", "no_such_table", catalog=self.catalog
        )


class TestTableVersion:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
        # Create a namespace and single table
        cls.namespace_obj = metastore.create_namespace(
            namespace="test_tv_ns",
            catalog=cls.catalog,
        )
        # Create a "base" table to attach versions to
        metastore.create_table_version(
            namespace="test_tv_ns",
            table_name="mytable",
            table_version="v1",
            catalog=cls.catalog,
        )
        # Now create an additional version
        cls.stream2 = metastore.create_table_version(
            namespace="test_tv_ns",
            table_name="mytable",
            table_version="v2",
            catalog=cls.catalog,
        )

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_table_versions(self):
        list_result = metastore.list_table_versions(
            "test_tv_ns",
            "mytable",
            catalog=self.catalog,
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
            "test_tv_ns", "mytable", "v1", catalog=self.catalog
        )
        assert tv is not None
        assert tv.table_version == "v1"

    def test_table_version_exists(self):
        # v2 should exist
        assert metastore.table_version_exists(
            "test_tv_ns", "mytable", "v2", catalog=self.catalog
        )

    def test_creation_fails_if_already_exists(self):
        # Assert that creating the same table version again raises a ValueError
        with pytest.raises(ValueError):
            metastore.create_table_version(
                namespace="test_tv_ns",
                table_name="mytable",
                table_version="v1",
                catalog=self.catalog,
            )


class TestStream:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
        # Create a table version for streams.
        # This call should automatically create a default stream (of format "deltacat").
        metastore.create_namespace("test_stream_ns", catalog=cls.catalog)
        metastore.create_table_version(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=cls.catalog,
        )
        # Retrieve the auto-created default stream.
        cls.default_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=cls.catalog,
        )
        # Ensure that the default stream was auto-created.
        assert cls.default_stream is not None, "Default stream was not auto-created."

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_streams(self):
        list_result = metastore.list_streams(
            "test_stream_ns", "mystreamtable", "v1", catalog=self.catalog
        )
        streams = list_result.all_items()
        # We expect exactly one stream (the default "deltacat" stream).
        assert len(streams) == 1

    def test_get_stream(self):
        stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=self.catalog,
        )
        assert stream is not None
        # The stream's format should be the default "deltacat"
        assert stream.stream_format.lower() == StreamFormat.DELTACAT.value.lower()

    def test_create_stream_singleton_constraint(self):
        # Attempting to create another committed stream for the same table/version
        # should fail since a default stream already exists.
        with pytest.raises(
            ValueError,
            match="A stream of format deltacat already exists on table version v1",
        ):
            metastore.create_stream(
                namespace="test_stream_ns",
                table_name="mystreamtable",
                table_version="v1",
                catalog=self.catalog,
            )

    def test_stage_and_commit_stream_replacement(self):
        """
        TODO current this raises an exception because the staged stream's locator mapping file already exists

        The only way to write a new locator mapping is to have an UPDATE transaction. But stage should not use UPDATE
        because it is creating a brand new metafile with a new immutable id.

        """
        original_stream = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=self.catalog,
        )

        staged_stream = metastore.stage_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            stream_format=StreamFormat.DELTACAT,
            catalog=self.catalog,
        )

        fetch_after_staging = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=self.catalog,
        )

        """
        TODO code needs to be updated so that locator_to_id will ignore latest revisions which have been staged

        then change below long to assert fetch_after_staging.id == stream.id
        """
        assert fetch_after_staging.id == staged_stream.id
        assert fetch_after_staging.id != original_stream.id
