import shutil
import tempfile

import pytest

from deltacat import Schema, Field
from deltacat.storage import (
    metastore,
    Namespace,
    NamespaceLocator, TableVersion,
)
from deltacat.storage.main import impl as storage_impl
from deltacat.storage.model.namespace import Namespace
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
        assert not metastore.table_exists("test_table_ns", "no_such_table", catalog=self.catalog)

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

    def test_table_version_exists_nonexisting(self):
        # "v999" should not exist
        assert not metastore.table_version_exists(
            "test_tv_ns", "mytable", "v999", catalog=self.catalog
        )

class TestStream:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
        # Create a table version
        metastore.create_namespace("test_stream_ns", catalog=cls.catalog)
        metastore.create_table_version(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=cls.catalog,
        )
        # Stage & commit a stream
        cls.stream = metastore.stage_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=cls.catalog,
        )
        cls.committed_stream = metastore.commit_stream(cls.stream, catalog=cls.catalog)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_get_stream(self):
        # The stream is created and committed in setup
        strm = metastore.get_stream(
            namespace="test_stream_ns",
            table_name="mystreamtable",
            table_version="v1",
            catalog=self.catalog,
        )
        assert strm is not None

    def test_list_stream_partitions_empty(self):
        # no partitions yet
        list_result = metastore.list_stream_partitions(self.committed_stream, catalog=self.catalog)
        partitions = list_result.all_items()
        assert len(partitions) == 0

    def test_delete_stream(self):
        # We can delete the stream
        metastore.delete_stream("test_stream_ns", "mystreamtable", "v1", catalog=self.catalog)
        # Now get_stream should return None
        stream = metastore.get_stream("test_stream_ns", "mystreamtable", "v1", catalog=self.catalog)
        assert stream is None

class TestPartition:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)
        # Create table version & stream
        metastore.create_namespace("test_partition_ns", catalog=cls.catalog)
        metastore.create_table_version(
            namespace="test_partition_ns",
            table_name="pt_table",
            table_version="v1",
            catalog=cls.catalog,
        )
        cls.stream = metastore.get_stream("test_partition_ns", "pt_table", "v1", catalog=cls.catalog)
        # Stage & commit partition
        cls.part_staged = metastore.stage_partition(cls.stream, ["USA"], catalog=cls.catalog)
        cls.part_committed = metastore.commit_partition(cls.part_staged, catalog=cls.catalog)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_get_partition(self):
        part = metastore.get_partition(self.stream.locator, ["USA"], catalog=self.catalog)
        assert part is not None
        # you could do part.equivalent_to(...) if your code supports that

    def test_list_partitions(self):
        list_result = metastore.list_partitions("test_partition_ns", "pt_table", "v1", catalog=self.catalog)
        all_parts = list_result.all_items()
        assert len(all_parts) == 1
        # Check we have that same partition
        p = all_parts[0]
        assert p.partition_values == ["USA"]

    def test_delete_partition(self):
        # confirm we can delete it
        metastore.delete_partition("test_partition_ns", "pt_table", "v1", ["USA"], catalog=self.catalog)
        # now listing partitions should yield none
        list_result = metastore.list_partitions("test_partition_ns", "pt_table", "v1", catalog=self.catalog)
        assert len(list_result.all_items()) == 0

class TestDelta:
    @classmethod
    def setup_class(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.catalog = PropertyCatalog(cls.tmpdir)

        # Create table version & stream & partition
        metastore.create_namespace("test_delta_ns", catalog=cls.catalog)
        metastore.create_table_version(
            namespace="test_delta_ns",
            table_name="delta_table",
            table_version="v1",
            catalog=cls.catalog,
        )
        cls.stream = metastore.get_stream("test_delta_ns", "delta_table", "v1", catalog=cls.catalog)
        cls.partition = metastore.stage_partition(cls.stream, ["CANADA"], catalog=cls.catalog)
        cls.partition = metastore.commit_partition(cls.partition, catalog=cls.catalog)

        # Stage & commit a delta
        cls.delta_staged = metastore.stage_delta(
            data=[],
            partition=cls.partition,
            catalog=cls.catalog,
        )
        cls.delta_committed = metastore.commit_delta(cls.delta_staged, catalog=cls.catalog)

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.tmpdir)

    def test_list_deltas(self):
        lr = metastore.list_deltas("test_delta_ns", "delta_table", ["CANADA"], "v1", catalog=self.catalog)
        all_deltas = lr.all_items()
        assert len(all_deltas) == 1
        # e.g. check the stream_position is 999 if your code does that
        assert all_deltas[0].stream_position is not None

    def test_get_delta(self):
        # your code might set stream_position=999 or something else
        # we'll fetch the "latest delta" first
        d_latest = metastore.get_latest_delta(
            "test_delta_ns", "delta_table", ["CANADA"], "v1", catalog=self.catalog
        )
        assert d_latest is not None
        # now explicitly get by that position
        d_exact = metastore.get_delta(
            "test_delta_ns", "delta_table",
            d_latest.stream_position,
            ["CANADA"], "v1",
            catalog=self.catalog
        )
        assert d_exact is not None

    def test_download_delta(self):
        # Just ensures it doesn't crash with the stubs
        downloaded = metastore.download_delta(
            self.delta_committed,
            catalog=self.catalog
        )
        # by default might be empty list or None
        assert downloaded is not None

    #
    # def test_create_namespace_and_tables(self, temp_catalog):
    #     # 1) Create a namespace
    #     ns_name = "test_ns"
    #     ns = storage_impl.create_namespace(
    #         namespace=ns_name,
    #         properties={"owner": "deltacat"},
    #         catalog=temp_catalog,
    #     )
    #     assert isinstance(ns, Namespace)
    #     assert ns.namespace == ns_name
    #
    #     # 2) Confirm the namespace exists
    #     assert storage_impl.namespace_exists(ns_name, catalog=temp_catalog) is True
    #
    #     # 3) Create two tables in that namespace
    #     tbl_name_1 = "mytable1"
    #     tbl_name_2 = "mytable2"
    #     storage_impl.create_table_version(
    #         namespace=ns_name,
    #         table_name=tbl_name_1,
    #         table_version="v1",
    #         catalog=temp_catalog,
    #     )
    #     storage_impl.create_table_version(
    #         namespace=ns_name,
    #         table_name=tbl_name_2,
    #         table_version="v1",
    #         catalog=temp_catalog,
    #     )
    #
    #     # 4) list tables
    #     tables_result = storage_impl.list_tables(ns_name, catalog=temp_catalog)
    #     all_tables = tables_result.all_items()
    #     assert len(all_tables) == 2, f"Expected 2 tables, got {len(all_tables)}"
    #     table_names = sorted([t.table_name for t in all_tables])
    #     assert table_names == [tbl_name_1, tbl_name_2]
    #
    # def test_create_partition_and_delta(self, temp_catalog):
    #     ns_name = "delta_ns"
    #     storage_impl.create_namespace(ns_name, catalog=temp_catalog)
    #     storage_impl.create_table_version(
    #         namespace=ns_name,
    #         table_name="dt_table",
    #         table_version="v1",
    #         catalog=temp_catalog,
    #     )
    #     # get the stream
    #     stream = storage_impl.get_stream(ns_name, "dt_table", "v1", catalog=temp_catalog)
    #     assert stream is not None
    #
    #     # stage & commit a partition
    #     part = storage_impl.stage_partition(stream, partition_values=["USA"], catalog=temp_catalog)
    #     committed_part = storage_impl.commit_partition(part, catalog=temp_catalog)
    #     assert committed_part.partition_values == ["USA"]
    #
    #     # stage a delta
    #     delta = storage_impl.stage_delta(
    #         data=[],
    #         partition=committed_part,
    #         catalog=temp_catalog,
    #     )
    #     # commit
    #     committed_delta = storage_impl.commit_delta(delta, catalog=temp_catalog)
    #     assert committed_delta.stream_position == 999  # from the reference code
    #
    #     # list deltas
    #     listed = storage_impl.list_partition_deltas(part, catalog=temp_catalog)
    #     all_deltas = listed.all_items()
    #     assert len(all_deltas) == 1
    #     # get latest delta
    #     latest = storage_impl.get_latest_delta(ns_name, "dt_table", ["USA"], "v1", catalog=temp_catalog)
    #     assert latest is not None
    #     assert latest.stream_position == 999