import unittest
import tempfile
import os

from deltacat.storage import (
    Namespace,
    NamespaceLocator,
    Table,
    TableLocator,
    TableVersionLocator,
    TableVersion,
    StreamLocator,
    StreamFormat,
    Stream,
    PartitionLocator,
    Partition,
    DeltaLocator,
    Delta,
    DeltaType,
)


class TestMetafileDecorator(unittest.TestCase):
    def test_namespace_serde(self):
        temp_dir = tempfile.gettempdir()
        namespace_locator = NamespaceLocator.of("test_namespace")
        namespace = Namespace.of(namespace_locator)
        temp_file_path = f"{temp_dir}/namespace.mpk"
        try:
            namespace.write(temp_file_path)
            deserialized_namespace = Namespace.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert namespace == deserialized_namespace

    def test_table_serde(self):
        temp_dir = tempfile.gettempdir()
        table_locator = TableLocator.at("test_namespace", "test_table")
        table = Table.of(table_locator, "test table description")
        temp_file_path = f"{temp_dir}/table.mpk"
        try:
            table.write(temp_file_path)
            deserialized_table = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert table == deserialized_table

    def test_table_version_serde(self):
        temp_dir = tempfile.gettempdir()
        table_version_locator = TableVersionLocator.at(
            "test_namespace",
            "test_table",
            "test_table_version",
        )
        table_version = TableVersion.of(table_version_locator, schema=None)
        temp_file_path = f"{temp_dir}/table_version.mpk"
        try:
            table_version.write(temp_file_path)
            deserialized_table_version = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert table_version == deserialized_table_version

    def test_stream_serde(self):
        temp_dir = tempfile.gettempdir()
        stream_locator = StreamLocator.at(
            "test_namespace",
            "test_table",
            "test_table_version",
            "test_stream_id",
            StreamFormat.DELTACAT,
        )
        stream = Stream.of(stream_locator, None)
        temp_file_path = f"{temp_dir}/stream.mpk"
        try:
            stream.write(temp_file_path)
            deserialized_stream = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert stream == deserialized_stream

    def test_partition_serde(self):
        temp_dir = tempfile.gettempdir()
        partition_locator = PartitionLocator.at(
            "test_namespace",
            "test_table",
            "test_table_version",
            "test_stream_id",
            StreamFormat.DELTACAT,
            ["a", 1],
            "test_partition_id",
            "test_partition_scheme_id",
        )
        partition = Partition.of(partition_locator, None, None)
        temp_file_path = f"{temp_dir}/partition.mpk"
        try:
            partition.write(temp_file_path)
            deserialized_partition = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert partition == deserialized_partition

    def test_delta_serde(self):
        temp_dir = tempfile.gettempdir()
        delta_locator = DeltaLocator.at(
            "test_namespace",
            "test_table",
            "test_table_version",
            "test_stream_id",
            StreamFormat.DELTACAT,
            ["a", 1],
            "test_partition_id",
            "test_partition_scheme_id",
            1,
        )
        delta = Delta.of(delta_locator, DeltaType.APPEND, None, None, None)
        temp_file_path = f"{temp_dir}/delta.mpk"
        try:
            delta.write(temp_file_path)
            deserialized_delta = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert delta == deserialized_delta

    def test_python_type_serde(self):
        temp_dir = tempfile.gettempdir()
        namespace_locator = NamespaceLocator.of("test_namespace")
        table_locator = TableLocator.of(namespace_locator, "test_table")
        # test basic python types except set, frozenset, and range which
        # msgpack can't serialize
        properties = {
            "foo": 1,
            "bar": 2.0,
            "baz": True,
            "qux": b"123",
            "quux": None,
            "corge": [1, 2, 3],
            "grault": {"foo": "bar"},
            "garply": (1, 2, 3),
            "waldo": bytearray(3),
            "fred": memoryview(bytes(4)),
        }

        table = Table.of(table_locator, "test table description", properties)
        temp_file_path = f"{temp_dir}/table.mpk"
        try:
            table.write(temp_file_path)
            deserialized_table = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        # exchange original table properties with the expected table properties
        expected_properties = properties.copy()
        # msgpack will tranlate tuples to lists
        expected_properties["garply"] = [1, 2, 3]
        # msgpack will expand bytearray and memoryview to the actual bytes
        expected_properties["waldo"] = b"\x00\x00\x00"
        expected_properties["fred"] = b"\x00\x00\x00\x00"
        table.properties = expected_properties
        assert table == deserialized_table
