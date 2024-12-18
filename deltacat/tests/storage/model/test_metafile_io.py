import unittest
import tempfile
import os

from deltacat import Schema, Field
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


class TestMetafileIO(unittest.TestCase):
    def test_namespace_serde(self):
        temp_dir = tempfile.gettempdir()
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        try:
            temp_file_path = namespace.write(temp_dir)
            deserialized_namespace = Namespace.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert namespace == deserialized_namespace

    def test_table_serde(self):
        temp_dir = tempfile.gettempdir()
        table_locator = TableLocator.at(
            namespace="test_namespace",
            table_name="test_table",
        )
        table = Table.of(
            locator=table_locator,
            description="test table description",
        )
        try:
            temp_file_path = table.write(temp_dir)
            deserialized_table = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert table == deserialized_table

    def test_table_version_serde(self):
        temp_dir = tempfile.gettempdir()
        table_version_locator = TableVersionLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
        )
        import pyarrow as pa

        schema = Schema.of(
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
        table_version = TableVersion.of(
            locator=table_version_locator,
            schema=schema,
        )
        try:
            temp_file_path = table_version.write(temp_dir)
            deserialized_table_version = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert table_version == deserialized_table_version

    def test_stream_serde(self):
        temp_dir = tempfile.gettempdir()
        stream_locator = StreamLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
        )
        stream = Stream.of(
            locator=stream_locator,
            partition_scheme=None,
        )
        try:
            temp_file_path = stream.write(temp_dir)
            deserialized_stream = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert stream == deserialized_stream

    def test_partition_serde(self):
        temp_dir = tempfile.gettempdir()
        partition_locator = PartitionLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
            partition_values=["a", 1],
            partition_id="test_partition_id",
            partition_scheme_id="test_partition_scheme_id",
        )
        partition = Partition.of(partition_locator, None, None)
        try:
            temp_file_path = partition.write(temp_dir)
            deserialized_partition = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert partition == deserialized_partition

    def test_delta_serde(self):
        temp_dir = tempfile.gettempdir()
        delta_locator = DeltaLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
            partition_values=["a", 1],
            partition_id="test_partition_id",
            partition_scheme_id="test_partition_scheme_id",
            stream_position=1,
        )
        delta = Delta.of(
            locator=delta_locator,
            delta_type=DeltaType.APPEND,
            meta=None,
            properties=None,
            manifest=None,
        )
        try:
            temp_file_path = delta.write(temp_dir)
            deserialized_delta = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        assert delta == deserialized_delta

    def test_python_type_serde(self):
        temp_dir = tempfile.gettempdir()
        namespace_locator = NamespaceLocator.of("test_namespace")
        table_locator = TableLocator.of(namespace_locator, "test_table")
        # test basic python types
        # (set, frozenset, and range can't be serialized by msgpack)
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
        table = Table.of(
            locator=table_locator,
            description="test table description",
            properties=properties,
        )
        try:
            temp_file_path = table.write(temp_dir)
            deserialized_table = Table.read(temp_file_path)
        finally:
            os.remove(temp_file_path)
        # exchange original table properties with the expected table properties
        expected_properties = properties.copy()
        # msgpack tranlates tuples to lists
        expected_properties["garply"] = [1, 2, 3]
        # msgpack unpacks bytearray and memoryview into bytes
        expected_properties["waldo"] = b"\x00\x00\x00"
        expected_properties["fred"] = b"\x00\x00\x00\x00"
        table.properties = expected_properties
        assert table == deserialized_table
