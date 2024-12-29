import shutil
import unittest
import tempfile
import os
import uuid

import pyarrow as pa

from deltacat import (
    Schema,
    Field,
    PartitionScheme,
    PartitionKey,
    ContentEncoding,
    ContentType,
    SortScheme,
    SortKey,
    SortOrder,
    NullOrder,
    LifecycleState,
)
from deltacat.storage import (
    BucketTransform,
    BucketTransformParameters,
    BucketingStrategy,
    CommitState,
    DeltaLocator,
    Delta,
    DeltaType,
    EntryParams,
    EntryType,
    Manifest,
    ManifestAuthor,
    ManifestEntry,
    ManifestMeta,
    Namespace,
    NamespaceLocator,
    PartitionLocator,
    Partition,
    StreamLocator,
    StreamFormat,
    Stream,
    Table,
    TableLocator,
    TableVersionLocator,
    TableVersion,
    Transaction,
    TransactionType,
    TransactionOperation,
    TransactionOperationType,
    TruncateTransform,
    TruncateTransformParameters,
)
from deltacat.storage.model.metafile import TXN_DIR_NAME


class TestMetafileIO(unittest.TestCase):
    def test_e2e_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))

        # namespace
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)

        # table
        table_locator = TableLocator.at(
            namespace="test_namespace",
            table_name="test_table",
        )
        table = Table.of(
            locator=table_locator,
            description="test table description",
        )

        # stream
        table_version_locator = TableVersionLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
        )
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
        bucket_transform = BucketTransform.of(
            BucketTransformParameters.of(
                num_buckets=2,
                bucketing_strategy=BucketingStrategy.DEFAULT,
            )
        )
        partition_keys = [
            PartitionKey.of(
                key=["some_string", "some_int32"],
                name="test_partition_key",
                field_id="test_field_id",
                transform=bucket_transform,
            )
        ]
        partition_scheme = PartitionScheme.of(
            keys=partition_keys,
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )
        sort_keys = [
            SortKey.of(
                key=["some_int32"],
                sort_order=SortOrder.DESCENDING,
                null_order=NullOrder.AT_START,
                transform=TruncateTransform.of(
                    TruncateTransformParameters.of(width=3),
                ),
            )
        ]
        sort_scheme = SortScheme.of(
            keys=sort_keys,
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )
        table_version = TableVersion.of(
            locator=table_version_locator,
            schema=schema,
            partition_scheme=partition_scheme,
            description="test table version description",
            properties={"test_property_key": "test_property_value"},
            content_types=[ContentType.PARQUET],
            sort_scheme=sort_scheme,
            watermark=1,
            lifecycle_state=LifecycleState.CREATED,
            schemas=[schema, schema, schema],
            partition_schemes=[partition_scheme, partition_scheme],
            sort_schemes=[sort_scheme, sort_scheme],
        )

        stream_locator = StreamLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
        )
        bucket_transform = BucketTransform.of(
            BucketTransformParameters.of(
                num_buckets=2,
                bucketing_strategy=BucketingStrategy.DEFAULT,
            )
        )
        partition_keys = [
            PartitionKey.of(
                key=["some_string", "some_int32"],
                name="test_partition_key",
                field_id="test_field_id",
                transform=bucket_transform,
            )
        ]
        partition_scheme = PartitionScheme.of(
            keys=partition_keys,
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )
        stream = Stream.of(
            locator=stream_locator,
            partition_scheme=partition_scheme,
            state=CommitState.STAGED,
            previous_stream_id="test_previous_stream_id",
            watermark=1,
        )

        # partition
        partition_locator = PartitionLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
            partition_values=["a", 1],
            partition_id="test_partition_id",
        )
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
        partition = Partition.of(
            locator=partition_locator,
            schema=schema,
            content_types=[ContentType.PARQUET],
            state=CommitState.STAGED,
            previous_stream_position=0,
            previous_partition_id="test_previous_partition_id",
            stream_position=1,
            next_partition_id="test_next_partition_id",
            partition_scheme_id="test_partition_scheme_id",
        )

        # delta
        delta_locator = DeltaLocator.at(
            namespace="test_namespace",
            table_name="test_table",
            table_version="test_table_version",
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
            partition_values=["a", 1],
            partition_id="test_partition_id",
            stream_position=1,
        )
        manifest_entry_params = EntryParams.of(
            equality_field_locators=["some_string", "some_int32"],
        )
        manifest_meta = ManifestMeta.of(
            record_count=1,
            content_length=10,
            content_type=ContentType.PARQUET.value,
            content_encoding=ContentEncoding.IDENTITY.value,
            source_content_length=100,
            credentials={"foo": "bar"},
            content_type_parameters=[{"param1": "value1"}],
            entry_type=EntryType.EQUALITY_DELETE,
            entry_params=manifest_entry_params,
        )
        manifest = Manifest.of(
            entries=[
                ManifestEntry.of(
                    url="s3://test/url",
                    meta=manifest_meta,
                )
            ],
            author=ManifestAuthor.of(
                name="deltacat",
                version="2.0",
            ),
            entry_type=EntryType.EQUALITY_DELETE,
            entry_params=manifest_entry_params,
        )
        delta = Delta.of(
            locator=delta_locator,
            delta_type=DeltaType.APPEND,
            meta=manifest_meta,
            properties={"property1": "value1"},
            manifest=manifest,
            previous_stream_position=0,
        )
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    namespace,
                ),
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    table,
                ),
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    table_version,
                ),
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    stream,
                ),
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    partition,
                ),
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    delta,
                ),
            ],
        )
        try:
            write_paths = transaction.commit(temp_dir)
            deserialized_delta = Delta.read(write_paths.pop())
            deserialized_partition = Partition.read(write_paths.pop())
            deserialized_stream = Stream.read(write_paths.pop())
            deserialized_table_version = TableVersion.read(write_paths.pop())
            deserialized_table = Table.read(write_paths.pop())
            deserialized_namespace = Namespace.read(write_paths.pop())
            assert os.path.exists(os.path.join(temp_dir, TXN_DIR_NAME, transaction.id))
        finally:
            shutil.rmtree(temp_dir)
        assert delta == deserialized_delta
        assert partition == deserialized_partition
        assert stream == deserialized_stream
        assert table_version == deserialized_table_version
        assert table == deserialized_table
        assert namespace == deserialized_namespace

    def test_namespace_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        namespace,
                    )
                ],
            ).commit(temp_dir)
            deserialized_namespace = Namespace.read(write_paths.pop())
            assert namespace == deserialized_namespace
        finally:
            shutil.rmtree(temp_dir)
        assert namespace == deserialized_namespace

    def test_table_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        table_locator = TableLocator.at(
            namespace=None,
            table_name="test_table",
        )
        table = Table.of(
            locator=table_locator,
            description="test table description",
        )
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        table,
                    )
                ],
            ).commit(temp_dir)
            deserialized_table = Table.read(write_paths.pop())
        finally:
            shutil.rmtree(temp_dir)
        assert table == deserialized_table

    def test_table_version_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        table_version_locator = TableVersionLocator.at(
            namespace=None,
            table_name=None,
            table_version="test_table_version",
        )
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
        bucket_transform = BucketTransform.of(
            BucketTransformParameters.of(
                num_buckets=2,
                bucketing_strategy=BucketingStrategy.DEFAULT,
            )
        )
        partition_keys = [
            PartitionKey.of(
                key=["some_string", "some_int32"],
                name="test_partition_key",
                field_id="test_field_id",
                transform=bucket_transform,
            )
        ]
        partition_scheme = PartitionScheme.of(
            keys=partition_keys,
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )
        sort_keys = [
            SortKey.of(
                key=["some_int32"],
                sort_order=SortOrder.DESCENDING,
                null_order=NullOrder.AT_START,
                transform=TruncateTransform.of(
                    TruncateTransformParameters.of(width=3),
                ),
            )
        ]
        sort_scheme = SortScheme.of(
            keys=sort_keys,
            name="test_sort_scheme",
            scheme_id="test_sort_scheme_id",
        )
        table_version = TableVersion.of(
            locator=table_version_locator,
            schema=schema,
            partition_scheme=partition_scheme,
            description="test table version description",
            properties={"test_property_key": "test_property_value"},
            content_types=[ContentType.PARQUET],
            sort_scheme=sort_scheme,
            watermark=1,
            lifecycle_state=LifecycleState.CREATED,
            schemas=[schema, schema, schema],
            partition_schemes=[partition_scheme, partition_scheme],
            sort_schemes=[sort_scheme, sort_scheme],
        )
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        table_version,
                    )
                ],
            ).commit(temp_dir)
            deserialized_table_version = TableVersion.read(write_paths.pop())
        finally:
            shutil.rmtree(temp_dir)
        assert table_version == deserialized_table_version

    def test_stream_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        stream_locator = StreamLocator.at(
            namespace=None,
            table_name=None,
            table_version=None,
            stream_id="test_stream_id",
            stream_format=StreamFormat.DELTACAT,
        )
        bucket_transform = BucketTransform.of(
            BucketTransformParameters.of(
                num_buckets=2,
                bucketing_strategy=BucketingStrategy.DEFAULT,
            )
        )
        partition_keys = [
            PartitionKey.of(
                key=["some_string", "some_int32"],
                name="test_partition_key",
                field_id="test_field_id",
                transform=bucket_transform,
            )
        ]
        partition_scheme = PartitionScheme.of(
            keys=partition_keys,
            name="test_partition_scheme",
            scheme_id="test_partition_scheme_id",
        )
        stream = Stream.of(
            locator=stream_locator,
            partition_scheme=partition_scheme,
            state=CommitState.STAGED,
            previous_stream_id="test_previous_stream_id",
            watermark=1,
        )
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        stream,
                    )
                ],
            ).commit(temp_dir)
            deserialized_stream = Stream.read(write_paths.pop())
        finally:
            shutil.rmtree(temp_dir)
        assert stream == deserialized_stream

    def test_partition_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        partition_locator = PartitionLocator.at(
            namespace=None,
            table_name=None,
            table_version=None,
            stream_id=None,
            stream_format=None,
            partition_values=["a", 1],
            partition_id="test_partition_id",
        )
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
        partition = Partition.of(
            locator=partition_locator,
            schema=schema,
            content_types=[ContentType.PARQUET],
            state=CommitState.STAGED,
            previous_stream_position=0,
            previous_partition_id="test_previous_partition_id",
            stream_position=1,
            next_partition_id="test_next_partition_id",
            partition_scheme_id="test_partition_scheme_id",
        )
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        partition,
                    )
                ],
            ).commit(temp_dir)
            deserialized_partition = Partition.read(write_paths.pop())
        finally:
            shutil.rmtree(temp_dir)
        assert partition == deserialized_partition

    def test_delta_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        delta_locator = DeltaLocator.at(
            namespace=None,
            table_name=None,
            table_version=None,
            stream_id=None,
            stream_format=None,
            partition_values=None,
            partition_id=None,
            stream_position=1,
        )
        manifest_entry_params = EntryParams.of(
            equality_field_locators=["some_string", "some_int32"],
        )
        manifest_meta = ManifestMeta.of(
            record_count=1,
            content_length=10,
            content_type=ContentType.PARQUET.value,
            content_encoding=ContentEncoding.IDENTITY.value,
            source_content_length=100,
            credentials={"foo": "bar"},
            content_type_parameters=[{"param1": "value1"}],
            entry_type=EntryType.EQUALITY_DELETE,
            entry_params=manifest_entry_params,
        )
        manifest = Manifest.of(
            entries=[
                ManifestEntry.of(
                    url="s3://test/url",
                    meta=manifest_meta,
                )
            ],
            author=ManifestAuthor.of(
                name="deltacat",
                version="2.0",
            ),
            entry_type=EntryType.EQUALITY_DELETE,
            entry_params=manifest_entry_params,
        )
        delta = Delta.of(
            locator=delta_locator,
            delta_type=DeltaType.APPEND,
            meta=manifest_meta,
            properties={"property1": "value1"},
            manifest=manifest,
            previous_stream_position=0,
        )
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        delta,
                    )
                ],
            ).commit(temp_dir)
            deserialized_delta = Delta.read(write_paths.pop())
        finally:
            shutil.rmtree(temp_dir)
        assert delta == deserialized_delta

    def test_python_type_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        table_locator = TableLocator.at(
            namespace=None,
            table_name="test_table",
        )
        # test basic python types
        # set, frozenset, and range can't be serialized by msgpack
        # memoryview can't be pickled by copy.deepcopy
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
        }
        table = Table.of(
            locator=table_locator,
            description="test table description",
            properties=properties,
        )
        try:
            write_paths = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=[
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        table,
                    )
                ],
            ).commit(temp_dir)
            deserialized_table = Table.read(write_paths.pop())
        finally:
            shutil.rmtree(temp_dir)
        # exchange original table properties with the expected table properties
        expected_properties = properties.copy()
        # msgpack tranlates tuples to lists
        expected_properties["garply"] = [1, 2, 3]
        # msgpack unpacks bytearray into bytes
        expected_properties["waldo"] = b"\x00\x00\x00"
        table.properties = expected_properties
        assert table == deserialized_table
