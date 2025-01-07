import copy
import shutil
import unittest
import tempfile
import os
import uuid
from typing import List, Tuple

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
from deltacat.storage.model.metafile import TXN_DIR_NAME, Metafile


def _commit_single_delta_table(temp_dir: str) -> List[Tuple[Metafile, Metafile, str]]:
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
    meta_to_create = [
        namespace,
        table,
        table_version,
        stream,
        partition,
        delta,
    ]
    txn_operations = [
        TransactionOperation.of(
            TransactionOperationType.CREATE,
            meta,
        )
        for meta in meta_to_create
    ]
    transaction = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=txn_operations,
    )
    write_paths = transaction.commit(temp_dir)
    write_paths_copy = write_paths.copy()
    assert os.path.exists(os.path.join(temp_dir, TXN_DIR_NAME, transaction.id))
    metafiles_created = [
        Delta.read(write_paths.pop()),
        Partition.read(write_paths.pop()),
        Stream.read(write_paths.pop()),
        TableVersion.read(write_paths.pop()),
        Table.read(write_paths.pop()),
        Namespace.read(write_paths.pop()),
    ]
    metafiles_created.reverse()
    return list(zip(meta_to_create, metafiles_created, write_paths_copy))


class TestMetafileIO(unittest.TestCase):
    def test_rename_table(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        try:
            commit_results = _commit_single_delta_table(temp_dir)
            for expected, actual, _ in commit_results:
                assert expected == actual
            original_table = commit_results[1][1]
            renamed_table = copy.deepcopy(original_table)
            renamed_table.locator = TableLocator.at(
                namespace="test_namespace",
                table_name="test_table_renamed",
            )
            txn_operations = [
                TransactionOperation.of(
                    TransactionOperationType.UPDATE,
                    renamed_table,
                    original_table,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.RESTATE,
                txn_operations=txn_operations,
            )
            write_paths = transaction.commit(temp_dir)

            # ensure that only the table metafile was overwritten
            assert len(write_paths) == 1

            # ensure that the table was successfully renamed
            actual_table = Table.read(write_paths[0])
            assert renamed_table == actual_table

            # ensure all new metafiles read return the new table name
            child_metafiles_read_post_rename = [
                Delta.read(commit_results[5][2]),
                Partition.read(commit_results[4][2]),
                Stream.read(commit_results[3][2]),
                TableVersion.read(commit_results[2][2]),
            ]
            for metafile in child_metafiles_read_post_rename:
                assert metafile.table_name == "test_table_renamed"

            # ensure the original metafiles return the original table name
            original_child_metafiles_to_create = [
                Delta(commit_results[5][0]),
                Partition(commit_results[4][0]),
                Stream(commit_results[3][0]),
                TableVersion(commit_results[2][0]),
            ]
            original_child_metafiles_created = [
                Delta(commit_results[5][1]),
                Partition(commit_results[4][1]),
                Stream(commit_results[3][1]),
                TableVersion(commit_results[2][1]),
            ]
            for i in range(len(original_child_metafiles_to_create)):
                assert (
                    original_child_metafiles_created[i].table_name
                    == original_child_metafiles_to_create[i].table_name
                    == "test_table"
                )

            # ensure that table updates using the old table name fail
            previous_table_copy = copy.deepcopy(original_table)
            bad_txn_operations = [
                TransactionOperation.of(
                    TransactionOperationType.UPDATE,
                    previous_table_copy,
                    original_table,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.RESTATE,
                txn_operations=bad_txn_operations,
            )
            self.assertRaises(
                ValueError,
                transaction.commit,
                root=temp_dir,
            )

            # ensure that table deletes using the old table name fail
            bad_txn_operations = [
                TransactionOperation.of(
                    TransactionOperationType.DELETE,
                    previous_table_copy,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.DELETE,
                txn_operations=bad_txn_operations,
            )
            self.assertRaises(
                ValueError,
                transaction.commit,
                root=temp_dir,
            )

            # ensure that delta/partition/stream/table-version creation using
            # the old table name fails
            for metafile in original_child_metafiles_created:
                bad_txn_operations = [
                    TransactionOperation.of(
                        TransactionOperationType.CREATE,
                        metafile,
                    )
                ]
                transaction = Transaction.of(
                    txn_type=TransactionType.APPEND,
                    txn_operations=bad_txn_operations,
                )
                self.assertRaises(
                    ValueError,
                    transaction.commit,
                    root=temp_dir,
                )

        finally:
            shutil.rmtree(temp_dir)
            pass

    def test_rename_namespace(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))
        try:
            commit_results = _commit_single_delta_table(temp_dir)
            for expected, actual, _ in commit_results:
                assert expected == actual
            previous_namespace = commit_results[0][1]
            expected_namespace = copy.deepcopy(commit_results[0][1])
            expected_namespace.locator = NamespaceLocator.of(
                namespace="test_namespace_renamed",
            )
            txn_operations = [
                TransactionOperation.of(
                    TransactionOperationType.UPDATE,
                    expected_namespace,
                    previous_namespace,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.RESTATE,
                txn_operations=txn_operations,
            )
            write_paths = transaction.commit(temp_dir)

            # ensure that only the namespace metafile was rewritten
            assert len(write_paths) == 1

            # ensure all new metafiles read return the new namespace name
            actual_namespace = Namespace.read(write_paths[0])
            assert expected_namespace == actual_namespace
            actual_namespace_name = Delta.read(commit_results[5][2]).namespace
            assert actual_namespace_name == "test_namespace_renamed"
            actual_namespace = Partition.read(commit_results[4][2]).namespace
            assert actual_namespace == "test_namespace_renamed"
            actual_namespace = Stream.read(commit_results[3][2]).namespace
            assert actual_namespace == "test_namespace_renamed"
            actual_namespace = TableVersion.read(commit_results[2][2]).namespace
            assert actual_namespace == "test_namespace_renamed"
            actual_namespace = Table.read(commit_results[1][2]).namespace
            assert actual_namespace == "test_namespace_renamed"

            # ensure the initial metafiles read return the original namespace name
            prev_namespace = Delta(commit_results[5][1]).namespace
            assert prev_namespace == commit_results[5][0].namespace == "test_namespace"
            prev_namespace = Partition(commit_results[4][1]).namespace
            assert prev_namespace == commit_results[4][0].namespace == "test_namespace"
            prev_namespace = Stream(commit_results[3][1]).namespace
            assert prev_namespace == commit_results[3][0].namespace == "test_namespace"
            prev_namespace = TableVersion(commit_results[2][1]).namespace
            assert prev_namespace == commit_results[2][0].namespace == "test_namespace"
            prev_namespace = Table(commit_results[1][1]).namespace
            assert prev_namespace == commit_results[1][0].namespace == "test_namespace"

            # TODO(pdames): Ensure that read-from/write-to old namespace name fails
        finally:
            shutil.rmtree(temp_dir)

    def test_e2e_serde(self):
        temp_dir = tempfile.gettempdir()
        temp_dir = os.path.join(temp_dir, str(uuid.uuid4()))

        try:
            commit_results = _commit_single_delta_table(temp_dir)
            for expected, actual, _ in commit_results:
                assert expected == actual
        finally:
            shutil.rmtree(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual

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
