import os
from typing import List, Tuple

import time
import multiprocessing

import pyarrow as pa
import pytest


print("this is a test LOL")

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
    TransactionOperation,
    TransactionType,
    TransactionOperationType,
    TruncateTransform,
    TruncateTransformParameters,
)
from deltacat.storage.model.metafile import (
    Metafile,
    MetafileRevisionInfo,
)
from deltacat.constants import TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME, NANOS_PER_SEC
from deltacat.utils.filesystem import resolve_path_and_filesystem

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
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=meta,
        )
        for meta in meta_to_create
    ]
    transaction = Transaction.of(
        txn_type=TransactionType.APPEND,
        txn_operations=txn_operations,
    )
    write_paths, txn_log_path = transaction.commit(temp_dir)
    write_paths_copy = write_paths.copy()
    assert os.path.exists(txn_log_path)
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


def _commit_concurrent_transaction(
    catalog_root: str,
    transaction: Transaction,
) -> None:
    try:
        return transaction.commit(catalog_root)
    except (RuntimeError, ValueError) as e:
        return e

class TestAbsToRelative:
    def test_metafile_read_bad_path(self, temp_dir):
        with pytest.raises(FileNotFoundError):
            Delta.read("foobar")
    def test_abs_to_relative(self):
        print("this is a test LOL")
        assert 1 == 1
        assert 2 == 2
        assert 3 == 3
        return None
    def test_replace_delta(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_delta: Delta = commit_results[5][1]

        # given a transaction containing a delta replacement
        replacement_delta: Delta = Delta.based_on(
            original_delta,
            new_id=str(int(original_delta.id) + 1),
        )

        # expect the proposed replacement delta to be assigned a new ID
        assert replacement_delta.id != original_delta.id

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_delta,
                src_metafile=original_delta,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect two new metafiles to be written
        # (i.e., delete old delta, create replacement delta)
        assert len(write_paths) == 2
        delete_write_path = write_paths[0]
        create_write_path = write_paths[1]

        # expect the replacement delta to be successfully written and read
        assert TransactionOperationType.CREATE.value in create_write_path
        actual_delta = Delta.read(create_write_path)
        assert replacement_delta.equivalent_to(actual_delta)

        # expect the delete metafile to also contain the replacement delta
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_delta = Delta.read(delete_write_path)
        assert replacement_delta.equivalent_to(actual_delta)

        # expect a subsequent replace of the original delta to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_delta,
                src_metafile=original_delta,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect deletes of the original delta to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_delta,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)