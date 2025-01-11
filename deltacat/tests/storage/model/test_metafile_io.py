import os
from typing import List, Tuple

import pyarrow as pa
import pytest

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
            operation_type=TransactionOperationType.CREATE,
            dest_metafile=meta,
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


class TestMetafileIO:
    def test_replace_table(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        original_table: Table = commit_results[1][1]

        # given a transaction containing a table replacement
        replacement_table: Table = Table.based_on(original_table)

        # expect the proposed replacement table to be assigned a new ID, but
        # continue to have the same name as the original table
        assert replacement_table.id != original_table.id
        assert replacement_table.table_name == original_table.table_name

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_table,
                src_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths = transaction.commit(temp_dir)

        # expect two new table metafiles to be written
        # (i.e., delete old table, create replacement table)
        assert len(write_paths) == 2

        # expect the replacement table to be successfully written and read
        actual_table = Table.read(write_paths[0])
        assert replacement_table == actual_table

        # expect old child metafiles for the replaced table to remain readable
        child_metafiles_read_post_replace = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
            TableVersion.read(commit_results[2][2]),
        ]
        # expect old child metafiles read to share the same parent table name as
        # the replacement table, but have a different parent table ID
        for metafile in child_metafiles_read_post_replace:
            assert (
                metafile.table_name
                == replacement_table.table_name
                == original_table.table_name
            )
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_table_id = ancestor_ids[1]
            assert parent_table_id == original_table.id

        # expect original child metafiles to share the original parent table ID
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
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_table_id = ancestor_ids[1]
            assert parent_table_id == original_table.id

        # expect a subsequent table replace of the original table to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_table,
                src_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect table deletes of the original table to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the old table to fail
        for metafile in original_child_metafiles_created:
            bad_txn_operations = [
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=metafile,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=bad_txn_operations,
            )
            with pytest.raises(ValueError):
                transaction.commit(temp_dir)

    def test_create_stream_bad_order_txn_op_chaining(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        # given a transaction containing:

        # 1. a new table version in an existing table
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id=original_table_version_created.id + "_2",
        )
        # 2. a new stream in the new table version
        original_stream_created = Stream(commit_results[3][1])
        new_stream: Stream = Stream.based_on(
            other=original_stream_created,
            new_id="test_stream_id",
        )
        new_stream.table_version_locator.table_version = new_table_version.table_version

        # 3. ordered transaction operations that try to put the new stream
        # in the new table version before it is created
        txn_operations = [
            TransactionOperation.of(
                TransactionOperationType.CREATE,
                new_stream,
            ),
            TransactionOperation.of(
                TransactionOperationType.CREATE,
                new_table_version,
            ),
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=txn_operations,
        )
        # when the transaction is committed,
        # expect stream creation to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)
        # when a transaction with the operations reversed is committed,
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=list(reversed(txn_operations)),
        )
        # expect table version and stream creation to succeed
        write_paths = transaction.commit(temp_dir)
        assert len(write_paths) == 2

    def test_table_rename_bad_order_txn_op_chaining(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        original_table: Table = commit_results[1][1]
        # given a transaction containing:
        # 1. a table rename
        renamed_table: Table = Table.update_for(original_table)
        renamed_table.locator = TableLocator.at(
            namespace="test_namespace",
            table_name="test_table_renamed",
        )
        # 2. a new table version in a renamed table
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version_to_create: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id=original_table_version_created.id + "_2",
        )
        new_table_version_to_create.table_locator.table_name = renamed_table.table_name
        # 3. ordered transaction operations that try to put the new table
        # version in the renamed table before the table is renamed
        txn_operations = [
            TransactionOperation.of(
                TransactionOperationType.CREATE,
                new_table_version_to_create,
            ),
            TransactionOperation.of(
                TransactionOperationType.UPDATE,
                renamed_table,
                original_table,
            ),
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=txn_operations,
        )
        # when the transaction is committed,
        # expect the transaction to fail due to incorrect operation order
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)
        # when a transaction with the operations reversed is committed,
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=list(reversed(txn_operations)),
        )
        # expect table and table version creation to succeed
        write_paths = transaction.commit(temp_dir)
        assert len(write_paths) == 2

    # TODO(pdames): Test isolation of creating a duplicate namespace/table/etc.
    #  between multiple concurrent transactions.
    def test_create_duplicate_namespace(self, temp_dir):
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        # given serial transaction that try to create two namespaces with
        # the same name
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    namespace,
                ),
            ],
        )
        # expect the first transaction to be successfully committed
        write_paths = transaction.commit(temp_dir)
        deserialized_namespace = Namespace.read(write_paths.pop())
        assert namespace == deserialized_namespace
        # but expect the second transaction to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

    def test_create_duplicate_namespace_txn_op_chaining(self, temp_dir):
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        # given a transaction that tries to create the same namespace twice
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    namespace,
                ),
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    namespace,
                ),
            ],
        )
        # when the transaction is committed,
        # expect duplicate namespace creation to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

    def test_create_stream_in_missing_table_version(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        # given a transaction that tries to create a single stream
        # in a table version that doesn't exist
        original_stream_created = Stream(commit_results[3][1])
        new_stream: Stream = Stream.based_on(
            other=original_stream_created,
            new_id="test_stream_id",
        )
        new_stream.table_version_locator.table_version = "missing_table_version"
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    new_stream,
                )
            ],
        )
        # when the transaction is committed,
        # expect stream creation to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

    def test_create_table_version_in_missing_namespace(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        # given a transaction that tries to create a single table version
        # in a namespace that doesn't exist
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id="test_table_version",
        )
        new_table_version.namespace_locator.namespace = "missing_namespace"
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    new_table_version,
                )
            ],
        )
        # when the transaction is committed,
        # expect table version creation to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

    def test_create_table_version_in_missing_table(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        # given a transaction that tries to create a single table version
        # in a table that doesn't exist
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id="test_table_version",
        )
        new_table_version.table_locator.table_name = "missing_table"
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    new_table_version,
                )
            ],
        )
        # when the transaction is committed,
        # expect table version creation to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

    def test_create_table_in_missing_namespace(self, temp_dir):
        table_locator = TableLocator.at(
            namespace="missing_namespace",
            table_name="test_table",
        )
        table = Table.of(
            locator=table_locator,
            description="test table description",
        )
        # given a transaction that tries to create a single table in a
        # namespace that doesn't exist
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    TransactionOperationType.CREATE,
                    table,
                )
            ],
        )
        # when the transaction is committed,
        # expect table creation to fail
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

    def test_rename_table_txn_op_chaining(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        original_table: Table = commit_results[1][1]
        # given a transaction containing:
        # 1. a table rename
        renamed_table: Table = Table.update_for(original_table)
        renamed_table.locator = TableLocator.at(
            namespace="test_namespace",
            table_name="test_table_renamed",
        )
        original_delta_created = Delta(commit_results[5][1])
        original_partition_created = Partition(commit_results[4][1])
        original_stream_created = Stream(commit_results[3][1])
        original_table_version_created = TableVersion(commit_results[2][1])
        # 2. a new table version in the renamed table
        new_table_version_to_create: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id=original_table_version_created.table_version + "_2",
        )
        new_table_version_to_create.table_locator.table_name = renamed_table.table_name
        # 3. a new stream in the new table version in the renamed table
        new_stream_to_create: Stream = Stream.based_on(
            other=original_stream_created,
            new_id=original_stream_created.stream_id + "_2",
        )
        new_stream_to_create.locator.table_version_locator = (
            new_table_version_to_create.locator
        )
        # 4. a new partition in the new stream in the new table version
        # in the renamed table
        new_partition_to_create: Partition = Partition.based_on(
            other=original_partition_created,
            new_id=original_partition_created.partition_id + "_2",
        )
        new_partition_to_create.locator.stream_locator = new_stream_to_create.locator
        # 5. a new delta in the new partition in the new stream in the new
        # table version in the renamed table
        new_delta_to_create = Delta.based_on(
            other=original_delta_created,
            new_id="2",
        )
        new_delta_to_create.locator.partition_locator = new_partition_to_create.locator
        # 6. ordered transaction operations that ensure all prior
        # dependencies are satisfied
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=renamed_table,
                src_metafile=original_table,
            ),
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=new_table_version_to_create,
            ),
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=new_stream_to_create,
            ),
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=new_partition_to_create,
            ),
            TransactionOperation.of(
                operation_type=TransactionOperationType.CREATE,
                dest_metafile=new_delta_to_create,
            ),
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths = transaction.commit(temp_dir)

        # expect the transaction to successfully create 5 new metafiles
        assert len(write_paths) == 5

        # expect the table to be successfully renamed
        actual_table = Table.read(write_paths[0])
        assert renamed_table == actual_table

        # expect the new table version in the renamed table to be
        # successfully created
        actual_table_version = TableVersion.read(write_paths[1])
        assert new_table_version_to_create == actual_table_version

        # expect the new stream in the new table version in the renamed
        # table to be successfully created
        actual_stream = Stream.read(write_paths[2])
        assert new_stream_to_create == actual_stream

        # expect the new partition in the new stream in the new table
        # version in the renamed table to be successfully created
        actual_partition = Partition.read(write_paths[3])
        assert new_partition_to_create == actual_partition

        # expect the new delta in the new partition in the new stream in
        # the new table version in the renamed table to be successfully
        # created
        actual_delta = Delta.read(write_paths[4])
        assert new_delta_to_create == actual_delta

    def test_rename_table(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        original_table: Table = commit_results[1][1]

        # given a transaction containing a table rename
        renamed_table: Table = Table.update_for(original_table)
        renamed_table.locator = TableLocator.at(
            namespace="test_namespace",
            table_name="test_table_renamed",
        )
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=renamed_table,
                src_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths = transaction.commit(temp_dir)

        # expect only one new table metafile to be written
        assert len(write_paths) == 1

        # expect the table to be successfully renamed
        actual_table = Table.read(write_paths[0])
        assert renamed_table == actual_table

        # expect all new child metafiles read to return the new table name
        child_metafiles_read_post_rename = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
            TableVersion.read(commit_results[2][2]),
        ]
        for metafile in child_metafiles_read_post_rename:
            assert metafile.table_name == renamed_table.table_name

        # expect all original metafiles to return the original table name
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
                == original_table.table_name
            )

        # expect a subsequent table update from the old table name to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=renamed_table,
                src_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.RESTATE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect table deletes of the old table name fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect child metafile creation under the old table name to fail
        for metafile in original_child_metafiles_created:
            bad_txn_operations = [
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=metafile,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=bad_txn_operations,
            )
            with pytest.raises(ValueError):
                transaction.commit(temp_dir)

    def test_rename_namespace(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected == actual
        original_namespace = commit_results[0][1]
        # given a transaction containing a namespace rename
        renamed_namespace: Namespace = Namespace.update_for(original_namespace)
        renamed_namespace.locator = NamespaceLocator.of(
            namespace="test_namespace_renamed",
        )
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=renamed_namespace,
                src_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths = transaction.commit(temp_dir)

        # expect only one new namespace metafile to be written
        assert len(write_paths) == 1

        # expect the namespace to be successfully renamed
        actual_namespace = Namespace.read(write_paths[0])
        assert renamed_namespace == actual_namespace

        # expect all child metafiles read to return the new namespace
        child_metafiles_read_post_rename = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
            TableVersion.read(commit_results[2][2]),
            Table.read(commit_results[1][2]),
        ]
        for metafile in child_metafiles_read_post_rename:
            assert metafile.namespace == "test_namespace_renamed"

        # expect the original metafiles to return the original namespace
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
            Partition(commit_results[4][0]),
            Stream(commit_results[3][0]),
            TableVersion(commit_results[2][0]),
            Table(commit_results[1][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
            Partition(commit_results[4][1]),
            Stream(commit_results[3][1]),
            TableVersion(commit_results[2][1]),
            Table(commit_results[1][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            assert (
                original_child_metafiles_created[i].namespace
                == original_child_metafiles_to_create[i].namespace
                == "test_namespace"
            )

        # expect a subsequent update of the old namespace name to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=renamed_namespace,
                src_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect namespace deletes of the old namespace name fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect child metafile creation under the old namespace to fail
        for metafile in original_child_metafiles_created:
            bad_txn_operations = [
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=metafile,
                )
            ]
            transaction = Transaction.of(
                txn_type=TransactionType.APPEND,
                txn_operations=bad_txn_operations,
            )
            with pytest.raises(ValueError):
                transaction.commit(temp_dir)

    def test_e2e_serde(self, temp_dir):
        # given a transaction that creates a single namespace, table,
        # table version, stream, partition, and delta
        commit_results = _commit_single_delta_table(temp_dir)
        # when the transaction is committed, expect all actual metafiles
        # created to match the expected/input metafiles to create
        for expected, actual, _ in commit_results:
            assert expected == actual

    def test_namespace_serde(self, temp_dir):
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        # given a transaction that creates a single namespace
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=namespace,
                )
            ],
        ).commit(temp_dir)
        # when the transaction is committed,
        # expect the namespace created to match the namespace given
        deserialized_namespace = Namespace.read(write_paths.pop())
        assert namespace == deserialized_namespace

    def test_table_serde(self, temp_dir):
        table_locator = TableLocator.at(
            namespace=None,
            table_name="test_table",
        )
        table = Table.of(
            locator=table_locator,
            description="test table description",
        )
        # given a transaction that creates a single table
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=table,
                )
            ],
        ).commit(temp_dir)
        # when the transaction is committed,
        # expect the table created to match the table given
        deserialized_table = Table.read(write_paths.pop())
        assert table == deserialized_table

    def test_table_version_serde(self, temp_dir):
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
        # given a transaction that creates a single table version
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=table_version,
                )
            ],
        ).commit(temp_dir)
        # when the transaction is committed,
        # expect the table version created to match the table version given
        deserialized_table_version = TableVersion.read(write_paths.pop())
        assert table_version == deserialized_table_version

    def test_stream_serde(self, temp_dir):
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
        # given a transaction that creates a single stream
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=stream,
                )
            ],
        ).commit(temp_dir)
        # when the transaction is committed,
        # expect the stream created to match the stream given
        deserialized_stream = Stream.read(write_paths.pop())
        assert stream == deserialized_stream

    def test_partition_serde(self, temp_dir):
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
        # given a transaction that creates a single partition
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=partition,
                )
            ],
        ).commit(temp_dir)
        # when the transaction is committed,
        # expect the partition created to match the partition given
        deserialized_partition = Partition.read(write_paths.pop())
        assert partition == deserialized_partition

    def test_delta_serde(self, temp_dir):
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
        # given a transaction that creates a single delta
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=delta,
                )
            ],
        ).commit(temp_dir)
        # when the transaction is committed,
        # expect the delta created to match the delta given
        deserialized_delta = Delta.read(write_paths.pop())
        assert delta == deserialized_delta

    def test_python_type_serde(self, temp_dir):
        table_locator = TableLocator.at(
            namespace=None,
            table_name="test_table",
        )
        # given a table whose property values contain every basic python type
        # except set, frozenset, and range which can't be serialized by msgpack
        # and memoryview which can't be pickled by copy.deepcopy
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
        # when a transaction commits this table
        write_paths = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=table,
                )
            ],
        ).commit(temp_dir)
        deserialized_table = Table.read(write_paths.pop())
        # expect the following SerDe transformations of the original properties:
        expected_properties = properties.copy()
        # 1. msgpack tranlates tuple to list
        expected_properties["garply"] = [1, 2, 3]
        # 2. msgpack unpacks bytearray into bytes
        expected_properties["waldo"] = b"\x00\x00\x00"
        # expect the table created to otherwise match the table given
        table.properties = expected_properties
        assert table == deserialized_table
