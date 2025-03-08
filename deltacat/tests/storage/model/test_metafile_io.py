import os
from typing import List, Tuple

import time
import multiprocessing

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
from deltacat.tests.test_utils.storage import (
    create_test_namespace,
    create_test_table,
    create_test_table_version,
    create_test_stream,
    create_test_partition,
    create_test_delta,
)


def _commit_single_delta_table(temp_dir: str) -> List[Tuple[Metafile, Metafile, str]]:
    namespace = create_test_namespace()
    table = create_test_table()
    table_version = create_test_table_version()
    stream = create_test_stream()
    partition = create_test_partition()
    delta = create_test_delta()

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


class TestMetafileIO:
    def test_txn_conflict_concurrent_multiprocess_table_create(self, temp_dir):
        base_table_name = "test_table"
        table_locator = TableLocator.at(
            namespace=None,
            table_name=base_table_name,
        )
        # given a transaction to create a table
        table = Table.of(
            locator=table_locator,
            description="test table description",
        )
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=table,
                )
            ],
        )
        # when K rounds of N concurrent transaction commits try to create the
        # same table
        rounds = 25
        concurrent_commit_count = multiprocessing.cpu_count()
        with multiprocessing.Pool(processes=concurrent_commit_count) as pool:
            for round_number in range(rounds):
                table.locator.table_name = f"{base_table_name}_{round_number}"
                futures = [
                    pool.apply_async(
                        _commit_concurrent_transaction, (temp_dir, transaction)
                    )
                    for _ in range(concurrent_commit_count)
                ]
                # expect all but one concurrent transaction to succeed each round
                results = [future.get() for future in futures]
                conflict_exception_count = 0
                for result in results:
                    # TODO(pdames): Add new concurrent conflict exception types.
                    if isinstance(result, RuntimeError) or isinstance(
                        result, ValueError
                    ):
                        conflict_exception_count += 1
                    else:
                        write_paths, txn_log_path = result
                        deserialized_table = Table.read(write_paths.pop())
                        assert table.equivalent_to(deserialized_table)
                assert conflict_exception_count == concurrent_commit_count - 1

    def test_txn_dual_commit_fails(self, temp_dir):
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        # given a transaction that creates a single namespace
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=namespace,
                )
            ],
        )
        write_paths, txn_log_path = transaction.commit(temp_dir)
        # when the transaction is committed,
        # expect the namespace created to match the namespace given
        deserialized_namespace = Namespace.read(write_paths.pop())
        assert namespace.equivalent_to(deserialized_namespace)
        # if we reread the transaction and commit it again,
        reread_transaction = Transaction.read(txn_log_path)
        # expect an exception to be raised
        with pytest.raises(RuntimeError):
            reread_transaction.commit(temp_dir)

    def test_txn_bad_end_time_fails(self, temp_dir, mocker):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        # given a transaction with an ending timestamp set in the past
        past_timestamp = time.time_ns() - NANOS_PER_SEC
        mocker.patch(
            "deltacat.storage.model.transaction.Transaction._parse_end_time",
            return_value=past_timestamp,
        )
        original_delta: Delta = commit_results[5][1]
        new_delta = Delta.update_for(original_delta)
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_delta,
                src_metafile=original_delta,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=txn_operations,
        )
        # expect the bad timestamp to be detected and its commit to fail
        with pytest.raises(RuntimeError):
            transaction.commit(temp_dir)

    def test_txn_conflict_concurrent_complete(self, temp_dir, mocker):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)

        # given an initial metafile revision of a committed delta
        write_paths = [result[2] for result in commit_results]
        orig_delta_write_path = write_paths[5]

        # a new delta metafile revision written by a transaction that completed
        # before seeing any concurrent conflicts
        mri = MetafileRevisionInfo.parse(orig_delta_write_path)
        mri.txn_id = "0000000000000_test-txn-id"
        mri.txn_op_type = TransactionOperationType.UPDATE
        mri.revision = mri.revision + 1
        conflict_delta_write_path = mri.path
        _, filesystem = resolve_path_and_filesystem(orig_delta_write_path)
        with filesystem.open_output_stream(conflict_delta_write_path):
            pass  # Just create an empty conflicting metafile revision
        txn_log_file_dir = os.path.join(
            temp_dir,
            TXN_DIR_NAME,
            SUCCESS_TXN_DIR_NAME,
            mri.txn_id,
        )
        filesystem.create_dir(txn_log_file_dir, recursive=True)
        txn_log_file_path = os.path.join(
            txn_log_file_dir,
            str(time.time_ns()),
        )
        with filesystem.open_output_stream(txn_log_file_path):
            pass  # Just create an empty log to mark the txn as complete

        # and a concurrent transaction that started before that transaction
        # completed, writes the same delta metafile revision, then sees the
        # conflict
        past_timestamp = time.time_ns() - NANOS_PER_SEC
        future_timestamp = 9999999999999
        end_time_mock = mocker.patch(
            "deltacat.storage.model.transaction.Transaction._parse_end_time",
        )
        end_time_mock.side_effect = (
            lambda path: future_timestamp if mri.txn_id in path else past_timestamp
        )
        original_delta = Delta.read(orig_delta_write_path)
        new_delta = Delta.update_for(original_delta)
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=new_delta,
                src_metafile=original_delta,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=txn_operations,
        )
        # expect the commit to fail due to a concurrent modification error
        with pytest.raises(RuntimeError):
            transaction.commit(temp_dir)

    def test_txn_conflict_concurrent_incomplete(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)

        # given an initial metafile revision of a committed delta
        write_paths = [result[2] for result in commit_results]
        orig_delta_write_path = write_paths[5]

        # and a new delta metafile revision written by an incomplete transaction
        mri = MetafileRevisionInfo.parse(orig_delta_write_path)
        mri.txn_id = "9999999999999_test-txn-id"
        mri.txn_op_type = TransactionOperationType.DELETE
        mri.revision = mri.revision + 1
        conflict_delta_write_path = mri.path
        _, filesystem = resolve_path_and_filesystem(orig_delta_write_path)
        with filesystem.open_output_stream(conflict_delta_write_path):
            pass  # Just create an empty conflicting metafile revision

        # when a concurrent transaction tries to update the same delta
        original_delta = Delta.read(orig_delta_write_path)
        new_delta = Delta.update_for(original_delta)
        transaction = Transaction.of(
            txn_type=TransactionType.ALTER,
            txn_operations=[
                TransactionOperation.of(
                    operation_type=TransactionOperationType.UPDATE,
                    dest_metafile=new_delta,
                    src_metafile=original_delta,
                )
            ],
        )
        # expect the commit to fail due to a concurrent modification error
        with pytest.raises(RuntimeError):
            transaction.commit(temp_dir)
        # expect a commit retry to also fail
        with pytest.raises(RuntimeError):
            transaction.commit(temp_dir)

    def test_append_multiple_deltas(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_delta: Delta = commit_results[5][1]

        # given a transaction containing several deltas to append
        txn_operations = []

        delta_append_count = 100
        for i in range(delta_append_count):
            new_delta = Delta.based_on(
                original_delta,
                new_id=str(int(original_delta.id) + i + 1),
            )
            txn_operations.append(
                TransactionOperation.of(
                    operation_type=TransactionOperationType.CREATE,
                    dest_metafile=new_delta,
                )
            )
        transaction = Transaction.of(
            txn_type=TransactionType.APPEND,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)
        # expect all new deltas to be successfully written
        assert len(write_paths) == delta_append_count
        for i in range(len(write_paths)):
            actual_delta = Delta.read(write_paths[i])
            assert txn_operations[i].dest_metafile.equivalent_to(actual_delta)

    def test_bad_update_mismatched_metafile_types(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_partition: Partition = commit_results[4][1]
        original_delta: Delta = commit_results[5][1]

        # given an attempt to replace a delta with a partition
        replacement_partition: Partition = Partition.based_on(
            original_partition,
            new_id=original_partition.id + "_2",
        )
        # expect the transaction operation initialization to raise a value error
        with pytest.raises(ValueError):
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_partition,
                src_metafile=original_delta,
            )

    def test_delete_delta(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_delta: Delta = commit_results[5][1]

        # given a transaction containing a delta to delete
        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_delta,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect one new delete metafile to be written
        assert len(write_paths) == 1
        delete_write_path = write_paths[0]

        # expect the delete metafile to contain the input txn op dest_metafile
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_delta = Delta.read(delete_write_path)
        assert original_delta.equivalent_to(actual_delta)

        # expect a subsequent replace of the deleted delta to fail
        replacement_delta: Delta = Delta.based_on(
            original_delta,
            new_id=str(int(original_delta.id) + 1),
        )
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

        # expect subsequent deletes of the deleted delta to fail
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

    def test_delete_partition(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_partition: Partition = commit_results[4][1]

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_partition,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect 1 new partition metafile to be written
        assert len(write_paths) == 1
        delete_write_path = write_paths[0]

        # expect the delete metafile to contain the input txn op dest_metafile
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_partition = Partition.read(delete_write_path)
        assert original_partition.equivalent_to(actual_partition)

        # expect child metafiles in the deleted partition to remain readable and unchanged
        child_metafiles_read_post_delete = [
            Delta.read(commit_results[5][2]),
        ]
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_to_create[i]
            )
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_created[i]
            )

        # expect a subsequent replace of the deleted partition to fail
        replacement_partition: Partition = Partition.based_on(
            original_partition,
            new_id=original_partition.id + "_2",
        )
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_partition,
                src_metafile=original_partition,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect subsequent deletes of the deleted partition to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_partition,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the deleted partition to fail
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

    def test_replace_partition(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_partition: Partition = commit_results[4][1]

        # given a transaction containing a partition replacement
        replacement_partition: Partition = Partition.based_on(
            original_partition,
            new_id=original_partition.id + "_2",
        )

        # expect the proposed replacement partition to be assigned a new ID
        assert replacement_partition.id != original_partition.id

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_partition,
                src_metafile=original_partition,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect two new partition metafiles to be written
        # (i.e., delete old partition, create replacement partition)
        assert len(write_paths) == 2
        delete_write_path = write_paths[0]
        create_write_path = write_paths[1]

        # expect the replacement partition to be successfully written and read
        assert TransactionOperationType.CREATE.value in create_write_path
        actual_partition = Partition.read(create_write_path)
        assert replacement_partition.equivalent_to(actual_partition)

        # expect the delete metafile to also contain the replacement partition
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_partition = Partition.read(delete_write_path)
        assert replacement_partition.equivalent_to(actual_partition)

        # expect old child metafiles for the replaced partition to remain readable
        child_metafiles_read_post_replace = [
            Delta.read(commit_results[5][2]),
        ]
        # expect old child metafiles read to share the same parent table name as
        # the replacement partition, but have a different parent partition ID
        for metafile in child_metafiles_read_post_replace:
            assert (
                metafile.table_name
                == replacement_partition.table_name
                == original_partition.table_name
            )
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_partition_id = ancestor_ids[4]
            assert parent_partition_id == original_partition.id

        # expect original child metafiles to share the original parent partition ID
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_partition_id = ancestor_ids[4]
            assert parent_partition_id == original_partition.id

        # expect a subsequent replace of the original partition to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_partition,
                src_metafile=original_partition,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect deletes of the original partition to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_partition,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the old partition to fail
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

    def test_delete_stream(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_stream: Stream = commit_results[3][1]

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_stream,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect 1 new stream metafile to be written
        assert len(write_paths) == 1
        delete_write_path = write_paths[0]

        # expect the delete metafile to contain the input txn op dest_metafile
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_stream = Stream.read(delete_write_path)
        assert original_stream == actual_stream

        # expect child metafiles in the deleted stream to remain readable and unchanged
        child_metafiles_read_post_delete = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
        ]
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
            Partition(commit_results[4][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
            Partition(commit_results[4][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_to_create[i]
            )
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_created[i]
            )

        # expect a subsequent replace of the deleted stream to fail
        replacement_stream: Stream = Stream.based_on(
            original_stream,
            new_id=original_stream.id + "_2",
        )
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_stream,
                src_metafile=original_stream,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect subsequent deletes of the deleted stream to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_stream,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the deleted stream to fail
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

    def test_replace_stream(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_stream: Stream = commit_results[3][1]

        # given a transaction containing a stream replacement
        replacement_stream: Stream = Stream.based_on(
            original_stream,
            new_id=original_stream.id + "_2",
        )

        # expect the proposed replacement stream to be assigned a new ID
        assert replacement_stream.id != original_stream.id

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_stream,
                src_metafile=original_stream,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect two new stream metafiles to be written
        # (i.e., delete old stream, create replacement stream)
        assert len(write_paths) == 2
        delete_write_path = write_paths[0]
        create_write_path = write_paths[1]

        # expect the replacement stream to be successfully written and read
        assert TransactionOperationType.CREATE.value in create_write_path
        actual_stream = Stream.read(create_write_path)
        assert replacement_stream.equivalent_to(actual_stream)

        # expect the delete metafile to also contain the replacement stream
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_stream = Stream.read(delete_write_path)
        assert replacement_stream.equivalent_to(actual_stream)

        # expect old child metafiles for the replaced stream to remain readable
        child_metafiles_read_post_replace = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
        ]
        # expect old child metafiles read to share the same parent table name as
        # the replacement stream, but have a different parent stream ID
        for metafile in child_metafiles_read_post_replace:
            assert (
                metafile.table_name
                == replacement_stream.table_name
                == original_stream.table_name
            )
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_stream_id = ancestor_ids[3]
            assert parent_stream_id == original_stream.id

        # expect original child metafiles to share the original parent stream ID
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
            Partition(commit_results[4][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
            Partition(commit_results[4][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_stream_id = ancestor_ids[3]
            assert parent_stream_id == original_stream.id

        # expect a subsequent replace of the original stream to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_stream,
                src_metafile=original_stream,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect deletes of the original stream to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_stream,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the old stream to fail
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

    def test_delete_table_version(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_table_version: TableVersion = commit_results[2][1]

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_table_version,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect 1 new table version metafile to be written
        assert len(write_paths) == 1
        delete_write_path = write_paths[0]

        # expect the delete metafile to contain the input txn op dest_metafile
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_table_version = TableVersion.read(delete_write_path)
        assert original_table_version.equivalent_to(actual_table_version)

        # expect child metafiles in the deleted table version to remain readable and unchanged
        child_metafiles_read_post_delete = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
        ]
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
            Partition(commit_results[4][0]),
            Stream(commit_results[3][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
            Partition(commit_results[4][1]),
            Stream(commit_results[3][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_to_create[i]
            )
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_created[i]
            )

        # expect a subsequent replace of the deleted table version to fail
        replacement_table_version: TableVersion = TableVersion.based_on(
            original_table_version,
            new_id=original_table_version.id + "0",
        )
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_table_version,
                src_metafile=original_table_version,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect subsequent deletes of the deleted table version to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_table_version,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the deleted table version to fail
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

    def test_replace_table_version(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_table_version: TableVersion = commit_results[2][1]

        # given a transaction containing a table version replacement
        replacement_table_version: TableVersion = TableVersion.based_on(
            original_table_version,
            new_id=original_table_version.id + "0",
        )

        # expect the proposed replacement table version to be assigned a new ID
        assert replacement_table_version.id != original_table_version.id

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_table_version,
                src_metafile=original_table_version,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect two new table version metafiles to be written
        # (i.e., delete old table version, create replacement table version)
        assert len(write_paths) == 2
        delete_write_path = write_paths[0]
        create_write_path = write_paths[1]

        # expect the replacement table version to be successfully written and read
        assert TransactionOperationType.CREATE.value in create_write_path
        actual_table_version = TableVersion.read(create_write_path)
        assert replacement_table_version.equivalent_to(actual_table_version)

        # expect the delete metafile to also contain the replacement table version
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_table_version = TableVersion.read(delete_write_path)
        assert replacement_table_version.equivalent_to(actual_table_version)

        # expect old child metafiles for the replaced table version to remain readable
        child_metafiles_read_post_replace = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
        ]
        # expect old child metafiles read to share the same parent table name as
        # the replacement table version, but have a different parent table
        # version ID
        for metafile in child_metafiles_read_post_replace:
            assert (
                metafile.table_name
                == replacement_table_version.table_name
                == original_table_version.table_name
            )
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_table_version_id = ancestor_ids[2]
            assert parent_table_version_id == original_table_version.id

        # expect original child metafiles to share the original parent table version ID
        original_child_metafiles_to_create = [
            Delta(commit_results[5][0]),
            Partition(commit_results[4][0]),
            Stream(commit_results[3][0]),
        ]
        original_child_metafiles_created = [
            Delta(commit_results[5][1]),
            Partition(commit_results[4][1]),
            Stream(commit_results[3][1]),
        ]
        for i in range(len(original_child_metafiles_to_create)):
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_table_version_id = ancestor_ids[2]
            assert parent_table_version_id == original_table_version.id

        # expect a subsequent replace of the original table version to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_table_version,
                src_metafile=original_table_version,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect deletes of the original table version to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_table_version,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect new child metafile creation under the old table version to fail
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

    def test_delete_table(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_table: Table = commit_results[1][1]

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_table,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect 1 new table metafile to be written
        assert len(write_paths) == 1
        delete_write_path = write_paths[0]

        # expect the delete metafile to contain the input txn op dest_metafile
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_table = Table.read(delete_write_path)
        assert original_table.equivalent_to(actual_table)

        # expect child metafiles in the deleted table to remain readable and unchanged
        child_metafiles_read_post_delete = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
            TableVersion.read(commit_results[2][2]),
        ]
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
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_to_create[i]
            )
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_created[i]
            )

        # expect a subsequent replace of the deleted table to fail
        replacement_table: Table = Table.based_on(original_table)
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

        # expect subsequent deletes of the deleted table to fail
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

        # expect new child metafile creation under the deleted table to fail
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

    def test_replace_table(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
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
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect two new table metafiles to be written
        # (i.e., delete old table, create replacement table)
        assert len(write_paths) == 2
        delete_write_path = write_paths[0]
        create_write_path = write_paths[1]

        # expect the replacement table to be successfully written and read
        assert TransactionOperationType.CREATE.value in create_write_path
        actual_table = Table.read(create_write_path)
        assert replacement_table.equivalent_to(actual_table)

        # expect the delete metafile to also contain the replacement table
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_table = Table.read(delete_write_path)
        assert replacement_table.equivalent_to(actual_table)

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

    def test_delete_namespace(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_namespace: Namespace = commit_results[0][1]

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.DELETE,
                dest_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.DELETE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect 1 new namespace metafile to be written
        assert len(write_paths) == 1
        delete_write_path = write_paths[0]

        # expect the delete metafile to contain the input txn op dest_metafile
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_namespace = Namespace.read(delete_write_path)
        assert original_namespace.equivalent_to(actual_namespace)

        # expect child metafiles in the deleted namespace to remain readable and unchanged
        child_metafiles_read_post_delete = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
            TableVersion.read(commit_results[2][2]),
            Table.read(commit_results[1][2]),
        ]
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
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_to_create[i]
            )
            assert child_metafiles_read_post_delete[i].equivalent_to(
                original_child_metafiles_created[i]
            )

        # expect a subsequent replace of the deleted namespace to fail
        replacement_namespace: Namespace = Namespace.based_on(original_namespace)
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_namespace,
                src_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect subsequent deletes of the deleted namespace to fail
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

        # expect new child metafile creation under the deleted namespace to fail
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

    def test_replace_namespace(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
        original_namespace: Namespace = commit_results[0][1]

        # given a transaction containing a namespace replacement
        replacement_namespace: Namespace = Namespace.based_on(original_namespace)

        # expect the proposed replacement namespace to be assigned a new ID, but
        # continue to have the same name as the original namespace
        assert replacement_namespace.id != original_namespace.id
        assert replacement_namespace.namespace == original_namespace.namespace

        txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_namespace,
                src_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=txn_operations,
        )
        # when the transaction is committed
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect two new namespace metafiles to be written
        # (i.e., delete old namespace, create replacement namespace)
        assert len(write_paths) == 2
        delete_write_path = write_paths[0]
        create_write_path = write_paths[1]

        # expect the replacement namespace to be successfully written and read
        assert TransactionOperationType.CREATE.value in create_write_path
        actual_namespace = Namespace.read(create_write_path)
        assert replacement_namespace.equivalent_to(actual_namespace)

        # expect the delete metafile to also contain the replacement namespace
        assert TransactionOperationType.DELETE.value in delete_write_path
        actual_namespace = Namespace.read(delete_write_path)
        assert replacement_namespace.equivalent_to(actual_namespace)

        # expect old child metafiles for the replaced namespace to remain readable
        child_metafiles_read_post_replace = [
            Delta.read(commit_results[5][2]),
            Partition.read(commit_results[4][2]),
            Stream.read(commit_results[3][2]),
            TableVersion.read(commit_results[2][2]),
            Table.read(commit_results[1][2]),
        ]
        # expect old child metafiles read to share the same parent namespace name as
        # the replacement namespace, but have a different parent namespace ID
        for metafile in child_metafiles_read_post_replace:
            assert (
                metafile.namespace
                == replacement_namespace.namespace
                == original_namespace.namespace
            )
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_namespace_id = ancestor_ids[0]
            assert parent_namespace_id == original_namespace.id

        # expect original child metafiles to share the original parent namespace ID
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
            ancestor_ids = metafile.ancestor_ids(catalog_root=temp_dir)
            parent_namespace_id = ancestor_ids[0]
            assert parent_namespace_id == original_namespace.id

        # expect a subsequent namespace replace of the original namespace to fail
        bad_txn_operations = [
            TransactionOperation.of(
                operation_type=TransactionOperationType.UPDATE,
                dest_metafile=replacement_namespace,
                src_metafile=original_namespace,
            )
        ]
        transaction = Transaction.of(
            txn_type=TransactionType.OVERWRITE,
            txn_operations=bad_txn_operations,
        )
        with pytest.raises(ValueError):
            transaction.commit(temp_dir)

        # expect namespace deletes of the original namespace to fail
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

        # expect new child metafile creation under the old namespace to fail
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
            assert expected.equivalent_to(actual)
        # given a transaction containing:

        # 1. a new table version in an existing table
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id=original_table_version_created.id + "0",
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
        write_paths, txn_log_path = transaction.commit(temp_dir)
        assert len(write_paths) == 2

    def test_table_rename_bad_order_txn_op_chaining(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
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
            new_id=original_table_version_created.id + "0",
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
        write_paths, txn_log_path = transaction.commit(temp_dir)
        assert len(write_paths) == 2

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
        write_paths, txn_log_path = transaction.commit(temp_dir)
        deserialized_namespace = Namespace.read(write_paths.pop())
        assert namespace.equivalent_to(deserialized_namespace)
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
            assert expected.equivalent_to(actual)
        # given a transaction that tries to create a single stream
        # in a table version that doesn't exist
        original_stream_created = Stream(commit_results[3][1])
        new_stream: Stream = Stream.based_on(
            other=original_stream_created,
            new_id="test_stream_id",
        )
        new_stream.table_version_locator.table_version = "missing_table_version.0"
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
            assert expected.equivalent_to(actual)
        # given a transaction that tries to create a single table version
        # in a namespace that doesn't exist
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id="test_table_version.1",
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
            assert expected.equivalent_to(actual)
        # given a transaction that tries to create a single table version
        # in a table that doesn't exist
        original_table_version_created = TableVersion(commit_results[2][1])
        new_table_version: TableVersion = TableVersion.based_on(
            other=original_table_version_created,
            new_id="test_table_version.1",
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
            assert expected.equivalent_to(actual)
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
            new_id=original_table_version_created.table_version + "0",
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
        write_paths, txn_log_path = transaction.commit(temp_dir)

        # expect the transaction to successfully create 5 new metafiles
        assert len(write_paths) == 5

        # expect the table to be successfully renamed
        actual_table = Table.read(write_paths[0])
        assert renamed_table.equivalent_to(actual_table)

        # expect the new table version in the renamed table to be
        # successfully created
        actual_table_version = TableVersion.read(write_paths[1])
        assert new_table_version_to_create.equivalent_to(actual_table_version)

        # expect the new stream in the new table version in the renamed
        # table to be successfully created
        actual_stream = Stream.read(write_paths[2])
        assert new_stream_to_create.equivalent_to(actual_stream)

        # expect the new partition in the new stream in the new table
        # version in the renamed table to be successfully created
        actual_partition = Partition.read(write_paths[3])
        assert new_partition_to_create.equivalent_to(actual_partition)

        # expect the new delta in the new partition in the new stream in
        # the new table version in the renamed table to be successfully
        # created
        actual_delta = Delta.read(write_paths[4])
        assert new_delta_to_create.equivalent_to(actual_delta)

    def test_rename_table(self, temp_dir):
        commit_results = _commit_single_delta_table(temp_dir)
        for expected, actual, _ in commit_results:
            assert expected.equivalent_to(actual)
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
        write_paths, txn_log_path = transaction.commit(temp_dir)

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
            assert expected.equivalent_to(actual)
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
        write_paths, txn_log_path = transaction.commit(temp_dir)

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
            assert expected.equivalent_to(actual)

    def test_namespace_serde(self, temp_dir):
        namespace_locator = NamespaceLocator.of(namespace="test_namespace")
        namespace = Namespace.of(locator=namespace_locator)
        # given a transaction that creates a single namespace
        write_paths, txn_log_path = Transaction.of(
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
        assert namespace.equivalent_to(deserialized_namespace)

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
        write_paths, txn_log_path = Transaction.of(
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
        assert table.equivalent_to(deserialized_table)

    def test_table_version_serde(self, temp_dir):
        table_version_locator = TableVersionLocator.at(
            namespace=None,
            table_name=None,
            table_version="test_table_version.1",
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
        write_paths, txn_log_path = Transaction.of(
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
        assert table_version.equivalent_to(deserialized_table_version)

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
        write_paths, txn_log_path = Transaction.of(
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
        assert stream.equivalent_to(deserialized_stream)

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
        write_paths, txn_log_path = Transaction.of(
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
        assert partition.equivalent_to(deserialized_partition)

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
        write_paths, txn_log_path = Transaction.of(
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
        assert delta.equivalent_to(deserialized_delta)

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
        write_paths, txn_log_path = Transaction.of(
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
        assert table.equivalent_to(deserialized_table)

    def test_metafile_read_bad_path(self, temp_dir):
        with pytest.raises(FileNotFoundError):
            Delta.read("foobar")
