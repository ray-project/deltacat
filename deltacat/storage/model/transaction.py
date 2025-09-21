from __future__ import annotations

import os
import copy
import time
import uuid
import logging
import posixpath
import threading
import contextvars
from collections import defaultdict

from types import TracebackType
from typing import Optional, List, Union, Tuple, Type, TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from deltacat.types.tables import Dataset

import msgpack
import pyarrow as pa
import pyarrow.fs
from pyarrow.fs import FileType

from deltacat.constants import (
    TXN_DIR_NAME,
    TXN_PART_SEPARATOR,
    RUNNING_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
    PAUSED_TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
    NANOS_PER_SEC,
)
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.types import (
    TransactionOperationType,
    TransactionState,
    TransactionStatus,
)
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import TableLocator
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.stream import StreamLocator
from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.model.delta import DeltaLocator
from deltacat.storage.model.metafile import (
    Metafile,
    MetafileRevisionInfo,
)
from deltacat.types.tables import (
    DatasetType,
    from_pyarrow,
)
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    list_directory,
    list_directory_partitioned,
    get_file_info,
    get_file_info_partitioned,
    write_file_partitioned,
    epoch_timestamp_partition_transform,
    parse_epoch_timestamp_partitions,
    absolute_path_to_relative,
)
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


# Context variable to store the current transaction
_current_transaction: contextvars.ContextVar[
    Optional[Transaction]
] = contextvars.ContextVar("current_transaction", default=None)


def get_current_transaction() -> Optional[Transaction]:
    """Get the currently active transaction from context."""
    return _current_transaction.get()


def set_current_transaction(transaction: Optional[Transaction]) -> contextvars.Token:
    """Set the current transaction in context, returns token for restoration."""
    return _current_transaction.set(transaction)


def setup_transaction(
    transaction: Optional[Transaction] = None,
    **kwargs,
) -> Tuple[Transaction, bool]:
    """
    Utility method to ensure a transaction exists and determine if it should be committed
    within the caller's context. Creates a new transaction if none is provided.

    Args:
        transaction: Optional existing transaction to use
        **kwargs: Additional arguments for catalog properties

    Returns:
        Tuple[Transaction, bool]: The transaction to use and whether to commit it
    """
    # Check for active transaction in context first
    if transaction is None:
        transaction = get_current_transaction()

    commit_transaction = transaction is None
    if commit_transaction:
        from deltacat.catalog.model.properties import get_catalog_properties

        catalog_properties = get_catalog_properties(**kwargs)
        transaction = Transaction.of().start(
            catalog_properties.root,
            catalog_properties.filesystem,
        )
    return transaction, commit_transaction


def transaction_log_dir_and_filesystem(
    catalog_name: Optional[str] = None,
) -> Tuple[str, pyarrow.fs.FileSystem]:
    """
    Get the transaction log directory and filesystem for the given catalog.

    Args:
        catalog_name: Name of the catalog to get the transaction log directory and filesystem for.
            If None, uses the default catalog.

    Returns:
        Tuple[str, pyarrow.fs.FileSystem]: The transaction log directory and filesystem for the given catalog.
    """
    # Get the catalog and its properties
    from deltacat.catalog.model.catalog import get_catalog

    catalog = get_catalog(catalog_name)
    catalog_properties = catalog.inner

    # Get transaction directory paths
    catalog_root_normalized, filesystem = resolve_path_and_filesystem(
        catalog_properties.root,
        catalog_properties.filesystem,
    )

    return posixpath.join(catalog_root_normalized, TXN_DIR_NAME), filesystem


def transaction(
    catalog_name: Optional[str] = None,
    as_of: Optional[int] = None,
    commit_message: Optional[str] = None,
) -> Transaction:
    """
    Start a new interactive transaction for the given catalog.

    Args:
        catalog_name: Optional name of the catalog to run the transaction against.
                     If None, uses the default catalog.
        as_of: Optional historic timestamp in nanoseconds since epoch.
                If provided, creates a read-only transaction that reads only transactions
                with end times strictly less than the specified timestamp.
        commit_message: Optional commit message to describe the transaction purpose.
                Helps with time travel functionality by providing context
                for each transaction when browsing transaction history.

    Returns:
        Transaction: A started interactive transaction ready for use with the given catalog.

    Example:
        # Read-write transaction with commit message
        with dc.transaction(commit_message="Initial data load for Q4 analytics") as txn:
            dc.write_to_table(data, "my_table")
            dc.write_to_table(more_data, "my_other_table")

        # Read-only historic transaction
        import time
        historic_time = time.time_ns() - 3600 * 1000000000  # 1 hour ago
        with dc.transaction(as_of=historic_time) as txn:
            # Only read operations allowed - provides snapshot as of historic_time
            data = dc.read_table("my_table")
    """
    from deltacat.catalog.model.catalog import get_catalog

    # Get the catalog and its properties
    catalog = get_catalog(catalog_name)
    catalog_properties = catalog.inner

    # Create interactive transaction
    if as_of is not None:
        # Create read-only historic transaction
        txn = Transaction.of(commit_message=commit_message).start(
            catalog_properties.root,
            catalog_properties.filesystem,
            historic_timestamp=as_of,
        )
    else:
        # Create regular read-write transaction
        txn = Transaction.of(commit_message=commit_message).start(
            catalog_properties.root, catalog_properties.filesystem
        )
        # Initialize the lazy transaction ID
        logger.info(f"Created transaction with ID: {txn.id}")

    # Store the catalog name in the transaction for later resolution in delegate functions
    txn.catalog_name = catalog_name
    return txn


def _read_txn(
    txn_log_dir: str,
    txn_status: TransactionStatus,
    transaction_id: str,
    filesystem: pyarrow.fs.FileSystem,
) -> Transaction:
    """
    Read a transaction ID with the expected status from the given root transaction log directory.

    Args:
        txn_log_dir: The directory containing the transaction log.
        txn_status: The expected status of the transaction.
        transaction_id: The ID of the transaction.
        filesystem: The filesystem to use for reading the transaction.

    Returns:
        Transaction: The transaction.
    """
    status_dir = posixpath.join(txn_log_dir, txn_status.dir_name())

    if txn_status in [TransactionStatus.SUCCESS, TransactionStatus.FAILED]:
        # Parse transaction ID to get start time for partitioning
        # FAILED transactions store metadata directly in partitioned files named with transaction ID
        # SUCCESS transactions are stored in partitioned directories
        start_time, _ = Transaction.parse_transaction_id(transaction_id)

        # Use get_file_info_partitioned to find the transaction directory
        try:
            file_info = get_file_info_partitioned(
                path=posixpath.join(status_dir, transaction_id),
                filesystem=filesystem,
                partition_value=start_time,
                partition_transform=epoch_timestamp_partition_transform,
            )
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Transaction with ID '{transaction_id}' and status '{txn_status}' not found."
            )
        if txn_status == TransactionStatus.SUCCESS:
            # The file_info should point to a directory
            if file_info.type != pyarrow.fs.FileType.Directory:
                raise FileNotFoundError(
                    f"Transaction directory for transaction ID '{transaction_id}' with status '{txn_status}' not found."
                )

            # List files in the transaction directory
            txn_files = list_directory(
                path=file_info.path,
                filesystem=filesystem,
                ignore_missing_path=True,
            )

            if not txn_files:
                raise FileNotFoundError(
                    f"No transaction file found for transaction ID '{transaction_id}' and status '{txn_status}'."
                )

            if len(txn_files) > 1:
                raise RuntimeError(
                    f"Expected 1 transaction file in '{file_info.path}', but found {len(txn_files)}"
                )

            # Get the transaction file path
            txn_file_path, _ = txn_files[0]

            # Read the transaction from the file
            return Transaction.read(txn_file_path, filesystem)
    else:
        # Other transaction types store files directly in status directory
        txn_file_path = posixpath.join(status_dir, transaction_id)

        try:
            file_info = get_file_info(txn_file_path, filesystem)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Transaction with ID '{transaction_id}' and status '{txn_status}' not found."
            )

    # The file_info should point to a file
    if file_info.type != pyarrow.fs.FileType.File:
        raise FileNotFoundError(
            f"Transaction file for transaction ID '{transaction_id}' with status '{txn_status}' not found."
        )

    # Read the transaction from the file
    return Transaction.read(file_info.path, filesystem)


def read_transaction(
    transaction_id: str,
    catalog_name: Optional[str] = None,
    status: TransactionStatus = TransactionStatus.SUCCESS,
) -> Transaction:
    """
    Read a transaction from the given catalog and transaction ID.
    """
    txn_log_dir, filesystem = transaction_log_dir_and_filesystem(catalog_name)
    return _read_txn(txn_log_dir, status, transaction_id, filesystem)


def transactions(
    catalog_name: Optional[str] = None,
    read_as: DatasetType = DatasetType.PYARROW,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: Optional[int] = 10,
    status_in: Iterable[TransactionStatus] = [TransactionStatus.SUCCESS],
) -> Dataset:
    """
    Query transaction history for a catalog.

    Args:
        catalog_name: Optional name of the catalog to query. If None, uses the default catalog.
        read_as: Dataset type to return results as. Defaults to DatasetType.PYARROW.
        start_time: Optional start timestamp in nanoseconds since epoch to filter transactions. Transactions whose start time is greater than or equal to this value will be included.
        end_time: Optional end timestamp in nanoseconds since epoch to filter transactions. Transactions whose start time is less than or equal to this value will be included.
        limit: Optional maximum number of transactions to return (most recent first). Defaults to 10.
        status_in: Optional iterable of transaction status types to include. Defaults to [TransactionStatus.SUCCESS].

    Returns:
        Dataset: Transaction history as the specified dataset type with columns:
                 - transaction_id: Unique transaction identifier
                 - commit_message: Optional user-provided commit message
                 - start_time: Transaction start timestamp (nanoseconds since epoch)
                 - end_time: Transaction end timestamp (nanoseconds since epoch, None for running)
                 - status: Transaction status (SUCCESS, RUNNING, FAILED, PAUSED)
                 - operation_count: Number of operations in the transaction
                 - operation_types: Comma-separated list of distinct operation types in the transaction
                 - namespace_count: Number of distinct namespaces affected by the transaction
                 - table_count: Number of distinct tables affected by the transaction
                 - table_version_count: Number of distinct table versions affected by the transaction
                 - stream_count: Number of distinct streams affected by the transaction
                 - partition_count: Number of distinct partitions affected by the transaction
                 - delta_count: Number of distinct deltas affected by the transaction

    Example:
        # Get recent successful transactions
        recent = dc.transactions(limit=20)

        # Get transactions for a specific time range
        import time
        hour_ago = time.time_ns() - 3600 * 1000000000
        recent_hour = dc.transactions(start_time=hour_ago)

        # Get transaction history as pandas DataFrame
        df = dc.transactions(read_as=dc.DatasetType.PANDAS)
    """
    # Validate inputs
    if limit is not None and limit <= 0:
        raise ValueError("limit must be greater than 0")

    # Set default read_as if not provided
    if read_as is None:
        read_as = DatasetType.PYARROW

    if not status_in:
        status_in = [TransactionStatus.SUCCESS]

    # Get transaction directory path and filesystem
    txn_log_dir, filesystem = transaction_log_dir_and_filesystem(catalog_name)

    # Collect transaction data
    transaction_records = {
        "transaction_id": [],
        "commit_message": [],
        "start_time": [],
        "end_time": [],
        "status": [],
        "operation_count": [],
        "operation_types": [],
        "namespace_count": [],
        "table_count": [],
        "table_version_count": [],
        "stream_count": [],
        "partition_count": [],
        "delta_count": [],
    }

    # Helper function to process transactions in a directory
    def process_transactions_in_directory(
        directory: str,
        expected_status: TransactionStatus,
    ):
        if expected_status in [TransactionStatus.SUCCESS, TransactionStatus.FAILED]:
            # list all success/failed transactions between start_time and end_time
            # Ensure partition_start_value <= partition_value constraint is maintained
            transaction_dir_paths_and_sizes = list_directory_partitioned(
                path=directory,
                filesystem=filesystem,
                partition_value=max(end_time or time.time_ns(), start_time or -1),
                partition_start_value=start_time,
                partition_transform=epoch_timestamp_partition_transform,
                partition_file_parser=lambda path: Transaction.parse_transaction_id(
                    posixpath.basename(path)
                )[0],
                partition_dir_parser=parse_epoch_timestamp_partitions,
                limit=limit,
                ignore_missing_path=True,
            )
            transaction_ids = {
                posixpath.basename(transaction_file_path)
                for transaction_file_path, _ in transaction_dir_paths_and_sizes
            }
        else:
            # other transactions still use flat structure
            file_info_and_sizes = list_directory(
                path=directory,
                filesystem=filesystem,
                ignore_missing_path=True,
            )
            # Extract transaction IDs from file paths
            transaction_ids = set()
            for file_path, _ in file_info_and_sizes:
                try:
                    txn_id = posixpath.basename(file_path)
                    transaction_id = Transaction.parse_transaction_id(txn_id)
                    transaction_ids.add(txn_id)
                except ValueError:
                    logger.warning(f"Skipping invalid transaction ID: {file_path}")
                    continue

        for transaction_id in transaction_ids:
            # Read the transaction from the file
            # TODO(pdames): Read transactions in parallel.
            try:
                txn = _read_txn(
                    txn_log_dir,
                    expected_status,
                    transaction_id,
                    filesystem,
                )
            except FileNotFoundError:
                # this may be a stray file or the transaction is being created - skip it
                logger.warning(f"Skipping missing transaction ID: {transaction_id}")
                continue
            except ValueError:
                # Skip invalid transaction IDs (like partition directories)
                logger.warning(f"Skipping invalid transaction ID: {transaction_id}")
                continue

            # Count operations and affected metadata objects by type.
            operation_count = len(txn.operations)
            operation_types = set()
            affected_namespaces = set()
            affected_tables = set()
            affected_table_versions = set()
            affected_streams = set()
            affected_partitions = set()
            affected_deltas = set()

            for op in txn.operations:
                operation_types.add(op.type)

                # Determine locator type and cast to appropriate locator class
                locator_dict = op.dest_metafile.get("locator", {})
                if "tableName" in locator_dict and "namespaceLocator" in locator_dict:
                    locator = TableLocator(locator_dict)
                elif "namespace" in locator_dict:
                    locator = NamespaceLocator(locator_dict)
                elif "tableVersion" in locator_dict:
                    locator = TableVersionLocator(locator_dict)
                elif "streamId" in locator_dict:
                    locator = StreamLocator(locator_dict)
                elif "partitionId" in locator_dict:
                    locator = PartitionLocator(locator_dict)
                elif "streamPosition" in locator_dict:
                    locator = DeltaLocator(locator_dict)
                else:
                    raise ValueError(
                        f"Unknown locator type from structure: {locator_dict}"
                    )

                # Extract distinct metafiles updated by common/alias name (e.g., a table rename impacts 2 tables instead of 1)
                if op.type in TransactionOperationType.write_operations():
                    if locator.namespace is not None:
                        affected_namespaces.add(locator.namespace)
                    if isinstance(locator, TableLocator):
                        affected_tables.add((locator.namespace, locator.table_name))
                    elif isinstance(locator, TableVersionLocator):
                        affected_table_versions.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                            )
                        )
                    elif isinstance(locator, StreamLocator):
                        affected_tables.add((locator.namespace, locator.table_name))
                        affected_table_versions.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                            )
                        )
                        affected_streams.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                                locator.stream_id,
                            )
                        )
                    elif isinstance(locator, PartitionLocator):
                        affected_tables.add((locator.namespace, locator.table_name))
                        affected_table_versions.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                            )
                        )
                        affected_streams.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                                locator.stream_id,
                            )
                        )
                        affected_partitions.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                                locator.stream_id,
                                locator.partition_id,
                            )
                        )
                    elif isinstance(locator, DeltaLocator):
                        affected_tables.add((locator.namespace, locator.table_name))
                        affected_table_versions.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                            )
                        )
                        affected_streams.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                                locator.stream_id,
                            )
                        )
                        affected_partitions.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                                locator.stream_id,
                                locator.partition_id,
                            )
                        )
                        affected_deltas.add(
                            (
                                locator.namespace,
                                locator.table_name,
                                locator.table_version,
                                locator.stream_id,
                                locator.partition_id,
                                locator.stream_position,
                            )
                        )

            # Create transaction record
            transaction_records["transaction_id"].append(txn.id)
            transaction_records["commit_message"].append(txn.commit_message)
            transaction_records["start_time"].append(txn.start_time)
            transaction_records["end_time"].append(txn.end_time)
            transaction_records["status"].append(expected_status)
            transaction_records["operation_count"].append(operation_count)
            transaction_records["operation_types"].append(operation_types)
            transaction_records["namespace_count"].append(len(affected_namespaces))
            transaction_records["table_count"].append(len(affected_tables))
            transaction_records["table_version_count"].append(
                len(affected_table_versions)
            )
            transaction_records["stream_count"].append(len(affected_streams))
            transaction_records["partition_count"].append(len(affected_partitions))
            transaction_records["delta_count"].append(len(affected_deltas))

    for status in status_in:
        dir_path = posixpath.join(txn_log_dir, status.dir_name())
        process_transactions_in_directory(dir_path, status)

    # Sort by start_time descending (most recent first)
    # Convert to list of records, sort, then convert back
    if transaction_records["transaction_id"]:  # Only sort if we have records
        # Create list of tuples (start_time, record_index)
        sorted_indices = sorted(
            range(len(transaction_records["start_time"])),
            key=lambda i: transaction_records["start_time"][i] or 0,
            reverse=True,
        )

        # Reorder all columns based on sorted indices
        for key in transaction_records:
            transaction_records[key] = [
                transaction_records[key][i] for i in sorted_indices
            ]

    # Apply limit
    # TODO(pdames): Apply limit during listing (pyarrow fs doesn't support limits natively).
    if limit is not None and limit > 0:
        for key in transaction_records:
            transaction_records[key] = transaction_records[key][:limit]

    # Convert to requested dataset type
    return from_pyarrow(pa.Table.from_pydict(transaction_records), read_as)


class TransactionTimeProvider:
    """
    Provider interface for transaction start and end times. An ideal
    transaction time provider is externally consistent (e.g.,
    https://cloud.google.com/spanner/docs/true-time-external-consistency),
    such that:
      1. A transaction start time is never less than a previously completed
      transaction's end time.
      2. A transaction end time is never less than an in-progress
      transaction's start time.
      3. Every transaction has a unique start and end time.
      4. Start/end time assignment is non-blocking.
    """

    def start_time(self) -> int:
        raise NotImplementedError("start_time not implemented")

    def end_time(self) -> int:
        raise NotImplementedError("end_time not implemented")


class TransactionSystemTimeProvider(TransactionTimeProvider):
    """
    A local transaction time provider that returns the current system clock
    epoch time in nanoseconds. Ensures that all local transaction start
    times are greater than all last known end times, and that all known end
    times are no less than all last known start time across all local threads
    using this time provider.

    Note that this time provider gives no external consistency guarantees due
    to potential clock skew between distributed nodes writing to the same
    catalog, and is only recommended for use with local catalogs.
    """

    last_known_start_times = defaultdict(int)
    last_known_end_times = defaultdict(int)

    # don't wait more than 60 seconds for the system clock to catch up
    # between transactions (assumed to be indicative of a larger system
    # clock change made between transactions)
    max_sync_wait_time = 60 * NANOS_PER_SEC

    def start_time(self) -> int:
        """
        Gets the current system time in nanoseconds since the epoch. Ensures
        that the start time returned is greater than the last known end time
        recorded at the time this method is invoked.
        :return: Current epoch time in nanoseconds.
        """
        # ensure serial transactions in a single process have start times after
        # the last known end time
        last_known_end_times = self.last_known_end_times.values() or [0]
        max_known_end_time = max(last_known_end_times)

        elapsed_start_time = time.monotonic_ns()
        current_time = time.time_ns()
        while current_time <= max_known_end_time:
            elapsed_time = time.monotonic_ns() - elapsed_start_time
            if elapsed_time > self.max_sync_wait_time:
                raise TimeoutError(
                    f"Failed to sync cross-transaction system clock time after "
                    f"{self.max_sync_wait_time / NANOS_PER_SEC} seconds, "
                    f"aborting."
                )
            time.sleep(0.000001)
            current_time = time.time_ns()

        # update the current thread's last known end time
        pid = os.getpid()
        tid = threading.current_thread().ident
        current_thread_time_key = (pid, tid)
        self.last_known_end_times[current_thread_time_key] = current_time

        return current_time

    def end_time(self) -> int:
        """
        Gets the current system time in nanoseconds since the epoch. Ensures
        that the end time returned is no less than the last known start time
        recorded at the time this method is invoked.
        :return: Current epoch time in nanoseconds.
        """
        # ensure serial transactions in a single process have end times no less
        # than the last known start time
        last_known_start_times = self.last_known_start_times.values() or [0]
        last_start_time = max(last_known_start_times)

        elapsed_start_time = time.monotonic_ns()
        current_time = time.time_ns()
        while current_time < last_start_time:
            elapsed_time = time.monotonic_ns() - elapsed_start_time
            if elapsed_time > self.max_sync_wait_time:
                raise TimeoutError(
                    f"Failed to sync cross-transaction system clock time after "
                    f"{self.max_sync_wait_time / NANOS_PER_SEC} seconds, "
                    f"aborting."
                )
            time.sleep(0.000001)
            current_time = time.time_ns()

        # update the current thread's last known end time
        pid = os.getpid()
        tid = threading.current_thread().ident
        current_thread_time_key = (pid, tid)
        self.last_known_start_times[current_thread_time_key] = current_time

        return current_time


class TransactionHistoricTimeProvider(TransactionTimeProvider):
    """
    A transaction time provider that returns a fixed historic timestamp
    for read-only transactions. This enables MVCC snapshot isolation
    as-of the specified timestamp.
    """

    def __init__(
        self,
        historic_timestamp: int,
        base_time_provider: TransactionTimeProvider,
    ):
        """
        Initialize with a fixed historic timestamp and a base time provider.

        Args:
            historic_timestamp: Timestamp in nanoseconds since epoch to use
                              for both start and end times.
            base_time_provider: Time provider to use for the end time.
        """
        # Validate that historic timestamp is not in the future
        if historic_timestamp > base_time_provider.start_time():
            raise ValueError(
                f"Historic timestamp {historic_timestamp} cannot be set in the future."
            )
        self.base_time_provider = base_time_provider
        self.historic_timestamp = historic_timestamp

    def start_time(self) -> int:
        """
        Returns the fixed historic timestamp.
        """
        return self.historic_timestamp

    def end_time(self) -> int:
        """
        Returns the end time of the base time provider.
        """
        return self.base_time_provider.end_time()


class TransactionOperation(dict):
    """
    Base class for DeltaCAT transaction operations against individual metafiles.
    """

    @staticmethod
    def of(
        operation_type: Optional[TransactionOperationType],
        dest_metafile: Metafile,
        src_metafile: Optional[Metafile] = None,
        read_limit: Optional[int] = None,
    ) -> TransactionOperation:
        if not dest_metafile:
            raise ValueError("Transaction operations must have a destination metafile.")
        if operation_type in [
            TransactionOperationType.UPDATE,
            TransactionOperationType.REPLACE,
        ]:
            if not src_metafile:
                raise ValueError(
                    f"{operation_type.value} transaction operations must have a source metafile."
                )
            elif type(dest_metafile) is not type(src_metafile):
                raise ValueError(
                    f"Source metafile type `{type(src_metafile)}` is not "
                    f"equal to dest metafile type `{type(dest_metafile)}`."
                )
        elif src_metafile:
            raise ValueError(
                f"Only {TransactionOperationType.UPDATE.value} and {TransactionOperationType.REPLACE.value} transaction operations may have a source metafile."
            )
        if operation_type.is_write_operation() and read_limit:
            raise ValueError(
                f"Only {TransactionOperationType.READ.value} transaction operations may have a read limit."
            )
        txn_op = TransactionOperation()
        txn_op.type = operation_type
        txn_op.dest_metafile = dest_metafile
        txn_op.src_metafile = src_metafile
        txn_op.read_limit = read_limit
        return txn_op

    @property
    def type(self) -> TransactionOperationType:
        """
        Returns the type of the transaction operation.
        """
        val = self["type"]
        if val is not None and not isinstance(val, TransactionOperationType):
            self["type"] = val = TransactionOperationType(val)
        return val

    @type.setter
    def type(self, txn_op_type: TransactionOperationType):
        self["type"] = txn_op_type

    @property
    def dest_metafile(self) -> Metafile:
        """
        Returns the metafile that is the target of this transaction operation.
        """
        val = self["dest_metafile"]
        if val is not None and not isinstance(val, Metafile):
            self["dest_metafile"] = val = Metafile(val)
        return val

    @dest_metafile.setter
    def dest_metafile(self, metafile: Metafile):
        self["dest_metafile"] = metafile

    @property
    def src_metafile(self) -> Optional[Metafile]:
        """
        Returns the metafile that is the source of this transaction operation.
        """
        val = self.get("src_metafile")
        if val is not None and not isinstance(val, Metafile):
            self["src_metafile"] = val = Metafile(val)
        return val

    @src_metafile.setter
    def src_metafile(self, src_metafile: Optional[Metafile]):
        self["src_metafile"] = src_metafile

    @property
    def read_limit(self) -> Optional[int]:
        """
        Returns the read limit for this transaction operation.
        """
        return self.get("read_limit")

    @read_limit.setter
    def read_limit(self, read_limit: Optional[int]):
        self["read_limit"] = read_limit

    @property
    def metafile_write_paths(self) -> List[str]:
        return self.get("metafile_write_paths") or []

    @property
    def locator_write_paths(self) -> List[str]:
        return self.get("locator_write_paths") or []

    def append_metafile_write_path(self, write_path: str):
        metafile_write_paths = self.get("metafile_write_paths")
        if not metafile_write_paths:
            metafile_write_paths = self["metafile_write_paths"] = []
        metafile_write_paths.append(write_path)

    def append_locator_write_path(self, write_path: str):
        locator_write_paths = self.get("locator_write_paths")
        if not locator_write_paths:
            locator_write_paths = self["locator_write_paths"] = []
        locator_write_paths.append(write_path)

    @metafile_write_paths.setter
    def metafile_write_paths(self, write_paths: List[str]) -> None:
        self["metafile_write_paths"] = write_paths

    @locator_write_paths.setter
    def locator_write_paths(self, write_paths: List[str]):
        self["locator_write_paths"] = write_paths


class TransactionOperationList(List[TransactionOperation]):
    @staticmethod
    def of(items: List[TransactionOperation]) -> TransactionOperationList:
        typed_items = TransactionOperationList()
        for item in items:
            if item is not None and not isinstance(item, TransactionOperation):
                item = TransactionOperation(item)
            typed_items.append(item)
        return typed_items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, TransactionOperation):
            self[item] = val = TransactionOperation(val)
        return val

    def __iter__(self):
        """Support enumeration by returning TransactionOperation objects."""
        for i in range(len(self)):
            yield self[i]  # This triggers __getitem__ conversion


class Transaction(dict):
    """
    Base class for DeltaCAT transactions.
    """

    @staticmethod
    def of(
        txn_operations: Optional[TransactionOperationList] = None,
        commit_message: Optional[str] = None,
    ) -> Transaction:
        if txn_operations is None:
            txn_operations = []
        transaction = Transaction()
        transaction.operations = txn_operations
        transaction.interactive = len(txn_operations) == 0
        if commit_message:
            transaction.commit_message = commit_message
        return transaction

    @staticmethod
    def read_end_time(
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Optional[int]:
        """
        Returns the end time of the transaction, or None if the transaction
        log file does not exist.
        :param path: Transaction log path to read.
        :param filesystem: File system to use for reading the Transaction file.
        :return: Deserialized object from the Transaction file.
        """
        # TODO(pdames): Validate that input file path is a valid txn log.
        if not filesystem:
            path, filesystem = resolve_path_and_filesystem(path, filesystem)
        file_info_and_sizes = list_directory(
            path=path,
            filesystem=filesystem,
            ignore_missing_path=True,
        )
        end_time = None
        if file_info_and_sizes:
            if len(file_info_and_sizes) > 1:
                raise ValueError(
                    f"Expected to find only one transaction log at {path}, "
                    f"but found {len(file_info_and_sizes)}"
                )
            end_time = Transaction._parse_end_time(file_info_and_sizes[0][0])
        return end_time

    @staticmethod
    def _parse_end_time(txn_log_file_name_or_path: str) -> int:
        return int(posixpath.basename(txn_log_file_name_or_path))

    @classmethod
    def read(
        cls,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Transaction:
        """
        Read a Transaction file and return the deserialized object.
        :param path: Transaction file path to read.
        :param filesystem: File system to use for reading the Transaction file.
        :return: Deserialized object from the Transaction file.
        """

        if not filesystem:
            path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_stream(path) as file:
            binary = file.readall()
        obj = cls(**msgpack.loads(binary))
        return obj

    @staticmethod
    def read_time_provider(provider_name: str):
        """
        Given the string name of a time provider class, return a new instance of it.
        Raises ValueError if the provider name is unknown.
        """
        TIME_PROVIDER_CLASSES = {
            "TransactionSystemTimeProvider": TransactionSystemTimeProvider,
            # Add additional mappings as needed
        }

        provider_cls = TIME_PROVIDER_CLASSES.get(provider_name)
        if provider_cls is None:
            raise ValueError(f"Unknown time provider: {provider_name}")

        return provider_cls()

    @property
    def id(self) -> Optional[str]:
        """
        Returns this transaction's unique ID assigned at commit start time, or
        None if the unique ID has not yet been assigned.
        """
        _id = self.get("id")
        if not _id and self.start_time:
            _id = self["id"] = f"{self.start_time}{TXN_PART_SEPARATOR}{uuid.uuid4()}"
        return _id

    def state(self, catalog_root_dir: str, filesystem: pyarrow.fs.FileSystem = None):
        """
        Infer the transaction state based on its presence in different directories.
        """

        txn_name = self.id

        catalog_root_normalized, filesystem = resolve_path_and_filesystem(
            catalog_root_dir
        )

        txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
        running_txn_log_dir = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME)
        filesystem.create_dir(running_txn_log_dir, recursive=True)
        failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
        filesystem.create_dir(failed_txn_log_dir, recursive=False)
        success_txn_log_dir = posixpath.join(txn_log_dir, SUCCESS_TXN_DIR_NAME)
        filesystem.create_dir(success_txn_log_dir, recursive=False)
        paused_txn_log_dir = posixpath.join(txn_log_dir, PAUSED_TXN_DIR_NAME)
        filesystem.create_dir(paused_txn_log_dir, recursive=False)

        # Check if the transaction file exists in the failed directory (including partitioned paths)
        failed_txn_path = Transaction.failed_txn_log_path(failed_txn_log_dir, txn_name)
        in_failed = filesystem.get_file_info(failed_txn_path).type != FileType.NotFound

        # Check if the transaction file exists in the running directory (flat structure)
        running_txn_path = posixpath.join(running_txn_log_dir, txn_name)
        in_running = (
            filesystem.get_file_info(running_txn_path).type != FileType.NotFound
        )

        # Check if the transaction file exists in the success directory (including partitioned paths)
        success_txn_path = Transaction.success_txn_log_dir_path(
            success_txn_log_dir, txn_name
        )
        in_success = (
            filesystem.get_file_info(success_txn_path).type != FileType.NotFound
        )

        # Check if the transaction file exists in the paused directory (flat structure)
        paused_txn_path = posixpath.join(paused_txn_log_dir, txn_name)
        in_paused = filesystem.get_file_info(paused_txn_path).type != FileType.NotFound

        if in_failed and in_running:
            return TransactionState.FAILED
        elif in_failed and not in_running:
            return TransactionState.PURGED
        elif in_success:
            return TransactionState.SUCCESS
        elif in_running:
            return TransactionState.RUNNING
        elif in_paused:
            return TransactionState.PAUSED

    @property
    def operations(self) -> TransactionOperationList:
        """
        Returns the list of transaction operations.
        """
        return TransactionOperationList(self["operations"])

    @operations.setter
    def operations(self, operations: TransactionOperationList):
        self["operations"] = operations

    @property
    def metafile_write_paths(self) -> List[str]:
        return [path for op in self.operations for path in op.metafile_write_paths]

    @property
    def locator_write_paths(self) -> List[str]:
        return [path for op in self.operations for path in op.locator_write_paths]

    @property
    def catalog_root_normalized(self) -> str:
        """
        Returns the catalog_root_normalized for this transaction.
        """
        return self.get("catalog_root_normalized")

    @catalog_root_normalized.setter
    def catalog_root_normalized(self, path: str):
        self["catalog_root_normalized"] = path

    @property
    def _time_provider(self) -> TransactionSystemTimeProvider:
        """
        Returns the time_provider of the transaction.
        """
        return self.get("_time_provider")

    @_time_provider.setter
    def _time_provider(
        self, tp: TransactionSystemTimeProvider
    ) -> TransactionSystemTimeProvider:
        self["_time_provider"] = tp

    @property
    def start_time(self) -> Optional[int]:
        """
        Returns the start time of the transaction.
        """
        return self.get("start_time")

    @property
    def pause_time(self) -> Optional[int]:
        """
        Returns the last pause time of the transaction.
        """
        return self.get("pause_time")

    @property
    def end_time(self) -> Optional[int]:
        """
        Returns the end time of the transaction.
        """
        return self.get("end_time")

    @property
    def commit_message(self) -> Optional[str]:
        """
        Returns the commit message for the transaction.
        """
        return self.get("commit_message")

    @commit_message.setter
    def commit_message(self, message: str):
        """
        Sets the commit message for the transaction.
        """
        self["commit_message"] = message

    @property
    def historic_timestamp(self) -> Optional[int]:
        """
        Returns the historic timestamp for the transaction.
        """
        return self.get("historic_timestamp")

    @historic_timestamp.setter
    def historic_timestamp(self, timestamp: int):
        """
        Sets the historic timestamp for the transaction.
        """
        self["historic_timestamp"] = timestamp

    @property
    def catalog_name(self) -> Optional[str]:
        """
        Returns the catalog name for the transaction.
        """
        return self.get("catalog_name")

    @catalog_name.setter
    def catalog_name(self, name: str):
        """
        Sets the catalog name for the transaction.
        """
        self["catalog_name"] = name

    def _mark_start_time(self, time_provider: TransactionTimeProvider) -> int:
        """
        Sets the start time of the transaction using the given
        TransactionTimeProvider. Raises a runtime error if the transaction
        start time has already been set by a previous commit.
        """
        if self.get("start_time"):
            raise RuntimeError("Cannot restart a previously started transaction.")
        start_time = self["start_time"] = time_provider.start_time()
        return start_time

    def _mark_end_time(self, time_provider: TransactionTimeProvider) -> int:
        """
        Sets the end time of the transaction using the given
        TransactionTimeProvider. Raises a runtime error if the transaction end
        time has already been set by a previous commit, or if the transaction
        start time has not been set.
        """
        if not self.get("start_time"):
            raise RuntimeError("Cannot end an unstarted transaction.")
        if self.get("end_time"):
            raise RuntimeError("Cannot end a completed transaction.")
        end_time = self["end_time"] = time_provider.end_time()
        return end_time

    def _mark_pause_time(self, time_provider: TransactionTimeProvider) -> int:
        """
        Sets the pause time of the transaction using the given
        TransactionTimeProvider. Raises a runtime error if the transaction pause
        time has already been set by a previous commit, or if the transaction
        start time has not been set.
        """
        if not self.get("start_time"):
            raise RuntimeError("Cannot pause an unstarted transaction.")
        if self.get("end_time"):
            raise RuntimeError("Cannot pause a completed transaction.")
        pause_time = self["pause_time"] = time_provider.end_time()
        return pause_time

    @staticmethod
    def _abs_txn_meta_path_to_relative(root: str, target: str) -> str:
        return absolute_path_to_relative(root, target)

    def relativize_operation_paths(
        self, operation: TransactionOperation, catalog_root: str
    ) -> None:
        """
        Converts all absolute paths in an operation to relative paths
        with respect to the catalog root directory.
        """
        # handle metafile paths
        if operation.metafile_write_paths:
            metafile_write_paths = [
                Transaction._abs_txn_meta_path_to_relative(catalog_root, path)
                for path in operation.metafile_write_paths
            ]
            operation.metafile_write_paths = metafile_write_paths
        # handle locator paths
        if operation.locator_write_paths:
            locator_write_paths = [
                Transaction._abs_txn_meta_path_to_relative(catalog_root, path)
                for path in operation.locator_write_paths
            ]
            operation.locator_write_paths = locator_write_paths

    def to_serializable(self, catalog_root) -> Transaction:
        """
        Prepare the object for serialization by converting any non-serializable
        types to serializable types. May also run any required pre-write
        validations on the serialized or deserialized object.
        :return: a serializable version of the object
        """
        # Only copy dictionary keys - all other members should not be serialized
        serializable = Transaction({})
        for key, value in self.items():
            serializable[key] = copy.deepcopy(value)

        # remove all src/dest metafile contents except IDs and locators to
        # reduce file size (they can be reconstructed from their corresponding
        # files as required).
        for operation in serializable.operations:
            # Sanity check that IDs exist on source and dest metafiles
            if operation.dest_metafile and operation.dest_metafile.id is None:
                raise ValueError(
                    f"Transaction operation ${operation} dest metafile does "
                    f"not have ID: ${operation.dest_metafile}"
                )
            if operation.src_metafile and operation.src_metafile.id is None:
                raise ValueError(
                    f"Transaction operation ${operation} src metafile does "
                    f"not have ID: ${operation.src_metafile}"
                )
            # relativize after checking that dest and src metafiles are valid
            self.relativize_operation_paths(operation, catalog_root)
            operation.dest_metafile = {
                "id": operation.dest_metafile.id,
                "locator": operation.dest_metafile.locator,
                "locator_alias": operation.dest_metafile.locator_alias,
            }
            if operation.src_metafile:
                operation.src_metafile = {
                    "id": operation.src_metafile.id,
                    "locator": operation.src_metafile.locator,
                    "locator_alias": operation.src_metafile.locator_alias,
                }
        # TODO(pdames): Ensure that all file paths recorded are relative to the
        #  catalog root.

        # TODO: check if we care about order or exact time stamps --> pickling time_provider?
        # serializable.pop("_time_provider", None)

        serializable["_time_provider"] = {
            "type": type(self._time_provider).__name__,
            "params": {},
        }

        serializable.catalog_root_normalized = self.catalog_root_normalized

        return serializable

    @staticmethod
    def parse_transaction_id(transaction_id: str) -> Tuple[int, str]:
        """
        Parse a transaction ID to extract the start time and transaction UUID.

        Args:
            transaction_id: The transaction ID in format "start_time_uuid"

        Returns:
            Tuple of (start_time: int, transaction_uuid: str)

        Raises:
            ValueError: If the transaction ID format is invalid
        """
        txn_log_parts = transaction_id.split(TXN_PART_SEPARATOR)
        if len(txn_log_parts) != 2:
            raise ValueError(
                f"Transaction ID `{transaction_id}` does not have the expected format "
                f"'start_time{TXN_PART_SEPARATOR}uuid'."
            )

        # ensure that the transaction start time is valid
        try:
            start_time = int(txn_log_parts[0])
        except ValueError as e:
            raise ValueError(
                f"Transaction ID `{transaction_id}` does not contain a valid start time."
            ) from e

        # ensure that the txn uuid is valid
        txn_uuid_str = txn_log_parts[1]
        try:
            uuid.UUID(txn_uuid_str)
        except ValueError as e:
            raise ValueError(
                f"Transaction ID `{transaction_id}` does not contain a valid UUID string."
            ) from e

        return start_time, txn_uuid_str

    @staticmethod
    def _partitioned_txn_log_path(txn_log_dir: str, txn_id: str) -> str:
        """
        Construct the partitioned path for a transaction log.
        """
        start_time, _ = Transaction.parse_transaction_id(txn_id)
        partition_dirs = epoch_timestamp_partition_transform(start_time)
        txn_log_path = txn_log_dir
        for dir_name in partition_dirs:
            txn_log_path = posixpath.join(txn_log_path, dir_name)
        txn_log_path = posixpath.join(txn_log_path, txn_id)
        return txn_log_path

    @staticmethod
    def success_txn_log_dir_path(success_txn_log_dir: str, txn_id: str) -> str:
        """
        Construct the partitioned path for a success transaction log's parent directory.

        Args:
            success_txn_log_dir: Base directory for success transaction logs
            txn_id: Transaction ID in format "start_time_uuid"

        Returns:
            Full partitioned path to the transaction log's parent directory
        """
        return Transaction._partitioned_txn_log_path(success_txn_log_dir, txn_id)

    @staticmethod
    def failed_txn_log_path(failed_txn_log_dir: str, txn_id: str) -> str:
        """
        Construct the partitioned path for a failed transaction log.

        Args:
            failed_txn_log_dir: Base directory for failed transaction logs
            txn_id: Transaction ID in format "start_time_uuid"

        Returns:
            Full partitioned path to the transaction log file
        """
        return Transaction._partitioned_txn_log_path(failed_txn_log_dir, txn_id)

    @staticmethod
    def _validate_txn_log_file(success_txn_log_file: str) -> None:
        txn_log_dir_name = posixpath.basename(posixpath.dirname(success_txn_log_file))
        # Use the new parse_transaction_id function
        start_time, _ = Transaction.parse_transaction_id(txn_log_dir_name)
        # ensure that the transaction end time is valid
        try:
            end_time = Transaction._parse_end_time(success_txn_log_file)
        except ValueError as e:
            raise ValueError(
                f"Transaction log file `{success_txn_log_file}` does not "
                f"contain a valid end time."
            ) from e
        # ensure transaction end time was not recorded before start time
        if end_time < start_time:
            raise OSError(
                f"Transaction end time {end_time} is earlier than start "
                f"time {start_time}! To preserve catalog integrity, the "
                f"corresponding completed transaction log at "
                f"`{success_txn_log_file}` has been removed."
            )

    def commit(
        self,
        catalog_root_dir: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Union[
        List[ListResult[Metafile]],
        Tuple[List[str], str],
        Tuple[List["ListResult[Metafile]"], List[str], str],
    ]:
        """
        Legacy wrapper that preserves the original `commit()` contract while
        delegating the heavy lifting to the incremental helpers.

        Returns
        -------
        - For READ transactions:  List[ListResult[Metafile]]
        - For WRITE transactions: Tuple[List[str], str]
            (list of successful write-paths, path to success-txn log file)
        - For mixed READ/WRITE transactions: Tuple[List["ListResult[Metafile]"], List[str], str]
        """

        if hasattr(self, "interactive") and self.interactive:
            raise RuntimeError(
                "Cannot commit an interactive transaction. Use transaction.start(),transaction.step(), and transaction.seal() instead."
            )

        if self.operations and len(self.operations) > 0:
            # Start a working copy (deep-copy, directory scaffolding, start-time, running/failed/success/paused dirs …)
            txn_active = self.start(catalog_root_dir, filesystem)  # deep copy
            # Sequentially execute every TransactionOperation
            for op in txn_active.operations:
                txn_active.step(op)
        return txn_active._seal_steps()

    def start(
        self,
        catalog_root_dir: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        historic_timestamp: Optional[int] = None,
    ) -> "Transaction":
        """
        Create directory scaffolding, timestamp the txn, and return a DEEP COPY
        that the caller should use for all subsequent calls to step(), pause(),
        and seal().  The original object remains read-only.

        Args:
            catalog_root_dir: Root directory for the catalog
            filesystem: Optional filesystem to use
            historic_timestamp: Optional timestamp in nanoseconds since epoch for snapshot isolation
        """
        # Create a deep copy
        txn: "Transaction" = copy.deepcopy(self)

        # Set up time provider based on transaction type
        if historic_timestamp is not None:
            # Use historic time provider for snapshot isolation
            # TODO(pdames): Set base time provider to the catalog's configured time provider when more than one is supported.
            txn._time_provider = TransactionHistoricTimeProvider(
                historic_timestamp,
                TransactionSystemTimeProvider(),
            )
            txn.historic_timestamp = historic_timestamp
        else:
            # Use system time provider for regular transactions
            txn._time_provider = TransactionSystemTimeProvider()

        txn._mark_start_time(txn._time_provider)  # start time on deep_copy

        # Set up filesystem and directories
        catalog_root_normalized, filesystem = resolve_path_and_filesystem(
            catalog_root_dir,
            filesystem,
        )
        txn.catalog_root_normalized = catalog_root_normalized
        txn._filesystem = filesystem  # keep for pause/resume
        txn.running_log_written = False  # internal flags
        txn._list_results = []

        # Make sure txn/ directories exist (idempotent)
        txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
        filesystem.create_dir(
            posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME),
            recursive=True,
        )
        for subdir in (FAILED_TXN_DIR_NAME, SUCCESS_TXN_DIR_NAME, PAUSED_TXN_DIR_NAME):
            try:
                filesystem.create_dir(
                    posixpath.join(txn_log_dir, subdir),
                    recursive=False,
                )
            except FileExistsError:
                pass  # allowed when catalog already initialised
        return txn

    def step(
        self,
        operation: "TransactionOperation",
    ) -> Union[ListResult[Metafile], Tuple[List[str], List[str]]]:
        """
        Executes a single transaction operation.

        Parameters
        ----------
        operation: TransactionOperation
            The transaction operation to execute.

        Returns
        -------
        - For READ transaction operation: ListResult[Metafile]
        - For WRITE transaction operation: Tuple[List[str], List[str]]
            (list of successful write-paths, list of successful locator write-paths)
        """

        catalog_root_normalized = self.catalog_root_normalized
        filesystem = self._filesystem
        txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)

        running_txn_log_file_path = posixpath.join(
            txn_log_dir, RUNNING_TXN_DIR_NAME, self.id
        )

        # Validate read-only transaction constraints
        if self.historic_timestamp is not None:
            if not operation.type.is_read_operation():
                raise RuntimeError(
                    f"Cannot perform {operation.type.value} operation in a read-only historic transaction."
                )

        # Add new operation to the transaction's list of operations
        if self.interactive:
            self.operations = self.operations + [operation]

        # (a) READ txn op
        if operation.type.is_read_operation():
            list_result = operation.dest_metafile.read_txn(
                catalog_root_dir=catalog_root_normalized,
                success_txn_log_dir=posixpath.join(txn_log_dir, SUCCESS_TXN_DIR_NAME),
                current_txn_op=operation,
                current_txn_start_time=self.start_time,
                current_txn_id=self.id,
                filesystem=filesystem,
            )
            self._list_results.append(list_result)
            return list_result

        # (b) WRITE txn op
        # First operation? -> create running log so an external janitor can
        # see that a txn is in-flight.
        if not self.running_log_written:
            self._write_running_log(running_txn_log_file_path)

        try:
            (
                metafile_write_paths,
                locator_write_paths,
            ) = operation.dest_metafile.write_txn(
                catalog_root_dir=catalog_root_normalized,
                success_txn_log_dir=posixpath.join(txn_log_dir, SUCCESS_TXN_DIR_NAME),
                current_txn_op=operation,
                current_txn_start_time=self.start_time,
                current_txn_id=self.id,
                filesystem=filesystem,
            )
            # Check for concurrent txn conflicts on the metafile and locator write paths just written
            # TODO(pdames): Remove the fast-fail check here if it grows too expensive?
            for path in metafile_write_paths + locator_write_paths:
                MetafileRevisionInfo.check_for_concurrent_txn_conflict(
                    success_txn_log_dir=posixpath.join(
                        txn_log_dir,
                        SUCCESS_TXN_DIR_NAME,
                    ),
                    current_txn_revision_file_path=path,
                    filesystem=filesystem,
                )
            return metafile_write_paths, locator_write_paths
        except Exception:
            # convert in-flight txn → FAILED and clean up partial files
            self._fail_and_cleanup(
                failed_txn_log_dir=posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME),
                running_log_path=running_txn_log_file_path,
            )
            raise  # surface original error

    def pause(self) -> None:
        fs = self._filesystem
        root = self.catalog_root_normalized
        txn_log_dir = posixpath.join(root, TXN_DIR_NAME)

        running_path = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME, self.id)
        paused_path = posixpath.join(txn_log_dir, PAUSED_TXN_DIR_NAME, self.id)

        fs.create_dir(posixpath.dirname(paused_path), recursive=True)

        # Record pause time (e.g., for time consistency guarantees)
        self._mark_pause_time(self._time_provider)

        # Serialize current transaction state into paused/txn_id
        with fs.open_output_stream(paused_path) as f:
            f.write(msgpack.dumps(self.to_serializable(root)))

        # Clean up original running log
        fs.delete_file(running_path)

    def resume(self) -> None:
        fs = self._filesystem
        root = self.catalog_root_normalized
        txn_log_dir = posixpath.join(root, TXN_DIR_NAME)

        running_path = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME, self.id)
        paused_path = posixpath.join(txn_log_dir, PAUSED_TXN_DIR_NAME, self.id)

        # Load serialized transaction state
        with fs.open_input_stream(paused_path) as f:
            loaded_txn_data = msgpack.loads(f.readall())

        # Restore relevant fields
        restored_txn = Transaction(**loaded_txn_data)
        self.__dict__.update(
            restored_txn.__dict__
        )  # make curr txn the same as restored (fill vars and stuff)

        # To support restoring time provider state if we ever add non-ephemeral ones.
        new_provider = Transaction.read_time_provider(
            restored_txn["_time_provider"]["type"]
        )

        # evaluate system clock
        now = new_provider.start_time()
        self._time_provider = new_provider  # start time should be preserved
        if now < self.pause_time:
            raise RuntimeError(
                f"System clock {now} is behind paused transaction time {self._pause_time}"
            )
            # TODO: set new start time or keep error if clock is off?

        # Move back to running state
        fs.create_dir(posixpath.dirname(running_path), recursive=True)
        with fs.open_output_stream(running_path) as f:
            f.write(msgpack.dumps(self.to_serializable(root)))
        fs.delete_file(paused_path)

    def seal(
        self,
    ) -> Union[
        List["ListResult[Metafile]"],
        Tuple[List[str], str],
        Tuple[List["ListResult[Metafile]"], List[str], str],
    ]:
        """
        For READ → returns list_results collected during step().
        For WRITE → returns (written_paths, success_log_path).
        """
        if not self.interactive:
            raise RuntimeError(
                "Cannot seal a non-interactive transaction. Call transaction.commit() instead."
            )

        # Read-only transactions can only perform read operations
        if self.historic_timestamp is not None:
            if self._has_write_operations():
                raise RuntimeError(
                    "Cannot seal a read-only historic transaction that contains write operations."
                )

        return self._seal_steps()

    def _has_write_operations(self) -> bool:
        """
        Check if the transaction contains any write operations.
        Read-only transactions should only contain READ operations.
        """
        for operation in self.operations:
            if not operation.type.is_read_operation():
                return True
        return False

    def _seal_steps(
        self,
    ) -> Union[
        List["ListResult[Metafile]"],
        Tuple[List[str], str],
        Tuple[List["ListResult[Metafile]"], List[str], str],
    ]:
        fs = self._filesystem
        root = self.catalog_root_normalized
        txn_log_dir = posixpath.join(root, TXN_DIR_NAME)
        end_time = self._mark_end_time(self._time_provider)

        # READ path: nothing persisted, so we are done
        if all(op.type.is_read_operation() for op in self.operations):
            return self._list_results

        running_path = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME, self.id)
        failed_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
        success_dir = posixpath.join(txn_log_dir, SUCCESS_TXN_DIR_NAME)

        # If no operations ever succeeded we still need a running log.
        if not self.running_log_written:
            self._write_running_log(running_path)
        try:
            # Check for concurrent txn conflicts on metafile and locator write paths
            for path in self.metafile_write_paths + self.locator_write_paths:
                MetafileRevisionInfo.check_for_concurrent_txn_conflict(
                    success_txn_log_dir=posixpath.join(
                        txn_log_dir, SUCCESS_TXN_DIR_NAME
                    ),
                    current_txn_revision_file_path=path,
                    filesystem=fs,
                )
        except Exception:
            self._fail_and_cleanup(
                failed_txn_log_dir=failed_dir,
                running_log_path=running_path,
            )
            # raise the original error
            raise
        success_log_path = None
        try:
            # write transaction log to partitioned structure
            success_log_path = posixpath.join(success_dir, self.id, str(end_time))
            success_log_path = write_file_partitioned(
                path=success_log_path,
                data=msgpack.dumps(self.to_serializable(root)),
                partition_value=self.start_time,
                partition_transform=epoch_timestamp_partition_transform,
                filesystem=fs,
                partition_levels_above_file=1,
            )

            Transaction._validate_txn_log_file(success_txn_log_file=success_log_path)

        except Exception as e1:
            self._fail_and_cleanup(
                failed_txn_log_dir=failed_dir,
                running_log_path=running_path,
                success_log_path=success_log_path,
            )
            raise RuntimeError(
                f"Transaction validation failed. To preserve catalog integrity, "
                f"the corresponding completed transaction log at "
                f"`{success_log_path}` has been removed."
            ) from e1

        else:
            fs.delete_file(running_path)
            if all(op.type.is_write_operation() for op in self.operations):
                # pure write transaction - just return write paths and success log path
                return self.metafile_write_paths, success_log_path
            else:
                # mixed read/write transaction - return read results, write paths, and success log path
                return self._list_results, self.metafile_write_paths, success_log_path

    #  Helper: write or overwrite the running/ID file exactly once
    def _write_running_log(self, running_log_path: str) -> None:
        with self._filesystem.open_output_stream(running_log_path) as f:
            f.write(msgpack.dumps(self.to_serializable(self.catalog_root_normalized)))
        self.running_log_written = True

    #  Helper: mark txn FAILED and clean partial output
    def _fail_and_cleanup(
        self,
        failed_txn_log_dir: str,
        running_log_path: str,
        success_log_path: Optional[str] = None,
    ) -> None:
        fs = self._filesystem

        # 1. write failed/ID
        failed_log_path = posixpath.join(failed_txn_log_dir, self.id)
        failed_log_path = write_file_partitioned(
            path=failed_log_path,
            data=msgpack.dumps(self.to_serializable(self.catalog_root_normalized)),
            partition_value=self.start_time,
            partition_transform=epoch_timestamp_partition_transform,
            filesystem=fs,
        )

        # 2. delete all provisional files
        for path in self.metafile_write_paths:
            try:
                fs.delete_file(path)
            except Exception:
                logger.warning(
                    f"Failed to delete metafile from failed transaction: {path}"
                )
                pass  # best-effort; janitor job will catch leftovers
        for path in self.locator_write_paths:
            try:
                fs.delete_file(path)
            except Exception:
                logger.warning(
                    f"Failed to delete locator from failed transaction: {path}"
                )
                pass  # best-effort; janitor job will catch leftovers

        # 3. tidy up bookkeeping logs
        try:
            fs.delete_file(running_log_path)
        except Exception:
            logger.warning(
                f"Failed to delete running transaction log for failed transaction: {running_log_path}"
            )
            pass
        if success_log_path:
            try:
                fs.delete_file(success_log_path)
            except Exception:
                logger.warning(
                    f"Failed to delete successful transaction log for invalid transaction: {success_log_path}"
                )
                pass

    def __enter__(self) -> "Transaction":
        """
        Context manager entry point. Sets this transaction as the current context.
        Supports nested transactions by preserving the context stack.
        """
        if not hasattr(self, "interactive") or not self.interactive:
            raise RuntimeError(
                "Transaction must be interactive to use with context manager. "
                "Use dc.transaction() to create an interactive transaction."
            )
        if self.start_time is None:
            raise RuntimeError(
                "Transaction has not been started. "
                "Use dc.transaction() to create a properly initialized transaction."
            )

        # Store the context token for restoration in __exit__
        self._context_token = set_current_transaction(self)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """
        Context manager exit point. Restores previous transaction context and
        automatically seals the transaction on successful completion or fails it
        if an exception occurred.

        Args:
            exc_type: Exception type if an exception occurred, None otherwise
            exc_value: Exception value if an exception occurred, None otherwise
            traceback: Exception traceback if an exception occurred, None otherwise
        """
        try:
            if exc_type is None and exc_value is None and traceback is None:
                # No exception occurred - seal the transaction
                self.seal()
            else:
                # Exception occurred during transaction - fail and cleanup
                try:
                    catalog_root_normalized = self.catalog_root_normalized
                    txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
                    running_txn_log_file_path = posixpath.join(
                        txn_log_dir, RUNNING_TXN_DIR_NAME, self.id
                    )
                    self._fail_and_cleanup(
                        failed_txn_log_dir=posixpath.join(
                            txn_log_dir, FAILED_TXN_DIR_NAME
                        ),
                        running_log_path=running_txn_log_file_path,
                    )
                except Exception:
                    # If cleanup fails, still let the original exception propagate
                    pass
        finally:
            # Always restore the previous transaction context using the token
            if hasattr(self, "_context_token"):
                try:
                    # Get the previous value from the token
                    old_value = self._context_token.old_value
                    # Only set if the old value is a valid transaction or None
                    if old_value is None or isinstance(old_value, Transaction):
                        _current_transaction.set(old_value)
                    else:
                        # If old_value is not valid (e.g., Token.MISSING), set to None
                        _current_transaction.set(None)
                except (AttributeError, LookupError):
                    # If token doesn't have old_value or context is corrupted, clear it
                    try:
                        _current_transaction.set(None)
                    except LookupError:
                        pass
