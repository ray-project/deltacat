from __future__ import annotations

import os
import copy
import time
import uuid
import posixpath
from pathlib import PosixPath
import threading
from collections import defaultdict

from itertools import chain
from typing import Optional, List, Union, Tuple

import msgpack
import pyarrow.fs

from deltacat.constants import (
    TXN_DIR_NAME,
    TXN_PART_SEPARATOR,
    RUNNING_TXN_DIR_NAME,
    FAILED_TXN_DIR_NAME,
    SUCCESS_TXN_DIR_NAME,
    NANOS_PER_SEC,
)
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.types import (
    TransactionOperationType,
    TransactionType,
)
from deltacat.storage.model.metafile import (
    Metafile,
    MetafileRevisionInfo,
)
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    list_directory,
)


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
        if operation_type == TransactionOperationType.UPDATE:
            if not src_metafile:
                raise ValueError(
                    "UPDATE transaction operations must have a source metafile."
                )
            elif type(dest_metafile) is not type(src_metafile):
                raise ValueError(
                    f"Source metafile type `{type(src_metafile)}` is not "
                    f"equal to dest metafile type `{type(dest_metafile)}`."
                )
        elif src_metafile:
            raise ValueError(
                "Only UPDATE transaction operations may have a source metafile."
            )
        if operation_type.is_write_operation() and read_limit:
            raise ValueError("Only READ transaction operations may have a read limit.")
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
        return TransactionOperationType(self["type"])

    @type.setter
    def type(self, txn_op_type: TransactionOperationType):
        self["type"] = txn_op_type

    @property
    def dest_metafile(self) -> Metafile:
        """
        Returns the metafile that is the target of this transaction operation.
        """
        return self["dest_metafile"]

    @dest_metafile.setter
    def dest_metafile(self, metafile: Metafile):
        self["dest_metafile"] = metafile

    @property
    def src_metafile(self) -> Optional[Metafile]:
        """
        Returns the metafile that is the source of this transaction operation.
        """
        return self["src_metafile"]

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


class Transaction(dict):
    """
    Base class for DeltaCAT transactions.
    """

    @staticmethod
    def of(
        txn_type: TransactionType,
        txn_operations: Optional[TransactionOperationList],
    ) -> Transaction:
        operation_types = set([op.type for op in txn_operations])
        if txn_type == TransactionType.READ:
            if operation_types - TransactionOperationType.read_operations():
                raise ValueError(
                    "Only READ transaction operation types may be specified as "
                    "part of a READ transaction."
                )
        elif (
            len(operation_types) == 1
            and TransactionOperationType.CREATE in operation_types
        ):
            if txn_type != TransactionType.APPEND:
                raise ValueError(
                    "Transactions with only CREATE operations must be "
                    "specified as part of an APPEND transaction."
                )
        elif TransactionOperationType.DELETE in operation_types:
            if txn_type != TransactionType.DELETE:
                raise ValueError(
                    "DELETE transaction operations must be specified as part "
                    "of a DELETE transaction."
                )
        elif TransactionOperationType.UPDATE in operation_types and txn_type not in {
            TransactionType.ALTER,
            TransactionType.RESTATE,
            TransactionType.OVERWRITE,
        }:
            raise ValueError(
                "Transactions with UPDATE operations must be specified "
                "as part of an ALTER, RESTATE, or OVERWRITE transaction."
            )
        transaction = Transaction()
        transaction.type = txn_type
        transaction.operations = txn_operations
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

    @property
    def type(self) -> TransactionType:
        """
        Returns the type of the transaction.
        """
        return TransactionType(self["type"])

    @type.setter
    def type(self, txn_type: TransactionType):
        self["type"] = txn_type

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
    def start_time(self) -> Optional[int]:
        """
        Returns the start time of the transaction.
        """
        return self.get("start_time")

    @property
    def end_time(self) -> Optional[int]:
        """
        Returns the end time of the transaction.
        """
        return self.get("end_time")

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

    @staticmethod
    def _abs_txn_meta_path_to_relative(root: str, target: str) -> str:
        """
        Takes an absolute root directory path and target absolute path to
        relativize with respect to the root directory. Returns the target
        path relative to the root directory path. Raises an error if the
        target path is not contained in the given root directory path, if
        either path is not an absolute path, or if the target path is equal
        to the root directory path.
        """
        root_path = PosixPath(root)
        target_path = PosixPath(target)
        # TODO (martinezdavid): Check why is_absolute() fails for certain Delta paths
        # if not root_path.is_absolute() or not target_path.is_absolute():
        #     raise ValueError("Both root and target must be absolute paths.")
        if root_path == target_path:
            raise ValueError(
                "Target and root are identical, but expected target to be a child of root."
            )
        try:
            relative_path = target_path.relative_to(root_path)
        except ValueError:
            raise ValueError("Expected target to be a child of root.")
        return str(relative_path)

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
        serializable = copy.deepcopy(self)
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
        return serializable

    @staticmethod
    def _validate_txn_log_file(success_txn_log_file: str) -> None:
        txn_log_dir_name = posixpath.basename(posixpath.dirname(success_txn_log_file))
        txn_log_parts = txn_log_dir_name.split(TXN_PART_SEPARATOR)
        # ensure that the transaction start time is valid
        try:
            start_time = int(txn_log_parts[0])
        except ValueError as e:
            raise ValueError(
                f"Transaction log file `{success_txn_log_file}` does not "
                f"contain a valid start time."
            ) from e
        # ensure that the txn uuid is valid
        txn_uuid_str = txn_log_parts[1]
        try:
            uuid.UUID(txn_uuid_str)
        except ValueError as e:
            raise OSError(
                f"Transaction log file `{success_txn_log_file}` does not "
                f"contain a valid UUID string."
            ) from e
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
    ) -> Union[List[ListResult[Metafile]], Tuple[List[str], str]]:
        # TODO(pdames): allow transactions to be durably staged and resumed
        #  across multiple sessions prior to commit

        # create a new internal copy of this transaction to guard against
        # external modification and dirty state across retries
        txn = copy.deepcopy(self)

        # create the transaction directory first to telegraph that at least 1
        # transaction at this root has been attempted
        catalog_root_normalized, filesystem = resolve_path_and_filesystem(
            catalog_root_dir,
            filesystem,
        )
        txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
        running_txn_log_dir = posixpath.join(txn_log_dir, RUNNING_TXN_DIR_NAME)
        filesystem.create_dir(running_txn_log_dir, recursive=True)
        failed_txn_log_dir = posixpath.join(txn_log_dir, FAILED_TXN_DIR_NAME)
        filesystem.create_dir(failed_txn_log_dir, recursive=False)
        success_txn_log_dir = posixpath.join(txn_log_dir, SUCCESS_TXN_DIR_NAME)
        filesystem.create_dir(success_txn_log_dir, recursive=False)

        # TODO(pdames): Support injection of other time providers, but ensure
        #  that ALL transactions in a catalog use the same time provider.
        time_provider = TransactionSystemTimeProvider()

        # record the transaction start time
        txn._mark_start_time(time_provider)

        if txn.type == TransactionType.READ:
            list_results = []
            for operation in self.operations:
                list_result = operation.dest_metafile.read_txn(
                    catalog_root_dir=catalog_root_normalized,
                    success_txn_log_dir=success_txn_log_dir,
                    current_txn_op=operation,
                    current_txn_start_time=txn.start_time,
                    current_txn_id=txn.id,
                    filesystem=filesystem,
                )
                list_results.append(list_result)
            return list_results
        else:
            return txn._commit_write(
                catalog_root_normalized=catalog_root_normalized,
                running_txn_log_dir=running_txn_log_dir,
                failed_txn_log_dir=failed_txn_log_dir,
                success_txn_log_dir=success_txn_log_dir,
                filesystem=filesystem,
                time_provider=time_provider,
            )

    def _commit_write(
        self,
        catalog_root_normalized: str,
        running_txn_log_dir: str,
        failed_txn_log_dir: str,
        success_txn_log_dir: str,
        filesystem: pyarrow.fs.FileSystem,
        time_provider: TransactionTimeProvider,
    ) -> Tuple[List[str], str]:
        # write the in-progress transaction log file
        running_txn_log_file_path = posixpath.join(
            running_txn_log_dir,
            self.id,
        )
        with filesystem.open_output_stream(running_txn_log_file_path) as file:
            packed = msgpack.dumps(self.to_serializable(catalog_root_normalized))
            file.write(packed)

        # write each metafile associated with the transaction
        metafile_write_paths = []
        locator_write_paths = []
        try:
            for operation in self.operations:
                operation.dest_metafile.write_txn(
                    catalog_root_dir=catalog_root_normalized,
                    success_txn_log_dir=success_txn_log_dir,
                    current_txn_op=operation,
                    current_txn_start_time=self.start_time,
                    current_txn_id=self.id,
                    filesystem=filesystem,
                )
                metafile_write_paths.extend(operation.metafile_write_paths)
                locator_write_paths.extend(operation.locator_write_paths)
                # check for conflicts with concurrent transactions
                for path in metafile_write_paths + locator_write_paths:
                    MetafileRevisionInfo.check_for_concurrent_txn_conflict(
                        success_txn_log_dir=success_txn_log_dir,
                        current_txn_revision_file_path=path,
                        filesystem=filesystem,
                    )
        except Exception:
            # write a failed transaction log file entry
            failed_txn_log_file_path = posixpath.join(
                failed_txn_log_dir,
                self.id,
            )
            with filesystem.open_output_stream(failed_txn_log_file_path) as file:
                packed = msgpack.dumps(self.to_serializable(catalog_root_normalized))
                file.write(packed)

            ###################################################################
            ###################################################################
            # failure past here telegraphs a failed transaction cleanup attempt
            ###################################################################
            ###################################################################

            # delete all files written during the failed transaction
            known_write_paths = chain.from_iterable(
                [
                    operation.metafile_write_paths + operation.locator_write_paths
                    for operation in self.operations
                ]
            )
            # TODO(pdames): Add separate janitor job to cleanup files that we
            #  either failed to add to the known write paths, or fail to delete.
            for write_path in known_write_paths:
                filesystem.delete_file(write_path)

            # delete the in-progress transaction log file entry
            filesystem.delete_file(running_txn_log_file_path)
            # failed transaction cleanup is now complete
            raise

        # record the completed transaction
        success_txn_log_file_dir = posixpath.join(
            success_txn_log_dir,
            self.id,
        )
        filesystem.create_dir(
            success_txn_log_file_dir,
            recursive=False,
        )
        end_time = self._mark_end_time(time_provider)
        success_txn_log_file_path = posixpath.join(
            success_txn_log_file_dir,
            str(end_time),
        )
        with filesystem.open_output_stream(success_txn_log_file_path) as file:
            packed = msgpack.dumps(self.to_serializable(catalog_root_normalized))
            file.write(packed)
        try:
            Transaction._validate_txn_log_file(
                success_txn_log_file=success_txn_log_file_path
            )
        except Exception as e1:
            try:
                # move the txn log from success dir to failed dir
                failed_txn_log_file_path = posixpath.join(
                    failed_txn_log_dir,
                    self.id,
                )
                filesystem.move(
                    src=success_txn_log_file_path,
                    dest=failed_txn_log_file_path,
                )
                # keep parent success txn log dir to telegraph failed validation

                ###############################################################
                ###############################################################
                # failure past here telegraphs a failed transaction validation
                # cleanup attempt
                ###############################################################
                ###############################################################
            except Exception as e2:
                raise OSError(
                    f"Failed to cleanup bad transaction log file at "
                    f"`{success_txn_log_file_path}`"
                ) from e2
            finally:
                raise RuntimeError(
                    f"Transaction validation failed. To preserve "
                    f"catalog integrity, the corresponding completed "
                    f"transaction log at `{success_txn_log_file_path}` has "
                    f"been removed."
                ) from e1
        finally:
            # delete the in-progress transaction log file entry
            filesystem.delete_file(running_txn_log_file_path)
        return metafile_write_paths, success_txn_log_file_path
