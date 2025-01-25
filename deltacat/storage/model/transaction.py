from __future__ import annotations

import copy
import datetime
import time
import uuid
import posixpath

from itertools import chain
from typing import Optional, List, Union, Tuple

import msgpack
import pyarrow.fs

from deltacat.constants import TXN_DIR_NAME, TXN_PART_SEPARATOR
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
    get_file_info,
)


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
    def end_time(
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Optional[int]:
        """
        Returns the end time of the transaction in milliseconds since the
        epoch based on the transaction log file's modified timestamp, or None
        if the transaction log file does not exist.
        :param path: Transaction log file path to read.
        :param filesystem: File system to use for reading the Transaction file.
        :return: Deserialized object from the Transaction file.
        """
        # TODO(pdames): Validate that input file path is a valid txn log.
        if not filesystem:
            path, filesystem = resolve_path_and_filesystem(path, filesystem)
        file_info = get_file_info(
            path=path,
            filesystem=filesystem,
            ignore_missing_path=True,
        )
        end_time_epoch_ms = None
        if file_info.mtime_ns:
            end_time_epoch_ms = file_info.mtime_ns // 1_000_000
        elif file_info.mtime:
            if isinstance(datetime.datetime, file_info.mtime):
                end_time_epoch_ms = int(file_info.mtime.timestamp() * 1000)
            else:
                # file_info.mtime must be a second-precision float
                end_time_epoch_ms = int(file_info.mtime * 1000)
        return end_time_epoch_ms

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
        Returns the start time of the transaction in milliseconds since the
        epoch.
        """
        return self.get("start_time")

    def _mark_start_time(self) -> None:
        """
        Sets the start time of the transaction as current milliseconds since
        the epoch. Raises a value error if the transaction start time has
        already been set by a previous commit.
        """
        if self.get("start_time"):
            raise RuntimeError("Cannot restart a previously committed transaction.")
        # TODO(pdames): Fix this (hack to assign unique serial txn start times)
        time.sleep(0.001)
        self["start_time"] = time.time_ns() // 1_000_000

    def to_serializable(self) -> Transaction:
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
    def _validate_txn_log_file(
        txn_log_file_path: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        # ensure that the transaction end time was recorded after the start time
        txn_log_file_name = posixpath.basename(txn_log_file_path)
        txn_log_file_parts = txn_log_file_name.split(TXN_PART_SEPARATOR)
        try:
            start_time = int(txn_log_file_parts[0])
        except ValueError as e:
            raise ValueError(
                f"Transaction log file `{txn_log_file_path}` does not "
                f"contain a valid start time."
            ) from e
        if start_time < 0:
            raise ValueError(
                f"Transaction log file `{txn_log_file_path}` does not "
                f"contain a valid start time ({start_time} is pre-epoch)."
            )
        if start_time > time.time_ns() // 1_000_000:
            raise ValueError(
                f"Transaction log file `{txn_log_file_path}` does not "
                f"contain a valid start time ({start_time} is in the future)."
            )
        # ensure that the txn uuid is valid
        txn_uuid_str = txn_log_file_parts[1]
        try:
            uuid.UUID(txn_uuid_str)
        except ValueError as e:
            raise OSError(
                f"Transaction log file `{txn_log_file_path}` does not "
                f"contain a valid UUID string."
            ) from e
        end_time = Transaction.end_time(
            path=txn_log_file_path,
            filesystem=filesystem,
        )
        if end_time < start_time:
            raise OSError(
                f"Transaction end time {end_time} is earlier than start "
                f"time {start_time}! This may indicate a problem "
                f"with either the system clock on the host serving the "
                f"transaction, or a loss of precision in the filesystem "
                f"recording the completed transaction file timestamp (at "
                f"least millisecond precision is required). To preserve "
                f"catalog integrity, the corresponding completed "
                f"transaction log at `{txn_log_file_path}` has been removed."
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

        # record the transaction start time
        txn._mark_start_time()

        # create the transaction directory first to telegraph that at least 1
        # transaction at this root has been attempted
        catalog_root_normalized, filesystem = resolve_path_and_filesystem(
            catalog_root_dir, filesystem
        )
        txn_log_dir = posixpath.join(catalog_root_normalized, TXN_DIR_NAME)
        filesystem.create_dir(txn_log_dir, recursive=True)

        if txn.type == TransactionType.READ:
            list_results = []
            for operation in self.operations:
                list_result = operation.dest_metafile.read_txn(
                    catalog_root_dir=catalog_root_normalized,
                    txn_log_dir=txn_log_dir,
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
                txn_log_dir=txn_log_dir,
                filesystem=filesystem,
            )

    def _commit_write(
        self,
        catalog_root_normalized: str,
        txn_log_dir: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> Tuple[List[str], str]:
        # write each metafile associated with the transaction
        metafile_write_paths = []
        locator_write_paths = []
        try:
            for operation in self.operations:
                operation.dest_metafile.write_txn(
                    catalog_root_dir=catalog_root_normalized,
                    txn_log_dir=txn_log_dir,
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
                        txn_log_dir=txn_log_dir,
                        current_txn_revision_file_path=path,
                        filesystem=filesystem,
                    )
        except Exception:
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
            raise

        # record the completed transaction
        txn_log_file_path = posixpath.join(txn_log_dir, self.id)
        with filesystem.open_output_stream(txn_log_file_path) as file:
            packed = msgpack.dumps(self.to_serializable())
            file.write(packed)
        try:
            Transaction._validate_txn_log_file(
                txn_log_file_path=txn_log_file_path,
                filesystem=filesystem,
            )
        except Exception as e1:
            try:
                filesystem.delete_file(txn_log_file_path)
            except Exception as e2:
                raise OSError(
                    f"Failed to cleanup bad transaction log file at "
                    f"`{txn_log_file_path}`"
                ) from e2
            finally:
                raise RuntimeError(
                    f"Transaction validation failed. To preserve "
                    f"catalog integrity, the corresponding completed "
                    f"transaction log at `{txn_log_file_path}` has been removed."
                ) from e1
        return metafile_write_paths, txn_log_file_path
