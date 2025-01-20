# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import copy
import re
import datetime
import time

from itertools import chain
from typing import Optional, Tuple, List, Union

import msgpack
import pyarrow.fs
import posixpath
import uuid

from pyarrow.fs import (
    FileInfo,
    FileSelector,
    FileType,
)

# TODO(pdames): Create internal DeltaCAT port
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.locator import Locator
from deltacat.storage.model.types import (
    TransactionType,
    TransactionOperationType,
)

TXN_PART_SEPARATOR = "_"
TXN_DIR_NAME: str = "txn"
REVISION_DIR_NAME: str = "rev"
METAFILE_EXT = ".mpk"


def _filesystem(
    path: str,
    filesystem: Optional[pyarrow.fs.FileSystem] = None,
) -> Tuple[str, pyarrow.fs.FileSystem]:
    """
    Normalizes the input path and resolves a corresponding file system.
    :param path: A file or directory path.
    :param filesystem: File system to use for path IO.
    :return: Normalized path and resolved file system for that path.
    """
    # TODO(pdames): resolve and cache filesystem at catalog root level
    #   ensure returned paths are normalized as posix paths
    paths, filesystem = _resolve_paths_and_filesystem(
        paths=path,
        filesystem=filesystem,
    )
    assert len(paths) == 1, len(paths)
    return paths[0], filesystem


def _handle_read_os_error(
    error: OSError,
    paths: Union[str, List[str]],
) -> str:
    # NOTE: this is not comprehensive yet, and should be extended as more errors arise.
    # NOTE: The latter patterns are raised in Arrow 10+, while the former is raised in
    # Arrow < 10.
    aws_error_pattern = (
        r"^(?:(.*)AWS Error \[code \d+\]: No response body\.(.*))|"
        r"(?:(.*)AWS Error UNKNOWN \(HTTP status 400\) during HeadObject operation: "
        r"No response body\.(.*))|"
        r"(?:(.*)AWS Error ACCESS_DENIED during HeadObject operation: No response "
        r"body\.(.*))$"
    )
    if re.match(aws_error_pattern, str(error)):
        # Specially handle AWS error when reading files, to give a clearer error
        # message to avoid confusing users. The real issue is most likely that the AWS
        # S3 file credentials have not been properly configured yet.
        if isinstance(paths, str):
            # Quote to highlight single file path in error message for better
            # readability. List of file paths will be shown up as ['foo', 'boo'],
            # so only quote single file path here.
            paths = f'"{paths}"'
        raise OSError(
            (
                f"Failing to read AWS S3 file(s): {paths}. "
                "Please check that file exists and has properly configured access. "
                "You can also run AWS CLI command to get more detailed error message "
                "(e.g., aws s3 ls <file-name>). "
                "See https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html "  # noqa
                "and https://docs.ray.io/en/latest/data/creating-datasets.html#reading-from-remote-storage "  # noqa
                "for more information."
            )
        )
    else:
        raise error


def _list_directory(
    path: str,
    filesystem: pyarrow.fs.FileSystem,
    exclude_prefixes: Optional[List[str]] = None,
    ignore_missing_path: bool = False,
    recursive: bool = False,
) -> List[Tuple[str, int]]:
    """
    Expand the provided directory path to a list of file paths.

    Args:
        path: The directory path to expand.
        filesystem: The filesystem implementation that should be used for
            reading these files.
        exclude_prefixes: The file relative path prefixes that should be
            excluded from the returned file set. Default excluded prefixes are
            "." and "_".
        recursive: Whether to expand subdirectories or not.

    Returns:
        An iterator of (file_path, file_size) tuples.
    """
    if exclude_prefixes is None:
        exclude_prefixes = [".", "_"]

    selector = FileSelector(
        base_dir=path,
        recursive=recursive,
        allow_not_found=ignore_missing_path,
    )
    try:
        files = filesystem.get_file_info(selector)
    except OSError as e:
        _handle_read_os_error(e, path)
    base_path = selector.base_dir
    out = []
    for file_ in files:
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        relative = file_path[len(base_path) :]
        if any(relative.startswith(prefix) for prefix in exclude_prefixes):
            continue
        out.append((file_path, file_.size))
    # We sort the paths to guarantee a stable order.
    return sorted(out)


def _get_file_info(
    path: str,
    filesystem: pyarrow.fs.FileSystem,
    ignore_missing_path: bool = False,
) -> Union[FileInfo, List[FileInfo]]:
    """Get the file info or list of file infos for the provided path."""
    try:
        file_info = filesystem.get_file_info(path)
    except OSError as e:
        _handle_read_os_error(e, path)
    if file_info.type == FileType.NotFound and not ignore_missing_path:
        raise FileNotFoundError(path)

    return file_info


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
            path, fs = _filesystem(path, filesystem)
        file_info = _get_file_info(
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
            path, fs = _filesystem(path, filesystem)
        with fs.open_input_stream(path) as file:
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
        except ValueError:
            raise ValueError(
                f"Transaction log file `{txn_log_file_path}` does not "
                f"contain a valid start time."
            )
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
        except ValueError:
            raise OSError(
                f"Transaction log file `{txn_log_file_path}` does not "
                f"contain a valid UUID string."
            )
        end_time = Transaction.end_time(
            path=txn_log_file_path,
            filesystem=filesystem,
        )
        if end_time - start_time < 0:
            try:
                filesystem.delete_file(txn_log_file_path)
            except Exception as e:
                raise OSError(
                    f"Failed to cleanup bad transaction log file at `{txn_log_file_path}`"
                ) from e
            finally:
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
        catalog_root_normalized, filesystem = _filesystem(catalog_root_dir, filesystem)
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
        Transaction._validate_txn_log_file(
            txn_log_file_path=txn_log_file_path,
            filesystem=filesystem,
        )
        return metafile_write_paths, txn_log_file_path


class MetafileRevisionInfo(dict):
    """
    Base class for DeltaCAT metafile revision info.
    """

    @staticmethod
    def undefined() -> MetafileRevisionInfo:
        mri = MetafileRevisionInfo()
        mri.revision = 0
        mri.txn_id = None
        mri.txn_op_type = None
        mri.dir_path = None
        return mri

    @staticmethod
    def parse(revision_file_path: str) -> MetafileRevisionInfo:
        dir_path = posixpath.dirname(revision_file_path)
        metafile_name = posixpath.basename(revision_file_path)
        metafile_and_ext = posixpath.splitext(metafile_name)
        metafile_ext = metafile_and_ext[1] if len(metafile_and_ext) > 1 else None
        metafile_rev_and_txn_info = metafile_and_ext[0]
        txn_info_parts = metafile_rev_and_txn_info.split(TXN_PART_SEPARATOR)

        mri = MetafileRevisionInfo()
        mri.dir_path = dir_path
        mri.extension = metafile_ext
        mri.revision = int(txn_info_parts[0])
        mri.txn_op_type = txn_info_parts[1]
        mri.txn_id = f"{txn_info_parts[2]}{TXN_PART_SEPARATOR}{txn_info_parts[3]}"
        return mri

    @staticmethod
    def list_revisions(
        revision_dir_path: str,
        filesystem: pyarrow.fs.FileSystem,
        txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[MetafileRevisionInfo]:
        if not txn_log_dir:
            err_msg = f"No transaction log found for: {revision_dir_path}."
            raise ValueError(err_msg)
        # find the latest committed revision of the target metafile
        sorted_metafile_paths = MetafileRevisionInfo._sorted_file_paths(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
            ignore_missing_revision=True,
        )
        revisions = []
        while sorted_metafile_paths:
            latest_metafile_path = sorted_metafile_paths.pop()
            mri = MetafileRevisionInfo.parse(latest_metafile_path)
            if not current_txn_id or mri.txn_id == current_txn_id:
                # consider the current transaction (if any) to be committed
                revisions.append(mri)
            else:
                # the current transaction can only build on top of the snapshot
                # of commits from transactions that completed before it started
                txn_end_time = Transaction.end_time(
                    path=posixpath.join(txn_log_dir, mri.txn_id),
                    filesystem=filesystem,
                )
                if txn_end_time is not None and txn_end_time < current_txn_start_time:
                    revisions.append(mri)
            if limit <= len(revisions):
                break
        return revisions

    @staticmethod
    def latest_revision(
        revision_dir_path: str,
        filesystem: pyarrow.fs.FileSystem,
        txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        ignore_missing_revision: bool = False,
    ) -> MetafileRevisionInfo:
        revisions = MetafileRevisionInfo.list_revisions(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
            txn_log_dir=txn_log_dir,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            limit=1,
        )
        if not revisions and not ignore_missing_revision:
            err_msg = f"No committed revision found at {revision_dir_path}."
            raise ValueError(err_msg)
        return revisions[0] if revisions else MetafileRevisionInfo.undefined()

    @staticmethod
    def new_revision(
        revision_dir_path: str,
        current_txn_op_type: TransactionOperationType,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
        extension: Optional[str] = METAFILE_EXT,
        txn_log_dir: Optional[str] = None,
    ) -> MetafileRevisionInfo:
        """
        Creates and returns a new MetafileRevisionInfo object for the next
        revision of the metafile.

        This method determines the next revision information based on the
        latest existing revision in the specified directory path and the
        current transaction details.

        Args:
            revision_dir_path (str): Metafile revision directory path to
            generate the next metafile revision info for.
            current_txn_op_type (TransactionOperationType): The current
            transaction's operation type.
            current_txn_start_time (int): The current transaction's start time.
            current_txn_id (str): The current transaction's ID.
            filesystem (pyarrow.fs.FileSystem): The filesystem interface to
            use for file operations
            extension (str, optional): The file extension for metafiles.
            Defaults to METAFILE_EXT.
            txn_log_dir (Optional[str], optional): Directory path for
            transaction logs. Will be automatically discovered by traversing
            revision directory parent paths if not specified.

        Returns:
            MetafileRevisionInfo: A new revision info object containing
            metadata for the next revision

        Notes:
            - For CREATE operations, the method will ignore missing previous
              revisions.
            - The method validates the transaction operation type before
              creating the new revision.
            - Uses the pyarrow filesystem interface for file operations.
        """
        is_create_txn = current_txn_op_type == TransactionOperationType.CREATE
        mri = MetafileRevisionInfo.latest_revision(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
            txn_log_dir=txn_log_dir,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            ignore_missing_revision=is_create_txn,
        )
        # validate the transaction operation type
        if mri.revision:
            # update/delete fails if the last metafile was deleted
            if mri.txn_op_type == TransactionOperationType.DELETE:
                if current_txn_op_type != TransactionOperationType.CREATE:
                    raise ValueError(
                        f"Metafile {current_txn_op_type.value} failed "
                        f"for transaction ID {current_txn_id} failed. "
                        f"Metafile state at {mri.path} is deleted."
                    )
            # create fails unless the last metafile was deleted
            elif is_create_txn:
                raise ValueError(
                    f"Metafile creation for transaction ID {current_txn_id} "
                    f"failed. Metafile commit at {mri.path} already exists."
                )
        elif not is_create_txn:
            # update/delete fails if the last metafile doesn't exist
            raise ValueError(
                f"Metafile {current_txn_op_type.value} failed for "
                f"transaction ID {current_txn_id} failed. Metafile at "
                f"{mri.path} does not exist."
            )
        mri.revision = mri.revision + 1
        mri.txn_id = current_txn_id
        mri.txn_op_type = current_txn_op_type
        mri.dir_path = revision_dir_path
        mri.extension = extension
        return mri

    @staticmethod
    def check_for_concurrent_txn_conflict(
        txn_log_dir: str,
        current_txn_revision_file_path: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        """
        Checks for a concurrent modification conflict between a file commited
        by the current transaction and another parallel transaction. Raises
        an exception if a concurrent modification conflict is found.

        :param txn_log_dir: Path to the catalog transaction log.
        :param current_txn_revision_file_path: Path to a metafile revision
        written by the current transaction to check for conflicts against.
        :param filesystem: Filesystem that can read the metafile revision.
        :raises RuntimeError: if a conflict is found with another transaction.
        """
        revision_dir_path = posixpath.dirname(current_txn_revision_file_path)
        cur_txn_mri = MetafileRevisionInfo.parse(current_txn_revision_file_path)

        sorted_metafile_paths = MetafileRevisionInfo._sorted_file_paths(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
        )
        conflict_mris = []
        while sorted_metafile_paths:
            next_metafile_path = sorted_metafile_paths.pop()
            mri = MetafileRevisionInfo.parse(next_metafile_path)
            if mri.revision < cur_txn_mri.revision:
                # no conflict was found
                break
            elif (
                mri.revision == cur_txn_mri.revision
                and mri.txn_id != cur_txn_mri.txn_id
            ):
                # we've found a conflict between txn_id and current_txn_id
                # defer to the transaction with the higher lexicographic order
                # (i.e., the transaction that started most recently)
                # TODO(pdames): Ensure the conflicting transaction is alive
                #  (e.g., give each transaction a heartbeat timeout that gives
                #  it 1-2 seconds per operation, and record known failed
                #  transaction IDs)
                if mri.txn_id > cur_txn_mri.txn_id:
                    raise RuntimeError(
                        f"Aborting transaction {cur_txn_mri.txn_id} due to "
                        f"concurrent conflict at "
                        f"{current_txn_revision_file_path} with transaction "
                        f"{mri.txn_id} at {next_metafile_path}."
                    )
                conflict_mris.append(mri)
        if conflict_mris:
            # current txn wins the ordering challenge among all conflicts,
            # but we still need to ensure that no conflicting transactions
            # completed before seeing the conflict with this transaction
            for mri in conflict_mris:
                txn_end_time = Transaction.end_time(
                    path=posixpath.join(txn_log_dir, mri.txn_id),
                    filesystem=filesystem,
                )
                # TODO(pdames): Resolve risk of passing this check if it
                #  runs before the conflicting transaction marks itself as
                #  complete in the txn log. Some fixes include enforcing
                #  serializable isolation of the txn log, eventually
                #  consistent detection & repair, writing a mutex file
                #  that tells future transactions to only consider this txn
                #  complete if the conflicting txn is not complete, etc.
                if txn_end_time:
                    raise RuntimeError(
                        f"Aborting transaction {cur_txn_mri.txn_id} due to "
                        f"concurrent conflict at {revision_dir_path} with "
                        f"previously completed transaction {mri.txn_id} at "
                        f"{next_metafile_path}."
                    )

    @staticmethod
    def _sorted_file_paths(
        revision_dir_path: str,
        filesystem: pyarrow.fs.FileSystem,
        ignore_missing_revision: bool = False,
    ) -> List[str]:
        file_paths_and_sizes = _list_directory(
            path=revision_dir_path,
            filesystem=filesystem,
            ignore_missing_path=True,
        )
        if not file_paths_and_sizes and not ignore_missing_revision:
            err_msg = (
                f"Expected to find at least 1 Metafile at "
                f"{revision_dir_path} but found none."
            )
            raise ValueError(err_msg)
        return list(list(zip(*file_paths_and_sizes))[0]) if file_paths_and_sizes else []

    @property
    def revision(self) -> int:
        return self["revision"]

    @revision.setter
    def revision(self, revision: int):
        self["revision"] = revision

    @property
    def txn_id(self) -> Optional[str]:
        return self["txn_id"]

    @txn_id.setter
    def txn_id(self, txn_id: str):
        self["txn_id"] = txn_id

    @property
    def txn_op_type(self) -> Optional[TransactionOperationType]:
        op_type = self.get("txn_op_type")
        return None if op_type is None else TransactionOperationType(op_type)

    @txn_op_type.setter
    def txn_op_type(self, txn_op_type: TransactionOperationType):
        self["txn_op_type"] = txn_op_type

    @property
    def dir_path(self) -> Optional[str]:
        return self["dir_path"]

    @dir_path.setter
    def dir_path(self, dir_path: str):
        self["dir_path"] = dir_path

    @property
    def extension(self) -> str:
        return self.get("extension") or METAFILE_EXT

    @extension.setter
    def extension(self, extension: str):
        self["extension"] = extension

    @property
    def file_name(self) -> Optional[str]:
        return (
            TXN_PART_SEPARATOR.join(
                [
                    f"{self.revision:020}",
                    self.txn_op_type,
                    f"{self.txn_id}{self.extension}",
                ]
            )
            if self.txn_op_type and self.txn_id
            else None
        )

    @property
    def path(self) -> Optional[str]:
        file_name = self.file_name
        return (
            posixpath.join(
                self.dir_path,
                file_name,
            )
            if self.dir_path and file_name
            else None
        )


class Metafile(dict):
    """
    Base class for DeltaCAT metadata files, with read and write methods
    for dict-based DeltaCAT models. Uses msgpack (https://msgpack.org/) for
    cross-language-compatible serialization and deserialization.
    """

    @staticmethod
    def update_for(other: Optional[Metafile]) -> Optional[Metafile]:
        """
        Returns a new metafile that can be used as the destination metafile
        in an update transaction operation against the input source metafile.
        The returned metafile starts as an identical deep copy of the input
        metafile such that, if the output is changed and committed as part of
        an update transaction operation on the source metafile, then it will
        update instead of replace the source metafile.
        :param other: Source metafile for the copy.
        :return: New copy of the source metafile.
        """
        return copy.deepcopy(other) if other is not None else None

    @staticmethod
    def based_on(
        other: Optional[Metafile],
        new_id: Optional[Locator] = None,
    ) -> Optional[Metafile]:
        """
        Returns a new metafile equivalent to the input metafile, but with a new
        ID assigned to distinguish it as a separate catalog object. This means
        that, if the output is simply committed as part of an update transaction
        operation on the source metafile, then it will replace instead of update
        the source metafile.
        :param other: Source metafile that is the basis for the new metafile.
        :param new_id: New immutable ID to assign to the new metafile. Should
        not be specified for metafiles with mutable names (e.g., namespaces and
        tables).
        :return: A new metafile based on the input metafile with a different ID.
        """
        metafile_copy = Metafile.update_for(other)
        if metafile_copy:
            # remove the source metafile ID so that this is treated as a
            # different catalog object with otherwise identical properties
            if not other.named_immutable_id:
                metafile_copy.pop("id", None)
                if new_id:
                    raise ValueError(
                        f"New ID cannot be specified for metafiles that "
                        f"don't have a named immutable ID."
                    )
            else:
                if not new_id:
                    raise ValueError(
                        f"New ID must be specified for metafiles that have a "
                        f"named immutable ID."
                    )
                metafile_copy.named_immutable_id = new_id
            # remove all ancestors of the original source metafile
            metafile_copy.pop("ancestor_ids", None)
        return metafile_copy

    @staticmethod
    def read_txn(
        catalog_root_dir: str,
        txn_log_dir: str,
        current_txn_op: TransactionOperation,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> ListResult[Metafile]:
        """
        Read one or more metadata files within the context of a transaction.
        :param catalog_root_dir: Catalog root dir to read the metafile from.
        :param txn_log_dir: Catalog root transaction log directory.
        :param current_txn_op: Transaction operation for this read.
        :param current_txn_start_time: Transaction start time for this read.
        :param current_txn_id: Transaction ID for this read.
        :param filesystem: File system to use for reading the metadata file. If
        not given, a default filesystem will be automatically selected based on
        the catalog root path.
        :return: ListResult of deserialized metadata files read.
        """
        kwargs = {
            "catalog_root": catalog_root_dir,
            "txn_log_dir": txn_log_dir,
            "current_txn_start_time": current_txn_start_time,
            "current_txn_id": current_txn_id,
            "filesystem": filesystem,
            "limit": current_txn_op.read_limit,
        }
        if current_txn_op.type == TransactionOperationType.READ_SIBLINGS:
            return current_txn_op.dest_metafile.siblings(**kwargs)
        elif current_txn_op.type == TransactionOperationType.READ_CHILDREN:
            return current_txn_op.dest_metafile.children(**kwargs)
        elif current_txn_op.type == TransactionOperationType.READ_LATEST:
            kwargs["limit"] = 1
        elif current_txn_op.type == TransactionOperationType.READ_EXISTS:
            kwargs["limit"] = 1
            kwargs["materialize_revisions"] = False
        else:
            raise ValueError(
                f"Unsupported transaction operation type: {current_txn_op.type}"
            )
        # return the latest metafile revision for READ_LATEST and READ_EXISTS
        list_result = current_txn_op.dest_metafile.revisions(**kwargs)
        revisions = list_result.all_items()
        metafiles = []
        if revisions:
            op_type = revisions[0][0]
            if op_type != TransactionOperationType.DELETE:
                metafiles.append(revisions[0][1])
            # TODO(pdames): Add Optional[Metafile] to return type and just
            #  return the latest metafile (if any) directly?
            return ListResult.of(
                items=metafiles,
                pagination_key=None,
                next_page_provider=None,
            )

    @classmethod
    def read(
        cls,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Metafile:
        """
        Read a metadata file and return the deserialized object.
        :param path: Metadata file path to read.
        :param filesystem: File system to use for reading the metadata file.
        :return: Deserialized object from the metadata file.
        """
        path, filesystem = _filesystem(path, filesystem)
        with filesystem.open_input_stream(path) as file:
            binary = file.readall()
        obj = cls(**msgpack.loads(binary)).from_serializable(path, filesystem)
        return obj

    def write_txn(
        self,
        catalog_root_dir: str,
        txn_log_dir: str,
        current_txn_op: TransactionOperation,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> None:
        """
        Serialize and write this object to a metadata file within the context
        of a transaction.
        :param catalog_root_dir: Catalog root dir to write the metafile to.
        :param txn_log_dir: Catalog root transaction log directory.
        :param current_txn_op: Transaction operation for this write.
        :param current_txn_start_time: Transaction start time for this write.
        :param current_txn_id: Transaction ID for this write.
        :param filesystem: File system to use for writing the metadata file. If
        not given, a default filesystem will be automatically selected based on
        the catalog root path.
        """
        path, fs = _filesystem(
            path=catalog_root_dir,
            filesystem=filesystem,
        )
        self._write_metafile_revisions(
            catalog_root=path,
            txn_log_dir=txn_log_dir,
            current_txn_op=current_txn_op,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=fs,
        )

    def write(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> None:
        """
        Serialize and write this object to a metadata file.
        :param path: Metadata file path to write to.
        :param filesystem: File system to use for writing the metadata file. If
        not given, a default filesystem will be automatically selected based on
        the catalog root path.
        """
        path, filesystem = _filesystem(path, filesystem)
        revision_dir_path = posixpath.dirname(path)
        filesystem.create_dir(revision_dir_path, recursive=True)
        with filesystem.open_output_stream(path) as file:
            packed = msgpack.dumps(self.to_serializable())
            file.write(packed)

    def equivalent_to(self, other: Metafile) -> bool:
        """
        True if this Metafile is equivalent to the other Metafile minus its
        unique ID and ancestor IDs.

        :param other: Metafile to compare to.
        :return: True if the other metafile is equivalent, false if not.
        """
        identifiers = {"id", "ancestor_ids"}
        for k, v in self.items():
            if k not in identifiers and (k not in other or other[k] != v):
                return False
        for k in other.keys():
            if k not in identifiers and k not in self:
                return False
        return True

    @property
    def named_immutable_id(self) -> Optional[str]:
        """
        If this metafile's locator name is immutable (i.e., if the object it
        refers to can't be renamed) then returns an immutable ID suitable for
        use in URLS or filesystem paths. Returns None if this locator name is
        mutable (i.e., if the object it refers to can be renamed).
        """
        return self.locator.name.immutable_id

    @named_immutable_id.setter
    def named_immutable_id(self, immutable_id: Optional[str]) -> None:
        """
        If this metafile's locator name is immutable (i.e., if the object it
        refers to can't be renamed), then sets an immutable ID for this
        locator name suitable for use in URLS or filesystem paths. Note that
        the ID is only considered immutable in durable catalog storage, and
        remains mutable in transient memory (i.e., this setter remains
        functional regardless of whether an ID is already assigned, but each
        update will cause it to refer to a different, distinct object in
        durable storage).
        :raises NotImplementedError: If this metafile type does not have a
        named immutable ID (i.e., its immutable ID is auto-generated).
        """
        self.locator.name.immutable_id = immutable_id

    @property
    def id(self) -> str:
        """
        Returns an existing immutable ID for this metafile or generates a new
        one. This ID can be used for equality checks (i.e. 2 metafiles refer
        to the same catalog object if they have the same ID) and deterministic
        references (e.g. for generating a root namespace or table path that
        remains the same regardless of renames).
        """

        # check if the locator name can be reused as an immutable ID
        # or if we need to use a generated UUID as an immutable ID
        _id = self.locator.name.immutable_id or self.get("id")
        if not _id:
            _id = self["id"] = str(uuid.uuid4())
        return _id

    @property
    def locator(self) -> Optional[Locator]:
        """
        Returns the canonical locator for this metafile, which is typically used
        to efficiently resolve internal system references to this object.
        """
        raise NotImplementedError()

    @property
    def locator_alias(self) -> Optional[Locator]:
        """
        Returns an optional locator alias for this metafile. This is
        typically used to resolve a unique, human-readable reference to this
        object (e.g., by using partition values instead of partition ID or
        stream format name instead of stream ID). Locator aliases are
        typically used during partition predicate pushdown (e.g., by
        partition value + partition scheme ID) or to display unique
        human-readable metafile names.
        """
        return None

    def children(
        self,
        catalog_root: str,
        txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
    ) -> ListResult[Metafile]:
        """
        Retrieve all children of this object.
        :return: ListResult containing all children of this object.
        """
        catalog_root, filesystem = _filesystem(
            catalog_root,
            filesystem,
        )
        ancestor_ids = self.ancestor_ids(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        parent_obj_path = posixpath.join(*[catalog_root] + ancestor_ids)
        metafile_root_dir_path = posixpath.join(
            parent_obj_path,
            self.id,
        )
        return self._list_metafiles(
            txn_log_dir=txn_log_dir,
            metafile_root_dir_path=metafile_root_dir_path,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            limit=limit,
        )

    def siblings(
        self,
        catalog_root: str,
        txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
    ) -> ListResult[Metafile]:
        """
        Retrieve all siblings of this object.
        :return: ListResult containing all siblings of this object.
        """
        catalog_root, filesystem = _filesystem(
            catalog_root,
            filesystem,
        )
        ancestor_ids = self.ancestor_ids(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        parent_obj_path = posixpath.join(*[catalog_root] + ancestor_ids)
        return self._list_metafiles(
            txn_log_dir=txn_log_dir,
            metafile_root_dir_path=parent_obj_path,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            limit=limit,
        )

    def revisions(
        self,
        catalog_root: str,
        txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
        materialize_revisions: bool = True,
    ) -> ListResult[Tuple[TransactionOperationType, Optional[Metafile]]]:
        """
        Retrieve all revisions of this object.
        :return: ListResult containing all revisions of this object.
        """
        catalog_root, filesystem = _filesystem(
            catalog_root,
            filesystem,
        )
        ancestor_ids = self.ancestor_ids(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        metafile_root = posixpath.join(*[catalog_root] + ancestor_ids)
        # TODO(pdames): Refactor id lazy assignment into explicit getter/setter
        immutable_id = self.get("id") or Metafile._locator_to_id(
            locator=self.locator,
            catalog_root=catalog_root,
            metafile_root=metafile_root,
            filesystem=filesystem,
            txn_start_time=current_txn_start_time,
            txn_id=current_txn_id,
        )
        revision_dir_path = posixpath.join(
            metafile_root,
            immutable_id,
            REVISION_DIR_NAME,
        )
        revisions = MetafileRevisionInfo.list_revisions(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
            txn_log_dir=txn_log_dir,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            limit=limit,
        )
        items = []
        for mri in revisions:
            if mri.revision:
                metafile = (
                    {}
                    if not materialize_revisions
                    else self.read(
                        path=mri.path,
                        filesystem=filesystem,
                    )
                )
                items.append((mri.txn_op_type, metafile))
        # TODO(pdames): Add pagination.
        return ListResult.of(
            items=items,
            pagination_key=None,
            next_page_provider=None,
        )

    def to_serializable(self) -> Metafile:
        """
        Prepare the object for serialization by converting any non-serializable
        types to serializable types. May also run any required pre-write
        validations on the serialized or deserialized object.
        :return: a serializable version of the object
        """
        return self

    def from_serializable(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Metafile:
        """
        Restore any non-serializable types from a serializable version of this
        object. May also run any required post-read validations on the
        serialized or deserialized object.
        :return: a deserialized version of the object
        """
        return self

    def ancestor_ids(
        self,
        catalog_root: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> List[str]:
        """
        Returns the IDs for this metafile's ancestor metafiles. IDs are
        listed in order from root to immediate parent.
        """
        ancestor_ids = self.get("ancestor_ids") or []
        if not ancestor_ids:
            catalog_root, filesystem = _filesystem(
                path=catalog_root,
                filesystem=filesystem,
            )
            parent_locators = []
            # TODO(pdames): Correctly resolve missing parents and K of N
            #  specified ancestors by using placeholder IDs for missing
            #  ancestors
            parent_locator = self.locator.parent
            while parent_locator:
                parent_locators.append(parent_locator)
                parent_locator = parent_locator.parent
            metafile_root = catalog_root
            while parent_locators:
                parent_locator = parent_locators.pop()
                ancestor_id = Metafile._locator_to_id(
                    locator=parent_locator,
                    catalog_root=catalog_root,
                    metafile_root=metafile_root,
                    filesystem=filesystem,
                    txn_start_time=current_txn_start_time,
                    txn_id=current_txn_id,
                )
                metafile_root = posixpath.join(
                    metafile_root,
                    ancestor_id,
                )
                try:
                    _get_file_info(
                        path=metafile_root,
                        filesystem=filesystem,
                    )
                except FileNotFoundError:
                    raise ValueError(
                        f"Ancestor {parent_locator} does not exist at: "
                        f"{metafile_root}"
                    )
                ancestor_ids.append(ancestor_id)
            self["ancestor_ids"] = ancestor_ids
        return ancestor_ids

    @staticmethod
    def _parent_metafile_rev_dir_path(
        base_metafile_path: str,
        parent_number,
    ):
        # TODO(pdames): Stop parent traversal at catalog root.
        current_dir = posixpath.dirname(  # base metafile root dir
            posixpath.dirname(  # base metafile revision dir
                base_metafile_path,
            )
        )
        while parent_number and current_dir != posixpath.sep:
            current_dir = posixpath.dirname(current_dir)
            parent_number -= 1
        return posixpath.join(
            current_dir,
            REVISION_DIR_NAME,
        )

    @staticmethod
    def _locator_to_id(
        locator: Locator,
        catalog_root: str,
        metafile_root: str,
        filesystem: pyarrow.fs.FileSystem,
        txn_start_time: Optional[int] = None,
        txn_id: Optional[str] = None,
    ) -> str:
        """
        Resolves the metafile ID for the given locator.
        """
        metafile_id = locator.name.immutable_id
        if not metafile_id:
            # the locator name is mutable, so we need to resolve the mapping
            # from the locator back to its immutable metafile ID
            locator_path = locator.path(metafile_root)
            mri = MetafileRevisionInfo.latest_revision(
                revision_dir_path=locator_path,
                filesystem=filesystem,
                txn_log_dir=posixpath.join(catalog_root, TXN_DIR_NAME),
                current_txn_start_time=txn_start_time,
                current_txn_id=txn_id,
            )
            if mri.txn_op_type == TransactionOperationType.DELETE:
                err_msg = (
                    f"Locator {locator} to metafile ID resolution failed "
                    f"because its metafile ID mapping was deleted. You may "
                    f"have an old reference to a renamed or deleted object."
                )
                raise ValueError(err_msg)
            metafile_id = posixpath.splitext(mri.path)[1][1:]
        return metafile_id

    def _write_locator_to_id_map_file(
        self,
        locator: Locator,
        txn_log_dir: str,
        parent_obj_path: str,
        current_txn_op: TransactionOperation,
        current_txn_op_type: TransactionOperationType,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        name_resolution_dir_path = locator.path(parent_obj_path)
        mri = MetafileRevisionInfo.new_revision(
            revision_dir_path=name_resolution_dir_path,
            current_txn_op_type=current_txn_op_type,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            extension=f".{self.id}",
            txn_log_dir=txn_log_dir,
        )
        revision_file_path = mri.path
        filesystem.create_dir(posixpath.dirname(revision_file_path), recursive=True)
        with filesystem.open_output_stream(revision_file_path):
            pass  # Just create an empty ID file to map to the locator
        current_txn_op.append_locator_write_path(revision_file_path)

    def _write_metafile_revision(
        self,
        txn_log_dir: str,
        revision_dir_path: str,
        current_txn_op: TransactionOperation,
        current_txn_op_type: TransactionOperationType,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        mri = MetafileRevisionInfo.new_revision(
            revision_dir_path=revision_dir_path,
            current_txn_op_type=current_txn_op_type,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            txn_log_dir=txn_log_dir,
        )
        self.write(
            path=mri.path,
            filesystem=filesystem,
        )
        current_txn_op.append_metafile_write_path(mri.path)

    def _write_metafile_revisions(
        self,
        catalog_root: str,
        txn_log_dir: str,
        current_txn_op: TransactionOperation,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        """
        Generates the fully qualified paths required to write this metafile as
        part of the given transaction. All paths returned will be based in the
        given root directory.
        """
        ancestor_path_elements = self.ancestor_ids(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        parent_obj_path = posixpath.join(*[catalog_root] + ancestor_path_elements)
        mutable_src_locator = None
        mutable_dest_locator = None
        if not self.named_immutable_id:
            mutable_src_locator = (
                current_txn_op.src_metafile.locator
                if current_txn_op.src_metafile
                else None
            )
            mutable_dest_locator = current_txn_op.dest_metafile.locator
        elif self.locator_alias:
            mutable_src_locator = (
                current_txn_op.src_metafile.locator_alias
                if current_txn_op.src_metafile
                else None
            )
            mutable_dest_locator = current_txn_op.dest_metafile.locator_alias
        if mutable_dest_locator:
            # the locator name is mutable, so we need to persist a mapping
            # from the locator back to its immutable metafile ID
            if (
                current_txn_op.type == TransactionOperationType.UPDATE
                and mutable_src_locator != mutable_dest_locator
            ):
                # this update includes a rename
                # mark the source metafile mapping as deleted
                current_txn_op.src_metafile._write_locator_to_id_map_file(
                    locator=mutable_src_locator,
                    txn_log_dir=txn_log_dir,
                    parent_obj_path=parent_obj_path,
                    current_txn_op=current_txn_op,
                    current_txn_op_type=TransactionOperationType.DELETE,
                    current_txn_start_time=current_txn_start_time,
                    current_txn_id=current_txn_id,
                    filesystem=filesystem,
                )
                # mark the dest metafile mapping as created
                self._write_locator_to_id_map_file(
                    locator=mutable_dest_locator,
                    txn_log_dir=txn_log_dir,
                    parent_obj_path=parent_obj_path,
                    current_txn_op=current_txn_op,
                    current_txn_op_type=TransactionOperationType.CREATE,
                    current_txn_start_time=current_txn_start_time,
                    current_txn_id=current_txn_id,
                    filesystem=filesystem,
                )
            else:
                self._write_locator_to_id_map_file(
                    locator=mutable_dest_locator,
                    txn_log_dir=txn_log_dir,
                    parent_obj_path=parent_obj_path,
                    current_txn_op=current_txn_op,
                    current_txn_op_type=current_txn_op.type,
                    current_txn_start_time=current_txn_start_time,
                    current_txn_id=current_txn_id,
                    filesystem=filesystem,
                )
        metafile_revision_dir_path = posixpath.join(
            parent_obj_path,
            self.id,
            REVISION_DIR_NAME,
        )
        if (
            current_txn_op.type == TransactionOperationType.UPDATE
            and current_txn_op.src_metafile.id != current_txn_op.dest_metafile.id
        ):
            # TODO(pdames): block operations including both a rename & replace?
            # this update includes a replace
            # mark the source metafile as deleted
            src_metafile_revision_dir_path = posixpath.join(
                parent_obj_path,
                current_txn_op.src_metafile.id,
                REVISION_DIR_NAME,
            )
            self._write_metafile_revision(
                txn_log_dir=txn_log_dir,
                revision_dir_path=src_metafile_revision_dir_path,
                current_txn_op=current_txn_op,
                current_txn_op_type=TransactionOperationType.DELETE,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )
            # mark the dest metafile as created
            self._write_metafile_revision(
                txn_log_dir=txn_log_dir,
                revision_dir_path=metafile_revision_dir_path,
                current_txn_op=current_txn_op,
                current_txn_op_type=TransactionOperationType.CREATE,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )
        else:
            self._write_metafile_revision(
                txn_log_dir=txn_log_dir,
                revision_dir_path=metafile_revision_dir_path,
                current_txn_op=current_txn_op,
                current_txn_op_type=current_txn_op.type,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )

    def _list_metafiles(
        self,
        txn_log_dir: str,
        metafile_root_dir_path: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
    ) -> ListResult[Metafile]:
        file_paths_and_sizes = _list_directory(
            path=metafile_root_dir_path,
            filesystem=filesystem,
            ignore_missing_path=True,
        )
        # TODO(pdames): Exclude name resolution directories
        revision_dir_paths = [
            posixpath.join(file_path_and_size[0], REVISION_DIR_NAME)
            for file_path_and_size in file_paths_and_sizes
            if file_path_and_size[0] != txn_log_dir
        ]
        items = []
        for path in revision_dir_paths:
            mri = MetafileRevisionInfo.latest_revision(
                revision_dir_path=path,
                filesystem=filesystem,
                txn_log_dir=txn_log_dir,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                ignore_missing_revision=True,
            )
            if mri.revision:
                item = self.read(
                    path=mri.path,
                    filesystem=filesystem,
                )
                items.append(item)
            if limit and limit <= len(items):
                break
        # TODO(pdames): Add pagination.
        return ListResult.of(
            items=items,
            pagination_key=None,
            next_page_provider=None,
        )

    def _ancestor_ids(
        self,
        catalog_root: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> List[str]:
        """
        Retrieve all ancestor IDs of this object.
        :return: List of ancestor IDs of this object.
        """
        catalog_root, filesystem = _filesystem(
            catalog_root,
            filesystem,
        )
        txn_log_dir = posixpath.join(catalog_root, TXN_DIR_NAME)
        mri = MetafileRevisionInfo.latest_revision(
            revision_dir_path=catalog_root,
            filesystem=filesystem,
            txn_log_dir=txn_log_dir,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            ignore_missing_revision=True,
        )
        if mri.revision:
            return mri.revision.ancestor_ids
        else:
            raise ValueError(f"Metafile {self.id} does not exist.")
        raise NotImplementedError()
