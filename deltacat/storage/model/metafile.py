# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Tuple, List

import msgpack
import pyarrow.fs
import posixpath
import uuid

from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import _get_file_infos

from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.locator import Locator
from deltacat.storage.model.types import (
    TransactionType,
    TransactionOperationType,
)

TXN_ID_SEPARATOR = "_"
TXN_DIR_NAME: str = "txn"
REVISION_DIR_NAME: str = "rev"
METAFILE_EXT = ".mpk"


class TransactionOperation(dict):
    """
    Base class for DeltaCAT transaction operations against individual metafiles.
    """

    @staticmethod
    def of(
        operation_type: Optional[TransactionOperationType],
        dest_metafile: Metafile,
        src_metafile: Optional[Metafile] = None,
    ) -> TransactionOperation:
        transaction_operation = TransactionOperation()
        transaction_operation.type = operation_type
        transaction_operation.dest_metafile = dest_metafile
        transaction_operation.src_metafile = src_metafile
        if operation_type == TransactionOperationType.UPDATE:
            if not src_metafile:
                raise ValueError(
                    "UPDATE transaction operations must have a source metafile."
                )
        elif src_metafile:
            raise ValueError(
                "Only UPDATE transaction operations may have a source metafile."
            )
        return transaction_operation

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
    Base class for DeltaCAT transactions against a list of metafiles.
    """

    @staticmethod
    def of(
        txn_type: Optional[TransactionType],
        txn_operations: Optional[TransactionOperationList],
    ) -> Transaction:
        operation_types = set([op.type for op in txn_operations])
        if (
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
        elif TransactionOperationType.UPDATE in operation_types:
            if len(operation_types) == 1 and txn_type != TransactionType.RESTATE:
                raise ValueError(
                    "Transactions with only UPDATE operations must be "
                    "specified as part of a RESTATE transaction."
                )
            elif txn_type not in {TransactionType.OVERWRITE, TransactionType.RESTATE}:
                raise ValueError(
                    "Mixed UPDATE+CREATE operations must be specified as part "
                    "of a RESTATE or OVERWRITE transaction."
                )
        transaction = Transaction()
        transaction.type = txn_type
        transaction.operations = txn_operations
        return transaction

    @property
    def id(self) -> str:
        """
        Returns this transaction's unique ID.
        """
        identifier = self.get("id")
        if not identifier:
            identifier = self["id"] = str(uuid.uuid4())
        return identifier

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

    def commit(
        self,
        root: str,
        filesystem: pyarrow.fs.FileSystem = None,
    ) -> List[str]:
        # TODO(pdames): (1) enforce table-version-level transaction isolation
        #  APPEND transactions run concurrently
        #  DELETE/RESTATE/OVERWRITE transactions run serially
        #  APPEND transactions may auto-resolve all conflicts via retry
        #  DELETE/RESTATE/OVERWRITE txns fail all conflicts with each-other
        #  (2) support catalog global and table-version-local transaction
        #  pointer queries and rollback/rollforward
        #  (3) allow transaction changes to be durably staged and resumed
        #  across multiple sessions prior to commit
        #  (4) Add locator-to-id mapping files to returned results

        # create the transaction directory first to telegraph that at least 1
        # transaction at this root has been attempted
        path, fs = Metafile.filesystem(root, filesystem)
        txn_dir_path = posixpath.join(path, TXN_DIR_NAME)
        fs.create_dir(txn_dir_path, recursive=True)

        # write each metafile associated with the transaction
        all_write_paths = []
        for operation in self.operations:
            write_paths = operation.dest_metafile.write(
                root=root,
                txn_operation=operation,
                txn_id=self.id,
                filesystem=filesystem,
            )
            all_write_paths.extend(write_paths)

        # record the completed transaction
        path, fs = Metafile.filesystem(root, filesystem)
        id_file_path = posixpath.join(txn_dir_path, self.id)
        with fs.open_output_stream(id_file_path):
            pass  # Just create an empty transaction ID file for now
        return all_write_paths


class MetafileCommitInfo(dict):
    """
    Base class for DeltaCAT metafile commit info.
    """

    @staticmethod
    def current(
        commit_dir_path: str,
        filesystem: pyarrow.fs.FileSystem,
        current_txn_id: Optional[str] = None,
        txn_log_dir: Optional[str] = None,
        ignore_missing_commit: bool = False,
    ) -> MetafileCommitInfo:
        # TODO(pdames): Stop parent traversal at catalog root.
        # resolve the directory path of the transaction log
        current_dir = commit_dir_path
        while not txn_log_dir:
            txn_log_dir = posixpath.join(
                current_dir,
                TXN_DIR_NAME,
            )
            try:
                _get_file_infos(txn_log_dir, filesystem)
            except FileNotFoundError:
                txn_log_dir = None
                if current_dir == posixpath.sep:
                    break
                current_dir = posixpath.dirname(current_dir)
        if not txn_log_dir:
            err_msg = f"No transaction log found for: {commit_dir_path}."
            raise ValueError(err_msg)
        # find the latest committed revision of the target metafile
        file_paths_and_sizes = _get_file_infos(
            commit_dir_path,
            filesystem,
            ignore_missing_path=ignore_missing_commit,
        )
        if not file_paths_and_sizes and not ignore_missing_commit:
            err_msg = (
                f"Expected to find at least 1 Metafile at "
                f"{commit_dir_path} but found none."
            )
            raise ValueError(err_msg)
        file_paths = list(zip(*file_paths_and_sizes))[0] if file_paths_and_sizes else []
        sorted_metafile_paths = sorted(file_paths)
        revision = None
        txn_id = None
        txn_op_type = None
        latest_committed_metafile_path = None
        while sorted_metafile_paths:
            latest_metafile_path = sorted_metafile_paths.pop()
            latest_metafile_name = posixpath.basename(latest_metafile_path)
            metafile_and_ext = posixpath.splitext(latest_metafile_name)
            metafile_rev_and_txn_id = metafile_and_ext[0]
            rev_and_txn_id_split = metafile_rev_and_txn_id.split(TXN_ID_SEPARATOR)
            revision = rev_and_txn_id_split[0]
            txn_op_type = rev_and_txn_id_split[1]
            txn_id = rev_and_txn_id_split[2]
            # consider the current in-progress transaction to be committed
            if current_txn_id and txn_id == current_txn_id:
                latest_committed_metafile_path = latest_metafile_path
                break
            else:
                file_paths_and_sizes = _get_file_infos(
                    posixpath.join(txn_log_dir, txn_id),
                    filesystem,
                )
                if file_paths_and_sizes:
                    latest_committed_metafile_path = latest_metafile_path
                    break
        if not latest_committed_metafile_path and not ignore_missing_commit:
            err_msg = (
                f"No committed transaction with ID {txn_id} found at " f"{txn_log_dir}."
            )
            raise ValueError(err_msg)
        mci = MetafileCommitInfo()
        mci.revision = int(revision) if revision else 0
        mci.txn_id = txn_id
        mci.txn_operation_type = txn_op_type
        mci.path = latest_committed_metafile_path
        return mci

    @staticmethod
    def next(
        base_metafile_dir_path: str,
        txn_operation_type: TransactionOperationType,
        txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
        extension: str = METAFILE_EXT,
        txn_log_dir: Optional[str] = None,
    ) -> MetafileCommitInfo:
        is_create_txn = txn_operation_type == TransactionOperationType.CREATE
        mci = MetafileCommitInfo.current(
            commit_dir_path=base_metafile_dir_path,
            filesystem=filesystem,
            current_txn_id=txn_id,
            txn_log_dir=txn_log_dir,
            ignore_missing_commit=is_create_txn,
        )
        # validate the transaction operation type
        if mci.revision:
            # update/delete fails if the last metafile was deleted
            if mci.txn_operation_type == TransactionOperationType.DELETE:
                if txn_operation_type != TransactionOperationType.CREATE:
                    raise ValueError(
                        f"Metafile {txn_operation_type.value} failed for "
                        f"transaction ID {txn_id} failed. Metafile state at "
                        f"{mci.path} is deleted."
                    )
            # create fails unless the last metafile was deleted
            elif is_create_txn:
                raise ValueError(
                    f"Metafile creation for transaction ID {txn_id} failed. "
                    f"Metafile commit at {mci.path} already exists."
                )
        elif not is_create_txn:
            # update/deletes fails if the last metafile doesn't exist
            raise ValueError(
                f"Metafile {txn_operation_type.value} failed for "
                f"transaction ID {txn_id} failed. Metafile at {mci.path} "
                f"doesn't exist."
            )

        mci.next_txn_id = txn_id
        mci.next_txn_operation_type = txn_operation_type
        metafile_name = TXN_ID_SEPARATOR.join(
            [
                f"{mci.next_revision:020}",
                txn_operation_type.value,
                f"{txn_id}{extension}",
            ]
        )
        mci.next_path = posixpath.join(
            base_metafile_dir_path,
            metafile_name,
        )
        return mci

    @property
    def revision(self) -> int:
        return self["revision"]

    @revision.setter
    def revision(self, revision: int):
        self["revision"] = revision

    @property
    def next_revision(self) -> Optional[int]:
        return self.revision + 1

    @property
    def txn_id(self) -> Optional[str]:
        return self["txn_id"]

    @txn_id.setter
    def txn_id(self, txn_id: str):
        self["txn_id"] = txn_id

    @property
    def txn_operation_type(self) -> Optional[TransactionOperationType]:
        op_type = self.get("txn_operation_type")
        return None if op_type is None else TransactionOperationType(op_type)

    @txn_operation_type.setter
    def txn_operation_type(self, txn_operation_type: TransactionOperationType):
        self["txn_operation_type"] = txn_operation_type

    @property
    def path(self) -> Optional[str]:
        return self["path"]

    @path.setter
    def path(self, path: str):
        self["path"] = path

    @property
    def next_txn_id(self) -> Optional[str]:
        return self["next_txn_id"]

    @next_txn_id.setter
    def next_txn_id(self, next_txn_id: str):
        self["next_txn_id"] = next_txn_id

    @property
    def next_txn_operation_type(self) -> Optional[TransactionOperationType]:
        op_type = self.get("next_txn_operation_type")
        return None if op_type is None else TransactionOperationType(op_type)

    @next_txn_operation_type.setter
    def next_txn_operation_type(
        self, next_txn_operation_type: TransactionOperationType
    ):
        self["next_txn_operation_type"] = next_txn_operation_type

    @property
    def next_path(self) -> Optional[str]:
        return self["next_path"]

    @next_path.setter
    def next_path(self, next_path: str):
        self["next_path"] = next_path


class Metafile(dict):
    """
    Base class for DeltaCAT metadata files, with read and write methods
    for dict-based DeltaCAT models. Uses msgpack (https://msgpack.org/) for
    cross-language-compatible serialization and deserialization.
    """

    @property
    def id(self) -> str:
        """
        Returns an immutable ID for the given metafile that can be used for
        equality checks (i.e. 2 metafiles are equal if they have the same ID)
        and deterministic references (e.g. for generating a table file path that
        remains the same regardless of renames).
        """
        _id = self.get("id")
        if not _id:
            # check if the locator name can be reused as an immutable ID
            # or if we need to generate a new immutable ID
            _id = self["id"] = self.locator.name().immutable_id() or str(uuid.uuid4())
        return _id

    @property
    def locator(self) -> Optional[Locator]:
        raise NotImplementedError()

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
        locator: Locator, root: str, filesystem: pyarrow.fs.FileSystem, txn_id: str
    ) -> str:
        """
        Resolves the metafile ID for the given locator.
        """
        metafile_id = locator.name().immutable_id()
        if not metafile_id:
            # the locator name is mutable, so we need to resolve the mapping
            # from the locator back to its immutable metafile ID
            locator_path = locator.path(root)
            mci = MetafileCommitInfo.current(
                commit_dir_path=locator_path,
                filesystem=filesystem,
                current_txn_id=txn_id,
                txn_log_dir=posixpath.join(root, TXN_DIR_NAME),
            )
            metafile_id = posixpath.splitext(mci.path)[1][1:]
        return metafile_id

    def ancestor_ids(
        self,
        root: str,
        filesystem: pyarrow.fs.FileSystem,
        txn_id: str,
    ) -> List[str]:
        """
        Returns the IDs for this metafile's ancestor metafiles. IDs are
        listed in order from root to immediate parent.
        """
        ancestor_ids = self.get("ancestor_ids") or []
        if not ancestor_ids:
            parent_locators = []
            # TODO(pdames): Correctly resolve missing parents and K of N
            #  specified ancestors by using placeholder IDs for missing
            #  ancestors
            parent_locator = self.locator.parent()
            while parent_locator:
                parent_locators.append(parent_locator)
                parent_locator = parent_locator.parent()
            while parent_locators:
                ancestor_id = Metafile._locator_to_id(
                    locator=parent_locators.pop(),
                    root=root,
                    filesystem=filesystem,
                    txn_id=txn_id,
                )
                root = posixpath.join(root, ancestor_id)
                ancestor_ids.append(ancestor_id)
        return ancestor_ids

    def _generate_locator_to_id_map_file(
        self,
        root: str,
        parent_path: str,
        txn_operation_type: TransactionOperationType,
        txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        id_dir_path = self.locator.path(parent_path)
        mci = MetafileCommitInfo.next(
            base_metafile_dir_path=id_dir_path,
            txn_operation_type=txn_operation_type,
            txn_id=txn_id,
            filesystem=filesystem,
            extension=f".{self.id}",
            txn_log_dir=posixpath.join(root, TXN_DIR_NAME),
        )
        id_file_path = mci.next_path
        filesystem.create_dir(posixpath.dirname(id_file_path), recursive=True)
        with filesystem.open_output_stream(id_file_path):
            pass  # Just create an empty ID file to map to the locator

    def generate_file_paths(
        self,
        root: str,
        txn_operation: TransactionOperation,
        txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> List[str]:
        """
        Generates the fully qualified path for this metafile based in the given
        root directory.
        """
        ancestor_path_elements = self.ancestor_ids(
            root=root,
            filesystem=filesystem,
            txn_id=txn_id,
        )
        parent_path = posixpath.join(*[root] + ancestor_path_elements)
        if not self.locator.name().immutable_id():
            # the locator name is mutable, so we need to persist a mapping
            # from the locator back to its immutable metafile ID
            # TODO(pdames): Mark replaces as updates.
            if (
                txn_operation.type == TransactionOperationType.UPDATE
                and txn_operation.src_metafile.locator
                != txn_operation.dest_metafile.locator
            ):
                # this update includes a rename
                # mark the source metafile mapping as deleted
                txn_operation.src_metafile._generate_locator_to_id_map_file(
                    root=root,
                    parent_path=parent_path,
                    txn_operation_type=TransactionOperationType.DELETE,
                    txn_id=txn_id,
                    filesystem=filesystem,
                )
                # mark the dest metafile mapping as created
                self._generate_locator_to_id_map_file(
                    root=root,
                    parent_path=parent_path,
                    txn_operation_type=TransactionOperationType.CREATE,
                    txn_id=txn_id,
                    filesystem=filesystem,
                )
            else:
                self._generate_locator_to_id_map_file(
                    root=root,
                    parent_path=parent_path,
                    txn_operation_type=txn_operation.type,
                    txn_id=txn_id,
                    filesystem=filesystem,
                )
        metafile_dir_path = posixpath.join(
            parent_path,
            self.id,
            REVISION_DIR_NAME,
        )
        paths = []
        if (
            txn_operation.type == TransactionOperationType.UPDATE
            and txn_operation.src_metafile.id != txn_operation.dest_metafile.id
        ):
            # TODO(pdames): block operations including both a rename & replace?
            # this update includes a replace
            # mark the source metafile as deleted
            src_metafile_dir_path = posixpath.join(
                parent_path,
                txn_operation.src_metafile.id,
                REVISION_DIR_NAME,
            )
            mci = MetafileCommitInfo.next(
                base_metafile_dir_path=src_metafile_dir_path,
                txn_operation_type=TransactionOperationType.DELETE,
                txn_id=txn_id,
                filesystem=filesystem,
                txn_log_dir=posixpath.join(root, TXN_DIR_NAME),
            )
            paths.append(mci.next_path)
            # mark the dest metafile as created
            mci = MetafileCommitInfo.next(
                base_metafile_dir_path=metafile_dir_path,
                txn_operation_type=TransactionOperationType.CREATE,
                txn_id=txn_id,
                filesystem=filesystem,
                txn_log_dir=posixpath.join(root, TXN_DIR_NAME),
            )
            paths.append(mci.next_path)
        else:
            mci = MetafileCommitInfo.next(
                base_metafile_dir_path=metafile_dir_path,
                txn_operation_type=txn_operation.type,
                txn_id=txn_id,
                filesystem=filesystem,
                txn_log_dir=posixpath.join(root, TXN_DIR_NAME),
            )
            paths.append(mci.next_path)
        return paths

    @staticmethod
    def filesystem(
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
        paths, filesystem = _resolve_paths_and_filesystem(path, filesystem)
        assert len(paths) == 1, len(paths)
        return paths[0], filesystem

    @property
    def children(self) -> ListResult[Metafile]:
        """
        Retrieve all children of this object.
        :return: ListResult containing all children of this object.
        """
        # from ray.data.datasource.file_meta_provider import _expand_directory
        # filesystem = Metafile.file_system(root)
        # file_paths_and_sizes = _expand_directory(root, filesystem)
        raise NotImplementedError()

    @property
    def siblings(self) -> ListResult[Metafile]:
        """
        Retrieve all siblings of this object.
        :return: ListResult containing all siblings of this object.
        """
        raise NotImplementedError()

    @property
    def revisions(self) -> ListResult[Metafile]:
        """
        Retrieve all revisions of this object.
        :return: ListResult containing all revisions of this object.
        """
        raise NotImplementedError()

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

    def write(
        self,
        root: str,
        txn_operation: TransactionOperation,
        txn_id: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> List[str]:
        """
        Serialize and write this object to a metadata file.
        :param root: Root directory of the metadata file.
        :param txn_operation: Transaction operation.
        :param txn_id: Transaction ID.
        :param filesystem: File system to use for writing the metadata file.
        :return: File path of the written metadata file.
        """
        path, fs = Metafile.filesystem(
            path=root,
            filesystem=filesystem,
        )
        paths = self.generate_file_paths(
            root=path,
            txn_operation=txn_operation,
            txn_id=txn_id,
            filesystem=fs,
        )
        for path in paths:
            fs.create_dir(posixpath.dirname(path), recursive=True)
            with fs.open_output_stream(path) as file:
                packed = msgpack.dumps(self.to_serializable())
                file.write(packed)
        return paths

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
        path, fs = Metafile.filesystem(path, filesystem)
        with fs.open_input_stream(path) as file:
            binary = file.readall()
        obj = cls(**msgpack.loads(binary)).from_serializable(path, fs)
        return obj
