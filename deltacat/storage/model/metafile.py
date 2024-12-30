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
METAFILE_EXT = "mpk"


class TransactionOperation(dict):
    """
    Base class for DeltaCAT transaction operations against individual metafiles.
    """

    @staticmethod
    def of(
        operation_type: Optional[TransactionOperationType],
        metafile: Optional[Metafile],
    ) -> TransactionOperation:
        transaction_operation = TransactionOperation()
        transaction_operation.type = operation_type
        transaction_operation.metafile = metafile
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
    def metafile(self) -> Metafile:
        """
        Returns the metafile that is the target of this transaction operation.
        """
        return self["metafile"]

    @metafile.setter
    def metafile(self, metafile: Metafile):
        self["metafile"] = metafile


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

        # create the transaction directory first to telegraph that at least 1
        # transaction at this root has been attempted
        path, fs = Metafile.filesystem(root, filesystem)
        txn_dir_path = posixpath.join(path, TXN_DIR_NAME)
        fs.create_dir(txn_dir_path, recursive=True)

        # write each metafile associated with the transaction
        write_paths = []
        for operation in self.operations:
            write_path = operation.metafile.write(
                root=root,
                txn_operation_type=operation.type,
                txn_id=self.id,
                filesystem=filesystem,
            )
            write_paths.append(write_path)

        # record the completed transaction
        path, fs = Metafile.filesystem(root, filesystem)
        id_file_path = posixpath.join(txn_dir_path, self.id)
        with fs.open_output_stream(id_file_path):
            pass  # Just create an empty transaction ID file for now
        return write_paths


class MetafileCommitInfo(dict):
    """
    Base class for DeltaCAT metadata file commit info, with read and write
    methods for dict-based DeltaCAT models. Uses msgpack for
    cross-language-compatible serialization and deserialization.
    """

    @staticmethod
    def read(
        base_metafile_path: str,
        filesystem: pyarrow.fs.FileSystem,
        parent_number: int = 0,
        transaction_log_dir: Optional[str] = None,
        ignore_missing_commit: bool = False,
    ) -> MetafileCommitInfo:
        # TODO(pdames): Stop parent traversal at catalog root.

        # resolve the directory path of the target metafile
        current_dir = posixpath.dirname(posixpath.dirname(base_metafile_path))
        while parent_number and current_dir != posixpath.sep:
            current_dir = posixpath.dirname(current_dir)
            parent_number -= 1
        target_metafile_revisions_dir = posixpath.join(
            current_dir,
            REVISION_DIR_NAME,
        )
        # resolve the directory path of the transaction log
        while not transaction_log_dir:
            transaction_log_dir = posixpath.join(
                current_dir,
                TXN_DIR_NAME,
            )
            try:
                _get_file_infos(transaction_log_dir, filesystem)
            except FileNotFoundError:
                transaction_log_dir = None
                if current_dir == posixpath.sep:
                    break
                current_dir = posixpath.dirname(current_dir)
        if not transaction_log_dir:
            err_msg = f"No transaction log found for: {base_metafile_path}."
            raise ValueError(err_msg)
        # find the latest committed revision of the target metafile
        file_paths_and_sizes = _get_file_infos(
            target_metafile_revisions_dir,
            filesystem,
            ignore_missing_path=ignore_missing_commit,
        )
        if not file_paths_and_sizes and not ignore_missing_commit:
            err_msg = (
                f"Expected to find at least 1 Metafile at "
                f"{target_metafile_revisions_dir} but found none."
            )
            raise ValueError(err_msg)
        file_paths = list(zip(*file_paths_and_sizes))[0] if file_paths_and_sizes else []
        sorted_metafile_paths = sorted(file_paths)
        revision = None
        txn_id = None
        latest_committed_metafile_path = None
        while sorted_metafile_paths:
            latest_metafile_path = sorted_metafile_paths.pop()
            latest_metafile_name = posixpath.basename(latest_metafile_path)
            metafile_and_ext = posixpath.splitext(latest_metafile_name)
            if metafile_and_ext[1] != f".{METAFILE_EXT}":
                err_msg = (
                    f"File at {latest_metafile_path} does not appear to be a valid "
                    f"Metafile. Expected extension {METAFILE_EXT} but found "
                    f"{metafile_and_ext[1]}"
                )
                raise ValueError(err_msg)
            metafile_rev_and_txn_id = metafile_and_ext[0]
            rev_and_txn_id_split = metafile_rev_and_txn_id.split(TXN_ID_SEPARATOR)
            revision = rev_and_txn_id_split[0]
            txn_id = rev_and_txn_id_split[1]
            file_paths_and_sizes = _get_file_infos(
                posixpath.join(transaction_log_dir, txn_id),
                filesystem,
            )
            if file_paths_and_sizes:
                latest_committed_metafile_path = latest_metafile_path
                break
        if not latest_committed_metafile_path and not ignore_missing_commit:
            err_msg = (
                f"No completed transaction with ID {txn_id} found at "
                f"{transaction_log_dir}."
            )
            raise ValueError(err_msg)
        mci = MetafileCommitInfo()
        mci.revision = int(revision) if revision else 0
        mci.txn_id = txn_id
        mci.path = latest_committed_metafile_path
        return mci

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
    def path(self) -> Optional[str]:
        return self["path"]

    @path.setter
    def path(self, path: str):
        self["path"] = path


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
    def _locator_to_id(
        locator: Locator,
        root: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> str:
        """
        Resolves the metafile ID for the given locator.
        """
        metafile_id = locator.name().immutable_id()
        if not metafile_id:
            # the locator name is mutable, so we need to resolve the mapping
            # from the locator back to its immutable metafile ID
            locator_path = locator.path(root)
            file_paths_and_sizes = _get_file_infos(
                locator_path,
                filesystem,
            )
            if len(file_paths_and_sizes) != 1:
                # TODO(pdames): Account for mappings from failed transactions
                err_msg = (
                    f"Expected to find 1 Locator to Metafile ID mapping at "
                    f"`{locator_path}` but found {len(file_paths_and_sizes)}"
                )
                raise ValueError(err_msg)
            metafile_id = posixpath.basename(file_paths_and_sizes[0][0])
        return metafile_id

    def ancestor_ids(
        self,
        root: str,
        filesystem: pyarrow.fs.FileSystem,
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
                    parent_locators.pop(),
                    root,
                    filesystem,
                )
                root = posixpath.join(root, ancestor_id)
                ancestor_ids.append(ancestor_id)
        return ancestor_ids

    def _generate_locator_to_id_map_file(
        self,
        parent_path: str,
        filesystem: pyarrow.fs.FileSystem,
    ):
        # the locator name is mutable, so we need to persist a mapping
        # from the locator back to its immutable metafile ID
        id_dir_path = self.locator.path(parent_path)
        file_paths_and_sizes = _get_file_infos(
            id_dir_path,
            filesystem,
            True,
        )
        if file_paths_and_sizes:
            # TODO(pdames): Ensure that the locator-to-id mapping is part
            #  of a committed transaction
            raise ValueError(
                f"Locator {self.locator} digest {self.locator.hexdigest()} already "
                f"mapped to ID {posixpath.basename(file_paths_and_sizes[0][0])}"
            )
        id_file_path = posixpath.join(id_dir_path, self.id)
        filesystem.create_dir(posixpath.dirname(id_file_path), recursive=True)
        with filesystem.open_output_stream(id_file_path):
            pass  # Just create an empty ID file to map to the locator

    def generate_file_path(
        self,
        root: str,
        txn_operation_type: TransactionOperationType,
        txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> str:
        """
        Generates the fully qualified path for this metafile based in the given
        root directory.
        """
        ancestor_path_elements = self.ancestor_ids(
            root,
            filesystem,
        )
        parent_path = posixpath.join(*[root] + ancestor_path_elements)
        if not self.locator.name().immutable_id():
            self._generate_locator_to_id_map_file(
                parent_path,
                filesystem,
            )
        metafile_path = posixpath.join(
            parent_path,
            self.id,
            REVISION_DIR_NAME,
            "temp_file_name",
        )
        is_create_txn = txn_operation_type == TransactionOperationType.CREATE
        metafile_commit_info = MetafileCommitInfo.read(
            metafile_path,
            filesystem,
            ignore_missing_commit=is_create_txn,
        )
        latest_revision_number = metafile_commit_info.revision
        # validate the transaction operation type
        if latest_revision_number and is_create_txn:
            raise ValueError(
                f"Metafile creation failed. Metafile commit at "
                f"{metafile_commit_info.path} already exists."
            )
        revision_number = latest_revision_number + 1
        return posixpath.join(
            parent_path,
            self.id,
            REVISION_DIR_NAME,
            f"{revision_number:020}{TXN_ID_SEPARATOR}{txn_id}.{METAFILE_EXT}",
        )

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
        txn_operation_type: TransactionOperationType,
        txn_id: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> str:
        """
        Serialize and write this object to a metadata file.
        :param root: Root directory of the metadata file.
        :param txn_operation_type: Transaction operation type.
        :param txn_id: Transaction ID.
        :param filesystem: File system to use for writing the metadata file.
        :return: File path of the written metadata file.
        """
        path, fs = Metafile.filesystem(
            path=root,
            filesystem=filesystem,
        )
        path = self.generate_file_path(
            root=root,
            txn_operation_type=txn_operation_type,
            txn_id=txn_id,
            filesystem=fs,
        )
        fs.create_dir(posixpath.dirname(path), recursive=True)
        with fs.open_output_stream(path) as file:
            packed = msgpack.dumps(self.to_serializable())
            file.write(packed)
        return path

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
