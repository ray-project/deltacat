# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Tuple, List

import msgpack
import pyarrow.fs
import os
import uuid

from ray.data.datasource.file_meta_provider import _get_file_infos

from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.locator import Locator
from deltacat.storage.model.types import (
    TransactionType,
    TransactionOperationType,
)


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
        transaction = Transaction()
        # TODO(pdames): validate proposed transaction type against operations
        #  (e.g., an APPEND transaction can't delete metafiles)
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
        separator: str = "/",
        filesystem: pyarrow.fs.FileSystem = None,
    ) -> List[str]:
        write_paths = []
        for operation in self.operations:
            write_path = operation.metafile.write(
                root=root,
                txn_operation_type=operation.type,
                txn_id=self.id,
                separator=separator,
                filesystem=filesystem,
            )
            write_paths.append(write_path)
        # TODO(pdames): enforce tabel-version-level transaction isolation
        # record the transaction as complete
        path, fs = Metafile.file_system(root, filesystem)
        id_file_path = separator.join([path, "transactions", self.id])
        fs.create_dir(os.path.dirname(id_file_path), recursive=True)
        with fs.open_output_stream(id_file_path):
            pass  # Just create an empty UUID file for the transaction
        return write_paths


class MetafileUrl:
    def __init__(self, url: str):

        from urllib.parse import urlparse

        self._parsed = urlparse(url, allow_fragments=False)  # support '#' in path
        if not self._parsed.scheme:  # support paths w/o 's3://' scheme
            url = f"s3://{url}"
            self._parsed = urlparse(url, allow_fragments=False)
        if self._parsed.query:  # support '?' in path
            self.key = f"{self._parsed.path.lstrip('/')}?{self._parsed.query}"
        else:
            self.key = self._parsed.path.lstrip("/")
        self.bucket = self._parsed.netloc
        self.url = self._parsed.geturl()


class Metafile(dict):
    """
    Base class for DeltaCAT metadata files, with read and write methods
    for dict-based DeltaCAT models. Uses msgpack (https://msgpack.org/) for
    cross-language-compatible serialization and deserialization.
    """

    @property
    def id(self) -> Optional[str]:
        """
        Returns an immutable ID for the given metafile that can be used for
        equality checks (i.e. 2 metafiles are equal if they have the same ID)
        and deterministic references (e.g. for generating a table file path that
        remains the same regardless of renames).
        """
        identifier = self.get("id")
        if not identifier:
            identifier = self["id"] = str(uuid.uuid4())
        return identifier

    @property
    def locator(self) -> Optional[Locator]:
        raise NotImplementedError()

    @staticmethod
    def _validate_id(metafile_id: str):
        try:
            uuid.UUID(metafile_id)
        except ValueError as e:
            err_msg = f"Metafile ID is malformed (UUID expected): {metafile_id}"
            raise ValueError(err_msg) from e

    @staticmethod
    def _locator_to_id(
        locator: Locator,
        root: str,
        filesystem: pyarrow.fs.FileSystem,
        separator: str = "/",
    ) -> str:
        """
        Resolves the metafile ID for the given locator.
        """
        locator_path = locator.path(
            root,
            separator,
        )
        file_paths_and_sizes = _get_file_infos(
            locator_path,
            filesystem,
        )

        if len(file_paths_and_sizes) != 1:
            err_msg = (
                f"Expected to find 1 locator to Metafile ID mapping at "
                f"`{locator_path}` but found {len(file_paths_and_sizes)}"
            )
            raise ValueError(err_msg)
        metafile_id = os.path.basename(file_paths_and_sizes[0][0])
        try:
            Metafile._validate_id(metafile_id)
        except ValueError as e:
            err_msg = f"No valid metafile ID found for locator: {locator}"
            raise ValueError(err_msg) from e
        return metafile_id

    def ancestor_ids(
        self,
        root: str,
        filesystem: pyarrow.fs.FileSystem,
        separator: str = "/",
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
                    separator,
                )
                root = separator.join([root, ancestor_id])
                ancestor_ids.append(ancestor_id)
        return ancestor_ids

    def generate_file_path(
        self,
        root: str,
        txn_operation_type: TransactionOperationType,
        txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
        separator: str = "/",
        extension: str = "mpk",
    ) -> str:
        """
        Generates the fully qualified path for this metafile based in the given
        root directory.
        """
        ancestor_path_elements = self.ancestor_ids(
            root,
            filesystem,
            separator,
        )
        ancestor_path = separator.join([root] + ancestor_path_elements)
        id_dir_path = self.locator.path(ancestor_path, separator)
        file_paths_and_sizes = _get_file_infos(
            id_dir_path,
            filesystem,
            True,
        )
        assert not file_paths_and_sizes, (
            f"Locator {self.locator} digest {self.locator.hexdigest()} already "
            f"mapped to ID {os.path.basename(file_paths_and_sizes[0][0])}"
        )
        id_path_elements = [
            id_dir_path,
            self.id,
        ]
        id_file_path = separator.join(id_path_elements)
        filesystem.create_dir(os.path.dirname(id_file_path), recursive=True)
        with filesystem.open_output_stream(id_file_path):
            pass  # Just create an empty ID file to map to the locator
        # TODO(pdames): resolve actual revision number together with
        #  transaction ID and staged/committed status... use CAS on writes
        #  that require revision number updates (e.g., metafile update)
        revision_number = 1
        metafile_path_elements = [
            ancestor_path,
            self.id,
            f"{revision_number:020}_{txn_id}.{extension}",
        ]
        return separator.join(metafile_path_elements)

    @staticmethod
    def file_system(
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Tuple[str, pyarrow.fs.FileSystem]:
        """
        Normalizes the input path and resolves a corresponding file system.
        :param path: A file or directory path.
        :param filesystem: File system to use for path IO.
        :return: Normalized path and resolved file system for that path.
        """
        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        # TODO(pdames): resolve and cache filesystem at catalog root level
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
        types to serializable types.
        :return: a serializable version of the object
        """
        return self

    def from_serializable(self) -> Metafile:
        """
        Restore any non-serializable types from a serializable version of this
        object.
        :return: a fully deserialized version of the object
        """
        return self

    def write(
        self,
        root: str,
        txn_operation_type: TransactionOperationType,
        txn_id: str,
        separator: str = "/",
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> str:
        """
        Serialize and write this object to a metadata file.
        :param root: Root directory of the metadata file.
        :param separator: Separator to use in the metadata file path.
        :param filesystem: File system to use for writing the metadata file.
        :return: File path of the written metadata file.
        """
        path, fs = Metafile.file_system(root, filesystem)
        path = self.generate_file_path(
            root=root,
            txn_operation_type=txn_operation_type,
            txn_id=txn_id,
            filesystem=fs,
            separator=separator,
        )
        fs.create_dir(os.path.dirname(path), recursive=True)
        with fs.open_output_stream(path) as file:
            packed = msgpack.dumps(self.to_serializable())
            file.write(packed)
        return path

    @classmethod
    def read(
        cls, path: str, filesystem: Optional[pyarrow.fs.FileSystem] = None
    ) -> Metafile:
        """
        Read a metadata file and return the deserialized object.
        :param path: Metadata file path to read.
        :param filesystem: File system to use for reading the metadata file.
        :return: Deserialized object from the metadata file.
        """
        path, fs = Metafile.file_system(path, filesystem)
        with fs.open_input_stream(path) as file:
            binary = file.readall()
        obj = cls(**msgpack.loads(binary)).from_serializable()
        return obj
