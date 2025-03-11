# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import copy

from typing import Optional, Tuple, List

import base64
import json
import msgpack
import pyarrow.fs
import posixpath
import uuid
import deltacat

from deltacat.constants import (
    METAFILE_FORMAT,
    REVISION_DIR_NAME,
    METAFILE_EXT,
    SUPPORTED_METAFILE_FORMATS,
    TXN_DIR_NAME,
    TXN_PART_SEPARATOR,
    SUCCESS_TXN_DIR_NAME,
)
from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.locator import Locator
from deltacat.storage.model.types import TransactionOperationType
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    list_directory,
    get_file_info,
)


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
        success_txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[MetafileRevisionInfo]:
        if not success_txn_log_dir:
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
            elif current_txn_start_time is not None:
                # the current transaction can only build on top of the snapshot
                # of commits from transactions that completed before it started
                txn_end_time = (
                    deltacat.storage.model.transaction.Transaction.read_end_time(
                        path=posixpath.join(success_txn_log_dir, mri.txn_id),
                        filesystem=filesystem,
                    )
                )
                if txn_end_time is not None and txn_end_time < current_txn_start_time:
                    revisions.append(mri)
            else:
                raise ValueError(
                    f"Current transaction ID `{current_txn_id} provided "
                    f"without a transaction start time."
                )
            if limit <= len(revisions):
                break
        return revisions

    @staticmethod
    def latest_revision(
        revision_dir_path: str,
        filesystem: pyarrow.fs.FileSystem,
        success_txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        ignore_missing_revision: bool = False,
    ) -> MetafileRevisionInfo:
        """
        Fetch latest revision of a metafile, or return None if no
        revisions exist.
        :param revision_dir_path: root path of directory for metafile
        :param ignore_missing_revision: if True, will return
        MetafileRevisionInfo.undefined() on no revisions
        :raises ValueError if no revisions are found AND
        ignore_missing_revision=False
        """
        revisions = MetafileRevisionInfo.list_revisions(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
            success_txn_log_dir=success_txn_log_dir,
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
        current_txn_op_type: deltacat.storage.model.transaction.TransactionOperationType,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
        extension: Optional[str] = METAFILE_EXT,
        success_txn_log_dir: Optional[str] = None,
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
            success_txn_log_dir (Optional[str], optional): Directory path for
            successful transaction logs. Will be automatically discovered by
            traversing revision directory parent paths if not specified.

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
            success_txn_log_dir=success_txn_log_dir,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            ignore_missing_revision=is_create_txn,
        )
        # validate the transaction operation type
        if mri.exists():
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
        success_txn_log_dir: str,
        current_txn_revision_file_path: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        """
        Checks for a concurrent modification conflict between a file commited
        by the current transaction and another parallel transaction. Raises
        an exception if a concurrent modification conflict is found.

        :param success_txn_log_dir: Path to the log of successful transactions.
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
                txn_end_time = (
                    deltacat.storage.model.transaction.Transaction.read_end_time(
                        path=posixpath.join(success_txn_log_dir, mri.txn_id),
                        filesystem=filesystem,
                    )
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
        file_paths_and_sizes = list_directory(
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

    def exists(self) -> bool:
        return bool(self.revision)


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
        success_txn_log_dir: str,
        current_txn_op: deltacat.storage.model.transaction.TransactionOperation,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> ListResult[Metafile]:
        """
        Read one or more metadata files within the context of a transaction.
        :param catalog_root_dir: Catalog root dir to read the metafile from.
        :param success_txn_log_dir: Catalog root successful transaction log
        directory.
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
            "success_txn_log_dir": success_txn_log_dir,
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
        else:
            # Could not find any revisions in list operations - return no results
            return ListResult.empty()

    @staticmethod
    def get_class(serialized_dict: dict):
        """
        Given a serialized dictionary of Metafile data, gets the metafile child
        class type to instantiate.
        """
        # TODO: more robust implementation. Right now this relies on the
        #  assumption that XLocator key will only be present in class X, and
        #  is brittle to renames. On the other hand, this implementation does
        #  not require any marker fields to be persisted, and a regression
        #  will be quickly detected by test_metafile.io or other unit tests
        if serialized_dict.__contains__("tableLocator"):
            return deltacat.storage.model.table.Table
        elif serialized_dict.__contains__("namespaceLocator"):
            return deltacat.storage.model.namespace.Namespace
        elif serialized_dict.__contains__("tableVersionLocator"):
            return deltacat.storage.model.table_version.TableVersion
        elif serialized_dict.__contains__("partitionLocator"):
            return deltacat.storage.model.partition.Partition
        elif serialized_dict.__contains__("streamLocator"):
            return deltacat.storage.model.stream.Stream
        elif serialized_dict.__contains__("deltaLocator"):
            return deltacat.storage.model.delta.Delta
        else:
            raise ValueError(
                f"Could not find metafile class from serialized form: "
                f"${serialized_dict}"
            )

    @classmethod
    def read(
        cls,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        format: Optional[str] = METAFILE_FORMAT,
    ) -> Metafile:
        """
        Read a metadata file and return the deserialized object.
        :param path: Metadata file path to read.
        :param filesystem: File system to use for reading the metadata file.
        :param format: Format to use for deserializing the metadata file.
        :return: Deserialized object from the metadata file.
        """
        if format not in SUPPORTED_METAFILE_FORMATS:
            raise ValueError(
                f"Unsupported format '{format}'. Supported formats include: {SUPPORTED_METAFILE_FORMATS}."
            )

        if not filesystem:
            path, filesystem = resolve_path_and_filesystem(path, filesystem)
        with filesystem.open_input_stream(path) as file:
            binary = file.readall()
        reader = {
            "json": lambda b: json.loads(
                b.decode("utf-8"),
                object_hook=lambda obj: {
                    k: base64.b64decode(v)
                    if isinstance(v, str) and v.startswith("b64:")
                    else v
                    for k, v in obj.items()
                },
            ),
            "msgpack": msgpack.loads,
        }[format]
        data = reader(binary)
        # cast this Metafile into the appropriate child class type
        clazz = Metafile.get_class(data)
        obj = clazz(**data).from_serializable(path, filesystem)
        return obj

    def write_txn(
        self,
        catalog_root_dir: str,
        success_txn_log_dir: str,
        current_txn_op: deltacat.storage.model.transaction.TransactionOperation,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> None:
        """
        Serialize and write this object to a metadata file within the context
        of a transaction.
        :param catalog_root_dir: Catalog root dir to write the metafile to.
        :param success_txn_log_dir: Catalog root successful transaction log
        directory.
        :param current_txn_op: Transaction operation for this write.
        :param current_txn_start_time: Transaction start time for this write.
        :param current_txn_id: Transaction ID for this write.
        :param filesystem: File system to use for writing the metadata file. If
        not given, a default filesystem will be automatically selected based on
        the catalog root path.
        """
        if not filesystem:
            catalog_root_dir, filesystem = resolve_path_and_filesystem(
                path=catalog_root_dir,
                filesystem=filesystem,
            )
        self._write_metafile_revisions(
            catalog_root=catalog_root_dir,
            success_txn_log_dir=success_txn_log_dir,
            current_txn_op=current_txn_op,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )

    def write(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        format: Optional[str] = METAFILE_FORMAT,
    ) -> None:
        """
        Serialize and write this object to a metadata file.
        :param path: Metadata file path to write to.
        :param filesystem: File system to use for writing the metadata file. If
        not given, a default filesystem will be automatically selected based on
        the catalog root path.
        param: format: Format to use for serializing the metadata file.
        """
        if format not in SUPPORTED_METAFILE_FORMATS:
            raise ValueError(
                f"Unsupported format '{format}'. Supported formats include: {SUPPORTED_METAFILE_FORMATS}."
            )

        if not filesystem:
            path, filesystem = resolve_path_and_filesystem(path, filesystem)
        revision_dir_path = posixpath.dirname(path)
        filesystem.create_dir(revision_dir_path, recursive=True)

        writer = {
            "json": lambda data: json.dumps(
                data,
                indent=4,
                default=lambda b: base64.b64encode(b).decode("utf-8")
                if isinstance(b, bytes)
                else b,
            ).encode("utf-8"),
            "msgpack": msgpack.dumps,
        }[format]

        with filesystem.open_output_stream(path) as file:
            file.write(writer(self.to_serializable()))

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
        success_txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
    ) -> ListResult[Metafile]:
        """
        Retrieve all children of this object.
        :return: ListResult containing all children of this object.
        """
        catalog_root, filesystem = resolve_path_and_filesystem(
            catalog_root,
            filesystem,
        )
        metafile_root_dir_path = self.metafile_root_path(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        # List metafiles with respect to this metafile's URI as root
        return self._list_metafiles(
            success_txn_log_dir=success_txn_log_dir,
            metafile_root_dir_path=metafile_root_dir_path,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            limit=limit,
        )

    def siblings(
        self,
        catalog_root: str,
        success_txn_log_dir: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
    ) -> ListResult[Metafile]:
        """
        Retrieve all siblings of this object.
        :return: ListResult containing all siblings of this object.
        """
        catalog_root, filesystem = resolve_path_and_filesystem(
            catalog_root,
            filesystem,
        )
        parent_obj_path = self.parent_root_path(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        return self._list_metafiles(
            success_txn_log_dir=success_txn_log_dir,
            metafile_root_dir_path=parent_obj_path,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            limit=limit,
        )

    def revisions(
        self,
        catalog_root: str,
        success_txn_log_dir: str,
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
        catalog_root, filesystem = resolve_path_and_filesystem(
            catalog_root,
            filesystem,
        )
        try:
            parent_root = self.parent_root_path(
                catalog_root=catalog_root,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )
        except ValueError:
            # one or more ancestor's don't exist - return an empty list result
            # TODO(pdames): Raise and catch a more explicit AncestorNotFound
            #  error type here.
            return ListResult.empty()
        try:
            locator = (
                self.locator
                if self.locator.name.exists()
                else self.locator_alias
                if self.locator_alias and self.locator_alias.name.exists()
                else None
            )
            immutable_id = (
                # TODO(pdames): Refactor id lazy assignment into explicit getter/setter
                self.get("id")
                or Metafile._locator_to_id(
                    locator=locator,
                    catalog_root=catalog_root,
                    metafile_root=parent_root,
                    filesystem=filesystem,
                    txn_start_time=current_txn_start_time,
                    txn_id=current_txn_id,
                )
                if locator
                else None
            )
        except ValueError:
            # the metafile has been deleted
            return ListResult.empty()
        if not immutable_id:
            # the metafile does not exist
            return ListResult.empty()
        revision_dir_path = posixpath.join(
            parent_root,
            immutable_id,
            REVISION_DIR_NAME,
        )
        revisions = MetafileRevisionInfo.list_revisions(
            revision_dir_path=revision_dir_path,
            filesystem=filesystem,
            success_txn_log_dir=success_txn_log_dir,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            limit=limit,
        )
        items = []
        for mri in revisions:
            if mri.exists():
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

    def parent_root_path(
        self,
        catalog_root: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> str:
        ancestor_ids = self.ancestor_ids(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        return posixpath.join(*[catalog_root] + ancestor_ids)

    def metafile_root_path(
        self,
        catalog_root: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> str:
        parent_obj_path = self.parent_root_path(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        return posixpath.join(
            parent_obj_path,
            self.id,
        )

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
            ancestor_ids = Metafile._ancestor_ids(
                locator=self.locator,
                catalog_root=catalog_root,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )
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
    ) -> Optional[str]:
        """
        Resolves the immutable metafile ID for the given locator.

        :return: Immutable ID read from mapping file. None if no mapping exists.
        :raises: ValueError if the id is found but has been deleted
        """
        metafile_id = locator.name.immutable_id
        if not metafile_id:
            # the locator name is mutable, so we need to resolve the mapping
            # from the locator back to its immutable metafile ID
            locator_path = locator.path(metafile_root)
            success_txn_log_dir = posixpath.join(
                catalog_root,
                TXN_DIR_NAME,
                SUCCESS_TXN_DIR_NAME,
            )
            mri = MetafileRevisionInfo.latest_revision(
                revision_dir_path=locator_path,
                filesystem=filesystem,
                success_txn_log_dir=success_txn_log_dir,
                current_txn_start_time=txn_start_time,
                current_txn_id=txn_id,
                ignore_missing_revision=True,
            )
            if not mri.exists():
                return None
            if mri.txn_op_type == TransactionOperationType.DELETE:
                err_msg = (
                    f"Locator {locator} to metafile ID resolution failed "
                    f"because its metafile ID mapping was deleted. You may "
                    f"have an old reference to a renamed or deleted object."
                )
                raise ValueError(err_msg)
            metafile_id = posixpath.splitext(mri.path)[1][1:]
        return metafile_id

    @staticmethod
    def _ancestor_ids(
        locator: Locator,
        catalog_root: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> List[str]:
        ancestor_ids = []
        catalog_root, filesystem = resolve_path_and_filesystem(
            path=catalog_root,
            filesystem=filesystem,
        )
        parent_locators = []
        # TODO(pdames): Correctly resolve missing parents and K of N
        #  specified ancestors by using placeholder IDs for missing
        #  ancestors
        parent_locator = locator.parent
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
            if not ancestor_id:
                err_msg = f"Ancestor does not exist: {parent_locator}."
                raise ValueError(err_msg)
            metafile_root = posixpath.join(
                metafile_root,
                ancestor_id,
            )
            try:
                get_file_info(
                    path=metafile_root,
                    filesystem=filesystem,
                )
            except FileNotFoundError:
                raise ValueError(
                    f"Ancestor {parent_locator} does not exist at: " f"{metafile_root}"
                )
            ancestor_ids.append(ancestor_id)
        return ancestor_ids

    def _write_locator_to_id_map_file(
        self,
        locator: Locator,
        success_txn_log_dir: str,
        parent_obj_path: str,
        current_txn_op: deltacat.storage.model.transaction.TransactionOperation,
        current_txn_op_type: TransactionOperationType,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        name_resolution_dir_path = locator.path(parent_obj_path)
        # TODO(pdames): Don't write updated revisions with the same mapping as
        #  the latest revision.
        mri = MetafileRevisionInfo.new_revision(
            revision_dir_path=name_resolution_dir_path,
            current_txn_op_type=current_txn_op_type,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
            extension=f".{self.id}",
            success_txn_log_dir=success_txn_log_dir,
        )
        revision_file_path = mri.path
        filesystem.create_dir(posixpath.dirname(revision_file_path), recursive=True)
        with filesystem.open_output_stream(revision_file_path):
            pass  # Just create an empty ID file to map to the locator
        current_txn_op.append_locator_write_path(revision_file_path)

    def _write_metafile_revision(
        self,
        success_txn_log_dir: str,
        revision_dir_path: str,
        current_txn_op: deltacat.storage.model.transaction.TransactionOperation,
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
            success_txn_log_dir=success_txn_log_dir,
        )
        self.write(
            path=mri.path,
            filesystem=filesystem,
        )
        current_txn_op.append_metafile_write_path(mri.path)

    def _write_metafile_revisions(
        self,
        catalog_root: str,
        success_txn_log_dir: str,
        current_txn_op: deltacat.storage.model.transaction.TransactionOperation,
        current_txn_start_time: int,
        current_txn_id: str,
        filesystem: pyarrow.fs.FileSystem,
    ) -> None:
        """
        Generates the fully qualified paths required to write this metafile as
        part of the given transaction. All paths returned will be based in the
        given root directory.
        """
        parent_obj_path = self.parent_root_path(
            catalog_root=catalog_root,
            current_txn_start_time=current_txn_start_time,
            current_txn_id=current_txn_id,
            filesystem=filesystem,
        )
        mutable_src_locator = None
        mutable_dest_locator = None
        # metafiles without named immutable IDs have mutable name mappings
        if not self.named_immutable_id:
            mutable_src_locator = (
                current_txn_op.src_metafile.locator
                if current_txn_op.src_metafile
                else None
            )
            mutable_dest_locator = current_txn_op.dest_metafile.locator
        # metafiles with named immutable IDs may have aliases
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
                and mutable_src_locator is not None
                and mutable_src_locator != mutable_dest_locator
            ):
                # this update includes a rename
                # mark the source metafile mapping as deleted
                current_txn_op.src_metafile._write_locator_to_id_map_file(
                    locator=mutable_src_locator,
                    success_txn_log_dir=success_txn_log_dir,
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
                    success_txn_log_dir=success_txn_log_dir,
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
                    success_txn_log_dir=success_txn_log_dir,
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
                success_txn_log_dir=success_txn_log_dir,
                revision_dir_path=src_metafile_revision_dir_path,
                current_txn_op=current_txn_op,
                current_txn_op_type=TransactionOperationType.DELETE,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )
            try:
                # mark the dest metafile as created
                self._write_metafile_revision(
                    success_txn_log_dir=success_txn_log_dir,
                    revision_dir_path=metafile_revision_dir_path,
                    current_txn_op=current_txn_op,
                    current_txn_op_type=TransactionOperationType.CREATE,
                    current_txn_start_time=current_txn_start_time,
                    current_txn_id=current_txn_id,
                    filesystem=filesystem,
                )
            except ValueError as e:
                # TODO(pdames): raise/catch a DuplicateMetafileCreate exception.
                if "already exists" not in str(e):
                    raise e
                # src metafile is being replaced by an existing dest metafile

        else:
            self._write_metafile_revision(
                success_txn_log_dir=success_txn_log_dir,
                revision_dir_path=metafile_revision_dir_path,
                current_txn_op=current_txn_op,
                current_txn_op_type=current_txn_op.type,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                filesystem=filesystem,
            )

    def _list_metafiles(
        self,
        success_txn_log_dir: str,
        metafile_root_dir_path: str,
        current_txn_start_time: Optional[int] = None,
        current_txn_id: Optional[str] = None,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        limit: Optional[int] = None,
    ) -> ListResult[Metafile]:
        file_paths_and_sizes = list_directory(
            path=metafile_root_dir_path,
            filesystem=filesystem,
            ignore_missing_path=True,
        )
        # TODO(pdames): Exclude name resolution directories
        revision_dir_paths = [
            posixpath.join(file_path_and_size[0], REVISION_DIR_NAME)
            for file_path_and_size in file_paths_and_sizes
            if file_path_and_size[0] != success_txn_log_dir
        ]
        items = []
        for path in revision_dir_paths:
            mri = MetafileRevisionInfo.latest_revision(
                revision_dir_path=path,
                filesystem=filesystem,
                success_txn_log_dir=success_txn_log_dir,
                current_txn_start_time=current_txn_start_time,
                current_txn_id=current_txn_id,
                ignore_missing_revision=True,
            )
            if mri.exists():
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
