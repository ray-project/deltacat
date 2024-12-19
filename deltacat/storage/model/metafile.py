# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Tuple

import msgpack
import pyarrow.fs
import os
import uuid

from deltacat.storage.model.list_result import ListResult


class Metafile(dict):
    """
    Base class for DeltaCAT metadata files, with read and write methods
    for dict-based DeltaCAT models. Uses msgpack (https://msgpack.org/) to
    serialize and deserialize metadata files.
    """

    @staticmethod
    def parse_uuid_from_file_path(file_path: str) -> str:
        """
        Extract and verify UUID from a file path's base name, raising exception
        if invalid.

        :param file_path: Full path to file containing a UUID in the base name.
        :return: File base name UUID string.
        :raises: ValueError: If no valid UUID was found in the file path.
        """

        try:
            # get the base name without extension
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            # try to parse the UUID
            return str(uuid.UUID(base_name))
        except ValueError:
            raise ValueError(f"No valid UUID found in file name: {file_path}")

    @property
    def uuid(self) -> Optional[str]:
        """
        Returns an immutable UUID for the given metafile that can be used for
        equality checks (i.e. 2 metafiles are equal if they have the same UUID)
        and deterministic references (e.g. for generating a table file path that
        remains the same regardless of renames).
        """
        return self.get("uuid")

    @uuid.setter
    def uuid(self, uuid: Optional[str]) -> None:
        self["uuid"] = uuid

    def path(
        self,
        root: str,
        separator: str = "/",
        extension: str = "mpk",
    ) -> str:
        """
        Returns a path for the locator of the form:
        "{root}{seperator}{uuid}.{extension}", where the default path separator
        of "/" may optionally be overridden with any string.
        """
        return f"{root}{separator}{self.uuid}.{extension}"

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
        if not self.uuid:
            self.uuid = str(uuid.uuid4())
        path = self.path(root, separator)
        path, fs = Metafile.file_system(path, filesystem)
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
        obj.uuid = Metafile.parse_uuid_from_file_path(path)
        return obj
