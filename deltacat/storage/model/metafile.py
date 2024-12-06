# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import msgpack

from deltacat.storage.model.list_result import ListResult
from deltacat.storage.model.locator import Locator


class Metafile(dict):
    """
    Base class for DeltaCAT metadata files, with read and write methods
    for dict-based DeltaCAT models. Uses msgpack (https://msgpack.org/) to
    serialize and deserialize metadata files.
    """

    @property
    def locator(self) -> Locator:
        """
        The locator of this object.
        :return: Locator of this object
        """
        raise NotImplementedError()

    def children(self) -> ListResult[Metafile]:
        """
        Retrieve all children of this object.
        :return: ListResult containing all children of this object.
        """
        raise NotImplementedError()

    def siblings(self) -> ListResult[Metafile]:
        """
        Retrieve all siblings of this object.
        :return: ListResult containing all siblings of this object.
        """
        raise NotImplementedError()

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

    def write(self, root: str, separator: str = "/") -> str:
        """
        Serialize and write this object to a metadata file.
        :param root: Root directory of the metadata file.
        :param separator: Separator to use in the metadata file path.
        :return: File path of the written metadata file.
        """
        path = self.locator.path(root, separator)
        with open(path, "wb") as file:
            packed = msgpack.dumps(self.to_serializable())
            file.write(packed)
        return path

    @classmethod
    def read(cls, path: str) -> Metafile:
        """
        Read a metadata file and return the deserialized object.
        :param path: Metadata file path to read.
        :return: Deserialized object from the metadata file.
        """
        with open(path, "rb") as file:
            bytes = file.read()
        return cls(**msgpack.loads(bytes)).from_serializable()
