# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import copy
import posixpath
from typing import Any, Dict, Optional, List

import pyarrow

from deltacat.storage.model.locator import Locator, LocatorName
from deltacat.storage.model.namespace import (
    NamespaceLocator,
    Namespace,
)
from deltacat.storage.model.metafile import Metafile, MetafileCommitInfo, TXN_DIR_NAME

TableProperties = Dict[str, Any]


class Table(Metafile):
    """
    Tables store properties common to every table version including the
    table's name, a high-level description of all table versions, and
    properties shared by all table versions.
    """

    @staticmethod
    def of(
        locator: Optional[TableLocator],
        description: Optional[str] = None,
        properties: Optional[TableProperties] = None,
        native_object: Optional[Any] = None,
    ) -> Table:
        table = Table()
        table.locator = locator
        table.description = description
        table.properties = properties
        table.native_object = native_object
        return table

    @property
    def locator(self) -> Optional[TableLocator]:
        val: Dict[str, Any] = self.get("tableLocator")
        if val is not None and not isinstance(val, TableLocator):
            self.locator = val = TableLocator(val)
        return val

    @locator.setter
    def locator(self, table_locator: Optional[TableLocator]) -> None:
        self["tableLocator"] = table_locator

    @property
    def description(self) -> Optional[str]:
        return self.get("description")

    @description.setter
    def description(self, description: Optional[str]) -> None:
        self["description"] = description

    @property
    def properties(self) -> Optional[TableProperties]:
        return self.get("properties")

    @properties.setter
    def properties(self, properties: Optional[TableProperties]) -> None:
        self["properties"] = properties

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")

    @native_object.setter
    def native_object(self, native_object: Optional[Any]) -> None:
        self["nativeObject"] = native_object

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        table_locator = self.locator
        if table_locator:
            return table_locator.namespace_locator
        return None

    @property
    def namespace(self) -> Optional[str]:
        table_locator = self.locator
        if table_locator:
            return table_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        table_locator = self.locator
        if table_locator:
            return table_locator.table_name
        return None

    def to_serializable(self) -> Table:
        serializable = self
        if serializable.namespace_locator:
            serializable = Table(copy.deepcopy(self))
            # remove the mutable namespace locator
            serializable.locator.namespace_locator = NamespaceLocator.of(self.id)
        return serializable

    def from_serializable(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Table:
        # restore the namespace locator from its mapped immutable metafile ID
        if self.namespace_locator and self.namespace_locator.namespace == self.id:
            parent_rev_dir_path = Metafile._parent_metafile_rev_dir_path(
                base_metafile_path=path,
                parent_number=1,
            )
            txn_log_dir = posixpath.join(
                posixpath.dirname(
                    posixpath.dirname(parent_rev_dir_path),
                ),
                TXN_DIR_NAME,
            )
            namespace = Namespace.read(
                MetafileCommitInfo.current(
                    commit_dir_path=parent_rev_dir_path,
                    filesystem=filesystem,
                    txn_log_dir=txn_log_dir,
                ).path,
                filesystem,
            )
            self.locator.namespace_locator = namespace.locator
        return self


class TableLocatorName(LocatorName):
    def __init__(self, locator: TableLocator):
        self.locator = locator

    def immutable_id(self) -> Optional[str]:
        return None

    def parts(self) -> List[str]:
        return [self.locator.table_name]


class TableLocator(Locator, dict):
    @staticmethod
    def of(
        namespace_locator: Optional[NamespaceLocator], table_name: Optional[str]
    ) -> TableLocator:
        table_locator = TableLocator()
        table_locator.namespace_locator = namespace_locator
        table_locator.table_name = table_name
        return table_locator

    @staticmethod
    def at(namespace: Optional[str], table_name: Optional[str]) -> TableLocator:
        namespace_locator = NamespaceLocator.of(namespace) if namespace else None
        return TableLocator.of(namespace_locator, table_name)

    def name(self) -> TableLocatorName:
        return TableLocatorName(self)

    def parent(self) -> Optional[NamespaceLocator]:
        return self.namespace_locator

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        val: Dict[str, Any] = self.get("namespaceLocator")
        if val is not None and not isinstance(val, NamespaceLocator):
            self.namespace_locator = val = NamespaceLocator(val)
        return val

    @namespace_locator.setter
    def namespace_locator(self, namespace_locator: Optional[NamespaceLocator]) -> None:
        self["namespaceLocator"] = namespace_locator

    @property
    def table_name(self) -> Optional[str]:
        return self.get("tableName")

    @table_name.setter
    def table_name(self, table_name: Optional[str]) -> None:
        self["tableName"] = table_name

    @property
    def namespace(self) -> Optional[str]:
        namespace_locator = self.namespace_locator
        if namespace_locator:
            return namespace_locator.namespace
        return None
