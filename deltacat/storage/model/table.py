# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, Optional

from deltacat.storage.model.locator import Locator
from deltacat.storage.model.namespace import NamespaceLocator

TableProperties = Dict[str, Any]


class Table(dict):
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
        namespace_locator = NamespaceLocator.of(namespace)
        return TableLocator.of(namespace_locator, table_name)

    @property
    def namespace_locator(self) -> NamespaceLocator:
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

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        nl_hexdigest = self.namespace_locator.hexdigest()
        table_name = self.table_name
        return f"{nl_hexdigest}|{table_name}"
