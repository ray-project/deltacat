# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import NamespaceLocator, Locator

from typing import Any, Dict, Optional


class Table(dict):
    @staticmethod
    def of(table_locator: Optional[TableLocator],
           table_permissions: Optional[Dict[str, Any]] = None,
           table_description: Optional[str] = None,
           table_properties: Optional[Dict[str, str]] = None) -> Table:
        return Table({
            "tableLocator": table_locator,
            "permissions": table_permissions,
            "description": table_description,
            "properties": table_properties,
        })

    @property
    def table_locator(self) -> Optional[TableLocator]:
        return self.get("tableLocator")

    @table_locator.setter
    def table_locator(self, table_locator: Optional[TableLocator]) -> None:
        self["tableLocator"] = table_locator

    @property
    def permissions(self) -> Optional[Dict[str, Any]]:
        return self.get("permissions")

    @permissions.setter
    def permissions(self, permissions: Optional[Dict[str, Any]]) -> None:
        self["permissions"] = permissions

    @property
    def description(self) -> Optional[str]:
        return self.get("description")

    @description.setter
    def description(self, description: Optional[str]) -> None:
        self["description"] = description

    @property
    def properties(self) -> Optional[Dict[str, str]]:
        return self.get("properties")

    @properties.setter
    def properties(self, properties: Optional[Dict[str, str]]) -> None:
        self["properties"] = properties

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        table_locator = self.table_locator
        if table_locator:
            return table_locator.namespace_locator
        return None

    @property
    def namespace(self) -> Optional[str]:
        table_locator = self.table_locator
        if table_locator:
            return table_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        table_locator = self.table_locator
        if table_locator:
            return table_locator.table_name
        return None


class TableLocator(Locator, dict):
    @staticmethod
    def of(namespace_locator: Optional[NamespaceLocator],
           table_name: Optional[str]) -> TableLocator:
        return TableLocator({
            "namespaceLocator": namespace_locator,
            "tableName": table_name
        })

    @property
    def namespace_locator(self) -> NamespaceLocator:
        return self.get("namespaceLocator")

    @namespace_locator.setter
    def namespace_locator(
            self,
            namespace_locator: Optional[NamespaceLocator]) -> None:
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
