# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pyarrow as pa

from deltacat.storage.model.locator import Locator
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import TableLocator
from deltacat.types.media import ContentType
from deltacat.storage.model.sort_key import SortKey


class TableVersion(dict):
    @staticmethod
    def of(
        locator: Optional[TableVersionLocator],
        schema: Optional[Union[pa.Schema, str, bytes]],
        partition_keys: Optional[List[Dict[str, Any]]] = None,
        primary_key_columns: Optional[List[str]] = None,
        description: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        content_types: Optional[List[ContentType]] = None,
        sort_keys: Optional[List[SortKey]] = None,
    ) -> TableVersion:
        table_version = TableVersion()
        table_version.locator = locator
        table_version.schema = schema
        table_version.partition_keys = partition_keys
        table_version.primary_keys = primary_key_columns
        table_version.description = description
        table_version.properties = properties
        table_version.content_types = content_types
        table_version.sort_keys = sort_keys
        return table_version

    @property
    def locator(self) -> Optional[TableVersionLocator]:
        val: Dict[str, Any] = self.get("tableVersionLocator")
        if val is not None and not isinstance(val, TableVersionLocator):
            self.locator = val = TableVersionLocator(val)
        return val

    @locator.setter
    def locator(self, table_version_locator: Optional[TableVersionLocator]) -> None:
        self["tableVersionLocator"] = table_version_locator

    @property
    def schema(self) -> Optional[Union[pa.Schema, str, bytes]]:
        return self.get("schema")

    @schema.setter
    def schema(self, schema: Optional[Union[pa.Schema, str, bytes]]) -> None:
        self["schema"] = schema

    @property
    def sort_keys(self) -> Optional[List[SortKey]]:
        return self.get("sortKeys")

    @sort_keys.setter
    def sort_keys(self, sort_keys: Optional[List[SortKey]]) -> None:
        self["sortKeys"] = sort_keys

    @property
    def partition_keys(self) -> Optional[List[Dict[str, Any]]]:
        return self.get("partitionKeys")

    @partition_keys.setter
    def partition_keys(self, partition_keys: Optional[List[Dict[str, Any]]]) -> None:
        self["partitionKeys"] = partition_keys

    @property
    def primary_keys(self) -> Optional[List[str]]:
        return self.get("primaryKeys")

    @primary_keys.setter
    def primary_keys(self, primary_keys: Optional[List[str]]) -> None:
        self["primaryKeys"] = primary_keys

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
    def content_types(self) -> Optional[List[ContentType]]:
        content_types = self.get("contentTypes")
        return (
            None
            if content_types is None
            else [None if _ is None else ContentType(_) for _ in content_types]
        )

    @content_types.setter
    def content_types(self, content_types: Optional[List[ContentType]]) -> None:
        self["contentTypes"] = content_types

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        table_version_locator = self.locator
        if table_version_locator:
            return table_version_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        table_version_locator = self.locator
        if table_version_locator:
            return table_version_locator.table_locator
        return None

    @property
    def namespace(self) -> Optional[str]:
        table_version_locator = self.locator
        if table_version_locator:
            return table_version_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        table_version_locator = self.locator
        if table_version_locator:
            return table_version_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        table_version_locator = self.locator
        if table_version_locator:
            return table_version_locator.table_version
        return None

    def is_supported_content_type(self, content_type: ContentType):
        supported_content_types = self.content_types
        return (not supported_content_types) or (
            content_type in supported_content_types
        )


class TableVersionLocator(Locator, dict):
    @staticmethod
    def of(
        table_locator: Optional[TableLocator], table_version: Optional[str]
    ) -> TableVersionLocator:
        table_version_locator = TableVersionLocator()
        table_version_locator.table_locator = table_locator
        table_version_locator.table_version = table_version
        return table_version_locator

    @staticmethod
    def at(
        namespace: Optional[str],
        table_name: Optional[str],
        table_version: Optional[str],
    ) -> TableVersionLocator:
        table_locator = TableLocator.at(namespace, table_name)
        return TableVersionLocator.of(table_locator, table_version)

    @property
    def table_locator(self) -> Optional[TableLocator]:
        val: Dict[str, Any] = self.get("tableLocator")
        if val is not None and not isinstance(val, TableLocator):
            self.table_locator = val = TableLocator(val)
        return val

    @table_locator.setter
    def table_locator(self, table_locator: Optional[TableLocator]) -> None:
        self["tableLocator"] = table_locator

    @property
    def table_version(self) -> Optional[str]:
        return self.get("tableVersion")

    @table_version.setter
    def table_version(self, table_version: Optional[str]) -> None:
        self["tableVersion"] = table_version

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

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        tl_hexdigest = self.table_locator.hexdigest()
        table_version = self.table_version
        return f"{tl_hexdigest}|{table_version}"
