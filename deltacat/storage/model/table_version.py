# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pyarrow as pa

import deltacat.storage.model.partition as partition

from deltacat.storage.model.metafile import Metafile
from deltacat.storage.model.schema import Schema, SchemaList
from deltacat.storage.model.locator import Locator
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import TableLocator
from deltacat.types.media import ContentType
from deltacat.storage.model.sort_key import SortScheme, SortSchemeList
from deltacat.storage.model.types import LifecycleState

TableVersionProperties = Dict[str, Any]


class TableVersion(Metafile):
    @staticmethod
    def of(
        locator: Optional[TableVersionLocator],
        schema: Optional[Schema],
        partition_scheme: Optional[partition.PartitionScheme] = None,
        description: Optional[str] = None,
        properties: Optional[TableVersionProperties] = None,
        content_types: Optional[List[ContentType]] = None,
        sort_scheme: Optional[SortScheme] = None,
        watermark: Optional[int] = None,
        lifecycle_state: Optional[LifecycleState] = None,
        schemas: Optional[SchemaList] = None,
        partition_schemes: Optional[partition.PartitionSchemeList] = None,
        sort_schemes: Optional[SortSchemeList] = None,
        native_object: Optional[Any] = None,
    ) -> TableVersion:
        table_version = TableVersion()
        table_version.locator = locator
        table_version.schema = schema
        table_version.partition_scheme = partition_scheme
        table_version.description = description
        table_version.properties = properties
        table_version.content_types = content_types
        table_version.sort_scheme = sort_scheme
        table_version.watermark = watermark
        table_version.lifecycle_state = lifecycle_state
        table_version.schemas = schemas
        table_version.partition_schemes = partition_schemes
        table_version.sort_schemes = sort_schemes
        table_version.native_object = native_object
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
    def schema(self) -> Optional[Schema]:
        return self.get("schema")

    @schema.setter
    def schema(self, schema: Optional[Schema]) -> None:
        self["schema"] = schema

    @property
    def schemas(self) -> Optional[SchemaList]:
        val: List[Schema] = self.get("schemas")
        if val is not None and not isinstance(val, SchemaList):
            self["schemas"] = val = SchemaList.of(val)
        return val

    @schemas.setter
    def schemas(self, schemas: Optional[SchemaList]) -> None:
        self["schemas"] = schemas

    @property
    def sort_scheme(self) -> Optional[SortScheme]:
        val: Dict[str, Any] = self.get("sortScheme")
        if val is not None and not isinstance(val, SortScheme):
            self["sortScheme"] = val = SortScheme(val)
        return val

    @sort_scheme.setter
    def sort_scheme(self, sort_scheme: Optional[SortScheme]) -> None:
        self["sortScheme"] = sort_scheme

    @property
    def sort_schemes(self) -> Optional[SortSchemeList]:
        val: Dict[str, Any] = self.get("sortSchemes")
        if val is not None and not isinstance(val, SortSchemeList):
            self["sortSchemes"] = val = SortSchemeList.of(val)
        return val

    @sort_schemes.setter
    def sort_schemes(self, sort_schemes: Optional[SortSchemeList]) -> None:
        self["sortSchemes"] = sort_schemes

    @property
    def watermark(self) -> Optional[int]:
        return self.get("watermark")

    @watermark.setter
    def watermark(self, watermark: Optional[int]) -> None:
        self["watermark"] = watermark

    @property
    def partition_scheme(self) -> Optional[partition.PartitionScheme]:
        val: Dict[str, Any] = self.get("partitionScheme")
        if val is not None and not isinstance(val, partition.PartitionScheme):
            self["partitionScheme"] = val = partition.PartitionScheme(val)
        return val

    @partition_scheme.setter
    def partition_scheme(
        self, partition_scheme: Optional[partition.PartitionScheme]
    ) -> None:
        self["partitionScheme"] = partition_scheme

    @property
    def partition_schemes(self) -> Optional[partition.PartitionSchemeList]:
        val: Dict[str, Any] = self.get("partitionSchemes")
        if val is not None and not isinstance(val, partition.PartitionSchemeList):
            self["partitionSchemes"] = val = partition.PartitionSchemeList.of(val)
        return val

    @partition_schemes.setter
    def partition_schemes(
        self, partition_schemes: Optional[partition.PartitionSchemeList]
    ) -> None:
        self["partitionSchemes"] = partition_schemes

    @property
    def description(self) -> Optional[str]:
        return self.get("description")

    @description.setter
    def description(self, description: Optional[str]) -> None:
        self["description"] = description

    @property
    def properties(self) -> Optional[TableVersionProperties]:
        return self.get("properties")

    @properties.setter
    def properties(self, properties: Optional[TableVersionProperties]) -> None:
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
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")

    @native_object.setter
    def native_object(self, native_object: Optional[Any]) -> None:
        self["nativeObject"] = native_object

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

    def to_serializable(self) -> TableVersion:
        self.schema = self.schema.serialize().to_pybytes() if self.schema else None
        self.schemas = (
            [_.serialize().to_pybytes() for _ in self.schemas] if self.schemas else None
        )
        return self

    def from_serializable(self) -> TableVersion:
        self.schema = (
            Schema.deserialize(pa.py_buffer(self.schema)) if self.schema else None
        )
        self.schemas = (
            [Schema.deserialize(pa.py_buffer(_)) for _ in self.schemas]
            if self.schemas
            else None
        )
        return self


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
