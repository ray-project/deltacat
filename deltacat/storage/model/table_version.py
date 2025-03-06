# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import base64
import re
import posixpath
from typing import Any, Dict, List, Optional, Tuple

import pyarrow
import pyarrow as pa

import deltacat.storage.model.partition as partition

from deltacat.storage.model.metafile import Metafile, MetafileRevisionInfo
from deltacat.constants import (
    METAFILE_FORMAT,
    METAFILE_FORMAT_JSON,
    TXN_DIR_NAME,
    BYTES_PER_KIBIBYTE,
)
from deltacat.storage.model.schema import (
    Schema,
    SchemaList,
)
from deltacat.storage.model.locator import (
    Locator,
    LocatorName,
)
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import (
    TableLocator,
    Table,
)
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
        previous_table_version: Optional[str] = None,
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
        table_version.state = lifecycle_state
        table_version.schemas = schemas
        table_version.partition_schemes = partition_schemes
        table_version.sort_schemes = sort_schemes
        table_version.previous_table_version = previous_table_version
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
        val: Dict[str, Any] = self.get("schema")
        if val is not None and not isinstance(val, Schema):
            self.schema = val = Schema(val)
        return val

    @schema.setter
    def schema(self, schema: Optional[Schema]) -> None:
        self["schema"] = schema

    @property
    def schemas(self) -> Optional[SchemaList]:
        val: Optional[SchemaList] = self.get("schemas")
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
    def state(self) -> Optional[LifecycleState]:
        state = self.get("state")
        return None if state is None else LifecycleState(state)

    @state.setter
    def state(self, state: Optional[LifecycleState]) -> None:
        self["state"] = state

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
    def previous_table_version(self) -> Optional[str]:
        return self.get("previous_table_version")

    @previous_table_version.setter
    def previous_table_version(self, previous_table_version: Optional[str]) -> None:
        self["previous_table_version"] = previous_table_version

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
        serializable: TableVersion = TableVersion.update_for(self)
        if serializable.schema:
            schema_bytes = serializable.schema.serialize().to_pybytes()
            serializable.schema = (
                base64.b64encode(schema_bytes).decode("utf-8")
                if METAFILE_FORMAT == METAFILE_FORMAT_JSON
                else schema_bytes
            )

        if serializable.schemas:
            serializable.schemas = [
                base64.b64encode(schema.serialize().to_pybytes()).decode("utf-8")
                if METAFILE_FORMAT == METAFILE_FORMAT_JSON
                else schema.serialize().to_pybytes()
                for schema in serializable.schemas
            ]
        if serializable.table_locator:
            # remove the mutable table locator
            serializable.locator.table_locator = TableLocator.at(
                namespace=self.id,
                table_name=self.id,
            )
        return serializable

    def from_serializable(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> TableVersion:
        if self.get("schema"):
            schema_data = self["schema"]
            schema_bytes = (
                base64.b64decode(schema_data)
                if METAFILE_FORMAT == "json"
                else schema_data
            )
            self["schema"] = Schema.deserialize(pa.py_buffer(schema_bytes))
        else:
            self["schema"] = None

        if self.get("schemas"):
            self.schemas = [
                Schema.deserialize(
                    pa.py_buffer(
                        base64.b64decode(schema)
                        if METAFILE_FORMAT == METAFILE_FORMAT_JSON
                        else schema
                    )
                )
                for schema in self["schemas"]
            ]
        else:
            self.schemas = None

        if self.sort_scheme:
            # force list-to-tuple conversion of sort keys via property invocation
            self.sort_scheme.keys
            [sort_scheme.keys for sort_scheme in self.sort_schemes]
        # restore the table locator from its mapped immutable metafile ID
        if self.table_locator and self.table_locator.table_name == self.id:
            parent_rev_dir_path = Metafile._parent_metafile_rev_dir_path(
                base_metafile_path=path,
                parent_number=1,
            )
            txn_log_dir = posixpath.join(
                posixpath.dirname(
                    posixpath.dirname(
                        posixpath.dirname(parent_rev_dir_path),
                    )
                ),
                TXN_DIR_NAME,
            )
            table = Table.read(
                MetafileRevisionInfo.latest_revision(
                    revision_dir_path=parent_rev_dir_path,
                    filesystem=filesystem,
                    success_txn_log_dir=txn_log_dir,
                ).path,
                filesystem,
            )
            self.locator.table_locator = table.locator
        return self

    def current_version_number(self) -> Optional[int]:
        """
        Returns the current table version number as an integer, or None if
        a table version has not yet been assigned.
        """
        prefix, version_number = (
            TableVersion.parse_table_version(
                self.table_version,
            )
            if self.table_version is not None
            else (None, None)
        )
        return int(version_number) if version_number is not None else None

    @staticmethod
    def next_version(previous_version: Optional[str] = None) -> str:
        """
        Assigns the next table version string given the previous table version
        by incrementing the version number of the given previous table version
        identifier. Returns "1" if the previous version is undefined.
        """
        prefix, previous_version_number = (
            TableVersion.parse_table_version(
                previous_version,
            )
            if previous_version is not None
            else (None, None)
        )
        new_version_number = (
            int(previous_version_number) + 1
            if previous_version_number is not None
            else 1
        )
        new_prefix = prefix if prefix is not None else ""
        return f"{new_prefix}{new_version_number}"

    @staticmethod
    def parse_table_version(table_version: str) -> Tuple[Optional[str], int]:
        """
        Parses a table version string into its prefix and version number.
        Returns a tuple of the prefix and version number.
        """
        if not table_version:
            raise ValueError(f"Table version to parse is undefined.")
        if len(table_version) > BYTES_PER_KIBIBYTE:
            raise ValueError(
                f"Invalid table version {table_version}. Table version "
                f"identifier cannot be greater than {BYTES_PER_KIBIBYTE} "
                f"characters."
            )
        version_match = re.match(
            rf"^(\w*\.)?(\d+)$",
            table_version,
        )
        if version_match:
            prefix, version_number = version_match.groups()
            return prefix, int(version_number)
        raise ValueError(
            f"Invalid table version {table_version}. Valid table versions "
            f"are of the form `TableVersionName.1` or simply `1`.",
        )


class TableVersionLocatorName(LocatorName):
    def __init__(self, locator: TableVersionLocator):
        self.locator = locator

    @property
    def immutable_id(self) -> Optional[str]:
        return self.locator.table_version

    @immutable_id.setter
    def immutable_id(self, immutable_id: Optional[str]):
        self.locator.table_version = immutable_id

    def parts(self) -> List[str]:
        return [self.locator.table_version]


class TableVersionLocator(Locator, dict):
    @staticmethod
    def of(
        table_locator: Optional[TableLocator],
        table_version: Optional[str],
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
        table_locator = TableLocator.at(namespace, table_name) if table_name else None
        return TableVersionLocator.of(table_locator, table_version)

    @property
    def name(self):
        return TableVersionLocatorName(self)

    @property
    def parent(self) -> Optional[TableLocator]:
        return self.table_locator

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
        # ensure that the table version is valid
        prefix, version_number = TableVersion.parse_table_version(table_version)
        # restate the table version number in its canonical form
        # (e.g., ensure that "MyVersion.0001" is saved as "MyVersion.1")
        self["tableVersion"] = (
            f"{prefix}{version_number}" if prefix else str(version_number)
        )

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
