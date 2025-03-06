# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import base64
import posixpath

import pyarrow
import pyarrow as pa

from typing import Any, Dict, List, Optional

from deltacat.storage.model.metafile import Metafile, MetafileRevisionInfo
from deltacat.constants import METAFILE_FORMAT, METAFILE_FORMAT_JSON, TXN_DIR_NAME
from deltacat.storage.model.schema import (
    FieldLocator,
    Schema,
)
from deltacat.storage.model.locator import (
    Locator,
    LocatorName,
)
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.stream import StreamLocator
from deltacat.storage.model.table import (
    TableLocator,
    Table,
)
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.transform import Transform
from deltacat.storage.model.types import (
    CommitState,
    StreamFormat,
)
from deltacat.types.media import ContentType


"""
An ordered list of partition values. Partition values are typically derived
by applying one or more transforms to a table's fields.
"""
PartitionValues = List[Any]
UNPARTITIONED_SCHEME_ID = "deadbeef-7277-49a4-a195-fdc8ed235d42"


class Partition(Metafile):
    @staticmethod
    def of(
        locator: Optional[PartitionLocator],
        schema: Optional[Schema],
        content_types: Optional[List[ContentType]],
        state: Optional[CommitState] = None,
        previous_stream_position: Optional[int] = None,
        previous_partition_id: Optional[str] = None,
        stream_position: Optional[int] = None,
        partition_scheme_id: Optional[str] = None,
    ) -> Partition:
        partition = Partition()
        partition.locator = locator
        partition.schema = schema
        partition.content_types = content_types
        partition.state = state
        partition.previous_stream_position = previous_stream_position
        partition.previous_partition_id = previous_partition_id
        partition.stream_position = stream_position
        partition.partition_scheme_id = (
            partition_scheme_id if locator.partition_values else UNPARTITIONED_SCHEME_ID
        )
        return partition

    @property
    def locator(self) -> Optional[PartitionLocator]:
        val: Dict[str, Any] = self.get("partitionLocator")
        if val is not None and not isinstance(val, PartitionLocator):
            self.locator = val = PartitionLocator(val)
        return val

    @locator.setter
    def locator(self, partition_locator: Optional[PartitionLocator]) -> None:
        self["partitionLocator"] = partition_locator

    @property
    def locator_alias(self) -> Optional[PartitionLocatorAlias]:
        return PartitionLocatorAlias.of(self)

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
    def content_types(self) -> Optional[List[ContentType]]:
        content_types = self.get("contentTypes")
        return (
            None
            if content_types is None
            else [None if _ is None else ContentType(_) for _ in content_types]
        )

    @content_types.setter
    def content_types(
        self, supported_content_types: Optional[List[ContentType]]
    ) -> None:
        self["contentTypes"] = supported_content_types

    @property
    def state(self) -> Optional[CommitState]:
        state = self.get("state")
        return None if state is None else CommitState(state)

    @state.setter
    def state(self, state: Optional[CommitState]) -> None:
        self["state"] = state

    @property
    def previous_stream_position(self) -> Optional[int]:
        return self.get("previousStreamPosition")

    @previous_stream_position.setter
    def previous_stream_position(self, previous_stream_position: Optional[int]) -> None:
        self["previousStreamPosition"] = previous_stream_position

    @property
    def previous_partition_id(self) -> Optional[str]:
        return self.get("previousPartitionId")

    @previous_partition_id.setter
    def previous_partition_id(self, previous_partition_id: Optional[str]) -> None:
        self["previousPartitionId"] = previous_partition_id

    @property
    def stream_position(self) -> Optional[int]:
        return self.get("streamPosition")

    @stream_position.setter
    def stream_position(self, stream_position: Optional[int]):
        self["streamPosition"] = stream_position

    @property
    def partition_scheme_id(self) -> Optional[str]:
        return self.get("partitionSchemeId")

    @partition_scheme_id.setter
    def partition_scheme_id(self, partition_scheme_id: Optional[str]) -> None:
        self["partitionSchemeId"] = partition_scheme_id

    @property
    def partition_id(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.partition_id
        return None

    @property
    def stream_id(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.stream_id
        return None

    @property
    def stream_format(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.stream_format
        return None

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.partition_values

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.table_locator
        return None

    @property
    def table_version_locator(self) -> Optional[TableVersionLocator]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.table_version_locator
        return None

    @property
    def stream_locator(self) -> Optional[StreamLocator]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.stream_locator
        return None

    @property
    def storage_type(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.stream_format
        return None

    @property
    def namespace(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        partition_locator = self.locator
        if partition_locator:
            return partition_locator.table_version
        return None

    def is_supported_content_type(self, content_type: ContentType) -> bool:
        supported_content_types = self.content_types
        return (not supported_content_types) or (
            content_type in supported_content_types
        )

    def to_serializable(self) -> Partition:
        serializable: Partition = Partition.update_for(self)
        if serializable.schema:
            schema_bytes = serializable.schema.serialize().to_pybytes()
            serializable.schema = (
                base64.b64encode(schema_bytes).decode("utf-8")
                if METAFILE_FORMAT == METAFILE_FORMAT_JSON
                else schema_bytes
            )

        if serializable.table_locator:
            # replace the mutable table locator
            serializable.table_version_locator.table_locator = TableLocator.at(
                namespace=self.id,
                table_name=self.id,
            )
        return serializable

    def from_serializable(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Partition:
        if self.get("schema"):
            schema_data = self["schema"]
            schema_bytes = (
                base64.b64decode(schema_data)
                if METAFILE_FORMAT == METAFILE_FORMAT_JSON
                else schema_data
            )
            self["schema"] = Schema.deserialize(pa.py_buffer(schema_bytes))
        else:
            self["schema"] = None

        # restore the table locator from its mapped immutable metafile ID
        if self.table_locator and self.table_locator.table_name == self.id:
            parent_rev_dir_path = Metafile._parent_metafile_rev_dir_path(
                base_metafile_path=path,
                parent_number=3,
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
            self.table_version_locator.table_locator = table.locator
        return self


class PartitionLocatorName(LocatorName):
    def __init__(self, locator: PartitionLocator):
        self.locator = locator

    @property
    def immutable_id(self) -> Optional[str]:
        return self.locator.partition_id

    @immutable_id.setter
    def immutable_id(self, immutable_id: Optional[str]):
        self.locator.partition_id = immutable_id

    def parts(self) -> List[str]:
        return [
            str(self.locator.partition_values),
            self.locator.partition_id,
        ]


class PartitionLocator(Locator, dict):
    @staticmethod
    def of(
        stream_locator: Optional[StreamLocator],
        partition_values: Optional[PartitionValues],
        partition_id: Optional[str],
    ) -> PartitionLocator:
        """
        Creates a stream partition locator. Partition ID is
        case-sensitive.

        Partition Value types must ensure that
        `str(partition_value1) == str(partition_value2)` always implies
        `partition_value1 == partition_value2` (i.e. if two string
        representations of partition values are equal, then the two partition
        values are equal).
        """
        partition_locator = PartitionLocator()
        partition_locator.stream_locator = stream_locator
        partition_locator.partition_values = partition_values
        partition_locator.partition_id = partition_id
        return partition_locator

    @staticmethod
    def at(
        namespace: Optional[str],
        table_name: Optional[str],
        table_version: Optional[str],
        stream_id: Optional[str],
        stream_format: Optional[StreamFormat],
        partition_values: Optional[PartitionValues],
        partition_id: Optional[str],
    ) -> PartitionLocator:
        stream_locator = (
            StreamLocator.at(
                namespace,
                table_name,
                table_version,
                stream_id,
                stream_format,
            )
            if stream_id and stream_format
            else None
        )
        return PartitionLocator.of(
            stream_locator,
            partition_values,
            partition_id,
        )

    @property
    def name(self) -> PartitionLocatorName:
        return PartitionLocatorName(self)

    @property
    def parent(self) -> Optional[StreamLocator]:
        return self.stream_locator

    @property
    def stream_locator(self) -> Optional[StreamLocator]:
        val: Dict[str, Any] = self.get("streamLocator")
        if val is not None and not isinstance(val, StreamLocator):
            self.stream_locator = val = StreamLocator(val)
        return val

    @stream_locator.setter
    def stream_locator(self, stream_locator: Optional[StreamLocator]) -> None:
        self["streamLocator"] = stream_locator

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        return self.get("partitionValues")

    @partition_values.setter
    def partition_values(self, partition_values: Optional[PartitionValues]) -> None:
        self["partitionValues"] = partition_values

    @property
    def partition_id(self) -> Optional[str]:
        return self.get("partitionId")

    @partition_id.setter
    def partition_id(self, partition_id: Optional[str]) -> None:
        self["partitionId"] = partition_id

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.table_locator
        return None

    @property
    def table_version_locator(self) -> Optional[TableVersionLocator]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.table_version_locator
        return None

    @property
    def stream_id(self) -> Optional[str]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.stream_id
        return None

    @property
    def stream_format(self) -> Optional[str]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.format
        return None

    @property
    def namespace(self) -> Optional[str]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        stream_locator = self.stream_locator
        if stream_locator:
            return stream_locator.table_version
        return None


class PartitionKey(dict):
    @staticmethod
    def of(
        key: List[FieldLocator],
        name: Optional[str] = None,
        field_id: Optional[int] = None,
        transform: Optional[Transform] = None,
        native_object: Optional[Any] = None,
    ) -> PartitionKey:
        return PartitionKey(
            {
                "key": key,
                "name": name,
                "fieldId": field_id,
                "transform": transform,
                "nativeObject": native_object,
            }
        )

    def equivalent_to(
        self,
        other: PartitionKey,
        check_identifiers: False,
    ):
        if other is None:
            return False
        if not isinstance(other, dict):
            return False
        if not isinstance(other, PartitionKey):
            other = PartitionKey(other)
        return (
            self.key == other.key
            and self.transform == other.transform
            and not check_identifiers
            or (self.name == other.name and self.id == other.id)
        )

    @property
    def key(self) -> List[FieldLocator]:
        return self.get("key")

    @property
    def name(self) -> Optional[str]:
        return self.get("name")

    @property
    def id(self) -> Optional[int]:
        return self.get("fieldId")

    @property
    def transform(self) -> Optional[Transform]:
        val: Dict[str, Any] = self.get("transform")
        if val is not None and not isinstance(val, Transform):
            self["transform"] = val = Transform(val)
        return val

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")


class PartitionKeyList(List[PartitionKey]):
    @staticmethod
    def of(items: List[PartitionKey]) -> PartitionKeyList:
        typed_items = PartitionKeyList()
        for item in items:
            if item is not None and not isinstance(item, PartitionKey):
                item = PartitionKey(item)
            typed_items.append(item)
        return typed_items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, PartitionKey):
            self[item] = val = PartitionKey(val)
        return val


class PartitionScheme(dict):
    @staticmethod
    def of(
        keys: Optional[PartitionKeyList],
        name: Optional[str] = None,
        scheme_id: Optional[str] = None,
        native_object: Optional[Any] = None,
    ) -> PartitionScheme:
        return PartitionScheme(
            {
                "keys": keys,
                "name": name,
                "id": scheme_id,
                "nativeObject": native_object,
            }
        )

    def equivalent_to(
        self,
        other: PartitionScheme,
        check_identifiers: bool = False,
    ) -> bool:
        if other is None:
            return False
        if not isinstance(other, dict):
            return False
        if not isinstance(other, PartitionScheme):
            other = PartitionScheme(other)
        for i in range(len(self.keys)):
            if not self.keys[i].equivalent_to(other.keys[i], check_identifiers):
                return False
        return not check_identifiers or (
            self.name == other.name and self.id == other.id
        )

    @property
    def keys(self) -> Optional[PartitionKeyList]:
        val: List[PartitionKey] = self.get("keys")
        if val is not None and not isinstance(val, PartitionKeyList):
            self["keys"] = val = PartitionKeyList.of(val)
        return val

    @property
    def name(self) -> Optional[str]:
        return self.get("name")

    @property
    def id(self) -> Optional[str]:
        return self.get("id")

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")


class PartitionSchemeList(List[PartitionScheme]):
    @staticmethod
    def of(items: List[PartitionScheme]) -> PartitionSchemeList:
        typed_items = PartitionSchemeList()
        for item in items:
            if item is not None and not isinstance(item, PartitionScheme):
                item = PartitionScheme(item)
            typed_items.append(item)
        return typed_items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, PartitionScheme):
            self[item] = val = PartitionScheme(val)
        return val


class PartitionLocatorAliasName(LocatorName):
    def __init__(self, locator: PartitionLocatorAlias):
        self.locator = locator

    @property
    def immutable_id(self) -> Optional[str]:
        return None

    def parts(self) -> List[str]:
        return [
            str(self.locator.partition_values),
            self.locator.partition_scheme_id,
        ]


class PartitionLocatorAlias(Locator, dict):
    @staticmethod
    def of(parent_partition: Partition):
        return (
            PartitionLocatorAlias(
                {
                    "partition_values": parent_partition.partition_values,
                    "partition_scheme_id": parent_partition.partition_scheme_id,
                    "parent": (
                        parent_partition.locator.parent
                        if parent_partition.locator
                        else None
                    ),
                }
            )
            if parent_partition.state == CommitState.COMMITTED
            else None  # only committed partitions can be resolved by alias
        )

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        return self.get("partition_values")

    @property
    def partition_scheme_id(self) -> Optional[str]:
        return self.get("partition_scheme_id")

    @property
    def name(self) -> PartitionLocatorAliasName:
        return PartitionLocatorAliasName(self)

    @property
    def parent(self) -> Optional[Locator]:
        return self.get("parent")
