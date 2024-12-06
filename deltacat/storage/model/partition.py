# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, List, Optional

from deltacat.storage.model.metafile import Metafile
from deltacat.storage.model.schema import (
    FieldLocator,
    Schema,
)
from deltacat.storage.model.locator import Locator
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.stream import StreamLocator
from deltacat.storage.model.table import TableLocator
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
        next_partition_id: Optional[str] = None,
    ) -> Partition:
        partition = Partition()
        partition.locator = locator
        partition.schema = schema
        partition.content_types = content_types
        partition.state = state
        partition.previous_stream_position = previous_stream_position
        partition.previous_partition_id = previous_partition_id
        partition.stream_position = stream_position
        partition.next_partition_id = next_partition_id
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
    def schema(self) -> Optional[Schema]:
        return self.get("schema")

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
    def next_partition_id(self) -> Optional[str]:
        return self.get("nextPartitionId")

    @next_partition_id.setter
    def next_partition_id(self, next_partition_id: Optional[str]):
        self["nextPartitionId"] = next_partition_id

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


class PartitionLocator(Locator, dict):
    @staticmethod
    def of(
        stream_locator: Optional[StreamLocator],
        partition_values: Optional[PartitionValues],
        partition_id: Optional[str],
        partition_scheme_id: Optional[str] = None,
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
        partition_locator.partition_scheme_id = partition_scheme_id
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
        partition_scheme_id: Optional[str],
    ) -> PartitionLocator:
        stream_locator = StreamLocator.at(
            namespace,
            table_name,
            table_version,
            stream_id,
            stream_format,
        )
        return PartitionLocator.of(
            stream_locator,
            partition_values,
            partition_id,
            partition_scheme_id,
        )

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
    def partition_scheme_id(self) -> Optional[str]:
        return self.get("partitionSchemeId")

    @partition_scheme_id.setter
    def partition_scheme_id(self, partition_scheme_id: Optional[str]) -> None:
        self["partitionSchemeId"] = partition_scheme_id

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

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        sl_hexdigest = self.stream_locator.hexdigest()
        partition_vals = str(self.partition_values)
        partition_id = self.partition_id
        scheme_id = self.partition_scheme_id
        return f"{sl_hexdigest}|{partition_vals}|{partition_id}|{scheme_id}"


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
            self["transform"] = val = Transform.of(val)
        return val

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")


class PartitionKeyList(List[PartitionKey]):
    @staticmethod
    def of(items: List[PartitionKey]) -> PartitionKeyList:
        items = PartitionKeyList()
        for entry in items:
            if entry is not None and not isinstance(entry, PartitionKey):
                entry = PartitionKey(entry)
            items.append(entry)
        return items

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
