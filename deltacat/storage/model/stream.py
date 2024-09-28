# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import deltacat.storage.model.partition as partition

from typing import Any, Dict, Optional

from deltacat.storage.model.locator import Locator
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import TableLocator
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.types import CommitState


class Stream(dict):
    """
    An unbounded stream of Deltas, where each delta's records are optionally
    partitioned according to the given partition scheme.
    """
    @staticmethod
    def of(
        locator: Optional[StreamLocator],
        partition_scheme: Optional[partition.PartitionScheme],
        state: Optional[CommitState] = None,
        previous_stream_id: Optional[bytes] = None,
        native_object: Optional[Any] = None,
    ) -> Stream:
        stream = Stream()
        stream.locator = locator
        stream.partition_scheme = partition_scheme
        stream.state = state
        stream.previous_stream_id = previous_stream_id
        stream.native_object = native_object
        return stream

    @property
    def locator(self) -> Optional[StreamLocator]:
        val: Dict[str, Any] = self.get("streamLocator")
        if val is not None and not isinstance(val, StreamLocator):
            self.locator = val = StreamLocator(val)
        return val

    @locator.setter
    def locator(self, stream_locator: Optional[StreamLocator]) -> None:
        self["streamLocator"] = stream_locator

    @property
    def partition_scheme(self) -> Optional[partition.PartitionScheme]:
        """
        A table's partition keys are defined within the context of a
        Partition Scheme, which supports defining both fields to partition
        a table by and optional transforms to apply to those fields to
        derive the Partition Values that a given field, and its corresponding
        record, belong to.
        """
        val: Dict[str, Any] = self.get("partitionScheme")
        if val is not None and not isinstance(val, partition.PartitionScheme):
            self.partition_scheme = val = partition.PartitionScheme(val)
        return val

    @partition_scheme.setter
    def partition_scheme(
        self, partition_scheme: Optional[partition.PartitionScheme]
    ) -> None:
        self["partitionScheme"] = partition_scheme

    @property
    def previous_stream_id(self) -> Optional[str]:
        return self.get("previousStreamId")

    @previous_stream_id.setter
    def previous_stream_id(self, previous_stream_id: Optional[str]) -> None:
        self["previousStreamId"] = previous_stream_id

    @property
    def state(self) -> Optional[CommitState]:
        """
        The commit state of a stream.
        """
        state = self.get("state")
        return None if state is None else CommitState(state)

    @state.setter
    def state(self, state: Optional[CommitState]) -> None:
        self["state"] = state

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")

    @native_object.setter
    def native_object(self, native_object: Optional[Any]) -> None:
        self["nativeObject"] = native_object

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.table_locator
        return None

    @property
    def table_version_locator(self) -> Optional[TableVersionLocator]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.table_version_locator
        return None

    @property
    def stream_id(self) -> Optional[str]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.stream_id
        return None

    @property
    def namespace(self) -> Optional[str]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.table_version
        return None


class StreamLocator(Locator, dict):
    @staticmethod
    def of(
        table_version_locator: Optional[TableVersionLocator],
        stream_id: Optional[str],
        storage_type: Optional[str],
    ) -> StreamLocator:
        """
        Creates a table version Stream Locator. All input parameters are
        case-sensitive.
        """
        stream_locator = StreamLocator()
        stream_locator.table_version_locator = table_version_locator
        stream_locator.stream_id = stream_id
        stream_locator.storage_type = storage_type
        return stream_locator

    @staticmethod
    def at(
        namespace: Optional[str],
        table_name: Optional[str],
        table_version: Optional[str],
        stream_id: Optional[str],
        storage_type: Optional[str],
    ) -> StreamLocator:
        table_version_locator = TableVersionLocator.at(
            namespace,
            table_name,
            table_version,
        )
        return StreamLocator.of(
            table_version_locator,
            stream_id,
            storage_type,
        )

    @property
    def table_version_locator(self) -> Optional[TableVersionLocator]:
        val: Dict[str, Any] = self.get("tableVersionLocator")
        if val is not None and not isinstance(val, TableVersionLocator):
            self.table_version_locator = val = TableVersionLocator(val)
        return val

    @table_version_locator.setter
    def table_version_locator(
        self, table_version_locator: Optional[TableVersionLocator]
    ) -> None:
        self["tableVersionLocator"] = table_version_locator

    @property
    def stream_id(self) -> Optional[str]:
        return self.get("streamId")

    @stream_id.setter
    def stream_id(self, stream_id: Optional[str]) -> None:
        self["streamId"] = stream_id

    @property
    def storage_type(self) -> Optional[str]:
        return self.get("storageType")

    @storage_type.setter
    def storage_type(self, storage_type: Optional[str]) -> None:
        self["storageType"] = storage_type

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        table_version_locator = self.table_version_locator
        if table_version_locator:
            return table_version_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        table_version_locator = self.table_version_locator
        if table_version_locator:
            return table_version_locator.table_locator
        return None

    @property
    def namespace(self) -> Optional[str]:
        table_version_locator = self.table_version_locator
        if table_version_locator:
            return table_version_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        table_version_locator = self.table_version_locator
        if table_version_locator:
            return table_version_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        table_version_locator = self.table_version_locator
        if table_version_locator:
            return table_version_locator.table_version
        return None

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        tvl_hexdigest = self.table_version_locator.hexdigest()
        stream_id = self.stream_id
        storage_type = self.storage_type
        return f"{tvl_hexdigest}|{stream_id}|{storage_type}"
