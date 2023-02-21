# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Any, Dict, List, Optional

from deltacat.storage.model.locator import Locator
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import TableLocator
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.types import CommitState


class Stream(dict):
    @staticmethod
    def of(
        locator: Optional[StreamLocator],
        partition_keys: Optional[List[Dict[str, Any]]],
        state: Optional[CommitState] = None,
        previous_stream_digest: Optional[bytes] = None,
    ) -> Stream:
        stream = Stream()
        stream.locator = locator
        stream.partition_keys = partition_keys
        stream.state = state
        stream.previous_stream_digest = previous_stream_digest
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
    def partition_keys(self) -> Optional[List[Dict[str, Any]]]:
        return self.get("partitionKeys")

    @partition_keys.setter
    def partition_keys(self, partition_keys: Optional[List[Dict[str, Any]]]) -> None:
        self["partitionKeys"] = partition_keys

    @property
    def previous_stream_digest(self) -> Optional[str]:
        return self.get("previousStreamDigest")

    @previous_stream_digest.setter
    def previous_stream_digest(self, previous_stream_digest: Optional[str]) -> None:
        self["previousStreamDigest"] = previous_stream_digest

    @property
    def state(self) -> Optional[CommitState]:
        state = self.get("state")
        return None if state is None else CommitState(state)

    @state.setter
    def state(self, state: Optional[CommitState]) -> None:
        self["state"] = state

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

    def validate_partition_values(self, partition_values: Optional[List[Any]]):
        # TODO (pdames): ensure value data types match key data types
        partition_keys = self.partition_keys
        num_keys = len(partition_keys) if partition_keys else 0
        num_values = len(partition_values) if partition_values else 0
        if num_values != num_keys:
            raise ValueError(
                f"Found {num_values} partition values but "
                f"{num_keys} partition keys: {self}"
            )


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
