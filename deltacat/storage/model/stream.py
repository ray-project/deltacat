# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import posixpath

import pyarrow

import deltacat.storage.model.partition as partition

from typing import Any, Dict, Optional, List

from deltacat.storage.model.metafile import Metafile, MetafileRevisionInfo
from deltacat.constants import TXN_DIR_NAME
from deltacat.storage.model.locator import (
    Locator,
    LocatorName,
)
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.table import (
    TableLocator,
    Table,
)
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.types import (
    CommitState,
    StreamFormat,
)


class Stream(Metafile):
    """
    An unbounded stream of Deltas, where each delta's records are optionally
    partitioned according to the given partition scheme.
    """

    @staticmethod
    def of(
        locator: Optional[StreamLocator],
        partition_scheme: Optional[partition.PartitionScheme],
        state: Optional[CommitState] = None,
        previous_stream_id: Optional[str] = None,
        watermark: Optional[int] = None,
        native_object: Optional[Any] = None,
    ) -> Stream:
        stream = Stream()
        stream.locator = locator
        stream.partition_scheme = partition_scheme
        stream.state = state
        stream.previous_stream_id = previous_stream_id
        stream.watermark = watermark
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
    def locator_alias(self) -> Optional[StreamLocatorAlias]:
        return StreamLocatorAlias.of(self)

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
    def watermark(self) -> Optional[int]:
        return self.get("watermark")

    @watermark.setter
    def watermark(self, watermark: Optional[int]) -> None:
        self["watermark"] = watermark

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
    def stream_format(self) -> Optional[str]:
        stream_locator = self.locator
        if stream_locator:
            return stream_locator.format
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

    def to_serializable(self) -> Stream:
        serializable = self
        if serializable.table_locator:
            serializable: Stream = Stream.update_for(self)
            # remove the mutable table locator
            serializable.table_version_locator.table_locator = TableLocator.at(
                namespace=self.id,
                table_name=self.id,
            )
        return serializable

    def from_serializable(
        self,
        path: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ) -> Stream:
        # restore the table locator from its mapped immutable metafile ID
        if self.table_locator and self.table_locator.table_name == self.id:
            parent_rev_dir_path = Metafile._parent_metafile_rev_dir_path(
                base_metafile_path=path,
                parent_number=2,
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


class StreamLocatorName(LocatorName):
    def __init__(self, locator: StreamLocator):
        self.locator = locator

    @property
    def immutable_id(self) -> Optional[str]:
        return self.locator.stream_id

    @immutable_id.setter
    def immutable_id(self, immutable_id: Optional[str]):
        self.locator.stream_id = immutable_id

    def parts(self) -> List[str]:
        return [
            self.locator.stream_id,
            self.locator.format,
        ]


class StreamLocator(Locator, dict):
    @staticmethod
    def of(
        table_version_locator: Optional[TableVersionLocator],
        stream_id: Optional[str],
        stream_format: Optional[StreamFormat],
    ) -> StreamLocator:
        """
        Creates a table version Stream Locator. All input parameters are
        case-sensitive.
        """
        stream_locator = StreamLocator()
        stream_locator.table_version_locator = table_version_locator
        stream_locator.stream_id = stream_id
        stream_locator.format = (
            stream_format.value
            if isinstance(stream_format, StreamFormat)
            else stream_format
        )
        return stream_locator

    @staticmethod
    def at(
        namespace: Optional[str],
        table_name: Optional[str],
        table_version: Optional[str],
        stream_id: Optional[str],
        stream_format: Optional[StreamFormat],
    ) -> StreamLocator:
        table_version_locator = (
            TableVersionLocator.at(
                namespace,
                table_name,
                table_version,
            )
            if table_version
            else None
        )
        return StreamLocator.of(
            table_version_locator,
            stream_id,
            stream_format,
        )

    @property
    def name(self) -> StreamLocatorName:
        return StreamLocatorName(self)

    @property
    def parent(self) -> Optional[TableVersionLocator]:
        return self.table_version_locator

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
    def format(self) -> Optional[str]:
        return self.get("format")

    @format.setter
    def format(self, stream_format: Optional[str]) -> None:
        self["format"] = stream_format

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


class StreamLocatorAliasName(LocatorName):
    def __init__(self, locator: StreamLocatorAlias):
        self.locator = locator

    @property
    def immutable_id(self) -> Optional[str]:
        return None

    def parts(self) -> List[str]:
        return [self.locator.format]


class StreamLocatorAlias(Locator, dict):
    @staticmethod
    def of(
        parent_stream: Stream,
    ) -> StreamLocatorAlias:
        return (
            StreamLocatorAlias(
                {
                    "format": parent_stream.stream_format,
                    "parent": (
                        parent_stream.locator.parent if parent_stream.locator else None
                    ),
                }
            )
            if parent_stream.state == CommitState.COMMITTED
            else None  # only committed streams can be resolved by alias
        )

    @property
    def format(self) -> Optional[str]:
        return self.get("format")

    @property
    def name(self) -> StreamLocatorAliasName:
        return StreamLocatorAliasName(self)

    @property
    def parent(self) -> Optional[Locator]:
        return self.get("parent")
