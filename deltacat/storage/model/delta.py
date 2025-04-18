# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import posixpath
from typing import Any, Dict, List, Optional

import pyarrow

from deltacat.storage.model.metafile import Metafile, MetafileRevisionInfo
from deltacat.constants import TXN_DIR_NAME
from deltacat.storage.model.manifest import (
    Manifest,
    ManifestMeta,
    ManifestAuthor,
)
from deltacat.storage.model.locator import (
    Locator,
    LocatorName,
)
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.partition import (
    PartitionLocator,
    PartitionValues,
)
from deltacat.storage.model.stream import StreamLocator
from deltacat.storage.model.table import (
    TableLocator,
    Table,
)
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.types import (
    DeltaType,
    StreamFormat,
)

DeltaProperties = Dict[str, Any]


class Delta(Metafile):
    @staticmethod
    def of(
        locator: Optional[DeltaLocator],
        delta_type: Optional[DeltaType],
        meta: Optional[ManifestMeta],
        properties: Optional[DeltaProperties],
        manifest: Optional[Manifest],
        previous_stream_position: Optional[int] = None,
    ) -> Delta:
        """
        Creates a Delta metadata model with the given Delta Locator, Delta Type,
        manifest metadata, properties, manifest, and previous delta stream
        position.
        """
        delta = Delta()
        delta.locator = locator
        delta.type = delta_type
        delta.meta = meta
        delta.properties = properties
        delta.manifest = manifest
        delta.previous_stream_position = previous_stream_position
        return delta

    @staticmethod
    def merge_deltas(
        deltas: List[Delta],
        manifest_author: Optional[ManifestAuthor] = None,
        stream_position: Optional[int] = None,
        properties: Optional[DeltaProperties] = None,
    ) -> Delta:
        """
        Merges the input list of deltas into a single delta. All input deltas to
        merge must belong to the same partition, share the same delta type, and
        have non-empty manifests.

        Manifest content type and content encoding will be set to None in the
        event of conflicting types or encodings between individual manifest
        entries. Missing record counts, content lengths, and source content
        lengths will be coalesced to 0. Delta properties, stream position, and
        manifest author will be set to None unless explicitly specified.

        The previous partition stream position of the merged delta is set to the
        maximum previous partition stream position of all input deltas, or None
        if the previous partition stream position of any input delta is None.

        Input delta manifest entry order will be preserved in the merged delta
        returned. That is, if manifest entry A preceded manifest entry B
        in the input delta list, then manifest entry A will precede manifest
        entry B in the merged delta.
        """
        if not deltas:
            raise ValueError("No deltas given to merge.")
        manifests = [d.manifest for d in deltas]
        if any(not m for m in manifests):
            raise ValueError("Deltas to merge must have non-empty manifests.")
        distinct_storage_types = set([d.storage_type for d in deltas])
        if len(distinct_storage_types) > 1:
            raise NotImplementedError(
                f"Deltas to merge must all share the same storage type "
                f"(found {len(distinct_storage_types)} storage types."
            )
        pl_digest_set = set([d.partition_locator.digest() for d in deltas])
        if len(pl_digest_set) > 1:
            raise ValueError(
                f"Deltas to merge must all belong to the same partition "
                f"(found {len(pl_digest_set)} partitions)."
            )
        distinct_delta_types = set([d.type for d in deltas])
        if len(distinct_delta_types) > 1:
            raise ValueError(
                f"Deltas to merge must all share the same delta type "
                f"(found {len(distinct_delta_types)} delta types)."
            )
        merged_manifest = Manifest.merge_manifests(
            manifests,
            manifest_author,
        )
        partition_locator = deltas[0].partition_locator
        prev_positions = [d.previous_stream_position for d in deltas]
        prev_position = None if None in prev_positions else max(prev_positions)
        return Delta.of(
            DeltaLocator.of(partition_locator, stream_position),
            distinct_delta_types.pop(),
            merged_manifest.meta,
            properties,
            merged_manifest,
            prev_position,
        )

    @property
    def manifest(self) -> Optional[Manifest]:
        val: Dict[str, Any] = self.get("manifest")
        if val is not None and not isinstance(val, Manifest):
            self.manifest = val = Manifest(val)
        return val

    @manifest.setter
    def manifest(self, manifest: Optional[Manifest]) -> None:
        self["manifest"] = manifest

    @property
    def meta(self) -> Optional[ManifestMeta]:
        val: Dict[str, Any] = self.get("meta")
        if val is not None and not isinstance(val, ManifestMeta):
            self.meta = val = ManifestMeta(val)
        return val

    @meta.setter
    def meta(self, meta: Optional[ManifestMeta]) -> None:
        self["meta"] = meta

    @property
    def properties(self) -> Optional[DeltaProperties]:
        return self.get("properties")

    @properties.setter
    def properties(self, properties: Optional[DeltaProperties]) -> None:
        self["properties"] = properties

    @property
    def type(self) -> Optional[DeltaType]:
        delta_type = self.get("type")
        return None if delta_type is None else DeltaType(delta_type)

    @type.setter
    def type(self, delta_type: Optional[DeltaType]) -> None:
        self["type"] = delta_type

    @property
    def locator(self) -> Optional[DeltaLocator]:
        val: Dict[str, Any] = self.get("deltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self.locator = val = DeltaLocator(val)
        return val

    @locator.setter
    def locator(self, delta_locator: Optional[DeltaLocator]) -> None:
        self["deltaLocator"] = delta_locator

    @property
    def previous_stream_position(self) -> Optional[int]:
        return self.get("previousStreamPosition")

    @previous_stream_position.setter
    def previous_stream_position(self, previous_stream_position: Optional[int]) -> None:
        self["previousStreamPosition"] = previous_stream_position

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.table_locator
        return None

    @property
    def table_version_locator(self) -> Optional[TableVersionLocator]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.table_version_locator
        return None

    @property
    def stream_locator(self) -> Optional[StreamLocator]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.stream_locator
        return None

    @property
    def partition_locator(self) -> Optional[PartitionLocator]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.partition_locator
        return None

    @property
    def storage_type(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.stream_format
        return None

    @property
    def namespace(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.table_version
        return None

    @property
    def stream_id(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.stream_id
        return None

    @property
    def stream_format(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.stream_format
        return None

    @property
    def partition_id(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.partition_id
        return None

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.partition_values
        return None

    @property
    def stream_position(self) -> Optional[int]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.stream_position
        return None

    def to_serializable(self) -> Delta:
        serializable = self
        if serializable.table_locator:
            serializable: Delta = Delta.update_for(self)
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
    ) -> Delta:
        # TODO(pdames): Lazily restore table locator on 1st property get.
        #  Cache Metafile ID <-> Table/Namespace-Name map at Catalog Init, then
        #  swap only Metafile IDs with Names here.
        if self.table_locator and self.table_locator.table_name == self.id:
            parent_rev_dir_path = Metafile._parent_metafile_rev_dir_path(
                base_metafile_path=path,
                parent_number=4,
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


class DeltaLocatorName(LocatorName):
    def __init__(self, locator: DeltaLocator):
        self.locator = locator

    @property
    def immutable_id(self) -> Optional[str]:
        return str(self.locator.stream_position)

    @immutable_id.setter
    def immutable_id(self, immutable_id: Optional[str]):
        self.locator.stream_position = int(immutable_id)

    def parts(self) -> List[str]:
        return [str(self.locator.stream_position)]


class DeltaLocator(Locator, dict):
    @staticmethod
    def of(
        partition_locator: Optional[PartitionLocator] = None,
        stream_position: Optional[int] = None,
    ) -> DeltaLocator:
        """
        Creates a partition delta locator. Stream Position, if provided, should
        be greater than that of any prior delta in the partition.
        """
        delta_locator = DeltaLocator()
        delta_locator.partition_locator = partition_locator
        delta_locator.stream_position = stream_position
        return delta_locator

    @staticmethod
    def at(
        namespace: Optional[str],
        table_name: Optional[str],
        table_version: Optional[str],
        stream_id: Optional[str],
        stream_format: Optional[StreamFormat],
        partition_values: Optional[PartitionValues],
        partition_id: Optional[str],
        stream_position: Optional[int],
    ) -> DeltaLocator:
        partition_locator = (
            PartitionLocator.at(
                namespace,
                table_name,
                table_version,
                stream_id,
                stream_format,
                partition_values,
                partition_id,
            )
            if partition_values and partition_id
            else None
        )
        return DeltaLocator.of(
            partition_locator,
            stream_position,
        )

    @property
    def name(self):
        return DeltaLocatorName(self)

    @property
    def parent(self) -> Optional[PartitionLocator]:
        return self.partition_locator

    @property
    def partition_locator(self) -> Optional[PartitionLocator]:
        val: Dict[str, Any] = self.get("partitionLocator")
        if val is not None and not isinstance(val, PartitionLocator):
            self.partition_locator = val = PartitionLocator(val)
        return val

    @partition_locator.setter
    def partition_locator(self, partition_locator: Optional[PartitionLocator]) -> None:
        self["partitionLocator"] = partition_locator

    @property
    def stream_position(self) -> Optional[int]:
        return self.get("streamPosition")

    @stream_position.setter
    def stream_position(self, stream_position: Optional[int]) -> None:
        self["streamPosition"] = stream_position

    @property
    def namespace_locator(self) -> Optional[NamespaceLocator]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.namespace_locator
        return None

    @property
    def table_locator(self) -> Optional[TableLocator]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.table_locator
        return None

    @property
    def table_version_locator(self) -> Optional[TableVersionLocator]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.table_version_locator
        return None

    @property
    def stream_locator(self) -> Optional[StreamLocator]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.stream_locator
        return None

    @property
    def partition_values(self) -> Optional[PartitionValues]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.partition_values
        return None

    @property
    def partition_id(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.partition_id
        return None

    @property
    def stream_id(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.stream_id
        return None

    @property
    def stream_format(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.stream_format
        return None

    @property
    def namespace(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.namespace
        return None

    @property
    def table_name(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.table_name
        return None

    @property
    def table_version(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.table_version
        return None
