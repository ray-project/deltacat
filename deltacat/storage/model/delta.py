# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage.model.types import DeltaType
from deltacat.storage.model.namespace import NamespaceLocator
from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.model.stream import StreamLocator
from deltacat.storage.model.table import TableLocator
from deltacat.storage.model.table_version import TableVersionLocator
from deltacat.storage.model.locator import Locator
from deltacat.aws.redshift import Manifest, ManifestMeta, ManifestAuthor

from typing import Any, Dict, List, Optional


class Delta(dict):
    @staticmethod
    def of(locator: Optional[DeltaLocator],
           delta_type: Optional[DeltaType],
           meta: Optional[ManifestMeta],
           properties: Optional[Dict[str, str]],
           manifest: Optional[Manifest],
           previous_stream_position: Optional[int] = None) -> Delta:
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
            properties: Optional[Dict[str, str]] = None) -> Delta:
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
            raise ValueError(f"Deltas to merge must have non-empty manifests.")
        distinct_storage_types = set([d.storage_type for d in deltas])
        if len(distinct_storage_types) > 1:
            raise NotImplementedError(
                f"Deltas to merge must all share the same storage type "
                f"(found {len(distinct_storage_types)} storage types.")
        pl_digest_set = set([d.partition_locator.digest()
                             for d in deltas])
        if len(pl_digest_set) > 1:
            raise ValueError(
                f"Deltas to merge must all belong to the same partition "
                f"(found {len(pl_digest_set)} partitions).")
        distinct_delta_types = set([d.type for d in deltas])
        if len(distinct_delta_types) > 1:
            raise ValueError(
                f"Deltas to merge must all share the same delta type "
                f"(found {len(distinct_delta_types)} delta types).")
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
    def manifest(
            self,
            manifest: Optional[Manifest]) -> None:
        self["manifest"] = manifest

    @property
    def meta(self) -> Optional[ManifestMeta]:
        val: Dict[str, Any] = self.get("meta")
        if val is not None and not isinstance(val, ManifestMeta):
            self.meta = val = ManifestMeta(val)
        return val

    @meta.setter
    def meta(
            self,
            meta: Optional[ManifestMeta]) -> None:
        self["meta"] = meta

    @property
    def properties(self) -> Optional[Dict[str, str]]:
        return self.get("properties")

    @properties.setter
    def properties(
            self,
            properties: Optional[Dict[str, str]]) -> None:
        self["properties"] = properties

    @property
    def type(self) -> Optional[DeltaType]:
        delta_type = self.get("type")
        return None if delta_type is None else DeltaType(delta_type)

    @type.setter
    def type(
            self,
            delta_type: Optional[DeltaType]) -> None:
        self["type"] = delta_type

    @property
    def locator(self) -> Optional[DeltaLocator]:
        val: Dict[str, Any] = self.get("deltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self.locator = val = DeltaLocator(val)
        return val

    @locator.setter
    def locator(
            self,
            delta_locator: Optional[DeltaLocator]) -> None:
        self["deltaLocator"] = delta_locator

    @property
    def previous_stream_position(self) -> Optional[int]:
        return self.get("previousStreamPosition")

    @previous_stream_position.setter
    def previous_stream_position(
            self,
            previous_stream_position: Optional[int]) -> None:
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
            return delta_locator.storage_type
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
    def partition_id(self) -> Optional[str]:
        delta_locator = self.locator
        if delta_locator:
            return delta_locator.partition_id
        return None

    @property
    def partition_values(self) -> Optional[List[Any]]:
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


class DeltaLocator(Locator, dict):
    @staticmethod
    def of(partition_locator: Optional[PartitionLocator],
           stream_position: Optional[int]) -> DeltaLocator:
        """
        Creates a partition delta locator. Stream Position, if provided, should
        be greater than that of any prior delta in the partition.
        """
        delta_locator = DeltaLocator()
        delta_locator.partition_locator = partition_locator
        delta_locator.stream_position = stream_position
        return delta_locator

    @property
    def partition_locator(self) -> Optional[PartitionLocator]:
        val: Dict[str, Any] = self.get("partitionLocator")
        if val is not None and not isinstance(val, PartitionLocator):
            self.partition_locator = val = PartitionLocator(val)
        return val

    @partition_locator.setter
    def partition_locator(
            self,
            partition_locator: Optional[PartitionLocator]) -> None:
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
    def partition_values(self) -> Optional[List[Any]]:
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
    def storage_type(self) -> Optional[str]:
        partition_locator = self.partition_locator
        if partition_locator:
            return partition_locator.storage_type
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

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        pl_hexdigest = self.partition_locator.hexdigest()
        stream_position = self.stream_position
        return f"{pl_hexdigest}|{stream_position}"
