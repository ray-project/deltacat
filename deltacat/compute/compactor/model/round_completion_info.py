# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Tuple
from deltacat.storage import DeltaLocator, PartitionLocator
from deltacat.compute.compactor.model.pyarrow_write_result import PyArrowWriteResult
from typing import Any, Dict, Optional


class HighWatermark(dict):
    """
    Inherit from dict to make it easy for serialization/deserialization.
    Keep both partition locator and high watermark as a tuple to be persisted in the rcf
    """

    def set(self, partition_locator: PartitionLocator, delta_stream_position: int):
        self[partition_locator.canonical_string()] = (
            partition_locator,
            delta_stream_position,
        )

    def get(self, partition_locator: PartitionLocator) -> int:
        if partition_locator.canonical_string() in self:
            return self[partition_locator.canonical_string()][1]
        return 0


class RoundCompletionInfo(dict):
    """
    In case of compacting deltas from multi-sources, the high_watermark dict records the high watermark for each source
    e.g., high_watermark = {"src1":100,"src2":200}
    src1 or src2 is the canonical_string of partition_locator
    """

    @staticmethod
    def of(
        high_watermark: HighWatermark,
        compacted_delta_locator: DeltaLocator,
        compacted_pyarrow_write_result: PyArrowWriteResult,
        sort_keys_bit_width: int,
        rebase_source_partition_locator: Optional[PartitionLocator] = None,
        manifest_entry_copied_by_reference_ratio: Optional[float] = None,
        compaction_audit_url: Optional[str] = None,
        hash_bucket_count: Optional[int] = None,
        hb_index_to_entry_range: Optional[Dict[int, Tuple[int, int]]] = None,
        compactor_version: Optional[str] = None,
        input_inflation: Optional[float] = None,
        input_average_record_size_bytes: Optional[float] = None,
    ) -> RoundCompletionInfo:

        rci = RoundCompletionInfo()
        rci["highWatermark"] = high_watermark
        rci["compactedDeltaLocator"] = compacted_delta_locator
        rci["compactedPyarrowWriteResult"] = compacted_pyarrow_write_result
        rci["sortKeysBitWidth"] = sort_keys_bit_width
        rci["rebaseSourcePartitionLocator"] = rebase_source_partition_locator
        rci[
            "manifestEntryCopiedByReferenceRatio"
        ] = manifest_entry_copied_by_reference_ratio
        rci["compactionAuditUrl"] = compaction_audit_url
        rci["hashBucketCount"] = hash_bucket_count
        rci["hbIndexToEntryRange"] = hb_index_to_entry_range
        rci["compactorVersion"] = compactor_version
        rci["inputInflation"] = input_inflation
        rci["inputAverageRecordSizeBytes"] = input_average_record_size_bytes
        return rci

    @property
    def high_watermark(self) -> HighWatermark:
        val: Dict[str, Any] = self.get("highWatermark")
        if (
            val is not None
            and isinstance(val, dict)
            and not isinstance(val, HighWatermark)
        ):
            self["highWatermark"] = val = HighWatermark(val)
        return val

    @property
    def compacted_delta_locator(self) -> DeltaLocator:
        val: Dict[str, Any] = self.get("compactedDeltaLocator")
        if val is not None and not isinstance(val, DeltaLocator):
            self["compactedDeltaLocator"] = val = DeltaLocator(val)
        return val

    @property
    def compacted_pyarrow_write_result(self) -> PyArrowWriteResult:
        val: Dict[str, Any] = self.get("compactedPyarrowWriteResult")
        if val is not None and not isinstance(val, PyArrowWriteResult):
            self["compactedPyarrowWriteResult"] = val = PyArrowWriteResult(val)
        return val

    @property
    def sort_keys_bit_width(self) -> int:
        return self["sortKeysBitWidth"]

    @property
    def compaction_audit_url(self) -> Optional[str]:
        return self.get("compactionAuditUrl")

    @property
    def rebase_source_partition_locator(self) -> Optional[PartitionLocator]:
        return self.get("rebaseSourcePartitionLocator")

    @property
    def manifest_entry_copied_by_reference_ratio(self) -> Optional[float]:
        return self["manifestEntryCopiedByReferenceRatio"]

    @property
    def hash_bucket_count(self) -> Optional[int]:
        return self["hashBucketCount"]

    @property
    def hb_index_to_entry_range(self) -> Optional[Dict[int, Tuple[int, int]]]:
        """
        The start index is inclusive and end index is exclusive by default.
        """
        return self["hbIndexToEntryRange"]

    @property
    def compactor_version(self) -> Optional[str]:
        return self.get("compactorVersion")

    @property
    def input_inflation(self) -> Optional[float]:
        return self.get("inputInflation")

    @property
    def input_average_record_size_bytes(self) -> Optional[float]:
        return self.get("inputAverageRecordSizeBytes")

    @staticmethod
    def get_audit_bucket_name_and_key(compaction_audit_url: str) -> Tuple[str, str]:
        return compaction_audit_url.replace("s3://", "").split("/", 1)
