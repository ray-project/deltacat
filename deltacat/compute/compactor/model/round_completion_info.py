# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from deltacat.storage import DeltaLocator
from deltacat.compute.compactor import PyArrowWriteResult, \
    PrimaryKeyIndexVersionLocator


class RoundCompletionInfo(dict):
    @staticmethod
    def of(high_watermark: int,
           compacted_delta_locator: DeltaLocator,
           compacted_pyarrow_write_result: PyArrowWriteResult,
           pk_index_pyarrow_write_result: PyArrowWriteResult,
           sort_keys_bit_width: int,
           primary_key_index_version_locator: PrimaryKeyIndexVersionLocator) \
            -> RoundCompletionInfo:

        return RoundCompletionInfo({
            "highWatermark": high_watermark,
            "compactedDeltaLocator": compacted_delta_locator,
            "compactedPyarrowWriteResult": compacted_pyarrow_write_result,
            "pkIndexPyarrowWriteResult": pk_index_pyarrow_write_result,
            "sortKeysBitWidth": sort_keys_bit_width,
            "primaryKeyIndexVersionLocator": primary_key_index_version_locator,
        })

    @property
    def high_watermark(self) -> int:
        return self["highWatermark"]

    @property
    def compacted_delta_locator(self) -> DeltaLocator:
        return self["compactedDeltaLocator"]

    @property
    def compacted_pyarrow_write_result(self) -> PyArrowWriteResult:
        return self["compactedPyarrowWriteResult"]

    @property
    def pk_index_pyarrow_write_result(self) -> PyArrowWriteResult:
        return self["pkIndexPyarrowWriteResult"]

    @property
    def sort_keys_bit_width(self) -> int:
        return self["sortKeysBitWidth"]

    @property
    def primary_key_index_version_locator(self) \
            -> PrimaryKeyIndexVersionLocator:
        return self["primaryKeyIndexVersionLocator"]
