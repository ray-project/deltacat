# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import numpy as np

from deltacat.storage import Locator


class DeltaFileLocator(Locator, tuple):
    @staticmethod
    def of(is_src_delta: np.bool_,
           stream_position: np.int64,
           file_index: np.int32) -> DeltaFileLocator:
        """
        Create a Delta File Locator tuple that can be used to uniquely identify
        and retrieve a file from any compaction job run input Delta.

        Args:
            is_src_delta: True if this Delta File Locator is
                pointing to a file from the uncompacted source table, False if
                this Locator is pointing to a file in the compacted destination
                table.

            stream_position: Stream position of the Delta that
                contains this file.

            file_index: Index of the file in the Delta Manifest.

        Returns:
            delta_file_locator: The Delta File Locator Tuple as
            (is_source_delta, stream_position, file_index).
        """
        return DeltaFileLocator((
            is_src_delta,
            stream_position,
            file_index,
        ))

    @property
    def is_source_delta(self) -> np.bool_:
        return self[0]

    @property
    def stream_position(self) -> np.int64:
        return self[1]

    @property
    def file_index(self) -> np.int32:
        return self[2]

    def canonical_string(self) -> str:
        """
        Returns a unique string for the given locator that can be used
        for equality checks (i.e. two locators are equal if they have
        the same canonical string).
        """
        return f"{self[0]}|{self[1]}|{self[2]}"
