# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from enum import Enum


class SortOrder(str, Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"


class SortKey(tuple):
    @staticmethod
    def of(key_name: str, sort_order: SortOrder = SortOrder.ASCENDING) -> SortKey:
        """
        Create a sort key from a field name to use as the sort key, and
        the sort order for this key. If no sort order is specified, then the
        data will be sorted in ascending order by default. Note that compaction
        always keeps the LAST occurrence of this key post-sort. For example, if
        you used an integer column as your sort key which contained the values
        [2, 1, 3] specifying SortOrder.ASCENDING would ensure that the
        value [3] is kept over [2, 1], and specifying SortOrder.DESCENDING
        would ensure that [1] is kept over [2, 3].
        """
        return SortKey((key_name, sort_order.value))

    @property
    def key_name(self) -> str:
        return self[0]

    @property
    def sort_order(self) -> SortOrder:
        return SortOrder(self[1])
