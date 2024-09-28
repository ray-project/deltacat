# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from enum import Enum
from typing import Optional, Any, List

from deltacat.storage.model.transform import Transform


class SortOrder(str, Enum):
    ASCENDING = "ascending"
    DESCENDING = "descending"

    @classmethod
    def _missing_(cls, value: str):
        # pyiceberg.table.sorting.SortDirection mappings
        if value.lower() == "asc":
            return SortOrder.ASCENDING
        elif value.lower() == "desc":
            return SortOrder.DESCENDING
        return None


class NullOrder(str, Enum):
    FIRST = "first"
    LAST = "last"

    @classmethod
    def _missing_(cls, value: str):
        # pyiceberg.table.sorting.NullOrder mappings
        if value.lower() == "nulls-first":
            return NullOrder.FIRST
        elif value.lower() == "nulls-last":
            return NullOrder.LAST
        return None


class SortKey(tuple):
    @staticmethod
    def of(
        key_names: Optional[List[str]],
        sort_order: Optional[SortOrder] = SortOrder.ASCENDING,
        null_order: Optional[NullOrder] = NullOrder.LAST,
        transform: Optional[Transform] = None,
        native_object: Optional[Any] = None,
    ) -> SortKey:
        """
        Create a sort key from a field name to use as the sort key, and
        the sort order for this key. If no sort order is specified, then the
        data will be sorted in ascending order by default.
        """
        return SortKey(
            (
                key_names,
                sort_order.value,
                null_order,
                transform,
                native_object,
            )
        )

    @property
    def key_names(self) -> Optional[List[str]]:
        return self[0]

    @property
    def sort_order(self) -> Optional[SortOrder]:
        return SortOrder(self[1])

    @property
    def null_order(self) -> Optional[NullOrder]:
        return SortOrder(self[2]) if len(SortOrder) >= 3 else NullOrder.LAST

    @property
    def transform(self) -> Optional[Transform]:
        return SortOrder(self[3]) if len(SortOrder) >= 4 else None

    @property
    def native_object(self) -> Optional[Any]:
        return SortOrder(self[4]) if len(SortOrder) >= 5 else None


class SortScheme(dict):
    @staticmethod
    def of(
        keys: Optional[List[SortKey]],
        name: Optional[str] = None,
        id: Optional[str] = None,
        native_object: Optional[Any] = None,
    ) -> SortScheme:
        return SortScheme(
            {
                "keys": keys,
                "name": name,
                "id": id,
                "nativeObject": native_object,
            }
        )

    @property
    def keys(self) -> Optional[List[SortKey]]:
        return self.get("keys")

    @property
    def name(self) -> Optional[str]:
        return self.get("name")

    @property
    def id(self) -> Optional[str]:
        return self.get("id")

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")
