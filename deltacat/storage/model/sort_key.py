# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

from typing import Optional, Any, List, Tuple, Dict

from pyarrow.compute import SortOptions

from deltacat.storage.model.types import (
    SortOrder,
    NullOrder,
)
from deltacat.storage.model.schema import FieldLocator
from deltacat.storage.model.transform import Transform


class SortKey(tuple):
    @staticmethod
    def of(
        key: Optional[List[FieldLocator]],
        sort_order: SortOrder = SortOrder.ASCENDING,
        null_order: NullOrder = NullOrder.AT_END,
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
                key,
                sort_order.value,
                null_order,
                transform,
                native_object,
            )
        )

    @property
    def key(self) -> Optional[List[FieldLocator]]:
        return self[0]

    @property
    def sort_order(self) -> SortOrder:
        return SortOrder(self[1])

    @property
    def null_order(self) -> NullOrder:
        return NullOrder(self[2])

    @property
    def transform(self) -> Optional[Transform]:
        val: Dict[str, Any] = self[3] if len(self) >= 4 else None
        if val is not None and not isinstance(val, Transform):
            self[3] = val = Transform.of(val)
        return val

    @property
    def arrow(self) -> List[Tuple[str, str]]:
        # TODO(pdames): Convert unsupported field locators to arrow field names,
        #   and transforms/multi-key-sorts to pyarrow compute expressions. Add
        #   null order via SortOptions when supported per field by Arrow.
        return (
            [(field_locator, self[1]) for field_locator in self[0]] if self[0] else []
        )

    @property
    def native_object(self) -> Optional[Any]:
        return self[4] if len(self) >= 5 else None


class SortKeyList(List[SortKey]):
    @staticmethod
    def of(items: List[SortKey]) -> SortKeyList:
        items = SortKeyList()
        for entry in items:
            if entry is not None and not isinstance(entry, SortKey):
                entry = SortKey(entry)
            items.append(entry)
        return items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, SortKey):
            self[item] = val = SortKey(val)
        return val


class SortScheme(dict):
    @staticmethod
    def of(
        keys: Optional[SortKeyList],
        name: Optional[str] = None,
        scheme_id: Optional[str] = None,
        native_object: Optional[Any] = None,
    ) -> SortScheme:
        return SortScheme(
            {
                "keys": keys,
                "name": name,
                "id": scheme_id,
                "nativeObject": native_object,
            }
        )

    @property
    def keys(self) -> Optional[SortKeyList]:
        val: List[SortKey] = self.get("keys")
        if val is not None and not isinstance(val, SortKeyList):
            self["keys"] = val = SortKeyList.of(val)
        return val

    @property
    def name(self) -> Optional[str]:
        return self.get("name")

    @property
    def id(self) -> Optional[str]:
        return self.get("id")

    @property
    def arrow(self) -> SortOptions:
        # TODO(pdames): Remove homogenous null ordering when supported by Arrow.
        if self.keys:
            if len(set([key.null_order for key in self.keys])) == 1:
                return SortOptions(
                    sort_keys=[pa_key for k in self.keys for pa_key in k.arrow],
                    null_placement=self.keys[0].null_order.value,
                )
            else:
                err_msg = "All arrow sort keys must use the same null order."
                raise ValueError(err_msg)
        return SortOptions()

    @property
    def native_object(self) -> Optional[Any]:
        return self.get("nativeObject")


class SortSchemeList(List[SortScheme]):
    @staticmethod
    def of(entries: List[SortScheme]) -> SortSchemeList:
        entries = SortSchemeList()
        for entry in entries:
            if entry is not None and not isinstance(entry, SortScheme):
                entry = SortScheme(entry)
            entries.append(entry)
        return entries

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, SortScheme):
            self[item] = val = SortScheme(val)
        return val
