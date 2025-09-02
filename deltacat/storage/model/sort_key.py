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

UNSORTED_SCHEME_NAME = "unsorted_scheme"
UNSORTED_SCHEME_ID = "deadbeef-7277-49a4-a195-fdc8ed235d42"


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
                sort_order.value if isinstance(sort_order, SortOrder) else sort_order,
                null_order.value if isinstance(null_order, NullOrder) else null_order,
                transform,
                native_object,
            )
        )

    def equivalent_to(
        self,
        other: SortKey,
    ):
        if other is None:
            return False
        if not isinstance(other, tuple):
            return False
        if not isinstance(other, SortKey):
            other = SortKey(other)
        return (
            self.key == other.key
            and self.transform == other.transform
            and self.sort_order == other.sort_order
            and self.null_order == other.null_order
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
        val: Dict[str, Any] = (
            Transform(self[3]) if len(self) >= 4 and self[3] is not None else None
        )
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
        typed_items = SortKeyList()
        for item in items:
            if item is not None and not isinstance(item, SortKey):
                item = SortKey(item)
            typed_items.append(item)
        return typed_items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, SortKey):
            self[item] = val = SortKey(val)
        return val

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]  # This triggers __getitem__ conversion


class SortScheme(dict):
    @staticmethod
    def of(
        keys: Optional[SortKeyList],
        name: Optional[str] = None,
        scheme_id: Optional[str] = None,
        native_object: Optional[Any] = None,
    ) -> SortScheme:
        # Validate keys if provided
        if keys is not None:
            # Check for empty keys list
            if len(keys) == 0:
                raise ValueError("Sort scheme cannot have empty keys list")

            # Check for duplicate keys
            key_names = []
            for key in keys:
                if key.key[0] in key_names:
                    raise ValueError(f"Duplicate sort key found: {key.key[0]}")
                key_names.append(key.key[0])

        return SortScheme(
            {
                "keys": keys,
                "name": name,
                "id": scheme_id,
                "nativeObject": native_object,
            }
        )

    def equivalent_to(
        self,
        other: SortScheme,
        check_identifiers: bool = False,
    ) -> bool:
        if other is None:
            return False
        if not isinstance(other, dict):
            return False
        if not isinstance(other, SortScheme):
            other = SortScheme(other)
        # If both have None keys, they are equivalent (for unsorted schemes)
        if self.keys is None and other.keys is None:
            return not check_identifiers or (
                self.name == other.name and self.id == other.id
            )
        # If only one has None keys, they are not equivalent
        if self.keys is None or other.keys is None:
            return False
        # Compare keys if both have them
        for i in range(len(self.keys)):
            if not self.keys[i].equivalent_to(other.keys[i]):
                return False
        return not check_identifiers or (
            self.name == other.name and self.id == other.id
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


UNSORTED_SCHEME = SortScheme.of(
    keys=None,
    name=UNSORTED_SCHEME_NAME,
    scheme_id=UNSORTED_SCHEME_ID,
)


class SortSchemeList(List[SortScheme]):
    @staticmethod
    def of(items: List[SortScheme]) -> SortSchemeList:
        typed_items = SortSchemeList()
        for item in items:
            if item is not None and not isinstance(item, SortScheme):
                item = SortScheme(item)
            typed_items.append(item)
        return typed_items

    def __getitem__(self, item):
        val = super().__getitem__(item)
        if val is not None and not isinstance(val, SortScheme):
            self[item] = val = SortScheme(val)
        return val

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]  # This triggers __getitem__ conversion
