from collections import OrderedDict
from typing import Any, List


class IntegerRangeDict(OrderedDict):
    """
    An integer dictionary with range-style access.
    """

    def __setitem__(self, key: int, value: Any):
        if not isinstance(key, int):
            raise ValueError("key must be an int type")
        return super().__setitem__(key, value)

    def __getitem__(self, key: int) -> Any:
        if not isinstance(key, int):
            raise ValueError("key must be an int type")
        try:
            val = super().__getitem__(key)
            if val:
                return val
        except KeyError as ke:
            keys = list(super().keys())
            for candidate_idx, candidate_key in enumerate(keys):
                if key < candidate_key:
                    key_of_next_greater_element = keys[candidate_idx]
                    return super().__getitem__(key_of_next_greater_element)
            raise KeyError(
                f"{ke}. Could not find this key {key} or the next key greater than {key}"
            ) from None

    def rebalance(self):
        keys = list(super().keys())
        if IntegerRangeDict._is_sorted(keys):
            return self
        sorted_keys = sorted(keys)
        for key in sorted_keys:
            val = super().pop(key)
            super().__setitem__(key, val)
        return self

    @staticmethod
    def _is_sorted(keys: List[int]) -> bool:
        for curr_key, next_key in zip(keys, keys[1:]):
            if curr_key > next_key:
                return False
        return True
