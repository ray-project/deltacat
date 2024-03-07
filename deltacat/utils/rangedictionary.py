from collections import OrderedDict
from typing import Any, Optional, Union


class IntegerRangeDict(OrderedDict):
    """

    An ordered dictionary that supports range-style access using integer keys.

    This class extends the functionality of the built-in OrderedDict by allowing retrieval of
    values using the next greater integer key if the requested key is not found. This behavior
    is particularly useful in scenarios where data is organized in a range-based structure,
    such as time series or indexing.

    If <k,v> are inserted in a sorted order then the order will be maintained as it subclasses of OrderedDict
    which preserves the order in <k,v> are inserted

    Example:
        >>> d = IntegerRangeDict()
        >>> d[1] = 'a'
        >>> d[3] = 'b'
        >>> d[2]
        'b'
        >>> d[1]
        'a'
        >>> d[4]
        KeyError

    Args:
        items (dict, optional): A dictionary or other iterable of (key, value) pairs to
            initialize the IntegerRangeDict.

    Attributes:
        None

    Methods:
        __setitem__(key: int, value: Any) -> None:
            Set the value for the specified integer key.

        __getitem__(key: int) -> Any:
            Get the value for the specified integer key or the value of the next greater key.

        get(key: int, default: Optional[Any] = None) -> Any:
            Get the value for the specified integer key or the default value if the key is not found.

        rebalance() -> IntegerRangeDict:
            Reorder the keys in ascending order and return a new IntegerRangeDict instance.

    Raises:
        ValueError: If a non-integer key is used for setting or getting values.
        KeyError: If the requested key or the next greater key is not found.
    """

    def __init__(self, items: Union[dict, "IntegerRangeDict", None] = None):
        super().__init__(items or {})

    def __setitem__(self, key: int, value: Any):
        if not isinstance(key, int):
            raise ValueError("key must be an int type")
        return super().__setitem__(key, value)

    def __getitem__(self, key: int) -> Any:
        if not isinstance(key, int):
            raise ValueError("key must be an int type")
        try:
            return super().__getitem__(key)
        except KeyError as ke:
            keys = list(super().keys())
            for i, existing_key in enumerate(keys):
                if key < existing_key:
                    key_of_next_greater_element = keys[i]
                    return super().__getitem__(key_of_next_greater_element)
            raise KeyError(
                f"{ke}. Could not find this key {key} or the next key greater than {key}"
            ) from None

    def get(self, item, default: Optional[Any] = None) -> Any:
        try:
            return self.__getitem__(item)
        except KeyError:
            return default

    def rebalance(self) -> "IntegerRangeDict":
        # NOTE: an explicit rebalancing method to ensure the keys are in sorted order without resorting
        # to a underlying self-balancing BST tree to maintain O(log(n)) item retrieval.
        # This implementation is O(n) to rebalance
        # TODO: rebalance during <k,v> insertion
        sorted_keys = sorted(list(super().keys()))
        for key in sorted_keys:
            val = super().pop(key)
            super().__setitem__(key, val)
        return self
