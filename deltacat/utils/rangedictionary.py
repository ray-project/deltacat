from collections import OrderedDict


class IntegerRangeDict(OrderedDict):
    """
    An integer dictionary with range-style access.
    """

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            raise ValueError("value must be an int")
        return super().__setitem__(key, value)

    def __getitem__(self, key):
        keys = list(super().keys())
        ordered_key_list = sorted(keys)
        for candidate_idx, candidate_key in enumerate(ordered_key_list):
            if key <= candidate_key:
                correct_key = ordered_key_list[candidate_idx]
                return super().__getitem__(correct_key)
        raise KeyError(
            f"Could not find {key} or any key greater than {key} in this RangeDict"
        )
