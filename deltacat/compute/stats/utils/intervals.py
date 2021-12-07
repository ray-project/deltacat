from typing import Set, Tuple
from collections import Iterable

# TODO: Allow users to add infinite ranges in intervals: i.e. (5, None) => (5, math.inf)
def merge_intervals(intervals: Set[Tuple[int, int]]):
    merged = set()
    intervals_list: List[Tuple[int, int]] = list(intervals)

    if len(intervals_list) == 0:
        return merged

    intervals_list.sort()  # sort by starting range numbers

    merge_start, merge_end = None, None
    for interval in intervals_list:
        start, end = _get_validated_interval(interval)

        if merge_start is None and merge_end is None:
            merge_start, merge_end = start, end
            continue

        if merge_end < start:
            merged.add((merge_start, merge_end))  # add current merge interval if no overlap, begin new interval
            merge_start, merge_end = start, end
        elif merge_end < end:
            merge_end = end  # expand current merge interval if there is an overlap

    merged.add((merge_start, merge_end))  # add final merge interval

    return merged


def _get_validated_interval(interval: Tuple[int, int]) -> Tuple[int, int]:
    if not isinstance(interval, Iterable) or len(interval) != 2:
        raise ValueError(f"Interval {interval} must be a tuple of size 2")

    start, end = interval

    if not isinstance(start, int) or not isinstance(end, int):
        raise ValueError(f"Invalid stream position value types: "
                         f"({start}, {end}) - ({type(start), type(end)})")

    if start > end:
        raise ValueError(f"Invalid stream position range interval: ({start}, {end})")

    return start, end