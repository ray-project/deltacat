from typing import Iterable, List, Optional, Set, Tuple, Union

DeltaPosition = Optional[int]
NumericDeltaPosition = Union[int, float]  # float is added here to support math.inf
DeltaRange = Tuple[DeltaPosition, DeltaPosition]


def merge_intervals(intervals: Set[DeltaRange]) -> Set[DeltaRange]:
    """Merges a set of N input intervals into a minimal number of output intervals.

    All input intervals will be merged into a minimal number of output intervals in O(N log N) time.

    Example:
        >>> merge_intervals((3, 9), (8, 12), (15, 19))
        ((3, 12), (15, 19))

    Example:
        >>> merge_intervals((3, 9), (None, 15), (13, 30))
        ((None, 30))

    Args:
        intervals: A list of intervals with an int type representing finite, closed bounded values and
            a None type representing infinity.

    Returns:
        A minimal number of output intervals
    """
    merged: Set[DeltaRange] = set()
    intervals_list: List[DeltaRange] = list(intervals)

    if len(intervals_list) == 0:
        return merged

    _to_numeric_values(intervals_list)
    intervals_list.sort()  # sort by starting range numbers

    merge_start, merge_end = None, None
    for interval in intervals_list:
        start, end = interval
        if start > end:
            raise ValueError(
                f"Invalid stream position range interval: ({start}, {end})"
            )

        if merge_start is None and merge_end is None:
            merge_start, merge_end = start, end
            continue

        if merge_end < start:
            # add current merge interval if no overlap, begin new interval
            _add_merged_interval(merged, merge_start, merge_end)
            merge_start, merge_end = start, end
        elif merge_end < end:
            merge_end = end  # expand current merge interval if there is an overlap

    # add final merge interval
    _add_merged_interval(merged, merge_start, merge_end)

    return merged


def _add_merged_interval(
    result_set: set, start: NumericDeltaPosition, end: NumericDeltaPosition
):
    start_pos: DeltaPosition = start if isinstance(start, int) else None
    end_pos: DeltaPosition = end if isinstance(end, int) else None
    result_set.add((start_pos, end_pos))


def _to_numeric_values(intervals_list: List[DeltaRange]):
    for i, interval in enumerate(intervals_list):
        start, end = _get_validated_interval(interval)
        if start is None:
            start = float("-inf")
        if end is None:
            end = float("inf")

        intervals_list[i] = (start, end)


def _get_validated_interval(interval: DeltaRange) -> DeltaRange:
    if not isinstance(interval, Iterable) or len(interval) != 2:
        raise ValueError(f"Interval {interval} must be a tuple of size 2")

    start, end = interval
    if not (isinstance(start, int) or start is None) or not (
        isinstance(end, int) or end is None
    ):
        raise ValueError(
            f"Invalid stream position value types: "
            f"({start}, {end}) - ({type(start), type(end)})"
        )

    return start, end
