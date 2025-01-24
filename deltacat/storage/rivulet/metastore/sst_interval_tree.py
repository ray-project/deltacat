from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from itertools import tee
from typing import Any, Dict, Set, List, FrozenSet, Iterable, TypeVar, NamedTuple

from intervaltree import Interval, IntervalTree

from deltacat.storage.rivulet.metastore.delta import DeltaContext
from deltacat.storage.rivulet.metastore.sst import SSTable, SSTableRow
from deltacat.storage.rivulet import Schema

T = TypeVar("T")


# Can be replaced with itertools.pairwise once we're on python 3.10+
def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


class Block(NamedTuple):
    row: SSTableRow
    context: DeltaContext
    """Context from the manifest around the placement of this row in the LSM-Tree"""


@dataclass(frozen=True)
class BlockGroup:
    """
    TODO discuss "Block" terminology.

    This data structure represents a set of data blocks which have to be traversed
     together, because rows in the block is in an overlapping key range

    The data and key ranges within a BlockGroup can be processed in parallel during a scan

    Each block may show up in many BlockGroups. This is because as soon as the combination of blocks in the group
    change, it becomes a different group.

    Take the following example: A dataset has Field Group 1 (FG1) and Field Group 2 (FG2). The SSTs rows look like below:

    Field group 1     Field group 2
    ------------------|---------------
    [0,100] -> Block1 | [0-10]  -> Block4
    [3-90]  -> Block2 | [0-100] -> Block5
    [10-95] -> Block3 |

    If we want to scan this data, we can index of off the SST rows into an interval tree and observe the following
    "boundaries" in the interval tree:

    BlockGroup1 - Covers rows [0-3), includes Blocks 1,4,5
    BlockGroup2 - Covers rows [3-10), includes Blocks 1, 2, 4, 5
    BlockGroup3 - Covers rows [10-90), includes Blocks 1, 2, 3, 4*, 5
    BlockGroup4 - Covers rows [90-95), includes Blocks 1, 2*, 3, 5
    BlockGroup5 - Covers rows [95-100], includes Blocks 1, 3*, 5
    *special case - interval end==block group start

    Creating a sorted list of BlockGroups like this allows us to know exactly which blocks contain records
    for any given point query, range query, or scan. For instance, if the user queries for key=3, we know
    to read BlockGroup1, or key[0-10] to read BlockGroup1+BlockGroup2.
    """

    key_min: T
    """
    Key min is inclusive (the block group covers data where primary key>=key_min)
    """
    key_max: T
    field_group_to_blocks: Dict[Schema, FrozenSet[Block]]
    key_max_inclusive: bool = False
    """
    By default, key_min is inclusive and key_max is non-inclusive.
    For the highest key in an SSTable, we need to set key_max_inclusive to True
    """

    def key_in_range(self, key: T) -> bool:
        if self.key_max_inclusive:
            return self.key_min <= key <= self.key_max
        else:
            return self.key_min <= key < self.key_max

    def key_below_range(self, key: T) -> bool:
        return key < self.key_min


@dataclass(frozen=True)
class OrderedBlockGroups:
    """
    Ordered block groups representing a sequential interval of a primary key range

    Block groups have an inclusive

    The block groups have a "boundary table" which represents primary key lower/upper ranges in table

    For example, a boundary table of [1,3,5,10] has blocks: [1,3), [3,5), [5,10]
    """

    key_min: T
    """
    Key min is inclusive
    """
    key_max: T
    """
    Key max is inclusive
    """
    block_groups: List[BlockGroup]
    boundary_table: List[T]


class BlockIntervalTree:
    """
    This interval tree combines all L0 SSTables in order to effectively traverse key ranges
    so that data can be zippered across field groups

    This interval tree is SHARED across N different field groups. This is because
        maintaining a different interval tree for N different field groups would require
        more complex traversal to achieve the same result
    """

    def __init__(self):
        self.tree: IntervalTree = IntervalTree()
        self.max_key_map: Dict[Any, List[Interval]] = {}

    def add_sst_table(self, sst: SSTable, context: DeltaContext):
        """
        Add intervals to SSTree which use primary key min and max as intervals
        The data for each interval is a tuple of (schema, SSTableRow)
        """
        self.add_sst_rows(sst.rows, context)

    def add_sst_rows(self, sst_rows: Iterable[SSTableRow], context: DeltaContext):
        """
        Add individual SSTable rows to tree
        """
        for row in sst_rows:
            interval: Interval = Interval(row.key_min, row.key_max, Block(row, context))
            self.tree.add(interval)
            if row.key_max not in self.max_key_map:
                self.max_key_map[row.key_max] = [interval]
            else:
                self.max_key_map[row.key_max].append(interval)

    def get_sorted_block_groups(
        self, min_key: Any | None = None, max_key: Any | None = None
    ) -> OrderedBlockGroups:
        """
        Returns an ordered list of block group by primary key range
        The IntervalTree boundary table contains each boundary where the set of intervals change
        This function traverses the boundary table and builds a list

        Edge case - incompatibility with Interval Tree Library

        Note that the IntervalTree Library treats ALL ranges as min=inclusive, max=non-inclusive
        This is a different from our SSTables, where min_key and max_key are both inclusive.

        As a suboptimal workaround, we can fix this by adding an SSTRow to an interval when the
        SST row's max key is equal to the lower bound of the interval. Take the following example:

        Field group 1     Field group 2
        ------------------|---------------
        [0,100] -> Block1 | [0-10]  -> Block4
        [3-90]  -> Block2 | [0-100] -> Block5
        [10-95] -> Block3 |

        Our workaround adds for example Block4 to the interval [10,90), because Block4 is inclusive
        of key 10 but the IntervalTree library thinks it is non-inclusive.

        BlockGroup1 - Covers rows [0-3), includes Blocks 1,4,5
        BlockGroup2 - Covers rows [3-10), includes Blocks 1, 2, 4, 5
        BlockGroup3 - Covers rows [10-90), includes Blocks 1, 2, 3, 4*, 5
        BlockGroup4 - Covers rows [90-95), includes Blocks 1, 2*, 3, 5
        BlockGroup5 - Covers rows [95-100], includes Blocks 1, 3*, 5
        *special case - interval end==block group start

        An ideal solution would produce block groups like below. To do this against the IntervalTree
        library, we would need to know how to convert an inclusive range like ["bar", "foo"] into a
        range like ["bar", "fooa") where the end range is non-inclusive. It is fine for our block groups
        to be non-optimal, we just need code when zipper merging to detect if a block group's max key is
        less than the current iterator and therfore not consider it.

        Optimal block groups:
        BlockGroup1 - Covers rows [0-3), includes Blocks 1,5,4
        BlockGroup2 - Covers rows [3-10), includes Blocks 1,5,2,4
        BlockGroup3 - Covers rows [10-11), includes Blocks 1, 2, 3, 4, 5
        BlockGroup3 - Covers rows [11-91), includes Blocks 1,2,3,5
        BlockGroup4 - Covers rows [91-96), includes Blocks 1,3,5
        BlockGroup5 - Covers rows [96-100], includes Blocks 1,5

        :param: min_key optionally restrict result so that they must overlap or be greater than min_key, INCLUSIVE
            A range [0,200) will be included for min_key <=100 because it overlaps with the min key
            A range [100, 200) will be included because it overlaps with the min key (100 key inclusive)
        :param: max_key optionally restrict result to be less than or overlap with max_key, INCLUSIVE
            A range like [100,200) will NOT included for max key=200 because range maxes are non-inclusive
        """
        key_boundaries = self.tree.boundary_table.keys()
        block_groups: List[BlockGroup] = []
        block_groups_min = None
        block_groups_max = None

        # Note that we need to expand min_key_idx and max_key_idx by 1 to cover cases where
        # the pairwise traversal (x,y) has x>=min_key>=y and x<=max_key<=y
        if min_key is not None and max_key is not None and min_key > max_key:
            raise ValueError(
                f"min_key {min_key} cannot be greater than max_key {max_key}"
            )

        min_key_idx = (
            max(0, bisect_left(key_boundaries, min_key) - 1)
            if min_key is not None
            else None
        )
        max_key_idx = (
            bisect_right(key_boundaries, max_key) + 1 if max_key is not None else None
        )
        boundary_table = key_boundaries[min_key_idx:max_key_idx]

        for lower_bound, upper_bound in pairwise(boundary_table):
            # Note that IntervalTree library treats lower bound of slice as inclusive and upper as exclusive
            # We follow the same structure in our BlockGroup
            intervals: Set[Interval] = self.tree.overlap(lower_bound, upper_bound)

            # Special case for if max key is equal to lower_bound. See method pydoc for more details
            for i in self.max_key_map.get(lower_bound, []):
                intervals.add(i)

            field_group_to_blocks = defaultdict(set)
            for interval in intervals:
                data: Block = interval.data
                schema = data.context.schema
                field_group_to_blocks[schema].add(data)

            # freeze dict to make it hashable
            field_group_to_blocks = {
                k: frozenset(v) for k, v in field_group_to_blocks.items()
            }

            # Special case - if this is the very last iteration, set key_max_inclusive to True
            max_key_inclusive = upper_bound == boundary_table[-1]

            block_group = BlockGroup(
                lower_bound, upper_bound, field_group_to_blocks, max_key_inclusive
            )
            block_groups_min = (
                lower_bound
                if block_groups_min is None
                else min(block_groups_min, lower_bound)
            )
            block_groups_max = (
                upper_bound
                if block_groups_max is None
                else max(block_groups_max, upper_bound)
            )
            block_groups.append(block_group)

        return OrderedBlockGroups(
            block_groups_min, block_groups_max, block_groups, boundary_table
        )
