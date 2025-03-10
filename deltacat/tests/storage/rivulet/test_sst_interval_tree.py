from typing import List, FrozenSet, Dict

import pytest

from deltacat.storage.rivulet.metastore.delta import DeltaContext
from deltacat.storage.rivulet.metastore.sst import SSTable, SSTableRow
from deltacat.storage.rivulet.metastore.sst_interval_tree import (
    BlockIntervalTree,
    BlockGroup,
    OrderedBlockGroups,
    Block,
)
from deltacat.storage.rivulet.schema.datatype import Datatype
from deltacat.storage.rivulet import Schema


@pytest.fixture
def schema1() -> Schema:
    return Schema(
        {
            ("id", Datatype.int32()),
            ("name", Datatype.string()),
            ("age", Datatype.int32()),
        },
        "id",
    )


@pytest.fixture
def schema2() -> Schema:
    return Schema(
        {
            ("id", Datatype.int32()),
            ("address", Datatype.string()),
            ("zip", Datatype.string()),
        },
        "id",
    )


@pytest.fixture
def sst_row_list() -> List[SSTableRow]:
    return [
        SSTableRow(0, 100, "block1", 0, 1),
        SSTableRow(3, 90, "block2", 0, 1),
        SSTableRow(10, 95, "block3", 0, 1),
        SSTableRow(0, 10, "block4", 0, 1),
        SSTableRow(0, 100, "block5", 0, 1),
    ]


@pytest.fixture
def sst1(sst_row_list) -> SSTable:
    return SSTable(sst_row_list[0:3], 0, 100)


@pytest.fixture
def sst2(sst_row_list) -> SSTable:
    return SSTable(sst_row_list[3:5], 0, 100)


@pytest.fixture
def manifest_context1(schema1) -> DeltaContext:
    return DeltaContext(schema1, "manifest-001", 0)


@pytest.fixture
def manifest_context2(schema2) -> DeltaContext:
    return DeltaContext(schema2, "manifest-002", 1)


def with_field_group(
    context: DeltaContext, rows: List[SSTableRow], indexes: List[int]
) -> Dict[Schema, FrozenSet[Block]]:
    """Construct a BlockGroup dict for a singular field group"""
    schema = context.schema
    return {schema: frozenset([Block(rows[i], context) for i in indexes])}


@pytest.fixture
def expected_block_groups(
    manifest_context1, manifest_context2, sst_row_list
) -> List[BlockGroup]:
    return [
        BlockGroup(
            0,
            3,
            with_field_group(manifest_context1, sst_row_list, [0])
            | with_field_group(manifest_context2, sst_row_list, [3, 4]),
        ),
        BlockGroup(
            3,
            10,
            with_field_group(manifest_context1, sst_row_list, [0, 1])
            | with_field_group(manifest_context2, sst_row_list, [3, 4]),
        ),
        BlockGroup(
            10,
            90,
            with_field_group(manifest_context1, sst_row_list, [0, 1, 2])
            | with_field_group(manifest_context2, sst_row_list, [3, 4]),
        ),
        BlockGroup(
            90,
            95,
            with_field_group(manifest_context1, sst_row_list, [0, 1, 2])
            | with_field_group(manifest_context2, sst_row_list, [4]),
        ),
        BlockGroup(
            95,
            100,
            with_field_group(manifest_context1, sst_row_list, [0, 2])
            | with_field_group(manifest_context2, sst_row_list, [4]),
        ),
    ]


def test_build_sst(
    sst1,
    sst2,
    manifest_context1,
    manifest_context2,
    sst_row_list,
    expected_block_groups,
):
    t = BlockIntervalTree()
    t.add_sst_table(sst1, manifest_context1)
    t.add_sst_table(sst2, manifest_context2)

    block_groups = t.get_sorted_block_groups()
    expected = _build_ordered_block_groups(expected_block_groups)
    assert expected == block_groups


def test_build_sst_with_bounds(
    sst1,
    sst2,
    manifest_context1,
    manifest_context2,
    sst_row_list,
    expected_block_groups,
):
    t = BlockIntervalTree()
    t.add_sst_table(sst1, manifest_context1)
    t.add_sst_table(sst2, manifest_context2)

    block_groups_filtered = t.get_sorted_block_groups(20, 100)
    expected = _build_ordered_block_groups(expected_block_groups[2:])
    assert expected == block_groups_filtered

    block_groups_filtered = t.get_sorted_block_groups(96, 100)
    expected = _build_ordered_block_groups(expected_block_groups[4:])
    assert expected == block_groups_filtered

    block_groups_filtered = t.get_sorted_block_groups(0, 10)
    expected = _build_ordered_block_groups(expected_block_groups[0:3])
    assert expected == block_groups_filtered

    # Max key of 95 is inclusive of last range so it is included
    block_groups_filtered = t.get_sorted_block_groups(None, 95)
    expected = _build_ordered_block_groups(expected_block_groups)
    assert expected == block_groups_filtered

    block_groups_filtered = t.get_sorted_block_groups(None, 94)
    expected = _build_ordered_block_groups(expected_block_groups[0:4])
    assert expected == block_groups_filtered

    block_groups_filtered = t.get_sorted_block_groups(0, 10)
    expected = _build_ordered_block_groups(expected_block_groups[0:3])
    assert expected == block_groups_filtered

    block_groups_filtered = t.get_sorted_block_groups(0, 0)
    expected = _build_ordered_block_groups(expected_block_groups[0:1])
    assert expected == block_groups_filtered


def test_build_sst_with_non_zero_min_key_matching_global_min_key(manifest_context1):
    # Using a non-0 value since 0 evaluates to False
    min_key = 1
    max_key = 95

    sst_row = SSTableRow(min_key, max_key, "row-with-non-zero-min-key", 0, 1)
    t = BlockIntervalTree()
    t.add_sst_table(SSTable([sst_row], min_key, max_key), manifest_context1)

    block_groups_filtered = t.get_sorted_block_groups(min_key, min_key + 1)
    expected = _build_ordered_block_groups(
        [
            BlockGroup(
                min_key,
                max_key,
                {
                    manifest_context1.schema: frozenset(
                        [Block(sst_row, manifest_context1)]
                    )
                },
            )
        ]
    )
    assert expected == block_groups_filtered


def test_build_sst_invalid_bounds(
    sst1, sst2, schema1, schema2, sst_row_list, expected_block_groups
):
    t = BlockIntervalTree()

    with pytest.raises(ValueError):
        t.get_sorted_block_groups(10, 0)


def _build_ordered_block_groups(block_groups: List[BlockGroup]) -> OrderedBlockGroups:
    """
    Helper method to build OrderedBlockGroups from a sorted list of block groups

    """
    ordered_groups = []
    boundary_table = []
    for i, bg in enumerate(block_groups):
        boundary_table.append(bg.key_min)
        is_last = i == len(block_groups) - 1
        if is_last:
            bg = BlockGroup(bg.key_min, bg.key_max, bg.field_group_to_blocks, True)
            boundary_table.append(bg.key_max)
        ordered_groups.append(bg)

    return OrderedBlockGroups(
        ordered_groups[0].key_min,
        ordered_groups[-1].key_max,
        ordered_groups,
        boundary_table,
    )
