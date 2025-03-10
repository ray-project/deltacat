from __future__ import annotations

import pytest


# Benchmarks for retrieving a single column in the Parquet file
SINGLE_COLUMN_BENCHMARKS = {
    "mvp": ("s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet", ["a"]),
    "TPCH-lineitems-200MB-2RG": (
        "s3://daft-public-data/test_fixtures/parquet-dev/daft_200MB_lineitem_chunk.RG-2.parquet",
        ["L_ORDERKEY"],
    ),
}

# Benchmarks for retrieving all columns in the Parquet file
ALL_COLUMN_BENCHMARKS = {
    "mvp": ("s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet", None),
    "TPCH-lineitems-200MB-2RG": (
        "s3://daft-public-data/test_fixtures/parquet-dev/daft_200MB_lineitem_chunk.RG-2.parquet",
        None,
    ),
}


@pytest.mark.benchmark(group="num_rowgroups_single_column")
@pytest.mark.parametrize(
    ["name", "path", "columns"],
    [
        (name, path, columns)
        for name, (path, columns) in SINGLE_COLUMN_BENCHMARKS.items()
    ],
    ids=[name for name in SINGLE_COLUMN_BENCHMARKS],
)
def test_read_parquet_num_rowgroups_single_column(
    name, path, columns, read_fn, benchmark
):
    data = benchmark(read_fn, path, columns=columns)
    if columns is not None:
        assert data.column_names == columns


@pytest.mark.benchmark(group="num_rowgroups_all_columns")
@pytest.mark.parametrize(
    ["name", "path", "columns"],
    [(name, path, columns) for name, (path, columns) in ALL_COLUMN_BENCHMARKS.items()],
    ids=[name for name in ALL_COLUMN_BENCHMARKS],
)
def test_read_parquet_num_rowgroups_all_columns(
    name, path, columns, read_fn, benchmark
):
    data = benchmark(read_fn, path, columns=columns)
    if columns is not None:
        assert data.column_names == columns
