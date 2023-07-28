from __future__ import annotations

import pytest

# Data to benchmark against:
# {`benchmark_name`: (`s3_path`, `column_to_read`), ...}
BENCHMARK_DATA = {
    "mvp": ("s3://daft-public-data/test_fixtures/parquet-dev/mvp.parquet", ["a"]),
    "TPCH-lineitems-200MB-1RG": ("s3://daft-public-data/test_fixtures/parquet-dev/daft_200MB_lineitem_chunk.RG-2.parquet", ["L_ORDERKEY"]),
}


@pytest.mark.benchmark(group="num_rowgroups_single_column")
@pytest.mark.parametrize(
    ["path", "columns"],
    [(path, columns) for _, (path, columns) in BENCHMARK_DATA.items()],
    ids=[name for name, _ in BENCHMARK_DATA.items()],
)
def test_read_parquet_num_rowgroups_single_column(path, columns, read_fn, benchmark):
    data = benchmark(read_fn, path, columns=columns)


@pytest.mark.benchmark(group="num_rowgroups_all_columns")
@pytest.mark.parametrize(
    "path",
    [path for _, (path, _) in BENCHMARK_DATA.items()],
    ids=[name for name, _ in BENCHMARK_DATA.items()],
)
def test_read_parquet_num_rowgroups_all_columns(path, read_fn, benchmark):
    data = benchmark(read_fn, path)
