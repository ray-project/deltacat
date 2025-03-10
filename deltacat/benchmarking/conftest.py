from __future__ import annotations

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as papq
import pytest
from _pytest.terminal import TerminalReporter

from deltacat.benchmarking.benchmark_report import BenchmarkReport
from deltacat.utils.pyarrow import s3_file_to_table
from deltacat.types.media import (
    ContentEncoding,
    ContentType,
)


@pytest.fixture(autouse=True, scope="function")
def report(request):
    report = BenchmarkReport(request.node.name)

    def final_callback():
        terminal_reporter: TerminalReporter = request.config.pluginmanager.get_plugin(
            "terminalreporter"
        )
        capture_manager = request.config.pluginmanager.get_plugin("capturemanager")
        with capture_manager.global_and_fixture_disabled():
            terminal_reporter.ensure_newline()
            terminal_reporter.section(request.node.name, sep="-", blue=True, bold=True)
            terminal_reporter.write(str(report))
            terminal_reporter.ensure_newline()

    request.addfinalizer(final_callback)
    return report


def pyarrow_read(path: str, columns: list[str] | None = None) -> pa.Table:
    assert path.startswith(
        "s3://"
    ), f"Expected file path to start with 's3://', but got {path}."
    fs = pafs.S3FileSystem()
    path = path.replace("s3://", "")
    return papq.read_table(path, columns=columns, filesystem=fs)


def deltacat_read(path: str, columns: list[str] | None = None) -> pa.Table:
    assert path.startswith("s3://")
    return s3_file_to_table(
        path,
        content_type=ContentType.PARQUET,
        content_encoding=ContentEncoding.IDENTITY,
        column_names=None,  # Parquet files are schemaful
        include_columns=columns,
    )


def daft_table_read(path: str, columns: list[str] | None = None) -> pa.Table:
    try:
        import daft
    except ImportError:
        raise ImportError(
            "Daft not installed. Install Daft using pip to run these benchmarks: `pip install getdaft`"
        )

    tbl = daft.table.Table.read_parquet(path, columns=columns)
    return tbl.to_arrow()


@pytest.fixture(
    params=[
        daft_table_read,
        pyarrow_read,
        deltacat_read,
    ],
    ids=[
        "daft_table",
        "pyarrow",
        "deltacat",
    ],
)
def read_fn(request):
    """Fixture which returns the function to read a PyArrow table from a path"""
    return request.param
