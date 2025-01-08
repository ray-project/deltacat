import inspect
import os
from contextlib import contextmanager

from pyarrow import RecordBatch

from deltacat.storage.rivulet.dataset import Dataset
from deltacat.storage.rivulet.reader.query_expression import QueryExpression
from deltacat.storage.rivulet.writer.dataset_writer import DatasetWriter
import shutil
import tempfile

from deltacat.storage.rivulet.mvp.Table import MvpTable, MvpRow
from deltacat.storage.rivulet import Schema
from typing import Dict, List, Generator, Set

FIXTURE_ROW_COUNT = 10000


@contextmanager
def make_tmpdir():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)
        pass


def write_mvp_table(writer: DatasetWriter, table: MvpTable):
    writer.write(table.to_rows_list())


def mvp_table_to_record_batches(table: MvpTable, schema: Schema) -> RecordBatch:
    data = table.to_rows_list()
    columns = {key: [d.get(key) for d in data] for key in schema.keys()}
    record_batch = RecordBatch.from_pydict(columns, schema=schema.to_pyarrow())
    return record_batch


def compare_mvp_table_to_scan_results(
    table: MvpTable, scan_results: List[dict], pk: str
):
    table_row_list = table.to_rows_list()
    assert len(scan_results) == len(table_row_list)
    rows_by_pk: Dict[str, MvpRow] = table.to_rows_by_key(pk)
    assert len(rows_by_pk) == len(scan_results)
    for record in scan_results:
        pk_val = record[pk]
        assert rows_by_pk[pk_val].data == record


def validate_with_full_scan(dataset: Dataset, expected: MvpTable, schema: Schema):
    # best way to validate is to use dataset reader and read records
    read_records = list(dataset.scan(QueryExpression()).to_pydict())
    compare_mvp_table_to_scan_results(
        expected, read_records, list(dataset.get_merge_keys())[0]
    )


def generate_data_files(dataset: Dataset) -> Generator[str, None, None]:
    for ma in dataset._metastore.generate_manifests():
        for data_file in ma.manifest.data_files:
            yield data_file


def assert_data_file_extension(dataset: Dataset, file_extension: str):
    data_file_count = 0
    for data_file in generate_data_files(dataset):
        data_file_count += 1
        assert data_file.endswith(file_extension)
    assert data_file_count > 0, "No data files found in dataset"
    print(f"Asserted that {data_file_count} data files end with {file_extension}")


def assert_data_file_extension_set(dataset: Dataset, file_extension_set: Set[str]):
    """
    Asserts that each file extension in set appears at least once in dataset
    """
    data_file_count = 0
    found_extensions = set()

    for data_file in generate_data_files(dataset):
        data_file_count += 1
        for extension in file_extension_set:
            if data_file.endswith(extension):
                found_extensions.add(extension)
                break

    assert data_file_count > 0, "No data files found in dataset"
    assert (
        found_extensions == file_extension_set
    ), f"Missing extensions: {file_extension_set - found_extensions}"
    print(
        f"Asserted that among {data_file_count} data files, all extensions {file_extension_set} were found"
    )


def create_dataset_for_method(temp_dir: str):
    """
    Given a temp directory, creates a directory within it based on the name of the function calling this.
    Then returns a dataset based from that directory
    """
    caller_frame = inspect.getouterframes(inspect.currentframe())[1]
    dataset_dir = os.path.join(temp_dir, caller_frame.function)
    os.makedirs(dataset_dir)
    return Dataset(
        dataset_name=f"dataset-${caller_frame.function}", metadata_uri=dataset_dir
    )
