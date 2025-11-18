from collections import defaultdict
import pytest
import ray
import threading
import re
from typing import List, Dict, Any, Tuple
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa
import daft

from deltacat.compute.converter.steps.convert import convert
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.pyiceberg.overrides import (
    fetch_all_bucket_files,
)
from deltacat.compute.converter.utils.converter_session_utils import (
    group_all_files_to_each_bucket,
)
from deltacat.tests.compute.converter.utils import (
    get_s3_file_system,
    drop_table_if_exists,
    commit_equality_delete_to_table,
)
from deltacat.compute.converter.pyiceberg.update_snapshot_overrides import (
    commit_append_snapshot,
)

from pyiceberg.typedef import Record
from pyiceberg.exceptions import CommitFailedException
from deltacat.compute.converter.utils.convert_task_options import BASE_MEMORY_BUFFER

# Task memory in bytes for testing
TASK_MEMORY_BYTES = BASE_MEMORY_BUFFER


# Test data fixtures
@pytest.fixture
def base_schema():
    return Schema(
        NestedField(
            field_id=1, name="number_partitioned", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2, name="primary_key", field_type=StringType(), required=False
        ),
        NestedField(
            field_id=2147483546,
            name="file_path",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=2147483545, name="pos", field_type=LongType(), required=False
        ),
        schema_id=0,
    )


@pytest.fixture
def base_schema_without_metadata():
    return Schema(
        NestedField(
            field_id=1, name="number_partitioned", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2, name="primary_key", field_type=StringType(), required=False
        ),
        schema_id=0,
    )


@pytest.fixture
def multi_key_schema():
    return Schema(
        NestedField(
            field_id=1, name="number_partitioned", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2, name="primary_key1", field_type=StringType(), required=False
        ),
        NestedField(
            field_id=3, name="primary_key2", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2147483546,
            name="file_path",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=2147483545, name="pos", field_type=LongType(), required=False
        ),
        schema_id=0,
    )


@pytest.fixture
def multi_key_schema_without_file_path():
    return Schema(
        NestedField(
            field_id=1, name="number_partitioned", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2, name="primary_key1", field_type=StringType(), required=False
        ),
        NestedField(
            field_id=3, name="primary_key2", field_type=LongType(), required=False
        ),
        schema_id=0,
    )


@pytest.fixture
def base_partition_spec():
    partition_field_identity = PartitionField(
        source_id=1,
        field_id=101,
        transform=IdentityTransform(),
        name="number_partitioned",
    )
    return PartitionSpec(partition_field_identity)


@pytest.fixture
def table_properties():
    return {
        "write.format.default": "parquet",
        "write.delete.mode": "merge-on-read",
        "write.update.mode": "merge-on-read",
        "write.merge.mode": "merge-on-read",
        "format-version": "2",
    }


def create_test_table(
    session_catalog: RestCatalog,
    namespace: str,
    table_name: str,
    schema: Schema,
    partition_spec: PartitionSpec,
    properties: Dict[str, str],
) -> str:
    """Helper function to create a test table"""
    identifier = f"{namespace}.{table_name}"
    drop_table_if_exists(identifier, session_catalog)
    session_catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=partition_spec,
        properties=properties,
    )
    return identifier


def create_mock_data_tables(test_case: Dict[str, Any]) -> Tuple[daft.DataFrame, ...]:
    """Helper function to create mock data tables based on test case"""
    tables = []
    for data in test_case["mock_data"]:
        if "primary_key2" in data:  # Multi-key case
            names = ["primary_key1", "primary_key2"]
            table = pa.Table.from_arrays(
                [pa.array(data["primary_key1"]), pa.array(data["primary_key2"])],
                names=names,
            )
        else:  # Single key case
            names = ["primary_key"]
            table = pa.Table.from_arrays([pa.array(data["primary_key"])], names=names)
        tables.append(daft.from_arrow(table))
    if "equality_delete_data_mock" in test_case:
        for data in test_case["equality_delete_data_mock"]:
            if "primary_key2" in data:  # Multi-key case
                names = ["primary_key1", "primary_key2"]
                table = pa.Table.from_arrays(
                    [pa.array(data["primary_key1"]), pa.array(data["primary_key2"])],
                    names=names,
                )
            else:  # Single key case
                names = ["primary_key"]
                table = pa.Table.from_arrays(
                    [pa.array(data["primary_key"])], names=names
                )
            tables.append(daft.from_arrow(table))
    return tuple(tables)


def run_spark_commands(spark, sqls: List[str]) -> None:
    """Helper function to run Spark SQL commands"""
    for sql in sqls:
        spark.sql(sql)


def insert_test_data(spark, identifier: str, test_case: Dict[str, Any]) -> None:
    """Helper function to insert test data into the table"""
    if "primary_key2" in test_case["mock_data"][0]:
        # Multi-key case
        for data in test_case["mock_data"]:
            values = ", ".join(
                f"(0, '{pk1}', {pk2})"
                for pk1, pk2 in zip(data["primary_key1"], data["primary_key2"])
            )
            run_spark_commands(spark, [f"INSERT INTO {identifier} VALUES {values}"])
    else:
        # Single key case
        if test_case["schema"] == "base_schema":
            # For drop duplicates test, use file_path and pos from mock_data
            for data in test_case["mock_data"]:
                values = ", ".join(
                    f"(0, '{pk}', '{path}', {pos})"
                    for pk, path, pos in zip(
                        data["primary_key"], data["file_path"], data["pos"]
                    )
                )
                run_spark_commands(spark, [f"INSERT INTO {identifier} VALUES {values}"])
        else:
            # For other tests, just include the basic columns
            for data in test_case["mock_data"]:
                values = ", ".join(f"(0, '{pk}')" for pk in data["primary_key"])
                run_spark_commands(spark, [f"INSERT INTO {identifier} VALUES {values}"])


def create_convert_input(
    tbl,
    convert_input_files_for_all_buckets: List[Any],
    test_case: Dict[str, Any],
    s3_file_system: Any,
) -> List[ConvertInput]:
    """Helper function to create convert inputs"""
    convert_inputs = []
    for i, one_bucket_files in enumerate(convert_input_files_for_all_buckets):
        convert_input = ConvertInput.of(
            convert_input_files=one_bucket_files,
            convert_task_index=i,
            iceberg_table_warehouse_prefix="warehouse/default",
            identifier_fields=test_case["identifier_fields"],
            table_io=tbl.io,
            table_metadata=tbl.metadata,
            compact_previous_position_delete_files=False,
            enforce_primary_key_uniqueness=True,
            position_delete_for_multiple_data_files=True,
            max_parallel_data_file_download=10,
            filesystem=s3_file_system,
            s3_client_kwargs={},
            task_memory=TASK_MEMORY_BYTES,
        )
        convert_inputs.append(convert_input)
    return convert_inputs


def process_convert_result(convert_result: Any) -> Tuple[List[Any], List[Any]]:
    """Helper function to process convert results

    Args:
        convert_result: The result from convert_session

    Returns:
        Tuple[List[Any], List[Any]]: Lists of files to be deleted and added
    """
    to_be_deleted_files_list = []
    to_be_added_files_list = []
    if convert_result.to_be_deleted_files:
        to_be_deleted_files_list.extend(convert_result.to_be_deleted_files.values())
    if convert_result.to_be_added_files:
        to_be_added_files_list.extend(convert_result.to_be_added_files)
    return to_be_deleted_files_list, to_be_added_files_list


def verify_result(result, expected_result, verify_pos_index=False):
    """Verify the result matches the expected result.

    Args:
        result: The result to verify
        expected_result: The expected result
        verify_pos_index: Whether to verify position values for primary keys
    """
    if "primary_keys" in expected_result and "primary_key" in result:
        # Single key case
        assert set(result["primary_key"]) == set(expected_result["primary_keys"])
        if verify_pos_index and "pk_to_pos" in expected_result:
            for index in range(len(result["primary_key"])):
                assert (
                    result["pos"][index]
                    == expected_result["pk_to_pos"][result["primary_key"][index]]
                )
    elif "pk_tuples" in expected_result:
        pk_combined_res = []
        for pk1, pk2 in zip(
            result["primary_key1"],
            result["primary_key2"],
        ):
            pk_combined_res.append((pk1, pk2))

        # Multi-key case
        assert set(pk_combined_res) == set(expected_result["pk_tuples"])
    else:
        assert set(result) == set(expected_result["primary_keys"])


def verify_spark_read_results(spark, identifier, expected_result, snapshot_id=None):
    if snapshot_id:
        spark_read_pos_delete = spark.sql(
            f"select * from {identifier} VERSION AS OF {snapshot_id}"
        ).collect()
    else:
        spark_read_pos_delete = spark.sql(f"select * from {identifier}").collect()
    all_pk = [
        spark_read_pos_delete[row_idx][1]
        for row_idx in range(len(spark_read_pos_delete))
    ]
    verify_result(all_pk, expected_result, verify_pos_index=False)


def get_file_prefix(tbl):
    """Get the file prefix from a table's data files.

    Args:
        tbl: The table to get the file prefix from

    Returns:
        str: The file prefix
    """
    df = tbl.inspect.entries()
    data_files = df.to_pydict()["data_file"]
    file_link = data_files[0]["file_path"]
    file_prefix = "/".join(file_link.split("/")[:-1])
    return file_prefix.split("//")[1]


# Test cases configuration
TEST_CASES = [
    {
        "name": "single_key_drop_duplicates",
        "table_name": "table_converter_ray_drop_duplicates_success",
        "schema": "base_schema",
        "identifier_fields": ["primary_key"],
        "mock_data": [
            {
                "primary_key": ["pk1", "pk2", "pk3"],
                "file_path": ["path1", "path2", "path3"],
                "pos": [1, 2, 3],
            },
            {
                "primary_key": ["pk1", "pk2", "pk3"],
                "file_path": ["path1", "path2", "path3"],
                "pos": [4, 5, 6],
            },
            {
                "primary_key": ["pk4", "pk2", "pk3"],
                "file_path": ["path4", "path2", "path3"],
                "pos": [7, 8, 9],
            },
        ],
        "expected_result": {
            "primary_keys": ["pk1", "pk2", "pk3", "pk4"],
            "pk_to_pos": {"pk1": 4, "pk2": 8, "pk3": 9, "pk4": 7},
        },
        "expected_result_after_convert": {
            "primary_keys": ["pk1", "pk2", "pk3", "pk4"],
            "pk_to_pos": {"pk1": 4, "pk2": 8, "pk3": 9, "pk4": 7},
        },
        "expected_result_main_branch": {
            "primary_keys": ["pk1", "pk2", "pk3", "pk4", "pk10", "pk11"],
            "pk_to_pos": {
                "pk1": 4,
                "pk2": 8,
                "pk3": 9,
                "pk4": 7,
                "pk10": 10,
                "pk11": 11,
            },
        },
        "verify_spark_read": True,
    },
]


@pytest.mark.parametrize("test_case", TEST_CASES)
@pytest.mark.integration
def test_converter_commit_conflict_resolution(
    test_case: Dict[str, Any],
    spark,
    session_catalog: RestCatalog,
    setup_ray_cluster,
    mocker,
    request,
) -> None:
    """
    Parameterized test for converter functionality with commit conflict resolution.
    Tests drop duplicates scenario with concurrent writes and conflict resolution.
    """
    # Get schema fixture based on test case
    schema = request.getfixturevalue(test_case["schema"])

    # Create test table
    identifier = create_test_table(
        session_catalog=session_catalog,
        namespace="default",
        table_name=test_case["table_name"],
        schema=schema,
        partition_spec=request.getfixturevalue("base_partition_spec"),
        properties=request.getfixturevalue("table_properties"),
    )

    # Insert test data
    insert_test_data(spark, identifier, test_case)

    # Get files and create convert input
    tbl = session_catalog.load_table(identifier)

    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(tbl)

    # Handle equality delete if present
    if "equality_delete_data" in test_case:
        tbl = session_catalog.load_table(identifier)
        file_prefix = get_file_prefix(tbl)
        partition_value = Record(number_partitioned=0)

        # Note: Just upload to S3 to mock input data here.
        # NOT committing to Iceberg metadata as equality delete write path not implemented in Pyiceberg/Spark.
        equality_file_list = commit_equality_delete_to_table(
            table=tbl,
            partition_value=partition_value,
            equality_delete_table=test_case["equality_delete_data"],
            file_link_prefix=file_prefix,
        )
        # Mock equality delete input to converter with latest file sequence, so equality delete can be applied to all data before
        equality_delete_dict = defaultdict()
        equality_delete_dict[partition_value] = [(4, equality_file_list[0])]

    convert_input_files_for_all_buckets = group_all_files_to_each_bucket(
        data_file_dict=data_file_dict,
        equality_delete_dict=equality_delete_dict,
        pos_delete_dict=pos_delete_dict,
    )

    s3_file_system = get_s3_file_system()
    convert_inputs = create_convert_input(
        tbl, convert_input_files_for_all_buckets, test_case, s3_file_system
    )

    # Create and set up mock data
    mock_data_tables = create_mock_data_tables(test_case)
    download_data_mock = mocker.patch(
        "deltacat.compute.converter.utils.io.daft_read_parquet"
    )

    download_data_mock.side_effect = mock_data_tables

    # Run conversion
    convert_ref = convert.remote(convert_inputs[0])
    convert_result = ray.get(convert_ref)

    # Process results
    to_be_deleted_files_list, to_be_added_files_list = process_convert_result(
        convert_result
    )

    # Create threading event to coordinate execution
    insert_complete_event = threading.Event()

    def insert_data_while_converting():
        """Insert data while converting to simulate concurrent writes.
        Note that Each INSERT INTO statement will create a snapshot."""

        run_spark_commands(
            spark,
            [
                f"""
                    INSERT INTO {identifier} VALUES (0, "pk10", "path10", 10)
                    """
            ],
        )
        run_spark_commands(
            spark,
            [
                f"""
                    INSERT INTO {identifier} VALUES (0, "pk11", "path11", 11)
                    """
            ],
        )
        print("Thread: Completed insert operation")
        # Signal that insert is complete
        insert_complete_event.set()

    # Start the insert operation in a separate thread
    insert_thread = threading.Thread(target=insert_data_while_converting)
    insert_thread.start()

    # Wait for insert to complete before proceeding with commit
    # This ensures the table state changes but main thread has stale metadata
    insert_complete_event.wait()
    print("Main thread: Insert completed, proceeding with commit using stale metadata")

    # Run commit_append_snapshot in the main thread with stale table metadata
    commit_exception = None
    try:
        commit_append_snapshot(
            iceberg_table=tbl,  # This has stale metadata since we didn't refresh after insert
            new_position_delete_files=to_be_added_files_list,
        )
    except CommitFailedException as e:
        commit_exception = e

    # Assert that CommitFailedException was thrown as expected
    assert (
        commit_exception is not None
    ), "Expected CommitFailedException to be thrown due to concurrent insert, but no exception occurred"
    assert isinstance(
        commit_exception, CommitFailedException
    ), f"Expected CommitFailedException, but got {type(commit_exception)}"

    match = re.search(r"expected id (\d+) != (\d+)", str(commit_exception))
    if match:
        expected_snapshot_id = match.group(1)
        appended_snapshot_id = match.group(2)

    run_spark_commands(
        spark,
        [
            f"""
            ALTER TABLE {identifier} CREATE BRANCH merge
        """,
        ],
    )

    spark.sql(
        f"""
            CALL integration.system.rollback_to_snapshot('{identifier}', {expected_snapshot_id})
        """
    )

    # Refresh the table to get the latest state after rollback
    tbl.refresh()
    metadata, converter_commit_uuid = commit_append_snapshot(
        iceberg_table=tbl,
        new_position_delete_files=to_be_added_files_list,
    )

    # Cherry-pick all snapshots between (expected_snapshot_id, appended_snapshot_id]
    # First, get all snapshots in the table to find the range
    tbl.refresh()
    snapshots = list(tbl.metadata.snapshots)

    # Find the sequence numbers for the expected and appended snapshot IDs
    expected_id = int(expected_snapshot_id)
    appended_id = int(appended_snapshot_id)

    expected_sequence_number = None
    appended_sequence_number = None

    # Map snapshot IDs to their sequence numbers
    for snapshot in snapshots:
        if snapshot.snapshot_id == expected_id:
            expected_sequence_number = snapshot.sequence_number
        if snapshot.snapshot_id == appended_id:
            appended_sequence_number = snapshot.sequence_number

    # Find snapshots in the range (expected_sequence_number, appended_sequence_number]
    snapshots_to_cherry_pick = []
    for snapshot in snapshots:
        if (
            expected_sequence_number
            < snapshot.sequence_number
            <= appended_sequence_number
        ):
            snapshots_to_cherry_pick.append(snapshot.snapshot_id)

    # Sort snapshots by sequence number to cherry-pick them in order
    snapshots_to_cherry_pick.sort(
        key=lambda sid: next(
            s.sequence_number for s in snapshots if s.snapshot_id == sid
        )
    )

    # Cherry-pick each snapshot in the range
    for snapshot_id in snapshots_to_cherry_pick:
        spark.sql(
            f"""
            CALL integration.system.cherrypick_snapshot('{identifier}', {snapshot_id})
            """
        )

    # Verify results for both converter commit and main branch
    tbl.refresh()

    # pyiceberg_scan_table_rows: Expected to contain only the converter's results (pk1-pk4)
    # This scans the specific snapshot created by the converter commit, which should only
    # include the original data processed by the converter (drop duplicates result)
    pyiceberg_scan_table_rows = (
        tbl.scan(snapshot_id=converter_commit_uuid).to_arrow().to_pydict()
    )

    # pyiceberg_scan_table_rows_main: Expected to contain all data including cherry-picked inserts (pk1-pk4, pk10, pk11)
    # This scans the current main branch which includes both the converter's work and the
    # concurrent insertions that were cherry-picked after conflict resolution
    pyiceberg_scan_table_rows_main = tbl.scan().to_arrow().to_pydict()

    # Verify Spark read results
    verify_spark_read_results(
        spark,
        identifier,
        test_case["expected_result_after_convert"],
        snapshot_id=converter_commit_uuid,
    )
    verify_spark_read_results(
        spark, identifier, test_case["expected_result_main_branch"]
    )

    # Verify PyIceberg scan results
    verify_result(
        pyiceberg_scan_table_rows,
        test_case["expected_result_after_convert"],
        verify_pos_index=test_case.get("verify_pos_index", False),
    )
    verify_result(
        pyiceberg_scan_table_rows_main,
        test_case["expected_result_main_branch"],
        verify_pos_index=test_case.get("verify_pos_index", False),
    )
