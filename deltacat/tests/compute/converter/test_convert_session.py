from collections import defaultdict
import pytest
import ray
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
    commit_replace_snapshot,
)

from pyiceberg.typedef import Record
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


def verify_spark_read_results(spark, identifier, expected_result):
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
    },
    {
        "name": "multi_key_drop_duplicates",
        "table_name": "table_converter_ray_pos_delete_multiple_identifier_fields",
        "schema": "multi_key_schema_without_file_path",
        "identifier_fields": ["primary_key1", "primary_key2"],
        "mock_data": [
            {"primary_key1": ["pk1", "pk2", "pk3"], "primary_key2": [1, 2, 3]},
            {"primary_key1": ["pk1", "pk2", "pk3"], "primary_key2": [1, 2, 3]},
            {"primary_key1": ["pk4", "pk2", "pk3"], "primary_key2": [1, 3, 4]},
        ],
        "expected_result": {
            "pk_tuples": [
                ("pk1", 1),
                ("pk2", 2),
                ("pk2", 3),
                ("pk3", 3),
                ("pk3", 4),
                ("pk4", 1),
            ]
        },
    },
    {
        "name": "equality_delete",
        "table_name": "table_converter_ray_equality_delete_success",
        "schema": "base_schema_without_metadata",
        "identifier_fields": ["primary_key"],
        "mock_data": [
            {"primary_key": ["pk1", "pk2", "pk3"]},
            {"primary_key": ["pk1", "pk2", "pk3"]},
            {"primary_key": ["pk4", "pk2", "pk3"]},
        ],
        "equality_delete_data_mock": [{"primary_key": ["pk1"]}],
        "equality_delete_data": pa.Table.from_arrays(["pk1"], names=["primary_key"]),
        "verify_spark_read": True,
        "expected_result": {"primary_keys": ["pk2", "pk3", "pk4"]},
    },
    {
        "name": "position_delete",
        "table_name": "table_converter_ray_position_delete_success",
        "schema": "base_schema_without_metadata",
        "identifier_fields": ["primary_key"],
        "mock_data": [
            {"primary_key": ["pk1", "pk2", "pk3"]},
            {"primary_key": ["pk1", "pk2", "pk3"]},
            {"primary_key": ["pk4", "pk2", "pk3"]},
        ],
        "expected_result": {"primary_keys": ["pk1", "pk2", "pk3", "pk4"]},
    },
    {
        "name": "position_delete_read_by_spark",
        "table_name": "table_converter_ray_pos_delete_read_by_spark_success",
        "schema": "base_schema_without_metadata",
        "identifier_fields": ["primary_key"],
        "mock_data": [
            {"primary_key": ["pk1", "pk2", "pk3"]},
            {"primary_key": ["pk1", "pk2", "pk3"]},
            {"primary_key": ["pk4", "pk2", "pk3"]},
        ],
        "expected_result": {"primary_keys": ["pk1", "pk2", "pk3", "pk4"]},
        "verify_spark_read": True,
        "expected_spark_count": 4,
    },
]


@pytest.mark.parametrize("test_case", TEST_CASES)
@pytest.mark.integration
def test_converter(
    test_case: Dict[str, Any],
    spark,
    session_catalog: RestCatalog,
    setup_ray_cluster,
    mocker,
    request,
) -> None:
    """
    Parameterized test for converter functionality.
    Tests drop duplicates, equality delete, and position delete scenarios.
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

    if not to_be_deleted_files_list:
        # Commit changes
        commit_append_snapshot(
            iceberg_table=tbl,
            new_position_delete_files=to_be_added_files_list,
        )
    else:
        commit_replace_snapshot(
            iceberg_table=tbl,
            to_be_deleted_files=to_be_deleted_files_list[0],
            new_position_delete_files=to_be_added_files_list,
        )
    tbl.refresh()

    # Verify results
    pyiceberg_scan_table_rows = tbl.scan().to_arrow().to_pydict()

    # Verify Spark read if required
    if test_case.get("verify_spark_read", False):
        verify_spark_read_results(spark, identifier, test_case["expected_result"])
    else:
        verify_result(
            pyiceberg_scan_table_rows,
            test_case["expected_result"],
            verify_pos_index=test_case.get("verify_pos_index", False),
        )
