import pytest
import ray
from typing import List
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import EqualTo
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
)
from deltacat.compute.converter.pyiceberg.update_snapshot_overrides import (
    commit_append_snapshot,
)


def run_spark_commands(spark, sqls: List[str]) -> None:
    for sql in sqls:
        spark.sql(sql)


@pytest.mark.integration
def test_pyiceberg_spark_setup_sanity(spark, session_catalog: RestCatalog) -> None:
    """
    This Test was copied over from Pyiceberg integ test: https://github.com/apache/iceberg-python/blob/main/tests/integration/test_deletes.py#L62
    First sanity check to ensure all integration with Pyiceberg and Spark are working as expected.
    """
    identifier = "default.table_partitioned_delete"

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned  int,
                number              int
            )
            USING iceberg
            PARTITIONED BY (number_partitioned)
            TBLPROPERTIES('format-version' = 2)
        """,
            f"""
            INSERT INTO {identifier} VALUES (10, 20), (10, 30)
        """,
            f"""
            INSERT INTO {identifier} VALUES (11, 20), (11, 30)
        """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.delete(EqualTo("number_partitioned", 10))

    # No overwrite operation
    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == [
        "append",
        "append",
        "delete",
    ]
    assert tbl.scan().to_arrow().to_pydict() == {
        "number_partitioned": [11, 11],
        "number": [20, 30],
    }


@pytest.mark.integration
def test_spark_position_delete_production_sanity(
    spark, session_catalog: RestCatalog
) -> None:
    """
    Sanity test to ensure Spark position delete production is successful with `merge-on-read` spec V2.
    Table has two partition levels. 1. BucketTransform on primary key
    """
    identifier = "default.table_spark_position_delete_production_sanity"

    run_spark_commands(
        spark,
        [
            f"DROP TABLE IF EXISTS {identifier}",
            f"""
            CREATE TABLE {identifier} (
                number_partitioned INT,
                primary_key STRING
            )
            USING iceberg
            PARTITIONED BY (bucket(3, primary_key), number_partitioned)
            TBLPROPERTIES(
                'format-version' = 2,
                'write.delete.mode'='merge-on-read',
                'write.update.mode'='merge-on-read',
                'write.merge.mode'='merge-on-read'
            )
            """,
            f"""
            INSERT INTO {identifier} VALUES (0, 'pk1'), (0, 'pk2'), (0, 'pk3')
            """,
            f"""
            INSERT INTO {identifier} VALUES (1, 'pk1'), (1, 'pk2'), (1, 'pk3')
            """,
        ],
    )

    run_spark_commands(
        spark,
        [
            f"""
                DELETE FROM {identifier} WHERE primary_key in ("pk1")
            """,
        ],
    )

    tbl = session_catalog.load_table(identifier)
    tbl.refresh()

    assert [snapshot.summary.operation.value for snapshot in tbl.snapshots()] == [
        "append",
        "append",
        "delete",
    ]

    assert tbl.scan().to_arrow().to_pydict() == {
        "number_partitioned": [1, 1, 0, 0],
        "primary_key": ["pk2", "pk3", "pk2", "pk3"],
    }


@pytest.mark.integration
def test_converter_drop_duplicates_success(
    spark, session_catalog: RestCatalog, setup_ray_cluster, mocker
) -> None:
    """
    Test for convert compute remote function happy case. Download file results are mocked.
    """

    # 1. Create Iceberg table
    namespace = "default"
    table_name = "table_converter_ray_pos_delete_drop_duplicates_compute"
    identifier = f"{namespace}.{table_name}"

    schema = Schema(
        NestedField(
            field_id=1, name="number_partitioned", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2, name="primary_key", field_type=StringType(), required=False
        ),
        # Explicitly define "file_path" and "pos" for assertion of deterministic record after dedupe
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

    partition_field_identity = PartitionField(
        source_id=1,
        field_id=101,
        transform=IdentityTransform(),
        name="number_partitioned",
    )
    partition_spec = PartitionSpec(partition_field_identity)

    properties = dict()
    properties["write.format.default"] = "parquet"
    properties["write.delete.mode"] = "merge-on-read"
    properties["write.update.mode"] = "merge-on-read"
    properties["write.merge.mode"] = "merge-on-read"
    properties["format-version"] = "2"

    drop_table_if_exists(identifier, session_catalog)
    session_catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=partition_spec,
        properties=properties,
    )

    # 2. Use Spark to generate initial data files
    tbl = session_catalog.load_table(identifier)
    tbl.refresh()
    run_spark_commands(
        spark,
        [
            f"""
            INSERT INTO {identifier} VALUES (0, "pk1", "path1", 1), (0, "pk2", "path2", 2), (0, "pk3", "path3", 3)
            """
        ],
    )
    run_spark_commands(
        spark,
        [
            f"""
                INSERT INTO {identifier} VALUES (0, "pk1", "path1", 4), (0, "pk2", "path2", 5), (0, "pk3", "path3", 6)
                """
        ],
    )
    run_spark_commands(
        spark,
        [
            f"""
                INSERT INTO {identifier} VALUES (0, "pk4", "path4", 7), (0, "pk2", "path2", 8), (0, "pk3", "path3", 9)
                """
        ],
    )

    tbl = session_catalog.load_table(identifier)
    # 3. Use convert.remote() function to compute position deletes
    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(tbl)

    convert_input_files_for_all_buckets = group_all_files_to_each_bucket(
        data_file_dict=data_file_dict,
        equality_delete_dict=equality_delete_dict,
        pos_delete_dict=pos_delete_dict,
    )

    s3_file_system = get_s3_file_system()
    for i, one_bucket_files in enumerate(convert_input_files_for_all_buckets):
        convert_input = ConvertInput.of(
            convert_input_files=one_bucket_files,
            convert_task_index=i,
            iceberg_table_warehouse_prefix="warehouse/default",
            identifier_fields=["primary_key"],
            table_io=tbl.io,
            table_metadata=tbl.metadata,
            compact_previous_position_delete_files=False,
            enforce_primary_key_uniqueness=True,
            position_delete_for_multiple_data_files=True,
            max_parallel_data_file_download=10,
            s3_file_system=s3_file_system,
            s3_client_kwargs={},
        )

    number_partitioned_array_1 = pa.array([0, 0, 0], type=pa.int32())
    primary_key_array_1 = pa.array(["pk1", "pk2", "pk3"])
    names = ["number_partitioned", "primary_key"]
    data_table_1 = pa.Table.from_arrays(
        [number_partitioned_array_1, primary_key_array_1], names=names
    )

    number_partitioned_array_2 = pa.array([0, 0, 0], type=pa.int32())
    primary_key_array_2 = pa.array(["pk1", "pk2", "pk3"])
    names = ["number_partitioned", "primary_key"]
    data_table_2 = pa.Table.from_arrays(
        [number_partitioned_array_2, primary_key_array_2], names=names
    )

    number_partitioned_array_3 = pa.array([0, 0, 0], type=pa.int32())
    primary_key_array_3 = pa.array(["pk4", "pk2", "pk3"])
    names = ["number_partitioned", "primary_key"]
    data_table_3 = pa.Table.from_arrays(
        [number_partitioned_array_3, primary_key_array_3], names=names
    )

    daft_df_1 = daft.from_arrow(data_table_1)
    daft_df_2 = daft.from_arrow(data_table_2)
    daft_df_3 = daft.from_arrow(data_table_3)

    download_data_mock = mocker.patch(
        "deltacat.compute.converter.utils.io.daft_read_parquet"
    )
    download_data_mock.side_effect = (daft_df_1, daft_df_2, daft_df_3)

    convert_ref = convert.remote(convert_input)

    to_be_deleted_files_list = []

    convert_result = ray.get(convert_ref)

    to_be_added_files_list = []
    # Check if there're files to delete
    if convert_result.to_be_deleted_files:
        to_be_deleted_files_list.extend(convert_result.to_be_deleted_files.values())
    if convert_result.to_be_added_files:
        to_be_added_files_list.extend(convert_result.to_be_added_files)

    commit_append_snapshot(
        iceberg_table=tbl,
        new_position_delete_files=to_be_added_files_list,
    )
    tbl.refresh()

    # 5. Only primary key 2 and 3 should exist in table, as primary key 1 is deleted.
    pyiceberg_scan_table_rows = tbl.scan().to_arrow().to_pydict()

    # Only one unique record for each pk exists
    all_pk = sorted(pyiceberg_scan_table_rows["primary_key"])
    assert all_pk == ["pk1", "pk2", "pk3", "pk4"]

    # Expected unique record to keep for each pk
    expected_pk_to_pos_mapping = {"pk1": 4, "pk2": 8, "pk3": 9, "pk4": 7}
    for pk, pos in zip(
        pyiceberg_scan_table_rows["primary_key"], pyiceberg_scan_table_rows["pos"]
    ):
        assert pos == expected_pk_to_pos_mapping[pk]


@pytest.mark.integration
def test_converter_pos_delete_read_by_spark_success(
    spark, session_catalog: RestCatalog, setup_ray_cluster, mocker
) -> None:
    """
    Test for convert compute remote function happy case. Download file results are mocked.
    """

    # 1. Create Iceberg table
    namespace = "default"
    table_name = "table_converter_ray_pos_delete_read_by_spark_success"
    identifier = f"{namespace}.{table_name}"

    schema = Schema(
        NestedField(
            field_id=1, name="number_partitioned", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=2, name="primary_key", field_type=StringType(), required=False
        ),
        schema_id=0,
    )

    partition_field_identity = PartitionField(
        source_id=1,
        field_id=101,
        transform=IdentityTransform(),
        name="number_partitioned",
    )
    partition_spec = PartitionSpec(partition_field_identity)

    properties = dict()
    properties["write.format.default"] = "parquet"
    properties["write.delete.mode"] = "merge-on-read"
    properties["write.update.mode"] = "merge-on-read"
    properties["write.merge.mode"] = "merge-on-read"
    properties["format-version"] = "2"

    drop_table_if_exists(identifier, session_catalog)
    session_catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=partition_spec,
        properties=properties,
    )

    # 2. Use Spark to generate initial data files
    tbl = session_catalog.load_table(identifier)

    run_spark_commands(
        spark,
        [
            f"""
               INSERT INTO {identifier} VALUES (0, "pk1"), (0, "pk2"), (0, "pk3")
               """
        ],
    )
    run_spark_commands(
        spark,
        [
            f"""
                   INSERT INTO {identifier} VALUES (0, "pk1"), (0, "pk2"), (0, "pk3")
                   """
        ],
    )
    run_spark_commands(
        spark,
        [
            f"""
                   INSERT INTO {identifier} VALUES (0, "pk4"), (0, "pk2"), (0, "pk3")
                   """
        ],
    )
    tbl.refresh()

    # 3. Use convert.remote() function to compute position deletes
    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(tbl)

    convert_input_files_for_all_buckets = group_all_files_to_each_bucket(
        data_file_dict=data_file_dict,
        equality_delete_dict=equality_delete_dict,
        pos_delete_dict=pos_delete_dict,
    )

    s3_file_system = get_s3_file_system()
    for i, one_bucket_files in enumerate(convert_input_files_for_all_buckets):
        convert_input = ConvertInput.of(
            convert_input_files=one_bucket_files,
            convert_task_index=i,
            iceberg_table_warehouse_prefix="warehouse/default",
            identifier_fields=["primary_key"],
            table_io=tbl.io,
            table_metadata=tbl.metadata,
            compact_previous_position_delete_files=False,
            enforce_primary_key_uniqueness=True,
            position_delete_for_multiple_data_files=True,
            max_parallel_data_file_download=10,
            s3_file_system=s3_file_system,
            s3_client_kwargs={},
        )

    primary_key_array_1 = pa.array(["pk1", "pk2", "pk3"])
    names = ["primary_key"]
    data_table_1 = pa.Table.from_arrays([primary_key_array_1], names=names)

    primary_key_array_2 = pa.array(["pk1", "pk2", "pk3"])
    names = ["primary_key"]
    data_table_2 = pa.Table.from_arrays([primary_key_array_2], names=names)

    primary_key_array_3 = pa.array(["pk4", "pk2", "pk3"])
    names = ["primary_key"]
    data_table_3 = pa.Table.from_arrays([primary_key_array_3], names=names)

    daft_df_1 = daft.from_arrow(data_table_1)
    daft_df_2 = daft.from_arrow(data_table_2)
    daft_df_3 = daft.from_arrow(data_table_3)

    download_data_mock = mocker.patch(
        "deltacat.compute.converter.utils.io.daft_read_parquet"
    )
    download_data_mock.side_effect = (daft_df_1, daft_df_2, daft_df_3)

    convert_ref = convert.remote(convert_input)

    to_be_deleted_files_list = []
    to_be_added_files_list = []
    convert_result = ray.get(convert_ref)

    if convert_result.to_be_deleted_files:
        to_be_deleted_files_list.extend(convert_result.to_be_deleted_files.values())
    if convert_result.to_be_added_files:
        to_be_added_files_list.extend(convert_result.to_be_added_files)

    # 4. Commit position delete, delete equality deletes from table
    commit_append_snapshot(
        iceberg_table=tbl,
        new_position_delete_files=to_be_added_files_list,
    )
    tbl.refresh()

    # 5. Result assertion: Spark read table contains unique primary key
    spark_read_pos_delete = spark.sql(f"select * from {identifier}").collect()
    all_pk = [
        spark_read_pos_delete[row_idx][1]
        for row_idx in range(len(spark_read_pos_delete))
    ]
    all_pk_sorted = sorted(all_pk)
    assert all_pk_sorted == ["pk1", "pk2", "pk3", "pk4"]


@pytest.mark.integration
def test_converter_pos_delete_multiple_identifier_fields_success(
    spark, session_catalog: RestCatalog, setup_ray_cluster, mocker
) -> None:
    """
    Test for convert compute remote function happy case. Download file results are mocked.
    """

    # 1. Create Iceberg table
    namespace = "default"
    table_name = "table_converter_ray_pos_delete_multiple_identifier_fields"

    identifier = f"{namespace}.{table_name}"

    schema = Schema(
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

    partition_field_identity = PartitionField(
        source_id=1,
        field_id=101,
        transform=IdentityTransform(),
        name="number_partitioned",
    )
    partition_spec = PartitionSpec(partition_field_identity)

    properties = dict()
    properties["write.format.default"] = "parquet"
    properties["write.delete.mode"] = "merge-on-read"
    properties["write.update.mode"] = "merge-on-read"
    properties["write.merge.mode"] = "merge-on-read"
    properties["format-version"] = "2"

    drop_table_if_exists(identifier, session_catalog)
    session_catalog.create_table(
        identifier,
        schema=schema,
        partition_spec=partition_spec,
        properties=properties,
    )

    # 2. Use Spark to generate initial data files
    tbl = session_catalog.load_table(identifier)

    run_spark_commands(
        spark,
        [
            f"""
               INSERT INTO {identifier} VALUES (0, "pk1", 1), (0, "pk2", 2), (0, "pk3", 3)
               """
        ],
    )
    run_spark_commands(
        spark,
        [
            f"""
               INSERT INTO {identifier} VALUES (0, "pk1", 1), (0, "pk2", 2), (0, "pk3", 3)
               """
        ],
    )
    run_spark_commands(
        spark,
        [
            f"""
               INSERT INTO {identifier} VALUES (0, "pk4", 1), (0, "pk2", 3), (0, "pk3", 4)
               """
        ],
    )
    tbl.refresh()

    # 3. Use convert.remote() function to compute position deletes
    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(tbl)

    convert_input_files_for_all_buckets = group_all_files_to_each_bucket(
        data_file_dict=data_file_dict,
        equality_delete_dict=equality_delete_dict,
        pos_delete_dict=pos_delete_dict,
    )

    s3_file_system = get_s3_file_system()
    for i, one_bucket_files in enumerate(convert_input_files_for_all_buckets):
        convert_input = ConvertInput.of(
            convert_input_files=one_bucket_files,
            convert_task_index=i,
            iceberg_table_warehouse_prefix="warehouse/default",
            identifier_fields=["primary_key1", "primary_key2"],
            table_io=tbl.io,
            table_metadata=tbl.metadata,
            compact_previous_position_delete_files=False,
            enforce_primary_key_uniqueness=True,
            position_delete_for_multiple_data_files=True,
            max_parallel_data_file_download=10,
            s3_file_system=s3_file_system,
            s3_client_kwargs={},
        )

    names = ["primary_key1", "primary_key2"]

    primary_key1_array_1 = pa.array(["pk1", "pk2", "pk3"])
    primary_key2_array_1 = pa.array([1, 2, 3])
    data_table_1 = pa.Table.from_arrays(
        [primary_key1_array_1, primary_key2_array_1], names=names
    )

    primary_key1_array_2 = pa.array(["pk1", "pk2", "pk3"])
    primary_key2_array_2 = pa.array([1, 2, 3])
    data_table_2 = pa.Table.from_arrays(
        [primary_key1_array_2, primary_key2_array_2], names=names
    )

    primary_key1_array_3 = pa.array(["pk4", "pk2", "pk3"])
    primary_key2_array_3 = pa.array([1, 3, 4])
    data_table_3 = pa.Table.from_arrays(
        [primary_key1_array_3, primary_key2_array_3], names=names
    )

    daft_df_1 = daft.from_arrow(data_table_1)
    daft_df_2 = daft.from_arrow(data_table_2)
    daft_df_3 = daft.from_arrow(data_table_3)

    download_data_mock = mocker.patch(
        "deltacat.compute.converter.utils.io.daft_read_parquet"
    )
    download_data_mock.side_effect = (daft_df_1, daft_df_2, daft_df_3)

    convert_ref = convert.remote(convert_input)

    to_be_deleted_files_list = []
    to_be_added_files_list = []
    convert_result = ray.get(convert_ref)

    if convert_result.to_be_deleted_files:
        to_be_deleted_files_list.extend(convert_result.to_be_deleted_files.values())
    if convert_result.to_be_added_files:
        to_be_added_files_list.extend(convert_result.to_be_added_files)

    # 4. Commit position delete, delete equality deletes from table

    commit_append_snapshot(
        iceberg_table=tbl,
        new_position_delete_files=to_be_added_files_list,
    )
    tbl.refresh()

    # 5. Result assertion: Expected unique primary keys to be kept
    pyiceberg_scan_table_rows = tbl.scan().to_arrow().to_pydict()
    expected_result_tuple_list = [
        ("pk1", 1),
        ("pk2", 2),
        ("pk2", 3),
        ("pk3", 3),
        ("pk3", 4),
        ("pk4", 1),
    ]
    pk_combined_res = []
    for pk1, pk2 in zip(
        pyiceberg_scan_table_rows["primary_key1"],
        pyiceberg_scan_table_rows["primary_key2"],
    ):
        pk_combined_res.append((pk1, pk2))

    # Assert elements are same disregard ordering in list
    assert sorted(pk_combined_res) == sorted(expected_result_tuple_list)
