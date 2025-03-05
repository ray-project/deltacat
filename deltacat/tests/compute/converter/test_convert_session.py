import pytest
import ray
from typing import List
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import EqualTo
from deltacat.tests.compute.converter.utils import commit_equality_delete_to_table
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
import pyarrow as pa
from pyiceberg.typedef import Record
from deltacat.compute.converter.steps.convert import convert
from deltacat.compute.converter.model.convert_input import ConvertInput
from deltacat.compute.converter.pyiceberg.overrides import (
    fetch_all_bucket_files,
    parquet_files_dict_to_iceberg_data_files,
)
from collections import defaultdict
from deltacat.compute.converter.utils.converter_session_utils import (
    check_data_files_sequence_number,
)
from deltacat.tests.compute.converter.utils import (
    get_s3_file_system,
    drop_table_if_exists,
)
from deltacat.compute.converter.pyiceberg.replace_snapshot import (
    commit_overwrite_snapshot,
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
def test_converter_success(
    spark, session_catalog: RestCatalog, setup_ray_cluster, mocker
) -> None:
    """
    Test for convert compute remote function happy case. Download file results are mocked.
    """

    # 1. Create Iceberg table
    namespace = "default"
    table_name = "table_converter_ray_pos_delete_compute"
    identifier = f"{namespace}.{table_name}"

    schema = Schema(
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

    # Use Spark to generate initial data files
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

    # 3. Append to table self-generated equality delete files
    tbl = session_catalog.load_table(identifier)
    df = tbl.inspect.entries()
    data_files = df.to_pydict()["data_file"]
    file_link = data_files[0]["file_path"]
    file_prefix = "/".join(file_link.split("/")[:-1])
    file_prefix = file_prefix.split("//")[1]
    number_partitioned_array = pa.array([0], type=pa.int32())
    primary_key_array = pa.array(["pk1"])
    names = ["number_partitioned", "primary_key"]
    equality_delete_table = pa.Table.from_arrays(
        [number_partitioned_array, primary_key_array], names=names
    )

    partition_value = Record(number_partitioned=0)
    commit_equality_delete_to_table(
        table=tbl,
        partition_value=partition_value,
        equality_delete_table=equality_delete_table,
        file_link_prefix=file_prefix,
    )

    # 4. Use convert.remote() function to compute position deletes
    data_file_dict, equality_delete_dict, pos_delete_dict = fetch_all_bucket_files(tbl)

    files_for_each_bucket = defaultdict(tuple)
    for partition_value, equality_delete_file_list in equality_delete_dict.items():
        (
            result_equality_delete_file,
            result_data_file,
        ) = check_data_files_sequence_number(
            data_files_list=data_file_dict[partition_value],
            equality_delete_files_list=equality_delete_dict[partition_value],
        )
        files_for_each_bucket[partition_value] = (
            result_data_file,
            result_equality_delete_file,
            [],
        )

    s3_file_system = get_s3_file_system()
    for i, one_bucket_files in enumerate(files_for_each_bucket.items()):
        convert_input = ConvertInput.of(
            files_for_each_bucket=one_bucket_files,
            convert_task_index=i,
            iceberg_table_warehouse_prefix="warehouse/default",
            identifier_fields=["primary_key"],
            compact_small_files=False,
            position_delete_for_multiple_data_files=True,
            max_parallel_data_file_download=10,
            s3_file_system=s3_file_system,
        )

    number_partitioned_array = pa.array([0, 0, 0], type=pa.int32())
    primary_key_array = pa.array(["pk1", "pk2", "pk3"])
    names = ["number_partitioned", "primary_key"]
    data_table = pa.Table.from_arrays(
        [number_partitioned_array, primary_key_array], names=names
    )

    number_partitioned_array = pa.array([0], type=pa.int32())
    primary_key_array = pa.array(["pk1"])
    names = ["number_partitioned", "primary_key"]
    equality_delete_table = pa.Table.from_arrays(
        [number_partitioned_array, primary_key_array], names=names
    )
    download_data_mock = mocker.patch(
        "deltacat.compute.converter.steps.convert.download_parquet_with_daft_hash_applied"
    )
    download_data_mock.side_effect = (data_table, equality_delete_table)

    convert_ref = convert.remote(convert_input)

    to_be_deleted_files_list = []
    to_be_added_files_dict_list = []
    convert_result = ray.get(convert_ref)

    to_be_deleted_files_list.extend(convert_result[0].values())
    file_location = convert_result[1][partition_value][0]
    to_be_added_files = f"s3://{file_location}"

    to_be_added_files_dict = defaultdict()
    to_be_added_files_dict[partition_value] = [to_be_added_files]
    to_be_added_files_dict_list.append(to_be_added_files_dict)

    # 5. Commit position delete, delete equality deletes from table
    new_position_delete_files = parquet_files_dict_to_iceberg_data_files(
        io=tbl.io,
        table_metadata=tbl.metadata,
        files_dict_list=to_be_added_files_dict_list,
    )
    commit_overwrite_snapshot(
        iceberg_table=tbl,
        # equality_delete_files + data file that all rows are deleted
        to_be_deleted_files_list=to_be_deleted_files_list[0],
        new_position_delete_files=new_position_delete_files,
    )
    tbl.refresh()

    # 6. Only primary key 2 and 3 should exist in table, as primary key 1 is deleted.
    pyicberg_scan_table_rows = tbl.scan().to_arrow().to_pydict()["primary_key"]
    assert pyicberg_scan_table_rows == ["pk2", "pk3"]
