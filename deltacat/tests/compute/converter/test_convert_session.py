import pytest
from typing import List
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.expressions import EqualTo


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
