from typing import Dict

import pytest
from deltacat.io.read_api import read_iceberg
from pyarrow.fs import S3FileSystem


@pytest.fixture()
def catalog_properties() -> Dict[str, str]:
    return {
        "type": "rest",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }


@pytest.fixture()
def mock_s3_file_system() -> S3FileSystem:
    return S3FileSystem(
        access_key="admin",
        secret_key="password",
        endpoint_override="http://localhost:9000",
    )


def test_read_all_types(
    catalog_properties: Dict[str, str], mock_s3_file_system: S3FileSystem
) -> None:
    table_name = "default.test_all_types"
    catalog_name = "local"
    ray_dataset = read_iceberg(
        table_name,
        catalog_name=catalog_name,
        catalog_properties=catalog_properties,
        filesystem=mock_s3_file_system,
    )
    df = ray_dataset.limit(100).to_pandas(limit=100)
    assert len(df) == 5
    # check that the columns are in the right order
    assert list(df.columns) == [
        "longCol",
        "intCol",
        "floatCol",
        "doubleCol",
        "dateCol",
        "timestampCol",
        "stringCol",
        "booleanCol",
        "binaryCol",
        "byteCol",
        "decimalCol",
        "shortCol",
        "mapCol",
        "arrayCol",
        "structCol",
    ]


def test_read_null_nan(
    catalog_properties: Dict[str, str], mock_s3_file_system: S3FileSystem
) -> None:
    table_name = "default.test_null_nan"
    catalog_name = "local"
    ray_dataset = read_iceberg(
        table_name,
        catalog_name=catalog_name,
        catalog_properties=catalog_properties,
        filesystem=mock_s3_file_system,
    )
    df = ray_dataset.limit(100).to_pandas(limit=100)
    assert len(df) == 3
    assert list(df.columns) == ["idx", "col_numeric"]


def test_read_deletes(
    catalog_properties: Dict[str, str], mock_s3_file_system: S3FileSystem
) -> None:
    table_name = "default.test_deletes"
    catalog_name = "local"
    ray_dataset = read_iceberg(
        table_name,
        catalog_name=catalog_name,
        catalog_properties=catalog_properties,
        filesystem=mock_s3_file_system,
    )
    df = ray_dataset.limit(100).to_pandas(limit=100)
    assert len(df) == 1
    assert list(df.columns) == ["idx", "deleted"]
