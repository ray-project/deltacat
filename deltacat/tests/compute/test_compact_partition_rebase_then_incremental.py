import ray
import os
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.common import (
    setup_general_source_and_destination_tables,
    setup_sort_and_partition_keys,
    PartitionKey,
    TEST_S3_RCF_BUCKET_NAME,
    BASE_TEST_SOURCE_NAMESPACE,
    BASE_TEST_SOURCE_TABLE_NAME,
    BASE_TEST_DESTINATION_NAMESPACE,
    BASE_TEST_DESTINATION_TABLE_NAME,
)
from deltacat.tests.compute.testcases import (
    INCREMENTAL_TEST_CASES,
)
from typing import Any, Dict

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)

"""
MODULE scoped fixtures
"""


@pytest.fixture(autouse=True, scope="module")
def mock_aws_credential():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield


@pytest.fixture(autouse=True, scope="module")
def cleanup_the_database_file_after_all_compaction_session_package_tests_complete():
    # make sure the database file is deleted after all the compactor package tests are completed
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


@pytest.fixture(scope="module")
def setup_s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(scope="module")
def setup_compaction_artifacts_s3_bucket(setup_s3_resource: ServiceResource):
    setup_s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


"""
FUNCTION scoped fixtures
"""


@pytest.fixture(scope="function")
def setup_local_deltacat_storage_conn(request: pytest.FixtureRequest):
    # see deltacat/tests/local_deltacat_storage/README.md for documentation
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage


def test_compact_partition_rebase_then_incremental(
    request: pytest.FixtureRequest,
    setup_s3_resource: ServiceResource,
    setup_local_deltacat_storage_conn: Dict[str, Any],
    setup_compaction_artifacts_s3_bucket: None,
):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentType
    from deltacat.storage import (
        PartitionLocator,
    )
    from deltacat.compute.compactor.model.compact_partition_params import (
        CompactPartitionParams,
    )
    from deltacat.utils.placement import (
        PlacementGroupManager,
    )
    from deltacat.compute.compactor import (
        RoundCompletionInfo,
    )

    ds_mock_kwargs = setup_local_deltacat_storage_conn
    (
        source_table_stream,
        destination_table_stream,
    ) = setup_general_source_and_destination_tables(
        source_table_version,
        destination_table_version,
        primary_keys_param,
        sort_keys,
        partition_keys,
        column_names_param,
        arrow_arrays_param,
        partition_values_param,
        ds_mock_kwargs,
    )
    ray.shutdown()
    ray.init(local_mode=True)
    assert ray.is_initialized()
