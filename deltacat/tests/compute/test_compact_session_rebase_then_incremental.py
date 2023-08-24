import ray
from moto import mock_s3
import pytest
import boto3
from boto3.resources.base import ServiceResource
import pyarrow as pa
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.common import (
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

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)
