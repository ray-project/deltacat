import unittest
from deltacat.aws.s3u import UuidBlockWritePathProvider, CapturedBlockWritePaths


import os
from unittest import mock
from unittest.mock import patch

import boto3
import pytest
from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from deltacat.exceptions import NonRetryableError
from moto import mock_s3
from tenacity import RetryError

from deltacat.aws import s3u

TEST_S3_BUCKET_NAME = "TEST_S3_BUCKET"
TEST_S3_KEY = "TEST_S3_KEY"


class TestUuidBlockWritePathProvider(unittest.TestCase):
    def test_uuid_block_write_provider_sanity(self):
        capture_object = CapturedBlockWritePaths()
        provider = UuidBlockWritePathProvider(capture_object=capture_object)

        result = provider("base_path")

        self.assertRegex(result, r"^base_path/[\w-]{36}$")


@pytest.fixture(autouse=True, scope="module")
def mock_aws_credential():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield


@pytest.fixture(scope="module")
def setup_s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(autouse=True, scope="module")
def setup_test_s3_bucket(setup_s3_resource: ServiceResource):
    setup_s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_BUCKET_NAME,
    )
    yield


def test_sanity(setup_test_s3_bucket):
    uri = f"s3://{TEST_S3_BUCKET_NAME}/{TEST_S3_KEY}"
    body = "test-body"
    uploaded_file = s3u.upload(uri, body)
    assert uploaded_file is not None
    assert uploaded_file["ResponseMetadata"]["HTTPStatusCode"] == 200
    downloaded_file = s3u.download(uri)
    downloaded_body = downloaded_file["Body"].read().decode("utf-8")
    assert downloaded_file["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert downloaded_body == body


@patch("deltacat.aws.s3u.RETRY_STOP_AFTER_DELAY", 1)
@patch("deltacat.aws.s3u.s3_client_cache")
def test_upload_throttled(mock_s3_client_cache, setup_test_s3_bucket):
    uri = f"s3://{TEST_S3_BUCKET_NAME}/{TEST_S3_KEY}"
    body = "test-body"
    err = ClientError({"Error": {"Code": "NoSuchKey"}}, "put_object")
    mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
    mock_s3.put_object.side_effect = err
    with pytest.raises(RetryError):
        s3u.upload(uri, body)
    assert mock_s3.put_object.call_count > 1


@patch("deltacat.aws.s3u.s3_client_cache")
def test_upload_unexpected_error_code(mock_s3_client_cache, setup_test_s3_bucket):
    uri = f"s3://{TEST_S3_BUCKET_NAME}/{TEST_S3_KEY}"
    body = "test-body"
    err = ClientError({"Error": {"Code": "UnexpectedError"}}, "put_object")
    mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
    mock_s3.put_object.side_effect = err
    file = None
    with pytest.raises(NonRetryableError):
        s3u.upload(uri, body)
    assert file is None
    assert mock_s3.put_object.call_count == 1


@patch("deltacat.aws.s3u.RETRY_STOP_AFTER_DELAY", 1)
@patch("deltacat.aws.s3u.s3_client_cache")
def test_download_throttled(mock_s3_client_cache, setup_test_s3_bucket):
    uri = f"s3://{TEST_S3_BUCKET_NAME}/{TEST_S3_KEY}"
    err = ClientError({"Error": {"Code": "ReadTimeoutError"}}, "put_object")
    mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
    mock_s3.get_object.side_effect = err
    file = None
    with pytest.raises(RetryError):
        file = s3u.download(uri)
    assert file is None
    assert mock_s3.get_object.call_count > 1


def test_download_not_exists(setup_test_s3_bucket):
    uri = f"s3://{TEST_S3_BUCKET_NAME}/key-not-exists"
    file = None
    with pytest.raises(NonRetryableError):
        file = s3u.download(uri)
    assert file is None

    file = s3u.download(uri, fail_if_not_found=False)
    assert file is None


@patch("deltacat.aws.s3u.s3_client_cache")
def test_download_unexpected_error_code(mock_s3_client_cache, setup_test_s3_bucket):
    uri = f"s3://{TEST_S3_BUCKET_NAME}/key-not-exists"
    err = ClientError({"Error": {"Code": "UnexpectedError"}}, "put_object")
    mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
    mock_s3.get_object.side_effect = err
    file = None
    with pytest.raises(NonRetryableError):
        file = s3u.download(uri)
    assert file is None
    assert mock_s3.get_object.call_count == 1
