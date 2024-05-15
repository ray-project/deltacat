import unittest
from deltacat.aws.s3u import UuidBlockWritePathProvider, CapturedBlockWritePaths


import os
from unittest import mock
from unittest.mock import patch

import boto3
import pytest
from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError, NoCredentialsError
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


class TestDownloadUpload(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def mock_aws_credential(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        yield

    @pytest.fixture(autouse=True)
    def setup_s3_resource(self):
        with mock_s3():
            yield boto3.resource("s3")

    @pytest.fixture(autouse=True)
    def setup_test_s3_bucket(self, setup_s3_resource: ServiceResource):
        setup_s3_resource.create_bucket(
            ACL="authenticated-read",
            Bucket=TEST_S3_BUCKET_NAME,
        )
        yield

    def test_sanity(self):
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
    def test_upload_throttled(self, mock_s3_client_cache):
        uri = f"s3://{TEST_S3_BUCKET_NAME}/{TEST_S3_KEY}"
        body = "test-body"
        throttling_err = ClientError({"Error": {"Code": "Throttling"}}, "put_object")
        mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
        mock_s3.put_object.side_effect = throttling_err
        with pytest.raises(RetryError):
            s3u.upload(uri, body)

        slowdown_err = ClientError({"Error": {"Code": "SlowDown"}}, "put_object")
        mock_s3.put_object.side_effect = slowdown_err
        with pytest.raises(RetryError):
            s3u.upload(uri, body)

        no_credentials_err = NoCredentialsError()
        mock_s3.put_object.side_effect = no_credentials_err
        with pytest.raises(RetryError):
            s3u.upload(uri, body)

        assert mock_s3.put_object.call_count > 3

    @patch("deltacat.aws.s3u.s3_client_cache")
    def test_upload_unexpected_error_code(self, mock_s3_client_cache):
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
    def test_download_throttled(self, mock_s3_client_cache):
        uri = f"s3://{TEST_S3_BUCKET_NAME}/{TEST_S3_KEY}"
        no_credentials_err = NoCredentialsError()
        mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
        mock_s3.get_object.side_effect = no_credentials_err
        file = None
        with pytest.raises(RetryError):
            file = s3u.download(uri)
        assert file is None
        assert mock_s3.get_object.call_count > 1

    def test_download_not_exists(self):
        uri = f"s3://{TEST_S3_BUCKET_NAME}/key-not-exists"
        file = None
        with pytest.raises(NonRetryableError):
            file = s3u.download(uri)
        assert file is None

        file = s3u.download(uri, fail_if_not_found=False)
        assert file is None
