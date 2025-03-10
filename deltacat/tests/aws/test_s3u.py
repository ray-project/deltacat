import unittest

import botocore

from deltacat.aws.constants import RETRYABLE_TRANSIENT_ERRORS
from deltacat.aws.s3u import UuidBlockWritePathProvider, CapturedBlockWritePaths


import os
from unittest import mock
from unittest.mock import patch

import boto3
import pytest
from boto3.resources.base import ServiceResource
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    ReadTimeoutError,
    ConnectTimeoutError,
    HTTPClientError,
)
from ray.data.datasource import FilenameProvider
from deltacat.exceptions import NonRetryableError
from moto import mock_s3
from tenacity import RetryError

from deltacat.aws import s3u


class TestUuidBlockWritePathProvider(unittest.TestCase):
    def test_uuid_block_write_provider_sanity(self):
        capture_object = CapturedBlockWritePaths()
        provider = UuidBlockWritePathProvider(capture_object=capture_object)

        result = provider("base_path")

        self.assertTrue(isinstance(provider, FilenameProvider))
        self.assertRegex(result, r"^base_path/[\w-]{36}$")


class TestDownloadUpload(unittest.TestCase):
    TEST_S3_BUCKET_NAME = "TEST_S3_BUCKET"
    TEST_S3_KEY = "TEST_S3_KEY"

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
            Bucket=self.TEST_S3_BUCKET_NAME,
        )
        yield

    def test_sanity(self):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/{self.TEST_S3_KEY}"
        body = "test-body"
        uploaded_file = s3u.upload(uri, body)
        assert uploaded_file is not None
        assert uploaded_file["ResponseMetadata"]["HTTPStatusCode"] == 200
        downloaded_file = s3u.download(uri)
        downloaded_body = downloaded_file["Body"].read().decode("utf-8")
        assert downloaded_file["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert downloaded_body == body

    @patch("deltacat.aws.s3u.UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY", 1)
    @patch("deltacat.aws.s3u.s3_client_cache")
    def test_upload_throttled(self, mock_s3_client_cache):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/{self.TEST_S3_KEY}"
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

    @patch("deltacat.aws.s3u.UPLOAD_SLICED_TABLE_RETRY_STOP_AFTER_DELAY", 1)
    @patch("deltacat.aws.s3u.ManifestEntry")
    @patch("deltacat.aws.s3u._get_metadata")
    @patch("deltacat.aws.s3u.CapturedBlockWritePaths")
    def test_upload_sliced_table_retry(
        self,
        mock_captured_block_write_paths,
        mock_get_metadata,
        mock_manifest_entry,
    ):
        mock_manifest_entry.from_s3_obj_url.side_effect = OSError(
            "Please reduce your request rate.."
        )
        mock_get_metadata.return_value = [mock.MagicMock()]
        cbwp = CapturedBlockWritePaths()
        cbwp._write_paths = ["s3_write_path"]
        cbwp._block_refs = [mock.MagicMock()]
        mock_captured_block_write_paths.return_value = cbwp
        with pytest.raises(RetryError):
            s3u.upload_sliced_table(
                mock.MagicMock(),
                "s3-prefix",
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
                mock.MagicMock(),
            )

    @patch("deltacat.aws.s3u.UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY", 1)
    @patch("deltacat.aws.s3u.s3_client_cache")
    def test_upload_transient_error_retry(self, mock_s3_client_cache):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/{self.TEST_S3_KEY}"
        body = "test-body"
        transient_errors = [*RETRYABLE_TRANSIENT_ERRORS]
        mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()

        while transient_errors:
            err_cls = transient_errors.pop()
            err_obj = self._populate_error_by_type(err_cls)
            mock_s3.put_object.side_effect = err_obj
            with pytest.raises(RetryError):
                s3u.upload(uri, body)

        assert mock_s3.put_object.call_count > len(RETRYABLE_TRANSIENT_ERRORS)

    @patch("deltacat.aws.s3u.s3_client_cache")
    def test_upload_unexpected_error_code(self, mock_s3_client_cache):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/{self.TEST_S3_KEY}"
        body = "test-body"
        err = ClientError({"Error": {"Code": "UnexpectedError"}}, "put_object")
        mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
        mock_s3.put_object.side_effect = err
        file = None
        with pytest.raises(NonRetryableError):
            s3u.upload(uri, body)
        assert file is None
        assert mock_s3.put_object.call_count == 1

    @patch("deltacat.aws.s3u.UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY", 1)
    @patch("deltacat.aws.s3u.s3_client_cache")
    def test_download_throttled(self, mock_s3_client_cache):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/{self.TEST_S3_KEY}"
        no_credentials_err = NoCredentialsError()
        mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()
        mock_s3.get_object.side_effect = no_credentials_err
        file = None
        with pytest.raises(RetryError):
            file = s3u.download(uri)
        assert file is None
        assert mock_s3.get_object.call_count > 1

    def test_download_not_exists(self):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/key-not-exists"
        file = None
        with pytest.raises(NonRetryableError):
            file = s3u.download(uri)
        assert file is None

        file = s3u.download(uri, fail_if_not_found=False)
        assert file is None

    @patch("deltacat.aws.s3u.UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY", 1)
    @patch("deltacat.aws.s3u.s3_client_cache")
    def test_download_transient_error_retry(self, mock_s3_client_cache):
        uri = f"s3://{self.TEST_S3_BUCKET_NAME}/{self.TEST_S3_KEY}"
        transient_errors = [*RETRYABLE_TRANSIENT_ERRORS]
        mock_s3_client_cache.return_value = mock_s3 = mock.MagicMock()

        while transient_errors:
            err_cls = transient_errors.pop()
            err_obj = self._populate_error_by_type(err_cls)
            mock_s3.get_object.side_effect = err_obj
            with pytest.raises(RetryError):
                s3u.download(uri)

        assert mock_s3.get_object.call_count > len(RETRYABLE_TRANSIENT_ERRORS)

    @staticmethod
    def _populate_error_by_type(err_cls):
        if err_cls in (ReadTimeoutError, ConnectTimeoutError):
            err_obj = err_cls(endpoint_url="127.0.0.1")
        elif err_cls in (HTTPClientError, botocore.exceptions.ConnectionError):
            err_obj = err_cls(endpoint_url="127.0.0.1", error=Exception)
        else:
            err_obj = err_cls()
        return err_obj
