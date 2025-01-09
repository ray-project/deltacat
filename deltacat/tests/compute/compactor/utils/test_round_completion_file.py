import pytest
import os
from moto import mock_s3
import boto3
from boto3.resources.base import ServiceResource
from deltacat.compute.compactor.utils.round_completion_file import (
    read_round_completion_file,
    write_round_completion_file,
)
from deltacat.tests.compute.test_util_common import get_test_partition_locator
from deltacat.compute.compactor import RoundCompletionInfo

RCF_BUCKET_NAME = "rcf-bucket"


@pytest.fixture(autouse=True, scope="module")
def mock_aws_credential():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield


@pytest.fixture(autouse=True, scope="module")
def s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(autouse=True, scope="function")
def setup_compaction_artifacts_s3_bucket(s3_resource: ServiceResource):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=RCF_BUCKET_NAME,
    )
    yield
    s3_resource.Bucket(RCF_BUCKET_NAME).objects.all().delete()


class TestReadWriteRoundCompletionFile:
    def test_read_when_rcf_written_without_destination(self):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        rcf_url = write_round_completion_file(
            RCF_BUCKET_NAME, source_locator, None, expected_rcf
        )

        rcf = read_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator
        )

        assert (
            rcf_url == "s3://rcf-bucket/f9829af39770d904dbb811bd8f4e886dd307f507.json"
        )
        assert rcf == expected_rcf

    def test_read_when_rcf_written_with_destination(self):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        rcf_url = write_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator, expected_rcf
        )

        rcf = read_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator
        )

        assert (
            rcf_url
            == "s3://rcf-bucket/f9829af39770d904dbb811bd8f4e886dd307f507/e9939deadc091b3289a2eb0ca56b1ba86b9892f4.json"
        )
        assert rcf == expected_rcf

    def test_read_without_destination_when_rcf_written_with_destination(self):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        write_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator, expected_rcf
        )

        rcf = read_round_completion_file(RCF_BUCKET_NAME, source_locator, None)

        assert rcf is None

    def test_read_without_destination_when_rcf_written_without_destination(self):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        write_round_completion_file(RCF_BUCKET_NAME, source_locator, None, expected_rcf)

        rcf = read_round_completion_file(RCF_BUCKET_NAME, source_locator, None)

        assert rcf == expected_rcf

    def test_read_when_rcf_written_both_with_and_without_destination(self):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator("destination")

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        expected_rcf_2 = RoundCompletionInfo.of(
            high_watermark=1223,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=1233,
        )

        write_round_completion_file(RCF_BUCKET_NAME, source_locator, None, expected_rcf)

        write_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator, expected_rcf_2
        )

        rcf = read_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator
        )

        assert rcf == expected_rcf_2

    def test_read_when_none_destination_partition_id(self):

        source_locator = get_test_partition_locator("source")
        destination_locator = get_test_partition_locator(None)

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        write_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator, expected_rcf
        )

        rcf = read_round_completion_file(
            RCF_BUCKET_NAME, source_locator, destination_locator
        )

        assert rcf == expected_rcf

    def test_write_when_custom_url_is_passed(self):
        """
        This test case tests the backward compatibility by successfully
        reading the previously written rcf.
        """

        source_locator = get_test_partition_locator("source")

        expected_rcf = RoundCompletionInfo.of(
            high_watermark=122,
            compacted_delta_locator={},
            compacted_pyarrow_write_result={},
            sort_keys_bit_width=12,
        )

        completion_file_s3_url = f"s3://{RCF_BUCKET_NAME}/test.json"
        rcf_url = write_round_completion_file(
            RCF_BUCKET_NAME,
            source_locator,
            None,
            expected_rcf,
            completion_file_s3_url=completion_file_s3_url,
        )

        rcf = read_round_completion_file(RCF_BUCKET_NAME, source_locator, None)

        assert rcf_url == completion_file_s3_url
        assert rcf is None
