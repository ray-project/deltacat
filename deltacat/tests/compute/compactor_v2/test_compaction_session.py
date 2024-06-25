from typing import Dict, Any
import ray
import os
import pytest
import boto3
from deltacat.compute.compactor.model.compaction_session_audit_info import (
    CompactionSessionAuditInfo,
)
from boto3.resources.base import ServiceResource
import deltacat.tests.local_deltacat_storage as ds
from deltacat.types.media import ContentType
from deltacat.compute.compactor_v2.compaction_session import (
    compact_partition,
)
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.tests.test_utils.utils import read_s3_contents
from deltacat.tests.compute.test_util_constant import (
    TEST_S3_RCF_BUCKET_NAME,
)
from deltacat.tests.compute.test_util_common import get_rcf
from deltacat.tests.test_utils.pyarrow import (
    stage_partition_from_file_paths,
    commit_delta_to_staged_partition,
    commit_delta_to_partition,
)
from moto import mock_s3

DATABASE_FILE_PATH_KEY, DATABASE_FILE_PATH_VALUE = (
    "db_file_path",
    "deltacat/tests/local_deltacat_storage/db_test.sqlite",
)


@pytest.fixture(autouse=True, scope="module")
def setup_ray_cluster():
    ray.init(local_mode=True, ignore_reinit_error=True)
    yield
    ray.shutdown()


@pytest.fixture(autouse=True, scope="module")
def mock_aws_credential():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_ID"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    yield


@pytest.fixture(scope="module")
def s3_resource(mock_aws_credential):
    with mock_s3():
        yield boto3.resource("s3")


@pytest.fixture(autouse=True, scope="module")
def setup_compaction_artifacts_s3_bucket(s3_resource: ServiceResource):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


@pytest.fixture(scope="function")
def local_deltacat_storage_kwargs(request: pytest.FixtureRequest):
    kwargs_for_local_deltacat_storage: Dict[str, Any] = {
        DATABASE_FILE_PATH_KEY: DATABASE_FILE_PATH_VALUE,
    }
    yield kwargs_for_local_deltacat_storage
    if os.path.exists(DATABASE_FILE_PATH_VALUE):
        os.remove(DATABASE_FILE_PATH_VALUE)


class TestCompactionSession:
    """
    This class adds specific tests that aren't part of the parametrized test suite.
    """

    NAMESPACE = "compact_partition_v2_namespace"
    BACKFILL_FILE_PATH = (
        "deltacat/tests/compute/compactor_v2/data/backfill_source_date_pk.csv"
    )
    INCREMENTAL_FILE_PATH = (
        "deltacat/tests/compute/compactor_v2/data/incremental_source_date_pk.csv"
    )

    def test_compact_partition_when_no_input_deltas_to_compact(
        self, local_deltacat_storage_kwargs
    ):
        # setup
        staged_source = stage_partition_from_file_paths(
            self.NAMESPACE, ["test"], **local_deltacat_storage_kwargs
        )
        source_partition = ds.commit_partition(
            staged_source, **local_deltacat_storage_kwargs
        )

        staged_dest = stage_partition_from_file_paths(
            self.NAMESPACE, ["destination"], **local_deltacat_storage_kwargs
        )
        dest_partition = ds.commit_partition(
            staged_dest, **local_deltacat_storage_kwargs
        )

        # action
        rcf_url = compact_partition(
            CompactPartitionParams.of(
                {
                    "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": ds,
                    "deltacat_storage_kwargs": local_deltacat_storage_kwargs,
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 2,
                    "last_stream_position_to_compact": source_partition.stream_position,
                    "list_deltas_kwargs": {
                        **local_deltacat_storage_kwargs,
                        **{"equivalent_table_types": []},
                    },
                    "primary_keys": ["pk"],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "s3_client_kwargs": {},
                    "source_partition_locator": source_partition.locator,
                }
            )
        )

        # verify that no RCF is written
        assert rcf_url is None

    def test_compact_partition_when_rcf_was_written_by_past_commit(
        self, s3_resource, local_deltacat_storage_kwargs
    ):
        """
        Backward compatibility test for when a RCF was written by a previous commit.
        """

        # setup
        staged_source = stage_partition_from_file_paths(
            self.NAMESPACE, ["source"], **local_deltacat_storage_kwargs
        )

        source_delta = commit_delta_to_staged_partition(
            staged_source, [self.BACKFILL_FILE_PATH], **local_deltacat_storage_kwargs
        )

        staged_dest = stage_partition_from_file_paths(
            self.NAMESPACE, ["destination"], **local_deltacat_storage_kwargs
        )
        dest_partition = ds.commit_partition(
            staged_dest, **local_deltacat_storage_kwargs
        )

        # action
        rcf_url = compact_partition(
            CompactPartitionParams.of(
                {
                    "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": ds,
                    "deltacat_storage_kwargs": local_deltacat_storage_kwargs,
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 1,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        **local_deltacat_storage_kwargs,
                        **{"equivalent_table_types": []},
                    },
                    "primary_keys": [],
                    "rebase_source_partition_locator": source_delta.partition_locator,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "s3_client_kwargs": {},
                    "source_partition_locator": source_delta.partition_locator,
                }
            )
        )

        bucket, backfill_key1, backfill_key2 = rcf_url.strip("s3://").split("/")
        assert bucket == TEST_S3_RCF_BUCKET_NAME

        # Now delete the RCF at new location and copy it to old location
        # Copy the RCF from rcf_url to another location
        s3_resource.Object(TEST_S3_RCF_BUCKET_NAME, f"{backfill_key1}.json").copy_from(
            CopySource=f"{TEST_S3_RCF_BUCKET_NAME}/{backfill_key1}/{backfill_key2}"
        )

        s3_resource.Object(
            TEST_S3_RCF_BUCKET_NAME, f"{backfill_key1}/{backfill_key2}"
        ).delete()

        # Now run an incremental compaction and verify if the previous RCF was read properly.

        new_source_delta = commit_delta_to_partition(
            source_delta.partition_locator,
            [self.INCREMENTAL_FILE_PATH],
            **local_deltacat_storage_kwargs,
        )

        new_rcf_url = compact_partition(
            CompactPartitionParams.of(
                {
                    "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": ds,
                    "deltacat_storage_kwargs": local_deltacat_storage_kwargs,
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 1,
                    "last_stream_position_to_compact": new_source_delta.stream_position,
                    "list_deltas_kwargs": {
                        **local_deltacat_storage_kwargs,
                        **{"equivalent_table_types": []},
                    },
                    "primary_keys": ["pk"],
                    "rebase_source_partition_locator": None,
                    "rebase_source_partition_high_watermark": None,
                    "records_per_compacted_file": 4000,
                    "s3_client_kwargs": {},
                    "source_partition_locator": new_source_delta.partition_locator,
                }
            )
        )

        new_bucket, incremental_key1, incremental_key2 = new_rcf_url.strip(
            "s3://"
        ).split("/")

        assert new_bucket == TEST_S3_RCF_BUCKET_NAME
        assert backfill_key1 == incremental_key1
        assert backfill_key2 != incremental_key2

        rcf = get_rcf(s3_resource, new_rcf_url)

        _, compaction_audit_key = rcf.compaction_audit_url.strip("s3://").split("/", 1)
        compaction_audit = CompactionSessionAuditInfo(
            **read_s3_contents(
                s3_resource, TEST_S3_RCF_BUCKET_NAME, compaction_audit_key
            )
        )

        # as it should be running incremental
        assert compaction_audit.uniform_deltas_created == 1
        assert compaction_audit.input_records == 6
