import mock
import ray
from mock import MagicMock
import sqlite3
import pandas as pd
from moto import mock_s3
import pytest
import os
import boto3
from sqlite3 import Connection, Cursor
from typing import Dict, Optional

TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE
REBASE_TEST_NAMESPACE = "rebase_test_namespace"
REBASE_TEST_SOURCE_TABLE_NAME = "rebase_test_table"
REBASE_TEST_SOURCE_TABLE_VERSION = "1"
REBASE_TEST_DESTINATION_TABLE_NAME = "rebase_destination_test_table"
REBASE_TEST_DESTINATION_TABLE_VERSION = None

# BACKFILL
BACKFILL_TEST_NAMESPACE = "backfill_test_namespace"
BACKFILL_TEST_SOURCE_TABLE_NAME = "backfill_test_table"
BACKFILL_TEST_SOURCE_TABLE_VERSION = "1"
BACKFILL_TEST_DESTINATION_TABLE_NAME = "backfill_destination_test_table"
BACKFILL_TEST_DESTINATION_TABLE_VERSION = None

# INCREMENTAL
INCREMENTAL_TEST_NAMESPACE = "incremental_test_namespace"
INCREMENTAL_TEST_SOURCE_TABLE_NAME = "incremental_test_table"
INCREMENTAL_TEST_SOURCE_TABLE_VERSION = "1"
INCREMENTAL_TEST_DESTINATION_TABLE_NAME = "incremental_destination_test_table"
INCREMENTAL_TEST_DESTINATION_TABLE_VERSION = None

# module-level fixtures


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


# function-level fixtures


@pytest.fixture(scope="function")
def deltacat_storage_mocks_kwargs():
    conn: Connection = sqlite3.connect(
        "deltacat/tests/local_deltacat_storage/db_test.sql"
    )
    cur: Cursor = conn.cursor()
    kwargs: Dict[str] = {"sqlite3_con": conn, "sqlite3_cur": cur}
    yield kwargs
    conn.close()


# rebase
@pytest.fixture(scope="function")
def create_table_version_rebase(deltacat_storage_mocks_kwargs):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentEncoding, ContentType, TableType

    ds.create_namespace(REBASE_TEST_NAMESPACE, {}, **deltacat_storage_mocks_kwargs)
    ds.create_table_version(
        REBASE_TEST_NAMESPACE, "test_table", "1", **deltacat_storage_mocks_kwargs
    )
    ds.create_table_version(
        REBASE_TEST_NAMESPACE,
        "destination_test_table",
        "1",
        **deltacat_storage_mocks_kwargs,
    )
    source_stream: Stream = ds.get_stream(
        namespace=REBASE_TEST_NAMESPACE,
        table_name="test_table",
        table_version="1",
        **deltacat_storage_mocks_kwargs,
    )
    destination_stream = ds.get_stream(
        namespace=REBASE_TEST_NAMESPACE,
        table_name="destination_test_table",
        table_version="1",
        **deltacat_storage_mocks_kwargs,
    )
    staged_source = ds.stage_partition(
        source_stream, [], **deltacat_storage_mocks_kwargs
    )
    ds.commit_partition(staged_source, **deltacat_storage_mocks_kwargs)
    staged_destination = ds.stage_partition(
        destination_stream, [], **deltacat_storage_mocks_kwargs
    )
    ds.commit_partition(staged_destination, **deltacat_storage_mocks_kwargs)
    yield
    ds.delete_partition(
        REBASE_TEST_NAMESPACE, "test_table", "1", [], **deltacat_storage_mocks_kwargs
    )
    ds.delete_stream(
        REBASE_TEST_NAMESPACE, "test_table", "1", **deltacat_storage_mocks_kwargs
    )

    ds.delete_partition(
        REBASE_TEST_NAMESPACE,
        "destination_test_table",
        "1",
        [],
        **deltacat_storage_mocks_kwargs,
    )
    ds.delete_stream(
        REBASE_TEST_NAMESPACE,
        "destination_test_table",
        "1",
        **deltacat_storage_mocks_kwargs,
    )


@pytest.fixture(scope="function")
def fake_compaction_artifacts_s3_bucket(s3_resource, monkeypatch):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )


def test_compact_partition_success(
    deltacat_storage_mocks_kwargs,
    fake_compaction_artifacts_s3_bucket,
    create_table_version_rebase,
):
    from deltacat.compute.compactor.compaction_session import compact_partition
    from deltacat.storage.model.partition import (
        Partition,
        PartitionLocator,
        StreamLocator,
        TableVersionLocator,
        TableLocator,
    )
    from deltacat.types.media import ContentType
    from deltacat.utils.placement import (
        PlacementGroupConfig,
        placement_group,
        PlacementGroupManager,
    )
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.storage import (
        Delta,
        DeltaLocator,
        DeltaType,
        DistributedDataset,
        LifecycleState,
        ListResult,
        LocalDataset,
        LocalTable,
        Manifest,
        ManifestAuthor,
        Namespace,
        NamespaceLocator,
        Partition,
        SchemaConsistencyType,
        Stream,
        StreamLocator,
        Table,
        TableVersion,
        TableVersionLocator,
        TableLocator,
        CommitState,
        SortKey,
        PartitionLocator,
        ManifestMeta,
        ManifestEntry,
        ManifestEntryList,
    )

    ray.init(local_mode=True)
    source_stream: Optional[Stream] = ds.get_stream(
        namespace=REBASE_TEST_NAMESPACE,
        table_name="test_table",
        table_version="1",
        **deltacat_storage_mocks_kwargs,
    )
    source_partition = ds.get_partition(
        source_stream.locator,
        [],
        **deltacat_storage_mocks_kwargs,
    )
    destination_stream: Stream = ds.get_stream(
        namespace=REBASE_TEST_NAMESPACE,
        table_name="destination_test_table",
        table_version="1",
        **deltacat_storage_mocks_kwargs,
    )
    destination_partition = ds.get_partition(
        destination_stream.locator, [], **deltacat_storage_mocks_kwargs
    )
    primary_keys = set(["id"])
    compaction_artifact_s3_bucket = TEST_S3_RCF_BUCKET_NAME
    last_stream_position_to_compact = 1689126110259
    hash_bucket_count = None
    records_per_compacted_file = 1
    input_delta_stats = None
    min_hash_bucket_chunk_size = None
    compacted_file_content_type = ContentType.PARQUET
    num_workers = 1
    worker_instance_cpu = 1
    total_cpus = num_workers * worker_instance_cpu
    rebase_source_partition_high_watermark = None
    list_deltas_kwargs = {"equivalent_table_types": []}
    pg_configs = PlacementGroupManager(1, total_cpus, worker_instance_cpu).pgs[0]
    actual_res = compact_partition(
        source_partition_locator=source_partition.locator,
        destination_partition_locator=destination_partition.locator,
        primary_keys=primary_keys,
        compaction_artifact_s3_bucket=compaction_artifact_s3_bucket,
        last_stream_position_to_compact=last_stream_position_to_compact,
        hash_bucket_count=hash_bucket_count,
        records_per_compacted_file=records_per_compacted_file,
        input_delta_stats=input_delta_stats,
        min_hash_bucket_chunk_size=min_hash_bucket_chunk_size,
        pg_config=pg_configs,
        deltacat_storage=ds,
        list_deltas_kwargs={**deltacat_storage_mocks_kwargs, **list_deltas_kwargs},
    )
