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
from typing import Any, Dict, Optional
from typing import List
import pyarrow as pa

MAX_RECORDS_PER_FILE: int = 1
COLUMN_NAMES: List[str] = ["pk", "sk"]

TEST_S3_RCF_BUCKET_NAME = "test-compaction-artifacts-bucket"
# REBASE  src = spark compacted table to create an initial version of ray compacted table
REBASE_TEST_SOURCE_NAMESPACE = "rebase_source_test_namespace"
REBASE_TEST_SOURCE_TABLE_NAME = "rebase_test_table"
REBASE_TEST_SOURCE_TABLE_VERSION = "1"
REBASE_TEST_SOURCE_PARTITION_KEYS = [{"keyName": "region_id", "keyType": "int"}]
REBASE_TEST_SOURCE_PRIMARY_KEYS = ["id"]

REBASE_TEST_DESTINATION_NAMESPACE = "rebase_destination_test_namespace"
REBASE_TEST_DESTINATION_TABLE_NAME = "rebase_destination_test_table"
REBASE_TEST_DESTINATION_TABLE_VERSION = "1"
REBASE_TEST_DESTINATION_PARTITION_KEYS = [{"keyName": "region_id", "keyType": "int"}]
REBASE_TEST_DESTINATION_PRIMARY_KEYS = ["id"]

# # INCREMENTAL = ray -> ray
# INCREMENTAL_TEST_NAMESPACE = "incremental_test_namespace"
# INCREMENTAL_TEST_SOURCE_TABLE_NAME = "incremental_test_table"
# INCREMENTAL_TEST_SOURCE_TABLE_VERSION = "1"
# INCREMENTAL_TEST_DESTINATION_TABLE_NAME = "incremental_destination_test_table"
# INCREMENTAL_TEST_DESTINATION_TABLE_VERSION = None

# IN-PLACE?

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
def ds_mock_kwargs():
    DATABASE_RELATIVE_PATH = "deltacat/tests/local_deltacat_storage/db_test.sql"
    conn: Connection = sqlite3.connect(DATABASE_RELATIVE_PATH)
    cur: Cursor = conn.cursor()
    kwargs: Dict[str, Any] = {
        "sqlite3_con": conn,
        "sqlite3_cur": cur,
        "sqlite3_rel_path": DATABASE_RELATIVE_PATH,
    }
    yield kwargs
    conn.close()


@pytest.fixture(scope="function")
def fake_compaction_artifacts_s3_bucket(s3_resource, monkeypatch):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


# rebase
@pytest.fixture(scope="function")
def sanity_viable_source_table(ds_mock_kwargs):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentEncoding, ContentType, TableType
    from deltacat.storage import Delta, Stream, Table, TableVersion

    ds.create_namespace(REBASE_TEST_SOURCE_NAMESPACE, {}, **ds_mock_kwargs)
    ds.create_table_version(
        REBASE_TEST_SOURCE_NAMESPACE,
        REBASE_TEST_SOURCE_TABLE_NAME,
        REBASE_TEST_SOURCE_TABLE_VERSION,
        primary_key_column_names=REBASE_TEST_SOURCE_PRIMARY_KEYS,
        **ds_mock_kwargs,
    )
    source_stream: Stream = ds.get_stream(
        namespace=REBASE_TEST_SOURCE_NAMESPACE,
        table_name=REBASE_TEST_SOURCE_TABLE_NAME,
        table_version=REBASE_TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    col1 = pa.array([i for i in range(10)])
    col2 = pa.array(["foo"] * 10)
    test_table = pa.Table.from_arrays([col1, col2], names=COLUMN_NAMES)
    staged_partition = ds.stage_partition(source_stream, [], **ds_mock_kwargs)
    committed_delta: Delta = ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    yield
    ds.delete_partition(
        REBASE_TEST_SOURCE_NAMESPACE,
        REBASE_TEST_SOURCE_TABLE_NAME,
        REBASE_TEST_SOURCE_TABLE_VERSION,
        [],
        **ds_mock_kwargs,
    )
    ds.delete_stream(
        REBASE_TEST_SOURCE_NAMESPACE,
        REBASE_TEST_SOURCE_TABLE_NAME,
        REBASE_TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    )


@pytest.fixture(scope="function")
def sanity_viable_destination_table(ds_mock_kwargs):
    import deltacat.tests.local_deltacat_storage as ds
    from deltacat.types.media import ContentEncoding, ContentType, TableType
    from deltacat.storage import Delta, Stream, Table, TableVersion

    ds.create_namespace(REBASE_TEST_DESTINATION_NAMESPACE, {}, **ds_mock_kwargs)
    ds.create_table_version(
        REBASE_TEST_DESTINATION_NAMESPACE,
        REBASE_TEST_DESTINATION_TABLE_NAME,
        REBASE_TEST_DESTINATION_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    destination_stream: Stream = ds.get_stream(
        namespace=REBASE_TEST_DESTINATION_NAMESPACE,
        table_name=REBASE_TEST_DESTINATION_TABLE_NAME,
        table_version=REBASE_TEST_DESTINATION_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    col1 = pa.array([i for i in range(10)])
    col2 = pa.array(["bar"] * 10)
    test_table = pa.Table.from_arrays([col1, col2], names=COLUMN_NAMES)
    staged_partition = ds.stage_partition(destination_stream, [], **ds_mock_kwargs)
    committed_delta: Delta = ds.commit_delta(
        ds.stage_delta(test_table, staged_partition, **ds_mock_kwargs), **ds_mock_kwargs
    )
    ds.commit_partition(staged_partition, **ds_mock_kwargs)
    yield
    ds.delete_partition(
        REBASE_TEST_DESTINATION_NAMESPACE,
        REBASE_TEST_DESTINATION_TABLE_NAME,
        REBASE_TEST_DESTINATION_TABLE_VERSION,
        [],
        **ds_mock_kwargs,
    )
    ds.delete_stream(
        REBASE_TEST_DESTINATION_NAMESPACE,
        REBASE_TEST_DESTINATION_TABLE_NAME,
        REBASE_TEST_DESTINATION_TABLE_VERSION,
        **ds_mock_kwargs,
    )


def test_compact_partition_success(
    ds_mock_kwargs,
    fake_compaction_artifacts_s3_bucket,
    sanity_viable_source_table,
    sanity_viable_destination_table,
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

    ray.init(local_mode=True)  # TODO (use non-deprecated alternative)
    source_table_version = ds.get_table_version(
        REBASE_TEST_SOURCE_NAMESPACE,
        REBASE_TEST_SOURCE_TABLE_NAME,
        REBASE_TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    source_table_stream: Optional[Stream] = ds.get_stream(
        namespace=REBASE_TEST_SOURCE_NAMESPACE,
        table_name=REBASE_TEST_SOURCE_TABLE_NAME,
        table_version=REBASE_TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    destination_table_stream: Stream = ds.get_stream(
        namespace=REBASE_TEST_DESTINATION_NAMESPACE,
        table_name=REBASE_TEST_DESTINATION_TABLE_NAME,
        table_version=REBASE_TEST_DESTINATION_TABLE_VERSION,
        **ds_mock_kwargs,
    )
    source_partition = ds.get_partition(
        source_table_stream.locator,
        [],
        **ds_mock_kwargs,
    )
    destination_partition = ds.get_partition(
        destination_table_stream.locator, [], **ds_mock_kwargs
    )
    num_workers = 1
    worker_instance_cpu = 1
    total_cpus = num_workers * worker_instance_cpu
    deltas = ds.list_deltas(
        source_table_stream.namespace,
        source_table_stream.table_name,
        [],
        source_table_stream.table_version,
        **ds_mock_kwargs,
    ).all_items()
    print(f"{deltas=}")
    # old_parent_table_stream_pos = int(deltas[0]["streamPosition"])
    # old_parent_table_stream_pos = deltas[0]["deltaLocator"]["streamPosition"]
    compact_partition_params: Dict[str, Any] = {
        "source_partition_locator": source_partition.locator,
        "destination_partition_locator": destination_partition.locator,
        "last_stream_position_to_compact": source_partition.stream_position,
        "primary_keys": set(source_table_version.primary_keys),
        "hash_bucket_count": None,
        "records_per_compacted_file": MAX_RECORDS_PER_FILE,
        "dd_max_parallelism_ratio": 1.0,
        "deltacat_storage": ds,
        "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
        "compacted_file_content_type": ContentType.PARQUET,
        "list_deltas_kwargs": {**ds_mock_kwargs},
        "pg_config": PlacementGroupManager(1, total_cpus, worker_instance_cpu).pgs[0],
        **ds_mock_kwargs,
    }
    list_deltas = ds.list_deltas(
        REBASE_TEST_SOURCE_NAMESPACE,
        REBASE_TEST_SOURCE_TABLE_NAME,
        [],
        REBASE_TEST_SOURCE_TABLE_VERSION,
        **ds_mock_kwargs,
    ).all_items()
    print(f"{list_deltas=}")
    actual_res = compact_partition(**compact_partition_params)
