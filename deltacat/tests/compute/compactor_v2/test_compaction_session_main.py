import ray
import os
import pytest
import tempfile
import shutil
import pandas as pd
from deltacat.storage import metastore
from deltacat.catalog import CatalogProperties
from deltacat.types.media import ContentType
from deltacat.storage.model.types import DeltaType
from deltacat.compute.compactor_v2.compaction_session import compact_partition
from deltacat.compute.compactor.model.compact_partition_params import CompactPartitionParams
from deltacat.tests.compute.test_util_constant import TEST_S3_RCF_BUCKET_NAME
from moto import mock_s3
import boto3


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
def setup_compaction_artifacts_s3_bucket(s3_resource):
    s3_resource.create_bucket(
        ACL="authenticated-read",
        Bucket=TEST_S3_RCF_BUCKET_NAME,
    )
    yield


@pytest.fixture
def catalog():
    """Create a temporary catalog for testing."""
    tmpdir = tempfile.mkdtemp()
    catalog = CatalogProperties(root=tmpdir)
    yield catalog
    shutil.rmtree(tmpdir)


class TestCompactionSessionMain:
    """Basic sanity tests for compact_partition using main deltacat metastore."""

    NAMESPACE = "compact_partition_main_test"
    
    def test_compact_partition_basic_sanity(self, catalog):
        """Basic sanity test to verify compact_partition works with main metastore."""
        
        # Create source namespace and table
        source_namespace = metastore.create_namespace(
            namespace=f"{self.NAMESPACE}_source",
            catalog=catalog,
        )
        
        # Create destination namespace and table
        dest_namespace = metastore.create_namespace(
            namespace=f"{self.NAMESPACE}_dest",
            catalog=catalog,
        )
        
        # Create a simple test dataset
        test_data = pd.DataFrame({
            "pk": [1, 2, 3, 4],
            "name": ["A", "B", "C", "D"],
            "value": [10, 20, 30, 40]
        })
        
        # Create source table and partition
        source_table, source_table_version, source_stream = metastore.create_table_version(
            namespace=source_namespace.locator.namespace,
            table_name="source_table",
            catalog=catalog,
        )
        
        source_partition = metastore.stage_partition(
            stream=source_stream,
            catalog=catalog,
        )
        source_partition = metastore.commit_partition(
            partition=source_partition,
            catalog=catalog,
        )
        
        # Stage and commit a delta to the source partition
        staged_delta = metastore.stage_delta(
            data=test_data,
            partition=source_partition,
            catalog=catalog,
            content_type=ContentType.PARQUET,
            delta_type=DeltaType.UPSERT,
        )
        
        source_delta = metastore.commit_delta(
            delta=staged_delta,
            catalog=catalog,
        )
        
        # Create destination table and partition
        dest_table, dest_table_version, dest_stream = metastore.create_table_version(
            namespace=dest_namespace.locator.namespace,
            table_name="dest_table",
            catalog=catalog,
        )
        
        dest_partition = metastore.stage_partition(
            stream=dest_stream,
            catalog=catalog,
        )
        dest_partition = metastore.commit_partition(
            partition=dest_partition,
            catalog=catalog,
        )
        
        # Test compact_partition with minimal parameters
        rcf_url = compact_partition(
            CompactPartitionParams.of(
                {
                    "compaction_artifact_s3_bucket": TEST_S3_RCF_BUCKET_NAME,
                    "compacted_file_content_type": ContentType.PARQUET,
                    "dd_max_parallelism_ratio": 1.0,
                    "deltacat_storage": metastore,
                    "deltacat_storage_kwargs": {"catalog": catalog},
                    "destination_partition_locator": dest_partition.locator,
                    "drop_duplicates": True,
                    "hash_bucket_count": 1,
                    "last_stream_position_to_compact": source_delta.stream_position,
                    "list_deltas_kwargs": {
                        "catalog": catalog,
                        "equivalent_table_types": [],
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
        
        # Basic verification - if we get here without exceptions, the basic flow works
        print(f"✅ Compaction completed successfully! RCF URL: {rcf_url}")
        
        # Verify that an RCF URL was generated
        assert rcf_url is not None, "Expected a non-None RCF URL"
        assert rcf_url.startswith("s3://"), "Expected RCF URL to be an S3 URL"
        
        # Get a fresh reference to the destination partition to see updates
        updated_dest_partition = metastore.get_partition(
            stream_locator=dest_stream.locator,
            partition_values=None,  # unpartitioned
            catalog=catalog,
        )
        
        print(f"Original destination partition stream position: {dest_partition.stream_position}")
        print(f"Updated destination partition stream position: {updated_dest_partition.stream_position}")
        
        # Verify that the destination partition now has some deltas
        dest_partition_deltas = metastore.list_partition_deltas(
            partition_like=updated_dest_partition,
            include_manifest=True,
            catalog=catalog,
        )
        
        delta_count = len(dest_partition_deltas.all_items())
        print(f"Found {delta_count} delta(s) in destination partition")
        
        # Verify that at least one compacted delta was written to the destination partition
        assert delta_count > 0, f"Expected at least one delta in destination partition, but found {delta_count}"
        
        # Print some info about the delta(s) found
        for i, delta in enumerate(dest_partition_deltas.all_items()):
            print(f"Delta {i+1}: stream_position={delta.stream_position}, type={delta.type}, record_count={delta.meta.record_count if delta.meta else 'N/A'}")
        
        print(f"✅ Basic sanity test PASSED! compact_partition works with main deltacat metastore and wrote {delta_count} delta(s) to destination partition.") 