import unittest
import mock
import ray
from mock import MagicMock


class TestCompactPartitionParams(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from deltacat.types.media import ContentType

        cls.VALID_COMPACT_PARTITION_PARAMS = {
            "compaction_artifact_s3_bucket": "foobar",
            "compacted_file_content_type": ContentType.PARQUET,
            "deltacat_storage": "foobar",
            "destination_partition_locator": {
                "streamLocator": {
                    "tableVersionLocator": {
                        "tableLocator": {
                            "namespaceLocator": {"namespace": "testNamespaceLocator"},
                            "tableName": "TABLE_FOO",
                        },
                        "tableVersion": "1",
                    },
                    "streamId": "foobar",
                    "storageType": "fooType",
                },
                "partitionValues": [],
                "partitionId": None,
            },
            "hash_bucket_count": None,
            "last_stream_position_to_compact": 168000000000,
            "list_deltas_kwargs": {"equivalent_table_types": []},
            "primary_keys": {"id"},
            "properties": {
                "parent_stream_position": "1688000000000",
            },
            "read_kwargs_provider": "foo",
            "rebase_source_partition_high_watermark": 1688000000000,
            "rebase_source_partition_locator": {
                "stream_locator": {
                    "table_version_locator": {
                        "table_locator": {
                            "namespace_locator": {"namespace": "testNamespaceLocator"},
                            "tableName": "TABLE_FOO",
                        },
                        "table_version": "1",
                    },
                    "streamId": "foobar",
                    "storageType": "fooType",
                },
                "partitionValues": [],
                "partitionId": "79612ea39ac5493eae925abe60767d42",
            },
            "s3_table_writer_kwargs": {
                "version": "1.0",
                "flavor": "foobar",
                "coerce_timestamps": "ms",
            },
            "source_partition_locator": {
                "streamLocator": {
                    "tableVersionLocator": {
                        "tableLocator": {
                            "namespaceLocator": {"namespace": "testNamespaceLocator"},
                            "tableName": "TABLE_FOO",
                        },
                        "tableVersion": "2",
                    },
                    "streamId": "foobar",
                    "storageType": "fooType",
                },
                "partitionValues": [],
                "partitionId": "79612ea39ac5493eae925abe60767d42",
            },
        }
        #     cls.ray_mock = mock.MagicMock()
        #     cls.ray_mock.cluster_resources.return_value = {
        #         "CPU": 10,
        #         "memory": 10,
        #         "object_store_memory": 5,
        #     }
        #     cls.ray_mock.available_resources.return_value = {
        #         "CPU": 6,
        #         "memory": 4,
        #         "object_store_memory": 5,
        #     }

        #     cls.module_patcher = mock.patch.dict("sys.modules", {"ray": cls.ray_mock})
        #     cls.module_patcher.start()

        #     # delete reference to reload from mocked ray
        #     if "deltacat.utils.resources" in sys.modules:
        #         del sys.modules["deltacat.utils.resources"]

        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        pass
        # cls.module_patcher.stop()

    def test_compact_partition_success(self):
        ray.init(local_mode=True)
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
        from deltacat.tests.mocks import mock_deltacat_storage

        source_partition_locator = PartitionLocator.of(
            stream_locator=StreamLocator.of(
                table_version_locator=TableVersionLocator.at(
                    namespace="bdt-ray-dev",
                    table_name="SHADOW_TEST_D_CR_BRIDGE",
                    table_version="1",
                ),
                stream_id="d5771c4b-a7b9-453e-932c-67bd9861f57d",
                storage_type="foo",
            ),
            partition_values=["1"],
            partition_id="a293ed05-ea68-4d95-974c-41e7e9d86648",
        )
        destination_partition_locator = PartitionLocator.of(
            stream_locator=StreamLocator.of(
                table_version_locator=TableVersionLocator.at(
                    namespace="bdt-ray-dev",
                    table_name="SHADOW_TEST_D_CR_BRIDGE",
                    table_version="1",
                ),
                stream_id="d5771c4b-a7b9-453e-932c-67bd9861f57d",
                storage_type="foo",
            ),
            partition_values=["1"],
            partition_id="a293ed05-ea68-4d95-974c-41e7e9d86648",
        )
        primary_keys = set(["id"])
        compaction_artifact_s3_bucket = "compaction-artifacts-foo"
        last_stream_position_to_compact = 1689126110259
        hash_bucket_count = None
        records_per_compacted_file = None
        input_delta_stats = None
        min_hash_bucket_chunk_size = None
        compacted_file_content_type = ContentType.PARQUET
        num_workers = 1
        worker_instance_cpu = 1
        total_cpus = num_workers * worker_instance_cpu
        rebase_source_partition_locator = None
        rebase_source_partition_high_watermark = None
        pg_configs = PlacementGroupManager(1, total_cpus, worker_instance_cpu).pgs
        print(mock_deltacat_storage.list_namespaces())
        assert False  # force a fail to see any output sent to stdout and stderr is captured
        # deltacat_storage =
        # assert pg_configs == "foo"
