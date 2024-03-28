import json

import unittest


class TestCompactPartitionParams(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from deltacat.types.media import ContentType
        from deltacat.utils.metrics import MetricsConfig, MetricsTarget

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
            "hash_bucket_count": 200,
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
            "memory_logs_enabled": True,
            "metrics_config": MetricsConfig("us-east-1", MetricsTarget.CLOUDWATCH_EMF),
        }

        super().setUpClass()

    def test_serialize_returns_json_string(self):
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        params = CompactPartitionParams.of(
            TestCompactPartitionParams.VALID_COMPACT_PARTITION_PARAMS
        )
        serialized_params = params.serialize()
        assert isinstance(serialized_params, str)

    def test_serialize_returns_json_string_with_all_fields(self):
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        params = CompactPartitionParams.of(
            TestCompactPartitionParams.VALID_COMPACT_PARTITION_PARAMS
        )
        serialized_params = params.serialize()
        assert isinstance(serialized_params, str)
        assert (
            json.loads(serialized_params)["compacted_file_content_type"]
            == params.compacted_file_content_type
        )
        assert (
            json.loads(serialized_params)["compaction_artifact_s3_bucket"]
            == params.compaction_artifact_s3_bucket
        )
        assert (
            json.loads(serialized_params)["hash_bucket_count"]
            == params.hash_bucket_count
        )
        assert (
            json.loads(serialized_params)["last_stream_position_to_compact"]
            == params.last_stream_position_to_compact
        )
        assert (
            json.loads(serialized_params)["list_deltas_kwargs"]
            == params.list_deltas_kwargs
        )
        assert json.loads(serialized_params)["primary_keys"] == params.primary_keys
        assert (
            json.loads(serialized_params)["rebase_source_partition_high_watermark"]
            == params.rebase_source_partition_high_watermark
        )
        assert (
            json.loads(serialized_params)["rebase_source_partition_locator"]
            == params.rebase_source_partition_locator
        )
        assert (
            json.loads(serialized_params)["source_partition_locator"]
            == params.source_partition_locator
        )
        assert (
            json.loads(serialized_params)["destination_partition_locator"]
            == params.destination_partition_locator
        )
        assert (
            json.loads(serialized_params)["memory_logs_enabled"]
            == params.memory_logs_enabled
        )
        assert (
            json.loads(serialized_params)["metrics_config"]["metrics_target"]
            == params.metrics_config.metrics_target
        )

    def test_serialize_handles_sets(self):
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        params = CompactPartitionParams.of(
            {
                **TestCompactPartitionParams.VALID_COMPACT_PARTITION_PARAMS,
                "primary_keys": {"foo", "bar", "baz"},
            }
        )
        serialized_params = params.serialize()
        self.assertCountEqual(
            json.loads(serialized_params)["primary_keys"], ["foo", "bar", "baz"]
        )

    def test_serialize_handles_objects_with_toJSON_method(self):
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        class MyObject:
            def toJSON(self) -> str:
                return "my-json-object"

        params = CompactPartitionParams.of(
            {
                **TestCompactPartitionParams.VALID_COMPACT_PARTITION_PARAMS,
                "compacted_file_content_type": MyObject(),
            }
        )
        serialized_params = params.serialize()
        assert (
            json.loads(serialized_params)["compacted_file_content_type"]
            == "my-json-object"
        )

    def test_json_handler_for_compact_partition_params_serializes_set_to_list(self):
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        my_set = {1, 2, 3}
        json_string = json.dumps(
            my_set,
            default=CompactPartitionParams.json_handler_for_compact_partition_params,
        )
        assert json.loads(json_string) == [1, 2, 3]

    def test_json_handler_for_compact_partition_params_serializes_object_with_toJSON_method_to_dict(
        self,
    ):
        from dataclasses import dataclass

        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        @dataclass
        class DummyObject:
            some_property: str = "foo"

            def toJSON(self):
                return self.__dict__

        dummy_object = DummyObject()
        json_string = json.dumps(
            dummy_object,
            default=CompactPartitionParams.json_handler_for_compact_partition_params,
        )
        assert json.loads(json_string) == {"some_property": "foo"}

    def test_json_handler_for_compact_partition_params_returns_class_name_for_unknown_objects(
        self,
    ):
        from deltacat.compute.compactor.model.compact_partition_params import (
            CompactPartitionParams,
        )

        my_object = object()
        json_string = json.dumps(
            my_object,
            default=CompactPartitionParams.json_handler_for_compact_partition_params,
        )
        assert json.loads(json_string) == "object"
