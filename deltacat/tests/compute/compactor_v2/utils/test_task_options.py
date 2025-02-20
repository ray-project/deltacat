import unittest
import ray
from deltacat.compute.compactor_v2.utils.task_options import (
    _get_task_options,
    _get_merge_task_options,
    logger,
)
from deltacat.compute.resource_estimation.model import (
    EstimateResourcesParams,
    ResourceEstimationMethod,
)
from deltacat.constants import PYARROW_INFLATION_MULTIPLIER
from deltacat.compute.compactor import (
    PyArrowWriteResult,
    RoundCompletionInfo,
)
from deltacat.types.media import (
    ContentType,
    ContentEncoding,
)
from deltacat.storage import (
    DeltaLocator,
    Manifest,
    ManifestMeta,
    ManifestEntry,
    ManifestEntryList,
    PartitionValues,
)
from unittest.mock import MagicMock
from typing import Optional

from deltacat.compute.compactor_v2.constants import (
    AVERAGE_RECORD_SIZE_BYTES as DEFAULT_AVERAGE_RECORD_SIZE_BYTES,
)


@ray.remote
def valid_func():
    return 2


@ray.remote
def throwing_func():
    raise ConnectionAbortedError()


class TestTaskOptions(unittest.TestCase):
    TEST_INDEX = 0
    TEST_HB_GROUP_IDX = 0
    TEST_STREAM_POSITION = 1_000_000
    TEST_NUM_HASH_GROUPS = 1

    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def _make_estimate_resource_params(
        cls,
        resource_estimation_method: Optional[
            ResourceEstimationMethod
        ] = ResourceEstimationMethod.DEFAULT,
        previous_inflation: Optional[int] = 7,
        average_record_size_bytes: Optional[int] = 1000,
    ):
        return EstimateResourcesParams.of(
            resource_estimation_method=resource_estimation_method,
            previous_inflation=previous_inflation,
            average_record_size_bytes=average_record_size_bytes,
        )

    def _make_manifest(
        self,
        source_content_length: Optional[int] = 1000,
        content_type: Optional[ContentType] = ContentType.PARQUET,
        content_encoding: Optional[ContentEncoding] = ContentEncoding.IDENTITY,
        partition_values: Optional[PartitionValues] = None,
        uri: Optional[str] = "test",
        url: Optional[str] = "test",
        author: Optional[str] = "foo",
        entry_uuid: Optional[str] = "foo",
        manifest_uuid: Optional[str] = "bar",
    ) -> Manifest:
        meta = ManifestMeta.of(
            10,
            10,
            content_type=content_type,
            content_encoding=content_encoding,
            source_content_length=source_content_length,
            partition_values=partition_values,
        )

        return Manifest.of(
            entries=ManifestEntryList.of(
                [
                    ManifestEntry.of(
                        uri=uri, url=url, meta=meta, mandatory=True, uuid=entry_uuid
                    )
                ]
            ),
            author=author,
            uuid=manifest_uuid,
        )

    def make_round_completion_info(
        self,
        high_watermark: Optional[int] = 1_000_000,
        compacted_delta_locator: Optional[DeltaLocator] = None,
        records_written: Optional[int] = 10,
        bytes_written: Optional[int] = 10,
        files_written: Optional[int] = 10,
        rows_dropped: Optional[int] = 10,
        sort_keys_bit_width: Optional[int] = 0,
        hash_bucket_count: Optional[int] = 1,
        hb_index_to_entry_range: Optional[dict] = None,
    ) -> RoundCompletionInfo:
        if compacted_delta_locator is None:
            compacted_delta_locator = MagicMock(spec=DeltaLocator)

        hb_index_to_entry_range = hb_index_to_entry_range or {"0": (0, 1)}

        return RoundCompletionInfo.of(
            compacted_delta_locator=compacted_delta_locator,
            high_watermark=high_watermark,
            compacted_pyarrow_write_result=PyArrowWriteResult.of(
                records_written, bytes_written, files_written, rows_dropped
            ),
            sort_keys_bit_width=sort_keys_bit_width,
            hb_index_to_entry_range=hb_index_to_entry_range,
            hash_bucket_count=hash_bucket_count,
        )

    def test_get_task_options_sanity(self):
        opts = _get_task_options(0.01, 0.01)
        result_ref = valid_func.options(**opts).remote()
        result = ray.get(result_ref)

        self.assertEqual(result, 2)

    def test_get_task_options_when_exception_is_thrown(self):
        opts = _get_task_options(0.01, 0.01)
        result_ref = throwing_func.options(**opts).remote()

        self.assertRaises(ConnectionAbortedError, lambda: ray.get(result_ref))

    def test_get_merge_task_options_memory_logs_enabled_sanity(self):
        test_index = 0
        test_hb_group_idx = 0
        test_debug_memory_params = {"merge_task_index": test_index}
        test_estimate_memory_params = self._make_estimate_resource_params()
        test_ray_custom_resources = {}
        test_rcf = self.make_round_completion_info()
        test_manifest = self._make_manifest()
        expected_task_opts = {
            "max_retries": 3,
            "memory": 1680.64,
            "num_cpus": 0.01,
            "scheduling_strategy": "SPREAD",
        }
        expected_previous_inflation = 1.0
        expected_average_record_size = 1.0
        with self.assertLogs(logger=logger.name, level="DEBUG") as cm:
            # At least one log of level DEBUG must be emitted
            actual_merge_tasks_opts = _get_merge_task_options(
                index=test_index,
                hb_group_idx=test_hb_group_idx,
                data_size=1,
                pk_size_bytes=1,
                num_rows=1,
                num_hash_groups=1,
                total_memory_buffer_percentage=1,
                incremental_index_array_size=1,
                debug_memory_params=test_debug_memory_params,
                ray_custom_resources=test_ray_custom_resources,
                estimate_resources_params=test_estimate_memory_params,
                round_completion_info=test_rcf,
                compacted_delta_manifest=test_manifest,
                memory_logs_enabled=True,
            )
            assert {k: actual_merge_tasks_opts[k] for k in expected_task_opts}
        log_message_round_completion_info = cm.records[0].getMessage()
        log_message_debug_memory_params = cm.records[1].getMessage()
        self.assertIn(
            f"[Merge task {test_index}]: Using previous compaction rounds to calculate merge memory",
            log_message_round_completion_info,
        )
        self.assertIn(
            f"[Merge task {test_index}]: Params used for calculating merge memory",
            log_message_debug_memory_params,
        )
        self.assertIn(
            f"'previous_inflation': {expected_previous_inflation}",
            log_message_debug_memory_params,
        )
        self.assertIn(
            f"'average_record_size': {expected_average_record_size}",
            log_message_debug_memory_params,
        )

    def test_get_merge_task_options_memory_logs_enabled_fallback_previous_inflation_fallback_average_record_size(
        self,
    ):
        test_index = 0
        test_hb_group_idx = 0
        test_debug_memory_params = {"merge_task_index": test_index}
        test_estimate_memory_params = self._make_estimate_resource_params()
        test_ray_custom_resources = {}
        test_rcf = self.make_round_completion_info(
            bytes_written=0, records_written=0, files_written=0, rows_dropped=0
        )
        test_manifest = self._make_manifest()
        expected_task_opts = {
            "max_retries": 3,
            "memory": 1680.64,
            "num_cpus": 0.01,
            "scheduling_strategy": "SPREAD",
        }
        expected_previous_inflation = PYARROW_INFLATION_MULTIPLIER
        expected_average_record_size = DEFAULT_AVERAGE_RECORD_SIZE_BYTES
        with self.assertLogs(logger=logger.name, level="DEBUG") as cm:
            # At least one log of level DEBUG must be emitted
            actual_merge_tasks_opts = _get_merge_task_options(
                index=test_index,
                hb_group_idx=test_hb_group_idx,
                data_size=1,
                pk_size_bytes=1,
                num_rows=1,
                num_hash_groups=1,
                total_memory_buffer_percentage=1,
                incremental_index_array_size=1,
                debug_memory_params=test_debug_memory_params,
                ray_custom_resources=test_ray_custom_resources,
                estimate_resources_params=test_estimate_memory_params,
                round_completion_info=test_rcf,
                compacted_delta_manifest=test_manifest,
                memory_logs_enabled=True,
            )
            assert {k: actual_merge_tasks_opts[k] for k in expected_task_opts}
        log_message_round_completion_info = cm.records[0].getMessage()
        log_message_debug_memory_params = cm.records[1].getMessage()
        self.assertIn(
            f"[Merge task {test_index}]: Using previous compaction rounds to calculate merge memory",
            log_message_round_completion_info,
        )
        self.assertIn(
            f"[Merge task {test_index}]: Params used for calculating merge memory",
            log_message_debug_memory_params,
        )
        self.assertIn(
            f"'previous_inflation': {expected_previous_inflation}",
            log_message_debug_memory_params,
        )
        self.assertIn(
            f"'average_record_size': {expected_average_record_size}",
            log_message_debug_memory_params,
        )

    def test_get_merge_task_options_memory_logs_enabled_not_using_previous_round_completion_info(
        self,
    ):
        test_index = 0
        test_hb_group_idx = 0
        test_debug_memory_params = {"merge_task_index": test_index}
        test_estimate_memory_params = self._make_estimate_resource_params()
        test_ray_custom_resources = {}
        test_rcf = None
        test_manifest = self._make_manifest()
        expected_task_opts = {
            "max_retries": 3,
            "memory": 1680.64,
            "num_cpus": 0.01,
            "scheduling_strategy": "SPREAD",
        }
        with self.assertLogs(logger=logger.name, level="DEBUG") as cm:
            # At least one log of level DEBUG must be emitted
            actual_merge_tasks_opts = _get_merge_task_options(
                index=test_index,
                hb_group_idx=test_hb_group_idx,
                data_size=1,
                pk_size_bytes=1,
                num_rows=1,
                num_hash_groups=1,
                total_memory_buffer_percentage=1,
                incremental_index_array_size=1,
                debug_memory_params=test_debug_memory_params,
                ray_custom_resources=test_ray_custom_resources,
                estimate_resources_params=test_estimate_memory_params,
                round_completion_info=test_rcf,
                compacted_delta_manifest=test_manifest,
                memory_logs_enabled=True,
            )
            assert {k: actual_merge_tasks_opts[k] for k in expected_task_opts}
        log_message_debug_memory_params = cm.records[0].getMessage()
        self.assertIn(
            f"[Merge task {test_index}]: Params used for calculating merge memory",
            log_message_debug_memory_params,
        )
        self.assertNotIn(
            "'average_record_size'",
            log_message_debug_memory_params,
        )
