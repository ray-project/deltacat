from __future__ import annotations
import importlib
import copy
import json
from typing import Any, Dict, List, Optional
from deltacat.io.object_store import IObjectStore
from deltacat.utils.common import ReadKwargsProvider
from deltacat.types.media import ContentType
from deltacat.utils.placement import PlacementGroupConfig
from deltacat.io.ray_plasma_object_store import RayPlasmaObjectStore
from deltacat.storage import (
    interface as unimplemented_deltacat_storage,
    PartitionLocator,
    SortKey,
)
from deltacat.compute.resource_estimation import (
    ResourceEstimationMethod,
    EstimateResourcesParams,
)
from deltacat.compute.compactor_v2.constants import (
    MAX_RECORDS_PER_COMPACTED_FILE,
    MIN_DELTA_BYTES_IN_BATCH,
    MIN_FILES_IN_BATCH,
    AVERAGE_RECORD_SIZE_BYTES,
    TASK_MAX_PARALLELISM,
    DROP_DUPLICATES,
    TOTAL_MEMORY_BUFFER_PERCENTAGE,
    DEFAULT_DISABLE_COPY_BY_REFERENCE,
    DEFAULT_NUM_ROUNDS,
    PARQUET_TO_PYARROW_INFLATION,
    MAX_PARQUET_METADATA_SIZE,
)
from deltacat.constants import PYARROW_INFLATION_MULTIPLIER
from deltacat.compute.compactor.utils.sort_key import validate_sort_keys
from deltacat.utils.metrics import MetricsConfig


class CompactPartitionParams(dict):
    """
    This class represents the parameters passed to compact_partition (deltacat/compute/compactor/compaction_session.py)
    """

    @staticmethod
    def of(params: Optional[Dict]) -> CompactPartitionParams:
        params = {} if params is None else params
        assert (
            params.get("destination_partition_locator") is not None
        ), "destination_partition_locator is a required arg"
        assert (
            params.get("last_stream_position_to_compact") is not None
        ), "last_stream_position_to_compact is a required arg"
        assert (
            params.get("source_partition_locator") is not None
        ), "source_partition_locator is a required arg"
        assert (
            params.get("compaction_artifact_s3_bucket") is not None
        ), "compaction_artifact_s3_bucket is a required arg"

        result = CompactPartitionParams(params)

        result.records_per_compacted_file = params.get(
            "records_per_compacted_file", MAX_RECORDS_PER_COMPACTED_FILE
        )
        result.compacted_file_content_type = params.get(
            "compacted_file_content_type", ContentType.PARQUET
        )
        result.object_store = params.get("object_store", RayPlasmaObjectStore())

        result.enable_profiler = params.get("enable_profiler", False)
        result.deltacat_storage = params.get(
            "deltacat_storage", unimplemented_deltacat_storage
        )
        result.s3_client_kwargs = params.get("s3_client_kwargs", {})
        result.deltacat_storage_kwargs = params.get("deltacat_storage_kwargs", {})
        result.list_deltas_kwargs = params.get("list_deltas_kwargs", {})
        result.s3_table_writer_kwargs = params.get("s3_table_writer_kwargs", {})
        result.bit_width_of_sort_keys = validate_sort_keys(
            result.source_partition_locator,
            result.sort_keys,
            result.deltacat_storage,
            result.deltacat_storage_kwargs,
        )
        result.task_max_parallelism = params.get(
            "task_max_parallelism", TASK_MAX_PARALLELISM
        )
        result.min_files_in_batch = params.get("min_files_in_batch", MIN_FILES_IN_BATCH)
        result.min_delta_bytes_in_batch = params.get(
            "min_delta_bytes_in_batch", MIN_DELTA_BYTES_IN_BATCH
        )
        result.previous_inflation = params.get(
            "previous_inflation", PYARROW_INFLATION_MULTIPLIER
        )
        result.average_record_size_bytes = params.get(
            "average_record_size_bytes", AVERAGE_RECORD_SIZE_BYTES
        )
        result.total_memory_buffer_percentage = params.get(
            "total_memory_buffer_percentage", TOTAL_MEMORY_BUFFER_PERCENTAGE
        )
        result.hash_group_count = params.get(
            "hash_group_count", result.hash_bucket_count
        )
        result.disable_copy_by_reference = params.get(
            "disable_copy_by_reference", DEFAULT_DISABLE_COPY_BY_REFERENCE
        )
        result.drop_duplicates = params.get("drop_duplicates", DROP_DUPLICATES)
        result.ray_custom_resources = params.get("ray_custom_resources")

        result.memory_logs_enabled = params.get("memory_logs_enabled", False)

        result.metrics_config = params.get("metrics_config")

        result.num_rounds = params.get("num_rounds", DEFAULT_NUM_ROUNDS)
        result.parquet_to_pyarrow_inflation = params.get(
            "parquet_to_pyarrow_inflation", PARQUET_TO_PYARROW_INFLATION
        )
        result.resource_estimation_method = ResourceEstimationMethod[
            params.get(
                "resource_estimation_method", ResourceEstimationMethod.DEFAULT.value
            )
        ]

        # disable input split during rebase as the rebase files are already uniform
        result.enable_input_split = (
            params.get("rebase_source_partition_locator") is None
        )
        result.max_parquet_meta_size_bytes = params.get(
            "max_parquet_meta_size_bytes", MAX_PARQUET_METADATA_SIZE
        )

        if not importlib.util.find_spec("memray"):
            result.enable_profiler = False

        if result.primary_keys:
            result.primary_keys = sorted(result.primary_keys)

        # assertions
        assert (
            result.source_partition_locator.partition_values
            == result.destination_partition_locator.partition_values
        ), "Source and destination partitions values must be equal"

        assert (
            result.records_per_compacted_file and result.records_per_compacted_file >= 1
        ), "Max records per output file must be a positive value"

        return result

    @property
    def destination_partition_locator(self) -> PartitionLocator:
        val = self["destination_partition_locator"]
        if not isinstance(val, PartitionLocator):
            val = PartitionLocator(val)

        return val

    @destination_partition_locator.setter
    def destination_partition_locator(self, locator: PartitionLocator) -> None:
        self["destination_partition_locator"] = locator

    @property
    def last_stream_position_to_compact(self) -> int:
        return self["last_stream_position_to_compact"]

    @last_stream_position_to_compact.setter
    def last_stream_position_to_compact(self, stream_position: int) -> None:
        self["last_stream_position_to_compact"] = stream_position

    @property
    def source_partition_locator(self) -> PartitionLocator:
        val = self["source_partition_locator"]
        if not isinstance(val, PartitionLocator):
            val = PartitionLocator(val)
        return val

    @source_partition_locator.setter
    def source_partition_locator(self, locator: PartitionLocator) -> None:
        self["source_partition_locator"] = locator

    @property
    def compaction_artifact_s3_bucket(self) -> str:
        return self["compaction_artifact_s3_bucket"]

    @compaction_artifact_s3_bucket.setter
    def compaction_artifact_s3_bucket(self, s3_bucket: str) -> None:
        self["compaction_artifact_s3_bucket"] = s3_bucket

    @property
    def deltacat_storage(self) -> unimplemented_deltacat_storage:
        return self["deltacat_storage"]

    @deltacat_storage.setter
    def deltacat_storage(self, storage: unimplemented_deltacat_storage) -> None:
        self["deltacat_storage"] = storage

    @property
    def object_store(self) -> IObjectStore:
        return self["object_store"]

    @object_store.setter
    def object_store(self, obj_store: IObjectStore) -> None:
        self["object_store"] = obj_store

    @property
    def compacted_file_content_type(self) -> ContentType:
        return self["compacted_file_content_type"]

    @compacted_file_content_type.setter
    def compacted_file_content_type(self, content_type: ContentType) -> None:
        self["compacted_file_content_type"] = content_type

    @property
    def task_max_parallelism(self) -> int:
        if self.pg_config:
            cluster_resources = self.pg_config.resource
            cluster_cpus = cluster_resources["CPU"]
            self.task_max_parallelism = cluster_cpus
        return self["task_max_parallelism"]

    @task_max_parallelism.setter
    def task_max_parallelism(self, max_parallelism: int) -> None:
        self["task_max_parallelism"] = max_parallelism

    @property
    def average_record_size_bytes(self) -> float:
        return self["average_record_size_bytes"]

    @average_record_size_bytes.setter
    def average_record_size_bytes(self, average_record_size_bytes: float) -> None:
        self["average_record_size_bytes"] = average_record_size_bytes

    @property
    def total_memory_buffer_percentage(self) -> int:
        return self["total_memory_buffer_percentage"]

    @total_memory_buffer_percentage.setter
    def total_memory_buffer_percentage(
        self, total_memory_buffer_percentage: int
    ) -> None:
        self["total_memory_buffer_percentage"] = total_memory_buffer_percentage

    @property
    def min_files_in_batch(self) -> float:
        return self["min_files_in_batch"]

    @min_files_in_batch.setter
    def min_files_in_batch(self, min_files_in_batch: float) -> None:
        self["min_files_in_batch"] = min_files_in_batch

    @property
    def min_delta_bytes_in_batch(self) -> float:
        return self["min_delta_bytes_in_batch"]

    @min_delta_bytes_in_batch.setter
    def min_delta_bytes_in_batch(self, min_delta_bytes_in_batch: float) -> None:
        self["min_delta_bytes_in_batch"] = min_delta_bytes_in_batch

    @property
    def previous_inflation(self) -> float:
        return self["previous_inflation"]

    @previous_inflation.setter
    def previous_inflation(self, previous_inflation: float) -> None:
        self["previous_inflation"] = previous_inflation

    @property
    def enable_profiler(self) -> bool:
        return self["enable_profiler"]

    @enable_profiler.setter
    def enable_profiler(self, value: bool) -> None:
        self["enable_profiler"] = value

    @property
    def disable_copy_by_reference(self) -> bool:
        return self["disable_copy_by_reference"]

    @disable_copy_by_reference.setter
    def disable_copy_by_reference(self, value: bool) -> None:
        self["disable_copy_by_reference"] = value

    @property
    def list_deltas_kwargs(self) -> dict:
        return self["list_deltas_kwargs"]

    @list_deltas_kwargs.setter
    def list_deltas_kwargs(self, kwargs: dict) -> None:
        self["list_deltas_kwargs"] = kwargs

    @property
    def s3_table_writer_kwargs(self) -> dict:
        return self["s3_table_writer_kwargs"]

    @s3_table_writer_kwargs.setter
    def s3_table_writer_kwargs(self, kwargs: dict) -> None:
        self["s3_table_writer_kwargs"] = kwargs

    @property
    def deltacat_storage_kwargs(self) -> dict:
        return self["deltacat_storage_kwargs"]

    @deltacat_storage_kwargs.setter
    def deltacat_storage_kwargs(self, kwargs: dict) -> None:
        self["deltacat_storage_kwargs"] = kwargs

    @property
    def s3_client_kwargs(self) -> dict:
        return self["s3_client_kwargs"]

    @s3_client_kwargs.setter
    def s3_client_kwargs(self, kwargs: dict) -> None:
        self["s3_client_kwargs"] = kwargs

    @property
    def records_per_compacted_file(self) -> int:
        return self["records_per_compacted_file"]

    @records_per_compacted_file.setter
    def records_per_compacted_file(self, count: int) -> None:
        self["records_per_compacted_file"] = count

    @property
    def drop_duplicates(self) -> bool:
        return self["drop_duplicates"]

    @drop_duplicates.setter
    def drop_duplicates(self, value: bool):
        self["drop_duplicates"] = value

    @property
    def bit_width_of_sort_keys(self) -> int:
        return self["bit_width_of_sort_keys"]

    @bit_width_of_sort_keys.setter
    def bit_width_of_sort_keys(self, width: int) -> None:
        self["bit_width_of_sort_keys"] = width

    @property
    def hash_bucket_count(self) -> Optional[int]:
        return self.get("hash_bucket_count")

    @hash_bucket_count.setter
    def hash_bucket_count(self, count: int) -> None:
        self["hash_bucket_count"] = count

    @property
    def hash_group_count(self) -> int:
        return self["hash_group_count"]

    @property
    def ray_custom_resources(self) -> Dict:
        return self["ray_custom_resources"]

    @ray_custom_resources.setter
    def ray_custom_resources(self, res) -> None:
        self["ray_custom_resources"] = res

    @hash_group_count.setter
    def hash_group_count(self, count: int) -> None:
        self["hash_group_count"] = count

    @property
    def primary_keys(self) -> Optional[List[str]]:
        return self.get("primary_keys")

    @primary_keys.setter
    def primary_keys(self, keys: List[str]) -> None:
        self["primary_keys"] = keys

    @property
    def rebase_source_partition_locator(self) -> Optional[PartitionLocator]:
        val = self.get("rebase_source_partition_locator")

        if val and not isinstance(val, PartitionLocator):
            val = PartitionLocator(val)

        return val

    @rebase_source_partition_locator.setter
    def rebase_source_partition_locator(self, locator: PartitionLocator) -> None:
        self["rebase_source_partition_locator"] = locator

    @property
    def rebase_source_partition_high_watermark(self) -> Optional[int]:
        return self.get("rebase_source_partition_high_watermark")

    @rebase_source_partition_high_watermark.setter
    def rebase_source_partition_high_watermark(self, high_watermark: int) -> None:
        self["rebase_source_partition_high_watermark"] = high_watermark

    @property
    def pg_config(self) -> Optional[PlacementGroupConfig]:
        return self.get("pg_config")

    @pg_config.setter
    def pg_config(self, config: PlacementGroupConfig) -> None:
        self["pg_config"] = config

    @property
    def read_kwargs_provider(self) -> Optional[ReadKwargsProvider]:
        return self.get("read_kwargs_provider")

    @read_kwargs_provider.setter
    def read_kwargs_provider(self, kwargs_provider: ReadKwargsProvider) -> None:
        self["read_kwargs_provider"] = kwargs_provider

    @property
    def sort_keys(self) -> Optional[List[SortKey]]:
        return self.get("sort_keys")

    @sort_keys.setter
    def sort_keys(self, keys: List[SortKey]) -> None:
        self["sort_keys"] = keys

    @property
    def memory_logs_enabled(self) -> bool:
        return self.get("memory_logs_enabled")

    @memory_logs_enabled.setter
    def memory_logs_enabled(self, value: bool) -> None:
        self["memory_logs_enabled"] = value

    @property
    def metrics_config(self) -> Optional[MetricsConfig]:
        return self.get("metrics_config")

    @metrics_config.setter
    def metrics_config(self, config: MetricsConfig) -> None:
        self["metrics_config"] = config

    @property
    def num_rounds(self) -> int:
        return self["num_rounds"]

    @num_rounds.setter
    def num_rounds(self, num_rounds: int) -> None:
        self["num_rounds"] = num_rounds

    @property
    def parquet_to_pyarrow_inflation(self) -> float:
        """
        The inflation factor for the parquet uncompressed_size_bytes to pyarrow table size.
        """
        return self["parquet_to_pyarrow_inflation"]

    @parquet_to_pyarrow_inflation.setter
    def parquet_to_pyarrow_inflation(self, value: float) -> None:
        self["parquet_to_pyarrow_inflation"] = value

    @property
    def enable_input_split(self) -> bool:
        """
        When this is True, the input split will be always enabled for parquet files.
        The input split feature will split the parquet files into individual row groups
        so that we could process them in different nodes in parallel.
        By default, input split is enabled for incremental compaction and disabled for rebase or backfill.
        """
        return self["enable_input_split"]

    @enable_input_split.setter
    def enable_input_split(self, value: bool) -> None:
        self["enable_input_split"] = value

    @property
    def max_parquet_meta_size_bytes(self) -> int:
        """
        The maximum size of the parquet metadata in bytes. Used for allocating tasks
        to fetch parquet metadata.
        """
        return self["max_parquet_meta_size_bytes"]

    @max_parquet_meta_size_bytes.setter
    def max_parquet_meta_size_bytes(self, value: int) -> None:
        self["max_parquet_meta_size_bytes"] = value

    @property
    def resource_estimation_method(self) -> ResourceEstimationMethod:
        return self["resource_estimation_method"]

    @resource_estimation_method.setter
    def resource_estimation_method(self, value: ResourceEstimationMethod) -> None:
        self["resource_estimation_method"] = value

    @property
    def estimate_resources_params(self) -> EstimateResourcesParams:
        return EstimateResourcesParams.of(
            resource_estimation_method=self.resource_estimation_method,
            previous_inflation=self.previous_inflation,
            parquet_to_pyarrow_inflation=self.parquet_to_pyarrow_inflation,
            average_record_size_bytes=self.average_record_size_bytes,
        )

    @staticmethod
    def json_handler_for_compact_partition_params(obj):
        """
        A handler for the `json.dumps()` function that can be used to serialize sets to JSON.
        If the `set_default()` handler is passed as the `default` argument to the `json.dumps()` function, it will be called whenever a set object is encountered.
        The `set_default()` handler will then serialize the set as a list.
        """
        try:
            if isinstance(obj, set):
                return list(obj)
            elif hasattr(obj, "toJSON"):
                return obj.toJSON()
            else:
                return obj.__dict__
        except Exception:
            return obj.__class__.__name__

    def serialize(self) -> str:
        """
        Serializes itself to a json-formatted string

        Returns:
            The serialized object.

        """
        to_serialize: Dict[str, Any] = {}
        # individually try deepcopy the values from the self dictionary and just use the class name for the value when it is not possible to deepcopy
        for attr, value in self.items():
            try:
                to_serialize[attr] = copy.deepcopy(value)
            except Exception:  # if unable to deep copy the objects like module objects for example then just provide the class name at minimum
                to_serialize[attr] = value.__class__.__name__
        serialized_arguments_compact_partition_args: str = json.dumps(
            to_serialize,
            default=CompactPartitionParams.json_handler_for_compact_partition_params,
        )
        return serialized_arguments_compact_partition_args
