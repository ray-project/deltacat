# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
from typing import Optional
import pyarrow as pa
import logging
from deltacat import logs
from typing import List, Union
from deltacat.compute.compactor.model.hash_bucket_result import HashBucketResult
from deltacat.compute.compactor.model.dedupe_result import DedupeResult
from deltacat.compute.compactor.model.materialize_result import MaterializeResult
from deltacat.utils.performance import timed_invocation
from deltacat.utils.resources import ClusterUtilization, get_size_of_object_in_bytes
from deltacat.compute.compactor import PyArrowWriteResult

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class CompactionSessionAuditInfo(dict):

    DEDUPE_STEP_NAME = "dedupe"
    MATERIALIZE_STEP_NAME = "materialize"
    HASH_BUCKET_STEP_NAME = "hashBucket"
    MERGE_STEP_NAME = "merge"

    def __init__(
        self,
        deltacat_version: Optional[str] = None,
        ray_version: Optional[str] = None,
        audit_url: Optional[str] = None,
        **kwargs,
    ):
        self.set_deltacat_version(deltacat_version)
        self.set_ray_version(ray_version)
        self.set_audit_url(audit_url)
        if kwargs:
            self.update(kwargs)

    @property
    def audit_url(self) -> str:
        return self.get("auditUrl")

    @property
    def deltacat_version(self) -> str:
        """
        The deltacat version used to run compaction job.
        """
        return self.get("deltacatVersion")

    @property
    def ray_version(self) -> str:
        """
        The ray distribution version used to run compaction job.
        """
        return self.get("rayVersion")

    @property
    def input_records(self) -> int:
        """
        The total number of records from input deltas that needs to be compacted
        (before deduplication).
        """
        return self.get("inputRecords")

    @property
    def input_file_count(self) -> int:
        """
        The total number of input files that needs to be compacted.
        """
        return self.get("inputFileCount")

    @property
    def uniform_deltas_created(self) -> int:
        """
        The total number of uniform deltas fed into the hash bucket step.
        """
        return self.get("uniformDeltasCreated")

    @property
    def records_deduped(self) -> int:
        """
        The total number of records that were deduplicated. For example,
        if there are 100 records with a particular primary key, 99 records
        will be deduplicated.
        """
        return self.get("recordsDeduped")

    @property
    def records_deleted(self) -> int:
        """
        The total count of deleted records in a compaction session if delete deltas are present.
        """
        return self.get("recordsDeleted")

    @property
    def input_size_bytes(self) -> float:
        """
        The on-disk size in bytes of the input. Analogous to bytes scanned
        """
        return self.get("inputSizeBytes")

    @property
    def hash_bucket_count(self) -> int:
        """
        Total number of hash buckets used during compaction.
        """
        return self.get("hashBucketCount")

    @property
    def compaction_time_in_seconds(self) -> float:
        """
        The total time taken by the compaction session to complete.
        """
        return self.get("compactionTimeInSeconds")

    @property
    def total_object_store_memory_used_bytes(self) -> float:
        """
        The total object store memory used by the compaction session across all
        nodes in the entire cluster.
        """
        return self.get("totalObjectStoreMemoryUsedBytes")

    @property
    def peak_memory_used_bytes_per_task(self) -> float:
        """
        The peak memory used by a single process in the compaction job. Note that
        Ray creates a single process to run each hash bucketing, dedupe and
        materialize task and the process is reused. Hence, you may see
        monotonically increasing values. Peak memory is important because,
        the cluster must be scaled to handle  the peak memory per node even
        though average memory usage is low.
        """
        return self.get("peakMemoryUsedBytesPerTask")

    @property
    def peak_memory_used_bytes_per_hash_bucket_task(self) -> float:
        """
        The peak memory used by a single hash bucketing process. For example,
        if peak usage of hash bucketing process is 40GB, it is not safe to run
        more than 3 hash bucketing tasks on a node with 120GB to avoid crashing
        due to memory overflow.
        """
        return self.get("hashBucketTaskPeakMemoryUsedBytes")

    @property
    def peak_memory_used_bytes_per_dedupe_task(self) -> float:
        """
        The peak memory used by a single dedupe python process. Note that
        results may be max of dedupe and hash bucketing as processes are
        reused by Ray to run dedupe and hash bucketing.
        """
        return self.get("dedupeTaskPeakMemoryUsedBytes")

    @property
    def peak_memory_used_bytes_per_materialize_task(self) -> float:
        """
        The peak memory used by a single materialize python process. Note
        that results may be max of materialize, dedupe and hash bucketing as
        processes are reused by Ray to run all compaction steps.
        """
        return self.get("materializeTaskPeakMemoryUsedBytes")

    @property
    def peak_memory_used_bytes_per_merge_task(self) -> float:
        """
        The peak memory used by a single merge python process. Note
        that results may be max of merge, and hash bucketing as
        processes are reused by Ray to run all compaction steps.
        """
        return self.get("mergeTaskPeakMemoryUsedBytes")

    @property
    def hash_bucket_post_object_store_memory_used_bytes(self) -> float:
        """
        The total object store memory used by hash bucketing step across
        cluster, before dedupe is run.
        """
        return self.get("hashBucketPostObjectStoreMemoryUsedBytes")

    @property
    def dedupe_post_object_store_memory_used_bytes(self) -> float:
        """
        The total object store memory used after dedupe step before materialize is run.
        """
        return self.get("dedupePostObjectStoreMemoryUsedBytes")

    @property
    def materialize_post_object_store_memory_used_bytes(self) -> float:
        """
        The total object store memory used after materialize step.
        """
        return self.get("materializePostObjectStoreMemoryUsedBytes")

    @property
    def merge_post_object_store_memory_used_bytes(self) -> float:
        """
        The total object store memory used after merge step.
        """
        return self.get("mergePostObjectStoreMemoryUsedBytes")

    @property
    def materialize_buckets(self) -> int:
        """
        The total number of materialize buckets created.
        """
        return self.get("materializeBuckets")

    @property
    def hash_bucket_time_in_seconds(self) -> float:
        """
        The time taken by hash bucketing step. This includes all hash bucket tasks.
        This includes invoke time.
        """
        return self.get("hashBucketTimeInSeconds")

    @property
    def hash_bucket_invoke_time_in_seconds(self) -> float:
        """
        The time taken to invoke and create all hash bucketing tasks.
        """
        return self.get("hashBucketInvokeTimeInSeconds")

    @property
    def hash_bucket_result_wait_time_in_seconds(self) -> float:
        """
        The time it takes ray.get() to resolve after the last hash bucket task has completed.
        This value may not be accurate at less than 1 second precision.
        """
        return self.get("hashBucketResultWaitTimeInSeconds")

    @property
    def dedupe_time_in_seconds(self) -> float:
        """
        The time taken by dedupe step. This include all dedupe tasks.
        """
        return self.get("dedupeTimeInSeconds")

    @property
    def dedupe_invoke_time_in_seconds(self) -> float:
        """
        The time taken to invoke all dedupe tasks.
        """
        return self.get("dedupeInvokeTimeInSeconds")

    @property
    def dedupe_result_wait_time_in_seconds(self) -> float:
        """
        The time it takes ray.get() to resolve after the last dedupe task has completed.
        This value may not be accurate at less than 1 second precision.
        """
        return self.get("dedupeResultWaitTimeInSeconds")

    @property
    def materialize_time_in_seconds(self) -> float:
        """
        The time taken by materialize step. This includes all materialize tasks.
        """
        return self.get("materializeTimeInSeconds")

    @property
    def materialize_invoke_time_in_seconds(self) -> float:
        """
        The time taken to invoke all materialize tasks.
        """
        return self.get("materializeInvokeTimeInSeconds")

    @property
    def materialize_result_wait_time_in_seconds(self) -> float:
        """
        The time it takes ray.get() to resolve after the last materialize task has completed.
        This value may not be accurate at less than 1 second precision.
        """
        return self.get("materializeResultWaitTimeInSeconds")

    @property
    def merge_result_wait_time_in_seconds(self) -> float:
        """
        The time it takes ray.get() to resolve after the last task has completed.
        This value may not be accurate at less than 1 second precision.
        """
        return self.get("mergeResultWaitTimeInSeconds")

    @property
    def merge_time_in_seconds(self) -> float:
        """
        The time taken by merge step. This includes all merge tasks.
        """
        return self.get("mergeTimeInSeconds")

    @property
    def merge_invoke_time_in_seconds(self) -> float:
        """
        The time taken to invoke all merge tasks.
        """
        return self.get("mergeInvokeTimeInSeconds")

    @property
    def delta_discovery_time_in_seconds(self) -> float:
        """
        The time taken by delta discovery step which is mostly run before hash bucketing is started.
        """
        return self.get("deltaDiscoveryTimeInSeconds")

    @property
    def output_file_count(self) -> int:
        """
        The total number of files in the compacted output (includes untouched files).
        """
        return self.get("outputFileCount")

    @property
    def output_size_bytes(self) -> float:
        """
        The on-disk size of the compacted output including any untouched files.
        """
        return self.get("outputSizeBytes")

    @property
    def output_size_pyarrow_bytes(self) -> float:
        """
        The pyarrow in-memory size of compacted output including any untouched files.
        """
        return self.get("outputSizePyarrowBytes")

    @property
    def total_cluster_memory_bytes(self) -> float:
        """
        The total memory allocated to the cluster.
        """
        return self.get("totalClusterMemoryBytes")

    @property
    def total_cluster_object_store_memory_bytes(self) -> float:
        """
        The total object store memory allocated to the cluster.
        """
        return self.get("totalClusterObjectStoreMemoryBytes")

    @property
    def untouched_file_count(self) -> int:
        """
        The total number of files that were untouched by materialize step.
        """
        return self.get("untouchedFileCount")

    @property
    def untouched_file_ratio(self) -> float:
        """
        The ratio between total number of files untouched and total number of files in the compacted output.
        """
        return self.get("untouchedFileRatio")

    @property
    def untouched_record_count(self) -> int:
        """
        The total number of records untouched during materialization.
        """
        return self.get("untouchedRecordCount")

    @property
    def untouched_size_bytes(self) -> float:
        """
        The on-disk size of the data untouched during materialization.
        """
        return self.get("untouchedSizeBytes")

    @property
    def telemetry_time_in_seconds(self) -> float:
        """
        The total time taken by all the telemetry activity across the nodes in the cluster. This includes
        collecting cluster resources information, emitting metrics, etc.
        """
        return self.get("telemetryTimeInSeconds")

    @property
    def hash_bucket_result_size_bytes(self) -> float:
        """
        The size of the results returned by hash bucket step.
        """
        return self.get("hashBucketResultSize")

    @property
    def dedupe_result_size_bytes(self) -> float:
        """
        The size of the results returned by dedupe step.
        """
        return self.get("dedupeResultSize")

    @property
    def materialize_result_size(self) -> float:
        """
        The size of the results returned by materialize step.
        """
        return self.get("materializeResultSize")

    @property
    def merge_result_size(self) -> float:
        """
        The size of the results returned by merge step.
        """
        return self.get("mergeResultSize")

    @property
    def peak_memory_used_bytes_by_compaction_session_process(self) -> float:
        """
        The peak memory used by the entrypoint for compaction_session.
        """
        return self.get("peakMemoryUsedBytesCompactionSessionProcess")

    @property
    def estimated_in_memory_size_bytes_during_discovery(self) -> float:
        """
        The estimated in-memory size during the discovery. This can be used
        to determine the accuracy of memory estimation logic.
        """
        return self.get("estimatedInMemorySizeBytesDuringDiscovery")

    @property
    def hash_bucket_processed_size_bytes(self) -> int:
        """
        The total size of the input data processed during hash bucket
        """
        return self.get("hashBucketProcessedSizeBytes")

    @property
    def pyarrow_version(self) -> str:
        """
        The version of PyArrow used.
        """
        return self.get("pyarrowVersion")

    @property
    def compactor_version(self) -> str:
        """
        The version of the compactor used to compact.
        """
        return self.get("compactorVersion")

    @property
    def observed_input_inflation(self) -> float:
        """
        The average inflation observed for input files only.
        This only accounts for files in the source.
        """
        return self.get("observedInputInflation")

    @property
    def observed_input_average_record_size_bytes(self) -> float:
        """
        The average record size observed for input files only.
        This only accounts for files in the source.
        """
        return self.get("observedInputAverageRecordSizeBytes")

    # Setters follow

    def set_audit_url(self, audit_url: str) -> CompactionSessionAuditInfo:
        self["auditUrl"] = audit_url
        return self

    def set_deltacat_version(self, version: str) -> CompactionSessionAuditInfo:
        self["deltacatVersion"] = version
        return self

    def set_ray_version(self, version: str) -> CompactionSessionAuditInfo:
        self["rayVersion"] = version
        return self

    def set_input_records(self, input_records: int) -> CompactionSessionAuditInfo:
        self["inputRecords"] = input_records
        return self

    def set_input_file_count(self, input_file_count: int) -> CompactionSessionAuditInfo:
        self["inputFileCount"] = input_file_count
        return self

    def set_uniform_deltas_created(
        self, uniform_deltas_created: int
    ) -> CompactionSessionAuditInfo:
        self["uniformDeltasCreated"] = uniform_deltas_created
        return self

    def set_records_deduped(self, records_deduped: int) -> CompactionSessionAuditInfo:
        self["recordsDeduped"] = records_deduped
        return self

    def set_records_deleted(self, records_deleted: int) -> CompactionSessionAuditInfo:
        self["recordsDeleted"] = records_deleted
        return self

    def set_input_size_bytes(
        self, input_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["inputSizeBytes"] = input_size_bytes
        return self

    def set_hash_bucket_count(
        self, hash_bucket_count: int
    ) -> CompactionSessionAuditInfo:
        self["hashBucketCount"] = hash_bucket_count
        return self

    def set_compaction_time_in_seconds(
        self, compaction_time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["compactionTimeInSeconds"] = compaction_time_in_seconds
        return self

    def set_total_object_store_memory_used_bytes(
        self, total_object_store_memory_used_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["totalObjectStoreMemoryUsedBytes"] = total_object_store_memory_used_bytes
        return self

    def set_peak_memory_used_bytes_per_task(
        self, peak_memory_used_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["peakMemoryUsedBytesPerTask"] = peak_memory_used_bytes
        return self

    def set_peak_memory_used_bytes_per_hash_bucket_task(
        self, peak_memory_used_bytes_per_hash_bucket_task: float
    ) -> CompactionSessionAuditInfo:
        self[
            "hashBucketTaskPeakMemoryUsedBytes"
        ] = peak_memory_used_bytes_per_hash_bucket_task
        return self

    def set_peak_memory_used_bytes_per_dedupe_task(
        self, peak_memory_used_bytes_per_dedupe_task: float
    ) -> CompactionSessionAuditInfo:
        self["dedupeTaskPeakMemoryUsedBytes"] = peak_memory_used_bytes_per_dedupe_task
        return self

    def set_peak_memory_used_bytes_per_materialize_task(
        self, peak_memory_used_bytes_per_materialize_task: float
    ) -> CompactionSessionAuditInfo:
        self[
            "materializeTaskPeakMemoryUsedBytes"
        ] = peak_memory_used_bytes_per_materialize_task
        return self

    def set_peak_memory_used_bytes_per_merge_task(
        self, peak_memory_used_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["mergeTaskPeakMemoryUsedBytes"] = peak_memory_used_bytes
        return self

    def set_hash_bucket_post_object_store_memory_used_bytes(
        self, object_store_memory_used_bytes_by_hb: float
    ) -> CompactionSessionAuditInfo:
        self[
            "hashBucketPostObjectStoreMemoryUsedBytes"
        ] = object_store_memory_used_bytes_by_hb
        return self

    def set_dedupe_post_object_store_memory_used_bytes(
        self, object_store_memory_used_bytes_by_dedupe: float
    ) -> CompactionSessionAuditInfo:
        self[
            "dedupePostObjectStoreMemoryUsedBytes"
        ] = object_store_memory_used_bytes_by_dedupe
        return self

    def set_materialize_post_object_store_memory_used_bytes(
        self, object_store_memory_used_bytes_by_dedupe: float
    ) -> CompactionSessionAuditInfo:
        self[
            "materializePostObjectStoreMemoryUsedBytes"
        ] = object_store_memory_used_bytes_by_dedupe
        return self

    def set_merge_post_object_store_memory_used_bytes(
        self, object_store_memory_used_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["mergePostObjectStoreMemoryUsedBytes"] = object_store_memory_used_bytes
        return self

    def set_materialize_buckets(
        self, materialize_buckets: int
    ) -> CompactionSessionAuditInfo:
        self["materializeBuckets"] = materialize_buckets
        return self

    def set_hash_bucket_time_in_seconds(
        self, hash_bucket_time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["hashBucketTimeInSeconds"] = hash_bucket_time_in_seconds
        return self

    def set_hash_bucket_invoke_time_in_seconds(
        self, hash_bucket_invoke_time: float
    ) -> CompactionSessionAuditInfo:
        self["hashBucketInvokeTimeInSeconds"] = hash_bucket_invoke_time
        return self

    def set_hash_bucket_result_wait_time_in_seconds(
        self, wait_time: float
    ) -> CompactionSessionAuditInfo:
        self.get["hashBucketResultWaitTimeInSeconds"] = wait_time
        return self

    def set_dedupe_time_in_seconds(
        self, dedupe_time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["dedupeTimeInSeconds"] = dedupe_time_in_seconds
        return self

    def set_dedupe_invoke_time_in_seconds(
        self, dedupe_invoke_time: float
    ) -> CompactionSessionAuditInfo:
        self["dedupeInvokeTimeInSeconds"] = dedupe_invoke_time
        return self

    def set_dedupe_result_wait_time_in_seconds(
        self, wait_time: float
    ) -> CompactionSessionAuditInfo:
        self.get["dedupeResultWaitTimeInSeconds"] = wait_time
        return self

    def set_materialize_time_in_seconds(
        self, materialize_time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["materializeTimeInSeconds"] = materialize_time_in_seconds
        return self

    def set_materialize_invoke_time_in_seconds(
        self, materialize_invoke_time: float
    ) -> CompactionSessionAuditInfo:
        self["materializeInvokeTimeInSeconds"] = materialize_invoke_time
        return self

    def set_materialize_result_wait_time_in_seconds(
        self, wait_time: float
    ) -> CompactionSessionAuditInfo:
        self.get["materializeResultWaitTimeInSeconds"] = wait_time
        return self

    def set_merge_time_in_seconds(
        self, time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["mergeTimeInSeconds"] = time_in_seconds
        return self

    def set_merge_invoke_time_in_seconds(
        self, invoke_time: float
    ) -> CompactionSessionAuditInfo:
        self["mergeInvokeTimeInSeconds"] = invoke_time
        return self

    def set_merge_result_wait_time_in_seconds(
        self, wait_time: float
    ) -> CompactionSessionAuditInfo:
        self.get["mergeResultWaitTimeInSeconds"] = wait_time
        return self

    def set_delta_discovery_time_in_seconds(
        self, delta_discovery_time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["deltaDiscoveryTimeInSeconds"] = delta_discovery_time_in_seconds
        return self

    def set_output_file_count(
        self, output_file_count: float
    ) -> CompactionSessionAuditInfo:
        self["outputFileCount"] = output_file_count
        return self

    def set_output_size_bytes(
        self, output_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["outputSizeBytes"] = output_size_bytes
        return output_size_bytes

    def set_output_size_pyarrow_bytes(
        self, output_size_pyarrow_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["outputSizePyarrowBytes"] = output_size_pyarrow_bytes
        return output_size_pyarrow_bytes

    def set_total_cluster_memory_bytes(
        self, total_cluster_memory_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["totalClusterMemoryBytes"] = total_cluster_memory_bytes
        return self

    def set_total_cluster_object_store_memory_bytes(
        self, total_cluster_object_store_memory_bytes: float
    ) -> CompactionSessionAuditInfo:
        self[
            "totalClusterObjectStoreMemoryBytes"
        ] = total_cluster_object_store_memory_bytes
        return self

    def set_untouched_file_count(
        self, untouched_file_count: int
    ) -> CompactionSessionAuditInfo:
        self["untouchedFileCount"] = untouched_file_count
        return self

    def set_untouched_file_ratio(
        self, untouched_file_ratio: float
    ) -> CompactionSessionAuditInfo:
        self["untouchedFileRatio"] = untouched_file_ratio
        return self

    def set_untouched_record_count(
        self, untouched_record_count: int
    ) -> CompactionSessionAuditInfo:
        self["untouchedRecordCount"] = untouched_record_count
        return self

    def set_untouched_size_bytes(
        self, untouched_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["untouchedSizeBytes"] = untouched_size_bytes
        return self

    def set_telemetry_time_in_seconds(
        self, telemetry_time_in_seconds: float
    ) -> CompactionSessionAuditInfo:
        self["telemetryTimeInSeconds"] = telemetry_time_in_seconds
        return self

    def set_hash_bucket_result_size_bytes(
        self, hash_bucket_result_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["hashBucketResultSize"] = hash_bucket_result_size_bytes
        return self

    def set_dedupe_result_size_bytes(
        self, dedupe_result_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["dedupeResultSize"] = dedupe_result_size_bytes
        return self

    def set_materialize_result_size_bytes(
        self, materialize_result_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["materializeResultSize"] = materialize_result_size_bytes
        return self

    def set_merge_result_size_bytes(
        self, merge_result_size_bytes: float
    ) -> CompactionSessionAuditInfo:
        self["mergeResultSize"] = merge_result_size_bytes
        return self

    def set_peak_memory_used_bytes_by_compaction_session_process(
        self, peak_memory: float
    ) -> CompactionSessionAuditInfo:
        self["peakMemoryUsedBytesCompactionSessionProcess"] = peak_memory
        return self

    def set_estimated_in_memory_size_bytes_during_discovery(
        self, memory: float
    ) -> CompactionSessionAuditInfo:
        self["estimatedInMemorySizeBytesDuringDiscovery"] = memory
        return self

    def set_hash_bucket_processed_size_bytes(
        self, size: int
    ) -> CompactionSessionAuditInfo:
        self["hashBucketProcessedSizeBytes"] = size
        return self

    def set_pyarrow_version(self, value: str) -> CompactionSessionAuditInfo:
        self["pyarrowVersion"] = value
        return self

    def set_compactor_version(self, value: str) -> CompactionSessionAuditInfo:
        self["compactorVersion"] = value
        return self

    def set_observed_input_inflation(self, value: float) -> CompactionSessionAuditInfo:
        self["observedInputInflation"] = value
        return self

    def set_observed_input_average_record_size_bytes(
        self, value: float
    ) -> CompactionSessionAuditInfo:
        self["observedInputAverageRecordSizeBytes"] = value
        return self

    # High level methods to save stats
    def save_step_stats(
        self,
        step_name: str,
        task_results: Union[
            List[HashBucketResult], List[DedupeResult], List[MaterializeResult]
        ],
        task_results_retrieved_at: float,
        invoke_time_in_seconds: float,
        task_time_in_seconds: float,
    ) -> float:
        """
        Saves the stats by calling individual setters and returns the cluster telemetry time.
        """

        self[f"{step_name}TimeInSeconds"] = task_time_in_seconds
        self[f"{step_name}InvokeTimeInSeconds"] = invoke_time_in_seconds

        self[f"{step_name}ResultSize"] = get_size_of_object_in_bytes(task_results)

        (
            cluster_utilization_after_task,
            cluster_util_after_task_latency,
        ) = timed_invocation(ClusterUtilization.get_current_cluster_utilization)

        self.set_total_cluster_object_store_memory_bytes(
            cluster_utilization_after_task.total_object_store_memory_bytes
        )
        self.set_total_cluster_memory_bytes(
            cluster_utilization_after_task.total_memory_bytes
        )
        self.set_total_object_store_memory_used_bytes(
            cluster_utilization_after_task.used_object_store_memory_bytes
        )

        self[
            f"{step_name}PostObjectStoreMemoryUsedBytes"
        ] = cluster_utilization_after_task.used_object_store_memory_bytes

        telemetry_time = 0
        if task_results:
            last_task_completed_at = max(
                result.task_completed_at for result in task_results
            )

            self[f"{step_name}ResultWaitTimeInSeconds"] = (
                task_results_retrieved_at - last_task_completed_at.item()
            )

            peak_task_memory = max(
                result.peak_memory_usage_bytes for result in task_results
            )

            telemetry_time = sum(
                result.telemetry_time_in_seconds for result in task_results
            )

            self[f"{step_name}TaskPeakMemoryUsedBytes"] = peak_task_memory.item()

        return cluster_util_after_task_latency + telemetry_time

    def save_round_completion_stats(
        self,
        mat_results: List[MaterializeResult],
    ) -> None:
        """
        This method saves all the relevant stats after all the steps are completed.
        """
        pyarrow_write_result = PyArrowWriteResult.union(
            [m.pyarrow_write_result for m in mat_results]
        )

        total_count_of_src_dfl_not_touched = sum(
            m.referenced_pyarrow_write_result.files
            if m.referenced_pyarrow_write_result
            else 0
            for m in mat_results
        )

        logger.info(
            f"Got total of {total_count_of_src_dfl_not_touched} manifest files not touched."
        )
        logger.info(
            f"Got total of {pyarrow_write_result.files} manifest files during compaction."
        )
        manifest_entry_copied_by_reference_ratio = (
            (
                round(
                    total_count_of_src_dfl_not_touched / pyarrow_write_result.files, 4
                )
                * 100
            )
            if pyarrow_write_result.files != 0
            else None
        )
        logger.info(
            f"{manifest_entry_copied_by_reference_ratio} percent of manifest files are copied by reference during materialize."
        )

        untouched_file_record_count = sum(
            m.referenced_pyarrow_write_result.records
            if m.referenced_pyarrow_write_result
            else 0
            for m in mat_results
        )
        untouched_file_size_bytes = sum(
            m.referenced_pyarrow_write_result.file_bytes
            if m.referenced_pyarrow_write_result
            else 0
            for m in mat_results
        )

        self.set_untouched_file_count(total_count_of_src_dfl_not_touched)
        self.set_untouched_file_ratio(manifest_entry_copied_by_reference_ratio)
        self.set_untouched_record_count(untouched_file_record_count)
        self.set_untouched_size_bytes(untouched_file_size_bytes)

        self.set_output_file_count(pyarrow_write_result.files)
        self.set_output_size_bytes(pyarrow_write_result.file_bytes)
        self.set_output_size_pyarrow_bytes(pyarrow_write_result.pyarrow_bytes)

        self.set_peak_memory_used_bytes_per_task(
            max(
                [
                    self.peak_memory_used_bytes_per_hash_bucket_task or 0,
                    self.peak_memory_used_bytes_per_dedupe_task or 0,
                    self.peak_memory_used_bytes_per_materialize_task or 0,
                    self.peak_memory_used_bytes_per_merge_task or 0,
                ]
            )
        )

        self.set_pyarrow_version(pa.__version__)
