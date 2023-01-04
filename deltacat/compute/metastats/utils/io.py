import logging
import json
import pyarrow
import ray

from deltacat import LocalTable, TableType
from deltacat.storage import Delta
from deltacat.compute.compactor import DeltaAnnotated
from deltacat.aws import s3u as s3_utils
from deltacat.utils.common import sha1_hexdigest
from deltacat.storage import interface as unimplemented_deltacat_storage
from deltacat.compute.metastats.model.partition_stats_dict import PartitionStats
from deltacat.compute.stats.models.delta_stats_cache_result import DeltaStatsCacheResult
from deltacat.compute.stats.models.delta_column_stats import DeltaColumnStats
from deltacat.compute.stats.models.delta_stats import DeltaStats, DeltaStatsCacheMiss
from deltacat.compute.stats.models.stats_result import StatsResult

from typing import Dict, List, Optional, Any
from collections import defaultdict
from deltacat import logs
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def cache_inflation_rate_data_for_delta_stats_ready(delta_stats_processed_list, inflation_rate_stats_s3_url,
                                                     deltacat_storage):
    meta_stats_processed_list: Dict[int, int] = {}

    for key, value in delta_stats_processed_list.items():
        delta_locator = value.column_stats[0].manifest_stats.delta_locator
        delta_meta_count = 0
        manifest = deltacat_storage.get_delta_manifest(delta_locator)
        delta = Delta.of(delta_locator, None, None, None, manifest)
        for entry in delta.manifest.entries:
            delta_meta_count += entry.meta.content_length
        meta_stats_processed_list[delta.stream_position] = delta_meta_count

    cache_inflation_rate_res = dict()

    for key, value in delta_stats_processed_list.items():
        delta_stats_pyarrow_bytes_sum = 0
        delta_stats_row_count = 0
        for column_stats in delta_stats_processed_list[key].column_stats[0].manifest_stats.stats:
            delta_stats_row_count += column_stats.get("rowCount")
        for stats in delta_stats_processed_list[key].get("column_stats"):

            delta_stats_pyarrow_bytes_sum += stats.get("stats").get("pyarrowTableBytes")
        cache_inflation_rate_res[key] = [meta_stats_processed_list[key], delta_stats_row_count,
                                         delta_stats_pyarrow_bytes_sum]

    if inflation_rate_stats_s3_url:
        logger.warning(
            f"reading previous inflation rate stats from: {inflation_rate_stats_s3_url}")

        result = s3_utils.download(inflation_rate_stats_s3_url, fail_if_not_found=False)

        prev_inflation_rate_stats = dict()
        if result:
            json_str = result["Body"].read().decode("utf-8")
            prev_inflation_rate_stats_read = json.loads(json_str)
            prev_inflation_rate_stats = prev_inflation_rate_stats_read if prev_inflation_rate_stats_read else dict()
            logger.debug(f"read stats completion info: {prev_inflation_rate_stats_read}")
        logger.debug(
            f"writing inflation rate info to S3: {inflation_rate_stats_s3_url}")
        prev_inflation_rate_stats.update(cache_inflation_rate_res)
        logger.debug(f"writing current inflation rate info to S3: {prev_inflation_rate_stats}")
        s3_utils.upload(
            inflation_rate_stats_s3_url,
            json.dumps(prev_inflation_rate_stats)
        )
    else:
        logger.warning(f"No valid s3 url received to cache inflation rate stats, got {inflation_rate_stats_s3_url}")


def read_cached_partition_stats(partition_canonical_string: str, stat_results_s3_bucket: str):
    partition_stats_url = get_partition_stats_s3_url(partition_canonical_string, stat_results_s3_bucket)
    logger.info(
        f"reading partition stats completion file from: {partition_stats_url}")

    result = s3_utils.download(partition_stats_url, fail_if_not_found=False)
    delta_stats_cache_res_map: Dict[int, List[DeltaStatsCacheResult]] = {}
    if result:
        json_str = result["Body"].read().decode("utf-8")
        partition_stats_str = json.loads(json_str)
        delta_stats_cache_res_map = get_delta_stats_from_partition_stats(partition_stats_str)

    return delta_stats_cache_res_map


def get_partition_stats_s3_url(partition_canonical_string: str, stat_results_s3_bucket: str):
    stats_partition_canonical_string = f"{partition_canonical_string}"
    stats_partition_hexdigest = sha1_hexdigest(stats_partition_canonical_string.encode("utf-8"))
    base_path = s3_utils.parse_s3_url(stat_results_s3_bucket).url

    return f"{base_path}/{stats_partition_hexdigest}.json"


def get_delta_stats_from_partition_stats(partition_stats_str: str):

    partition_stats = PartitionStats.build_from_dict(partition_stats_str)

    found_columns_stats_map: Dict[int, List[DeltaStatsCacheResult]] = {}
    for stream_position, delta_stats in partition_stats.delta_stats.items():
        found_columns_stats: List[DeltaColumnStats] = []
        missed_columns: List[str] = []
        for cs in delta_stats.column_stats:
            if cs.manifest_stats:
                found_columns_stats.append(cs)
            else:
                missed_columns.append(cs.column)

        delta_locator = delta_stats.column_stats[0].manifest_stats.delta_locator
        found_stats: Optional[DeltaStats] = DeltaStats.of(found_columns_stats) if found_columns_stats else None
        missed_stats: Optional[DeltaStatsCacheMiss] = DeltaStatsCacheMiss(missed_columns, delta_locator) \
            if missed_columns else None
        delta_stats_cache_res = DeltaStatsCacheResult.of(found_stats, missed_stats)
        found_columns_stats_map[int(stream_position)] = delta_stats_cache_res
    return found_columns_stats_map


def cache_partition_stats_to_s3(stat_results_s3_bucket, delta_stream_range_stats, partition_canonical_string):
    partition_stats = PartitionStats.of(delta_stream_range_stats, partition_canonical_string)
    logger.info(
        f"writing partition stats completion for {partition_canonical_string}")
    partition_stats_completion_file_s3_url = get_partition_stats_s3_url(
        partition_canonical_string,
        stat_results_s3_bucket
    )
    s3_utils.upload(
        partition_stats_completion_file_s3_url,
        str(json.dumps(partition_stats))
    )
    logger.debug(
        f"stats completion file written to: {partition_stats_completion_file_s3_url}")


@ray.remote
def collect_stats_by_columns(delta_annotated: DeltaAnnotated,
                              columns_to_compute: Optional[List[str]] = None,
                              deltacat_storage=unimplemented_deltacat_storage) -> Dict[str, Any]:
    """Materializes one manifest entry at a time to save memory usage and calculate stats from each of its columns.

    Args:
        delta: A delta object to calculate stats for
        columns_to_compute: Columns to calculate stats for. If not provided, all columns are considered.
        deltacat_storage: Client implementation of the DeltaCAT storage interface

    Returns:
        A delta wide stats container
    """
    total_tables_size = 0

    # Mapping of column_name -> [stats_file_idx_1, stats_file_idx_2, ... stats_file_idx_n]
    column_stats_map = defaultdict(lambda: [[None, None]] * len(delta_annotated["manifest"].get("entries")))
    src_da_entries = delta_annotated["manifest"].get("entries")
    manifest_annotations = delta_annotated["annotations"]
    for file_idx, manifest in enumerate(src_da_entries):
        entry_pyarrow_table: LocalTable = \
            deltacat_storage.download_delta_manifest_entry(delta_annotated, file_idx, TableType.PYARROW, columns_to_compute, equivalent_table_types="uncompacted")
        assert isinstance(entry_pyarrow_table, pyarrow.Table), \
            f"Stats collection is only supported for PyArrow tables, but received a table of " \
            f"type '{type(entry_pyarrow_table)}' for manifest entry {file_idx} of delta: {delta_annotated.locator}."
        total_tables_size += entry_pyarrow_table.nbytes
        if not columns_to_compute:
            columns_to_compute = entry_pyarrow_table.column_names

        for column_idx, pyarrow_column in enumerate(entry_pyarrow_table.columns):
            column_name = columns_to_compute[column_idx]
            origin_delta_stream_position = manifest_annotations[file_idx][-1]
            column_stats_map[column_name][file_idx] = [StatsResult.of(len(pyarrow_column), pyarrow_column.nbytes),
                                                       origin_delta_stream_position]

    return column_stats_map