import logging
import json
from typing import Dict
from deltacat.storage import Delta
from deltacat.aws import s3u as s3_utils

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
        for column_stats in delta_stats_processed_list[key].get("column_stats")[0].get("manifestStats").get("stats"):
            delta_stats_row_count += column_stats.get("rowCount")
        for stats in delta_stats_processed_list[key].get("column_stats"):

            delta_stats_pyarrow_bytes_sum += stats.get("stats").get("pyarrowTableBytes")
        cache_inflation_rate_res[key] = [meta_stats_processed_list[key], delta_stats_row_count,
                                         delta_stats_pyarrow_bytes_sum]

    if inflation_rate_stats_s3_url:
        logger.info(
            f"reading previous inflation rate stats from: {inflation_rate_stats_s3_url}")

        result = s3_utils.download(inflation_rate_stats_s3_url, fail_if_not_found=False)

        prev_inflation_rate_stats = dict()
        if result:
            json_str = result["Body"].read().decode("utf-8")
            prev_inflation_rate_stats_read = json.loads(json_str)
            prev_inflation_rate_stats = prev_inflation_rate_stats_read if prev_inflation_rate_stats_read else dict()
            logger.info(f"read stats completion info: {prev_inflation_rate_stats_read}")
        logger.info(
            f"writing inflation rate info to S3: {inflation_rate_stats_s3_url}")
        prev_inflation_rate_stats.update(cache_inflation_rate_res)
        logger.info(f"writing current inflation rate info to S3: {prev_inflation_rate_stats}")
        s3_utils.upload(
            inflation_rate_stats_s3_url,
            json.dumps(prev_inflation_rate_stats)
        )
    else:
        logger.info(f"No valid s3 url received to cache inflation rate stats, got {inflation_rate_stats_s3_url}")
