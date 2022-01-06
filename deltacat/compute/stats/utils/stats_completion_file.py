import logging
import json
from typing import List

from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.compute.stats.models.delta_column_stats import DeltaColumnStats
from deltacat.storage import DeltaLocator
from deltacat import logs
from deltacat.aws import s3u as s3_utils
from deltacat.utils.common import sha1_hexdigest

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_stats_completion_file_s3_url(
        bucket: str,
        column_name: str,
        delta_locator: DeltaLocator) -> str:
    stats_column_id = f"{delta_locator.canonical_string()}|{column_name}"
    stats_column_hexdigest = sha1_hexdigest(stats_column_id.encode("utf-8"))
    base_path = s3_utils.parse_s3_url(bucket).url
    return f"{base_path}/{stats_column_hexdigest}.json"


def read_stats_completion_file_by_columns(
        bucket: str,
        column_names: List[str],
        delta_locator: DeltaLocator) -> List[DeltaColumnStats]:
    return [DeltaColumnStats.of(column, read_stats_completion_file(bucket, column, delta_locator))
            for column in column_names]


def read_stats_completion_file(
        bucket: str,
        column_name: str,
        delta_locator: DeltaLocator) -> ManifestEntryStats:

    stats_completion_file_url = get_stats_completion_file_s3_url(
        bucket,
        column_name,
        delta_locator
    )
    logger.info(
        f"reading stats completion file from: {stats_completion_file_url}")
    stats_completion_info_file = None
    result = s3_utils.download(stats_completion_file_url, fail_if_not_found=False)
    if result:
        json_str = result["Body"].read().decode("utf-8")
        stats_completion_info_file = ManifestEntryStats(json.loads(json_str))
        logger.info(f"read stats completion info: {stats_completion_info_file}")
    return stats_completion_info_file


def write_stats_completion_file(
        bucket: str,
        column_name: str,
        stats_completion_info: ManifestEntryStats):
    logger.info(
        f"writing stats completion file contents: {stats_completion_info}")
    stats_completion_file_s3_url = get_stats_completion_file_s3_url(
        bucket,
        column_name,
        stats_completion_info.delta_locator,
    )
    logger.info(
        f"writing stats completion file to: {stats_completion_file_s3_url}")
    s3_utils.upload(
        stats_completion_file_s3_url,
        str(json.dumps(stats_completion_info))
    )
    logger.info(
        f"stats completion file written to: {stats_completion_file_s3_url}")
