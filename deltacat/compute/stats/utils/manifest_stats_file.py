import json
import logging
from typing import List

from deltacat import logs
from deltacat.aws import s3u as s3_utils
from deltacat.compute.stats.models.delta_column_stats import DeltaColumnStats
from deltacat.compute.stats.models.manifest_entry_stats import ManifestEntryStats
from deltacat.storage import DeltaLocator
from deltacat.utils.common import sha1_hexdigest

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_manifest_stats_s3_url(
    bucket: str, column_name: str, delta_locator: DeltaLocator
) -> str:
    """Returns the S3 URL path to the column-oriented delta stats

    Args:
        bucket: The S3 bucket
        column_name: The name of the column to look up stats for
        delta_locator: The reference to the delta corresponding to the manifest entries

    Returns:
        A S3 URL path
    """
    stats_column_id = f"{delta_locator.canonical_string()}|{column_name}"
    stats_column_hexdigest = sha1_hexdigest(stats_column_id.encode("utf-8"))
    base_path = s3_utils.parse_s3_url(bucket).url
    return f"{base_path}/{stats_column_hexdigest}.json"


def read_manifest_stats_by_columns(
    bucket: str, column_names: List[str], delta_locator: DeltaLocator
) -> List[DeltaColumnStats]:
    """Fetch a list of delta column stats by reading each column-oriented delta stats file from S3

    Args:
        bucket: The S3 bucket
        column_names: A list of column names to look up stats for
        delta_locator: The reference to the delta corresponding to the manifest entries

    Returns:
        A list of delta column stats
    """
    return [
        DeltaColumnStats.of(
            column, read_manifest_stats_file(bucket, column, delta_locator)
        )
        for column in column_names
    ]


def read_manifest_stats_file(
    bucket: str, column_name: str, delta_locator: DeltaLocator
) -> ManifestEntryStats:
    """Read a manifest entry stats from S3

    Args:
        bucket: The S3 bucket
        column_name: The name of the column to look up stats for
        delta_locator: The reference to the delta corresponding to the manifest entries

    Returns:
        A container that holds a list of manifest entry stats for the given column name
    """

    stats_completion_file_url = get_manifest_stats_s3_url(
        bucket, column_name, delta_locator
    )
    logger.info(f"reading stats completion file from: {stats_completion_file_url}")
    stats_completion_info_file = None
    result = s3_utils.download(stats_completion_file_url, fail_if_not_found=False)
    if result:
        json_str = result["Body"].read().decode("utf-8")
        stats_completion_info_file = ManifestEntryStats(json.loads(json_str))
        logger.info(f"read stats completion info: {stats_completion_info_file}")
    return stats_completion_info_file


def write_manifest_stats_file(
    bucket: str, column_name: str, manifest_entry_stats: ManifestEntryStats
) -> None:
    """Write a manifest entry stats into S3

    Args:
        bucket: The S3 bucket
        column_name: The name of the column which represents this manifest entry stats
        manifest_entry_stats: The manifest entry stats to serialize and store into S3
    """
    logger.info(f"writing stats completion file contents: {manifest_entry_stats}")
    stats_completion_file_s3_url = get_manifest_stats_s3_url(
        bucket,
        column_name,
        manifest_entry_stats.delta_locator,
    )
    logger.info(f"writing stats completion file to: {stats_completion_file_s3_url}")
    s3_utils.upload(stats_completion_file_s3_url, str(json.dumps(manifest_entry_stats)))
    logger.info(f"stats completion file written to: {stats_completion_file_s3_url}")
