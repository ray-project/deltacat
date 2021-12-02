import logging
import json

from deltacat.compute.stats.models.stats_completion_info import StatsCompletionInfo
from deltacat.storage import DeltaLocator
from deltacat import logs
from deltacat.aws import s3u as s3_utils

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_stats_completion_file_s3_url(
        bucket: str,
        delta_locator: DeltaLocator) -> str:

    base_url = delta_locator.path(f"s3://{bucket}")
    return f"{base_url}.json"


def read_stats_completion_file(
        bucket: str,
        delta_locator: DeltaLocator) -> StatsCompletionInfo:

    stats_completion_file_url = get_stats_completion_file_s3_url(
        bucket,
        delta_locator
    )
    logger.info(
        f"reading stats completion file from: {stats_completion_file_url}")
    stats_completion_info_file = None
    result = s3_utils.download(stats_completion_file_url, fail_if_not_found=False)
    if result:
        json_str = result["Body"].read().decode("utf-8")
        stats_completion_info_file = StatsCompletionInfo(json.loads(json_str))
        logger.info(f"read stats completion info: {stats_completion_info_file}")
    return stats_completion_info_file


def write_stats_completion_file(
        bucket: str,
        stats_completion_info: StatsCompletionInfo):

    logger.info(
        f"writing stats completion file contents: {stats_completion_info}")
    stats_completion_file_s3_url = get_stats_completion_file_s3_url(
        bucket,
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
