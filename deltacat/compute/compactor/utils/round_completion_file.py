import json
import logging

from deltacat import logs
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.storage import PartitionLocator
from deltacat.aws import s3u as s3_utils

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_round_completion_file_s3_url(
    bucket: str, source_partition_locator: PartitionLocator
) -> str:

    base_url = source_partition_locator.path(f"s3://{bucket}")
    return f"{base_url}.json"


def read_round_completion_file(
    bucket: str, source_partition_locator: PartitionLocator
) -> RoundCompletionInfo:

    round_completion_file_url = get_round_completion_file_s3_url(
        bucket,
        source_partition_locator,
    )
    logger.info(f"reading round completion file from: {round_completion_file_url}")
    round_completion_info = None
    result = s3_utils.download(round_completion_file_url, False)
    if result:
        json_str = result["Body"].read().decode("utf-8")
        round_completion_info = RoundCompletionInfo(json.loads(json_str))
        logger.info(f"read round completion info: {round_completion_info}")
    return round_completion_info


def write_round_completion_file(
    bucket: str,
    source_partition_locator: PartitionLocator,
    round_completion_info: RoundCompletionInfo,
) -> str:

    logger.info(f"writing round completion file contents: {round_completion_info}")
    round_completion_file_s3_url = get_round_completion_file_s3_url(
        bucket,
        source_partition_locator,
    )
    logger.info(f"writing round completion file to: {round_completion_file_s3_url}")
    s3_utils.upload(
        round_completion_file_s3_url, str(json.dumps(round_completion_info))
    )
    logger.info(f"round completion file written to: {round_completion_file_s3_url}")
    return round_completion_file_s3_url


# write rcf given s3 url
def write_repartition_completion_file(
    repartition_completion_file_s3_url: str,
    round_completion_info: RoundCompletionInfo,
) -> str:

    logger.info(
        f"writing repartition completion file contents: {round_completion_info}"
    )
    logger.info(
        f"writing repartition completion file to: {repartition_completion_file_s3_url}"
    )
    s3_utils.upload(
        repartition_completion_file_s3_url, str(json.dumps(round_completion_info))
    )
    logger.info(
        f"repartition completion file written to: {repartition_completion_file_s3_url}"
    )
    return repartition_completion_file_s3_url
