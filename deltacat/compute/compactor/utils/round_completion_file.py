import logging
import json

from deltacat.storage import PartitionLocator
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_round_completion_file_s3_url(
        bucket: str,
        source_partition_locator: PartitionLocator,
        pki_root_path: str) -> str:

    base_url = source_partition_locator.path(f"s3://{bucket}")
    return f"{base_url}/{pki_root_path}.json"


def read_round_completion_file(
        bucket: str,
        source_partition_locator: PartitionLocator,
        primary_key_index_root_path: str) -> RoundCompletionInfo:

    from deltacat.aws import s3u as s3_utils
    round_completion_file_url = get_round_completion_file_s3_url(
        bucket,
        source_partition_locator,
        primary_key_index_root_path,
    )
    logger.info(
        f"reading round completion file from: {round_completion_file_url}")
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
        primary_key_index_root_path: str,
        round_completion_info: RoundCompletionInfo):

    from deltacat.aws import s3u as s3_utils
    logger.info(
        f"writing round completion file contents: {round_completion_info}")
    round_completion_file_s3_url = get_round_completion_file_s3_url(
        bucket,
        source_partition_locator,
        primary_key_index_root_path,
    )
    logger.info(
        f"writing round completion file to: {round_completion_file_s3_url}")
    s3_utils.upload(
        round_completion_file_s3_url,
        str(json.dumps(round_completion_info))
    )
    logger.info(
        f"round completion file written to: {round_completion_file_s3_url}")
