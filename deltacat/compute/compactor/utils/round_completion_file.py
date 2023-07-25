import json
import logging
from typing import Dict, Any
from deltacat import logs
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.storage import PartitionLocator
from deltacat.aws import s3u as s3_utils
from typing import Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_round_completion_file_s3_url(
    bucket: str, source_partition_locator: PartitionLocator
) -> str:

    base_url = source_partition_locator.path(f"s3://{bucket}")
    return f"{base_url}.json"


def read_round_completion_file(
    bucket: str,
    source_partition_locator: PartitionLocator,
    **s3_client_kwargs: Optional[Dict[str, Any]],
) -> RoundCompletionInfo:

    round_completion_file_url = get_round_completion_file_s3_url(
        bucket,
        source_partition_locator,
    )
    logger.info(f"reading round completion file from: {round_completion_file_url}")
    round_completion_info = None
    result = s3_utils.download(round_completion_file_url, False, **s3_client_kwargs)
    if result:
        json_str = result["Body"].read().decode("utf-8")
        round_completion_info = RoundCompletionInfo(json.loads(json_str))
        logger.info(f"read round completion info: {round_completion_info}")
    return round_completion_info


def write_round_completion_file(
    bucket: Optional[str],
    source_partition_locator: Optional[PartitionLocator],
    round_completion_info: RoundCompletionInfo,
    completion_file_s3_url: str = None,
    **s3_client_kwargs: Optional[Dict[str, Any]],
) -> str:
    if bucket is None and completion_file_s3_url is None:
        raise AssertionError("Either bucket or completion_file_s3_url must be passed")

    logger.info(f"writing round completion file contents: {round_completion_info}")
    if completion_file_s3_url is None:
        completion_file_s3_url = get_round_completion_file_s3_url(
            bucket,
            source_partition_locator,
        )
    logger.info(f"writing round completion file to: {completion_file_s3_url}")
    s3_utils.upload(
        completion_file_s3_url,
        str(json.dumps(round_completion_info)),
        **s3_client_kwargs,
    )
    logger.info(f"round completion file written to: {completion_file_s3_url}")
    return completion_file_s3_url
