import json
import logging
from typing import Dict, Any
from deltacat import logs
from deltacat.compute.compactor import RoundCompletionInfo
from deltacat.storage import PartitionLocator
from deltacat.aws import s3u as s3_utils
from typing import Optional
from deltacat.utils.metrics import metrics

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_round_completion_file_s3_url(
    bucket: str,
    source_partition_locator: PartitionLocator,
    destination_partition_locator: Optional[PartitionLocator] = None,
) -> str:

    base_url = source_partition_locator.path(f"s3://{bucket}")
    if destination_partition_locator:
        base_url = destination_partition_locator.path(
            f"s3://{bucket}/{source_partition_locator.hexdigest()}"
        )

    return f"{base_url}.json"


@metrics
def read_round_completion_file(
    bucket: str,
    source_partition_locator: PartitionLocator,
    destination_partition_locator: Optional[PartitionLocator] = None,
    **s3_client_kwargs: Optional[Dict[str, Any]],
) -> RoundCompletionInfo:

    all_uris = []
    if destination_partition_locator:
        round_completion_file_url_with_destination = get_round_completion_file_s3_url(
            bucket,
            source_partition_locator,
            destination_partition_locator,
        )
        all_uris.append(round_completion_file_url_with_destination)

    # Note: we read from RCF at two different URI for backward
    # compatibility reasons.
    round_completion_file_url_prev = get_round_completion_file_s3_url(
        bucket,
        source_partition_locator,
    )

    all_uris.append(round_completion_file_url_prev)

    round_completion_info = None

    for rcf_uri in all_uris:
        logger.info(f"Reading round completion file from: {rcf_uri}")
        result = s3_utils.download(rcf_uri, False, **s3_client_kwargs)
        if result:
            json_str = result["Body"].read().decode("utf-8")
            round_completion_info = RoundCompletionInfo(json.loads(json_str))
            logger.info(f"Read round completion info: {round_completion_info}")
            break
        else:
            logger.warning(f"Round completion file not present at {rcf_uri}")

    return round_completion_info


@metrics
def write_round_completion_file(
    bucket: Optional[str],
    source_partition_locator: Optional[PartitionLocator],
    destination_partition_locator: Optional[PartitionLocator],
    round_completion_info: RoundCompletionInfo,
    completion_file_s3_url: Optional[str] = None,
    **s3_client_kwargs: Optional[Dict[str, Any]],
) -> str:
    if bucket is None and completion_file_s3_url is None:
        raise AssertionError("Either bucket or completion_file_s3_url must be passed")

    logger.info(f"writing round completion file contents: {round_completion_info}")
    if completion_file_s3_url is None:
        completion_file_s3_url = get_round_completion_file_s3_url(
            bucket,
            source_partition_locator,
            destination_partition_locator,
        )
    logger.info(f"writing round completion file to: {completion_file_s3_url}")
    s3_utils.upload(
        completion_file_s3_url,
        str(json.dumps(round_completion_info)),
        **s3_client_kwargs,
    )
    logger.info(f"round completion file written to: {completion_file_s3_url}")
    return completion_file_s3_url
