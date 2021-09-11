import logging
import json
from deltacat import logs
from deltacat.storage.model import partition_locator as pl
from deltacat.compute.compactor.model \
    import primary_key_index_version_locator as pkivl
from deltacat.aws import s3u as s3_utils
from typing import Any, Dict

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def get_round_completion_file_s3_url(
        bucket: str,
        source_partition_locator: Dict[str, Any],
        pki_root_path: str) -> str:

    source_hexdigest = pl.hexdigest(source_partition_locator)
    return f"s3://{bucket}/{source_hexdigest}/{pki_root_path}.json"


def read_round_completion_file(
        bucket: str,
        source_partition_locator: Dict[str, Any],
        primary_key_index_root_path: str) -> Dict[str, Any]:

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
        round_completion_info = json.loads(json_str)
        print(f"read round completion info: {round_completion_info}")
    return round_completion_info


def write_round_completion_file(
        bucket: str,
        source_partition_locator: Dict[str, Any],
        primary_key_index_root_path: str,
        round_completion_info: Dict[str, Any]):

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
