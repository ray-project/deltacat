import logging
from typing import Any, Dict, Generator, Optional
from botocore.config import Config
from deltacat.aws.constants import (
    BOTO_MAX_RETRIES,
    BOTO_THROTTLING_ERROR_CODES,
)
from deltacat.constants import (
    UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY,
    RETRYABLE_TRANSIENT_ERRORS,
)

from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_delay,
    wait_random_exponential,
)
import deltacat.aws.clients as aws_utils
from deltacat import logs
from deltacat.exceptions import (
    RetryableError,
    RetryableDownloadFileError,
    RetryableUploadFileError,
    NonRetryableDownloadFileError,
    NonRetryableUploadFileError,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class S3Url:
    def __init__(self, url: str):

        from urllib.parse import urlparse

        self._parsed = urlparse(url, allow_fragments=False)  # support '#' in path
        if not self._parsed.scheme:  # support paths w/o 's3://' scheme
            url = f"s3://{url}"
            self._parsed = urlparse(url, allow_fragments=False)
        if self._parsed.query:  # support '?' in path
            self.key = f"{self._parsed.path.lstrip('/')}?{self._parsed.query}"
        else:
            self.key = self._parsed.path.lstrip("/")
        self.bucket = self._parsed.netloc
        self.url = self._parsed.geturl()


def parse_s3_url(url: str) -> S3Url:
    return S3Url(url)


def s3_resource_cache(region: Optional[str], **kwargs) -> ServiceResource:

    return aws_utils.resource_cache(
        "s3",
        region,
        **kwargs,
    )


def s3_client_cache(region: Optional[str], **kwargs) -> BaseClient:

    return aws_utils.client_cache("s3", region, **kwargs)


def get_object_at_url(url: str, **s3_client_kwargs) -> Dict[str, Any]:

    s3 = s3_client_cache(None, **s3_client_kwargs)

    parsed_s3_url = parse_s3_url(url)
    return s3.get_object(Bucket=parsed_s3_url.bucket, Key=parsed_s3_url.key)


def delete_files_by_prefix(bucket: str, prefix: str, **s3_client_kwargs) -> None:

    s3 = s3_resource_cache(None, **s3_client_kwargs)
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()


def filter_paths_by_prefix(bucket, prefix):
    return objects_to_paths(
        bucket,
        filter_objects_by_prefix(bucket, prefix),
    )


def objects_to_paths(bucket, objects):
    for obj in objects:
        yield get_path_from_object(bucket, obj)


def get_path_from_object(bucket, obj):
    return "s3://{}/{}".format(bucket, obj["Key"])


def filter_objects_by_prefix(
    bucket: str, prefix: str, **s3_client_kwargs
) -> Generator[Dict[str, Any], None, None]:

    s3 = s3_client_cache(None, **s3_client_kwargs)
    params = {"Bucket": bucket, "Prefix": prefix}
    more_objects_to_list = True
    while more_objects_to_list:
        response = s3.list_objects_v2(**params)
        if "Contents" in response:
            for obj in response["Contents"]:
                yield obj
        params["ContinuationToken"] = response.get("NextContinuationToken")
        more_objects_to_list = params["ContinuationToken"] is not None


def upload(s3_url: str, body, **s3_client_kwargs) -> Dict[str, Any]:

    parsed_s3_url = parse_s3_url(s3_url)
    s3 = s3_client_cache(None, **s3_client_kwargs)
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=15),
        stop=stop_after_delay(UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )
    return retrying(
        _put_object,
        s3,
        body,
        parsed_s3_url.bucket,
        parsed_s3_url.key,
    )


def _put_object(
    s3_client, body: Any, bucket: str, key: str, **s3_put_object_kwargs
) -> Dict[str, Any]:
    try:
        return s3_client.put_object(
            Body=body, Bucket=bucket, Key=key, **s3_put_object_kwargs
        )
    except ClientError as e:
        if e.response["Error"]["Code"] in BOTO_THROTTLING_ERROR_CODES:
            error_code = e.response["Error"]["Code"]
            raise RetryableUploadFileError(
                f"Retry upload for: {bucket}/{key} after receiving {error_code}",
            ) from e
        raise NonRetryableUploadFileError(
            f"Failed table upload to: {bucket}/{key}"
        ) from e
    except RETRYABLE_TRANSIENT_ERRORS as e:
        raise RetryableUploadFileError(
            f"Retry upload for: {bucket}/{key} after receiving {type(e).__name__}"
        ) from e
    except BaseException as e:
        logger.error(
            f"Upload has failed for {bucket}/{key}. Error: {type(e).__name__}",
            exc_info=True,
        )
        raise NonRetryableUploadFileError(
            f"Failed table upload to: {bucket}/{key}"
        ) from e


def download(
    s3_url: str, fail_if_not_found: bool = True, **s3_client_kwargs
) -> Optional[Dict[str, Any]]:

    parsed_s3_url = parse_s3_url(s3_url)
    s3 = s3_client_cache(None, **s3_client_kwargs)
    retrying = Retrying(
        wait=wait_random_exponential(multiplier=1, max=15),
        stop=stop_after_delay(UPLOAD_DOWNLOAD_RETRY_STOP_AFTER_DELAY),
        retry=retry_if_exception_type(RetryableError),
    )
    return retrying(
        _get_object,
        s3,
        parsed_s3_url.bucket,
        parsed_s3_url.key,
        fail_if_not_found=fail_if_not_found,
    )


def _get_object(s3_client, bucket: str, key: str, fail_if_not_found: bool = True):
    try:
        return s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            if fail_if_not_found:
                raise NonRetryableDownloadFileError(
                    f"Failed get object from: {bucket}/{key}"
                ) from e
            logger.info(f"file not found: {bucket}/{key}")
    except RETRYABLE_TRANSIENT_ERRORS as e:
        raise RetryableDownloadFileError(
            f"Retry get object: {bucket}/{key} after receiving {type(e).__name__}"
        ) from e

    return None


def _get_s3_client_kwargs_from_token(token_holder) -> Dict[Any, Any]:
    conf = Config(retries={"max_attempts": BOTO_MAX_RETRIES, "mode": "adaptive"})
    return (
        {
            "aws_access_key_id": token_holder["accessKeyId"],
            "aws_secret_access_key": token_holder["secretAccessKey"],
            "aws_session_token": token_holder["sessionToken"],
            "config": conf,
        }
        if token_holder
        else {"config": conf}
    )
