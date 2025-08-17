import logging
from functools import lru_cache
from typing import Optional
from http import HTTPStatus

import boto3
from botocore.exceptions import CredentialRetrievalError
from boto3.exceptions import ResourceNotExistsError
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config
from requests.adapters import Response
from tenacity import (
    RetryError,
    Retrying,
    wait_fixed,
    retry_if_exception,
    stop_after_delay,
    retry_if_exception_type,
    wait_random_exponential,
)

from deltacat import logs
from deltacat.aws.constants import BOTO_MAX_RETRIES
import requests


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

BOTO3_PROFILE_NAME_KWARG_KEY = "boto3_profile_name"
INSTANCE_METADATA_SERVICE_IPV4_URI = "http://169.254.169.254/latest/meta-data/"  # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
RETRYABLE_HTTP_STATUS_CODES = [
    # 429
    HTTPStatus.TOO_MANY_REQUESTS,
    # 5xx
    HTTPStatus.INTERNAL_SERVER_ERROR,
    HTTPStatus.NOT_IMPLEMENTED,
    HTTPStatus.BAD_GATEWAY,
    HTTPStatus.SERVICE_UNAVAILABLE,
    HTTPStatus.GATEWAY_TIMEOUT,
]

boto_retry_wrapper = Retrying(
    wait=wait_random_exponential(multiplier=1, max=10),
    stop=stop_after_delay(60 * 10),
    # CredentialRetrievalError can still be thrown due to throttling, even if IMDS health checks succeed.
    retry=retry_if_exception_type(CredentialRetrievalError),
)


class RetryIfRetryableHTTPStatusCode(retry_if_exception):
    """
    Retry strategy that retries if the exception is an ``HTTPError`` with
    a status code in the retryable errors list.
    """

    def __init__(self):
        def is_retryable_error(exception):
            return isinstance(exception, requests.exceptions.ConnectionError) or (
                isinstance(exception, requests.exceptions.HTTPError)
                and exception.response.status_code in RETRYABLE_HTTP_STATUS_CODES
            )

        super().__init__(predicate=is_retryable_error)


def _log_attempt_number(retry_state):
    """return the result of the last call attempt"""
    logger.warning(f"Retrying: {retry_state.attempt_number}...")


def _get_url(url: str, get_url_kwargs=None):
    if get_url_kwargs is None:
        get_url_kwargs = {}
    resp = requests.get(url, **get_url_kwargs)
    resp.raise_for_status()
    return resp


def retrying_get(
    url: str,
    retry_strategy,
    wait_strategy,
    stop_strategy,
) -> Optional[Response]:
    """Retries a request to the given URL until it succeeds.

    Args:
        retry_strategy (Callable): A function that returns a retry strategy.
        wait_strategy (Callable): A function that returns a wait strategy.
        stop_strategy (Callable): A function that returns a stop strategy.
        url (str): The URL to retry.

    Returns:
        Optional[Response]: The response from the URL, or None if the request
            failed after the maximum number of retries.
    """
    try:
        for attempt in Retrying(
            retry=retry_strategy(),
            wait=wait_strategy,
            stop=stop_strategy,
            after=_log_attempt_number,
        ):
            with attempt:
                try:
                    resp = _get_url(url)
                except requests.exceptions.HTTPError as exception:
                    # Unauthorized errors imply IMDSv2 is being used
                    if exception.response.status_code == HTTPStatus.UNAUTHORIZED:
                        return None

                    raise exception

                return resp
    except RetryError as re:
        logger.error(f"Failed to retry URL: {url} - {re}")
    logger.info(f"Unable to get from URL: {url}")
    return None


def block_until_instance_metadata_service_returns_success(
    url=INSTANCE_METADATA_SERVICE_IPV4_URI,
    retry_strategy=RetryIfRetryableHTTPStatusCode,
    wait_strategy=wait_fixed(2),  # wait 2 seconds before retrying,
    stop_strategy=stop_after_delay(60 * 30),  # stop trying after 30 minutes
) -> Optional[Response]:
    """Blocks until the instance metadata service returns a successful response.

    Args:
        retry_strategy (Callable): A function that returns a retry strategy.
        wait_strategy (Callable): A function that returns a wait strategy.
        stop_strategy (Callable): A function that returns a stop strategy.
        url (str): The URL of the instance metadata service.

    Returns:
        Optional[Response]: The response from the instance metadata service,
            or None if the request failed after the maximum number of retries.

    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    """
    # We will get a 403 HTTP status code if running deltacat not in an EC2 instance. In that case we won't want to block.
    return retrying_get(
        url,
        retry_strategy,
        wait_strategy,
        stop_strategy,
    )


def _get_session_from_kwargs(input_kwargs):
    block_until_instance_metadata_service_returns_success()
    if input_kwargs.get(BOTO3_PROFILE_NAME_KWARG_KEY) is not None:
        boto3_session = boto3.Session(
            profile_name=input_kwargs.get(BOTO3_PROFILE_NAME_KWARG_KEY)
        )
        input_kwargs.pop(BOTO3_PROFILE_NAME_KWARG_KEY)
        return boto3_session
    else:
        return boto3.Session()


def _resource(name: str, region: Optional[str], **kwargs) -> ServiceResource:
    boto3_session = _get_session_from_kwargs(kwargs)

    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES, "mode": "adaptive"})
    kwargs = {"config": boto_config, **kwargs}
    return boto3_session.resource(
        name,
        region,
        **kwargs,
    )


def _client(name: str, region: Optional[str], **kwargs) -> BaseClient:
    try:
        # try to re-use a client from the resource cache first
        return resource_cache(name, region, **kwargs).meta.client
    except ResourceNotExistsError:
        # fall back for clients without an associated resource
        boto3_session = _get_session_from_kwargs(kwargs)
        boto_config = Config(
            retries={"max_attempts": BOTO_MAX_RETRIES, "mode": "adaptive"}
        )
        kwargs = {"config": boto_config, **kwargs}
        return boto3_session.client(
            name,
            region,
            **kwargs,
        )


def resource_cache(name: str, region: Optional[str], **kwargs) -> ServiceResource:
    # we don't use the @lru_cache decorator because Ray can't pickle it
    cached_function = lru_cache()(_resource)
    return boto_retry_wrapper(cached_function, name, region, **kwargs)


def client_cache(name: str, region: Optional[str], **kwargs) -> BaseClient:
    # we don't use the @lru_cache decorator because Ray can't pickle it
    cached_function = lru_cache()(_client)
    return boto_retry_wrapper(cached_function, name, region, **kwargs)
