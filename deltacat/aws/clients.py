import logging
from functools import lru_cache
import requests
from typing import Optional
from http import HTTPStatus

import boto3
from boto3.exceptions import ResourceNotExistsError
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config
from requests.adapters import HTTPAdapter, Retry, Response

from deltacat import logs
from deltacat.aws.constants import BOTO_MAX_RETRIES

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

BOTO3_PROFILE_NAME_KWARG_KEY = "boto3_profile_name"
INSTANCE_METADATA_SERVICE_URI_BASE = "169.254.169.254/latest/meta-data"
INSTANCE_PROFILE_ROLE_CREDENTIALS_PATH = (
    "/identity-credentials/ec2/security-credentials/ec2-instance"
)


def block_until_instance_metadata_service_returns_success(
    total_number_of_retries=10,
    backoff_factor=1,
) -> Optional[Response]:
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    with requests.Session as session:
        retries = Retry(
            total=total_number_of_retries,
            backoff_factor=backoff_factor,  # A backoff factor to apply between attempts after the second try: {backoff factor} * (2 ** ({number of previous retries}))
            # For example, if the backoff_factor is 1, then Retry.sleep() will sleep for [1s, 2s, 4s, 8s, …] between retries.
            status_forcelist=[
                HTTPStatus.TOO_MANY_REQUESTS,
                HTTPStatus.INTERNAL_SERVER_ERROR,
                HTTPStatus.BAD_GATEWAY,
                HTTPStatus.SERVICE_UNAVAILABLE,
                HTTPStatus.GATEWAY_TIMEOUT,
            ],
            raise_on_status=True,  # Whether, if the number of retries are exhausted, to raise a MaxRetryError, or to return a response with a response code in the 3xx range.
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http", adapter)
        response = session.get(
            f"http://{INSTANCE_METADATA_SERVICE_URI_BASE}{INSTANCE_PROFILE_ROLE_CREDENTIALS_PATH}"
        )
        print(f"response={response.text}")
        logger.info(
            f"Instance profile credentials are now accessible. Instance Metadata Service (IMDS) returned success with status code {response.status_code}"
        )
        return response


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

    boto_config = Config(retries={"max_attempts": BOTO_MAX_RETRIES, "mode": "standard"})
    return boto3_session.resource(
        name,
        region,
        config=boto_config,
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
            retries={"max_attempts": BOTO_MAX_RETRIES, "mode": "standard"}
        )
        return boto3_session.client(
            name,
            region,
            config=boto_config,
            **kwargs,
        )


def resource_cache(name: str, region: Optional[str], **kwargs) -> ServiceResource:
    # we don't use the @lru_cache decorator because Ray can't pickle it
    cached_function = lru_cache()(_resource)
    return cached_function(name, region, **kwargs)


def client_cache(name: str, region: Optional[str], **kwargs) -> BaseClient:
    # we don't use the @lru_cache decorator because Ray can't pickle it
    cached_function = lru_cache()(_client)
    return cached_function(name, region, **kwargs)


# print(block_until_instance_metadata_service_returns_success())
