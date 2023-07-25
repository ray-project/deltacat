import logging
from functools import lru_cache
from typing import Optional

import boto3
from boto3.exceptions import ResourceNotExistsError
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config

from deltacat import logs
from deltacat.aws.constants import BOTO_MAX_RETRIES

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

BOTO3_PROFILE_NAME_KWARG_KEY = "boto3_profile_name"


def _get_session_from_kwargs(input_kwargs):
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
