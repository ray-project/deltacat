import boto3
import logging
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.config import Config
from boto3.exceptions import ResourceNotExistsError
from functools import lru_cache
from deltacat import logs
from deltacat.aws.constants import BOTO_MAX_RETRIES

from typing import Optional

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def _resource(
        name: str,
        region: Optional[str],
        **kwargs) -> ServiceResource:
    boto_config = Config(
        retries={
            "max_attempts": BOTO_MAX_RETRIES,
            "mode": 'standard'
        }
    )
    return boto3.resource(
        name,
        region,
        config=boto_config,
        **kwargs,
    )


def _client(
        name: str,
        region: Optional[str],
        **kwargs) -> BaseClient:
    try:
        # try to re-use a client from the resource cache first
        return resource_cache(name, region, **kwargs).meta.client
    except ResourceNotExistsError:
        # fall back for clients without an associated resource
        boto_config = Config(
            retries={
                "max_attempts": BOTO_MAX_RETRIES,
                "mode": 'standard'
            }
        )
        return boto3.client(
            name,
            region,
            config=boto_config,
            **kwargs,
        )


def resource_cache(
        name: str,
        region: Optional[str],
        **kwargs) -> ServiceResource:
    # we don't use the @lru_cache decorator because Ray can't pickle it
    cached_function = lru_cache()(_resource)
    return cached_function(name, region, **kwargs)


def client_cache(
        name: str,
        region: Optional[str],
        **kwargs) -> BaseClient:
    # we don't use the @lru_cache decorator because Ray can't pickle it
    cached_function = lru_cache()(_client)
    return cached_function(name, region, **kwargs)
