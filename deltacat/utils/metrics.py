from dataclasses import dataclass
import functools
import time

import logging

from aws_embedded_metrics import metric_scope
from aws_embedded_metrics.config import get_config
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from deltacat import logs
from enum import Enum
from typing import Dict, Any, List, Callable
from deltacat.aws.clients import resource_cache
from datetime import datetime
import pyrsistent

from ray.util import get_node_ip_address

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

DEFAULT_DELTACAT_METRICS_NAMESPACE = "ray-deltacat-metrics"
DEFAULT_DELTACAT_LOG_GROUP_NAME = "ray-deltacat-metrics-EMF-logs"
DEFAULT_DELTACAT_LOG_STREAM_CALLABLE = get_node_ip_address


class MetricsTarget(str, Enum):
    CLOUDWATCH = "cloudwatch"
    CLOUDWATCH_EMF = "cloudwatch_emf"


# All fields should be considered read-only
@dataclass
class MetricsConfig:
    def __init__(
        self,
        region: str,
        metrics_target: MetricsTarget,
        metrics_namespace: str = DEFAULT_DELTACAT_METRICS_NAMESPACE,
        metrics_dimensions: List[Dict[str, str]] = [],
        metrics_kwargs: Dict[str, Any] = {},
    ):
        self.region = region
        self.metrics_target = metrics_target
        self.metrics_namespace = metrics_namespace

        # Pyrsistent is unable to enforce immutability on nested objects
        # Please avoid modifying stored objects to preserve read-only status
        self.metrics_dimensions = pyrsistent.v(*metrics_dimensions)
        self.metrics_kwargs = pyrsistent.m(**metrics_kwargs)

    # Enforce fields to be read-only after initialization
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name not in self.__dict__:
            self.__dict__[__name] = __value
        else:
            raise RuntimeError(f"Tried to set immutable property {__name}!")


# Global access point for a singleton metrics_config object
# It is important to re-initialize the MetricsConfigSingleton class in functions decorated with @ray.remote
class MetricsConfigSingleton(object):
    _instance = None
    _total_telemetry_time = 0

    def __init__(self):
        raise RuntimeError(
            "Tried to directly initialize MetricsConfigSingleton object, call instance() instead"
        )

    @classmethod
    def instance(cls, metrics_config: MetricsConfig = None):
        if cls._instance is None:
            assert metrics_config, (
                f"metrics_config cannot be {metrics_config} when "
                "initializing MetricsConfigSingleton instance."
            )
            cls._instance = cls.__new__(cls)
            # Put any initialization here.
            cls._metrics_config = metrics_config
        else:
            if metrics_config:
                logger.warning(
                    "MetricsConfigSingleton.instance() called with non-null metrics_config. "
                    "Passed-in metrics_config will be ignored in favor of using pre-existing value."
                )
        return cls._instance

    # The metrics_config should be considered immutable and read-only
    @property
    def metrics_config(self):
        assert (
            self._metrics_config
        ), "Private attribute _metrics_config must be initialized before accessing!"
        return self._metrics_config

    @property
    def total_telemetry_time(self):
        return self._total_telemetry_time

    def increment_telemetry_time(self, telemetry_time_in_seconds):
        self._total_telemetry_time += telemetry_time_in_seconds


class MetricsType(str, Enum):
    TIMER = "timer"


def _build_metrics_name(metrics_type: Enum, metrics_name: str) -> str:
    metrics_name_with_type = f"{metrics_name}_{metrics_type}"
    return metrics_name_with_type


def _build_cloudwatch_metrics(
    metrics_name: str,
    metrics_type: Enum,
    value: str,
    metrics_dimensions: List[Dict[str, str]],
    timestamp: datetime,
    **kwargs,
) -> Dict[str, Any]:
    metrics_name_with_type = _build_metrics_name(metrics_type, metrics_name)
    return [
        {
            "MetricName": f"{metrics_name_with_type}",
            "Dimensions": metrics_dimensions,
            "Timestamp": timestamp,
            "Value": value,
            **kwargs,
        }
    ]


def _emit_metrics(
    metrics_name: str,
    metrics_type: Enum,
    metrics_config: MetricsConfig,
    value: str,
    **kwargs,
) -> None:
    metrics_target = metrics_config.metrics_target
    assert isinstance(
        metrics_target, MetricsTarget
    ), f"{metrics_target} is not a valid supported metrics target type! "
    if metrics_target in METRICS_TARGET_TO_EMITTER_DICT:
        METRICS_TARGET_TO_EMITTER_DICT.get(metrics_target)(
            metrics_name=metrics_name,
            metrics_type=metrics_type,
            metrics_config=metrics_config,
            value=value,
            **kwargs,
        )
    else:
        logger.warning(f"{metrics_target} is not a supported metrics target type.")


def _emit_cloudwatch_metrics(
    metrics_name: str,
    metrics_type: Enum,
    metrics_config: MetricsConfig,
    value: str,
    **kwargs,
) -> None:
    current_time = datetime.now()
    current_instance_region = metrics_config.region
    cloudwatch_resource = resource_cache("cloudwatch", current_instance_region)
    cloudwatch_client = cloudwatch_resource.meta.client

    metrics_namespace = metrics_config.metrics_namespace
    metrics_dimensions = metrics_config.metrics_dimensions

    metrics_data = _build_cloudwatch_metrics(
        metrics_name,
        metrics_type,
        value,
        metrics_dimensions,
        current_time,
        **metrics_config.metrics_kwargs,
        **kwargs,
    )

    response = None
    try:
        response = cloudwatch_client.put_metric_data(
            Namespace=metrics_namespace,
            MetricData=metrics_data,
        )
    except Exception as e:
        logger.warning(
            f"Failed to publish Cloudwatch metrics with name: {metrics_name}, "
            f"type: {metrics_type}, with exception: {e}, response: {response}"
        )


@metric_scope
def _emit_cloudwatch_emf_metrics(
    metrics_name: str,
    metrics_type: Enum,
    metrics_config: MetricsConfig,
    value: str,
    metrics: MetricsLogger,
    **kwargs,
) -> None:
    """
    Publishes CloudWatch EMF logs, which are metrics embedded in CloudWatch logs.

    There are multiple possible metrics_kwargs keys that can be expected in this method.
    The documentation can be found here: github.com/awslabs/aws-embedded-metrics-python

    Keyword arguments currently being checked for:
     - log_group_name (str): specifies the CloudWatch log group name
     - log_stream_name (callable): specifies the log stream name to be used within the log group
    """
    current_time = datetime.now()
    dimensions = dict(
        [(dim["Name"], dim["Value"]) for dim in metrics_config.metrics_dimensions]
    )
    metrics_name_with_type = _build_metrics_name(metrics_type, metrics_name)
    try:
        metrics.set_timestamp(current_time)
        metrics.set_dimensions(dimensions)
        metrics.set_namespace(metrics_config.metrics_namespace)

        metrics_kwargs = metrics_config.metrics_kwargs

        emf_config = get_config()
        emf_config.log_group_name = (
            metrics_kwargs["log_group_name"]
            if "log_group_name" in metrics_kwargs
            else DEFAULT_DELTACAT_LOG_GROUP_NAME
        )

        # use a callable to differentiate many possible log streams
        emf_config.log_stream_name = (
            metrics_kwargs["log_stream_name"]()
            if "log_stream_name" in metrics_kwargs
            else DEFAULT_DELTACAT_LOG_STREAM_CALLABLE()
        )

        metrics.put_metric(metrics_name_with_type, value)
    except Exception as e:
        logger.warning(
            f"Failed to publish Cloudwatch EMF metrics with name: {metrics_name_with_type}, "
            f"type: {metrics_type}, with exception: {e}"
        )


METRICS_TARGET_TO_EMITTER_DICT: Dict[str, Callable] = {
    MetricsTarget.CLOUDWATCH: _emit_cloudwatch_metrics,
    MetricsTarget.CLOUDWATCH_EMF: _emit_cloudwatch_emf_metrics,
}


def emit_timer_metrics(metrics_name, **metrics_kwargs):
    def decorator_emit_timer_metrics(func):
        @functools.wraps(func)
        def wrapper_emit_timer_metrics(*args, **kwargs):
            metrics_config_singleton = None
            try:
                metrics_config_singleton = MetricsConfigSingleton.instance()
            except Exception as e:
                logger.warn(f"Skipping emitting timer metrics due to exception: {e}")
            start = time.perf_counter()
            result = func(*args, **kwargs)
            stop = time.perf_counter()
            if metrics_config_singleton:
                telemetry_start = time.perf_counter()
                _emit_metrics(
                    metrics_name=metrics_name,
                    metrics_type=MetricsType.TIMER,
                    metrics_config=metrics_config_singleton.metrics_config,
                    value=stop - start,
                    **metrics_kwargs,
                )
                telemetry_stop = time.perf_counter()
                metrics_config_singleton.increment_telemetry_time(
                    telemetry_stop - telemetry_start
                )
            return result

        return wrapper_emit_timer_metrics

    return decorator_emit_timer_metrics
