from dataclasses import dataclass

import logging

from aws_embedded_metrics import metric_scope
from aws_embedded_metrics.config import get_config
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from deltacat import logs
from enum import Enum
from typing import Dict, Any, List, Callable
from deltacat.aws.clients import resource_cache
from datetime import datetime

from ray._private.services import get_node_ip_address

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

DEFAULT_DELTACAT_METRICS_NAMESPACE = "ray-deltacat-metrics"
DEFAULT_DELTACAT_LOG_GROUP_NAME = "ray-deltacat-metrics-EMF-logs"
DEFAULT_DELTACAT_LOG_STREAM_CALLABLE = get_node_ip_address


class MetricsTarget(str, Enum):
    CLOUDWATCH = "cloudwatch"
    CLOUDWATCH_EMF = "cloudwatch_emf"


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
        self.metrics_dimensions = metrics_dimensions
        self.metrics_kwargs = metrics_kwargs


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
    **metrics_kwargs,
) -> Dict[str, Any]:
    metrics_name_with_type = _build_metrics_name(metrics_type, metrics_name)
    return [
        {
            "MetricName": f"{metrics_name_with_type}",
            "Dimensions": metrics_dimensions,
            "Timestamp": timestamp,
            "Value": value,
            **metrics_kwargs,
        }
    ]


def _emit_metrics(
    metrics_name: str,
    metrics_type: Enum,
    metrics_config: MetricsConfig,
    value: str,
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
        )
    else:
        logger.warning(f"{metrics_target} is not a supported metrics target type.")


def _emit_cloudwatch_metrics(
    metrics_name: str,
    metrics_type: Enum,
    metrics_config: MetricsConfig,
    value: str,
) -> None:
    ct = datetime.now()
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
        ct,
        **metrics_config.metrics_kwargs,
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
    metrics: MetricsLogger,
    metrics_name: str,
    metrics_type: Enum,
    metrics_config: MetricsConfig,
    value: str,
) -> None:
    ct = datetime.now()
    dimensions = dict(
        [(dim["Name"], dim["Value"]) for dim in metrics_config.metrics_dimensions]
    )
    metrics_name_with_type = _build_metrics_name(metrics_type, metrics_name)
    try:
        metrics.set_timestamp(ct)
        metrics.set_dimensions(dimensions)
        metrics.set_namespace(metrics_config.metrics_namespace)

        metrics_kwargs = metrics_config.metrics_kwargs

        Config = get_config()
        Config.log_group_name = (
            metrics_kwargs["log_group_name"]
            if "log_group_name" in metrics_kwargs
            else DEFAULT_DELTACAT_LOG_GROUP_NAME
        )

        # use a callable to differentiate many possible log streams
        Config.log_stream_name = (
            metrics_kwargs["log_stream_name"]()
            if "log_stream_name" in metrics_kwargs
            else DEFAULT_DELTACAT_LOG_STREAM_CALLABLE
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


def emit_timer_metrics(metrics_name, value, metrics_config, **kwargs):
    _emit_metrics(
        metrics_name=metrics_name,
        metrics_type=MetricsType.TIMER,
        metrics_config=metrics_config,
        value=value,
    )
