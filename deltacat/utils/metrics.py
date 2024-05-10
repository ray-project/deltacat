from dataclasses import dataclass
from typing import Optional

import logging
import time
import functools

from aws_embedded_metrics import metric_scope
from aws_embedded_metrics.config import get_config
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from deltacat import logs
from enum import Enum
from typing import Dict, Any, List, Callable
from deltacat.aws.clients import resource_cache
from datetime import datetime

from ray.util import get_node_ip_address

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

DEFAULT_DELTACAT_METRICS_NAMESPACE = "ray-deltacat-metrics"
DEFAULT_DELTACAT_LOG_GROUP_NAME = "ray-deltacat-metrics-EMF-logs"
DEFAULT_DELTACAT_LOG_STREAM_CALLABLE = get_node_ip_address


class MetricsTarget(str, Enum):
    CLOUDWATCH = "cloudwatch"
    CLOUDWATCH_EMF = "cloudwatch_emf"
    NOOP = "noop"


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


def _build_cloudwatch_metrics(
    metrics_name: str,
    value: str,
    metrics_dimensions: List[Dict[str, str]],
    timestamp: datetime,
    **kwargs,
) -> Dict[str, Any]:
    return [
        {
            "MetricName": metrics_name,
            "Dimensions": metrics_dimensions,
            "Timestamp": timestamp,
            "Value": value,
            **kwargs,
        }
    ]


def _emit_metrics(
    metrics_name: str,
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
            metrics_config=metrics_config,
            value=value,
            **kwargs,
        )
    else:
        logger.warning(f"{metrics_target} is not a supported metrics target type.")


def _emit_noop_metrics(
    *args,
    **kwargs,
) -> None:
    pass


def _emit_cloudwatch_metrics(
    metrics_name: str,
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
            f"with exception: {e}, response: {response}"
        )


@metric_scope
def _emit_cloudwatch_emf_metrics(
    metrics_name: str,
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

        metrics.put_metric(metrics_name, value)
    except Exception as e:
        logger.warning(
            f"Failed to publish Cloudwatch EMF metrics with name: {metrics_name}", e
        )


METRICS_TARGET_TO_EMITTER_DICT: Dict[str, Callable] = {
    MetricsTarget.CLOUDWATCH: _emit_cloudwatch_metrics,
    MetricsTarget.CLOUDWATCH_EMF: _emit_cloudwatch_emf_metrics,
    MetricsTarget.NOOP: _emit_noop_metrics,
}


class MetricsConfigCache:
    metrics_config: MetricsConfig = None


def _emit_ignore_exceptions(name: Optional[str], value: Any):
    try:
        config = MetricsConfigCache.metrics_config
        if config:
            _emit_metrics(metrics_name=name, value=value, metrics_config=config)
    except BaseException as ex:
        logger.warning("Emitting metrics failed", ex)


def emit_timer_metrics(metrics_name, value, metrics_config, **kwargs):
    _emit_metrics(
        metrics_name=metrics_name,
        metrics_config=metrics_config,
        value=value,
        **kwargs,
    )


def latency_metric(original_func=None, name: Optional[str] = None):
    """
    A decorator that emits latency metrics of a function
    based on configured metrics config. Hence, make sure to set it in all worker processes:

    def setup_cache():
        from deltacat.utils.metrics import MetricsConfigCache
        MetricsConfigCache.metrics_config = metrics_config

    setup_cache();
    ray.init(address="auto", runtime_env={"worker_process_setup_hook": setup_cache})
    """

    def _decorate(func):
        metrics_name = name or f"{func.__name__}_time"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return func(*args, **kwargs)
            finally:
                end = time.monotonic()
                _emit_ignore_exceptions(name=metrics_name, value=end - start)

        return wrapper

    if original_func:
        return _decorate(original_func)

    return _decorate


def success_metric(original_func=None, name: Optional[str] = None):
    """
    A decorator that emits success metrics of a function
    based on configured metrics config. Hence, make sure to set it in all worker processes:

    def setup_cache():
        from deltacat.utils.metrics import MetricsConfigCache
        MetricsConfigCache.metrics_config = metrics_config

    setup_cache();
    ray.init(address="auto", runtime_env={"worker_process_setup_hook": setup_cache})
    """

    def _decorate(func):
        metrics_name = name or f"{func.__name__}_success_count"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            _emit_ignore_exceptions(name=metrics_name, value=1)
            return result

        return wrapper

    if original_func:
        return _decorate(original_func)

    return _decorate


def failure_metric(original_func=None, name: Optional[str] = None):
    """
    A decorator that emits failure metrics of a function
    based on configured metrics config. Hence, make sure to set it in all worker processes:

    def setup_cache():
        from deltacat.utils.metrics import MetricsConfigCache
        MetricsConfigCache.metrics_config = metrics_config

    setup_cache();
    ray.init(address="auto", runtime_env={"worker_process_setup_hook": setup_cache})
    """

    def _decorate(func):
        metrics_name = name or f"{func.__name__}_failure_count"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except BaseException as ex:
                exception_name = type(ex).__name__
                # We emit two metrics, one for exception class and
                # another for just the specified metric name.
                _emit_ignore_exceptions(
                    name=f"{metrics_name}.{exception_name}", value=1
                )
                _emit_ignore_exceptions(name=metrics_name, value=1)
                raise ex

        return wrapper

    if original_func:
        return _decorate(original_func)

    return _decorate


def metrics(original_func=None, prefix: Optional[str] = None):
    """
    A decorator that emits all metrics for a function. This decorator depends
    on a metrics config. Hence, make sure to set it in all worker processes:

    def setup_cache():
        from deltacat.utils.metrics import MetricsConfigCache
        MetricsConfigCache.metrics_config = metrics_config

    setup_cache();
    ray.init(address="auto", runtime_env={"worker_process_setup_hook": setup_cache})
    """

    def _decorate(func):
        name = prefix or func.__name__
        failure_metrics_name = f"{name}_failure_count"
        success_metrics_name = f"{name}_success_count"
        latency_metrics_name = f"{name}_time"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                result = func(*args, **kwargs)
                _emit_ignore_exceptions(name=success_metrics_name, value=1)
                return result
            except BaseException as ex:
                exception_name = type(ex).__name__
                _emit_ignore_exceptions(
                    name=f"{failure_metrics_name}.{exception_name}", value=1
                )
                _emit_ignore_exceptions(name=failure_metrics_name, value=1)
                raise ex
            finally:
                end = time.monotonic()
                _emit_ignore_exceptions(name=latency_metrics_name, value=end - start)

        return wrapper

    if original_func:
        return _decorate(original_func)

    return _decorate
