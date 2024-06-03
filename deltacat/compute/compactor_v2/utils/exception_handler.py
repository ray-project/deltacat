import botocore
import ray
from ray.exceptions import (
    RayError,
    RuntimeEnvSetupError,
    WorkerCrashedError,
    NodeDiedError,
    OutOfMemoryError,
)
from pyarrow.lib import ArrowException, ArrowInvalid, ArrowCapacityError
from deltacat.exceptions import (
    DependencyPyarrowError,
    DependencyRayRuntimeSetupError,
    DependencyPyarrowInvalidError,
    DependencyPyarrowCapacityError,
    DependencyDaftError,
    DependencyDaftTransientError,
    GeneralAssertionError,
    DependencyRayWorkerDiedError,
    DependencyRayOutOfMemoryError,
    DependencyRayError,
)
from typing import Callable
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
)
from daft.exceptions import DaftTransientError, DaftCoreException
import tenacity

# List of retryable errors that can be automatically retried by Ray remote tasks
RAY_TASK_RETRYABLE_ERROR_CODES = [
    botocore.exceptions.ConnectionError,
    botocore.exceptions.HTTPClientError,
    ConnectionError,
    TimeoutError,
    DaftTransientError,
    tenacity.RetryError,
]


def handle_exception(func: Callable):
    def handle_compaction_exception(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException as e:
            handle_compaction_step_exception(e)

    return handle_compaction_exception


def handle_compaction_step_exception(e: BaseException):
    if isinstance(e, RayError):
        _handle_ray_error(e)
    if isinstance(e, ArrowException):
        _handle_dependency_pyarrow_error(e)
    elif isinstance(e, AssertionError):
        _handle_assertion_error(e)
    elif isinstance(e, DaftCoreException):
        _handle_daft_error(e)
    else:
        raise e


def _get_ray_task_id_and_node_ip():
    task_id = get_current_ray_task_id()
    node_ip = ray.util.get_node_ip_address()
    return task_id, node_ip


def _handle_ray_error(e: Exception):
    task_id, node_ip = _get_ray_task_id_and_node_ip()
    if isinstance(e, RuntimeEnvSetupError):
        raise DependencyRayRuntimeSetupError(
            msg=f"Ray failed to setup runtime env while executing task:{task_id} on node ip:{node_ip}",
            task_id=task_id,
            node_ip=node_ip,
        ) from e
    elif isinstance(e, WorkerCrashedError) or isinstance(e, NodeDiedError):
        raise DependencyRayWorkerDiedError(
            msg=f"Ray worker died unexpectedly while executing task:{task_id} on node ip:{node_ip}.",
            task_id=task_id,
            node_ip=node_ip,
        ) from e
    elif isinstance(e, OutOfMemoryError):
        raise DependencyRayOutOfMemoryError(
            msg=f"Ray worker Out Of Memory while executing task: {task_id} on node ip:{node_ip}.",
            task_id=task_id,
            node_ip=node_ip,
        ) from e
    else:
        raise DependencyRayError(
            msg=f"Dependency Ray error occurred while executing task:{task_id} on node ip:{node_ip}",
            task_id=task_id,
            node_ip=node_ip,
        ) from e


def _handle_dependency_pyarrow_error(e: Exception):
    task_id, node_ip = _get_ray_task_id_and_node_ip()
    if isinstance(e, ArrowInvalid):
        raise DependencyPyarrowInvalidError(
            msg=f"Pyarrow Invalid error occurred while executing task:{task_id} on node ip:{node_ip}.",
            task_id=task_id,
            node_ip=node_ip,
        ) from e
    elif isinstance(e, ArrowCapacityError):
        raise DependencyPyarrowCapacityError(
            msg=f"Pyarrow Invalid error occurred while executing task:{task_id} on node ip:{node_ip}.",
            task_id=task_id,
            node_ip=node_ip,
        ) from e
    else:
        raise DependencyPyarrowError(
            msg=f"Pyarrow error occurred while executing task:{task_id} on node ip:{node_ip}.",
            task_id=task_id,
            node_ip=node_ip,
        ) from e


def _handle_daft_error(e: Exception):
    task_id, node_ip = _get_ray_task_id_and_node_ip()
    if isinstance(e, DaftTransientError):
        raise DependencyDaftTransientError()
    elif isinstance(e, DaftCoreException):
        raise DependencyDaftError(
            msg=f"Daft error occurred while executing task:{task_id} on node ip:{node_ip}.",
            task_id=task_id,
            node_ip=node_ip,
        ) from e


def _handle_assertion_error(e: Exception):
    task_id, node_ip = _get_ray_task_id_and_node_ip()
    raise GeneralAssertionError(
        msg=f"Assertion error occurred while executing task:{task_id} on node ip:{node_ip}.",
        task_id=task_id,
        node_ip=node_ip,
    ) from e
