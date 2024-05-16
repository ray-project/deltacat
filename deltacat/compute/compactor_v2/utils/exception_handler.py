import botocore
from typing import Optional
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
    GeneralAssertionError,
    RetryableTimeoutError,
    DependencyRayWorkerDiedError,
    DependencyRayOutOfMemoryError,
    DependencyRayError,
)
from typing import Callable


RAY_TASK_RETRYABLE_TIMEOUT_ERROR_CODES = (
    botocore.exceptions.ConnectionError,
    botocore.exceptions.HTTPClientError,
    ConnectionError,
    TimeoutError,
)


def parametrized(dec):
    def layer(*args, **kwargs):
        def repl(f):
            return dec(f, *args, **kwargs)

        return repl

    return layer


@parametrized
def handle_exception(func: Callable, task_id: Optional[str]):
    def handle_compaction_exception(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException as e:
            handle_compaction_step_exception(e, task_id)
        return res

    return handle_compaction_exception


def handle_compaction_step_exception(e: BaseException, task_id: Optional[str] = None):
    if isinstance(e, RayError):
        _handle_ray_error(e, task_id)
    if isinstance(e, ArrowException):
        _handle_dependency_pyarrow_error(e, task_id)
    elif isinstance(e, AssertionError):
        _handle_assertion_error(e, task_id)
    elif "DaftError" in str(e):
        _handle_daft_error(e, task_id)
    elif isinstance(e, RAY_TASK_RETRYABLE_TIMEOUT_ERROR_CODES):
        _handle_retryable_timeout_error(e, task_id)
    else:
        raise e


def _handle_ray_error(e: Exception, task_id: Optional[str] = None):
    if isinstance(e, RuntimeEnvSetupError):
        raise DependencyRayRuntimeSetupError(
            msg=f"Ray failed to setup runtime env while executing task:{task_id})",
            task_id=task_id,
        ) from e
    elif isinstance(e, WorkerCrashedError) or isinstance(e, NodeDiedError):
        raise DependencyRayWorkerDiedError(
            msg=f"Ray worker died unexpectedly while executing task:{task_id}.",
            task_id=task_id,
        ) from e
    elif isinstance(e, OutOfMemoryError):
        raise DependencyRayOutOfMemoryError(
            msg=f"Ray worker Out Of Memory while executing task: {task_id}.",
            task_id=task_id,
        ) from e
    else:
        raise DependencyRayError()


def _handle_dependency_pyarrow_error(e: Exception, task_id: Optional[str] = None):
    if isinstance(e, ArrowInvalid):
        raise DependencyPyarrowInvalidError(
            msg=f"Pyarrow Invalid error occurred while executing task:{task_id}.",
            task_id=task_id,
        ) from e
    elif isinstance(e, ArrowCapacityError):
        raise DependencyPyarrowCapacityError(
            msg=f"Pyarrow Invalid error occurred while executing task:{task_id}.",
            task_id=task_id,
        ) from e
    else:
        raise DependencyPyarrowError(
            msg=f"Pyarrow error occurred while executing task:{task_id}.",
            task_id=task_id,
        ) from e


def _handle_daft_error(e: Exception, task_id: Optional[str] = None):
    raise DependencyDaftError(
        msg=f"Daft error occurred while executing task:{task_id}.",
        task_id=task_id,
    ) from e


def _handle_assertion_error(e: Exception, task_id: Optional[str] = None):
    raise GeneralAssertionError(
        msg=f"Assertion error occurred while executing task:{task_id}.",
        task_id=task_id,
    ) from e


def _handle_retryable_timeout_error(e: Exception, task_id: Optional[str] = None):
    raise RetryableTimeoutError(
        msg=f"Timeout error occurred while executing task:{task_id}.",
        task_id=task_id,
    ) from e
