from typing import Optional
from pyarrow.lib import ArrowException, ArrowInvalid
from ray.exceptions import WorkerCrashedError, NodeDiedError, OutOfMemoryError, RayError
from deltacat.exceptions import (
    UploadTableThrottlingError,
    DependencyRayWorkerDiedError,
    DependencyRayError,
    DependencyBotocoreError,
    DependencyRayOutOfMemoryError,
    DependencyPyarrowError,
)
from botocore.exceptions import ClientError as BotocoreClientError

UPLOAD_TABLE_POSSIBLE_ERROR_CODE = ["RequestTimeTooSkewed", "SlowDown"]


def handle_compaction_step_exception(e: Exception, task_id: Optional[str] = None):
    if isinstance(e, RayError):
        _handle_dependency_ray_error(e, task_id)
    elif isinstance(e, BotocoreClientError):
        _handle_dependency_botocore_error(e, task_id)
    elif isinstance(e, ArrowException):
        _handle_dependency_pyarrow_error(e, task_id)


def _handle_dependency_ray_error(e: Exception, task_id: Optional[str] = None):
    if isinstance(e, WorkerCrashedError) or isinstance(e, NodeDiedError):
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
        raise DependencyRayError(
            msg=f"Ray exception occurred while executing task:{task_id}.",
            task_id=task_id,
        ) from e


def _handle_dependency_botocore_error(e: Exception, task_id: Optional[str] = None):
    if e.response["Error"]["Code"] in UPLOAD_TABLE_POSSIBLE_ERROR_CODE:
        raise UploadTableThrottlingError(
            msg=f"Botocore upload table error occurred while executing task: {task_id}.",
            task_id=task_id,
        ) from e
    else:
        raise DependencyBotocoreError(
            msg=f"Botocore error occurred while executing task: {task_id}.",
            task_id=task_id,
        ) from e


def _handle_dependency_pyarrow_error(e: Exception, task_id: Optional[str] = None):
    if isinstance(e, ArrowInvalid):
        raise DependencyPyarrowError(
            msg=f"Pyarrow error occurred while executing task:{task_id}.",
            task_id=task_id,
        ) from e
